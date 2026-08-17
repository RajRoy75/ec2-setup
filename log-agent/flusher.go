package main

import (
	"context"
	"log"
	"sync"
	"time"
)

type Flusher struct {
	bufMgr   *BufferManager
	uploader *S3Uploader
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewFlusher(bufMgr *BufferManager, uploader *S3Uploader, interval time.Duration) *Flusher {
	return &Flusher{
		bufMgr:   bufMgr,
		uploader: uploader,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

func (f *Flusher) Start() {
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		ticker := time.NewTicker(f.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				f.FlushAll()
			case <-f.stopCh:
				return
			}
		}
	}()
}

// FlushContainer flushes a single container's buffer
func (f *Flusher) FlushContainer(containerName string) {
	buf := f.bufMgr.GetOrCreate(containerName)
	records := buf.Drain()
	if len(records) == 0 {
		return
	}

	parquetBytes, err := EncodeToParquet(records)
	if err != nil {
		log.Printf("[Flusher] Parquet encode error for %s: %v", containerName, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := f.uploader.UploadParquet(ctx, containerName, parquetBytes); err != nil {
		log.Printf("[Flusher] S3 upload error for %s: %v", containerName, err)
	}
}

// FlushAll drains and uploads logs for all known containers
func (f *Flusher) FlushAll() {
	containers := f.bufMgr.GetAllContainers()
	for _, c := range containers {
		f.FlushContainer(c)
	}
}

// Stop initiates graceful shutdown and flushes all pending logs
func (f *Flusher) Stop() {
	close(f.stopCh)
	f.wg.Wait()
	log.Println("[Flusher] Performing final flush before shutdown...")
	f.FlushAll()
	log.Println("[Flusher] All buffers flushed successfully.")
}
