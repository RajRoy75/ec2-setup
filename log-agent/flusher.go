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
				// Idle safety flush: writes any logs that haven't hit the 1MB threshold within the max interval
				f.FlushAll()
			case <-f.stopCh:
				return
			}
		}
	}()
}

// FlushContainer flushes a single container's buffer to S3 as an optimized Parquet file
func (f *Flusher) FlushContainer(containerName string) {
	buf := f.bufMgr.GetOrCreate(containerName)
	records := buf.Drain()
	if len(records) == 0 {
		return
	}

	parquetBytes, err := EncodeToParquet(records)
	if err != nil {
		log.Printf("[Flusher] Parquet encode error for %s (%d records): %v", containerName, len(records), err)
		return
	}

	log.Printf("[Flusher] Uploading Parquet archive for container '%s' (%d records, %d bytes) to S3...", containerName, len(records), len(parquetBytes))

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if err := f.uploader.UploadParquet(ctx, containerName, parquetBytes); err != nil {
		log.Printf("[Flusher] ❌ S3 upload error for %s: %v", containerName, err)
	} else {
		log.Printf("[Flusher] ✓ Successfully archived %d records for '%s' to S3", len(records), containerName)
	}
}

// FlushAll drains and uploads logs for all known containers
func (f *Flusher) FlushAll() {
	containers := f.bufMgr.GetAllContainers()
	for _, c := range containers {
		f.FlushContainer(c)
	}
}

// Stop initiates graceful shutdown and flushes all pending in-memory buffers to S3
func (f *Flusher) Stop() {
	close(f.stopCh)
	f.wg.Wait()
	log.Println("[Flusher] Performing final flush of all memory buffers before shutdown...")
	f.FlushAll()
	log.Println("[Flusher] All memory buffers flushed successfully.")
}
