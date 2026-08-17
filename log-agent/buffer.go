package main

import (
	"sync"
)

// LogRecord represents a single row in the Parquet and JSON schema
type LogRecord struct {
	Timestamp     int64  `parquet:"timestamp" json:"timestamp"`
	InstanceID    string `parquet:"instance_id" json:"instance_id"`
	ContainerName string `parquet:"container_name" json:"container_name"`
	Stream        string `parquet:"stream" json:"stream"`
	Message       string `parquet:"message" json:"message"`
}

// ContainerBuffer stores records for one specific container
type ContainerBuffer struct {
	mu      sync.Mutex
	records []LogRecord
	maxSize int
}

func NewContainerBuffer(maxSize int) *ContainerBuffer {
	return &ContainerBuffer{
		records: make([]LogRecord, 0, maxSize),
		maxSize: maxSize,
	}
}

// Append adds a record and returns true if the buffer has reached maxSize
func (b *ContainerBuffer) Append(rec LogRecord) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.records = append(b.records, rec)
	return len(b.records) >= b.maxSize
}

// Drain clears the buffer and returns all collected records
func (b *ContainerBuffer) Drain() []LogRecord {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.records) == 0 {
		return nil
	}

	out := b.records
	b.records = make([]LogRecord, 0, b.maxSize)
	return out
}

// Count returns the current number of buffered items
func (b *ContainerBuffer) Count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}

// BufferManager manages buffers for all running containers
type BufferManager struct {
	mu          sync.RWMutex
	buffers     map[string]*ContainerBuffer
	batchSize   int
	onFullBatch func(containerName string)
}

func NewBufferManager(batchSize int, onFullBatch func(containerName string)) *BufferManager {
	return &BufferManager{
		buffers:     make(map[string]*ContainerBuffer),
		batchSize:   batchSize,
		onFullBatch: onFullBatch,
	}
}

func (bm *BufferManager) GetOrCreate(containerName string) *ContainerBuffer {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if buf, exists := bm.buffers[containerName]; exists {
		return buf
	}
	buf := NewContainerBuffer(bm.batchSize)
	bm.buffers[containerName] = buf
	return buf
}

func (bm *BufferManager) Push(containerName string, rec LogRecord) {
	buf := bm.GetOrCreate(containerName)
	if isFull := buf.Append(rec); isFull && bm.onFullBatch != nil {
		bm.onFullBatch(containerName)
	}
}

func (bm *BufferManager) GetAllContainers() []string {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	keys := make([]string, 0, len(bm.buffers))
	for k := range bm.buffers {
		keys = append(keys, k)
	}
	return keys
}

func (bm *BufferManager) Stats() map[string]int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	stats := make(map[string]int)
	for k, v := range bm.buffers {
		stats[k] = v.Count()
	}
	return stats
}
