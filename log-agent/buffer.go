package main

import (
	"sync"
	"time"
)

// LogRecord represents a single row in the Parquet and JSON schema
type LogRecord struct {
	Timestamp     int64  `parquet:"timestamp" json:"timestamp"`
	InstanceID    string `parquet:"instance_id" json:"instance_id"`
	ContainerName string `parquet:"container_name" json:"container_name"`
	Stream        string `parquet:"stream" json:"stream"`
	Message       string `parquet:"message" json:"message"`
}

// ContainerBuffer stores records for one specific container with a byte-size threshold
type ContainerBuffer struct {
	mu           sync.Mutex
	records      []LogRecord
	currentBytes int
	maxBytes     int
	lastFlushed  time.Time
}

func NewContainerBuffer(maxBytes int) *ContainerBuffer {
	if maxBytes <= 0 {
		maxBytes = 1 * 1024 * 1024 // default 1MB buffer
	}
	return &ContainerBuffer{
		records:      make([]LogRecord, 0, 1024),
		currentBytes: 0,
		maxBytes:     maxBytes,
		lastFlushed:  time.Now(),
	}
}

// Append adds a record, tracks byte memory usage, and returns true if the buffer has reached maxBytes (e.g., 1MB)
func (b *ContainerBuffer) Append(rec LogRecord) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Approximate memory usage of the record: message string length + stream + metadata overhead (~32 bytes)
	recordSize := len(rec.Message) + len(rec.Stream) + len(rec.InstanceID) + len(rec.ContainerName) + 32
	b.records = append(b.records, rec)
	b.currentBytes += recordSize

	return b.currentBytes >= b.maxBytes
}

// Drain clears the buffer and returns all collected records and resets byte counter
func (b *ContainerBuffer) Drain() []LogRecord {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.records) == 0 {
		return nil
	}

	out := b.records
	b.records = make([]LogRecord, 0, 1024)
	b.currentBytes = 0
	b.lastFlushed = time.Now()
	return out
}

// Count returns the current number of buffered items
func (b *ContainerBuffer) Count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}

// Bytes returns the current byte size in the buffer
func (b *ContainerBuffer) Bytes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.currentBytes
}

// BufferManager manages 1MB memory buffers for all running containers
type BufferManager struct {
	mu          sync.RWMutex
	buffers     map[string]*ContainerBuffer
	maxBytes    int
	onFullBatch func(containerName string)
}

func NewBufferManager(maxBytes int, onFullBatch func(containerName string)) *BufferManager {
	if maxBytes <= 0 {
		maxBytes = 1 * 1024 * 1024 // default 1MB
	}
	return &BufferManager{
		buffers:     make(map[string]*ContainerBuffer),
		maxBytes:    maxBytes,
		onFullBatch: onFullBatch,
	}
}

func (bm *BufferManager) GetOrCreate(containerName string) *ContainerBuffer {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if buf, exists := bm.buffers[containerName]; exists {
		return buf
	}
	buf := NewContainerBuffer(bm.maxBytes)
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

func (bm *BufferManager) Stats() map[string]map[string]int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	stats := make(map[string]map[string]int)
	for k, v := range bm.buffers {
		stats[k] = map[string]int{
			"records": v.Count(),
			"bytes":   v.Bytes(),
		}
	}
	return stats
}
