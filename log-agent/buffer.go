package main

import "sync"

// LogRecord represent a single row int the parquet schema
type LogRecord struct {
	Timestamp     int64  `parquet:"timestamp"`
	InstanceID    string `parquet:"instance_id"`
	ContainerName string `parquet:"container_name"`
	Stream        string `parquet:"stream"`
	Message       string `parquet:"message"`
}

type ContainerBuffer struct {
	mu      sync.Mutex
	records []LogRecord
	maxSize int
}

// Append adds a record and return true if the buffer has reached the maxSize
func (b *ContainerBuffer) Append(rec LogRecord) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.records = append(b.records, rec)

	return len(b.records) >= b.maxSize
}

// Drain clear the buffer and return all collected records
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
