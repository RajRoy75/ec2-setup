package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestParquetEncoding(t *testing.T) {
	records := []LogRecord{
		{
			Timestamp:     time.Now().UTC().UnixMicro(),
			InstanceID:    "i-0123456789abcdef0",
			ContainerName: "nginx",
			Stream:        "stdout",
			Message:       "127.0.0.1 - - [17/Aug/2026:18:00:00 +0000] \"GET / HTTP/1.1\" 200 612",
		},
		{
			Timestamp:     time.Now().UTC().UnixMicro(),
			InstanceID:    "i-0123456789abcdef0",
			ContainerName: "nginx",
			Stream:        "stderr",
			Message:       "error connecting to upstream",
		},
	}

	data, err := EncodeToParquet(records)
	if err != nil {
		t.Fatalf("EncodeToParquet failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("Expected non-empty parquet bytes")
	}

	// Read back to verify schema and data
	reader := parquet.NewGenericReader[LogRecord](bytes.NewReader(data))
	defer reader.Close()

	readRecords := make([]LogRecord, len(records))
	n, err := reader.Read(readRecords)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("Failed to read parquet data: %v", err)
	}

	if n != 2 {
		t.Fatalf("Expected 2 records, got %d", n)
	}

	if readRecords[0].ContainerName != "nginx" || readRecords[0].Stream != "stdout" {
		t.Fatalf("Record mismatch: %+v", readRecords[0])
	}
}

func TestBufferDrain(t *testing.T) {
	buf := NewContainerBuffer(10)
	for i := 0; i < 5; i++ {
		buf.Append(LogRecord{
			Timestamp:     int64(i),
			ContainerName: "test-app",
			Message:       "test line",
		})
	}

	if buf.Count() != 5 {
		t.Fatalf("Expected 5 items, got %d", buf.Count())
	}

	drained := buf.Drain()
	if len(drained) != 5 {
		t.Fatalf("Expected 5 items in drain, got %d", len(drained))
	}

	if buf.Count() != 0 {
		t.Fatalf("Expected 0 items after drain, got %d", buf.Count())
	}
}
