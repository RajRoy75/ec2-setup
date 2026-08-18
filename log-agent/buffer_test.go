package main

import (
	"testing"
	"time"
)

func TestBufferMemoryThreshold(t *testing.T) {
	// Set threshold to 1KB (1024 bytes) for test
	thresholdBytes := 1024
	buf := NewContainerBuffer(thresholdBytes)

	// Append small records
	smallMsg := "short log line"
	isFull := false
	for i := 0; i < 10; i++ {
		isFull = buf.Append(LogRecord{
			Timestamp:     time.Now().UnixMicro(),
			InstanceID:    "test-node",
			ContainerName: "web",
			Stream:        "stdout",
			Message:       smallMsg,
		})
	}

	if isFull {
		t.Fatalf("Buffer should not be full yet (bytes: %d)", buf.Bytes())
	}

	// Append a 1.5KB record to breach threshold
	largeMsg := string(make([]byte, 1500))
	isFull = buf.Append(LogRecord{
		Timestamp:     time.Now().UnixMicro(),
		InstanceID:    "test-node",
		ContainerName: "web",
		Stream:        "stdout",
		Message:       largeMsg,
	})

	if !isFull {
		t.Fatalf("Buffer should trigger full on exceeding %d bytes (current: %d)", thresholdBytes, buf.Bytes())
	}

	// Verify Drain resets bytes and count
	drained := buf.Drain()
	if len(drained) != 11 {
		t.Fatalf("Expected 11 records in drain, got %d", len(drained))
	}
	if buf.Bytes() != 0 {
		t.Fatalf("Buffer bytes should be 0 after drain, got %d", buf.Bytes())
	}
	if buf.Count() != 0 {
		t.Fatalf("Buffer count should be 0 after drain, got %d", buf.Count())
	}
}

func TestBufferManagerMultiContainer(t *testing.T) {
	bm := NewBufferManager(500, func(containerName string) {})

	// Push logs for 3 different containers
	containers := []string{"auth-service", "api-gateway", "billing"}
	for _, c := range containers {
		for i := 0; i < 5; i++ {
			bm.Push(c, LogRecord{
				Timestamp:     time.Now().UnixMicro(),
				ContainerName: c,
				Stream:        "stdout",
				Message:       "processing request",
			})
		}
	}

	all := bm.GetAllContainers()
	if len(all) != 3 {
		t.Fatalf("Expected 3 containers, got %d", len(all))
	}

	stats := bm.Stats()
	if len(stats) != 3 {
		t.Fatalf("Expected stats for 3 containers, got %d", len(stats))
	}

	for _, c := range containers {
		if stats[c]["records"] != 5 {
			t.Fatalf("Expected 5 records for %s, got %d", c, stats[c]["records"])
		}
		if stats[c]["bytes"] <= 0 {
			t.Fatalf("Expected positive bytes for %s", c)
		}
	}
}
