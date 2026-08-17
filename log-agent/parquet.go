package main

import (
	"bytes"
	"fmt"

	"github.com/parquet-go/parquet-go"
)

// EncodeToParquet encodes a slice of LogRecord to Snappy-compressed Parquet bytes
func EncodeToParquet(records []LogRecord) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer

	// Configure Parquet writer with Snappy compression
	writer := parquet.NewGenericWriter[LogRecord](
		&buf,
		parquet.Compression(&parquet.Snappy),
	)

	// Write rows
	_, err := writer.Write(records)
	if err != nil {
		return nil, fmt.Errorf("failed to write parquet records: %w", err)
	}

	// Flush and finalize footer
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return buf.Bytes(), nil
}
