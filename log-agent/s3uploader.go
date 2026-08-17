package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
)

type S3FileInfo struct {
	Key          string    `json:"key"`
	FileName     string    `json:"file_name"`
	Size         int64     `json:"size_bytes"`
	LastModified time.Time `json:"last_modified"`
}

type S3Uploader struct {
	client     *s3.Client
	bucket     string
	prefix     string
	instanceID string
	seqCounter uint64
}

func NewS3Uploader(ctx context.Context, bucket, prefix, region, instanceID string) (*S3Uploader, error) {
	var opts []func(*config.LoadOptions) error

	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKey != "" && secretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	// Ensure prefix ends with a slash if non-empty
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	return &S3Uploader{
		client:     client,
		bucket:     bucket,
		prefix:     prefix,
		instanceID: instanceID,
	}, nil
}

// UploadParquet uploads Parquet bytes to day-partitioned S3 key
func (u *S3Uploader) UploadParquet(ctx context.Context, containerName string, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	now := time.Now().UTC()
	dateStr := now.Format("2006-01-02")
	seq := atomic.AddUint64(&u.seqCounter, 1)

	// S3 Day-based partition format:
	// logs/instance_id=i-xxx/container=app/date=2026-08-17/part-1723908000-0001.parquet
	key := fmt.Sprintf("%sinstance_id=%s/container=%s/date=%s/part-%d-%04d.parquet",
		u.prefix,
		u.instanceID,
		containerName,
		dateStr,
		now.Unix(),
		seq%10000,
	)

	_, err := u.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(u.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/vnd.apache.parquet"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload to s3://%s/%s: %w", u.bucket, key, err)
	}

	log.Printf("[S3] Successfully uploaded %d bytes to s3://%s/%s", len(data), u.bucket, key)
	return nil
}

// ListFiles lists all Parquet log files in S3 matching the given filters
func (u *S3Uploader) ListFiles(ctx context.Context, instanceID, containerName, date string) ([]S3FileInfo, error) {
	if instanceID == "" {
		instanceID = u.instanceID
	}

	prefix := fmt.Sprintf("%sinstance_id=%s/", u.prefix, instanceID)
	if containerName != "" {
		prefix += fmt.Sprintf("container=%s/", containerName)
		if date != "" {
			prefix += fmt.Sprintf("date=%s/", date)
		}
	}

	output, err := u.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(u.bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects in s3: %w", err)
	}

	var files []S3FileInfo
	for _, obj := range output.Contents {
		if strings.HasSuffix(*obj.Key, ".parquet") {
			parts := strings.Split(*obj.Key, "/")
			fileName := parts[len(parts)-1]
			files = append(files, S3FileInfo{
				Key:          *obj.Key,
				FileName:     fileName,
				Size:         *obj.Size,
				LastModified: *obj.LastModified,
			})
		}
	}

	return files, nil
}

// ReadParquetFile downloads and parses a Parquet file from S3 into []LogRecord
func (u *S3Uploader) ReadParquetFile(ctx context.Context, key string) ([]LogRecord, error) {
	resp, err := u.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get s3 object: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read s3 body: %w", err)
	}

	reader := parquet.NewGenericReader[LogRecord](bytes.NewReader(data))
	defer reader.Close()

	records := make([]LogRecord, reader.NumRows())
	n, err := reader.Read(records)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to decode parquet records: %w", err)
	}

	return records[:n], nil
}
