package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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

// UploadParquet uploads Parquet bytes to Hive-partitioned S3 key
func (u *S3Uploader) UploadParquet(ctx context.Context, containerName string, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	now := time.Now().UTC()
	dateStr := now.Format("2006-01-02")
	hourStr := now.Format("15")
	seq := atomic.AddUint64(&u.seqCounter, 1)

	// S3 Hive partition format:
	// logs/instance_id=i-xxx/container=app/date=2026-08-17/hour=18/part-1723908000-0001.parquet
	key := fmt.Sprintf("%sinstance_id=%s/container=%s/date=%s/hour=%s/part-%d-%04d.parquet",
		u.prefix,
		u.instanceID,
		containerName,
		dateStr,
		hourStr,
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
