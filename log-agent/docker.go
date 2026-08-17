package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

// S3LogCollector buffers Docker output into lines and pushes to BufferManager
type S3LogCollector struct {
	containerName string
	stream        string
	instanceID    string
	bufMgr        *BufferManager
}

func (s *S3LogCollector) Write(p []byte) (n int, err error) {
	scanner := bufio.NewScanner(bytes.NewReader(p))
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		s.bufMgr.Push(s.containerName, LogRecord{
			Timestamp:     time.Now().UTC().UnixMicro(),
			InstanceID:    s.instanceID,
			ContainerName: s.containerName,
			Stream:        s.stream,
			Message:       line,
		})
	}
	return len(p), nil
}

// WatchContainers continuously detects and tails running containers in the background for S3 archival
func WatchContainers(ctx context.Context, cli *client.Client, instanceID string, bufMgr *BufferManager) {
	activeTails := make(map[string]context.CancelFunc)
	var mu sync.Mutex

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			mu.Lock()
			for _, cancel := range activeTails {
				cancel()
			}
			mu.Unlock()
			return
		case <-ticker.C:
			containers, err := cli.ContainerList(ctx, container.ListOptions{})
			if err != nil {
				log.Printf("[Docker] Failed to list containers: %v", err)
				continue
			}

			running := make(map[string]bool)
			for _, c := range containers {
				if len(c.Names) == 0 {
					continue
				}
				name := c.Names[0][1:] // strip leading '/'
				if name == "log-agent" {
					continue // ignore self to avoid log loops
				}
				running[name] = true

				mu.Lock()
				if _, exists := activeTails[name]; !exists {
					tailCtx, cancel := context.WithCancel(ctx)
					activeTails[name] = cancel
					go tailContainerLogsForS3(tailCtx, cli, name, instanceID, bufMgr)
				}
				mu.Unlock()
			}

			// Clean up exited containers
			mu.Lock()
			for name, cancel := range activeTails {
				if !running[name] {
					cancel()
					delete(activeTails, name)
				}
			}
			mu.Unlock()
		}
	}
}

func tailContainerLogsForS3(ctx context.Context, cli *client.Client, containerName, instanceID string, bufMgr *BufferManager) {
	log.Printf("[S3-Archiver] Starting continuous log collector for: %s", containerName)
	options := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Tail:       "50",
	}

	out, err := cli.ContainerLogs(ctx, containerName, options)
	if err != nil {
		log.Printf("[S3-Archiver] Error attaching to logs for %s: %v", containerName, err)
		return
	}
	defer out.Close()

	stdoutWriter := &S3LogCollector{containerName: containerName, stream: "stdout", instanceID: instanceID, bufMgr: bufMgr}
	stderrWriter := &S3LogCollector{containerName: containerName, stream: "stderr", instanceID: instanceID, bufMgr: bufMgr}

	// stdcopy strips the 8-byte Docker multiplexing header and routes to stdout/stderr
	_, err = stdcopy.StdCopy(stdoutWriter, stderrWriter, out)
	if err != nil && err != io.EOF && ctx.Err() == nil {
		log.Printf("[S3-Archiver] Stream ended with error for %s: %v", containerName, err)
	}
}
