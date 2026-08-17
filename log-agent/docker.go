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
	"github.com/gorilla/websocket"
)

type Hub struct {
	mu      sync.Mutex
	clients map[string]map[*websocket.Conn]bool
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[string]map[*websocket.Conn]bool),
	}
}

func (h *Hub) Register(containerName string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.clients[containerName]; !exists {
		h.clients[containerName] = make(map[*websocket.Conn]bool)
	}
	h.clients[containerName][conn] = true
}

func (h *Hub) Unregister(containerName string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if conns, exists := h.clients[containerName]; exists {
		delete(conns, conn)
		if len(conns) == 0 {
			delete(h.clients, containerName)
		}
	}
}

func (h *Hub) Broadcast(containerName string, msg []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for conn := range h.clients[containerName] {
		_ = conn.WriteMessage(websocket.TextMessage, msg)
	}
}

// StreamSplitter splits Docker output into lines, saves to S3 Buffer, and sends to WebSockets
type StreamSplitter struct {
	containerName string
	stream        string
	instanceID    string
	bufMgr        *BufferManager
	hub           *Hub
}

func (s *StreamSplitter) Write(p []byte) (n int, err error) {
	s.hub.Broadcast(s.containerName, p)

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

// WatchContainers continuously detects and tails running containers in the background
func WatchContainers(ctx context.Context, cli *client.Client, instanceID string, bufMgr *BufferManager, hub *Hub) {
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
					go tailContainerLogs(tailCtx, cli, name, instanceID, bufMgr, hub)
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

func tailContainerLogs(ctx context.Context, cli *client.Client, containerName, instanceID string, bufMgr *BufferManager, hub *Hub) {
	log.Printf("[Docker] Starting log collector for: %s", containerName)
	options := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Tail:       "50",
	}

	out, err := cli.ContainerLogs(ctx, containerName, options)
	if err != nil {
		log.Printf("[Docker] Error attaching to logs for %s: %v", containerName, err)
		return
	}
	defer out.Close()

	stdoutWriter := &StreamSplitter{containerName: containerName, stream: "stdout", instanceID: instanceID, bufMgr: bufMgr, hub: hub}
	stderrWriter := &StreamSplitter{containerName: containerName, stream: "stderr", instanceID: instanceID, bufMgr: bufMgr, hub: hub}

	// stdcopy strips the 8-byte Docker multiplexing header and routes to stdout/stderr
	_, err = stdcopy.StdCopy(stdoutWriter, stderrWriter, out)
	if err != nil && err != io.EOF && ctx.Err() == nil {
		log.Printf("[Docker] Stream ended with error for %s: %v", containerName, err)
	}
}
