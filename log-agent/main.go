package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	// Configuration from Environment Variables
	s3Bucket := os.Getenv("S3_BUCKET")
	if s3Bucket == "" {
		log.Fatal("FATAL: Environment variable S3_BUCKET must be set")
	}
	s3Prefix := getEnv("S3_PREFIX", "logs/")
	awsRegion := os.Getenv("AWS_REGION")
	flushIntervalStr := getEnv("FLUSH_INTERVAL", "5m")
	flushBatchSizeStr := getEnv("FLUSH_BATCH_SIZE", "10000")
	port := getEnv("PORT", "8081")

	flushInterval, err := time.ParseDuration(flushIntervalStr)
	if err != nil {
		log.Fatalf("Invalid FLUSH_INTERVAL: %v", err)
	}

	flushBatchSize, err := strconv.Atoi(flushBatchSizeStr)
	if err != nil {
		log.Fatalf("Invalid FLUSH_BATCH_SIZE: %v", err)
	}

	// 1. Identify Instance / Server
	instanceID := GetInstanceID()
	log.Printf("Starting log-agent on instance: %s | S3 Bucket: %s | Flush Interval: %v", instanceID, s3Bucket, flushInterval)

	// 2. Connect to Docker
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("Failed to connect to docker daemon: %v", err)
	}

	// 3. Initialize AWS S3 Uploader
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uploader, err := NewS3Uploader(ctx, s3Bucket, s3Prefix, awsRegion, instanceID)
	if err != nil {
		log.Fatalf("Failed to initialize S3 uploader: %v", err)
	}

	// 4. Initialize Buffer & Flusher
	var flusher *Flusher
	bufMgr := NewBufferManager(flushBatchSize, func(containerName string) {
		if flusher != nil {
			go flusher.FlushContainer(containerName)
		}
	})

	flusher = NewFlusher(bufMgr, uploader, flushInterval)
	flusher.Start()

	// 5. Initialize Live WebSocket Hub & Background Collector
	hub := NewHub()
	go WatchContainers(ctx, cli, instanceID, bufMgr, hub)

	// 6. HTTP Endpoints
	// GET /containers
	http.HandleFunc("/containers", func(w http.ResponseWriter, r *http.Request) {
		containers, err := cli.ContainerList(context.Background(), container.ListOptions{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var names []string
		for _, c := range containers {
			if len(c.Names) > 0 {
				names = append(names, c.Names[0][1:])
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(names)
	})

	// WS /logs?container=<name>
	http.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		containerName := r.URL.Query().Get("container")
		if containerName == "" {
			http.Error(w, "missing container parameter", http.StatusBadRequest)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("WS Upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		hub.Register(containerName, conn)
		defer hub.Unregister(containerName, conn)

		// Keep connection open until client disconnects
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	})

	// GET /health
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		stats := map[string]interface{}{
			"status":      "healthy",
			"instance_id": instanceID,
			"buffers":     bufMgr.Stats(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	})

	// POST /flush -> manually trigger flush on demand
	http.HandleFunc("/flush", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		go flusher.FlushAll()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"flush triggered"}`))
	})

	server := &http.Server{Addr: ":" + port}

	// 7. Graceful Shutdown Listener
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("log-agent HTTP listening on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-sigCh
	log.Println("Received shutdown signal. Stopping services...")

	// Cancel background Docker tailing
	cancel()

	// Shutdown HTTP Server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)

	// Flush all remaining records to S3
	flusher.Stop()

	log.Println("log-agent stopped cleanly.")
}
