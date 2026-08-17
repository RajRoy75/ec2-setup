package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"


	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// wsWriter wraps a WebSocket connection to implement io.Writer with a mutex
type wsWriter struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (w *wsWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	err = w.conn.WriteMessage(websocket.TextMessage, p)
	return len(p), err
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
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

	// 5. Start 24/7 background Docker log collector for S3
	go WatchContainers(ctx, cli, instanceID, bufMgr)

	// 6. HTTP Endpoints
	// GET /containers
	http.HandleFunc("/containers", enableCORS(func(w http.ResponseWriter, r *http.Request) {
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
	}))

	// WS /logs?container=<name>&tail=<n>
	http.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		containerName := r.URL.Query().Get("container")
		if containerName == "" {
			http.Error(w, "missing container parameter", http.StatusBadRequest)
			return
		}

		// Support ?tail=0 or ?tail=all for full history (used by test.html "Load Full History")
		tailParam := r.URL.Query().Get("tail")
		tailSetting := "100"
		if tailParam == "0" || tailParam == "all" {
			tailSetting = "all"
		} else if tailParam != "" {
			tailSetting = tailParam
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("WS Upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		logCtx, logCancel := context.WithCancel(context.Background())
		defer logCancel()

		// Read pump to detect client disconnection
		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					logCancel()
					return
				}
			}
		}()

		options := container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
			Tail:       tailSetting,
		}

		out, err := cli.ContainerLogs(logCtx, containerName, options)
		if err != nil {
			_ = conn.WriteMessage(websocket.TextMessage, []byte("Error attaching to logs: "+err.Error()))
			return
		}
		defer out.Close()

		writer := &wsWriter{conn: conn}
		_, err = stdcopy.StdCopy(writer, writer, out)
		if err != nil && logCtx.Err() == nil {
			log.Printf("Streaming ended for %s: %v", containerName, err)
		}
	})

	// GET /health
	http.HandleFunc("/health", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		stats := map[string]interface{}{
			"status":      "healthy",
			"instance_id": instanceID,
			"buffers":     bufMgr.Stats(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	}))

	// POST /flush -> manually trigger flush on demand
	http.HandleFunc("/flush", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		go flusher.FlushAll()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"flush triggered"}`))
	}))

	// ── S3 Parquet Archive Query Endpoints ──

	// GET /api/archive/files?instance_id=<id>&container=<name>&date=<YYYY-MM-DD>
	http.HandleFunc("/api/archive/files", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		instID := r.URL.Query().Get("instance_id")
		contName := r.URL.Query().Get("container")
		date := r.URL.Query().Get("date")

		files, err := uploader.ListFiles(r.Context(), instID, contName, date)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to list S3 files: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(files)
	}))

	// GET /api/archive/read?key=<s3_key>
	http.HandleFunc("/api/archive/read", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing 'key' query parameter", http.StatusBadRequest)
			return
		}

		records, err := uploader.ReadParquetFile(r.Context(), key)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read Parquet file: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(records)
	}))

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
