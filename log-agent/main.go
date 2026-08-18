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
	"strings"
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
	flushIntervalStr := getEnv("FLUSH_INTERVAL", "30m")
	port := getEnv("PORT", "8081")

	// 1MB Memory Buffer Configuration (flushes when buffer reaches 1MB)
	bufferSizeMBStr := getEnv("BUFFER_SIZE_MB", "1")
	bufferBytesStr := getEnv("FLUSH_BUFFER_BYTES", "")
	bufferMaxBytes := 1024 * 1024 // default 1MB (1,048,576 bytes)

	if bufferBytesStr != "" {
		if b, err := strconv.Atoi(bufferBytesStr); err == nil && b > 0 {
			bufferMaxBytes = b
		}
	} else if mb, err := strconv.ParseFloat(bufferSizeMBStr, 64); err == nil && mb > 0 {
		bufferMaxBytes = int(mb * 1024 * 1024)
	}

	flushInterval, err := time.ParseDuration(flushIntervalStr)
	if err != nil {
		log.Fatalf("Invalid FLUSH_INTERVAL: %v", err)
	}

	// 1. Identify Instance / Server
	instanceID := GetInstanceID()
	log.Printf("Starting log-agent on instance: %s | S3 Bucket: %s | Buffer Memory: %d KB | Max Idle Interval: %v", instanceID, s3Bucket, bufferMaxBytes/1024, flushInterval)

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

	// 4. Initialize Buffer & Flusher with 1MB Memory Buffer Threshold
	var flusher *Flusher
	bufMgr := NewBufferManager(bufferMaxBytes, func(containerName string) {
		if flusher != nil {
			log.Printf("[Buffer] Container '%s' reached 1MB threshold -> Triggering immediate S3 flush", containerName)
			go flusher.FlushContainer(containerName)
		}
	})

	flusher = NewFlusher(bufMgr, uploader, flushInterval)
	flusher.Start()

	// 5. Start 24/7 background Docker log collector for S3
	go WatchContainers(ctx, cli, instanceID, bufMgr)

	// 6. Initialize Machine Metrics Collector & WebSocket Hub
	metricsCollector := NewMetricsCollector(instanceID)
	metricsHub := NewMetricsHub(metricsCollector)

	// WS /ws/metrics
	http.HandleFunc("/ws/metrics", metricsHub.HandleWS)
	// GET /api/metrics
	http.HandleFunc("/api/metrics", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		metric := metricsCollector.Collect()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]MachineMetric{metric})
	}))

	// 7. HTTP Endpoints
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

	// WS /logs and /ws/logs?container=<name>&tail=<n>
	logsHandler := func(w http.ResponseWriter, r *http.Request) {
		containerName := r.URL.Query().Get("container")
		if containerName == "" {
			http.Error(w, "missing container parameter", http.StatusBadRequest)
			return
		}

		// Support ?tail=0 or ?tail=all for full history
		tailParam := r.URL.Query().Get("tail")
		tailSetting := "150"
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
	}
	http.HandleFunc("/logs", logsHandler)
	http.HandleFunc("/ws/logs", logsHandler)

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

	// POST /flush -> manually trigger flush on demand (synchronously uploads to S3)
	http.HandleFunc("/flush", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		flusher.FlushAll()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"flushed","message":"all container buffers successfully uploaded to S3"}`))
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

	// GET /api/servers and /servers -> returns configured monitored machines from SERVERS env var
	serversHandler := enableCORS(func(w http.ResponseWriter, r *http.Request) {
		serversRaw := os.Getenv("SERVERS")
		w.Header().Set("Content-Type", "application/json")

		type ServerItem struct {
			ID         string `json:"id"`
			Name       string `json:"name"`
			IP         string `json:"ip"`
			MetricsURL string `json:"metrics_url"`
			AgentURL   string `json:"agent_url"`
		}

		if serversRaw == "" {
			defaultList := []ServerItem{
				{ID: "local", Name: "Localhost Dev", IP: "127.0.0.1", MetricsURL: "http://localhost:8080", AgentURL: "http://localhost:" + port},
				{ID: instanceID, Name: "Current Machine (" + instanceID + ")", IP: "127.0.0.1", MetricsURL: "http://localhost:8080", AgentURL: "http://localhost:" + port},
			}
			_ = json.NewEncoder(w).Encode(defaultList)
			return
		}

		// Try parsing as []ServerItem
		var objList []ServerItem
		if err := json.Unmarshal([]byte(serversRaw), &objList); err == nil && len(objList) > 0 && objList[0].IP != "" {
			_ = json.NewEncoder(w).Encode(objList)
			return
		}

		// Try parsing as []string of IPs
		var strList []string
		if err := json.Unmarshal([]byte(serversRaw), &strList); err == nil && len(strList) > 0 {
			var result []ServerItem
			for i, ip := range strList {
				ip = strings.TrimSpace(ip)
				if ip == "" {
					continue
				}
				result = append(result, ServerItem{
					ID:         fmt.Sprintf("server-%d", i+1),
					Name:       fmt.Sprintf("Server %d (%s)", i+1, ip),
					IP:         ip,
					MetricsURL: fmt.Sprintf("http://%s:8080", ip),
					AgentURL:   fmt.Sprintf("http://%s:%s", ip, port),
				})
			}
			_ = json.NewEncoder(w).Encode(result)
			return
		}

		// Fallback: Parse comma/newline separated IPs
		rawParts := strings.FieldsFunc(serversRaw, func(c rune) bool {
			return c == ',' || c == '\n' || c == ';'
		})
		var result []ServerItem
		for i, part := range rawParts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			result = append(result, ServerItem{
				ID:         fmt.Sprintf("server-%d", i+1),
				Name:       fmt.Sprintf("Server %d (%s)", i+1, part),
				IP:         part,
				MetricsURL: fmt.Sprintf("http://%s:8080", part),
				AgentURL:   fmt.Sprintf("http://%s:%s", part, port),
			})
		}
		_ = json.NewEncoder(w).Encode(result)
	})
	http.HandleFunc("/api/servers", serversHandler)
	http.HandleFunc("/servers", serversHandler)

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
	log.Println("[Shutdown] Received shutdown signal (SIGTERM/SIGINT). Initiating graceful shutdown...")

	// 1. Cancel background Docker tailing so no new log reads occur
	cancel()

	// 2. Flush all remaining in-memory container logs to S3
	log.Println("[Shutdown] Flushing all remaining in-memory logs to S3...")
	flusher.Stop()
	log.Println("[Shutdown] ✓ All in-memory logs successfully flushed to S3.")

	// 3. Gracefully shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)

	log.Println("[Shutdown] ✓ log-agent stopped cleanly.")
}
