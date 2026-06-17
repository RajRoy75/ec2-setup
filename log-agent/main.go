package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// wsWriter wraps a WebSocket connection to implement io.Writer
type wsWriter struct {
	conn *websocket.Conn
}

func (w *wsWriter) Write(p []byte) (n int, err error) {
	err = w.conn.WriteMessage(websocket.TextMessage, p)
	return len(p), err
}

func main() {
	// Connect to the local Docker socket
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("Failed to connect to docker daemon: %v", err)
	}

	// 1. GET /containers -> returns JSON array of container names
	http.HandleFunc("/containers", func(w http.ResponseWriter, r *http.Request) {
		containers, err := cli.ContainerList(context.Background(), container.ListOptions{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var names []string
		for _, c := range containers {
			if len(c.Names) > 0 {
				names = append(names, c.Names[0][1:]) // Strip leading slash
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(names)
	})

	// 2. WS /logs?container=<name> -> stream logs
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

		options := container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
			Tail:       "100",
		}

		out, err := cli.ContainerLogs(context.Background(), containerName, options)
		if err != nil {
			conn.WriteMessage(websocket.TextMessage, []byte("Error attaching to logs: "+err.Error()))
			return
		}
		defer out.Close()

		// Docker sends multiplexed logs (stdout/stderr have 8-byte headers).
		// stdcopy.StdCopy strips the headers and routes the raw text.
		writer := &wsWriter{conn: conn}
		_, err = stdcopy.StdCopy(writer, writer, out)
		if err != nil {
			log.Printf("Error streaming logs for %s: %v", containerName, err)
		}
	})

	log.Println("log-agent listening on :8081")
	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
