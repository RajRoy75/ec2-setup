package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestMetricsCollector(t *testing.T) {
	collector := NewMetricsCollector("i-test-12345")
	m := collector.Collect()

	if m.MachineID != "i-test-12345" {
		t.Fatalf("Expected MachineID i-test-12345, got %s", m.MachineID)
	}

	if !m.Online {
		t.Fatalf("Expected Online true, got %v", m.Online)
	}

	// Verify JSON serializability
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("Failed to marshal MachineMetric: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Expected non-empty JSON data")
	}
}

func TestMetricsWebSocketAndREST(t *testing.T) {
	collector := NewMetricsCollector("i-ws-test")
	hub := NewMetricsHub(collector)

	// 1. Test REST endpoint
	restHandler := enableCORS(func(w http.ResponseWriter, r *http.Request) {
		m := collector.Collect()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]MachineMetric{m})
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws/metrics" {
			hub.HandleWS(w, r)
			return
		}
		if r.URL.Path == "/api/metrics" {
			restHandler(w, r)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	// Verify REST GET
	res, err := http.Get(server.URL + "/api/metrics")
	if err != nil {
		t.Fatalf("Failed GET /api/metrics: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK, got %d", res.StatusCode)
	}

	var metricsList []MachineMetric
	if err := json.NewDecoder(res.Body).Decode(&metricsList); err != nil {
		t.Fatalf("Failed to decode /api/metrics response: %v", err)
	}
	if len(metricsList) != 1 || metricsList[0].MachineID != "i-ws-test" {
		t.Fatalf("Unexpected metrics response: %+v", metricsList)
	}

	// 2. Test WS connection
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/metrics"
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect WS: %v", err)
	}
	defer wsConn.Close()

	_ = wsConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := wsConn.ReadMessage()
	if err != nil {
		t.Fatalf("Failed to read WS message from MetricsHub: %v", err)
	}

	var wsMetrics []MachineMetric
	if err := json.Unmarshal(msg, &wsMetrics); err != nil {
		t.Fatalf("Failed to parse WS JSON message: %v (raw: %s)", err, string(msg))
	}
	if len(wsMetrics) != 1 || wsMetrics[0].MachineID != "i-ws-test" {
		t.Fatalf("Unexpected WS payload: %+v", wsMetrics)
	}
}
