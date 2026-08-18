package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

type MachineMetric struct {
	MachineID   string  `json:"machine_id"`
	Label       string  `json:"label"`
	IP          string  `json:"ip"`
	Online      bool    `json:"online"`
	CPUPercent  float64 `json:"cpu_percent"`
	RAMUsedMB   int     `json:"ram_used_mb"`
	RAMTotalMB  int     `json:"ram_total_mb"`
	RAMPercent  float64 `json:"ram_percent"`
	DiskUsedGB  int     `json:"disk_used_gb"`
	DiskTotalGB int     `json:"disk_total_gb"`
	DiskPercent float64 `json:"disk_percent"`
	NetRxBytes  int64   `json:"net_rx_bytes"`
	NetTxBytes  int64   `json:"net_tx_bytes"`
}

type MetricsCollector struct {
	instanceID string
	mu         sync.Mutex
	lastCPUTotal uint64
	lastCPUIdle  uint64
	lastNetRx    int64
	lastNetTx    int64
}

func NewMetricsCollector(instanceID string) *MetricsCollector {
	return &MetricsCollector{
		instanceID: instanceID,
	}
}

// Collect reads system metrics directly from Linux /proc or syscalls
func (m *MetricsCollector) Collect() MachineMetric {
	m.mu.Lock()
	defer m.mu.Unlock()

	metric := MachineMetric{
		MachineID: m.instanceID,
		Label:     fmt.Sprintf("EC2 Instance (%s)", m.instanceID),
		IP:        "127.0.0.1",
		Online:    true,
	}

	// 1. CPU Usage %
	if cpuPct, err := m.readCPU(); err == nil {
		metric.CPUPercent = cpuPct
	}

	// 2. RAM Usage
	if ramTotal, ramUsed, ramPct, err := m.readRAM(); err == nil {
		metric.RAMTotalMB = ramTotal
		metric.RAMUsedMB = ramUsed
		metric.RAMPercent = ramPct
	}

	// 3. Disk Usage
	if diskTotal, diskUsed, diskPct, err := m.readDisk("/"); err == nil {
		metric.DiskTotalGB = diskTotal
		metric.DiskUsedGB = diskUsed
		metric.DiskPercent = diskPct
	}

	// 4. Network I/O
	if rx, tx, err := m.readNetwork(); err == nil {
		metric.NetRxBytes = rx
		metric.NetTxBytes = tx
	}

	return metric
}

func (m *MetricsCollector) readCPU() (float64, error) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) >= 5 && fields[0] == "cpu" {
			var total, idle uint64
			for i := 1; i < len(fields); i++ {
				val, _ := strconv.ParseUint(fields[i], 10, 64)
				total += val
				if i == 4 { // 4th field is idle
					idle = val
				}
			}

			if m.lastCPUTotal > 0 && total > m.lastCPUTotal {
				diffTotal := float64(total - m.lastCPUTotal)
				diffIdle := float64(idle - m.lastCPUIdle)
				pct := ((diffTotal - diffIdle) / diffTotal) * 100.0
				if pct < 0 {
					pct = 0
				}
				if pct > 100 {
					pct = 100
				}

				m.lastCPUTotal = total
				m.lastCPUIdle = idle
				return mathRound(pct, 1), nil
			}

			m.lastCPUTotal = total
			m.lastCPUIdle = idle
		}
	}
	return 0, nil
}

func (m *MetricsCollector) readRAM() (int, int, float64, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, 0, err
	}
	defer file.Close()

	var memTotalKB, memFreeKB, memAvailableKB uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		valFields := strings.Fields(parts[1])
		if len(valFields) == 0 {
			continue
		}
		val, _ := strconv.ParseUint(valFields[0], 10, 64)

		switch key {
		case "MemTotal":
			memTotalKB = val
		case "MemFree":
			memFreeKB = val
		case "MemAvailable":
			memAvailableKB = val
		}
	}

	if memTotalKB > 0 {
		totalMB := int(memTotalKB / 1024)
		availKB := memAvailableKB
		if availKB == 0 {
			availKB = memFreeKB
		}
		usedMB := int((memTotalKB - availKB) / 1024)
		pct := (float64(memTotalKB-availKB) / float64(memTotalKB)) * 100.0
		return totalMB, usedMB, mathRound(pct, 1), nil
	}

	return 0, 0, 0, fmt.Errorf("no meminfo")
}

func (m *MetricsCollector) readDisk(path string) (int, int, float64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0, 0, err
	}

	totalBytes := stat.Blocks * uint64(stat.Bsize)
	freeBytes := stat.Bavail * uint64(stat.Bsize)
	usedBytes := totalBytes - freeBytes

	totalGB := int(totalBytes / (1024 * 1024 * 1024))
	usedGB := int(usedBytes / (1024 * 1024 * 1024))
	pct := 0.0
	if totalBytes > 0 {
		pct = (float64(usedBytes) / float64(totalBytes)) * 100.0
	}

	return totalGB, usedGB, mathRound(pct, 1), nil
}

func (m *MetricsCollector) readNetwork() (int64, int64, error) {
	file, err := os.Open("/proc/net/dev")
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	var totalRx, totalTx int64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			continue
		}
		iface := strings.TrimSpace(parts[0])
		if iface == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) >= 9 {
			rx, _ := strconv.ParseInt(fields[0], 10, 64)
			tx, _ := strconv.ParseInt(fields[8], 10, 64)
			totalRx += rx
			totalTx += tx
		}
	}
	return totalRx, totalTx, nil
}

func mathRound(val float64, precision int) float64 {
	p := 1.0
	for i := 0; i < precision; i++ {
		p *= 10.0
	}
	return float64(int(val*p+0.5)) / p
}

// ──────────────────────────────────────────────────────────────────────────
// WebSocket Metrics Hub
// ──────────────────────────────────────────────────────────────────────────
type MetricsHub struct {
	collector *MetricsCollector
	clients   map[*websocket.Conn]bool
	mu        sync.Mutex
}

func NewMetricsHub(collector *MetricsCollector) *MetricsHub {
	hub := &MetricsHub{
		collector: collector,
		clients:   make(map[*websocket.Conn]bool),
	}
	go hub.run()
	return hub
}

func (h *MetricsHub) run() {
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		metric := h.collector.Collect()
		payload, err := json.Marshal([]MachineMetric{metric})
		if err != nil {
			continue
		}

		h.mu.Lock()
		for client := range h.clients {
			err := client.WriteMessage(websocket.TextMessage, payload)
			if err != nil {
				client.Close()
				delete(h.clients, client)
			}
		}
		h.mu.Unlock()
	}
}

func (h *MetricsHub) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[MetricsHub] WS Upgrade error: %v", err)
		return
	}

	h.mu.Lock()
	h.clients[conn] = true
	h.mu.Unlock()

	// Keep alive & disconnect detector
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			h.mu.Lock()
			delete(h.clients, conn)
			h.mu.Unlock()
			conn.Close()
			break
		}
	}
}
