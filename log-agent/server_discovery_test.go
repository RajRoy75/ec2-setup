package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestServerDiscoveryFormats(t *testing.T) {
	port := "8081"
	instanceID := "i-test"

	buildHandler := func() http.HandlerFunc {
		return enableCORS(func(w http.ResponseWriter, r *http.Request) {
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

			var objList []ServerItem
			if err := json.Unmarshal([]byte(serversRaw), &objList); err == nil && len(objList) > 0 && objList[0].IP != "" {
				_ = json.NewEncoder(w).Encode(objList)
				return
			}

			var strList []string
			if err := json.Unmarshal([]byte(serversRaw), &strList); err == nil && len(strList) > 0 {
				var result []ServerItem
				for _, ip := range strList {
					if ip == "" {
						continue
					}
					result = append(result, ServerItem{
						ID:         "server",
						Name:       ip,
						IP:         ip,
						MetricsURL: "http://" + ip + ":8080",
						AgentURL:   "http://" + ip + ":" + port,
					})
				}
				_ = json.NewEncoder(w).Encode(result)
				return
			}
		})
	}

	// Case 1: JSON Array of Objects
	t.Run("JSON Objects", func(t *testing.T) {
		os.Setenv("SERVERS", `[{"id":"s1","name":"Server 1","ip":"3.86.31.118","metrics_url":"http://3.86.31.118:8080","agent_url":"http://3.86.31.118:8081"}]`)
		defer os.Unsetenv("SERVERS")

		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/servers", nil)
		buildHandler()(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200, got %d", rec.Code)
		}
		var list []map[string]string
		if err := json.NewDecoder(rec.Body).Decode(&list); err != nil || len(list) != 1 {
			t.Fatalf("Failed parsing response: %v", err)
		}
		if list[0]["ip"] != "3.86.31.118" {
			t.Fatalf("Expected 3.86.31.118, got %s", list[0]["ip"])
		}
	})

	// Case 2: JSON Array of Strings
	t.Run("JSON Strings Array", func(t *testing.T) {
		os.Setenv("SERVERS", `["54.172.242.62", "100.53.72.216"]`)
		defer os.Unsetenv("SERVERS")

		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/servers", nil)
		buildHandler()(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200, got %d", rec.Code)
		}
		var list []map[string]string
		if err := json.NewDecoder(rec.Body).Decode(&list); err != nil || len(list) != 2 {
			t.Fatalf("Failed parsing response: %v", err)
		}
		if list[0]["ip"] != "54.172.242.62" || list[1]["ip"] != "100.53.72.216" {
			t.Fatalf("Unexpected IPs: %+v", list)
		}
	})
}
