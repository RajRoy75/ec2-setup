package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// GetInstanceID retrieves the EC2 instance ID via IMDSv2, or falls back to hostname.
func GetInstanceID() string {
	// If explicitly set via environment variable, use it
	if envID := os.Getenv("INSTANCE_ID"); envID != "" {
		return envID
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := &http.Client{}

	// Step 1: Request IMDSv2 session token
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://169.254.169.254/latest/api/token", nil)
	if err != nil {
		return fallbackHostname()
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")

	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return fallbackHostname()
	}
	defer resp.Body.Close()

	tokenBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fallbackHostname()
	}
	token := strings.TrimSpace(string(tokenBytes))

	// Step 2: Retrieve instance ID using token
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, "http://169.254.169.254/latest/meta-data/instance-id", nil)
	if err != nil {
		return fallbackHostname()
	}
	req.Header.Set("X-aws-ec2-metadata-token", token)

	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return fallbackHostname()
	}
	defer resp.Body.Close()

	idBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fallbackHostname()
	}

	id := strings.TrimSpace(string(idBytes))
	if id == "" {
		return fallbackHostname()
	}
	return id
}

func fallbackHostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "unknown-instance"
	}
	return h
}
