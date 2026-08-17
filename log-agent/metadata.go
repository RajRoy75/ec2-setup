package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func GetInstanceID() string {
	// If explicitly set as env var, use it
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID != "" {
		return instanceID
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	defer cancel()
	client := &http.Client{}
	// Request IMDSv2 session token
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://169.254.169.254/latest/api/token", nil)
	if err != nil {
		return fallbackHostName()
	}

	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")

	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return fallbackHostName()
	}

	defer resp.Body.Close()

	tokenBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fallbackHostName()
	}

	token := strings.TrimSpace(string(tokenBytes))

	// step 2: Retrieve Instace ID using token
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, "http://169.254.169.254/latest/meta-data/instance-id", nil)
	if err != nil {
		return fallbackHostName()
	}

	req.Header.Set("X-aws-ec2-metadata-token", token)

	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return fallbackHostName()
	}

	defer resp.Body.Close()

	idBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fallbackHostName()
	}

	id := strings.TrimSpace(string(idBytes))
	if id == "" {
		return fallbackHostName()
	}

	return id
}

func fallbackHostName() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "unknown instance"
	}
	return h
}
