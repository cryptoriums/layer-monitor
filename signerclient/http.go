package signerclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

// defaultHTTPPort is the signer's health server port, which serves GET /address
// without mTLS.
const defaultHTTPPort = "9192"

// HTTPAddr returns host:port of the signer's HTTP server: REMOTE_SIGNER_HTTP_ADDR
// if set, otherwise the host of Addr with port 9192.
func (c Config) HTTPAddr() string {
	if v := os.Getenv("REMOTE_SIGNER_HTTP_ADDR"); v != "" {
		return v
	}
	host, _, err := net.SplitHostPort(c.Addr)
	if err != nil || host == "" {
		return ""
	}
	return net.JoinHostPort(host, defaultHTTPPort)
}

// FetchAddressHTTP gets the reporter's bech32 address from the signer's
// GET /address endpoint.
func FetchAddressHTTP(ctx context.Context, cfg Config, timeout time.Duration) (string, error) {
	addr := cfg.HTTPAddr()
	if addr == "" {
		return "", fmt.Errorf("no signer HTTP address (REMOTE_SIGNER_ADDR=%q)", cfg.Addr)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := "http://" + addr + "/address?prefix=" + addressPrefix
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: %s", url, resp.Status)
	}

	var body struct {
		Address string `json:"address"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&body); err != nil {
		return "", fmt.Errorf("decode %s: %w", url, err)
	}
	if body.Address == "" {
		return "", fmt.Errorf("GET %s: empty address", url)
	}
	return body.Address, nil
}
