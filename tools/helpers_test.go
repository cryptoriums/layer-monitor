package tools

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

var envFile = flag.String("env-file", "", "Path to .env file to load before running tests")

func parseAPIURLs() []string {
	urlsStr := os.Getenv("API_URLS")
	if urlsStr == "" {
		return nil
	}
	urls := strings.Split(urlsStr, ",")
	result := make([]string, 0, len(urls))
	for _, u := range urls {
		if trimmed := strings.TrimSpace(u); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func parseRPCURLs() []string {
	urlsStr := os.Getenv("LAYER_RPC_URLS")
	if urlsStr == "" {
		return nil
	}
	urls := strings.Split(urlsStr, ",")
	result := make([]string, 0, len(urls))
	for _, u := range urls {
		if trimmed := strings.TrimSpace(u); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func queryAPI(ctx context.Context, client *http.Client, apiURLs []string, path string) ([]byte, error) {
	var lastErr error

	for _, baseURL := range apiURLs {
		url := strings.TrimSuffix(baseURL, "/") + path

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
			continue
		}

		return body, nil
	}

	return nil, fmt.Errorf("all API URLs failed, last error: %w", lastErr)
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
