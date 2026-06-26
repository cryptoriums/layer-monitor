package tools

import (
	"flag"
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

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
