package dispute

import (
	"os"
	"strconv"
	"strings"
	"time"
)

func LoadConfigFromEnv() Config {
	cfg := Config{CheckInterval: time.Second}

	if urls := os.Getenv("API_URLS"); urls != "" {
		for _, url := range strings.Split(urls, ",") {
			if trimmed := strings.TrimSpace(url); trimmed != "" {
				cfg.LayerAPIURLs = append(cfg.LayerAPIURLs, trimmed)
			}
		}
	}

	if ignoreIDs := os.Getenv("DISPUTE_IGNORE_IDS"); ignoreIDs != "" {
		for _, idStr := range strings.Split(ignoreIDs, ",") {
			if id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64); err == nil {
				cfg.IgnoreDisputes = append(cfg.IgnoreDisputes, id)
			}
		}
	}

	if interval := os.Getenv("DISPUTE_CHECK_INTERVAL"); interval != "" {
		if d, err := time.ParseDuration(interval); err == nil && d > 0 {
			cfg.CheckInterval = d
		}
	}

	return cfg
}
