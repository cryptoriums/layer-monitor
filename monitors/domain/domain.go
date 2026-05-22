package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	monitor "github.com/cryptoriums/layer-monitor/metrics"

	"cosmossdk.io/log"
)

// Config holds domain checker configuration
type Config struct {
	Domain        string        // Domain to check (from DOMAIN env)
	CheckInterval time.Duration // How often to recheck (0 = startup only)
}

// Checker verifies that a domain resolves to the server's public IP
type Checker struct {
	logger   log.Logger
	config   Config
	ipMatch  prometheus.Gauge
	publicIP string
}

// New creates a new domain checker with Prometheus metrics
func New(logger log.Logger, config Config, reg prometheus.Registerer) (*Checker, error) {
	if config.Domain == "" {
		return nil, fmt.Errorf("domain is required")
	}
	if config.CheckInterval == 0 {
		config.CheckInterval = 5 * time.Minute
	}

	return &Checker{
		logger: logger.With("component", "domain-checker"),
		config: config,
		ipMatch: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: monitor.MetricsNamespace,
			Subsystem: "domain",
			Name:      "ip_match",
			Help:      "Indicates whether the domain resolves to this server's public IP (1=match, 0=mismatch)",
		}),
	}, nil
}

// Run starts the domain checker
func (c *Checker) Run(ctx context.Context) error {
	// Get public IP at startup
	c.refreshPublicIP(ctx)

	// Initial check
	c.checkDomain(ctx)

	// Periodic checks
	ticker := time.NewTicker(c.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.checkDomain(ctx)
		}
	}
}

// refreshPublicIP fetches and caches the server's public IP.
func (c *Checker) refreshPublicIP(ctx context.Context) {
	ip, err := c.getPublicIP(ctx)
	if err != nil {
		c.logger.Error("failed to get public IP", "error", err)
		c.ipMatch.Set(0)
		return
	}
	c.publicIP = ip
	c.logger.Info("detected public IP", "ip", c.publicIP)
}

// checkDomain verifies the domain resolves to the expected IP
func (c *Checker) checkDomain(ctx context.Context) {
	if c.publicIP == "" {
		c.refreshPublicIP(ctx)
		if c.publicIP == "" {
			return
		}
	}

	ips, err := net.LookupIP(c.config.Domain)
	if err != nil {
		c.logger.Error("failed to resolve domain",
			"domain", c.config.Domain,
			"error", err)
		c.ipMatch.Set(0)
		return
	}

	for _, ip := range ips {
		if ip.String() == c.publicIP {
			c.logger.Info("domain IP check passed",
				"domain", c.config.Domain,
				"ip", c.publicIP)
			c.ipMatch.Set(1)
			return
		}
	}

	// Mismatch
	resolvedIPs := make([]string, len(ips))
	for i, ip := range ips {
		resolvedIPs[i] = ip.String()
	}
	c.logger.Warn("domain IP mismatch",
		"domain", c.config.Domain,
		"expected", c.publicIP,
		"resolved", resolvedIPs)
	c.ipMatch.Set(0)
}

// getPublicIP fetches the server's public IP address
func (c *Checker) getPublicIP(ctx context.Context) (string, error) {
	client := &http.Client{Timeout: monitor.DefaultRequestTimeout}

	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.ipify.org?format=json", nil)
	if err != nil {
		return "", err
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var result struct {
		IP string `json:"ip"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	return result.IP, nil
}
