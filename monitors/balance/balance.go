package balance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	monitor "github.com/cryptoriums/layer-monitor/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/shopspring/decimal"

	"cosmossdk.io/log"
)

const (
	ComponentName        = "balance_monitor"
	DefaultCheckInterval = 5 * time.Minute
)

// Config holds the configuration for the balance monitor.
type Config struct {
	LayerAPIURLs  []string      // API URLs for querying balances
	WalletAddress string        // Our wallet address (tellor1xxx)
	CheckInterval time.Duration // How often to check balances
}

// Monitor periodically checks balances and exposes Prometheus metrics.
type Monitor struct {
	logger     log.Logger
	cfg        Config
	httpClient *http.Client

	walletBalance    prometheus.Gauge
	stakingBalance   prometheus.Gauge
	tipsBalance      prometheus.Gauge
	unbondingBalance prometheus.Gauge
	totalBalance     prometheus.Gauge
	lastCheckTime    prometheus.Gauge
}

// New creates a new balance monitor.
func New(logger log.Logger, cfg Config, reg prometheus.Registerer) (*Monitor, error) {
	if len(cfg.LayerAPIURLs) == 0 {
		return nil, fmt.Errorf("LayerAPIURLs is required")
	}
	if cfg.WalletAddress == "" {
		return nil, fmt.Errorf("WalletAddress is required")
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = DefaultCheckInterval
	}

	walletBalance := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "balance",
		Name:      "wallet_balance_loya",
		Help:      "Wallet (bank) balance in loya",
	})
	stakingBalance := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "balance",
		Name:      "staking_balance_loya",
		Help:      "Total staked balance in loya",
	})
	tipsBalance := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "balance",
		Name:      "tips_balance_loya",
		Help:      "Available tips balance in loya",
	})
	unbondingBalance := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "balance",
		Name:      "unbonding_balance_loya",
		Help:      "Unbonding delegation balance in loya",
	})
	totalBalance := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "balance",
		Name:      "total_balance_loya",
		Help:      "Total balance (wallet + staking + tips + unbonding) in loya",
	})
	lastCheckTime := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "balance",
		Name:      "last_balance_check_timestamp",
		Help:      "Unix timestamp of last successful balance check",
	})

	return &Monitor{
		logger: logger.With("component", ComponentName),
		cfg:    cfg,
		httpClient: &http.Client{
			Timeout: monitor.DefaultRequestTimeout,
		},
		walletBalance:    walletBalance,
		stakingBalance:   stakingBalance,
		tipsBalance:      tipsBalance,
		unbondingBalance: unbondingBalance,
		totalBalance:     totalBalance,
		lastCheckTime:    lastCheckTime,
	}, nil
}

// Run starts the balance monitor loop.
func (m *Monitor) Run(ctx context.Context) error {
	m.logger.Info("starting balance monitor", "check_interval", m.cfg.CheckInterval, "wallet", m.cfg.WalletAddress)

	m.checkBalances(ctx)

	ticker := time.NewTicker(m.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			m.checkBalances(ctx)
		}
	}
}

func (m *Monitor) checkBalances(ctx context.Context) {
	wallet := m.fetchWalletBalance(ctx)
	staking := m.fetchStakingBalance(ctx)
	unbonding := m.fetchUnbondingBalance(ctx)
	tips := m.fetchTipsBalance(ctx)

	m.walletBalance.Set(wallet)
	m.stakingBalance.Set(staking)
	m.unbondingBalance.Set(unbonding)
	m.tipsBalance.Set(tips)
	m.totalBalance.Set(wallet + staking + unbonding + tips)
	m.lastCheckTime.Set(float64(time.Now().Unix()))

	m.logger.Info("balance check complete",
		"wallet_loya", wallet,
		"staking_loya", staking,
		"unbonding_loya", unbonding,
		"tips_loya", tips,
		"total_loya", wallet+staking+unbonding+tips,
	)
}

// fetchWalletBalance returns the bank balance in loya.
func (m *Monitor) fetchWalletBalance(ctx context.Context) float64 {
	for _, baseURL := range m.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/cosmos/bank/v1beta1/balances/%s", baseURL, m.cfg.WalletAddress)
		body, ok := m.doGet(ctx, url)
		if !ok {
			continue
		}

		var result struct {
			Balances []struct {
				Denom  string `json:"denom"`
				Amount string `json:"amount"`
			} `json:"balances"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			m.logger.Debug("failed to decode wallet balance", "url", url, "error", err)
			continue
		}

		var total float64
		for _, b := range result.Balances {
			if b.Denom == "loya" {
				if v, err := strconv.ParseFloat(b.Amount, 64); err == nil {
					total += v
				}
			}
		}
		return total
	}
	m.logger.Warn("failed to fetch wallet balance from all API URLs")
	return 0
}

// fetchStakingBalance returns the total delegated balance in loya.
func (m *Monitor) fetchStakingBalance(ctx context.Context) float64 {
	for _, baseURL := range m.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/cosmos/staking/v1beta1/delegations/%s", baseURL, m.cfg.WalletAddress)
		body, ok := m.doGet(ctx, url)
		if !ok {
			continue
		}

		var result struct {
			DelegationResponses []struct {
				Balance struct {
					Denom  string `json:"denom"`
					Amount string `json:"amount"`
				} `json:"balance"`
			} `json:"delegation_responses"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			m.logger.Debug("failed to decode staking balance", "url", url, "error", err)
			continue
		}

		var total float64
		for _, d := range result.DelegationResponses {
			if d.Balance.Denom == "loya" {
				if v, err := strconv.ParseFloat(d.Balance.Amount, 64); err == nil {
					total += v
				}
			}
		}
		return total
	}
	m.logger.Warn("failed to fetch staking balance from all API URLs")
	return 0
}

// fetchUnbondingBalance returns the total unbonding balance in loya.
func (m *Monitor) fetchUnbondingBalance(ctx context.Context) float64 {
	for _, baseURL := range m.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/cosmos/staking/v1beta1/delegators/%s/unbonding_delegations", baseURL, m.cfg.WalletAddress)
		body, ok := m.doGet(ctx, url)
		if !ok {
			continue
		}

		var result struct {
			UnbondingResponses []struct {
				Entries []struct {
					Balance string `json:"balance"`
				} `json:"entries"`
			} `json:"unbonding_responses"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			m.logger.Debug("failed to decode unbonding balance", "url", url, "error", err)
			continue
		}

		var total float64
		for _, ur := range result.UnbondingResponses {
			for _, entry := range ur.Entries {
				if v, err := strconv.ParseFloat(entry.Balance, 64); err == nil {
					total += v
				}
			}
		}
		return total
	}
	m.logger.Warn("failed to fetch unbonding balance from all API URLs")
	return 0
}

// fetchTipsBalance returns available tips in loya (converted from decimal TRB-based value).
func (m *Monitor) fetchTipsBalance(ctx context.Context) float64 {
	for _, baseURL := range m.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/tellor-io/layer/reporter/available-tips/%s", baseURL, m.cfg.WalletAddress)
		body, ok := m.doGet(ctx, url)
		if !ok {
			continue
		}

		var result struct {
			AvailableTips string `json:"available_tips"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			m.logger.Debug("failed to decode tips balance", "url", url, "error", err)
			continue
		}

		// available_tips is already in loya as a decimal string.
		d, err := decimal.NewFromString(result.AvailableTips)
		if err != nil {
			m.logger.Debug("failed to parse tips value", "value", result.AvailableTips, "error", err)
			continue
		}
		f, _ := d.Float64()
		return f
	}
	m.logger.Warn("failed to fetch tips balance from all API URLs")
	return 0
}

func (m *Monitor) doGet(ctx context.Context, url string) ([]byte, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		m.logger.Debug("request create error", "url", url, "error", err)
		return nil, false
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		m.logger.Debug("request error", "url", url, "error", err)
		return nil, false
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		m.logger.Debug("request status error", "url", url, "status", resp.StatusCode)
		return nil, false
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		m.logger.Debug("read error", "url", url, "error", err)
		return nil, false
	}

	return body, true
}
