package jail

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	monitor "github.com/cryptoriums/layer-monitor/metrics"
	"github.com/cryptoriums/layer-monitor/addr"
	"github.com/cryptoriums/layer-monitor/encoding"
	reportertypes "github.com/tellor-io/layer/x/reporter/types"

	"cosmossdk.io/log"

	"github.com/cosmos/cosmos-sdk/codec"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
)

const (
	ComponentName        = "jail_monitor"
	DefaultCheckInterval = 1 * time.Minute
)

var (
	ourReporterJailed     prometheus.Gauge
	ourValidatorJailed    prometheus.Gauge
	totalJailedReporters  prometheus.Gauge
	totalJailedValidators prometheus.Gauge
)

// Config holds the configuration for the jail monitor.
type Config struct {
	LayerAPIURLs  []string      // API URLs for querying jail status
	WalletAddress string        // Our wallet address (tellor1xxx)
	CheckInterval time.Duration // How often to check jail status
}

// Monitor periodically checks jail status for reporters and validators.
type Monitor struct {
	logger     log.Logger
	cfg        Config
	httpClient *http.Client
	cdc        *codec.ProtoCodec

	// Metrics for jail status
	ourReporterJailed     prometheus.Gauge
	ourValidatorJailed    prometheus.Gauge
	totalJailedReporters  prometheus.Gauge
	totalJailedValidators prometheus.Gauge
}

// New creates a new jail monitor.
func New(logger log.Logger, cfg Config, reg prometheus.Registerer) (*Monitor, error) {
	if len(cfg.LayerAPIURLs) == 0 {
		return nil, fmt.Errorf("LayerAPIURLs is required")
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = DefaultCheckInterval
	}

	ourReporterJailed = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "jail",
		Name:      "our_reporter_jailed",
		Help:      "1 if our reporter is jailed, 0 otherwise",
	})
	ourValidatorJailed = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "jail",
		Name:      "our_validator_jailed",
		Help:      "1 if our validator is jailed, 0 otherwise",
	})
	totalJailedReporters = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "jail",
		Name:      "total_jailed_reporters",
		Help:      "Total number of jailed reporters",
	})
	totalJailedValidators = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "jail",
		Name:      "total_jailed_validators",
		Help:      "Total number of jailed validators",
	})

	return &Monitor{
		logger: logger.With("component", ComponentName),
		cfg:    cfg,
		cdc:    encoding.MakeCodec(),
		httpClient: &http.Client{
			Timeout: monitor.DefaultRequestTimeout,
		},
		ourReporterJailed:     ourReporterJailed,
		ourValidatorJailed:    ourValidatorJailed,
		totalJailedReporters:  totalJailedReporters,
		totalJailedValidators: totalJailedValidators,
	}, nil
}

// Run starts the jail monitor loop.
func (m *Monitor) Run(ctx context.Context) error {
	m.logger.Info("starting jail monitor", "check_interval", m.cfg.CheckInterval, "wallet", m.cfg.WalletAddress)

	// Do initial check immediately
	m.checkJailStatus(ctx)

	ticker := time.NewTicker(m.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			m.checkJailStatus(ctx)
		}
	}
}

func (m *Monitor) checkJailStatus(ctx context.Context) {
	m.checkJailedReporters(ctx)
	m.checkJailedValidators(ctx)
}

// checkJailedReporters fetches jailed reporters from the API and updates metrics.
func (m *Monitor) checkJailedReporters(ctx context.Context) {
	allReporters := m.fetchAllReporters(ctx)
	jailedReporters := m.fetchJailedReporters(ctx)

	// Update total jailed reporters metric
	m.totalJailedReporters.Set(float64(len(jailedReporters)))

	// Check if our reporter is jailed
	ourJailed := false
	for _, reporter := range jailedReporters {
		if reporter.Address == m.cfg.WalletAddress {
			m.logger.Error("OUR REPORTER IS JAILED!", "address", reporter.Address)
			ourJailed = true
		}
	}

	if ourJailed {
		m.ourReporterJailed.Set(1)
	} else {
		m.ourReporterJailed.Set(0)
	}

	m.logger.Debug("checked reporter jail status", "total", len(allReporters), "jailed", len(jailedReporters))
}

// checkJailedValidators fetches validators and updates metrics for jailed ones.
func (m *Monitor) checkJailedValidators(ctx context.Context) {
	validators := m.fetchAllValidators(ctx)
	ourValidatorAddr := addr.ToValidatorOperator(m.cfg.WalletAddress)

	jailedCount := 0
	ourJailed := false
	for _, validator := range validators {
		if validator.Jailed {
			jailedCount++
			if validator.OperatorAddress == ourValidatorAddr {
				m.logger.Error("OUR VALIDATOR IS JAILED!", "address", validator.OperatorAddress, "moniker", validator.Description.Moniker)
				ourJailed = true
			}
		}
	}

	// Update metrics
	m.totalJailedValidators.Set(float64(jailedCount))
	if ourJailed {
		m.ourValidatorJailed.Set(1)
	} else {
		m.ourValidatorJailed.Set(0)
	}

	m.logger.Debug("checked validator jail status", "total", len(validators), "jailed", jailedCount)
}

func (m *Monitor) fetchAllReporters(ctx context.Context) []*reportertypes.Reporter {
	cdc := encoding.MakeCodec()
	for _, baseURL := range m.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/tellor-io/layer/reporter/reporters?pagination.limit=500", baseURL)
		reporters, ok := m.doFetchReporters(ctx, url, cdc)
		if ok {
			return reporters
		}
	}
	return nil
}

func (m *Monitor) fetchJailedReporters(ctx context.Context) []*reportertypes.Reporter {
	for _, baseURL := range m.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/tellor-io/layer/reporter/jailed-reporters?pagination.limit=500", baseURL)
		reporters, ok := m.doFetchReporters(ctx, url, m.cdc)
		if ok {
			return reporters
		}
	}
	return nil
}

func (m *Monitor) doFetchReporters(ctx context.Context, url string, cdc *codec.ProtoCodec) ([]*reportertypes.Reporter, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		m.logger.Debug("reporter request error", "url", url, "error", err)
		return nil, false
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		m.logger.Debug("reporter fetch error", "url", url, "error", err)
		return nil, false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		m.logger.Debug("reporter fetch status error", "url", url, "status", resp.StatusCode)
		return nil, false
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		m.logger.Debug("reporter read error", "url", url, "error", err)
		return nil, false
	}

	var result reportertypes.QueryReportersResponse
	if err := cdc.UnmarshalJSON(body, &result); err != nil {
		m.logger.Debug("reporter decode error", "url", url, "error", err)
		return nil, false
	}

	return result.Reporters, true
}

func (m *Monitor) fetchAllValidators(ctx context.Context) []stakingtypes.Validator {
	for _, baseURL := range m.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/cosmos/staking/v1beta1/validators?pagination.limit=100", baseURL)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			m.logger.Debug("validator request error", "url", url, "error", err)
			continue
		}

		resp, err := m.httpClient.Do(req)
		if err != nil {
			m.logger.Debug("validator fetch error", "url", url, "error", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			m.logger.Debug("validator fetch status error", "url", url, "status", resp.StatusCode)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			m.logger.Debug("validator read error", "url", url, "error", err)
			continue
		}

		var result stakingtypes.QueryValidatorsResponse
		if err := m.cdc.UnmarshalJSON(body, &result); err != nil {
			m.logger.Debug("validator decode error", "url", url, "error", err)
			continue
		}

		return result.Validators
	}

	return nil
}
