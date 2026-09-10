// Package health exports the authoritative chain-health signals that used to come
// from an out-of-tree shell script writing node_exporter textfiles. Everything here
// is queried from the chain over HTTP, so the monitor is the single source of the
// metrics that alerts fire on - a textfile that stops being written looks exactly
// like a healthy one, which is how two alerts silently stopped protecting anything.
package health

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"cosmossdk.io/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cryptoriums/layer-monitor/addr"
	"github.com/cryptoriums/layer-monitor/encoding"
	monitor "github.com/cryptoriums/layer-monitor/metrics"

	"github.com/cosmos/cosmos-sdk/codec"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	reportertypes "github.com/tellor-io/layer/x/reporter/types"
)

const (
	ComponentName        = "health_monitor"
	DefaultCheckInterval = 1 * time.Minute

	// sourceNone is reported when every configured RPC endpoint failed, so a total
	// source outage is visible on the dashboard rather than being a flat line.
	sourceNone = -1
)

// Config holds the configuration for the health monitor.
type Config struct {
	// LayerRPCURLs are CometBFT RPC endpoints tried in order for node status.
	// Whether the chain is progressing is public data, so a public endpoint first
	// keeps this reporting while our own node is down or restarting.
	LayerRPCURLs []string
	// LayerAPIURLs are the REST endpoints used for staking and reporter queries.
	LayerAPIURLs  []string
	WalletAddress string
	CheckInterval time.Duration
}

// Monitor periodically publishes authoritative chain-health metrics.
type Monitor struct {
	logger     log.Logger
	cfg        Config
	httpClient *http.Client
	cdc        *codec.ProtoCodec

	lastSuccess     prometheus.Gauge
	height          prometheus.Gauge
	nodeSynced      prometheus.Gauge
	rpcSourceIndex  prometheus.Gauge
	validatorBonded prometheus.Gauge
	validatorTokens prometheus.Gauge
	reporterPower   prometheus.Gauge
}

// New creates a new health monitor.
func New(logger log.Logger, cfg Config, reg prometheus.Registerer) (*Monitor, error) {
	if len(cfg.LayerRPCURLs) == 0 {
		return nil, fmt.Errorf("LayerRPCURLs is required")
	}
	if len(cfg.LayerAPIURLs) == 0 {
		return nil, fmt.Errorf("LayerAPIURLs is required")
	}
	if cfg.WalletAddress == "" {
		return nil, fmt.Errorf("WalletAddress is required")
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = DefaultCheckInterval
	}

	gauge := func(name, help string) prometheus.Gauge {
		return promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: monitor.MetricsNamespace,
			Subsystem: "health",
			Name:      name,
			Help:      help,
		})
	}

	return &Monitor{
		logger:     logger.With("component", ComponentName),
		cfg:        cfg,
		cdc:        encoding.MakeCodec(),
		httpClient: &http.Client{Timeout: monitor.DefaultRequestTimeout},

		lastSuccess:     gauge("last_success_timestamp", "Unix time of the last successful authoritative chain health query"),
		height:          gauge("height", "Block height reported by the answering RPC endpoint"),
		nodeSynced:      gauge("node_synced", "1 if the answering node is synchronized, 0 if it is catching up"),
		rpcSourceIndex:  gauge("rpc_source_index", "Index of the RPC endpoint that served this sample (-1 = all failed)"),
		validatorBonded: gauge("our_validator_bonded", "1 if our validator is bonded, 0 otherwise"),
		validatorTokens: gauge("our_validator_tokens_loya", "Our validator's staked tokens in loya"),
		reporterPower:   gauge("our_reporter_power", "Our reporter's reporting power"),
	}, nil
}

// Run starts the health monitor loop.
func (m *Monitor) Run(ctx context.Context) error {
	m.logger.Info("starting health monitor", "check_interval", m.cfg.CheckInterval, "wallet", m.cfg.WalletAddress)

	m.check(ctx)

	ticker := time.NewTicker(m.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			m.check(ctx)
		}
	}
}

// check refreshes every metric. A failure in one query must not suppress the
// others, so each section reports independently and only the heartbeat is gated
// on the chain status actually being readable.
func (m *Monitor) check(ctx context.Context) {
	m.checkValidator(ctx)
	m.checkReporter(ctx)

	status, index, ok := m.fetchStatus(ctx)
	if !ok {
		m.rpcSourceIndex.Set(sourceNone)
		m.logger.Error("all RPC endpoints failed, chain health is unknown")
		monitor.IncError("rpc_unavailable", ComponentName)
		return
	}

	m.rpcSourceIndex.Set(float64(index))
	m.height.Set(float64(status.height))
	m.nodeSynced.Set(boolGauge(!status.catchingUp))

	// Written last: it is the freshness signal alerts use, so it must only advance
	// once the authoritative query behind it has actually succeeded.
	m.lastSuccess.Set(float64(time.Now().Unix()))
}

type nodeStatus struct {
	height     int64
	catchingUp bool
}

// statusResponse mirrors the subset of the CometBFT /status payload we rely on.
type statusResponse struct {
	Result struct {
		SyncInfo struct {
			LatestBlockHeight string `json:"latest_block_height"`
			CatchingUp        bool   `json:"catching_up"`
		} `json:"sync_info"`
	} `json:"result"`
}

// fetchStatus returns the first endpoint's status along with its index in the
// configured list, so a silent failover to a fallback endpoint stays visible.
func (m *Monitor) fetchStatus(ctx context.Context) (nodeStatus, int, bool) {
	for index, baseURL := range m.cfg.LayerRPCURLs {
		status, ok := m.doFetchStatus(ctx, baseURL+"/status")
		if ok {
			return status, index, true
		}
	}
	return nodeStatus{}, sourceNone, false
}

func (m *Monitor) doFetchStatus(ctx context.Context, url string) (nodeStatus, bool) {
	body, ok := m.get(ctx, url)
	if !ok {
		return nodeStatus{}, false
	}

	var parsed statusResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		m.logger.Debug("status decode error", "url", url, "error", err)
		return nodeStatus{}, false
	}

	height, err := strconv.ParseInt(parsed.Result.SyncInfo.LatestBlockHeight, 10, 64)
	if err != nil {
		m.logger.Debug("status height parse error", "url", url, "error", err)
		return nodeStatus{}, false
	}

	return nodeStatus{height: height, catchingUp: parsed.Result.SyncInfo.CatchingUp}, true
}

// checkValidator publishes our validator's bonded state and stake.
func (m *Monitor) checkValidator(ctx context.Context) {
	operator := addr.ToValidatorOperator(m.cfg.WalletAddress)
	for _, baseURL := range m.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/cosmos/staking/v1beta1/validators/%s", baseURL, operator)
		body, ok := m.get(ctx, url)
		if !ok {
			continue
		}

		var result stakingtypes.QueryValidatorResponse
		if err := m.cdc.UnmarshalJSON(body, &result); err != nil {
			m.logger.Debug("validator decode error", "url", url, "error", err)
			continue
		}

		m.validatorBonded.Set(boolGauge(result.Validator.Status == stakingtypes.Bonded))
		m.validatorTokens.Set(float64(result.Validator.Tokens.Int64()))
		return
	}

	m.logger.Error("could not query our validator from any endpoint", "operator", operator)
	monitor.IncError("validator_query_failed", ComponentName)
}

// checkReporter publishes our reporter's power.
func (m *Monitor) checkReporter(ctx context.Context) {
	for _, baseURL := range m.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/tellor-io/layer/reporter/reporters?pagination.limit=500", baseURL)
		body, ok := m.get(ctx, url)
		if !ok {
			continue
		}

		var result reportertypes.QueryReportersResponse
		if err := m.cdc.UnmarshalJSON(body, &result); err != nil {
			m.logger.Debug("reporters decode error", "url", url, "error", err)
			continue
		}

		for _, reporter := range result.Reporters {
			if reporter.Address != m.cfg.WalletAddress {
				continue
			}
			m.reporterPower.Set(float64(reporter.Power))
			return
		}

		// The endpoint answered and simply does not list us. Retrying other
		// endpoints would return the same chain state, so stop here.
		m.logger.Error("our reporter is not in the reporters list", "address", m.cfg.WalletAddress)
		monitor.IncError("reporter_missing", ComponentName)
		return
	}

	m.logger.Error("could not query reporters from any endpoint")
	monitor.IncError("reporter_query_failed", ComponentName)
}

func (m *Monitor) get(ctx context.Context, url string) ([]byte, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		m.logger.Debug("request error", "url", url, "error", err)
		return nil, false
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		m.logger.Debug("fetch error", "url", url, "error", err)
		return nil, false
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		m.logger.Debug("fetch status error", "url", url, "status", resp.StatusCode)
		return nil, false
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		m.logger.Debug("read error", "url", url, "error", err)
		return nil, false
	}

	return body, true
}

func boolGauge(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
