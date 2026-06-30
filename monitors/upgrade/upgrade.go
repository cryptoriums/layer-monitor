// Package upgrade monitors the chain for upcoming software upgrades and exposes them as
// Prometheus gauges so the standard Grafana alerting (Telegram) can fire on them. It does
// not send alerts itself — the 0/1 layerc_upgrade_pending metric drives the usual alert.
package upgrade

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cryptoriums/layer-monitor/encoding"
	monitor "github.com/cryptoriums/layer-monitor/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"cosmossdk.io/log"
	upgradetypes "cosmossdk.io/x/upgrade/types"

	"github.com/cosmos/cosmos-sdk/client/grpc/cmtservice"
	"github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	govv1 "github.com/cosmos/cosmos-sdk/x/gov/types/v1"
	"github.com/cosmos/gogoproto/proto"
)

const (
	ComponentName        = "upgrade_monitor"
	DefaultCheckInterval = 2 * time.Minute
)

// Config holds the configuration for the upgrade monitor.
type Config struct {
	LayerAPIURLs  []string      // API URLs for querying upgrade plan / proposals
	CheckInterval time.Duration // How often to check (defaults to DefaultCheckInterval)
}

// Monitor periodically checks for a scheduled chain upgrade and exposes it as metrics.
type Monitor struct {
	logger     log.Logger
	cfg        Config
	httpClient *http.Client
	cdc        *codec.ProtoCodec

	pending         prometheus.Gauge // 1 when an upgrade plan is scheduled
	proposed        prometheus.Gauge // 1 when a software-upgrade proposal is in voting
	planHeight      prometheus.Gauge // scheduled upgrade height (0 if none)
	blocksRemaining prometheus.Gauge // blocks until the scheduled height (0 if none)
}

// New creates a new upgrade monitor and registers its metrics.
func New(logger log.Logger, cfg Config, reg prometheus.Registerer) (*Monitor, error) {
	if len(cfg.LayerAPIURLs) == 0 {
		return nil, fmt.Errorf("LayerAPIURLs is required")
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = DefaultCheckInterval
	}

	gauge := func(name, help string) prometheus.Gauge {
		return promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: monitor.MetricsNamespace,
			Subsystem: "upgrade",
			Name:      name,
			Help:      help,
		})
	}

	return &Monitor{
		logger:          logger.With("component", ComponentName),
		cfg:             cfg,
		httpClient:      &http.Client{Timeout: monitor.DefaultRequestTimeout},
		cdc:             encoding.MakeCodec(),
		pending:         gauge("pending", "1 if a chain upgrade is scheduled (current_plan set), 0 otherwise"),
		proposed:        gauge("proposed", "1 if a software-upgrade governance proposal is in the voting period, 0 otherwise"),
		planHeight:      gauge("plan_height", "Target block height of the scheduled upgrade (0 if none)"),
		blocksRemaining: gauge("blocks_remaining", "Blocks remaining until the scheduled upgrade height (0 if none)"),
	}, nil
}

// Run starts the upgrade monitor loop.
func (m *Monitor) Run(ctx context.Context) error {
	m.logger.Info("starting upgrade monitor", "check_interval", m.cfg.CheckInterval)

	m.check(ctx) // initial check immediately

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

// check updates the upgrade metrics from the scheduled plan and voting proposals.
func (m *Monitor) check(ctx context.Context) {
	m.updatePlanMetrics(ctx, m.currentHeight(ctx))
	m.updateProposedMetric(ctx)
}

// updatePlanMetrics sets pending/plan_height/blocks_remaining from the scheduled plan.
func (m *Monitor) updatePlanMetrics(ctx context.Context, height int64) {
	plan, err := m.currentPlan(ctx)
	if err != nil {
		// Leave metrics unchanged on a transient query error rather than clearing a
		// real pending upgrade.
		m.logger.Warn("upgrade monitor: current_plan query failed", "error", err)
		return
	}
	if plan == nil {
		m.pending.Set(0)
		m.planHeight.Set(0)
		m.blocksRemaining.Set(0)
		return
	}

	remaining := plan.Height - height
	if remaining < 0 {
		remaining = 0
	}
	m.pending.Set(1)
	m.planHeight.Set(float64(plan.Height))
	m.blocksRemaining.Set(float64(remaining))
	m.logger.Error("CHAIN UPGRADE SCHEDULED",
		"name", plan.Name, "height", plan.Height, "current_height", height, "blocks_remaining", remaining)
}

// updateProposedMetric sets proposed=1 while a software-upgrade proposal is in voting.
func (m *Monitor) updateProposedMetric(ctx context.Context) {
	m.proposed.Set(0)
	if name, ok := m.upgradeProposalInVoting(ctx); ok {
		m.proposed.Set(1)
		m.logger.Warn("software-upgrade proposal in voting period", "name", name)
	}
}

// currentPlan returns the scheduled upgrade plan, or nil if none is set.
func (m *Monitor) currentPlan(ctx context.Context) (*upgradetypes.Plan, error) {
	var resp upgradetypes.QueryCurrentPlanResponse
	if err := m.getProto(ctx, "/cosmos/upgrade/v1beta1/current_plan", &resp); err != nil {
		return nil, err
	}
	return resp.Plan, nil
}

// currentHeight returns the latest block height, or 0 if it cannot be fetched.
func (m *Monitor) currentHeight(ctx context.Context) int64 {
	var resp cmtservice.GetLatestBlockResponse
	if err := m.getProto(ctx, "/cosmos/base/tendermint/v1beta1/blocks/latest", &resp); err != nil {
		m.logger.Debug("upgrade monitor: latest height query failed", "error", err)
		return 0
	}
	if resp.SdkBlock != nil {
		return resp.SdkBlock.Header.Height
	}
	return 0
}

// upgradeProposalInVoting reports whether a software-upgrade proposal is in the voting
// period, returning its plan name.
func (m *Monitor) upgradeProposalInVoting(ctx context.Context) (string, bool) {
	var resp govv1.QueryProposalsResponse
	if err := m.getProto(ctx, "/cosmos/gov/v1/proposals?pagination.limit=200", &resp); err != nil {
		m.logger.Debug("upgrade monitor: proposals query failed", "error", err)
		return "", false
	}
	swTypeURL := sdk.MsgTypeURL(&upgradetypes.MsgSoftwareUpgrade{})
	for _, p := range resp.Proposals {
		if p.Status != govv1.StatusVotingPeriod {
			continue
		}
		for _, anyMsg := range p.Messages {
			if anyMsg.TypeUrl != swTypeURL {
				continue
			}
			var sw upgradetypes.MsgSoftwareUpgrade
			if err := m.cdc.Unmarshal(anyMsg.Value, &sw); err != nil {
				continue
			}
			return sw.Plan.Name, true
		}
	}
	return "", false
}

// getProto performs a GET against each configured API URL until one succeeds, decoding the
// body into the given proto message with the codec's strict proto-JSON unmarshal (so a
// change to the chain's response shape surfaces as an error instead of being dropped).
func (m *Monitor) getProto(ctx context.Context, path string, msg proto.Message) error {
	var lastErr error
	for _, base := range m.cfg.LayerAPIURLs {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+path, nil)
		if err != nil {
			lastErr = err
			continue
		}
		res, err := m.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if res.StatusCode != http.StatusOK {
			_ = res.Body.Close()
			lastErr = fmt.Errorf("GET %s -> HTTP %d", path, res.StatusCode)
			continue
		}
		body, err := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if err := m.cdc.UnmarshalJSON(body, msg); err != nil {
			lastErr = fmt.Errorf("decode %s: %w", path, err)
			continue
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no API URLs configured")
	}
	return lastErr
}
