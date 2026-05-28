package dispute

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	blockdb "github.com/cryptoriums/layer-monitor/db"
	"github.com/cryptoriums/layer-monitor/encoding"
	monitor "github.com/cryptoriums/layer-monitor/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tellor-io/layer/x/dispute/types"
	"golang.org/x/sync/errgroup"

	"cosmossdk.io/log"

	"github.com/cosmos/cosmos-sdk/codec"
)

const (
	Component           = "dispute_monitor"
	ReasonOpenDisputes  = "OPEN DISPUTES DETECTED - not safe to continue reporting. Add dispute IDs to DISPUTE_IGNORE_IDS if safe to ignore"
	ReasonTooManyErrors = "too many consecutive errors querying dispute endpoints"
	ErrorThreshold      = 20
)

type Config struct {
	LayerAPIURLs   []string      // Layer API URLs for querying disputes
	IgnoreDisputes []uint64      // Dispute IDs to ignore
	CheckInterval  time.Duration // How often to check for disputes
	Db             blockdb.Db    // DB for failsafe queries
}

type Monitor struct {
	cfg               Config
	openDisputesCount prometheus.Gauge
	logger            log.Logger
	httpClient        *http.Client
	cdc               *codec.ProtoCodec
	mtx               sync.RWMutex
}

func New(logger log.Logger, cfg Config, reg prometheus.Registerer) *Monitor {
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = time.Second // Default to 1 second for safety
	}

	openDisputesGauge := promauto.With(reg).NewGauge(
		prometheus.GaugeOpts{
			Namespace: monitor.MetricsNamespace,
			Subsystem: "dispute",
			Name:      "open_disputes_count",
			Help:      "Number of open disputes",
		},
	)

	return &Monitor{
		cfg:               cfg,
		openDisputesCount: openDisputesGauge,
		logger:            logger.With("component", Component),
		httpClient:        &http.Client{Timeout: 10 * time.Second},
		cdc:               encoding.MakeCodec(),
	}
}

func (m *Monitor) Run(ctx context.Context) {
	m.mtx.RLock()
	cfg := m.cfg
	m.mtx.RUnlock()

	m.logger.Info("starting dispute monitor",
		"api_urls", cfg.LayerAPIURLs,
		"check_interval", cfg.CheckInterval,
		"ignore_disputes", cfg.IgnoreDisputes,
		"db", cfg.Db,
	)

	// Do initial check immediately
	m.checkDisputes(ctx)

	ticker := time.NewTicker(cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("dispute monitor stopped")
			return
		case <-ticker.C:
			m.checkDisputes(ctx)
		}
	}
}

func (m *Monitor) checkDisputes(ctx context.Context) {
	// Take a snapshot of config to avoid race conditions
	m.mtx.RLock()
	cfg := m.cfg
	m.mtx.RUnlock()

	// Collect disputes from all sources
	allDisputes := make(map[uint64]struct{})

	// Query all API nodes in parallel
	apiDisputes := m.queryAllAPINodes(ctx, cfg.LayerAPIURLs)
	for _, id := range apiDisputes {
		allDisputes[id] = struct{}{}
	}

	// DB-based check is an optional secondary safety net used by the
	// monitor process. The reporter wires this monitor with cfg.Db == nil
	// and relies on the API query above.
	if cfg.Db != nil {
		dbDisputes, err := blockdb.GetOpenDisputes(ctx, cfg.Db)
		if err != nil {
			m.logger.Error("failed to query disputes from DB", "error", err)
			monitor.IncError("db_query_failed", Component)
		} else {
			for _, id := range dbDisputes {
				allDisputes[id] = struct{}{}
			}
		}
	}

	// Update metrics
	m.openDisputesCount.Set(float64(len(allDisputes)))

	// Check for non-ignored disputes
	for disputeID := range allDisputes {
		if !isIgnored(cfg.IgnoreDisputes, disputeID) {
			m.logger.Error("OPEN DISPUTE DETECTED - PANIC",
				"dispute_id", disputeID,
				"ignored_ids", cfg.IgnoreDisputes,
			)
			panic(fmt.Sprintf("%s: dispute_id=%d", ReasonOpenDisputes, disputeID))
		}
		m.logger.Warn("open dispute found but ignored",
			"dispute_id", disputeID,
		)
	}

	if len(allDisputes) > 0 {
		m.logger.Debug("dispute check complete", "open_disputes", len(allDisputes), "all_ignored", true)
	}
}

func (m *Monitor) queryAllAPINodes(ctx context.Context, apiURLs []string) []uint64 {
	if len(apiURLs) == 0 {
		return nil
	}

	type result struct {
		node string
		ids  []uint64
		err  error
	}
	resultsCh := make(chan result, len(apiURLs))

	g, gCtx := errgroup.WithContext(ctx)
	for _, apiURL := range apiURLs {
		url := apiURL
		g.Go(func() error {
			ids, err := m.queryDisputesFromAPI(gCtx, url)
			select {
			case resultsCh <- result{node: url, ids: ids, err: err}:
			case <-gCtx.Done():
			}
			return nil
		})
	}
	_ = g.Wait()
	close(resultsCh)

	// Collect all dispute IDs from all nodes
	allIDs := make(map[uint64]struct{})
	errorCount := 0
	for res := range resultsCh {
		if res.err != nil {
			m.logger.Debug("API query failed", "url", res.node, "error", res.err)
			errorCount++
			continue
		}
		for _, id := range res.ids {
			allIDs[id] = struct{}{}
		}
	}

	// If ALL nodes failed, this is a critical error
	if errorCount == len(apiURLs) && errorCount > 0 {
		m.logger.Error("all API nodes failed to respond")
		monitor.IncError("all_nodes_failed", Component)
	}

	// Convert to slice
	var ids []uint64
	for id := range allIDs {
		ids = append(ids, id)
	}
	return ids
}

func (m *Monitor) queryDisputesFromAPI(ctx context.Context, baseURL string) ([]uint64, error) {
	url := fmt.Sprintf("%s/tellor-io/layer/dispute/open-disputes", baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result types.QueryOpenDisputesResponse
	if err := m.cdc.UnmarshalJSON(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	if result.OpenDisputes == nil {
		return nil, nil
	}

	return result.OpenDisputes.Ids, nil
}

func isIgnored(ignoreList []uint64, disputeID uint64) bool {
	for _, ignoreID := range ignoreList {
		if ignoreID == disputeID {
			return true
		}
	}
	return false
}

// ParseDisputeID converts decimal strings to uint64.
func ParseDisputeID(val string) (uint64, error) {
	return strconv.ParseUint(val, 10, 64)
}
