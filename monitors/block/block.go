package block

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/cryptoriums/layer-monitor/db"
	"github.com/cryptoriums/layer-monitor/monitors/block/processor"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"cosmossdk.io/log"
)

// BlockFetcher provides block data from an RPC endpoint.
// This interface enables testing with mocks instead of real RPC connections.
type BlockFetcher interface {
	// LatestHeight returns the latest block height on the chain.
	LatestHeight(ctx context.Context) (int64, error)
	// EarliestHeight returns the lowest block height still retained by the
	// nodes (the floor below which blocks have been pruned and are no longer
	// fetchable). Used to keep the processor from starting below the pruned
	// floor and wedging on unavailable blocks.
	EarliestHeight(ctx context.Context) (int64, error)
	// FetchBlock returns block data for the specified height.
	FetchBlock(ctx context.Context, height int64) (ctypes.EventDataNewBlock, error)
}

const ComponentName = "monitor"

// rpcTimeout caps how long a single RPC call to one node may take.
// This prevents the monitor from stalling when a fallback node has dropped packets
// (firewall, etc.) that cause TCP connections to hang for minutes.
const rpcTimeout = 5 * time.Second

// Config controls the RPC-based monitor behavior.
type Config struct {
	Nodes                     []string      `yaml:"nodes"`
	LayerAPIURLs              []string      `yaml:"layer_api_urls"`    // Layer API URLs for fetching reporters
	BackfillLookback          int           `yaml:"backfill_lookback"` // Days to look back for backfilling (0 = disabled)
	PollInterval              time.Duration `yaml:"poll_interval"`
	FetchWorkers              int           `yaml:"fetch_workers"`               // Number of parallel block fetchers (default 10)
	WalletAddress             string        `yaml:"wallet_address"`              // Our wallet address (tellor1xxx) for "our" metric labels
	ValidatorConsensusAddress string        `yaml:"validator_consensus_address"` // Our validator consensus address (tellorvalcons) for "our" metric labels
	Registerer                prometheus.Registerer
}

// Monitor polls ABCI endpoints to ingest blocks without using websockets.
type Monitor struct {
	cfg       Config
	logger    log.Logger
	fetcher   BlockFetcher
	db        db.Db
	processor processor.BlockProcessor
}

// rpcFetcher implements BlockFetcher using multiple RPC clients with failover.
type rpcFetcher struct {
	clients []*rpchttp.HTTP
}

// New creates a new RPC monitor.
func New(ctx context.Context, logger log.Logger, cfg Config, db db.Db) (*Monitor, error) {
	// ValidatorConsensusAddress must never be empty — refuse to start without it
	// so validator metrics/alerts can't silently no-op.
	if cfg.ValidatorConsensusAddress == "" {
		return nil, errors.New("ValidatorConsensusAddress must not be empty")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 800 * time.Millisecond
	}

	clients := make([]*rpchttp.HTTP, 0, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		client, err := rpchttp.New(node, "/websocket")
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	if len(clients) == 0 {
		return nil, errors.New("no RPC nodes configured")
	}

	fetcher := &rpcFetcher{clients: clients}
	return NewWithFetcher(ctx, logger, cfg, db, fetcher)
}

// NewWithFetcher creates a monitor with a custom BlockFetcher.
// This is primarily used for testing with mock fetchers.
func NewWithFetcher(ctx context.Context, logger log.Logger, cfg Config, db db.Db, fetcher BlockFetcher) (*Monitor, error) {
	// Apply defaults
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 800 * time.Millisecond
	}
	if cfg.FetchWorkers <= 0 {
		cfg.FetchWorkers = 10
	}

	procCfg := processor.ProcessorConfig{
		WalletAddress:             cfg.WalletAddress,
		ValidatorConsensusAddress: cfg.ValidatorConsensusAddress,
		LayerAPIURLs:              cfg.LayerAPIURLs,
		Registerer:                cfg.Registerer,
	}

	return &Monitor{
		cfg:       cfg,
		logger:    logger.With("component", ComponentName+"-rpc"),
		fetcher:   fetcher,
		db:        db,
		processor: processor.NewWithConfig(ctx, logger, db, procCfg),
	}, nil
}

// Run starts the polling loop until ctx is canceled.
func (m *Monitor) Run(ctx context.Context) error {
	nextHeight, err := m.startHeight(ctx)
	if err != nil {
		return err
	}

	m.logger.Debug("starting", "height", nextHeight)

	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()

	for {
		lastProcessed, err := m.catchUp(ctx, nextHeight)
		if err != nil {
			m.logger.Error("catch up failed", "error", err)
		}
		if lastProcessed >= nextHeight {
			nextHeight = lastProcessed + 1
		}
		// No progress and an error: the nodes may have pruned past our cursor
		// (every block from nextHeight up is gone). Re-check the earliest available
		// height and skip the unavailable gap so we don't retry pruned blocks forever.
		if lastProcessed < nextHeight && err != nil {
			if earliest, eErr := m.fetcher.EarliestHeight(ctx); eErr == nil && earliest > nextHeight {
				m.logger.Warn("skipping pruned gap; advancing to earliest available height",
					"from", nextHeight, "to", earliest)
				nextHeight = earliest
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *Monitor) catchUp(ctx context.Context, nextHeight int64) (lastProcessed int64, err error) {
	latest, err := m.fetcher.LatestHeight(ctx)
	if err != nil {
		return nextHeight - 1, err
	}

	m.logger.Debug("chain height", "num", latest)
	if latest < nextHeight {
		return nextHeight - 1, nil
	}

	// Process blocks in batches with parallel fetching
	batchSize := m.cfg.FetchWorkers
	for batchStart := nextHeight; batchStart <= latest; batchStart += int64(batchSize) {
		batchEnd := batchStart + int64(batchSize) - 1
		if batchEnd > latest {
			batchEnd = latest
		}

		// Fetch blocks in parallel
		blocks, fetchErr := m.fetchBlocksParallel(ctx, batchStart, batchEnd)
		if fetchErr != nil {
			// Return last successfully processed height
			return batchStart - 1, fetchErr
		}

		// Process blocks in order
		for h := batchStart; h <= batchEnd; h++ {
			event, ok := blocks[h]
			if !ok {
				return h - 1, fmt.Errorf("missing block %d from parallel fetch", h)
			}
			m.processor.ProcessBlock(ctx, event)
			lastProcessed = h
		}

		// Flush buffered records after each batch for consistent writes
		if err := m.processor.Flush(ctx); err != nil {
			m.logger.Error("flush failed", "error", err)
		}

		// Log progress for long backfills
		if batchEnd-nextHeight > 1000 && (batchEnd-nextHeight)%10000 == 0 {
			m.logger.Info("backfill progress",
				"processed", batchEnd-nextHeight+1,
				"total", latest-nextHeight+1,
				"current", batchEnd,
				"target", latest,
			)
		}
	}
	return lastProcessed, nil
}

// fetchBlocksParallel fetches a range of blocks concurrently.
// Returns a map of height -> block event.
func (m *Monitor) fetchBlocksParallel(ctx context.Context, start, end int64) (map[int64]ctypes.EventDataNewBlock, error) {
	results := make(map[int64]ctypes.EventDataNewBlock)
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	for h := start; h <= end; h++ {
		height := h
		g.Go(func() error {
			event, err := m.fetcher.FetchBlock(ctx, height)
			if err != nil {
				return fmt.Errorf("fetch block %d: %w", height, err)
			}

			mu.Lock()
			results[height] = event
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return results, nil
}

func (m *Monitor) startHeight(ctx context.Context) (int64, error) {
	last, err := m.lastStoredHeight(ctx)
	if err != nil {
		return 0, err
	}

	latest, err := m.fetcher.LatestHeight(ctx)
	if err != nil {
		return 0, err
	}

	start := m.computeStart(last, latest)

	// Never start below the pruned floor. If the nodes have pruned everything
	// below `earliest`, starting at the requested height would wedge the
	// processor retrying blocks that no node can serve — so clamp up to the
	// earliest available height and ingest from the oldest block still retained.
	// Best-effort: if earliest can't be determined, don't clamp.
	if earliest, eErr := m.fetcher.EarliestHeight(ctx); eErr != nil {
		m.logger.Debug("could not determine earliest available height; not clamping start", "error", eErr)
	} else if earliest > start {
		m.logger.Info("clamping start to earliest available height (nodes are pruned)",
			"requested_start", start,
			"earliest_available", earliest,
		)
		start = earliest
	}

	return start, nil
}

// computeStart returns the desired start height from the configured backfill
// lookback and the last stored height, before any pruned-floor clamping.
func (m *Monitor) computeStart(last, latest int64) int64 {
	// If backfill disabled (0 days), skip to current chain height.
	if m.cfg.BackfillLookback <= 0 {
		if latest > last {
			last = latest
		}
		return last + 1
	}

	// Calculate the lookback height (approximate: ~1.5 seconds per block).
	const blocksPerDay = 57600 // 24 * 60 * 60 / 1.5
	lookbackBlocks := int64(m.cfg.BackfillLookback * blocksPerDay)
	lookbackHeight := latest - lookbackBlocks
	if lookbackHeight < 1 {
		lookbackHeight = 1
	}

	if last > 0 {
		// If last stored block is within the lookback period, continue from there.
		if last >= lookbackHeight {
			m.logger.Info("resuming from last stored block",
				"last_stored", last,
				"lookback_height", lookbackHeight,
				"lookback_days", m.cfg.BackfillLookback,
			)
			return last + 1
		}
		// Last stored block is older than the lookback period.
		m.logger.Info("last stored block too old, starting from lookback height",
			"last_stored", last,
			"lookback_height", lookbackHeight,
			"lookback_days", m.cfg.BackfillLookback,
		)
		return lookbackHeight
	}

	m.logger.Info("no stored data, starting from lookback height",
		"lookback_height", lookbackHeight,
		"lookback_days", m.cfg.BackfillLookback,
	)
	return lookbackHeight
}

func (m *Monitor) lastStoredHeight(ctx context.Context) (int64, error) {
	// Query MAX height from multiple tables to handle blocks without transactions.
	// block_signs is always populated (every block has validator signatures),
	// so this ensures we don't miss blocks that have no transactions with fees.
	query := fmt.Sprintf(`
		SELECT greatest(
			(SELECT COALESCE(MAX(%s), 0) FROM %s),
			(SELECT COALESCE(MAX(%s), 0) FROM %s)
		)`,
		db.ColBlockHeight, db.TableNameTxs,
		db.ColBlockHeight, db.TableNameBlockSigns)
	rows, err := m.db.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			m.logger.Error("failed to close rows", "error", closeErr)
		}
	}()

	var height sql.NullInt64
	if rows.Next() {
		if err := rows.Scan(&height); err != nil {
			return 0, err
		}
	}
	if !height.Valid {
		return 0, nil
	}
	return height.Int64, nil
}

// LatestHeight implements BlockFetcher for rpcFetcher.
// Queries all RPC nodes in parallel and returns as soon as the first succeeds.
// This avoids blocking on slow or unreachable fallback nodes.
func (f *rpcFetcher) LatestHeight(ctx context.Context) (int64, error) {
	type res struct {
		height int64
		err    error
	}
	resCh := make(chan res, len(f.clients))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, client := range f.clients {
		cli := client
		go func() {
			rCtx, rCancel := context.WithTimeout(ctx, rpcTimeout)
			defer rCancel()
			resp, err := cli.Status(rCtx)
			if err != nil {
				select {
				case resCh <- res{err: err}:
				case <-ctx.Done():
				}
				return
			}
			select {
			case resCh <- res{height: resp.SyncInfo.LatestBlockHeight}:
			case <-ctx.Done():
			}
		}()
	}

	// Return as soon as any client succeeds; cancel remaining in-flight requests.
	var errs []error
	for i := 0; i < len(f.clients); i++ {
		select {
		case r := <-resCh:
			if r.err == nil {
				cancel() // signal other goroutines to stop
				return r.height, nil
			}
			errs = append(errs, r.err)
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	if len(errs) > 0 {
		return 0, errs[0]
	}
	return 0, errors.New("unable to query any node")
}

// EarliestHeight implements BlockFetcher. It returns the minimum
// earliest-available height across all responding nodes — the lowest height
// that is still fetchable from at least one node. A height below this has been
// pruned everywhere. Unlike LatestHeight, it waits for all nodes so it can take
// the most permissive (smallest) floor.
func (f *rpcFetcher) EarliestHeight(ctx context.Context) (int64, error) {
	type res struct {
		height int64
		err    error
	}
	resCh := make(chan res, len(f.clients))

	qctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, client := range f.clients {
		cli := client
		go func() {
			rCtx, rCancel := context.WithTimeout(qctx, rpcTimeout)
			defer rCancel()
			resp, err := cli.Status(rCtx)
			if err != nil {
				select {
				case resCh <- res{err: err}:
				case <-qctx.Done():
				}
				return
			}
			select {
			case resCh <- res{height: resp.SyncInfo.EarliestBlockHeight}:
			case <-qctx.Done():
			}
		}()
	}

	var (
		minEarliest int64 = -1
		errs        []error
	)
	for i := 0; i < len(f.clients); i++ {
		select {
		case r := <-resCh:
			if r.err != nil {
				errs = append(errs, r.err)
				continue
			}
			if minEarliest < 0 || r.height < minEarliest {
				minEarliest = r.height
			}
		case <-qctx.Done():
			return 0, qctx.Err()
		}
	}

	if minEarliest >= 0 {
		return minEarliest, nil
	}
	if len(errs) > 0 {
		return 0, errs[0]
	}
	return 0, errors.New("unable to query any node")
}

// FetchBlock implements BlockFetcher for rpcFetcher.
// It queries all nodes in parallel and returns the first successful response.
// Canceling context of slower nodes as soon as one succeeds avoids blocking on
// unresponsive or high-latency fallback nodes during backfill.
func (f *rpcFetcher) FetchBlock(ctx context.Context, height int64) (ctypes.EventDataNewBlock, error) {
	type res struct {
		event ctypes.EventDataNewBlock
		err   error
	}
	resCh := make(chan res, len(f.clients))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, client := range f.clients {
		cli := client
		h := height
		go func() {
			rCtx, rCancel := context.WithTimeout(ctx, rpcTimeout)
			defer rCancel()
			blockRes, err := cli.Block(rCtx, &h)
			if err != nil {
				select {
				case resCh <- res{err: err}:
				case <-ctx.Done():
				}
				return
			}
			resultsRes, err := cli.BlockResults(rCtx, &h)
			if err != nil {
				select {
				case resCh <- res{err: err}:
				case <-ctx.Done():
				}
				return
			}
			event := ctypes.EventDataNewBlock{
				Block:               blockRes.Block,
				BlockID:             blockRes.BlockID,
				ResultFinalizeBlock: finalizeToResponse(resultsRes),
			}
			select {
			case resCh <- res{event: event}:
			case <-ctx.Done():
			}
		}()
	}

	// Return as soon as any client succeeds; cancel remaining in-flight requests.
	// Only fall through to report an error if every client fails.
	var errs []error
	for i := 0; i < len(f.clients); i++ {
		select {
		case r := <-resCh:
			if r.err == nil && r.event.Block != nil {
				cancel() // signal other goroutines to stop
				return r.event, nil
			}
			if r.err != nil {
				errs = append(errs, r.err)
			}
		case <-ctx.Done():
			return ctypes.EventDataNewBlock{}, ctx.Err()
		}
	}

	if len(errs) > 0 {
		return ctypes.EventDataNewBlock{}, errs[0]
	}
	return ctypes.EventDataNewBlock{}, errors.New("no block data available")
}

func finalizeToResponse(results *coretypes.ResultBlockResults) abci.ResponseFinalizeBlock {
	if results == nil {
		return abci.ResponseFinalizeBlock{}
	}

	resp := abci.ResponseFinalizeBlock{
		Events:                results.FinalizeBlockEvents,
		TxResults:             results.TxsResults,
		ValidatorUpdates:      results.ValidatorUpdates,
		ConsensusParamUpdates: results.ConsensusParamUpdates,
		AppHash:               results.AppHash,
	}

	return resp
}
