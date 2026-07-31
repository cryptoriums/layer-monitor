package processor

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	ctypes "github.com/cometbft/cometbft/types"
	cryptoaddr "github.com/cryptoriums/layer-monitor/addr"
	blockdb "github.com/cryptoriums/layer-monitor/db"
	monitor "github.com/cryptoriums/layer-monitor/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/shopspring/decimal"
	"github.com/tellor-io/layer/app"
	"github.com/tellor-io/layer/x/oracle/types"

	"cosmossdk.io/log"

	sdk "github.com/cosmos/cosmos-sdk/types"
	authsigning "github.com/cosmos/cosmos-sdk/x/auth/signing"
)

const (
	ComponentName = "block_processor"

	// DefaultDBTimeout is the timeout for database operations.
	DefaultDBTimeout = 5 * time.Second

	unknownSender = "unknown"
)

// Event attribute keys from Layer blockchain events.
// These match the attribute names emitted by the Layer chain.
const (
	AttrKeyQueryID     = "query_id"
	AttrKeyReporter    = "reporter"
	AttrKeyPower       = "power"
	AttrKeyQueryType   = "query_type"
	AttrKeyAggMethod   = "aggregate_method"
	AttrKeyValue       = "value"
	AttrKeyTimestamp   = "timestamp"
	AttrKeyCyclelist   = "cyclelist"
	AttrKeyBlockNumber = "block_number"
	AttrKeyMetaID      = "meta_id"
	AttrKeyValidator   = "validator"
	AttrKeyAmount      = "amount"
	AttrKeyDelegator   = "delegator"
	AttrKeySender      = "sender"
	AttrKeyNetReward   = "net_reward"
	AttrKeyCommission  = "commission"
)

type BlockProcessor interface {
	ProcessBlock(context.Context, ctypes.EventDataNewBlock)
	// Flush writes any buffered records to the database.
	// Should be called after processing a batch of blocks during backfill.
	Flush(context.Context) error
}

// batchInsertThreshold is the number of records to buffer before auto-flushing.
const batchInsertThreshold = 1000

// bufferedBlockSign represents a buffered block signature for batch insert.
type bufferedBlockSign struct {
	blockHeight int64
	blockTime   time.Time
	validator   string
	signed      uint8
	// sigType is which kind of signature this row records: consensus precommit,
	// valset checkpoint or oracle attestation. Consensus rows carry a consensus
	// address; the vote-extension rows carry an operator address.
	sigType string
}

// bufferedReport represents a buffered oracle report for batch insert into the reports table.
type bufferedReport struct {
	reporter        string
	power           uint64
	queryType       string
	queryIDHex      string
	aggregateMethod string
	value           string
	timestamp       time.Time
	blockTime       time.Time
	cyclelist       uint8
	blockNumber     uint64
	metaID          uint64
}

// bufferedTx represents a buffered transaction for batch insert into the txs table.
type bufferedTx struct {
	blockHeight int64
	blockTime   time.Time
	txHash      string
	sender      string
	gasUsed     int64
	fee         string
}

// bufferedCycleRotation represents a buffered cycle rotation for batch insert.
type bufferedCycleRotation struct {
	blockHeight int64
	blockTime   time.Time
	queryID     string
}

// bufferedReward represents a buffered reward for batch insert into the unified rewards table.
// Supports all reward types: reporter_tip, validator_commission, validator_delegator.
type bufferedReward struct {
	blockHeight int64
	blockTime   time.Time
	sender      string
	recipient   string
	amount      string
	rewardType  string
}

// ProcessorConfig holds optional configuration for the processor.
type ProcessorConfig struct {
	WalletAddress             string   // Our wallet address (tellor1xxx) for reporter "our" metric labels
	ValidatorConsensusAddress string   // Our validator consensus address (tellorvalcons) for validator "our" metric labels
	LayerAPIURLs              []string // Layer API URLs for fetching reporters at startup
	Registerer                prometheus.Registerer
}

type Processor struct {
	logger     log.Logger
	db         blockdb.Db
	txDecoder  sdk.TxDecoder
	httpClient *http.Client
	cfg        ProcessorConfig

	mtx              sync.Mutex
	processedHeights map[int64]struct{}
	heightQueue      []int64

	// Cycle-based reporter tracking for all reporters
	cycleMtx                sync.Mutex
	currentCycleQueryID     string
	knownReporters          map[string]struct{} // All reporters who have ever submitted
	reportersInCurrentCycle map[string]struct{} // Reporters who submitted in current cycle

	// Cumulative amount tracking for reward increment calculation.
	// Maps reporter address -> last known cumulative amount.
	rewardCumulative sync.Map
	// Maps "validator:allocType" -> last known cumulative amount.
	validatorRewardCumulative sync.Map

	// Batch insert buffer for all reward types (unified rewards table).
	rewardBuffer []bufferedReward
	bufferMtx    sync.Mutex

	// Batch insert buffer for block signatures.
	blockSignBuffer []bufferedBlockSign
	blockSignMtx    sync.Mutex

	// Batch insert buffer for oracle reports.
	reportBuffer []bufferedReport
	reportMtx    sync.Mutex

	// Batch insert buffer for transactions.
	txBuffer []bufferedTx
	txMtx    sync.Mutex

	// Batch insert buffer for cycle rotations.
	cycleRotationBuffer []bufferedCycleRotation
	cycleRotationMtx    sync.Mutex

	// The only two metrics this processor exposes, both for our own node:
	//   missedBlocks  — signatures our validator missed, labelled by sig type
	//                   (consensus, valset_sig, oracle_attestation)
	//   missedReports — reporter cycles our reporter missed
	missedBlocks  *prometheus.CounterVec
	missedReports prometheus.Counter
}

func New(
	ctx context.Context,
	logger log.Logger,
	db blockdb.Db,
) *Processor {
	return NewWithConfig(ctx, logger, db, ProcessorConfig{})
}

// NewWithConfig creates a processor with additional configuration options.
// The provided context governs the background reporter-fetch goroutine; cancel
// it (e.g. the same ctx passed to Monitor.Run) to stop the fetch on shutdown.
func NewWithConfig(
	ctx context.Context,
	logger log.Logger,
	db blockdb.Db,
	cfg ProcessorConfig,
) *Processor {
	p := &Processor{
		logger:                  logger.With("component", ComponentName),
		db:                      db,
		txDecoder:               NewTxDecoder(),
		httpClient:              &http.Client{Timeout: 10 * time.Second},
		cfg:                     cfg,
		processedHeights:        make(map[int64]struct{}),
		knownReporters:          make(map[string]struct{}),
		reportersInCurrentCycle: make(map[string]struct{}),
	}
	if cfg.Registerer == nil {
		// Use an isolated registry by default to avoid duplicate registration
		// when multiple processors are created in tests.
		cfg.Registerer = prometheus.NewRegistry()
	}
	p.missedBlocks = promauto.With(cfg.Registerer).NewCounterVec(prometheus.CounterOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "processor",
		Name:      "missed_our_validator_blocks_total",
		Help: "Total signatures our validator missed, by type: consensus precommits, " +
			"valset checkpoints and oracle attestations",
	}, []string{"type"})
	p.missedReports = promauto.With(cfg.Registerer).NewCounter(prometheus.CounterOpts{
		Namespace: monitor.MetricsNamespace,
		Subsystem: "processor",
		Name:      "missed_our_reporter_cycles_total",
		Help:      "Total reporter cycles our reporter missed",
	})
	// Do not pre-seed knownReporters from the API: jailed/inactive reporters
	// that never submit would be counted as missing every cycle, inflating the
	// network miss rate. Build the list incrementally from actual reports only.
	return p
}

func (p *Processor) ProcessBlock(ctx context.Context, blockEv ctypes.EventDataNewBlock) {
	if blockEv.Block == nil {
		return
	}
	if !p.shouldProcess(blockEv.Block.Height) {
		return
	}
	p.insertTx(blockEv)
	p.insertEvents(ctx, blockEv)
	p.insertBlockSigns(blockEv)
}

// handleCycleRotation is called when a rotating-cyclelist-with-next-query event is detected.
// Records the cycle rotation to DB for historical tracking and missed reports calculation.
// Missed reports are now calculated from DB: total cycles - reports per reporter.
func (p *Processor) handleCycleRotation(ctx context.Context, height int64, blockTime time.Time, newQueryID string) {
	p.cycleMtx.Lock()
	defer p.cycleMtx.Unlock()

	// Check if this is the first cycle (no previous query to check)
	if p.currentCycleQueryID == "" {
		p.currentCycleQueryID = newQueryID
		p.recordCycleRotation(ctx, height, blockTime, newQueryID)
		p.logger.Info("first cycle detected, initializing cycle tracking",
			"height", height,
			"query_id", newQueryID,
		)
		return
	}

	// Same query ID - no rotation needed
	if p.currentCycleQueryID == newQueryID {
		return
	}

	// New cycle detected - record to DB
	p.recordCycleRotation(ctx, height, blockTime, newQueryID)

	// Count a missed report if our reporter did not submit in the cycle that just
	// completed. WalletAddress is guaranteed non-empty (validated at startup).
	if _, submitted := p.reportersInCurrentCycle[p.cfg.WalletAddress]; !submitted {
		if _, known := p.knownReporters[p.cfg.WalletAddress]; known {
			p.missedReports.Inc()
			p.logger.Warn("our reporter missed submitting report in cycle",
				"height", height,
				"missed_query_id", p.currentCycleQueryID,
				"new_query_id", newQueryID,
				"reporter", p.cfg.WalletAddress,
			)
		}
	}

	// Reset for new cycle
	p.currentCycleQueryID = newQueryID
	p.reportersInCurrentCycle = make(map[string]struct{})
}

// recordCycleRotation buffers a cycle rotation event for batch insert.
// Used for calculating missed reports: total cycles - actual reports per reporter.
func (p *Processor) recordCycleRotation(_ context.Context, height int64, blockTime time.Time, queryID string) {
	p.cycleRotationMtx.Lock()
	p.cycleRotationBuffer = append(p.cycleRotationBuffer, bufferedCycleRotation{
		blockHeight: height,
		blockTime:   blockTime,
		queryID:     queryID,
	})
	p.cycleRotationMtx.Unlock()
}

// markReporterReportedInCycle marks that a reporter has submitted a report in the current cycle.
// Also adds to knownReporters to catch any new reporters that joined after startup.
func (p *Processor) markReporterReportedInCycle(reporter string) {
	p.cycleMtx.Lock()
	defer p.cycleMtx.Unlock()
	p.knownReporters[reporter] = struct{}{}
	p.reportersInCurrentCycle[reporter] = struct{}{}
}

const processedHeightsLimit = 1000

func (p *Processor) shouldProcess(height int64) bool {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if _, seen := p.processedHeights[height]; seen {
		return false
	}

	p.processedHeights[height] = struct{}{}
	p.heightQueue = append(p.heightQueue, height)
	if len(p.heightQueue) > processedHeightsLimit {
		oldest := p.heightQueue[0]
		p.heightQueue = p.heightQueue[1:]
		delete(p.processedHeights, oldest)
	}
	return true
}

func (p *Processor) insertTx(blockEv ctypes.EventDataNewBlock) {
	txs := blockEv.Block.Txs
	txsResults := blockEv.ResultFinalizeBlock.TxResults
	if len(txs) != len(txsResults) {
		p.logger.Warn("txs length mismatch", "txs", len(txs), "results", len(txsResults))
	}

	for i := 0; i < len(txs) && i < len(txsResults); i++ {
		raw := txs[i]

		if voteExtTx, ok := ParseVoteExtensionTx(raw); ok {
			// Vote extensions carry no fees/messages, so they are not decoded as
			// normal txs, but they do tell us which operators signed the valset
			// checkpoint and the oracle attestations at this height.
			p.recordVoteExtSigs(voteExtTx, blockEv.Block.Height, blockEv.Block.Time)
			continue
		}

		tx, err := p.txDecoder(raw)
		if err != nil {
			p.logger.Error("decode tx", "error", err)
			monitor.IncError("txDecode", ComponentName)
			continue
		}
		feeTx, ok := tx.(sdk.FeeTx)
		if !ok {
			continue
		}
		feeCoins := feeTx.GetFee()
		if feeCoins.Len() == 0 {
			continue
		}
		fee := feeCoins[0].Amount.String()

		txResp := txsResults[i]
		sender := findSenderFromTx(tx)
		if sender == unknownSender {
			sender = findSenderFromEvents(txResp.GetEvents())
		}

		p.txMtx.Lock()
		p.txBuffer = append(p.txBuffer, bufferedTx{
			blockHeight: blockEv.Block.Height,
			blockTime:   blockEv.Block.Time,
			txHash:      fmt.Sprintf("%X", raw.Hash()),
			sender:      sender,
			gasUsed:     txResp.GasUsed,
			fee:         fee,
		})
		p.txMtx.Unlock()
	}
}

// insertEvents processes block events and tracks reporter activity per cycle.
func (p *Processor) insertEvents(ctx context.Context, blockEv ctypes.EventDataNewBlock) {
	height := blockEv.Block.Height
	blockTime := blockEv.Block.Time

	var allEvents []abci.Event
	allEvents = append(allEvents, blockEv.ResultFinalizeBlock.Events...)
	for _, txResult := range blockEv.ResultFinalizeBlock.TxResults {
		if txResult != nil {
			allEvents = append(allEvents, txResult.Events...)
		}
	}

	var _ string // currentQueryID removed; retained for future use if needed
	for _, ev := range allEvents {
		switch ev.Type {

		// Cyclelist rotation event - indicates a new query cycle has started.
		// Check if our reporter submitted in the previous cycle.
		case "rotating-cyclelist-with-next-query":
			newQueryID := getAttribute(ev, AttrKeyQueryID)
			p.handleCycleRotation(ctx, height, blockTime, newQueryID)

		// Contains oracle data submission details. Used to track reporter activity.
		case "new_report":
			report, err := DecodeReportEvent(height, ev)
			if err != nil {
				p.logger.Error("failed to decode report event", "error", err)
				monitor.IncError("reportDecode", ComponentName)
				continue
			}

			// Track this reporter submitted a report in this cycle
			p.markReporterReportedInCycle(report.Reporter)

			if err := p.storeReport(blockTime, *report); err != nil {
				p.logger.Error("failed to store report", "error", err)
				monitor.IncError("reportInsert", ComponentName)
			}

		// Contains query_id for the aggregated report.
		case "aggregate_report":
			// retained for potential future use

		// Emitted by DivvyingTips for every reporter reward (both time-based and tip-based).
		// Attributes: reporter, commission, net_reward, period_total.
		// commission + net_reward = total gross reward for this event.
		case "rewards_accumulated":
			if err := p.insertReporterAccumulatedReward(ctx, height, blockTime, ev); err != nil {
				p.logger.Error("failed to store reporter accumulated reward", "error", err)
				monitor.IncError("rewardInsert", ComponentName)
			}

		// Contains validator commission allocation per block. Used to calculate validator earnings.
		case "commission":
			validator := getAttribute(ev, AttrKeyValidator)
			amount := getAttribute(ev, AttrKeyAmount)
			p.logger.Debug("commission event received",
				"height", height,
				"validator", validator,
				"amount", amount,
			)
			if err := p.insertValidatorReward(ctx, height, blockTime, ev, blockdb.RewardTypeValidatorCommission); err != nil {
				p.logger.Error("failed to store validator commission allocation", "error", err)
				monitor.IncError("validatorRewardsAllocInsert", ComponentName)
			}

		// Contains validator staking rewards per block. Used to track delegator reward accrual.
		case "rewards":
			validator := getAttribute(ev, AttrKeyValidator)
			amount := getAttribute(ev, AttrKeyAmount)
			p.logger.Debug("rewards event received",
				"height", height,
				"validator", validator,
				"amount", amount,
			)
			if err := p.insertValidatorReward(ctx, height, blockTime, ev, blockdb.RewardTypeValidatorDelegator); err != nil {
				p.logger.Error("failed to store validator rewards allocation", "error", err)
				monitor.IncError("validatorRewardsAllocInsert", ComponentName)
			}
		}
	}
}

// insertReporterReward stores reporter tip rewards from the rewards_added event.
// The reporter is both the sender and recipient (self-reward for reporting).
func (p *Processor) insertReporterReward(ctx context.Context, height int64, blockTime time.Time, ev abci.Event, _ string) error {
	delegatorRaw := getAttribute(ev, AttrKeyDelegator)
	if delegatorRaw == "" {
		return fmt.Errorf("reward missing delegator")
	}

	amountStr := getAttribute(ev, AttrKeyAmount)
	if amountStr == "" {
		return fmt.Errorf("reward missing amount")
	}

	// Parse delegator address - try bech32 first, then raw bytes
	reporter, err := parseAddressFromEventValue(delegatorRaw)
	if err != nil {
		return fmt.Errorf("invalid delegator address: %w", err)
	}

	// Parse current cumulative amount from chain event using full decimal precision
	currentAmount, err := parseDecimalAmountPrecise(amountStr)
	if err != nil {
		return fmt.Errorf("parse reward amount: %w", err)
	}

	// Tiny threshold (< 0.0001 loya) to skip only near-zero amounts after withdrawal
	tinyThreshold := decimal.NewFromFloat(0.0001)

	// Get previous cumulative amount from in-memory cache
	prev, hasPrev := p.rewardCumulative.Load(reporter)

	// First event for this reporter - just populate cache baseline, no insert
	if !hasPrev {
		p.rewardCumulative.Store(reporter, currentAmount)
		p.logger.Debug("first reward event, populating cache baseline",
			"reporter", reporter,
			"baseline", currentAmount.String(),
		)
		return nil
	}

	prevAmount := prev.(decimal.Decimal)

	// Calculate increment
	increment := currentAmount.Sub(prevAmount)

	// Handle withdrawal detection (negative increment means cumulative was reset)
	if increment.LessThanOrEqual(decimal.Zero) {
		// Check if this is a near-zero cumulative (true withdrawal drain)
		if currentAmount.LessThan(tinyThreshold) {
			// True withdrawal with near-zero cumulative - no meaningful reward to insert
			p.rewardCumulative.Store(reporter, currentAmount)
			p.logger.Debug("withdrawal detected, resetting baseline",
				"reporter", reporter,
				"previous", prevAmount.String(),
				"new_baseline", currentAmount.String(),
			)
			return nil
		}

		// This is a post-withdrawal reward - use currentAmount as increment
		// (because baseline was effectively reset to 0 after withdrawal)
		p.logger.Debug("post-withdrawal reward detected",
			"reporter", reporter,
			"previous", prevAmount.String(),
			"current", currentAmount.String(),
			"using_as_increment", currentAmount.String(),
		)
		increment = currentAmount
	}

	// Update cache with current cumulative value
	p.rewardCumulative.Store(reporter, currentAmount)

	// For reporter tips, sender and recipient are the same (reporter receives their own tip)
	return p.bufferReward(ctx, height, blockTime, reporter, reporter, increment, blockdb.RewardTypeReporterTip)
}

// insertReporterAccumulatedReward stores reporter rewards from the rewards_accumulated event.
// This event is emitted by DivvyingTips for every reporter reward — both time-based (TBR)
// and tip-based. It is the single correct source of truth for all reporter earnings.
//
// Attributes: reporter, commission, net_reward, period_total.
// Total gross reward = commission + net_reward (both are per-event increments, not cumulative).
func (p *Processor) insertReporterAccumulatedReward(ctx context.Context, height int64, blockTime time.Time, ev abci.Event) error {
	reporterRaw := getAttribute(ev, AttrKeyReporter)
	if reporterRaw == "" {
		return fmt.Errorf("rewards_accumulated missing reporter")
	}

	commissionStr := getAttribute(ev, AttrKeyCommission)
	netRewardStr := getAttribute(ev, AttrKeyNetReward)
	if commissionStr == "" && netRewardStr == "" {
		return fmt.Errorf("rewards_accumulated missing both commission and net_reward")
	}

	reporter, err := parseAddressFromEventValue(reporterRaw)
	if err != nil {
		return fmt.Errorf("invalid reporter address: %w", err)
	}

	commission := decimal.Zero
	if commissionStr != "" {
		if commission, err = parseDecimalAmountPrecise(commissionStr); err != nil {
			return fmt.Errorf("parse commission: %w", err)
		}
	}

	netReward := decimal.Zero
	if netRewardStr != "" {
		if netReward, err = parseDecimalAmountPrecise(netRewardStr); err != nil {
			return fmt.Errorf("parse net_reward: %w", err)
		}
	}

	total := commission.Add(netReward)
	if total.LessThanOrEqual(decimal.Zero) {
		return nil
	}

	return p.bufferReward(ctx, height, blockTime, reporter, reporter, total, blockdb.RewardTypeReporterTip)
}

// insertValidatorReward stores validator rewards allocation events from the distribution module.
// Handles: "commission" and "rewards" events emitted in BeginBlock for each active validator.
// These events track the per-block allocation of staking rewards before withdrawal.
// Stores incremental amounts (current - previous) instead of cumulative values.
func (p *Processor) insertValidatorReward(ctx context.Context, height int64, blockTime time.Time, ev abci.Event, rewardType string) error {
	validator := getAttribute(ev, AttrKeyValidator)
	if validator == "" {
		return fmt.Errorf("validator rewards allocation missing validator address")
	}

	amountStr := getAttribute(ev, AttrKeyAmount)
	if amountStr == "" {
		// Empty amount only ever applies to commission events with a 0%
		// commission rate (the validator keeps 0 from the delegators' share).
		// It does NOT mean the validator earned nothing — staking rewards are
		// still distributed to delegators in that case. So skipping is safe
		// for commission only; a missing rewards amount would be a real bug.
		if rewardType == blockdb.RewardTypeValidatorCommission {
			return nil
		}
		return fmt.Errorf("validator reward event missing amount (type=%s validator=%s)", rewardType, validator)
	}

	// Parse current cumulative amount from chain event using full decimal precision
	currentAmount, err := parseDecimalAmountPrecise(amountStr)
	if err != nil {
		return fmt.Errorf("parse validator reward amount: %w", err)
	}

	// Tiny threshold (< 0.0001 loya) to skip only near-zero amounts after withdrawal
	tinyThreshold := decimal.NewFromFloat(0.0001)

	// Get previous cumulative amount from in-memory cache
	cacheKey := validator + ":" + rewardType
	prev, hasPrev := p.validatorRewardCumulative.Load(cacheKey)

	// If no previous value, just populate cache and skip insert (first event for this validator)
	if !hasPrev {
		p.validatorRewardCumulative.Store(cacheKey, currentAmount)
		p.logger.Debug("first validator reward event, populating cache baseline",
			"validator", validator,
			"type", rewardType,
			"baseline", currentAmount.String(),
		)
		return nil
	}

	prevAmount := prev.(decimal.Decimal)

	// Calculate increment
	increment := currentAmount.Sub(prevAmount)

	// Handle withdrawal detection (negative increment means cumulative was reset)
	if increment.LessThanOrEqual(decimal.Zero) {
		// Check if this is a near-zero cumulative (true withdrawal drain)
		if currentAmount.LessThan(tinyThreshold) {
			// True withdrawal with near-zero cumulative - no meaningful reward to insert
			p.validatorRewardCumulative.Store(cacheKey, currentAmount)
			p.logger.Debug("validator reward reset detected, updating baseline",
				"validator", validator,
				"type", rewardType,
				"previous", prevAmount.String(),
				"new_baseline", currentAmount.String(),
			)
			return nil
		}

		// This is a post-withdrawal reward - use currentAmount as increment
		p.logger.Debug("post-withdrawal validator reward detected",
			"validator", validator,
			"type", rewardType,
			"previous", prevAmount.String(),
			"current", currentAmount.String(),
			"using_as_increment", currentAmount.String(),
		)
		increment = currentAmount
	}

	// Update cache with current cumulative value
	p.validatorRewardCumulative.Store(cacheKey, currentAmount)

	// For validator rewards, sender and recipient are the same (validator receives from distribution)
	return p.bufferReward(ctx, height, blockTime, validator, validator, increment, rewardType)
}

// bufferReward adds a reward record to the unified buffer for batch insert.
// Supports all reward types: reporter_tip, validator_commission, validator_delegator.
func (p *Processor) bufferReward(ctx context.Context, height int64, blockTime time.Time, sender, recipient string, amount decimal.Decimal, rewardType string) error {
	p.bufferMtx.Lock()
	p.rewardBuffer = append(p.rewardBuffer, bufferedReward{
		blockHeight: height,
		blockTime:   blockTime,
		sender:      sender,
		recipient:   recipient,
		amount:      amount.String(),
		rewardType:  rewardType,
	})
	shouldFlush := len(p.rewardBuffer) >= batchInsertThreshold
	p.bufferMtx.Unlock()

	if shouldFlush {
		return p.flushRewards(ctx)
	}
	return nil
}

// flushRewards writes buffered rewards to the database using batch INSERT.
// Handles all reward types in the unified rewards table.
func (p *Processor) flushRewards(ctx context.Context) error {
	p.bufferMtx.Lock()
	if len(p.rewardBuffer) == 0 {
		p.bufferMtx.Unlock()
		return nil
	}
	records := p.rewardBuffer
	p.rewardBuffer = nil
	p.bufferMtx.Unlock()

	// Build batch INSERT for ClickHouse performance
	var values []string
	var args []any
	for _, r := range records {
		values = append(values, "(?, ?, ?, ?, ?, ?)")
		args = append(args, r.blockHeight, r.blockTime, r.sender, r.recipient, r.amount, r.rewardType)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES %s",
		blockdb.TableNameRewards,
		blockdb.ColBlockHeight, blockdb.ColBlockTime, blockdb.ColSender,
		blockdb.ColRecipient, blockdb.ColAmount, blockdb.ColType,
		strings.Join(values, ", "))

	insertCtx, cancel := context.WithTimeout(ctx, DefaultDBTimeout*10) // longer timeout for batch
	defer cancel()

	_, err := p.db.Exec(insertCtx, query, args...)
	if err != nil {
		p.logger.Error("batch insert rewards failed", "count", len(records), "error", err)
		return err
	}

	p.logger.Debug("batch inserted rewards", "count", len(records))
	return nil
}

// flushBlockSigns writes buffered block_signs to the database using batch INSERT.
func (p *Processor) flushBlockSigns(ctx context.Context) error {
	p.blockSignMtx.Lock()
	if len(p.blockSignBuffer) == 0 {
		p.blockSignMtx.Unlock()
		return nil
	}
	records := p.blockSignBuffer
	p.blockSignBuffer = nil
	p.blockSignMtx.Unlock()

	var values []string
	var args []any
	for _, r := range records {
		values = append(values, "(?, ?, ?, ?, ?)")
		args = append(args, r.blockHeight, r.blockTime, r.validator, r.signed, r.sigType)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s) VALUES %s",
		blockdb.TableNameBlockSigns,
		blockdb.ColBlockHeight, blockdb.ColBlockTimestamp, blockdb.ColValidatorAddress, blockdb.ColSigned,
		blockdb.ColSigType,
		strings.Join(values, ", "))

	insertCtx, cancel := context.WithTimeout(ctx, DefaultDBTimeout*10)
	defer cancel()

	_, err := p.db.Exec(insertCtx, query, args...)
	if err != nil {
		p.logger.Error("batch insert block_signs failed", "count", len(records), "error", err)
		return err
	}

	p.logger.Debug("batch inserted block_signs", "count", len(records))
	return nil
}

// recordVoteExtSigs buffers the operators that signed each vote-extension
// payload at this height into the same block_signs buffer, tagged by sig_type.
// Only signers are written: absence of a row for a (height, sig_type) is what
// counts as a miss, exactly as for consensus rows.
func (p *Processor) recordVoteExtSigs(voteExtTx *app.VoteExtTx, blockHeight int64, blockTime time.Time) {
	sets := []struct {
		sigType   string
		operators []string
	}{
		{blockdb.SigTypeValsetSig, voteExtTx.ValsetSigs.OperatorAddresses},
		{blockdb.SigTypeOracleAttestation, voteExtTx.OracleAttestations.OperatorAddresses},
	}

	ourOperator := cryptoaddr.ToValidatorOperator(p.cfg.WalletAddress)

	p.blockSignMtx.Lock()
	defer p.blockSignMtx.Unlock()
	for _, set := range sets {
		if len(set.operators) == 0 {
			// No payload of this kind at this height (valset checkpoints only
			// appear on validator-set changes), so nobody could have missed it.
			continue
		}
		ourSigFound := false
		for _, op := range set.operators {
			if op == "" {
				continue
			}
			if op == ourOperator {
				ourSigFound = true
			}
			p.blockSignBuffer = append(p.blockSignBuffer, bufferedBlockSign{
				blockHeight: blockHeight,
				blockTime:   blockTime,
				validator:   op,
				signed:      1,
				sigType:     set.sigType,
			})
		}
		if !ourSigFound && ourOperator != "" {
			p.missedBlocks.WithLabelValues(set.sigType).Inc()
		}
	}
}

// flushReports writes buffered oracle reports to the database using batch INSERT.
func (p *Processor) flushReports(ctx context.Context) error {
	p.reportMtx.Lock()
	if len(p.reportBuffer) == 0 {
		p.reportMtx.Unlock()
		return nil
	}
	records := p.reportBuffer
	p.reportBuffer = nil
	p.reportMtx.Unlock()

	var values []string
	var args []any
	for _, r := range records {
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, r.reporter, r.power, r.queryType, r.queryIDHex, r.aggregateMethod, r.value, r.timestamp, r.blockTime, r.cyclelist, r.blockNumber, r.metaID)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES %s",
		blockdb.TableNameReports,
		blockdb.ColReporter, blockdb.ColPower, blockdb.ColQueryType, blockdb.ColQueryID,
		blockdb.ColAggregateMethod, blockdb.ColValue, blockdb.ColTimestamp, blockdb.ColBlockTime,
		blockdb.ColCyclelist, blockdb.ColBlockNumber, blockdb.ColMetaID,
		strings.Join(values, ", "))

	insertCtx, cancel := context.WithTimeout(ctx, DefaultDBTimeout*10)
	defer cancel()

	_, err := p.db.Exec(insertCtx, query, args...)
	if err != nil {
		p.logger.Error("batch insert reports failed", "count", len(records), "error", err)
		return err
	}
	p.logger.Debug("batch inserted reports", "count", len(records))
	return nil
}

// flushTxs writes buffered transactions to the database using batch INSERT.
func (p *Processor) flushTxs(ctx context.Context) error {
	p.txMtx.Lock()
	if len(p.txBuffer) == 0 {
		p.txMtx.Unlock()
		return nil
	}
	records := p.txBuffer
	p.txBuffer = nil
	p.txMtx.Unlock()

	var values []string
	var args []any
	for _, t := range records {
		values = append(values, "(?, ?, ?, ?, ?, ?)")
		args = append(args, t.blockHeight, t.blockTime, t.txHash, t.sender, t.gasUsed, t.fee)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES %s",
		blockdb.TableNameTxs,
		blockdb.ColBlockHeight, blockdb.ColBlockTime, blockdb.ColTxHash,
		blockdb.ColSender, blockdb.ColGasUsed, blockdb.ColFeeAmount,
		strings.Join(values, ", "))

	insertCtx, cancel := context.WithTimeout(ctx, DefaultDBTimeout*10)
	defer cancel()

	_, err := p.db.Exec(insertCtx, query, args...)
	if err != nil {
		p.logger.Error("batch insert txs failed", "count", len(records), "error", err)
		return err
	}
	p.logger.Debug("batch inserted txs", "count", len(records))
	return nil
}

// flushCycleRotations writes buffered cycle rotations to the database using batch INSERT.
func (p *Processor) flushCycleRotations(ctx context.Context) error {
	p.cycleRotationMtx.Lock()
	if len(p.cycleRotationBuffer) == 0 {
		p.cycleRotationMtx.Unlock()
		return nil
	}
	records := p.cycleRotationBuffer
	p.cycleRotationBuffer = nil
	p.cycleRotationMtx.Unlock()

	var values []string
	var args []any
	for _, r := range records {
		values = append(values, "(?, ?, ?)")
		args = append(args, r.blockHeight, r.queryID, r.blockTime)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s) VALUES %s",
		blockdb.TableNameCycleRotations,
		blockdb.ColBlockHeight, blockdb.ColQueryID, blockdb.ColTimestamp,
		strings.Join(values, ", "))

	insertCtx, cancel := context.WithTimeout(ctx, DefaultDBTimeout*10)
	defer cancel()

	_, err := p.db.Exec(insertCtx, query, args...)
	if err != nil {
		p.logger.Error("batch insert cycle_rotations failed", "count", len(records), "error", err)
		return err
	}
	p.logger.Debug("batch inserted cycle_rotations", "count", len(records))
	return nil
}

// Flush writes all buffered records to the database.
// This should be called after processing a batch of blocks during backfill.
func (p *Processor) Flush(ctx context.Context) error {
	if err := p.flushRewards(ctx); err != nil {
		return err
	}
	if err := p.flushBlockSigns(ctx); err != nil {
		return err
	}
	if err := p.flushReports(ctx); err != nil {
		return err
	}
	if err := p.flushTxs(ctx); err != nil {
		return err
	}
	return p.flushCycleRotations(ctx)
}

// parseDecimalAmountPrecise parses amount strings like "123.456789loya" or "123.456789"
// using shopspring/decimal for arbitrary precision to match chain's 18-decimal tracking.
func parseDecimalAmountPrecise(amountStr string) (decimal.Decimal, error) {
	// Remove common suffixes like "loya"
	cleaned := strings.TrimSuffix(amountStr, "loya")
	cleaned = strings.TrimSpace(cleaned)

	amount, err := decimal.NewFromString(cleaned)
	if err != nil {
		return decimal.Zero, fmt.Errorf("invalid amount format %q: %w", amountStr, err)
	}
	return amount, nil
}

func (p *Processor) storeReport(blockTime time.Time, r types.MicroReport) error {
	var cycle uint8
	if r.Cyclelist {
		cycle = 1
	}

	queryIDHex, err := EncodeQueryID(r.QueryId)
	if err != nil {
		return err
	}

	p.reportMtx.Lock()
	p.reportBuffer = append(p.reportBuffer, bufferedReport{
		reporter:        r.Reporter,
		power:           r.Power,
		queryType:       r.QueryType,
		queryIDHex:      queryIDHex,
		aggregateMethod: r.AggregateMethod,
		value:           r.Value,
		timestamp:       r.Timestamp,
		blockTime:       blockTime,
		cyclelist:       cycle,
		blockNumber:     r.BlockNumber,
		metaID:          r.MetaId,
	})
	p.reportMtx.Unlock()
	return nil
}

func (p *Processor) insertBlockSigns(blockEv ctypes.EventDataNewBlock) {
	if blockEv.Block == nil || blockEv.Block.LastCommit == nil {
		return
	}

	// IMPORTANT: LastCommit contains signatures for the PREVIOUS block (height-1),
	// not the current block. This is by design in CometBFT.
	currentHeight := blockEv.Block.Height
	if currentHeight <= 1 {
		// Genesis block (height 1) has no previous block to have signatures for
		return
	}

	// The signatures are for block at height-1
	commitHeight := currentHeight - 1
	blockTime := blockEv.Block.Time
	for _, sig := range blockEv.Block.LastCommit.Signatures {
		// Skip empty signatures (validator not in set at that height)
		if len(sig.ValidatorAddress) == 0 {
			continue
		}

		// Determine signed status based on BlockIDFlag:
		// - BlockIDFlagCommit: validator signed the block (signed = 1)
		// - BlockIDFlagNil: validator voted nil - valid vote but not for this block (signed = 0)
		// - BlockIDFlagAbsent: validator was absent/missed (signed = 0)
		var signed uint8
		if sig.BlockIDFlag == ctypes.BlockIDFlagCommit {
			signed = 1
		}
		// Note: Both BlockIDFlagNil and BlockIDFlagAbsent result in signed = 0
		// If more granular tracking is needed, add a vote_type column

		// Convert to bech32 consensus address format
		validatorAddr := sdk.ConsAddress(sig.ValidatorAddress).String()

		// Log a warning if our validator missed signing while in the active set.
		if validatorAddr == p.cfg.ValidatorConsensusAddress && signed == 0 {
			p.missedBlocks.WithLabelValues(blockdb.SigTypeConsensus).Inc()
			p.logger.Warn("our validator missed signing block", "height", commitHeight, "validator", validatorAddr)
		}

		p.blockSignMtx.Lock()
		p.blockSignBuffer = append(p.blockSignBuffer, bufferedBlockSign{
			blockHeight: commitHeight,
			blockTime:   blockTime,
			validator:   validatorAddr,
			signed:      signed,
			sigType:     blockdb.SigTypeConsensus,
		})
		p.blockSignMtx.Unlock()
	}

	// Blocks where our validator is out of the active set (jailed/unbonded) are
	// intentionally NOT counted as missed: it is absent from the commit entirely,
	// exactly like every other out-of-set validator. Counting only in-set absences
	// keeps the missed-block calculation identical for all validators.
}

func getAttribute(ev abci.Event, key string) string {
	for _, attr := range ev.Attributes {
		if attr.Key == key {
			return attr.Value
		}
	}
	return ""
}

// parseAddressFromEventValue parses an address from an event attribute value.
// It tries bech32 decoding first. If that fails, it treats the value as raw bytes
// and converts to bech32. This handles both formats used by Cosmos SDK events.
func parseAddressFromEventValue(value string) (string, error) {
	// First try parsing as bech32 (most common case)
	if addr, err := sdk.AccAddressFromBech32(value); err == nil {
		return addr.String(), nil
	}

	// If bech32 fails, try interpreting as raw bytes
	// Event attribute values can contain raw address bytes (20 bytes for cosmos addresses)
	rawBytes := []byte(value)
	if len(rawBytes) == 20 {
		addr := sdk.AccAddress(rawBytes)
		return addr.String(), nil
	}

	// Neither format worked
	return "", fmt.Errorf("cannot parse address: not valid bech32 and not 20 raw bytes (got %d bytes)", len(rawBytes))
}

func findSenderFromEvents(events []abci.Event) string {
	for _, ev := range events {
		if ev.Type != "message" {
			continue
		}
		if sender := getAttribute(ev, AttrKeySender); sender != "" {
			return sender
		}
	}
	return unknownSender
}

func findSenderFromTx(tx sdk.Tx) string {
	if signingTx, ok := tx.(authsigning.Tx); ok {
		signers, err := signingTx.GetSigners()
		if err == nil && len(signers) > 0 {
			return sdk.AccAddress(signers[0]).String()
		}
	}
	return unknownSender
}
