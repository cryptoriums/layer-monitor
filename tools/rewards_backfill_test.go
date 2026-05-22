package tools

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	nethttp "net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/rpc/client/http"
	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/joho/godotenv"
	"github.com/shopspring/decimal"
	blockdb "github.com/cryptoriums/layer-monitor/db"
	"github.com/cryptoriums/layer-packages/encoding"
	"github.com/cryptoriums/layer-monitor/monitors/block/processor"
	_ "github.com/tellor-io/layer/app/config" // Import to trigger init() for bech32 prefix setup

	sdk "github.com/cosmos/cosmos-sdk/types"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
)

var (
	backfillStart         = flag.Int64("backfill-start", 0, "Start block height for backfill")
	backfillEnd           = flag.Int64("backfill-end", 0, "End block height for backfill")
	backfillRPC           = flag.String("backfill-rpc", "", "RPC endpoint URL (defaults to LAYER_RPC_URLS env)")
	backfillAPI           = flag.String("backfill-api", "", "REST API URL for withdrawal queries (defaults to LAYER_API_URLS env)")
	backfillWorkers       = flag.Int("backfill-workers", 10, "Number of parallel fetch workers")
	backfillDryRun        = flag.Bool("backfill-dry-run", false, "Don't insert to DB, just show what would be inserted")
	backfillBaselineBlock = flag.Int64("backfill-baseline-block", 0, "Block height to fetch baseline cumulative values from (usually start-1)")
	backfillReporter      = flag.String("backfill-reporter", "", "Filter backfill to specific reporter address (optional)")
)

const eventTypeRewardsAdded = "rewards_added"

// backfillWithdrawal represents a withdrawal event for splitting backfill ranges.
type backfillWithdrawal struct {
	Height   int64
	Reporter string
}

// TestBackfillRewards runs a backfill of rewards data from a block range.
// This replicates the monitor's backfill logic for rewards extraction.
//
// Usage:
//
//	go test -v ./cryptoriums/tools/... -run TestBackfillRewards \
//	  -args -env=/root/.layer/.env \
//	  -backfill-start=14000000 \
//	  -backfill-end=14100000
//
// With baseline block (to avoid skipping first rewards in range):
//
//	go test -v ./cryptoriums/tools/... -run TestBackfillRewards \
//	  -args -env=/root/.layer/.env \
//	  -backfill-start=14480285 \
//	  -backfill-end=14530285 \
//	  -backfill-baseline-block=14480284
//
// Dry run (no DB writes):
//
//	go test -v ./cryptoriums/tools/... -run TestBackfillRewards \
//	  -args -backfill-start=14000000 -backfill-end=14001000 -backfill-dry-run
func TestBackfillRewards(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping backfill in short mode")
	}

	// Validate required flags
	if *backfillStart == 0 || *backfillEnd == 0 {
		t.Skip("skipping: -backfill-start and -backfill-end flags are required")
	}
	if *backfillStart > *backfillEnd {
		t.Fatal("Start block must be <= end block")
	}

	// Load environment from file
	if *envFile != "" {
		if err := godotenv.Load(*envFile); err != nil {
			t.Logf("Warning: could not load env file %s: %v", *envFile, err)
		} else {
			t.Logf("Loaded env from %s", *envFile)
		}
	}

	ctx := context.Background()

	// Resolve RPC and API URLs from flags or environment
	rpcURL := *backfillRPC
	if rpcURL == "" {
		rpcURLs := parseRPCURLs()
		if len(rpcURLs) == 0 {
			t.Fatal("LAYER_RPC_URLS not set and -backfill-rpc not provided")
		}
		rpcURL = rpcURLs[0]
	}
	apiURL := *backfillAPI
	if apiURL == "" {
		apiURLs := parseAPIURLs()
		if len(apiURLs) == 0 {
			t.Fatal("LAYER_API_URLS not set and -backfill-api not provided")
		}
		apiURL = apiURLs[0]
	}

	t.Logf("Backfill Configuration:")
	t.Logf("  Block range: %d to %d (%d blocks)", *backfillStart, *backfillEnd, *backfillEnd-*backfillStart+1)
	t.Logf("  RPC: %s", rpcURL)
	t.Logf("  API: %s", apiURL)
	t.Logf("  Workers: %d", *backfillWorkers)
	t.Logf("  Dry run: %v", *backfillDryRun)
	if *backfillBaselineBlock > 0 {
		t.Logf("  Baseline block: %d", *backfillBaselineBlock)
	}

	// Connect to RPC
	client, err := http.New(rpcURL, "/websocket")
	if err != nil {
		t.Fatalf("Failed to connect to RPC: %v", err)
	}
	t.Logf("Connected to RPC: %s", rpcURL)

	// Connect to ClickHouse (unless dry run) - writable connection for backfill
	var db *sql.DB
	if !*backfillDryRun {
		db = connectClickHouseWritable(t)
		defer db.Close()

		// Ensure tables exist
		wrapped := blockdb.SQLDB{DB: db}
		if err := blockdb.EnsureTables(ctx, wrapped); err != nil {
			t.Fatalf("Failed to ensure tables: %v", err)
		}

		// Initialize known reporters cache for address matching
		if err := initKnownReporters(db); err != nil {
			t.Logf("Warning: failed to init known reporters: %v", err)
		} else {
			t.Logf("Loaded %d known reporter addresses for matching", len(knownReporterAddresses))
		}
	}

	// Fetch withdrawals in the range to properly handle baseline resets
	t.Log("Fetching withdrawal events in range...")
	withdrawals := fetchWithdrawalsInRange(ctx, t, apiURL, *backfillStart-1, *backfillEnd+1)
	reporterWithdrawals := groupWithdrawalsByReporter(withdrawals)
	t.Logf("Found %d withdrawals from %d reporters", len(withdrawals), len(reporterWithdrawals))

	// Log withdrawal summary per reporter
	for reporter, heights := range reporterWithdrawals {
		displayAddr := reporter
		if len(displayAddr) > 20 {
			displayAddr = displayAddr[:12] + "..." + displayAddr[len(displayAddr)-6:]
		}
		t.Logf("  %s: %d withdrawals at blocks %v", displayAddr, len(heights), heights)
	}

	// Run backfill
	backfiller := &rewardsBackfiller{
		t:                   t,
		client:              client,
		db:                  db,
		workers:             *backfillWorkers,
		dryRun:              *backfillDryRun,
		filterReporter:      *backfillReporter,
		rewardCumulative:    sync.Map{},
		reporterWithdrawals: reporterWithdrawals,
	}

	// Pre-populate baseline cumulative values if baseline block is specified
	if *backfillBaselineBlock > 0 {
		if err := backfiller.fetchBaseline(ctx, *backfillBaselineBlock); err != nil {
			t.Fatalf("Failed to fetch baseline: %v", err)
		}
	}

	if err := backfiller.run(ctx, *backfillStart, *backfillEnd); err != nil {
		t.Fatalf("Backfill failed: %v", err)
	}

	t.Log("========== BACKFILL SUMMARY ==========")
	failedCount := atomic.LoadInt64(&backfiller.failedParseCount)
	resetCount := atomic.LoadInt64(&backfiller.withdrawalResetCount)
	t.Logf("Failed address parses: %d", failedCount)
	t.Logf("Withdrawal baseline resets: %d", resetCount)
	if failedCount > 0 {
		t.Log("WARNING: Some rewards were skipped due to failed address parsing!")
	}
	t.Log("Backfill completed successfully")
}

// rewardsBackfiller handles the backfill process.
type rewardsBackfiller struct {
	t                    *testing.T
	client               *http.HTTP
	db                   *sql.DB
	workers              int
	dryRun               bool
	filterReporter       string             // optional: only process this reporter address
	rewardCumulative     sync.Map           // reporter -> cumulative amount
	lastProcessedHeight  sync.Map           // reporter -> last processed block height
	failedParseCount     int64              // count of failed address parses
	reporterWithdrawals  map[string][]int64 // reporter -> sorted list of withdrawal heights
	withdrawalResetCount int64              // count of baseline resets due to withdrawals
}

// hasWithdrawalBetween checks if there's a withdrawal for the reporter between
// the last processed height and the current height.
func (b *rewardsBackfiller) hasWithdrawalBetween(reporter string, currentHeight int64) bool {
	if b.reporterWithdrawals == nil {
		return false
	}

	withdrawals, exists := b.reporterWithdrawals[reporter]
	if !exists || len(withdrawals) == 0 {
		return false
	}

	// Get last processed height for this reporter
	lastHeight := int64(0)
	if prev, ok := b.lastProcessedHeight.Load(reporter); ok {
		lastHeight = prev.(int64)
	}

	// Check if any withdrawal falls between lastHeight and currentHeight
	for _, wh := range withdrawals {
		if wh > lastHeight && wh < currentHeight {
			return true
		}
	}
	return false
}

// markWithdrawalsProcessed marks all withdrawals up to currentHeight as processed
// by updating the lastProcessedHeight for the reporter.
func (b *rewardsBackfiller) updateLastProcessedHeight(reporter string, height int64) {
	b.lastProcessedHeight.Store(reporter, height)
}

// rewardRecord represents a reward to be inserted into the unified rewards table.
// Supports all reward types: reporter_tip, validator_commission, validator_delegator.
type rewardRecord struct {
	BlockHeight int64
	BlockTime   time.Time
	Sender      string
	Recipient   string
	Amount      decimal.Decimal
	RewardType  string
}

// run executes the backfill for the given block range.
func (b *rewardsBackfiller) run(ctx context.Context, start, end int64) error {
	totalBlocks := end - start + 1
	processedBlocks := int64(0)
	var rewards []rewardRecord
	var totalRewardsInserted int

	startTime := time.Now()
	lastLogTime := startTime

	// Process blocks in batches
	for batchStart := start; batchStart <= end; batchStart += int64(b.workers) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchEnd := batchStart + int64(b.workers) - 1
		if batchEnd > end {
			batchEnd = end
		}

		// Fetch blocks in parallel
		blocks, err := b.fetchBlocksParallel(ctx, batchStart, batchEnd)
		if err != nil {
			return fmt.Errorf("fetch blocks %d-%d: %w", batchStart, batchEnd, err)
		}

		// Process blocks in order
		for height := batchStart; height <= batchEnd; height++ {
			blockData, ok := blocks[height]
			if !ok {
				b.t.Logf("Warning: missing block %d", height)
				continue
			}

			// Extract rewards from block
			blockRewards := b.extractRewards(height, blockData)
			rewards = append(rewards, blockRewards...)

			processedBlocks++
		}

		// Insert batch if enough records (every 1000 rewards for better performance)
		if len(rewards) >= 1000 && !b.dryRun {
			if err := b.insertRewards(ctx, rewards); err != nil {
				return fmt.Errorf("insert rewards: %w", err)
			}
			totalRewardsInserted += len(rewards)
			rewards = rewards[:0]
		}

		// Log progress every 5 seconds
		if time.Since(lastLogTime) > 5*time.Second {
			elapsed := time.Since(startTime)
			blocksPerSec := float64(processedBlocks) / elapsed.Seconds()
			remaining := float64(totalBlocks-processedBlocks) / blocksPerSec
			b.t.Logf("Progress: %d/%d blocks (%.1f%%) | %.1f blocks/sec | %d rewards | ETA: %s",
				processedBlocks, totalBlocks,
				float64(processedBlocks)/float64(totalBlocks)*100,
				blocksPerSec,
				totalRewardsInserted+len(rewards),
				time.Duration(remaining*float64(time.Second)).Round(time.Second))
			lastLogTime = time.Now()
		}
	}

	// Insert remaining rewards
	if len(rewards) > 0 && !b.dryRun {
		if err := b.insertRewards(ctx, rewards); err != nil {
			return fmt.Errorf("insert final rewards: %w", err)
		}
		totalRewardsInserted += len(rewards)
	}

	elapsed := time.Since(startTime)
	b.t.Logf("Completed: %d blocks in %s (%.1f blocks/sec), %d rewards inserted",
		processedBlocks, elapsed.Round(time.Second), float64(processedBlocks)/elapsed.Seconds(), totalRewardsInserted)

	return nil
}

// fetchBlocksParallel fetches multiple blocks concurrently.
func (b *rewardsBackfiller) fetchBlocksParallel(ctx context.Context, start, end int64) (map[int64]*ctypes.ResultBlockResults, error) {
	results := make(map[int64]*ctypes.ResultBlockResults)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, end-start+1)

	for height := start; height <= end; height++ {
		wg.Add(1)
		go func(h int64) {
			defer wg.Done()

			blockRes, err := b.client.BlockResults(ctx, &h)
			if err != nil {
				errCh <- fmt.Errorf("block %d: %w", h, err)
				return
			}

			mu.Lock()
			results[h] = blockRes
			mu.Unlock()
		}(height)
	}

	wg.Wait()
	close(errCh)

	// Return first error if any
	for err := range errCh {
		return nil, err
	}

	return results, nil
}

// extractRewards extracts reward records from block results.
// Matches the monitor's approach: combine all events first, then process in order.
func (b *rewardsBackfiller) extractRewards(height int64, blockRes *ctypes.ResultBlockResults) []rewardRecord {
	// Combine all events like the monitor does
	var allEvents []abci.Event
	allEvents = append(allEvents, blockRes.FinalizeBlockEvents...)
	for _, txRes := range blockRes.TxsResults {
		if txRes != nil {
			allEvents = append(allEvents, txRes.Events...)
		}
	}

	// Debug logging
	if b.dryRun {
		rewardsAddedCount := 0
		for _, ev := range allEvents {
			if ev.Type == eventTypeRewardsAdded {
				rewardsAddedCount++
			}
		}
		if len(allEvents) > 0 || rewardsAddedCount > 0 {
			b.t.Logf("[Block %d] Total events: %d, rewards_added events: %d", height, len(allEvents), rewardsAddedCount)
		}
	}

	var rewards []rewardRecord
	var currentQueryID string

	// Process all events in order
	for _, ev := range allEvents {
		switch ev.Type {
		case "aggregate_report":
			currentQueryID = getBackfillAttribute(ev, processor.AttrKeyQueryID)

		case eventTypeRewardsAdded:
			if record := b.processRewardEvent(height, ev, currentQueryID); record != nil {
				rewards = append(rewards, *record)
			}
			currentQueryID = ""
		}
	}

	return rewards
}

// fetchBaseline fetches blocks from baselineHeight backwards (up to 100 blocks) to
// pre-populate the rewardCumulative cache with the latest cumulative values for each reporter.
// This allows the backfill to correctly calculate increments for the first rewards in the target range.
func (b *rewardsBackfiller) fetchBaseline(ctx context.Context, baselineHeight int64) error {
	const maxBaselineBlocks = 100
	startHeight := baselineHeight - maxBaselineBlocks + 1
	if startHeight < 1 {
		startHeight = 1
	}

	b.t.Logf("Fetching baseline cumulative values from blocks %d to %d...", startHeight, baselineHeight)

	// Fetch blocks and extract rewards_added events
	// Process in forward order so later blocks overwrite earlier ones (getting the latest cumulative)
	var totalCount int
	for height := startHeight; height <= baselineHeight; height++ {
		blockRes, err := b.client.BlockResults(ctx, &height)
		if err != nil {
			b.t.Logf("  Warning: failed to fetch block %d: %v", height, err)
			continue
		}

		// Combine all events
		var allEvents []abci.Event
		allEvents = append(allEvents, blockRes.FinalizeBlockEvents...)
		for _, txRes := range blockRes.TxsResults {
			if txRes != nil {
				allEvents = append(allEvents, txRes.Events...)
			}
		}

		// Extract cumulative values from rewards_added events
		for _, ev := range allEvents {
			if ev.Type != eventTypeRewardsAdded {
				continue
			}

			delegatorRaw := getBackfillAttribute(ev, "delegator")
			if delegatorRaw == "" {
				continue
			}

			amountStr := getBackfillAttribute(ev, "amount")
			if amountStr == "" {
				continue
			}

			// Parse delegator address - try multiple methods
			reporter := parseReporterAddress(delegatorRaw)
			if reporter == "" {
				continue
			}

			// Parse cumulative amount
			cumulativeAmount, err := parseBackfillAmount(amountStr)
			if err != nil {
				continue
			}

			// Store in cache (later blocks will overwrite, keeping the latest)
			b.rewardCumulative.Store(reporter, cumulativeAmount)
			totalCount++
		}
	}

	// Count unique reporters
	var uniqueReporters int
	b.rewardCumulative.Range(func(key, value any) bool {
		uniqueReporters++
		reporter := key.(string)
		amount := value.(decimal.Decimal)
		b.t.Logf("  Baseline: %s = %s loya", truncateAddr(reporter), amount.String())
		return true
	})

	b.t.Logf("Pre-populated %d unique reporter baselines from %d blocks", uniqueReporters, baselineHeight-startHeight+1)
	return nil
}

// processRewardEvent processes a rewards_added event and returns a record if applicable.
// Note: blockTime is passed as zero time since rewardsBackfiller doesn't have access to block data.
// For accurate block_time, use allTablesBackfiller which fetches both block and results.
func (b *rewardsBackfiller) processRewardEvent(height int64, ev abci.Event, _ string) *rewardRecord {
	// Use zero time since we don't have block data in this backfiller
	blockTime := time.Time{}
	// Debug logging
	if b.dryRun {
		b.t.Logf("  [%d] processRewardEvent called, event type: %s, attributes: %d", height, ev.Type, len(ev.Attributes))
	}

	delegatorRaw := getBackfillAttribute(ev, "delegator")
	if delegatorRaw == "" {
		if b.dryRun {
			b.t.Logf("  [%d] No delegator attribute found", height)
		}
		return nil
	}

	amountStr := getBackfillAttribute(ev, "amount")
	if amountStr == "" {
		if b.dryRun {
			b.t.Logf("  [%d] No amount attribute found", height)
		}
		return nil
	}

	// Debug: print raw hex of delegator
	if b.dryRun {
		rawBytes := []byte(delegatorRaw)
		b.t.Logf("  [%d] Delegator raw hex (len=%d): %x", height, len(rawBytes), rawBytes)
	}

	// Parse delegator address - try multiple methods
	reporter := parseReporterAddress(delegatorRaw)
	if reporter == "" {
		// Always log failed parses with amount info for debugging
		atomic.AddInt64(&b.failedParseCount, 1)
		b.t.Logf("  [%d] FAILED PARSE: delegator len=%d, amount=%s (total failed: %d)",
			height, len([]byte(delegatorRaw)), amountStr, atomic.LoadInt64(&b.failedParseCount))
		return nil
	}

	// Filter by reporter if specified
	if b.filterReporter != "" && reporter != b.filterReporter {
		return nil
	}

	if b.dryRun {
		b.t.Logf("  [%d] Parsed reporter: %s", height, reporter)
	}

	// Parse current cumulative amount
	currentAmount, err := parseBackfillAmount(amountStr)
	if err != nil {
		if b.dryRun {
			b.t.Logf("  [%d] Failed to parse amount: %s, err: %v", height, amountStr, err)
		}
		return nil
	}

	// Threshold constants using decimal
	oneMillion := decimal.NewFromInt(1_000_000)

	// Get previous cumulative amount from cache
	prev, hasPrev := b.rewardCumulative.Load(reporter)

	// First event for this reporter
	if !hasPrev {
		b.rewardCumulative.Store(reporter, currentAmount)
		b.updateLastProcessedHeight(reporter, height)

		// Log for debugging
		if b.dryRun {
			b.t.Logf("  [%d] %s: FIRST cumulative=%s (raw: %s)", height, truncateAddr(reporter), currentAmount.String(), amountStr)
		}

		// If cumulative < 1 TRB (1_000_000 loya), assume it's the true first reward
		// and insert it. Otherwise skip as baseline from before backfill range.
		if currentAmount.LessThan(oneMillion) {
			return &rewardRecord{
				BlockHeight: height,
				BlockTime:   blockTime,
				Sender:      reporter,
				Recipient:   reporter, // Reporter receives their own tip
				Amount:      currentAmount,
				RewardType:  blockdb.RewardTypeReporterTip,
			}
		}
		if b.dryRun {
			b.t.Logf("  [%d] %s: FIRST but cumulative %s >= 1M, skipping as baseline", height, truncateAddr(reporter), currentAmount.String())
		}
		return nil
	}

	prevAmount := prev.(decimal.Decimal)

	// Check if a withdrawal happened for this reporter that we need to account for
	// This handles the case where baseline was from before a withdrawal
	withdrawalInBetween := b.hasWithdrawalBetween(reporter, height)

	// Calculate increment
	increment := currentAmount.Sub(prevAmount)

	// Log for debugging
	if b.dryRun {
		b.t.Logf("  [%d] %s: prev=%s curr=%s incr=%s withdrawal_between=%v (raw: %s)",
			height, truncateAddr(reporter), prevAmount.String(), currentAmount.String(), increment.String(), withdrawalInBetween, amountStr)
	}

	// Handle withdrawal detection and post-withdrawal events
	// Only reset increment when cumulative actually dropped (negative) or withdrawal in between
	// For withdrawal blocks with positive increment, the previousCumulative was already
	// correctly set from an earlier event in this block, so use the actual increment
	if increment.LessThanOrEqual(decimal.Zero) || withdrawalInBetween {
		// Check if this is truly a withdrawal event (cumulative dropped to essentially 0)
		// Use tiny threshold (< 0.0001 loya) to skip only near-zero amounts
		// Post-withdrawal EndBlock rewards (typically 0.1-1 loya) should still be inserted
		// for exact matching with chain tips
		tinyThreshold := decimal.NewFromFloat(0.0001)
		if currentAmount.LessThan(tinyThreshold) {
			// True withdrawal with near-zero cumulative - no meaningful reward to insert
			if b.dryRun {
				b.t.Logf("  [%d] %s: cumulative %s < 0.0001 loya, skipping", height, truncateAddr(reporter), currentAmount.String())
			}
			b.rewardCumulative.Store(reporter, currentAmount)
			b.updateLastProcessedHeight(reporter, height)
			return nil
		}

		// This is a reward after withdrawal - use currentAmount as increment
		// (because baseline was reset to 0 after withdrawal)
		if b.dryRun {
			if withdrawalInBetween {
				b.t.Logf("  [%d] %s: WITHDRAWAL BETWEEN - using cumulative %s as increment",
					height, truncateAddr(reporter), currentAmount.String())
			} else {
				b.t.Logf("  [%d] %s: negative incr=%s, post-withdrawal event - using cumulative %s as increment",
					height, truncateAddr(reporter), increment.String(), currentAmount.String())
			}
		}
		if withdrawalInBetween {
			atomic.AddInt64(&b.withdrawalResetCount, 1)
		}
		increment = currentAmount
	}

	// Update cache and last processed height
	b.rewardCumulative.Store(reporter, currentAmount)
	b.updateLastProcessedHeight(reporter, height)

	return &rewardRecord{
		BlockHeight: height,
		BlockTime:   blockTime,
		Sender:      reporter,
		Recipient:   reporter, // Reporter receives their own tip
		Amount:      increment,
		RewardType:  blockdb.RewardTypeReporterTip,
	}
}

// insertRewards inserts reward records to ClickHouse using batch INSERT.
// Uses the unified rewards table schema.
func (b *rewardsBackfiller) insertRewards(ctx context.Context, rewards []rewardRecord) error {
	if len(rewards) == 0 {
		return nil
	}

	// Build batch INSERT for better performance
	// ClickHouse is optimized for bulk inserts
	const batchSize = 1000
	for i := 0; i < len(rewards); i += batchSize {
		end := i + batchSize
		if end > len(rewards) {
			end = len(rewards)
		}
		batch := rewards[i:end]

		// Build VALUES clause for unified rewards table
		var values []string
		var args []any
		for _, r := range batch {
			values = append(values, "(?, ?, ?, ?, ?, ?)")
			args = append(args, r.BlockHeight, r.BlockTime, r.Sender, r.Recipient, r.Amount.String(), r.RewardType)
		}

		query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES %s", //nolint:gosec // G201: table name is constant
			blockdb.TableNameRewards,
			blockdb.ColBlockHeight, blockdb.ColBlockTime, blockdb.ColSender, blockdb.ColRecipient, blockdb.ColAmount, blockdb.ColType,
			strings.Join(values, ", "))

		if _, err := b.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("batch insert %d records: %w", len(batch), err)
		}
	}

	return nil
}

// getBackfillAttribute extracts an attribute value from an ABCI event.
func getBackfillAttribute(ev abci.Event, key string) string {
	for _, attr := range ev.Attributes {
		if attr.Key == key {
			return attr.Value
		}
	}
	return ""
}

// parseReporterAddress tries multiple methods to parse a reporter address.
// Returns empty string if all methods fail.
func parseReporterAddress(raw string) string {
	// Method 1: Try as bech32 directly
	if addr, err := sdk.AccAddressFromBech32(raw); err == nil {
		return addr.String()
	}

	// Method 2: Try base64 decode then as raw bytes
	if decoded, err := base64.StdEncoding.DecodeString(raw); err == nil {
		if len(decoded) == 20 {
			return sdk.AccAddress(decoded).String()
		}
	}

	// Method 3: Try as raw bytes directly (if string is 20 bytes)
	rawBytes := []byte(raw)
	if len(rawBytes) == 20 {
		return sdk.AccAddress(rawBytes).String()
	}

	// Method 4: Try to match against known reporters using partial bytes
	// When binary data is JSON-decoded as string, invalid UTF-8 bytes become U+FFFD (replacement char)
	// We extract valid bytes and try to match against known reporter addresses
	if matched := matchKnownReporter(raw); matched != "" {
		return matched
	}

	// Method 5: Try hex decode (in case the address is hex-encoded)
	if len(raw)%2 == 0 && len(raw) >= 40 {
		if decoded, err := hex.DecodeString(raw); err == nil && len(decoded) == 20 {
			return sdk.AccAddress(decoded).String()
		}
	}

	return ""
}

// knownReporterAddresses is a cache of known reporter bech32 addresses.
// This is populated from the database at startup and used for matching
// corrupted binary addresses from RPC responses.
var knownReporterAddresses = map[string]string{
	// Format: partial_hex_key -> bech32_address
	// Will be populated dynamically
}

// initKnownReporters populates the known reporters cache from database.
func initKnownReporters(db *sql.DB) error {
	query := fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s != ''", blockdb.ColReporter, blockdb.TableNameReports, blockdb.ColReporter) //nolint:gosec // G201: table name is constant
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			continue
		}
		// Decode bech32 to get raw bytes, create lookup key
		if accAddr, err := sdk.AccAddressFromBech32(addr); err == nil {
			rawBytes := []byte(accAddr)
			// Create multiple keys for partial matching - use EVERY 8-byte window
			for i := 0; i <= len(rawBytes)-8; i++ {
				key := fmt.Sprintf("%x", rawBytes[i:i+8])
				knownReporterAddresses[key] = addr
			}
			// Also store full address as key
			fullKey := fmt.Sprintf("%x", rawBytes)
			knownReporterAddresses[fullKey] = addr
		}
	}
	return nil
}

// matchKnownReporter tries to match corrupted binary data to a known reporter.
// Debug: logs extracted bytes for troubleshooting.
func matchKnownReporter(raw string) string {
	// Extract ALL non-replacement bytes
	var allBytes []byte
	var replacementCount int
	for _, r := range raw {
		if r == '\uFFFD' {
			replacementCount++
		} else if r < 256 {
			allBytes = append(allBytes, byte(r))
		}
	}

	// Debug: uncomment to see what's being extracted
	// fmt.Printf("DEBUG matchKnownReporter: %d valid bytes, %d replacements, hex=%x\n",
	//     len(allBytes), replacementCount, allBytes)

	// Try matching with different key lengths and positions
	// The corruption might affect the first few bytes, so try multiple strategies

	// Strategy 1: Use first 8 valid bytes
	if len(allBytes) >= 8 {
		key := fmt.Sprintf("%x", allBytes[:8])
		if addr, ok := knownReporterAddresses[key]; ok {
			return addr
		}
	}

	// Strategy 2: Try matching any 8-byte subsequence
	for i := 0; i <= len(allBytes)-8; i++ {
		key := fmt.Sprintf("%x", allBytes[i:i+8])
		if addr, ok := knownReporterAddresses[key]; ok {
			return addr
		}
	}

	// Strategy 3: If we have exactly 20 bytes, it might be the full address
	if len(allBytes) == 20 {
		return sdk.AccAddress(allBytes).String()
	}

	// Strategy 4: Try full hex match
	if len(allBytes) > 0 {
		fullKey := fmt.Sprintf("%x", allBytes)
		if addr, ok := knownReporterAddresses[fullKey]; ok {
			return addr
		}
	}

	return ""
}

// parseBackfillAmount parses a decimal amount string to decimal.Decimal.
// Uses arbitrary precision to match chain's 18-decimal sdk.Dec tracking.
func parseBackfillAmount(amountStr string) (decimal.Decimal, error) {
	// Remove common suffixes
	cleaned := strings.TrimSuffix(amountStr, "loya")
	cleaned = strings.TrimSpace(cleaned)

	return decimal.NewFromString(cleaned)
}

// truncateAddr truncates an address for display.
func truncateAddr(addr string) string {
	if len(addr) > 20 {
		return addr[:12] + "..." + addr[len(addr)-6:]
	}
	return addr
}

// connectClickHouseWritable establishes a writable connection to ClickHouse for backfill operations.
func connectClickHouseWritable(t *testing.T) *sql.DB {
	t.Helper()

	host := getEnvOrDefault("CLICKHOUSE_HOST", "localhost")
	port := getEnvOrDefault("CLICKHOUSE_PORT", "9000")
	user := getEnvOrDefault("CLICKHOUSE_USER", "default")
	password := getEnvOrDefault("CLICKHOUSE_PASSWORD", "")
	database := getEnvOrDefault("CLICKHOUSE_DB", "default")

	// Connect WITHOUT readonly=1 to allow write operations
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s",
		user, password, host, port, database)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("failed to connect to ClickHouse: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("ClickHouse not available: %v", err)
	}

	t.Log("Connected to ClickHouse (writable mode)")
	return db
}

// TestBackfillBlockSigns runs a backfill of block signing data from a block range.
// This extracts validator signatures from block commits.
//
// Usage:
//
//	go test -v ./cryptoriums/tools/... -run TestBackfillBlockSigns \
//	  -args -env=/root/.layer/.env \
//	  -backfill-start=13942861 \
//	  -backfill-end=14145023
//
// Dry run (no DB writes):
//
//	go test -v ./cryptoriums/tools/... -run TestBackfillBlockSigns \
//	  -args -backfill-start=13942861 -backfill-end=13943000 -backfill-dry-run
func TestBackfillBlockSigns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping backfill in short mode")
	}

	// Validate required flags
	if *backfillStart == 0 || *backfillEnd == 0 {
		t.Skip("skipping: -backfill-start and -backfill-end flags are required")
	}
	if *backfillStart > *backfillEnd {
		t.Fatal("Start block must be <= end block")
	}

	// Load environment from file
	if *envFile != "" {
		if err := godotenv.Load(*envFile); err != nil {
			t.Logf("Warning: could not load env file %s: %v", *envFile, err)
		} else {
			t.Logf("Loaded env from %s", *envFile)
		}
	}

	ctx := context.Background()

	// Resolve RPC URL from flag or environment
	rpcURL := *backfillRPC
	if rpcURL == "" {
		rpcURLs := parseRPCURLs()
		if len(rpcURLs) == 0 {
			t.Fatal("LAYER_RPC_URLS not set and -backfill-rpc not provided")
		}
		rpcURL = rpcURLs[0]
	}

	t.Logf("Block Signs Backfill Configuration:")
	t.Logf("  Block range: %d to %d (%d blocks)", *backfillStart, *backfillEnd, *backfillEnd-*backfillStart+1)
	t.Logf("  RPC: %s", rpcURL)
	t.Logf("  Workers: %d", *backfillWorkers)
	t.Logf("  Dry run: %v", *backfillDryRun)

	// Connect to RPC
	client, err := http.New(rpcURL, "/websocket")
	if err != nil {
		t.Fatalf("Failed to connect to RPC: %v", err)
	}
	t.Logf("Connected to RPC: %s", rpcURL)

	// Connect to ClickHouse (unless dry run)
	var db *sql.DB
	if !*backfillDryRun {
		db = connectClickHouseWritable(t)
		defer db.Close()

		// Ensure tables exist
		wrapped := blockdb.SQLDB{DB: db}
		if err := blockdb.EnsureTables(ctx, wrapped); err != nil {
			t.Fatalf("Failed to ensure tables: %v", err)
		}
	}

	// Run backfill
	backfiller := &blockSignsBackfiller{
		t:       t,
		client:  client,
		db:      db,
		workers: *backfillWorkers,
		dryRun:  *backfillDryRun,
	}

	if err := backfiller.run(ctx, *backfillStart, *backfillEnd); err != nil {
		t.Fatalf("Backfill failed: %v", err)
	}

	t.Log("Block signs backfill completed successfully")
}

// blockSignsBackfiller handles the block signs backfill process.
type blockSignsBackfiller struct {
	t       *testing.T
	client  *http.HTTP
	db      *sql.DB
	workers int
	dryRun  bool
}

// blockSignRecord represents a block sign record to be inserted.
type blockSignRecord struct {
	BlockHeight      int64
	BlockTimestamp   time.Time
	ValidatorAddress string
	Signed           uint8
}

// run executes the block signs backfill for the given block range.
func (b *blockSignsBackfiller) run(ctx context.Context, start, end int64) error {
	totalBlocks := end - start + 1
	processedBlocks := int64(0)
	var signs []blockSignRecord
	var totalSignsInserted int

	startTime := time.Now()
	lastLogTime := startTime

	// Process blocks in batches
	for batchStart := start; batchStart <= end; batchStart += int64(b.workers) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchEnd := batchStart + int64(b.workers) - 1
		if batchEnd > end {
			batchEnd = end
		}

		// Fetch blocks in parallel
		blocks, err := b.fetchBlocksParallel(ctx, batchStart, batchEnd)
		if err != nil {
			return fmt.Errorf("fetch blocks %d-%d: %w", batchStart, batchEnd, err)
		}

		// Process blocks in order
		for height := batchStart; height <= batchEnd; height++ {
			block, ok := blocks[height]
			if !ok {
				b.t.Logf("Warning: missing block %d", height)
				continue
			}

			// Extract block signs from LastCommit
			blockSigns := b.extractBlockSigns(block)
			signs = append(signs, blockSigns...)

			processedBlocks++
		}

		// Insert batch if enough records (every 1000 signs)
		if len(signs) >= 1000 && !b.dryRun {
			if err := b.insertBlockSigns(ctx, signs); err != nil {
				return fmt.Errorf("insert block signs: %w", err)
			}
			totalSignsInserted += len(signs)
			signs = signs[:0]
		}

		// Log progress every 5 seconds
		if time.Since(lastLogTime) > 5*time.Second {
			elapsed := time.Since(startTime)
			blocksPerSec := float64(processedBlocks) / elapsed.Seconds()
			remaining := float64(totalBlocks-processedBlocks) / blocksPerSec
			b.t.Logf("Progress: %d/%d blocks (%.1f%%) | %.1f blocks/sec | %d signs | ETA: %s",
				processedBlocks, totalBlocks,
				float64(processedBlocks)/float64(totalBlocks)*100,
				blocksPerSec,
				totalSignsInserted+len(signs),
				time.Duration(remaining*float64(time.Second)).Round(time.Second))
			lastLogTime = time.Now()
		}
	}

	// Insert remaining signs
	if len(signs) > 0 && !b.dryRun {
		if err := b.insertBlockSigns(ctx, signs); err != nil {
			return fmt.Errorf("insert final block signs: %w", err)
		}
		totalSignsInserted += len(signs)
	}

	elapsed := time.Since(startTime)
	b.t.Logf("Completed: %d blocks in %s (%.1f blocks/sec), %d block signs inserted",
		processedBlocks, elapsed.Round(time.Second), float64(processedBlocks)/elapsed.Seconds(), totalSignsInserted)

	return nil
}

// fetchBlocksParallel fetches multiple blocks concurrently.
func (b *blockSignsBackfiller) fetchBlocksParallel(ctx context.Context, start, end int64) (map[int64]*ctypes.ResultBlock, error) {
	results := make(map[int64]*ctypes.ResultBlock)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, end-start+1)

	for height := start; height <= end; height++ {
		wg.Add(1)
		go func(h int64) {
			defer wg.Done()

			block, err := b.client.Block(ctx, &h)
			if err != nil {
				errCh <- fmt.Errorf("block %d: %w", h, err)
				return
			}

			mu.Lock()
			results[h] = block
			mu.Unlock()
		}(height)
	}

	wg.Wait()
	close(errCh)

	// Return first error if any
	for err := range errCh {
		return nil, err
	}

	return results, nil
}

// extractBlockSigns extracts validator signatures from block's LastCommit.
// Note: LastCommit contains signatures for the PREVIOUS block (height-1).
func (b *blockSignsBackfiller) extractBlockSigns(block *ctypes.ResultBlock) []blockSignRecord {
	if block.Block == nil || block.Block.LastCommit == nil {
		return nil
	}

	currentHeight := block.Block.Height
	if currentHeight <= 1 {
		// Genesis block has no previous block signatures
		return nil
	}

	// LastCommit contains signatures for block at height-1
	commitHeight := currentHeight - 1
	blockTime := block.Block.Time

	var records []blockSignRecord

	for _, sig := range block.Block.LastCommit.Signatures {
		// Skip empty signatures (validator not in set at that height)
		if len(sig.ValidatorAddress) == 0 {
			continue
		}

		// Determine signed status based on BlockIDFlag:
		// - BlockIDFlagCommit: validator signed the block (signed = 1)
		// - BlockIDFlagNil/BlockIDFlagAbsent: validator missed (signed = 0)
		var signed uint8
		if sig.BlockIDFlag == cmttypes.BlockIDFlagCommit {
			signed = 1
		}

		// Convert to bech32 consensus address format
		validatorAddr := sdk.ConsAddress(sig.ValidatorAddress).String()

		records = append(records, blockSignRecord{
			BlockHeight:      commitHeight,
			BlockTimestamp:   blockTime,
			ValidatorAddress: validatorAddr,
			Signed:           signed,
		})
	}

	return records
}

// insertBlockSigns inserts block sign records to ClickHouse.
func (b *blockSignsBackfiller) insertBlockSigns(ctx context.Context, signs []blockSignRecord) error {
	if len(signs) == 0 {
		return nil
	}

	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)", //nolint:gosec // G201: table name is constant
		blockdb.TableNameBlockSigns,
		blockdb.ColBlockHeight, blockdb.ColBlockTimestamp, blockdb.ColValidatorAddress, blockdb.ColSigned)

	for _, s := range signs {
		if _, err := b.db.ExecContext(ctx, query, s.BlockHeight, s.BlockTimestamp, s.ValidatorAddress, s.Signed); err != nil {
			return err
		}
	}

	return nil
}

// fetchWithdrawalsInRange fetches all withdrawal events in the given block range.
// Returns withdrawals sorted by height ascending.
func fetchWithdrawalsInRange(ctx context.Context, t *testing.T, apiURL string, minHeight, maxHeight int64) []backfillWithdrawal {
	t.Helper()

	client := &nethttp.Client{Timeout: 60 * time.Second}

	// Query withdrawals with pagination
	var allWithdrawals []backfillWithdrawal
	pageKey := ""

	for {
		query := fmt.Sprintf("message.action='/layer.reporter.MsgWithdrawTip' AND tx.height>%d AND tx.height<%d", minHeight, maxHeight)
		url := fmt.Sprintf("%s/cosmos/tx/v1beta1/txs?query=%s&pagination.limit=100&order_by=ORDER_BY_ASC",
			apiURL, strings.ReplaceAll(query, " ", "%20"))
		if pageKey != "" {
			url += "&pagination.key=" + pageKey
		}

		req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodGet, url, nil)
		if err != nil {
			t.Logf("Warning: failed to create withdrawals request: %v", err)
			return allWithdrawals
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Logf("Warning: failed to fetch withdrawals: %v", err)
			return allWithdrawals
		}

		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != nethttp.StatusOK {
			t.Logf("Warning: withdrawals API returned status %d", resp.StatusCode)
			return allWithdrawals
		}

		var result struct {
			TxResponses []struct {
				Height string `json:"height"`
				Tx     struct {
					Body struct {
						Messages []struct {
							SelectorAddress string `json:"selector_address"`
						} `json:"messages"`
					} `json:"body"`
				} `json:"tx"`
			} `json:"tx_responses"`
			Pagination struct {
				NextKey string `json:"next_key"`
			} `json:"pagination"`
		}

		if err := json.Unmarshal(body, &result); err != nil {
			t.Logf("Warning: failed to parse withdrawals response: %v", err)
			return allWithdrawals
		}

		for _, tx := range result.TxResponses {
			var height int64
			_, _ = fmt.Sscanf(tx.Height, "%d", &height)

			var reporter string
			if len(tx.Tx.Body.Messages) > 0 {
				reporter = tx.Tx.Body.Messages[0].SelectorAddress
			}

			if reporter != "" && height > 0 {
				allWithdrawals = append(allWithdrawals, backfillWithdrawal{
					Height:   height,
					Reporter: reporter,
				})
			}
		}

		// Check if there are more pages
		if result.Pagination.NextKey == "" || len(result.TxResponses) == 0 {
			break
		}
		pageKey = result.Pagination.NextKey
	}

	// Sort by height ascending
	sort.Slice(allWithdrawals, func(i, j int) bool {
		return allWithdrawals[i].Height < allWithdrawals[j].Height
	})

	return allWithdrawals
}

// groupWithdrawalsByReporter groups withdrawals by reporter address.
func groupWithdrawalsByReporter(withdrawals []backfillWithdrawal) map[string][]int64 {
	result := make(map[string][]int64)
	for _, w := range withdrawals {
		result[w.Reporter] = append(result[w.Reporter], w.Height)
	}
	// Sort heights for each reporter
	for reporter := range result {
		sort.Slice(result[reporter], func(i, j int) bool {
			return result[reporter][i] < result[reporter][j]
		})
	}
	return result
}

// ============================================================================
// TestBackfillAll - Unified backfill for all tables
// ============================================================================

// TestBackfillAll runs backfill for all tables: rewards, block_signs, reports, txs, validator_rewards_allocation.
//
// Usage:
//
//	go test -v ./cryptoriums/tools/... -run TestBackfillAll \
//	  -args -env=/root/.layer/.env \
//	  -backfill-start=14542053 \
//	  -backfill-end=14592053
func TestBackfillAll(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping backfill in short mode")
	}

	if *backfillStart == 0 || *backfillEnd == 0 {
		t.Skip("skipping: -backfill-start and -backfill-end flags are required")
	}
	if *backfillStart > *backfillEnd {
		t.Fatal("Start block must be <= end block")
	}

	if *envFile != "" {
		if err := godotenv.Load(*envFile); err != nil {
			t.Logf("Warning: could not load env file %s: %v", *envFile, err)
		}
	}

	ctx := context.Background()

	// Resolve RPC and API URLs from flags or environment
	rpcURL := *backfillRPC
	if rpcURL == "" {
		rpcURLs := parseRPCURLs()
		if len(rpcURLs) == 0 {
			t.Fatal("LAYER_RPC_URLS not set and -backfill-rpc not provided")
		}
		rpcURL = rpcURLs[0]
	}
	apiURL := *backfillAPI
	if apiURL == "" {
		apiURLs := parseAPIURLs()
		if len(apiURLs) == 0 {
			t.Fatal("LAYER_API_URLS not set and -backfill-api not provided")
		}
		apiURL = apiURLs[0]
	}

	t.Logf("========== BACKFILL ALL TABLES ==========")
	t.Logf("Block range: %d to %d (%d blocks)", *backfillStart, *backfillEnd, *backfillEnd-*backfillStart+1)
	t.Logf("RPC: %s", rpcURL)
	t.Logf("API: %s", apiURL)
	t.Logf("Workers: %d", *backfillWorkers)
	t.Logf("Dry run: %v", *backfillDryRun)

	// Connect to RPC
	client, err := http.New(rpcURL, "/websocket")
	if err != nil {
		t.Fatalf("Failed to connect to RPC: %v", err)
	}

	// Connect to ClickHouse
	var db *sql.DB
	if !*backfillDryRun {
		db = connectClickHouseWritable(t)
		defer db.Close()

		wrapped := blockdb.SQLDB{DB: db}
		if err := blockdb.EnsureTables(ctx, wrapped); err != nil {
			t.Fatalf("Failed to ensure tables: %v", err)
		}
	}

	// Fetch withdrawals for rewards backfill
	t.Log("Fetching withdrawal events...")
	withdrawals := fetchWithdrawalsInRange(ctx, t, apiURL, *backfillStart-1, *backfillEnd+1)
	reporterWithdrawals := groupWithdrawalsByReporter(withdrawals)
	t.Logf("Found %d withdrawals from %d reporters", len(withdrawals), len(reporterWithdrawals))

	// Create unified backfiller
	backfiller := &allTablesBackfiller{
		t:                   t,
		client:              client,
		db:                  db,
		workers:             *backfillWorkers,
		dryRun:              *backfillDryRun,
		reporterWithdrawals: reporterWithdrawals,
		rewardCumulative:    sync.Map{},
		validatorCumulative: sync.Map{},
	}

	// Pre-populate baseline if specified
	if *backfillBaselineBlock > 0 {
		if err := backfiller.fetchBaselines(ctx, *backfillBaselineBlock); err != nil {
			t.Fatalf("Failed to fetch baselines: %v", err)
		}
	}

	if err := backfiller.run(ctx, *backfillStart, *backfillEnd); err != nil {
		t.Fatalf("Backfill failed: %v", err)
	}

	t.Log("========== BACKFILL ALL COMPLETED ==========")
}

// allTablesBackfiller handles backfill for all tables.
type allTablesBackfiller struct {
	t                   *testing.T
	client              *http.HTTP
	db                  *sql.DB
	workers             int
	dryRun              bool
	reporterWithdrawals map[string][]int64
	rewardCumulative    sync.Map
	validatorCumulative sync.Map
	lastProcessedHeight sync.Map
}

// Counters for summary
type backfillStats struct {
	rewards    int // All reward types (reporter_tip, validator_commission, validator_delegator)
	reports    int
	txs        int
	blockSigns int
}

func (b *allTablesBackfiller) run(ctx context.Context, start, end int64) error {
	totalBlocks := end - start + 1
	processedBlocks := int64(0)
	stats := &backfillStats{}

	// Buffers for batch insert (unified rewards table includes all reward types)
	var rewardBuf []rewardRecord
	var reportBuf []reportRecord
	var txBuf []txRecord
	var blockSignBuf []blockSignRecord

	startTime := time.Now()
	lastLogTime := startTime

	for batchStart := start; batchStart <= end; batchStart += int64(b.workers) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchEnd := batchStart + int64(b.workers) - 1
		if batchEnd > end {
			batchEnd = end
		}

		// Fetch block data and results in parallel
		blockData, blockResults, err := b.fetchBlocksAndResultsParallel(ctx, batchStart, batchEnd)
		if err != nil {
			return fmt.Errorf("fetch blocks %d-%d: %w", batchStart, batchEnd, err)
		}

		// Process blocks in order
		for height := batchStart; height <= batchEnd; height++ {
			block := blockData[height]
			results := blockResults[height]

			if block == nil || results == nil {
				b.t.Logf("Warning: missing block %d", height)
				continue
			}

			// Extract all data from block (unified rewards table)
			rewards, reports, txs, signs := b.extractAllData(height, block, results)

			rewardBuf = append(rewardBuf, rewards...)
			reportBuf = append(reportBuf, reports...)
			txBuf = append(txBuf, txs...)
			blockSignBuf = append(blockSignBuf, signs...)

			processedBlocks++
		}

		// Flush buffers when they reach threshold
		if !b.dryRun {
			if len(rewardBuf) >= 1000 {
				if err := b.insertRewards(ctx, rewardBuf); err != nil {
					return err
				}
				stats.rewards += len(rewardBuf)
				rewardBuf = rewardBuf[:0]
			}
			if len(reportBuf) >= 1000 {
				if err := b.insertReports(ctx, reportBuf); err != nil {
					return err
				}
				stats.reports += len(reportBuf)
				reportBuf = reportBuf[:0]
			}
			if len(txBuf) >= 1000 {
				if err := b.insertTxs(ctx, txBuf); err != nil {
					return err
				}
				stats.txs += len(txBuf)
				txBuf = txBuf[:0]
			}
			if len(blockSignBuf) >= 1000 {
				if err := b.insertBlockSigns(ctx, blockSignBuf); err != nil {
					return err
				}
				stats.blockSigns += len(blockSignBuf)
				blockSignBuf = blockSignBuf[:0]
			}
		}

		// Log progress
		if time.Since(lastLogTime) > 5*time.Second {
			elapsed := time.Since(startTime)
			blocksPerSec := float64(processedBlocks) / elapsed.Seconds()
			remaining := float64(totalBlocks-processedBlocks) / blocksPerSec
			b.t.Logf("Progress: %d/%d blocks (%.1f%%) | %.1f blocks/sec | ETA: %s",
				processedBlocks, totalBlocks,
				float64(processedBlocks)/float64(totalBlocks)*100,
				blocksPerSec,
				time.Duration(remaining*float64(time.Second)).Round(time.Second))
			b.t.Logf("  Pending: rewards=%d reports=%d txs=%d signs=%d",
				len(rewardBuf), len(reportBuf), len(txBuf), len(blockSignBuf))
			lastLogTime = time.Now()
		}
	}

	// Flush remaining buffers
	if !b.dryRun {
		if len(rewardBuf) > 0 {
			if err := b.insertRewards(ctx, rewardBuf); err != nil {
				return err
			}
			stats.rewards += len(rewardBuf)
		}
		if len(reportBuf) > 0 {
			if err := b.insertReports(ctx, reportBuf); err != nil {
				return err
			}
			stats.reports += len(reportBuf)
		}
		if len(txBuf) > 0 {
			if err := b.insertTxs(ctx, txBuf); err != nil {
				return err
			}
			stats.txs += len(txBuf)
		}
		if len(blockSignBuf) > 0 {
			if err := b.insertBlockSigns(ctx, blockSignBuf); err != nil {
				return err
			}
			stats.blockSigns += len(blockSignBuf)
		}
	}

	elapsed := time.Since(startTime)
	b.t.Logf("Completed: %d blocks in %s (%.1f blocks/sec)", processedBlocks, elapsed.Round(time.Second), float64(processedBlocks)/elapsed.Seconds())
	b.t.Logf("Inserted: rewards=%d reports=%d txs=%d blockSigns=%d",
		stats.rewards, stats.reports, stats.txs, stats.blockSigns)

	return nil
}

func (b *allTablesBackfiller) fetchBlocksAndResultsParallel(ctx context.Context, start, end int64) (map[int64]*ctypes.ResultBlock, map[int64]*ctypes.ResultBlockResults, error) {
	blocks := make(map[int64]*ctypes.ResultBlock)
	results := make(map[int64]*ctypes.ResultBlockResults)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, (end-start+1)*2)

	// Semaphore to limit concurrent requests
	sem := make(chan struct{}, b.workers)

	for height := start; height <= end; height++ {
		wg.Add(2)
		go func(h int64) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			block, err := b.fetchBlockWithRetry(ctx, h)
			if err != nil {
				errCh <- fmt.Errorf("block %d: %w", h, err)
				return
			}
			mu.Lock()
			blocks[h] = block
			mu.Unlock()
		}(height)
		go func(h int64) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			res, err := b.fetchBlockResultsWithRetry(ctx, h)
			if err != nil {
				errCh <- fmt.Errorf("block results %d: %w", h, err)
				return
			}
			mu.Lock()
			results[h] = res
			mu.Unlock()
		}(height)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return nil, nil, err
	}

	return blocks, results, nil
}

func (b *allTablesBackfiller) fetchBlockWithRetry(ctx context.Context, height int64) (*ctypes.ResultBlock, error) {
	const maxRetries = 5
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		block, err := b.client.Block(ctx, &height)
		if err == nil {
			return block, nil
		}
		lastErr = err

		// Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms
		backoff := time.Duration(100<<i) * time.Millisecond
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
	return nil, lastErr
}

func (b *allTablesBackfiller) fetchBlockResultsWithRetry(ctx context.Context, height int64) (*ctypes.ResultBlockResults, error) {
	const maxRetries = 5
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		res, err := b.client.BlockResults(ctx, &height)
		if err == nil {
			return res, nil
		}
		lastErr = err

		// Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms
		backoff := time.Duration(100<<i) * time.Millisecond
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
	return nil, lastErr
}

// Record types for batch insert
type reportRecord struct {
	Reporter        string
	Power           uint64
	QueryType       string
	QueryID         string
	AggregateMethod string
	Value           string
	Timestamp       time.Time
	Cyclelist       uint8
	BlockNumber     uint64
	MetaID          uint64
}

type txRecord struct {
	BlockHeight int64
	TxHash      string
	Sender      string
	GasUsed     int64
	FeeAmount   string
}

// validatorRewardRecord is deprecated - use rewardRecord with appropriate RewardType instead.
// Keeping for backward compatibility during migration, will be removed.

func (b *allTablesBackfiller) extractAllData(height int64, block *ctypes.ResultBlock, results *ctypes.ResultBlockResults) (
	rewards []rewardRecord,
	reports []reportRecord,
	txs []txRecord,
	signs []blockSignRecord,
) {
	// Extract block signs from LastCommit
	if block.Block != nil && block.Block.LastCommit != nil && height > 1 {
		commitHeight := height - 1
		blockTime := block.Block.Time
		for _, sig := range block.Block.LastCommit.Signatures {
			if len(sig.ValidatorAddress) == 0 {
				continue
			}
			var signed uint8
			if sig.BlockIDFlag == cmttypes.BlockIDFlagCommit {
				signed = 1
			}
			signs = append(signs, blockSignRecord{
				BlockHeight:      commitHeight,
				BlockTimestamp:   blockTime,
				ValidatorAddress: sdk.ConsAddress(sig.ValidatorAddress).String(),
				Signed:           signed,
			})
		}
	}

	// Extract transactions
	if block.Block != nil && results != nil {
		txDecoder := newBackfillTxDecoder()
		for i, rawTx := range block.Block.Txs {
			if i >= len(results.TxsResults) {
				break
			}
			txRes := results.TxsResults[i]

			// Skip vote extension txs
			if _, ok := parseBackfillVoteExtTx(rawTx); ok {
				continue
			}

			tx, err := txDecoder(rawTx)
			if err != nil {
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

			sender := findBackfillSender(tx, txRes.GetEvents())

			txs = append(txs, txRecord{
				BlockHeight: height,
				TxHash:      fmt.Sprintf("%X", rawTx.Hash()),
				Sender:      sender,
				GasUsed:     txRes.GasUsed,
				FeeAmount:   feeCoins[0].Amount.String(),
			})
		}
	}

	// Combine all events
	var allEvents []abci.Event
	if results != nil {
		allEvents = append(allEvents, results.FinalizeBlockEvents...)
		for _, txRes := range results.TxsResults {
			if txRes != nil {
				allEvents = append(allEvents, txRes.Events...)
			}
		}
	}

	// Get block time for rewards
	var blockTime time.Time
	if block.Block != nil {
		blockTime = block.Block.Time
	}

	// Process events
	for _, ev := range allEvents {
		switch ev.Type {
		case "new_report":
			if report := b.extractReport(height, ev); report != nil {
				reports = append(reports, *report)
			}

		case eventTypeRewardsAdded:
			if reward := b.extractReporterReward(height, blockTime, ev); reward != nil {
				rewards = append(rewards, *reward)
			}

		case "commission":
			if vr := b.extractValidatorRewardUnified(height, blockTime, ev, blockdb.RewardTypeValidatorCommission); vr != nil {
				rewards = append(rewards, *vr)
			}

		case "rewards":
			if vr := b.extractValidatorRewardUnified(height, blockTime, ev, blockdb.RewardTypeValidatorDelegator); vr != nil {
				rewards = append(rewards, *vr)
			}
		}
	}

	return rewards,
		reports,
		txs,
		signs
}

func (b *allTablesBackfiller) extractReport(height int64, ev abci.Event) *reportRecord {
	var r reportRecord

	for _, attr := range ev.Attributes {
		switch attr.Key {
		case processor.AttrKeyReporter:
			r.Reporter = attr.Value
		case processor.AttrKeyPower:
			if v, err := strconv.ParseUint(attr.Value, 10, 64); err == nil {
				r.Power = v
			}
		case processor.AttrKeyQueryType:
			r.QueryType = attr.Value
		case processor.AttrKeyQueryID:
			r.QueryID = attr.Value
		case processor.AttrKeyAggMethod:
			r.AggregateMethod = attr.Value
		case processor.AttrKeyValue:
			r.Value = attr.Value
		case processor.AttrKeyTimestamp:
			if ts, err := time.Parse(time.RFC3339Nano, attr.Value); err == nil {
				r.Timestamp = ts.UTC()
			} else if ms, err := strconv.ParseInt(attr.Value, 10, 64); err == nil {
				r.Timestamp = time.UnixMilli(ms).UTC()
			}
		case processor.AttrKeyCyclelist:
			if attr.Value == "true" {
				r.Cyclelist = 1
			}
		case processor.AttrKeyBlockNumber:
			if v, err := strconv.ParseUint(attr.Value, 10, 64); err == nil {
				r.BlockNumber = v
			}
		case processor.AttrKeyMetaID:
			if v, err := strconv.ParseUint(attr.Value, 10, 64); err == nil {
				r.MetaID = v
			}
		}
	}

	if r.BlockNumber == 0 && height > 0 {
		r.BlockNumber = uint64(height)
	}

	if r.Reporter == "" {
		return nil
	}

	return &r
}

// extractReporterReward extracts a reporter tip reward from a rewards_added event.
func (b *allTablesBackfiller) extractReporterReward(height int64, blockTime time.Time, ev abci.Event) *rewardRecord {
	delegatorRaw := getBackfillAttribute(ev, "delegator")
	if delegatorRaw == "" {
		return nil
	}
	amountStr := getBackfillAttribute(ev, "amount")
	if amountStr == "" {
		return nil
	}

	reporter := parseReporterAddress(delegatorRaw)
	if reporter == "" {
		return nil
	}

	currentAmount, err := parseBackfillAmount(amountStr)
	if err != nil {
		return nil
	}

	tinyThreshold := decimal.NewFromFloat(0.0001)
	prev, hasPrev := b.rewardCumulative.Load(reporter)

	if !hasPrev {
		b.rewardCumulative.Store(reporter, currentAmount)
		b.lastProcessedHeight.Store(reporter, height)
		return nil
	}

	prevAmount := prev.(decimal.Decimal)
	withdrawalInBetween := b.hasWithdrawalBetween(reporter, height)
	increment := currentAmount.Sub(prevAmount)

	if increment.LessThanOrEqual(decimal.Zero) || withdrawalInBetween {
		if currentAmount.LessThan(tinyThreshold) {
			b.rewardCumulative.Store(reporter, currentAmount)
			b.lastProcessedHeight.Store(reporter, height)
			return nil
		}
		increment = currentAmount
	}

	b.rewardCumulative.Store(reporter, currentAmount)
	b.lastProcessedHeight.Store(reporter, height)

	return &rewardRecord{
		BlockHeight: height,
		BlockTime:   blockTime,
		Sender:      reporter,
		Recipient:   reporter, // Reporter receives their own tip
		Amount:      increment,
		RewardType:  blockdb.RewardTypeReporterTip,
	}
}

// extractValidatorRewardUnified extracts a validator reward (commission or delegator) from an event.
func (b *allTablesBackfiller) extractValidatorRewardUnified(height int64, blockTime time.Time, ev abci.Event, rewardType string) *rewardRecord {
	validator := getBackfillAttribute(ev, "validator")
	if validator == "" {
		return nil
	}
	amountStr := getBackfillAttribute(ev, "amount")
	if amountStr == "" {
		return nil
	}

	currentAmount, err := parseBackfillAmount(amountStr)
	if err != nil {
		return nil
	}

	tinyThreshold := decimal.NewFromFloat(0.0001)
	cacheKey := validator + ":" + rewardType
	prev, hasPrev := b.validatorCumulative.Load(cacheKey)

	if !hasPrev {
		b.validatorCumulative.Store(cacheKey, currentAmount)
		return nil
	}

	prevAmount := prev.(decimal.Decimal)
	increment := currentAmount.Sub(prevAmount)

	if increment.LessThanOrEqual(decimal.Zero) {
		if currentAmount.LessThan(tinyThreshold) {
			b.validatorCumulative.Store(cacheKey, currentAmount)
			return nil
		}
		increment = currentAmount
	}

	b.validatorCumulative.Store(cacheKey, currentAmount)

	return &rewardRecord{
		BlockHeight: height,
		BlockTime:   blockTime,
		Sender:      validator,
		Recipient:   validator, // Validator receives from distribution
		Amount:      increment,
		RewardType:  rewardType,
	}
}

func (b *allTablesBackfiller) hasWithdrawalBetween(reporter string, currentHeight int64) bool {
	if b.reporterWithdrawals == nil {
		return false
	}
	withdrawals, exists := b.reporterWithdrawals[reporter]
	if !exists || len(withdrawals) == 0 {
		return false
	}
	lastHeight := int64(0)
	if prev, ok := b.lastProcessedHeight.Load(reporter); ok {
		lastHeight = prev.(int64)
	}
	for _, wh := range withdrawals {
		if wh > lastHeight && wh < currentHeight {
			return true
		}
	}
	return false
}

func (b *allTablesBackfiller) fetchBaselines(ctx context.Context, baselineHeight int64) error {
	const maxBaselineBlocks = 100
	startHeight := baselineHeight - maxBaselineBlocks + 1
	if startHeight < 1 {
		startHeight = 1
	}

	b.t.Logf("Fetching baselines from blocks %d to %d...", startHeight, baselineHeight)

	for height := startHeight; height <= baselineHeight; height++ {
		results, err := b.client.BlockResults(ctx, &height)
		if err != nil {
			continue
		}

		var allEvents []abci.Event
		allEvents = append(allEvents, results.FinalizeBlockEvents...)
		for _, txRes := range results.TxsResults {
			if txRes != nil {
				allEvents = append(allEvents, txRes.Events...)
			}
		}

		for _, ev := range allEvents {
			switch ev.Type {
			case eventTypeRewardsAdded:
				delegatorRaw := getBackfillAttribute(ev, "delegator")
				amountStr := getBackfillAttribute(ev, "amount")
				if delegatorRaw == "" || amountStr == "" {
					continue
				}
				reporter := parseReporterAddress(delegatorRaw)
				if reporter == "" {
					continue
				}
				if amount, err := parseBackfillAmount(amountStr); err == nil {
					b.rewardCumulative.Store(reporter, amount)
				}

			case "commission", "rewards":
				validator := getBackfillAttribute(ev, "validator")
				amountStr := getBackfillAttribute(ev, "amount")
				if validator == "" || amountStr == "" {
					continue
				}
				allocType := ev.Type
				if amount, err := parseBackfillAmount(amountStr); err == nil {
					b.validatorCumulative.Store(validator+":"+allocType, amount)
				}
			}
		}
	}

	var rewardCount, validatorCount int
	b.rewardCumulative.Range(func(_, _ any) bool { rewardCount++; return true })
	b.validatorCumulative.Range(func(_, _ any) bool { validatorCount++; return true })
	b.t.Logf("Pre-populated %d reporter baselines, %d validator baselines", rewardCount, validatorCount)

	return nil
}

// Batch insert functions
// insertRewards inserts rewards into the unified rewards table.
func (b *allTablesBackfiller) insertRewards(ctx context.Context, records []rewardRecord) error {
	if len(records) == 0 {
		return nil
	}
	var values []string
	var args []any
	for _, r := range records {
		values = append(values, "(?, ?, ?, ?, ?, ?)")
		args = append(args, r.BlockHeight, r.BlockTime, r.Sender, r.Recipient, r.Amount.String(), r.RewardType)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES %s", //nolint:gosec // G201: table name is constant
		blockdb.TableNameRewards,
		blockdb.ColBlockHeight, blockdb.ColBlockTime, blockdb.ColSender, blockdb.ColRecipient, blockdb.ColAmount, blockdb.ColType,
		strings.Join(values, ", "))
	_, err := b.db.ExecContext(ctx, query, args...)
	return err
}

func (b *allTablesBackfiller) insertReports(ctx context.Context, records []reportRecord) error {
	if len(records) == 0 {
		return nil
	}
	var values []string
	var args []any
	for _, r := range records {
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, r.Reporter, r.Power, r.QueryType, r.QueryID, r.AggregateMethod,
			r.Value, r.Timestamp, r.Cyclelist, r.BlockNumber, r.MetaID)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES %s", //nolint:gosec // G201: table name is constant
		blockdb.TableNameReports,
		blockdb.ColReporter, blockdb.ColPower, blockdb.ColQueryType, blockdb.ColQueryID, blockdb.ColAggregateMethod,
		blockdb.ColValue, blockdb.ColTimestamp, blockdb.ColCyclelist, blockdb.ColBlockNumber, blockdb.ColMetaID,
		strings.Join(values, ", "))
	_, err := b.db.ExecContext(ctx, query, args...)
	return err
}

func (b *allTablesBackfiller) insertTxs(ctx context.Context, records []txRecord) error {
	if len(records) == 0 {
		return nil
	}
	var values []string
	var args []any
	for _, r := range records {
		values = append(values, "(?, ?, ?, ?, ?)")
		args = append(args, r.BlockHeight, r.TxHash, r.Sender, r.GasUsed, r.FeeAmount)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s) VALUES %s", //nolint:gosec // G201: table name is constant
		blockdb.TableNameTxs,
		blockdb.ColBlockHeight, blockdb.ColTxHash, blockdb.ColSender, blockdb.ColGasUsed, blockdb.ColFeeAmount,
		strings.Join(values, ", "))
	_, err := b.db.ExecContext(ctx, query, args...)
	return err
}

func (b *allTablesBackfiller) insertBlockSigns(ctx context.Context, records []blockSignRecord) error {
	if len(records) == 0 {
		return nil
	}
	var values []string
	var args []any
	for _, r := range records {
		values = append(values, "(?, ?, ?, ?)")
		args = append(args, r.BlockHeight, r.BlockTimestamp, r.ValidatorAddress, r.Signed)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES %s", //nolint:gosec // G201: table name is constant
		blockdb.TableNameBlockSigns,
		blockdb.ColBlockHeight, blockdb.ColBlockTimestamp, blockdb.ColValidatorAddress, blockdb.ColSigned,
		strings.Join(values, ", "))
	_, err := b.db.ExecContext(ctx, query, args...)
	return err
}

// Helper functions for tx decoding
func newBackfillTxDecoder() sdk.TxDecoder {
	cdc := encoding.MakeCodec()
	txConfig := authtx.NewTxConfig(cdc, authtx.DefaultSignModes)
	return txConfig.TxDecoder()
}

func parseBackfillVoteExtTx(raw []byte) (any, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || (trimmed[0] != '{' && trimmed[0] != '[') {
		return nil, false
	}
	var result map[string]any
	if err := json.Unmarshal(trimmed, &result); err != nil {
		return nil, false
	}
	if _, ok := result["block_height"]; ok {
		return result, true
	}
	return nil, false
}

func findBackfillSender(tx sdk.Tx, events []abci.Event) string {
	if signingTx, ok := tx.(interface{ GetSigners() ([][]byte, error) }); ok {
		if signers, err := signingTx.GetSigners(); err == nil && len(signers) > 0 {
			return sdk.AccAddress(signers[0]).String()
		}
	}
	for _, ev := range events {
		if ev.Type != "message" {
			continue
		}
		for _, attr := range ev.Attributes {
			if attr.Key == "sender" && attr.Value != "" {
				return attr.Value
			}
		}
	}
	return "unknown"
}
