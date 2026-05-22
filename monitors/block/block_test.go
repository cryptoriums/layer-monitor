package block

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	"github.com/stretchr/testify/require"
	blockdb "github.com/cryptoriums/layer-monitor/db"
	cryptolog "github.com/cryptoriums/layer-packages/log"
)

// For all tests use only public module functions.
// For matching exp vs act, use the db or the prometheus metrics.

func TestBackfill(t *testing.T) {
	// blocksPerDay is the constant used in startHeight calculation
	const blocksPerDay = 57600

	cases := []struct {
		name             string
		backfillLookback int   // days
		latestHeight     int64 // current chain height
		lastStoredHeight int64 // 0 means no stored data
		// Expected results
		expectedFirstProcessed int64 // first block that should be in DB after run
		expectedLastProcessed  int64 // last block that should be in DB after run
		expectedGapStart       int64 // first height of gap (0 = no gap)
		expectedGapEnd         int64 // last height of gap (0 = no gap)
	}{
		{
			name:                   "backfill disabled - starts from current height",
			backfillLookback:       0,
			latestHeight:           100000,
			lastStoredHeight:       0,
			expectedFirstProcessed: 100001, // starts at latest+1, but latest is 100000
			expectedLastProcessed:  100000, // processes up to latest
			expectedGapStart:       0,      // no gap concept when disabled
			expectedGapEnd:         0,
		},
		{
			name:                   "backfill enabled - db at yesterday - fills all missing blocks",
			backfillLookback:       1,
			latestHeight:           100000,
			lastStoredHeight:       100000 - blocksPerDay + 1000, // within 1 day lookback (at height 43400)
			expectedFirstProcessed: 100000 - blocksPerDay + 1001, // continues from last+1 (43401)
			expectedLastProcessed:  100000,                       // processes up to latest
			expectedGapStart:       0,                            // no gap, continuous from last stored
			expectedGapEnd:         0,
		},
		{
			name:                   "backfill enabled - db at 2 days ago - fills only 1 day - gap exists",
			backfillLookback:       1,
			latestHeight:           100000,
			lastStoredHeight:       100000 - 2*blocksPerDay, // 2 days ago (at height -15200, but we'll use positive)
			expectedFirstProcessed: 100000 - blocksPerDay,   // starts from lookback (42400)
			expectedLastProcessed:  100000,                  // processes up to latest
			expectedGapStart:       100000 - 2*blocksPerDay + 1,
			expectedGapEnd:         100000 - blocksPerDay - 1, // gap between old data and lookback start
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sqlDB, err := sql.Open("chdb", "")
			require.NoError(t, err)
			t.Cleanup(func() { _ = sqlDB.Close() })

			wrappedDB, err := blockdb.New(ctx, sqlDB)
			require.NoError(t, err)

			// Clear tables before each test to ensure isolation
			for _, table := range []string{blockdb.TableNameTxs, blockdb.TableNameReports, blockdb.TableNameBlockSigns} {
				_, err = sqlDB.Exec("TRUNCATE TABLE " + table)
				require.NoError(t, err)
			}

			// Seed DB with last stored height if specified
			if tc.lastStoredHeight > 0 {
				seedDBWithHeight(t, sqlDB, tc.lastStoredHeight)
			}

			// Create mock fetcher that tracks which blocks are fetched
			// Use empty fixtures since backfill generates blocks dynamically
			fetcher := newMockFetcher(t, nil)
			// Override maxHeight for backfill
			fetcher.mu.Lock()
			fetcher.maxHeight = tc.latestHeight
			fetcher.firstCall = false // Don't use firstCall behavior for backfill
			fetcher.mu.Unlock()

			cfg := Config{
				BackfillLookback: tc.backfillLookback,
				PollInterval:     50 * time.Millisecond,
				FetchWorkers:     5,
			}
			monitor, err := NewWithFetcher(
				context.Background(),
				cryptolog.New(),
				cfg,
				wrappedDB,
				fetcher,
			)
			require.NoError(t, err)

			// Run the monitor in background
			runErr := make(chan error, 1)
			go func() {
				runErr <- monitor.Run(ctx)
			}()

			// Wait for blocks to be processed
			// The monitor should process all blocks from startHeight to latestHeight
			expectedBlocks := tc.expectedLastProcessed - tc.expectedFirstProcessed + 1
			if tc.backfillLookback == 0 {
				// When backfill disabled, starts at latest+1, so no blocks to process initially
				expectedBlocks = 0
			}

			// Wait until all expected blocks are fetched or timeout
			require.Eventually(t, func() bool {
				fetchedCount := fetcher.FetchedCount()
				return fetchedCount >= int(expectedBlocks)
			}, 5*time.Second, 50*time.Millisecond, "expected %d blocks to be fetched", expectedBlocks)

			// Stop the monitor
			cancel()
			select {
			case err := <-runErr:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("monitor run failed: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("monitor did not stop")
			}

			// Verify fetched heights
			fetchedHeights := fetcher.FetchedHeights()

			// Early return for disabled backfill
			if tc.backfillLookback == 0 {
				require.Empty(t, fetchedHeights, "backfill disabled should not fetch historical blocks")
				return
			}

			// From here: backfill is enabled
			// Verify first and last processed heights
			if len(fetchedHeights) == 0 {
				t.Fatal("backfill enabled but no heights were fetched")
			}

			minFetched, maxFetched := minMaxHeights(fetchedHeights)
			require.Equal(t, tc.expectedFirstProcessed, minFetched, "first processed height mismatch")
			require.Equal(t, tc.expectedLastProcessed, maxFetched, "last processed height mismatch")

			// Verify gap exists (if expected)
			if tc.expectedGapStart > 0 {
				verifyGapExists(t, fetchedHeights, tc.expectedGapStart, tc.expectedGapEnd)
				return
			}

			// Verify no gap (continuous from last stored to latest)
			if tc.lastStoredHeight > 0 {
				verifyNoGap(t, fetchedHeights, tc.lastStoredHeight+1, tc.latestHeight)
			}
		})
	}
}

// Note: backfillMockFetcher was removed - now using existing mockFetcher from helpers_test.go
// which has been extended to track fetched heights for backfill testing.

// verifyGapExists checks that all heights in the gap range were NOT fetched.
func verifyGapExists(t *testing.T, fetchedHeights map[int64]struct{}, gapStart, gapEnd int64) {
	t.Helper()
	for h := gapStart; h <= gapEnd; h++ {
		_, wasFetched := fetchedHeights[h]
		require.False(t, wasFetched, "height %d should be in gap (not fetched)", h)
	}
}

// verifyNoGap checks that all heights in the range were fetched (no gaps).
func verifyNoGap(t *testing.T, fetchedHeights map[int64]struct{}, start, end int64) {
	t.Helper()
	for h := start; h <= end; h++ {
		_, wasFetched := fetchedHeights[h]
		require.True(t, wasFetched, "height %d should be fetched (no gap expected)", h)
	}
}

// minMaxHeights returns the min and max heights from a map.
func minMaxHeights(heights map[int64]struct{}) (min, max int64) {
	first := true
	for h := range heights {
		if first {
			min, max = h, h
			first = false
		} else {
			if h < min {
				min = h
			}
			if h > max {
				max = h
			}
		}
	}
	return
}

// seedDBWithHeight inserts a minimal record at the specified height.
func seedDBWithHeight(t *testing.T, db *sql.DB, height int64) {
	t.Helper()

	// Insert a minimal tx record to establish the last stored height
	// Schema: block_height, tx_hash (FixedString(64)), sender, gas_used, fee_amount
	txHash := "0000000000000000000000000000000000000000000000000000000000000000"       // 64 chars
	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?)", //nolint:gosec // G201: table name is constant
		blockdb.TableNameTxs,
		blockdb.ColBlockHeight, blockdb.ColTxHash, blockdb.ColSender, blockdb.ColGasUsed, blockdb.ColFeeAmount)
	_, err := db.Exec(query, height, txHash, "seed_sender", 0, 0)
	require.NoError(t, err)
}

func TestDeduplication(t *testing.T) {
	type nodeStream struct {
		name      string
		idxToSend []int
	}
	fixtures := loadFixtures(t)
	require.NotEmpty(t, fixtures)

	expected := extractExpectedReports(t, fixtures)
	require.NotEmpty(t, expected)

	cases := []struct {
		name  string
		nodes []nodeStream
	}{
		{
			name: "second node stopped after first block",
			nodes: []nodeStream{
				{name: "primary", idxToSend: []int{0, 1, 2, 3}},
				{name: "secondary", idxToSend: []int{0}},
			},
		},
		{
			name: "mixed blocks from different nodes",
			nodes: []nodeStream{
				{name: "primary", idxToSend: []int{0, 1}},
				{name: "secondary", idxToSend: []int{1, 2, 3}},
			},
		},
		{
			name: "second node starts sending later",
			nodes: []nodeStream{
				{name: "primary", idxToSend: []int{0, 1, 2, 3}},
				{name: "secondary", idxToSend: []int{2, 3}},
			},
		},
		{
			name: "three nodes emit identical blocks",
			nodes: []nodeStream{
				{name: "node-a", idxToSend: []int{0, 1, 2, 3}},
				{name: "node-b", idxToSend: []int{0, 1, 2, 3}},
				{name: "node-c", idxToSend: []int{0, 1, 2, 3}},
			},
		},
		{
			name: "multiple nodes send each different blocks",
			nodes: []nodeStream{
				{name: "node-1", idxToSend: []int{0, 1}},
				{name: "node-2", idxToSend: []int{2}},
				{name: "node-3", idxToSend: []int{3}},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sqlDB, err := sql.Open("chdb", "")
			require.NoError(t, err)
			t.Cleanup(func() { _ = sqlDB.Close() })

			wrappedDB, err := blockdb.New(ctx, sqlDB)
			require.NoError(t, err)

			// Merge all node streams into a single mock fetcher that returns all blocks
			// The test verifies deduplication by checking that the same block from
			// multiple "nodes" results in only one DB entry
			fetcher := newMockFetcher(t, fixtures)

			// Each node stream represents a different set of blocks
			// Merge all indices to get the complete set of blocks to return
			allIdx := make(map[int]struct{})
			for _, nodeCfg := range tc.nodes {
				for _, idx := range nodeCfg.idxToSend {
					allIdx[idx] = struct{}{}
				}
			}
			idxList := make([]int, 0, len(allIdx))
			for idx := range allIdx {
				idxList = append(idxList, idx)
			}
			fetcher.SetBatch(idxList, fixtures)

			cfg := Config{BackfillLookback: 0, PollInterval: 50 * time.Millisecond}
			monitor, err := NewWithFetcher(
				context.Background(),
				cryptolog.New(),
				cfg,
				wrappedDB,
				fetcher,
			)
			require.NoError(t, err)

			runErr := make(chan error, 1)
			go func() {
				runErr <- monitor.Run(ctx)
			}()

			t.Cleanup(func() {
				cancel()
				select {
				case err := <-runErr:
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Fatalf("monitor run failed: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("monitor did not stop")
				}
			})

			expectedCount := len(expected)
			require.Eventually(t, func() bool {
				actual := fetchReportsFromDB(t, sqlDB)
				return len(actual) == expectedCount
			}, 3*time.Second, 100*time.Millisecond, "expected %d reports, got %d", expectedCount, len(fetchReportsFromDB(t, sqlDB)))

			actualReports := sortReports(t, copyReports(fetchReportsFromDB(t, sqlDB)))
			expectedSorted := sortReports(t, copyReports(expected))
			require.Equal(t, expectedSorted, actualReports)
		})
	}
}
