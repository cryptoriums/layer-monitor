package block

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"
	blockdb "github.com/cryptoriums/layer-monitor/db"
	blockprocessor "github.com/cryptoriums/layer-monitor/monitors/block/processor"
	"github.com/tellor-io/layer/x/oracle/types"
)

func loadFixtures(t *testing.T) []ctypes.EventDataNewBlock {
	t.Helper()

	path := fixturePath(t, "blocks_fixtures.json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var fixtures []ctypes.EventDataNewBlock
	require.NoError(t, json.Unmarshal(data, &fixtures))

	return fixtures
}

func fixturePath(t *testing.T, name string) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to determine caller path")

	dir := filepath.Dir(filename)
	return filepath.Join(dir, name)
}

func fixtureHeight(f ctypes.EventDataNewBlock) int64 {
	if f.Block == nil {
		return 0
	}
	return f.Block.Height
}

func extractExpectedReports(t *testing.T, fixtures []ctypes.EventDataNewBlock) []types.MicroReport {
	t.Helper()

	var reports []types.MicroReport
	for _, fixture := range fixtures {
		height := fixtureHeight(fixture)
		for _, tx := range fixture.ResultFinalizeBlock.TxResults {
			if tx == nil {
				continue
			}
			for _, ev := range tx.Events {
				if ev.Type != "new_report" {
					continue
				}
				report, err := blockprocessor.DecodeReportEvent(height, ev)
				require.NoError(t, err)

				reports = append(reports, *report)
			}
		}
	}

	return reports
}

func fetchReportsFromDB(t *testing.T, db *sql.DB) []types.MicroReport {
	t.Helper()

	query := fmt.Sprintf("SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s ORDER BY %s, %s", //nolint:gosec // G201: table name is constant
		blockdb.ColReporter, blockdb.ColPower, blockdb.ColQueryType, blockdb.ColQueryID, blockdb.ColAggregateMethod,
		blockdb.ColValue, blockdb.ColTimestamp, blockdb.ColCyclelist, blockdb.ColBlockNumber, blockdb.ColMetaID,
		blockdb.TableNameReports, blockdb.ColBlockNumber, blockdb.ColReporter)
	rows, err := db.QueryContext(context.Background(), query)
	require.NoError(t, err)
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			t.Logf("failed to close rows: %v", closeErr)
		}
	}()

	var reports []types.MicroReport
	for rows.Next() {
		var (
			reporter        string
			power           uint64
			queryType       string
			queryIDStr      string
			aggregateMethod string
			value           string
			ts              time.Time
			cycle           uint8
			blockNumber     uint64
			metaID          uint64
		)
		require.NoError(t, rows.Scan(&reporter, &power, &queryType, &queryIDStr, &aggregateMethod, &value, &ts, &cycle, &blockNumber, &metaID))

		queryID, err := blockprocessor.DecodeQueryID(queryIDStr)
		require.NoError(t, err)

		reports = append(reports, types.MicroReport{
			Reporter:        reporter,
			Power:           power,
			QueryType:       queryType,
			QueryId:         queryID,
			AggregateMethod: aggregateMethod,
			Value:           value,
			Timestamp:       ts.UTC(),
			Cyclelist:       cycle == 1,
			BlockNumber:     blockNumber,
			MetaId:          metaID,
		})
	}
	require.NoError(t, rows.Err())

	return reports
}

func sortReports(t *testing.T, reports []types.MicroReport) []types.MicroReport {
	t.Helper()

	sort.Slice(reports, func(i, j int) bool {
		if reports[i].BlockNumber != reports[j].BlockNumber {
			return reports[i].BlockNumber < reports[j].BlockNumber
		}
		if reports[i].Reporter != reports[j].Reporter {
			return reports[i].Reporter < reports[j].Reporter
		}
		// Use QueryId as tie-breaker for same reporter in same block
		return string(reports[i].QueryId) < string(reports[j].QueryId)
	})

	return reports
}

func copyReports(reports []types.MicroReport) []types.MicroReport {
	if reports == nil {
		return nil
	}
	out := make([]types.MicroReport, len(reports))
	for i, r := range reports {
		out[i] = r
		if len(r.QueryId) > 0 {
			out[i].QueryId = append([]byte(nil), r.QueryId...)
		}
	}
	return out
}

// mockFetcher implements BlockFetcher for testing.
// It returns all fixture blocks immediately.
type mockFetcher struct {
	t              *testing.T
	mu             sync.Mutex
	blocks         map[int64]ctypes.EventDataNewBlock
	minHeight      int64
	maxHeight      int64
	firstCall      bool
	fetchedHeights map[int64]struct{} // Track which heights were fetched (for backfill testing)
}

func newMockFetcher(t *testing.T, fixtures []ctypes.EventDataNewBlock) *mockFetcher {
	t.Helper()
	blocks := make(map[int64]ctypes.EventDataNewBlock)
	var minHeight, maxHeight int64
	for _, f := range fixtures {
		if f.Block != nil {
			h := f.Block.Height
			blocks[h] = f
			if minHeight == 0 || h < minHeight {
				minHeight = h
			}
			if h > maxHeight {
				maxHeight = h
			}
		}
	}
	return &mockFetcher{
		t:              t,
		blocks:         blocks,
		minHeight:      minHeight,
		maxHeight:      maxHeight,
		firstCall:      true,
		fetchedHeights: make(map[int64]struct{}),
	}
}

// SetBatch simulates multiple nodes sending different blocks.
// It updates the mock to return specific blocks based on indices.
func (m *mockFetcher) SetBatch(idxToSend []int, fixtures []ctypes.EventDataNewBlock) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Build map of which fixtures to include with full data
	includeData := make(map[int]struct{})
	for _, idx := range idxToSend {
		includeData[idx] = struct{}{}
	}

	// Update blocks map with the filtered data
	for idx, f := range fixtures {
		if f.Block == nil {
			continue
		}
		h := f.Block.Height
		if _, ok := includeData[idx]; ok {
			m.blocks[h] = f
		} else {
			// Store block without tx results (simulates node that missed the data)
			m.blocks[h] = ctypes.EventDataNewBlock{
				Block:   f.Block,
				BlockID: f.BlockID,
			}
		}
	}
}

func (m *mockFetcher) LatestHeight(_ context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// First call returns minHeight-1 so the monitor starts at minHeight
	// Subsequent calls return the actual maxHeight
	if m.firstCall {
		m.firstCall = false
		return m.minHeight - 1, nil
	}
	return m.maxHeight, nil
}

func (m *mockFetcher) FetchBlock(_ context.Context, height int64) (ctypes.EventDataNewBlock, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Track that this height was fetched (for backfill testing)
	m.fetchedHeights[height] = struct{}{}

	if block, ok := m.blocks[height]; ok {
		return block, nil
	}
	// Return empty block for heights we don't have (backfill uses this)
	return ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{
				Height: height,
				Time:   time.Now(),
			},
			Data: ctypes.Data{
				Txs: []ctypes.Tx{}, // Empty tx list to avoid parse errors
			},
			LastCommit: &ctypes.Commit{},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{
			TxResults: []*abci.ExecTxResult{}, // Empty results
		},
	}, nil
}

// FetchedCount returns the number of unique heights that were fetched.
// Used by backfill tests to verify correct blocks were processed.
func (m *mockFetcher) FetchedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.fetchedHeights)
}

// FetchedHeights returns a copy of the fetched heights map.
// Used by backfill tests to verify which blocks were processed.
func (m *mockFetcher) FetchedHeights() map[int64]struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Return a copy to avoid race conditions
	copy := make(map[int64]struct{}, len(m.fetchedHeights))
	for h := range m.fetchedHeights {
		copy[h] = struct{}{}
	}
	return copy
}
