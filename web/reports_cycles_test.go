package web

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	blockdb "github.com/cryptoriums/layer-monitor/db"
	cryptolog "github.com/cryptoriums/layer-monitor/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupWebTestDB creates an in-memory chdb database for testing.
// It truncates all tables to ensure test isolation.
func setupWebTestDB(t *testing.T) blockdb.SQLDB {
	t.Helper()
	ctx := context.Background()

	sqlDB, err := sql.Open("chdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	wrappedDB, err := blockdb.New(ctx, sqlDB)
	require.NoError(t, err)

	// Truncate all tables for test isolation
	tables := []string{
		blockdb.TableNameReports,
		blockdb.TableNameCycleRotations,
	}
	for _, table := range tables {
		_, err := wrappedDB.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", table))
		require.NoError(t, err)
	}

	return wrappedDB
}

// newTestServer creates a minimal Server for testing getMissedCyclesPerReporterFromDB.
func newTestServer(db blockdb.Db, lookbackDays int) *Server {
	if lookbackDays <= 0 {
		lookbackDays = DefaultLookbackPeriodDays
	}
	return &Server{
		cfg: Config{
			LookbackPeriodDays: lookbackDays,
		},
		logger: cryptolog.New().With("component", "web_test"),
		db:     db,
	}
}

// insertCycleRotation inserts a cycle rotation record into the database.
func insertCycleRotation(t *testing.T, db blockdb.Db, height int64, queryID string, timestamp time.Time) {
	t.Helper()
	ctx := context.Background()

	query := fmt.Sprintf(`
		INSERT INTO %s (%s, %s, %s)
		VALUES (?, ?, ?)
	`, blockdb.TableNameCycleRotations,
		blockdb.ColBlockHeight, blockdb.ColQueryID, blockdb.ColTimestamp)

	_, err := db.Exec(ctx, query, height, queryID, timestamp)
	require.NoError(t, err)
}

// insertReport inserts a report record into the database.
func insertReport(
	t *testing.T,
	db blockdb.Db,
	reporter string,
	queryID string,
	timestamp time.Time,
	cyclelist uint8,
	blockNum int64,
) {
	t.Helper()
	ctx := context.Background()

	query := fmt.Sprintf(`
		INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, blockdb.TableNameReports,
		blockdb.ColReporter, blockdb.ColPower, blockdb.ColQueryType, blockdb.ColQueryID,
		blockdb.ColAggregateMethod, blockdb.ColValue, blockdb.ColTimestamp,
		blockdb.ColCyclelist, blockdb.ColBlockNumber, blockdb.ColMetaID)

	_, err := db.Exec(ctx, query,
		reporter,
		1000000,           // power
		"SpotPrice",       // query_type
		queryID,           // query_id
		"weighted-median", // aggregate_method
		"0x1234",          // value
		timestamp,
		cyclelist,
		blockNum,
		1, // meta_id
	)
	require.NoError(t, err)
}

func TestGetMissedCyclesPerReporterFromDB_NoCycles(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, DefaultLookbackPeriodDays)

	// No cycles in database
	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// Should return empty map when there are no cycles
	assert.Empty(t, result, "should return empty map when no cycles exist")
}

func TestGetMissedCyclesPerReporterFromDB_CyclesNoReports(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, DefaultLookbackPeriodDays)

	now := time.Now().UTC()

	// Insert 10 cycle rotations within lookback period
	for i := 0; i < 10; i++ {
		insertCycleRotation(t, db, int64(100+i), fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)))
	}

	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// Should return empty map since no reporters have submitted reports
	// (reporters are only tracked if they've submitted at least one report)
	assert.Empty(t, result, "should return empty map when no reporters have submitted reports")
}

func TestGetMissedCyclesPerReporterFromDB_AllReportsSubmitted(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, DefaultLookbackPeriodDays)

	now := time.Now().UTC()
	reporter := "tellor1reporter1abc123"

	// Insert 10 cycle rotations
	for i := 0; i < 10; i++ {
		insertCycleRotation(t, db, int64(100+i), fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)))
	}

	// Reporter submits 10 cyclelist reports (one per cycle)
	for i := 0; i < 10; i++ {
		insertReport(t, db, reporter, fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)), 1, int64(100+i))
	}

	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// Reporter should have 0 missed cycles (10 cycles - 10 reports = 0)
	require.Contains(t, result, reporter)
	assert.Equal(t, int64(0), result[reporter], "reporter with all reports should have 0 missed cycles")
}

func TestGetMissedCyclesPerReporterFromDB_SomeMissedCycles(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, DefaultLookbackPeriodDays)

	now := time.Now().UTC()
	reporter := "tellor1reporter2xyz789"

	// Insert 10 cycle rotations
	for i := 0; i < 10; i++ {
		insertCycleRotation(t, db, int64(100+i), fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)))
	}

	// Reporter submits only 6 cyclelist reports
	for i := 0; i < 6; i++ {
		insertReport(t, db, reporter, fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)), 1, int64(100+i))
	}

	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// Reporter should have 4 missed cycles (10 cycles - 6 reports = 4)
	require.Contains(t, result, reporter)
	assert.Equal(t, int64(4), result[reporter], "reporter should have 4 missed cycles")
}

func TestGetMissedCyclesPerReporterFromDB_MultipleReporters(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, DefaultLookbackPeriodDays)

	now := time.Now().UTC()

	reporters := []struct {
		address        string
		reportCount    int
		expectedMissed int64
	}{
		{"tellor1reporterA", 10, 0}, // All cycles covered
		{"tellor1reporterB", 7, 3},  // 3 missed
		{"tellor1reporterC", 5, 5},  // 5 missed
		{"tellor1reporterD", 0, 0},  // No reports (won't appear in result)
	}

	// Insert 10 cycle rotations
	for i := 0; i < 10; i++ {
		insertCycleRotation(t, db, int64(100+i), fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)))
	}

	// Insert reports for each reporter
	for _, r := range reporters {
		for i := 0; i < r.reportCount; i++ {
			insertReport(t, db, r.address, fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)), 1, int64(100+i))
		}
	}

	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// Check each reporter's missed cycles
	for _, r := range reporters {
		if r.reportCount == 0 {
			// Reporters with no reports won't appear in result
			assert.NotContains(t, result, r.address, "reporter with no reports should not appear in result")
		} else {
			require.Contains(t, result, r.address, "reporter %s should be in result", r.address)
			assert.Equal(t, r.expectedMissed, result[r.address], "reporter %s should have %d missed cycles", r.address, r.expectedMissed)
		}
	}
}

func TestGetMissedCyclesPerReporterFromDB_NonCyclelistReportsIgnored(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, DefaultLookbackPeriodDays)

	now := time.Now().UTC()
	reporter := "tellor1reporterNonCycle"

	// Insert 10 cycle rotations
	for i := 0; i < 10; i++ {
		insertCycleRotation(t, db, int64(100+i), fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)))
	}

	// Reporter submits 10 reports, but only 3 are cyclelist reports
	for i := 0; i < 10; i++ {
		cyclelist := uint8(0) // Non-cyclelist by default
		if i < 3 {
			cyclelist = 1 // Only first 3 are cyclelist
		}
		insertReport(t, db, reporter, fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)), cyclelist, int64(100+i))
	}

	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// Only cyclelist reports count, so missed = 10 - 3 = 7
	require.Contains(t, result, reporter)
	assert.Equal(t, int64(7), result[reporter], "only cyclelist reports should count, so 7 missed")
}

func TestGetMissedCyclesPerReporterFromDB_LookbackPeriodRespected(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, 1) // Only 1 day lookback

	now := time.Now().UTC()
	reporter := "tellor1reporterLookback"

	// Insert 5 cycles within lookback period (recent)
	for i := 0; i < 5; i++ {
		insertCycleRotation(t, db, int64(100+i), fmt.Sprintf("queryid_recent%d", i), now.Add(-time.Hour*time.Duration(i)))
	}

	// Insert 5 cycles outside lookback period (older than 1 day)
	for i := 0; i < 5; i++ {
		insertCycleRotation(t, db, int64(200+i), fmt.Sprintf("queryid_old%d", i), now.Add(-time.Hour*48-time.Hour*time.Duration(i)))
	}

	// Reporter submits 3 cyclelist reports within lookback period
	for i := 0; i < 3; i++ {
		insertReport(t, db, reporter, fmt.Sprintf("queryid_recent%d", i), now.Add(-time.Hour*time.Duration(i)), 1, int64(100+i))
	}

	// Reporter submits 5 cyclelist reports outside lookback period (should be ignored)
	for i := 0; i < 5; i++ {
		insertReport(t, db, reporter, fmt.Sprintf("queryid_old%d", i), now.Add(-time.Hour*48-time.Hour*time.Duration(i)), 1, int64(200+i))
	}

	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// Only cycles and reports within lookback should count
	// 5 recent cycles - 3 recent reports = 2 missed
	require.Contains(t, result, reporter)
	assert.Equal(t, int64(2), result[reporter], "should only count cycles/reports within lookback period")
}

func TestGetMissedCyclesPerReporterFromDB_MoreReportsThanCycles(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, DefaultLookbackPeriodDays)

	now := time.Now().UTC()
	reporter := "tellor1reporterMoreReports"

	// Insert 5 cycle rotations
	for i := 0; i < 5; i++ {
		insertCycleRotation(t, db, int64(100+i), fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)))
	}

	// Reporter submits 8 cyclelist reports (more than cycles): block numbers 100..107 all
	// fall on or after the 5 rotation boundaries (100..104), so every cycle is covered.
	for i := 0; i < 8; i++ {
		insertReport(t, db, reporter, fmt.Sprintf("queryid%d", i%5), now.Add(-time.Minute*time.Duration(i)), 1, int64(100+i))
	}

	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// All 5 cycles covered -> 0 missed (extra reports within cycles do not go negative).
	require.Contains(t, result, reporter)
	assert.Equal(t, int64(0), result[reporter], "covering every cycle should yield 0 missed regardless of report count")
}

// The regression this whole change is about: a reporter that submits SEVERAL reports in the
// few cycles it does cover, while missing the rest. The old "cycles - report_count" formula
// let the extra reports cancel the misses (showing a falsely low miss rate); counting
// distinct covered cycles reports the real number.
func TestGetMissedCyclesPerReporterFromDB_MultipleReportsPerCycleDoNotHideMisses(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, DefaultLookbackPeriodDays)

	now := time.Now().UTC()
	reporter := "tellor1reporterBursty"

	// 10 cycles at heights 100..109.
	for i := 0; i < 10; i++ {
		insertCycleRotation(t, db, int64(100+i), fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)))
	}

	// Reporter covers only cycles 100, 101, 102 but submits 5 reports inside EACH of them
	// (block numbers land in [100,101), [101,102), [102,103)). That is 15 reports total.
	for _, h := range []int64{100, 101, 102} {
		for j := 0; j < 5; j++ {
			insertReport(t, db, reporter, "queryid", now.Add(-time.Minute*time.Duration(h)), 1, h)
		}
	}

	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// Only 3 of 10 cycles covered -> 7 missed. The old formula gave 10-15 -> clamped 0.
	require.Contains(t, result, reporter)
	assert.Equal(t, int64(7), result[reporter], "multiple reports per cycle must not hide missed cycles")
}

func TestGetMissedCyclesPerReporterFromDB_ZeroLookbackDays(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	// Server with 0 lookback days should use default
	server := newTestServer(db, 0)

	now := time.Now().UTC()
	reporter := "tellor1reporterZeroLookback"

	// Insert cycles within default lookback period
	for i := 0; i < 5; i++ {
		insertCycleRotation(t, db, int64(100+i), fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)))
	}

	// Reporter submits 3 reports
	for i := 0; i < 3; i++ {
		insertReport(t, db, reporter, fmt.Sprintf("queryid%d", i), now.Add(-time.Hour*time.Duration(i)), 1, int64(100+i))
	}

	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// Should work with default lookback period
	require.Contains(t, result, reporter)
	assert.Equal(t, int64(2), result[reporter], "should use default lookback period")
}

func TestGetMissedCyclesPerReporterFromDB_EmptyQueryID(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, DefaultLookbackPeriodDays)

	now := time.Now().UTC()
	reporter := "tellor1reporterEmptyQueryID"

	// Insert cycles with empty query IDs (edge case)
	for i := 0; i < 5; i++ {
		insertCycleRotation(t, db, int64(100+i), "", now.Add(-time.Hour*time.Duration(i)))
	}

	// Reporter submits reports
	for i := 0; i < 3; i++ {
		insertReport(t, db, reporter, "", now.Add(-time.Hour*time.Duration(i)), 1, int64(100+i))
	}

	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// Should still calculate correctly even with empty query IDs
	require.Contains(t, result, reporter)
	assert.Equal(t, int64(2), result[reporter], "should handle empty query IDs")
}

func TestGetMissedCyclesPerReporterFromDB_SameTimestampMultipleCycles(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, DefaultLookbackPeriodDays)

	now := time.Now().UTC()
	reporter := "tellor1reporterSameTimestamp"

	// Insert 3 cycles at the same timestamp (edge case - rapid rotation)
	for i := 0; i < 3; i++ {
		insertCycleRotation(t, db, int64(100+i), fmt.Sprintf("queryid%d", i), now)
	}

	// Reporter submits 2 reports at the same timestamp
	for i := 0; i < 2; i++ {
		insertReport(t, db, reporter, fmt.Sprintf("queryid%d", i), now, 1, int64(100+i))
	}

	result, _ := server.getMissedCyclesPerReporterFromDB(ctx)

	// 3 cycles - 2 reports = 1 missed
	require.Contains(t, result, reporter)
	assert.Equal(t, int64(1), result[reporter], "should handle same timestamp correctly")
}

func TestGetMissedCyclesPerReporterFromDBForPeriod_OverridesLookback(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, 30) // Default window is 30 days

	now := time.Now().UTC()
	reporter := "tellor1reporterPeriodOverride"

	// Insert 5 cycles within the last day.
	for i := 0; i < 5; i++ {
		insertCycleRotation(t, db, int64(100+i), fmt.Sprintf("queryid_recent%d", i), now.Add(-time.Hour*time.Duration(i)))
	}

	// Insert 5 cycles older than 1 day (but still within 30 days).
	for i := 0; i < 5; i++ {
		insertCycleRotation(t, db, int64(200+i), fmt.Sprintf("queryid_old%d", i), now.Add(-48*time.Hour-time.Hour*time.Duration(i)))
	}

	// Reporter submits 3 cyclelist reports in the last day.
	for i := 0; i < 3; i++ {
		insertReport(t, db, reporter, fmt.Sprintf("queryid_recent%d", i), now.Add(-time.Hour*time.Duration(i)), 1, int64(100+i))
	}

	// Reporter also has old reports that should be ignored for period=1.
	for i := 0; i < 5; i++ {
		insertReport(t, db, reporter, fmt.Sprintf("queryid_old%d", i), now.Add(-48*time.Hour-time.Hour*time.Duration(i)), 1, int64(200+i))
	}

	result, _ := server.getMissedCyclesPerReporterFromDBForPeriod(ctx, 1)

	// For period=1: 5 recent cycles - 3 recent reports = 2 missed.
	require.Contains(t, result, reporter)
	assert.Equal(t, int64(2), result[reporter], "period override should use requested day window")
}

func TestPopulateCachedMissedCyclesForPeriod_UpdatesReporterMetrics(t *testing.T) {
	ctx := context.Background()
	db := setupWebTestDB(t)
	server := newTestServer(db, 30)

	now := time.Now().UTC()
	reporterA := "tellor1reporterOverlayA"
	reporterB := "tellor1reporterOverlayB"

	// Period=1 data: 5 cycles, reporterA submitted 4 cyclelist reports.
	for i := 0; i < 5; i++ {
		insertCycleRotation(t, db, int64(300+i), fmt.Sprintf("queryid_overlay%d", i), now.Add(-time.Hour*time.Duration(i)))
	}
	for i := 0; i < 4; i++ {
		insertReport(t, db, reporterA, fmt.Sprintf("queryid_overlay%d", i), now.Add(-time.Hour*time.Duration(i)), 1, int64(300+i))
	}

	validators := []CachedValidatorTree{
		{
			Reporters: []CachedReporterTree{
				{Address: reporterA, Status: "Degraded"},
				{Address: reporterB, Status: "Active"},
			},
		},
	}

	server.populateCachedMissedCyclesForPeriod(ctx, validators, 1)

	require.Len(t, validators, 1)
	require.Len(t, validators[0].Reporters, 2)

	assert.Equal(t, int64(1), validators[0].Reporters[0].MissedCycles)
	assert.Equal(t, "20%", validators[0].Reporters[0].MissedCyclesPct)
	assert.Equal(t, "Active", validators[0].Reporters[0].Status)

	// reporterB has no cyclelist reports in this period, so it misses all cycles.
	assert.Equal(t, int64(5), validators[0].Reporters[1].MissedCycles)
	assert.Equal(t, "100%", validators[0].Reporters[1].MissedCyclesPct)
}
