package processor

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	abci "github.com/cometbft/cometbft/abci/types"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	blockdb "github.com/cryptoriums/layer-monitor/db"
	cryptolog "github.com/cryptoriums/layer-monitor/log"
)

// setupTestDB creates an in-memory chdb database for testing.
// It truncates all tables to ensure test isolation.
func setupTestDB(t *testing.T) blockdb.SQLDB {
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
		blockdb.TableNameTxs,
		blockdb.TableNameRewards,
		blockdb.TableNameBlockSigns,
		blockdb.TableNameCycleRotations,
		blockdb.TableNameDisputes,
	}
	for _, table := range tables {
		_, err := wrappedDB.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", table))
		require.NoError(t, err)
	}

	return wrappedDB
}

// blockSignRecord represents a row from the block_signs table.
type blockSignRecord struct {
	BlockHeight      int64
	ValidatorAddress string
	Signed           uint8
}

// fetchBlockSigns queries all block_signs records from the database.
func fetchBlockSigns(t *testing.T, db blockdb.SQLDB) []blockSignRecord {
	t.Helper()
	ctx := context.Background()

	rows, err := db.Query(ctx, fmt.Sprintf("SELECT %s, %s, %s FROM %s ORDER BY %s, %s",
		blockdb.ColBlockHeight, blockdb.ColValidatorAddress, blockdb.ColSigned,
		blockdb.TableNameBlockSigns, blockdb.ColBlockHeight, blockdb.ColValidatorAddress))
	require.NoError(t, err)
	defer rows.Close()

	var records []blockSignRecord
	for rows.Next() {
		var rec blockSignRecord
		err := rows.Scan(&rec.BlockHeight, &rec.ValidatorAddress, &rec.Signed)
		require.NoError(t, err)
		records = append(records, rec)
	}
	require.NoError(t, rows.Err())
	return records
}

// reportRecord represents a row from the reports table.
type reportRecord struct {
	Reporter  string
	Power     uint64
	QueryType string
	QueryID   string
	BlockNum  uint64
	MetaID    uint64
	Cyclelist uint8
}

// fetchReports queries all report records from the database.
func fetchReports(t *testing.T, db blockdb.SQLDB) []reportRecord {
	t.Helper()
	ctx := context.Background()

	rows, err := db.Query(ctx, fmt.Sprintf("SELECT %s, %s, %s, %s, %s, %s, %s FROM %s ORDER BY %s",
		blockdb.ColReporter, blockdb.ColPower, blockdb.ColQueryType, blockdb.ColQueryID, blockdb.ColBlockNumber, blockdb.ColMetaID, blockdb.ColCyclelist,
		blockdb.TableNameReports, blockdb.ColBlockNumber))
	require.NoError(t, err)
	defer rows.Close()

	var records []reportRecord
	for rows.Next() {
		var rec reportRecord
		err := rows.Scan(&rec.Reporter, &rec.Power, &rec.QueryType, &rec.QueryID, &rec.BlockNum, &rec.MetaID, &rec.Cyclelist)
		require.NoError(t, err)
		records = append(records, rec)
	}
	require.NoError(t, rows.Err())
	return records
}

// rewardRecord represents a row from the unified rewards table.
type rewardRecord struct {
	BlockHeight int64
	BlockTime   time.Time
	Sender      string
	Recipient   string
	Amount      string
	RewardType  string
}

// fetchRewards queries all reward records from the database.
func fetchRewards(t *testing.T, db blockdb.SQLDB) []rewardRecord {
	t.Helper()
	ctx := context.Background()

	rows, err := db.Query(ctx, fmt.Sprintf("SELECT %s, %s, %s, %s, toString(%s), %s FROM %s ORDER BY %s, %s, %s",
		blockdb.ColBlockHeight, blockdb.ColBlockTime, blockdb.ColSender, blockdb.ColRecipient, blockdb.ColAmount, blockdb.ColType,
		blockdb.TableNameRewards, blockdb.ColBlockHeight, blockdb.ColType, blockdb.ColRecipient))
	require.NoError(t, err)
	defer rows.Close()

	var records []rewardRecord
	for rows.Next() {
		var rec rewardRecord
		err := rows.Scan(&rec.BlockHeight, &rec.BlockTime, &rec.Sender, &rec.Recipient, &rec.Amount, &rec.RewardType)
		require.NoError(t, err)
		records = append(records, rec)
	}
	require.NoError(t, rows.Err())
	return records
}

// fetchRewardsByType queries reward records filtered by type.
func fetchRewardsByType(t *testing.T, db blockdb.SQLDB, rewardType string) []rewardRecord {
	t.Helper()
	ctx := context.Background()

	rows, err := db.Query(ctx, fmt.Sprintf("SELECT %s, %s, %s, %s, toString(%s), %s FROM %s WHERE %s = ? ORDER BY %s, %s",
		blockdb.ColBlockHeight, blockdb.ColBlockTime, blockdb.ColSender, blockdb.ColRecipient, blockdb.ColAmount, blockdb.ColType,
		blockdb.TableNameRewards, blockdb.ColType, blockdb.ColBlockHeight, blockdb.ColRecipient), rewardType)
	require.NoError(t, err)
	defer rows.Close()

	var records []rewardRecord
	for rows.Next() {
		var rec rewardRecord
		err := rows.Scan(&rec.BlockHeight, &rec.BlockTime, &rec.Sender, &rec.Recipient, &rec.Amount, &rec.RewardType)
		require.NoError(t, err)
		records = append(records, rec)
	}
	require.NoError(t, rows.Err())
	return records
}

func TestInsertBlockSigns_SignedBlock(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	// Create block with signed commit
	validatorAddr := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
	}

	// Block at height 100, LastCommit contains signatures for height 99
	blockEv := ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{
				Height: 100,
				Time:   time.Now(),
			},
			LastCommit: &ctypes.Commit{
				Signatures: []ctypes.CommitSig{
					{
						BlockIDFlag:      ctypes.BlockIDFlagCommit,
						ValidatorAddress: validatorAddr,
						Timestamp:        time.Now(),
					},
				},
			},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{},
	}

	p.insertBlockSigns(blockEv)
	require.NoError(t, p.Flush(ctx))

	// Verify DB records
	records := fetchBlockSigns(t, db)
	require.Len(t, records, 1)

	// Verify height is 99 (height-1), not 100
	assert.Equal(t, int64(99), records[0].BlockHeight, "should record height-1 since LastCommit is for previous block")

	// Verify signed=1
	assert.Equal(t, uint8(1), records[0].Signed, "signed block should have signed=1")
}

func TestInsertBlockSigns_MissedBlock(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	validatorAddr := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
	}

	// Non-commit flag indicates missed block
	blockEv := ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{
				Height: 100,
				Time:   time.Now(),
			},
			LastCommit: &ctypes.Commit{
				Signatures: []ctypes.CommitSig{
					{
						BlockIDFlag:      ctypes.BlockIDFlagAbsent, // Missed!
						ValidatorAddress: validatorAddr,
						Timestamp:        time.Now(),
					},
				},
			},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{},
	}

	p.insertBlockSigns(blockEv)
	require.NoError(t, p.Flush(ctx))

	records := fetchBlockSigns(t, db)
	require.Len(t, records, 1)

	// Verify signed=0 for missed block
	assert.Equal(t, uint8(0), records[0].Signed, "missed block should have signed=0")
}

func TestInsertBlockSigns_EmptyAddressSkipped(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	blockEv := ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{
				Height: 100,
				Time:   time.Now(),
			},
			LastCommit: &ctypes.Commit{
				Signatures: []ctypes.CommitSig{
					{
						BlockIDFlag:      ctypes.BlockIDFlagCommit,
						ValidatorAddress: []byte{}, // Empty address
						Timestamp:        time.Now(),
					},
				},
			},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{},
	}

	p.insertBlockSigns(blockEv)
	require.NoError(t, p.Flush(ctx))

	// No records should be inserted for empty address
	records := fetchBlockSigns(t, db)
	assert.Empty(t, records, "should skip empty validator address")
}

func TestInsertBlockSigns_GenesisBlockSkipped(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	validatorAddr := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
	}

	// Genesis block at height 1 - should be skipped since there's no previous block
	blockEv := ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{
				Height: 1, // Genesis
				Time:   time.Now(),
			},
			LastCommit: &ctypes.Commit{
				Signatures: []ctypes.CommitSig{
					{
						BlockIDFlag:      ctypes.BlockIDFlagCommit,
						ValidatorAddress: validatorAddr,
						Timestamp:        time.Now(),
					},
				},
			},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{},
	}

	p.insertBlockSigns(blockEv)
	require.NoError(t, p.Flush(ctx))

	// Genesis block should be skipped
	records := fetchBlockSigns(t, db)
	assert.Empty(t, records, "genesis block should be skipped")
}

func TestInsertBlockSigns_NilVote(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	validatorAddr := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
	}

	blockEv := ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{
				Height: 100,
				Time:   time.Now(),
			},
			LastCommit: &ctypes.Commit{
				Signatures: []ctypes.CommitSig{
					{
						BlockIDFlag:      ctypes.BlockIDFlagNil, // Voted nil
						ValidatorAddress: validatorAddr,
						Timestamp:        time.Now(),
					},
				},
			},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{},
	}

	p.insertBlockSigns(blockEv)
	require.NoError(t, p.Flush(ctx))

	records := fetchBlockSigns(t, db)
	require.Len(t, records, 1)

	// Nil vote should have signed=0 (not a commit)
	assert.Equal(t, uint8(0), records[0].Signed, "nil vote should have signed=0")
}

func TestInsertBlockSigns_MultipleValidators(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	addr1 := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
	}
	addr2 := []byte{
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a,
		0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34,
	}
	addr3 := []byte{
		0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a,
		0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x51, 0x52, 0x53, 0x54,
	}

	blockEv := ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{
				Height: 100,
				Time:   time.Now(),
			},
			LastCommit: &ctypes.Commit{
				Signatures: []ctypes.CommitSig{
					{BlockIDFlag: ctypes.BlockIDFlagCommit, ValidatorAddress: addr1}, // Signed
					{BlockIDFlag: ctypes.BlockIDFlagAbsent, ValidatorAddress: addr2}, // Missed
					{BlockIDFlag: ctypes.BlockIDFlagCommit, ValidatorAddress: addr3}, // Signed
				},
			},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{},
	}

	p.insertBlockSigns(blockEv)
	require.NoError(t, p.Flush(ctx))

	records := fetchBlockSigns(t, db)
	require.Len(t, records, 3, "should record all 3 validators")

	// Build a map of validator address -> signed status for flexible assertion
	signedByAddr := make(map[string]uint8)
	for _, rec := range records {
		signedByAddr[rec.ValidatorAddress] = rec.Signed
	}

	// addr1 should be signed=1, addr2 should be signed=0, addr3 should be signed=1
	assert.Len(t, signedByAddr, 3, "should have 3 unique validators")
	// Count signed vs missed
	var signedCount, missedCount int
	for _, signed := range signedByAddr {
		if signed == 1 {
			signedCount++
		} else {
			missedCount++
		}
	}
	assert.Equal(t, 2, signedCount, "should have 2 signed validators")
	assert.Equal(t, 1, missedCount, "should have 1 missed validator")
}

func TestStoreReport_Basic(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	// Create report event
	reportEvent := abci.Event{
		Type: "new_report",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyReporter, Value: "tellor1abc123"},
			{Key: AttrKeyPower, Value: "1000000"},
			{Key: AttrKeyQueryType, Value: "SpotPrice"},
			{Key: AttrKeyQueryID, Value: "83a7f3d48786ac2667503a61e8c415438ed2922eb86a2906e4ee66d9a2ce4992"},
			{Key: AttrKeyAggMethod, Value: "weighted-median"},
			{Key: AttrKeyValue, Value: "0x1234"},
			{Key: AttrKeyTimestamp, Value: "2024-01-01T00:00:00Z"},
			{Key: AttrKeyCyclelist, Value: "true"},
			{Key: AttrKeyBlockNumber, Value: "100"},
			{Key: AttrKeyMetaID, Value: "1"},
		},
	}

	report, err := DecodeReportEvent(100, reportEvent)
	require.NoError(t, err)
	require.NotNil(t, report)

	err = p.storeReport(time.Now(), *report)
	require.NoError(t, err)
	require.NoError(t, p.Flush(ctx))

	records := fetchReports(t, db)
	require.Len(t, records, 1)
	assert.Equal(t, "tellor1abc123", records[0].Reporter)
	assert.Equal(t, uint64(1000000), records[0].Power)
	assert.Equal(t, "SpotPrice", records[0].QueryType)
}

func TestProcessBlock_Deduplication(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	blockEv := ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{Height: 100, Time: time.Now()},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{
			Events: []abci.Event{
				createReportEvent("tellor1reporter1", "100"),
			},
		},
	}

	// Process same block twice
	p.ProcessBlock(ctx, blockEv)
	recordsAfterFirst := len(fetchReports(t, db))

	p.ProcessBlock(ctx, blockEv)
	recordsAfterSecond := len(fetchReports(t, db))

	// Second process should be skipped
	assert.Equal(t, recordsAfterFirst, recordsAfterSecond,
		"deduplication should prevent second processing")
}

func TestInsertReporterReward_Basic(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	// Use valid bech32 address
	validAddr := "tellor1x6n9dgye3qqn7sl9svlesxcca426tl9xcqu7c7"
	blockTime := time.Now()

	// First event: populates cache baseline (no insert)
	baselineEvent := abci.Event{
		Type: "rewards_added",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyDelegator, Value: validAddr},
			{Key: AttrKeyAmount, Value: "500000"},
		},
	}
	err := p.insertReporterReward(ctx, 99, blockTime, baselineEvent, "queryid0")
	require.NoError(t, err)
	require.Len(t, fetchRewards(t, db), 0, "first event should only populate cache, no insert")

	// Second event: stores the increment
	rewardEvent := abci.Event{
		Type: "rewards_added",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyDelegator, Value: validAddr},
			{Key: AttrKeyAmount, Value: "1500000"}, // cumulative: 1500000, prev: 500000, increment: 1000000
		},
	}

	err = p.insertReporterReward(ctx, 100, blockTime, rewardEvent, "83a7f3d48786ac2667503a61e8c415438ed2922eb86a2906e4ee66d9a2ce4992")
	require.NoError(t, err)

	// Flush buffered records to database
	require.NoError(t, p.Flush(ctx))

	records := fetchRewardsByType(t, db, blockdb.RewardTypeReporterTip)
	require.Len(t, records, 1)

	// Verify stored values
	assert.Equal(t, int64(100), records[0].BlockHeight)
	assert.Equal(t, validAddr, records[0].Sender)
	assert.Equal(t, validAddr, records[0].Recipient)
	assert.Equal(t, blockdb.RewardTypeReporterTip, records[0].RewardType)
	assert.Equal(t, "1000000", records[0].Amount) // increment = 1500000 - 500000
}

func TestInsertReporterReward_MultipleRewards(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)
	blockTime := time.Now()

	// Use valid bech32 addresses
	addrs := []string{
		"tellor1x6n9dgye3qqn7sl9svlesxcca426tl9xcqu7c7",
		"tellor14anzpcluwawzy27e7l982fwc3h65ep82mqz8zg",
		"tellor1tygms3xhhs3yv487phx3dw4a95jn7t7lpdv94k",
	}

	// First pass: populate cache baselines (no inserts)
	baselines := []abci.Event{
		{Type: "rewards_added", Attributes: []abci.EventAttribute{
			{Key: AttrKeyDelegator, Value: addrs[0]},
			{Key: AttrKeyAmount, Value: "50"},
		}},
		{Type: "rewards_added", Attributes: []abci.EventAttribute{
			{Key: AttrKeyDelegator, Value: addrs[1]},
			{Key: AttrKeyAmount, Value: "100"},
		}},
		{Type: "rewards_added", Attributes: []abci.EventAttribute{
			{Key: AttrKeyDelegator, Value: addrs[2]},
			{Key: AttrKeyAmount, Value: "150"},
		}},
	}
	for _, ev := range baselines {
		err := p.insertReporterReward(ctx, 99, blockTime, ev, "queryid0")
		require.NoError(t, err)
	}
	require.Len(t, fetchRewards(t, db), 0, "baseline events should not insert")

	// Second pass: now these should insert increments
	rewards := []abci.Event{
		{Type: "rewards_added", Attributes: []abci.EventAttribute{
			{Key: AttrKeyDelegator, Value: addrs[0]},
			{Key: AttrKeyAmount, Value: "150"}, // cumulative 150, prev 50, increment 100
		}},
		{Type: "rewards_added", Attributes: []abci.EventAttribute{
			{Key: AttrKeyDelegator, Value: addrs[1]},
			{Key: AttrKeyAmount, Value: "300"}, // cumulative 300, prev 100, increment 200
		}},
		{Type: "rewards_added", Attributes: []abci.EventAttribute{
			{Key: AttrKeyDelegator, Value: addrs[2]},
			{Key: AttrKeyAmount, Value: "450"}, // cumulative 450, prev 150, increment 300
		}},
	}

	for _, ev := range rewards {
		err := p.insertReporterReward(ctx, 100, blockTime, ev, "queryid123")
		require.NoError(t, err)
	}

	// Flush buffered records to database
	require.NoError(t, p.Flush(ctx))

	records := fetchRewardsByType(t, db, blockdb.RewardTypeReporterTip)
	assert.Len(t, records, 3, "should record all 3 rewards")
}

func TestDecodeReportEvent_QueryID(t *testing.T) {
	expectedQueryID := "83a7f3d48786ac2667503a61e8c415438ed2922eb86a2906e4ee66d9a2ce4992"

	reportEvent := abci.Event{
		Type: "new_report",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyReporter, Value: "tellor1abc"},
			{Key: AttrKeyPower, Value: "1000"},
			{Key: AttrKeyQueryType, Value: "SpotPrice"},
			{Key: AttrKeyQueryID, Value: expectedQueryID},
			{Key: AttrKeyAggMethod, Value: "median"},
			{Key: AttrKeyValue, Value: "0x1234"},
			{Key: AttrKeyTimestamp, Value: "1704067200000"}, // Unix millis
			{Key: AttrKeyCyclelist, Value: "false"},
			{Key: AttrKeyBlockNumber, Value: "100"},
			{Key: AttrKeyMetaID, Value: "1"},
		},
	}

	report, err := DecodeReportEvent(100, reportEvent)
	require.NoError(t, err)

	// Encode back and verify
	encoded, err := EncodeQueryID(report.QueryId)
	require.NoError(t, err)
	assert.Equal(t, expectedQueryID, encoded)
}

func TestDecodeReportEvent_TimestampParsing(t *testing.T) {
	cases := []struct {
		name      string
		timestamp string
		expected  time.Time
	}{
		{
			name:      "RFC3339 format",
			timestamp: "2024-01-01T12:30:00Z",
			expected:  time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC),
		},
		{
			name:      "Unix milliseconds",
			timestamp: "1704106200000",
			expected:  time.UnixMilli(1704106200000).UTC(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reportEvent := abci.Event{
				Type: "new_report",
				Attributes: []abci.EventAttribute{
					{Key: AttrKeyReporter, Value: "tellor1abc"},
					{Key: AttrKeyPower, Value: "1000"},
					{Key: AttrKeyQueryType, Value: "SpotPrice"},
					{Key: AttrKeyQueryID, Value: "83a7f3d48786ac2667503a61e8c415438ed2922eb86a2906e4ee66d9a2ce4992"},
					{Key: AttrKeyAggMethod, Value: "median"},
					{Key: AttrKeyValue, Value: "0x1234"},
					{Key: AttrKeyTimestamp, Value: tc.timestamp},
					{Key: AttrKeyCyclelist, Value: "false"},
					{Key: AttrKeyBlockNumber, Value: "100"},
					{Key: AttrKeyMetaID, Value: "1"},
				},
			}

			report, err := DecodeReportEvent(100, reportEvent)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, report.Timestamp)
		})
	}
}

func TestDecodeReportEvent_PowerValue(t *testing.T) {
	reportEvent := abci.Event{
		Type: "new_report",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyReporter, Value: "tellor1abc"},
			{Key: AttrKeyPower, Value: "1000000"},
			{Key: AttrKeyQueryType, Value: "SpotPrice"},
			{Key: AttrKeyQueryID, Value: "83a7f3d48786ac2667503a61e8c415438ed2922eb86a2906e4ee66d9a2ce4992"},
			{Key: AttrKeyAggMethod, Value: "median"},
			{Key: AttrKeyValue, Value: "0x1234"},
			{Key: AttrKeyTimestamp, Value: "2024-01-01T00:00:00Z"},
			{Key: AttrKeyCyclelist, Value: "false"},
			{Key: AttrKeyBlockNumber, Value: "100"},
			{Key: AttrKeyMetaID, Value: "1"},
		},
	}

	report, err := DecodeReportEvent(100, reportEvent)
	require.NoError(t, err)
	assert.Equal(t, uint64(1000000), report.Power)
}

func TestDecodeReportEvent_BlockNumberFallback(t *testing.T) {
	tests := []struct {
		name           string
		height         int64
		hasBlockNumber bool
		blockNumberVal string
		expectedBlock  uint64
	}{
		{
			name:           "block_number attribute present",
			height:         500,
			hasBlockNumber: true,
			blockNumberVal: "100",
			expectedBlock:  100,
		},
		{
			name:           "block_number attribute missing - uses height",
			height:         500,
			hasBlockNumber: false,
			blockNumberVal: "",
			expectedBlock:  500,
		},
		{
			name:           "block_number is zero - uses height",
			height:         500,
			hasBlockNumber: true,
			blockNumberVal: "0",
			expectedBlock:  500,
		},
		{
			name:           "block_number missing and height is zero",
			height:         0,
			hasBlockNumber: false,
			blockNumberVal: "",
			expectedBlock:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attrs := []abci.EventAttribute{
				{Key: AttrKeyReporter, Value: "tellor1abc"},
				{Key: AttrKeyPower, Value: "1000"},
				{Key: AttrKeyQueryType, Value: "SpotPrice"},
				{Key: AttrKeyQueryID, Value: "83a7f3d48786ac2667503a61e8c415438ed2922eb86a2906e4ee66d9a2ce4992"},
				{Key: AttrKeyAggMethod, Value: "median"},
				{Key: AttrKeyValue, Value: "0x1234"},
				{Key: AttrKeyTimestamp, Value: "2024-01-01T00:00:00Z"},
				{Key: AttrKeyCyclelist, Value: "false"},
				{Key: AttrKeyMetaID, Value: "1"},
			}

			if tc.hasBlockNumber {
				attrs = append(attrs, abci.EventAttribute{Key: AttrKeyBlockNumber, Value: tc.blockNumberVal})
			}

			reportEvent := abci.Event{
				Type:       "new_report",
				Attributes: attrs,
			}

			report, err := DecodeReportEvent(tc.height, reportEvent)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedBlock, report.BlockNumber, "BlockNumber mismatch")
		})
	}
}

func TestInsertReporterReward_MissingDelegator(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)
	blockTime := time.Now()

	rewardEvent := abci.Event{
		Type: "rewards_added",
		Attributes: []abci.EventAttribute{
			// Missing delegator
			{Key: AttrKeyAmount, Value: "1000000"},
		},
	}

	err := p.insertReporterReward(ctx, 100, blockTime, rewardEvent, "queryid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delegator")
}

func TestInsertReporterReward_MissingAmount(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)
	blockTime := time.Now()

	rewardEvent := abci.Event{
		Type: "rewards_added",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyDelegator, Value: "tellor1x6n9dgye3qqn7sl9svlesxcca426tl9xcqu7c7"},
			// Missing amount
		},
	}

	err := p.insertReporterReward(ctx, 100, blockTime, rewardEvent, "queryid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "amount")
}

func createReportEvent(reporter, blockNum string) abci.Event {
	return abci.Event{
		Type: "new_report",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyReporter, Value: reporter},
			{Key: AttrKeyPower, Value: "1000000"},
			{Key: AttrKeyQueryType, Value: "SpotPrice"},
			{Key: AttrKeyQueryID, Value: "83a7f3d48786ac2667503a61e8c415438ed2922eb86a2906e4ee66d9a2ce4992"},
			{Key: AttrKeyAggMethod, Value: "median"},
			{Key: AttrKeyValue, Value: "0x1234"},
			{Key: AttrKeyTimestamp, Value: "2024-01-01T00:00:00Z"},
			{Key: AttrKeyCyclelist, Value: "false"},
			{Key: AttrKeyBlockNumber, Value: blockNum},
			{Key: AttrKeyMetaID, Value: "1"},
		},
	}
}

func TestInsertValidatorReward_Commission(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)
	blockTime := time.Now()

	// First event: populate cache baseline (no insert)
	baselineEvent := abci.Event{
		Type: "commission",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyAmount, Value: "50.000000loya"},
			{Key: "validator", Value: "tellorvaloper1abc123def456"},
		},
	}
	err := p.insertValidatorReward(ctx, 99, blockTime, baselineEvent, blockdb.RewardTypeValidatorCommission)
	require.NoError(t, err)
	require.Len(t, fetchRewardsByType(t, db, blockdb.RewardTypeValidatorCommission), 0, "first event should only populate cache")

	// Second event: stores the increment
	commissionEvent := abci.Event{
		Type: "commission",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyAmount, Value: "173.456789loya"}, // cumulative 173.456789, prev 50, increment 123.456789
			{Key: "validator", Value: "tellorvaloper1abc123def456"},
		},
	}

	err = p.insertValidatorReward(ctx, 100, blockTime, commissionEvent, blockdb.RewardTypeValidatorCommission)
	require.NoError(t, err)

	// Flush buffered records to database
	require.NoError(t, p.Flush(ctx))

	records := fetchRewardsByType(t, db, blockdb.RewardTypeValidatorCommission)
	require.Len(t, records, 1)

	// Verify stored values
	assert.Equal(t, int64(100), records[0].BlockHeight)
	assert.Equal(t, "tellorvaloper1abc123def456", records[0].Sender)
	assert.Equal(t, "tellorvaloper1abc123def456", records[0].Recipient)
	assert.Equal(t, blockdb.RewardTypeValidatorCommission, records[0].RewardType)
}

func TestInsertValidatorReward_Delegator(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)
	blockTime := time.Now()

	// First event: populate cache baseline (no insert)
	baselineEvent := abci.Event{
		Type: "rewards",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyAmount, Value: "500.000000loya"},
			{Key: "validator", Value: "tellorvaloper1xyz789"},
		},
	}
	err := p.insertValidatorReward(ctx, 199, blockTime, baselineEvent, blockdb.RewardTypeValidatorDelegator)
	require.NoError(t, err)
	require.Len(t, fetchRewardsByType(t, db, blockdb.RewardTypeValidatorDelegator), 0, "first event should only populate cache")

	// Second event: stores the increment
	rewardsEvent := abci.Event{
		Type: "rewards",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyAmount, Value: "1500.000000loya"}, // cumulative 1500, prev 500, increment 1000
			{Key: "validator", Value: "tellorvaloper1xyz789"},
		},
	}

	err = p.insertValidatorReward(ctx, 200, blockTime, rewardsEvent, blockdb.RewardTypeValidatorDelegator)
	require.NoError(t, err)

	// Flush buffered records to database
	require.NoError(t, p.Flush(ctx))

	records := fetchRewardsByType(t, db, blockdb.RewardTypeValidatorDelegator)
	require.Len(t, records, 1)

	// Verify stored values
	assert.Equal(t, int64(200), records[0].BlockHeight)
	assert.Equal(t, "tellorvaloper1xyz789", records[0].Sender)
	assert.Equal(t, "tellorvaloper1xyz789", records[0].Recipient)
	assert.Equal(t, blockdb.RewardTypeValidatorDelegator, records[0].RewardType)
}

func TestInsertValidatorReward_MissingValidator(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)
	blockTime := time.Now()

	// Event missing validator attribute
	event := abci.Event{
		Type: "commission",
		Attributes: []abci.EventAttribute{
			{Key: AttrKeyAmount, Value: "100loya"},
			// Missing validator
		},
	}

	err := p.insertValidatorReward(ctx, 100, blockTime, event, blockdb.RewardTypeValidatorCommission)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validator")
}

func TestInsertValidatorReward_MissingAmount(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)
	blockTime := time.Now()

	// Event missing amount attribute
	event := abci.Event{
		Type: "rewards",
		Attributes: []abci.EventAttribute{
			{Key: "validator", Value: "tellorvaloper1abc"},
		},
	}

	err := p.insertValidatorReward(ctx, 100, blockTime, event, blockdb.RewardTypeValidatorDelegator)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing amount")
}

func TestInsertValidatorReward_MultipleValidators(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)
	blockTime := time.Now()

	validators := []string{
		"tellorvaloper1aaa",
		"tellorvaloper1bbb",
		"tellorvaloper1ccc",
	}

	// First pass: populate cache baselines (no inserts)
	for i, val := range validators {
		commEvent := abci.Event{
			Type: "commission",
			Attributes: []abci.EventAttribute{
				{Key: AttrKeyAmount, Value: fmt.Sprintf("%d.000000loya", (i+1)*50)},
				{Key: "validator", Value: val},
			},
		}
		err := p.insertValidatorReward(ctx, 99, blockTime, commEvent, blockdb.RewardTypeValidatorCommission)
		require.NoError(t, err)

		rewEvent := abci.Event{
			Type: "rewards",
			Attributes: []abci.EventAttribute{
				{Key: AttrKeyAmount, Value: fmt.Sprintf("%d.000000loya", (i+1)*500)},
				{Key: "validator", Value: val},
			},
		}
		err = p.insertValidatorReward(ctx, 99, blockTime, rewEvent, blockdb.RewardTypeValidatorDelegator)
		require.NoError(t, err)
	}
	require.Len(t, fetchRewards(t, db), 0, "baseline events should not insert")

	// Second pass: now these should insert increments
	for i, val := range validators {
		commEvent := abci.Event{
			Type: "commission",
			Attributes: []abci.EventAttribute{
				// cumulative = prev + increment (prev = (i+1)*50, increment = (i+1)*50)
				{Key: AttrKeyAmount, Value: fmt.Sprintf("%d.000000loya", (i+1)*100)},
				{Key: "validator", Value: val},
			},
		}
		err := p.insertValidatorReward(ctx, 100, blockTime, commEvent, blockdb.RewardTypeValidatorCommission)
		require.NoError(t, err)

		rewEvent := abci.Event{
			Type: "rewards",
			Attributes: []abci.EventAttribute{
				// cumulative = prev + increment (prev = (i+1)*500, increment = (i+1)*500)
				{Key: AttrKeyAmount, Value: fmt.Sprintf("%d.000000loya", (i+1)*1000)},
				{Key: "validator", Value: val},
			},
		}
		err = p.insertValidatorReward(ctx, 100, blockTime, rewEvent, blockdb.RewardTypeValidatorDelegator)
		require.NoError(t, err)
	}

	// Flush buffered records to database
	require.NoError(t, p.Flush(ctx))

	records := fetchRewards(t, db)
	assert.Len(t, records, 6, "should record 3 commission + 3 delegator reward events")
}

func TestProcessBlock_ValidatorRewardEvents(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	// First block: populate cache baselines (no inserts)
	baselineBlock := ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{Height: 99, Time: time.Now()},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{
			Events: []abci.Event{
				{
					Type: "commission",
					Attributes: []abci.EventAttribute{
						{Key: AttrKeyAmount, Value: "25.000000loya"},
						{Key: "validator", Value: "tellorvaloper1test"},
					},
				},
				{
					Type: "rewards",
					Attributes: []abci.EventAttribute{
						{Key: AttrKeyAmount, Value: "250.000000loya"},
						{Key: "validator", Value: "tellorvaloper1test"},
					},
				},
			},
		},
	}
	p.ProcessBlock(ctx, baselineBlock)
	require.NoError(t, p.Flush(ctx)) // Flush any buffered records
	require.Len(t, fetchRewards(t, db), 0, "baseline block should not insert")

	// Second block: these should insert increments
	blockEv := ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{Height: 100, Time: time.Now()},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{
			Events: []abci.Event{
				// Commission event: cumulative 75, prev 25, increment 50
				{
					Type: "commission",
					Attributes: []abci.EventAttribute{
						{Key: AttrKeyAmount, Value: "75.000000loya"},
						{Key: "validator", Value: "tellorvaloper1test"},
					},
				},
				// Rewards event: cumulative 750, prev 250, increment 500
				{
					Type: "rewards",
					Attributes: []abci.EventAttribute{
						{Key: AttrKeyAmount, Value: "750.000000loya"},
						{Key: "validator", Value: "tellorvaloper1test"},
					},
				},
			},
		},
	}

	p.ProcessBlock(ctx, blockEv)
	require.NoError(t, p.Flush(ctx)) // Flush buffered records to database

	records := fetchRewards(t, db)
	// Should have 2 records: one for commission, one for delegator rewards
	require.Len(t, records, 2)

	// Records are ordered by type, recipient
	commRecords := fetchRewardsByType(t, db, blockdb.RewardTypeValidatorCommission)
	require.Len(t, commRecords, 1)
	assert.Equal(t, blockdb.RewardTypeValidatorCommission, commRecords[0].RewardType)
	assert.Equal(t, "tellorvaloper1test", commRecords[0].Sender)
	assert.Equal(t, int64(100), commRecords[0].BlockHeight)

	delRecords := fetchRewardsByType(t, db, blockdb.RewardTypeValidatorDelegator)
	require.Len(t, delRecords, 1)
	assert.Equal(t, blockdb.RewardTypeValidatorDelegator, delRecords[0].RewardType)
	assert.Equal(t, "tellorvaloper1test", delRecords[0].Sender)
	assert.Equal(t, int64(100), delRecords[0].BlockHeight)
}

// ============================================================================
// Cycle Rotation Tests
// ============================================================================

// cycleRotationRecord represents a row from the cycle_rotations table.
type cycleRotationRecord struct {
	BlockHeight int64
	QueryID     string
	Timestamp   time.Time
}

// fetchCycleRotations queries all cycle_rotations records from the database.
func fetchCycleRotations(t *testing.T, db blockdb.SQLDB) []cycleRotationRecord {
	t.Helper()
	ctx := context.Background()

	rows, err := db.Query(ctx, fmt.Sprintf("SELECT %s, %s, %s FROM %s ORDER BY %s",
		blockdb.ColBlockHeight, blockdb.ColQueryID, blockdb.ColTimestamp,
		blockdb.TableNameCycleRotations, blockdb.ColBlockHeight))
	require.NoError(t, err)
	defer rows.Close()

	var records []cycleRotationRecord
	for rows.Next() {
		var rec cycleRotationRecord
		err := rows.Scan(&rec.BlockHeight, &rec.QueryID, &rec.Timestamp)
		require.NoError(t, err)
		records = append(records, rec)
	}
	require.NoError(t, rows.Err())
	return records
}

// setupCycleTestProcessor creates a processor for cycle rotation tests.
// Uses setupTestDB which already truncates cycle_rotations table.
func setupCycleTestProcessor(t *testing.T) (*Processor, blockdb.SQLDB) {
	t.Helper()
	db := setupTestDB(t)
	p := New(context.Background(), cryptolog.New(), db)
	return p, db
}

func TestHandleCycleRotation_FirstCycle(t *testing.T) {
	ctx := context.Background()
	p, db := setupCycleTestProcessor(t)
	blockTime := time.Now()

	// First cycle - should initialize without warning
	p.handleCycleRotation(ctx, 100, blockTime, "query-id-1")
	require.NoError(t, p.Flush(ctx))

	// Verify cycle was recorded
	records := fetchCycleRotations(t, db)
	require.Len(t, records, 1)
	assert.Equal(t, int64(100), records[0].BlockHeight)
	assert.Equal(t, "query-id-1", records[0].QueryID)

	// Verify internal state was set
	assert.Equal(t, "query-id-1", p.currentCycleQueryID)
}

func TestHandleCycleRotation_NormalRotation(t *testing.T) {
	ctx := context.Background()
	p, db := setupCycleTestProcessor(t)
	blockTime := time.Now()

	// First cycle
	p.handleCycleRotation(ctx, 100, blockTime, "query-id-1")

	// Second cycle - normal rotation
	p.handleCycleRotation(ctx, 200, blockTime.Add(time.Minute), "query-id-2")
	require.NoError(t, p.Flush(ctx))

	// Verify both cycles were recorded
	records := fetchCycleRotations(t, db)
	require.Len(t, records, 2)
	assert.Equal(t, int64(100), records[0].BlockHeight)
	assert.Equal(t, "query-id-1", records[0].QueryID)
	assert.Equal(t, int64(200), records[1].BlockHeight)
	assert.Equal(t, "query-id-2", records[1].QueryID)

	// Verify internal state was updated
	assert.Equal(t, "query-id-2", p.currentCycleQueryID)
}

func TestHandleCycleRotation_ResetsReportersInCycle(t *testing.T) {
	ctx := context.Background()
	p, _ := setupCycleTestProcessor(t)
	blockTime := time.Now()

	// First cycle
	p.handleCycleRotation(ctx, 100, blockTime, "query-id-1")

	// Mark reporters as having reported in cycle
	p.markReporterReportedInCycle("tellor1reporter1")
	p.markReporterReportedInCycle("tellor1reporter2")

	// Verify reporters are tracked
	assert.Len(t, p.reportersInCurrentCycle, 2)

	// Rotate to next cycle - should reset reporters
	p.handleCycleRotation(ctx, 200, blockTime.Add(time.Minute), "query-id-2")

	// Verify reporters map was reset
	assert.Len(t, p.reportersInCurrentCycle, 0)
}

func TestHandleCycleRotation_SameQueryID_NoRotation(t *testing.T) {
	ctx := context.Background()
	p, db := setupCycleTestProcessor(t)
	blockTime := time.Now()

	// First cycle
	p.handleCycleRotation(ctx, 100, blockTime, "query-id-1")

	// Mark a reporter
	p.markReporterReportedInCycle("tellor1reporter1")
	assert.Len(t, p.reportersInCurrentCycle, 1)

	// Same query ID - should NOT rotate or reset reporters
	p.handleCycleRotation(ctx, 101, blockTime.Add(time.Second), "query-id-1")
	require.NoError(t, p.Flush(ctx))

	// Verify only one cycle was recorded (no duplicate)
	records := fetchCycleRotations(t, db)
	require.Len(t, records, 1)

	// Verify reporters were NOT reset
	assert.Len(t, p.reportersInCurrentCycle, 1)
}

func TestMarkReporterReportedInCycle(t *testing.T) {
	ctx := context.Background()
	p, _ := setupCycleTestProcessor(t)
	blockTime := time.Now()

	// Initialize first cycle
	p.handleCycleRotation(ctx, 100, blockTime, "query-id-1")

	// Mark reporter
	p.markReporterReportedInCycle("tellor1reporter1")

	// Verify reporter is tracked
	_, ok := p.reportersInCurrentCycle["tellor1reporter1"]
	assert.True(t, ok, "reporter should be tracked in current cycle")

	// Verify reporter is added to known reporters
	_, known := p.knownReporters["tellor1reporter1"]
	assert.True(t, known, "reporter should be added to known reporters")
}

func TestMarkReporterReportedInCycle_MultipleSameReporter(t *testing.T) {
	ctx := context.Background()
	p, _ := setupCycleTestProcessor(t)
	blockTime := time.Now()

	// Initialize first cycle
	p.handleCycleRotation(ctx, 100, blockTime, "query-id-1")

	// Mark same reporter multiple times (should not cause issues)
	p.markReporterReportedInCycle("tellor1reporter1")
	p.markReporterReportedInCycle("tellor1reporter1")
	p.markReporterReportedInCycle("tellor1reporter1")

	// Verify only one entry exists
	assert.Len(t, p.reportersInCurrentCycle, 1)
	assert.Len(t, p.knownReporters, 1)
}

func TestMarkReporterReportedInCycle_MultipleReporters(t *testing.T) {
	ctx := context.Background()
	p, _ := setupCycleTestProcessor(t)
	blockTime := time.Now()

	// Initialize first cycle
	p.handleCycleRotation(ctx, 100, blockTime, "query-id-1")

	// Mark multiple different reporters
	reporters := []string{
		"tellor1reporter1",
		"tellor1reporter2",
		"tellor1reporter3",
		"tellor1reporter4",
	}
	for _, r := range reporters {
		p.markReporterReportedInCycle(r)
	}

	// Verify all reporters are tracked
	assert.Len(t, p.reportersInCurrentCycle, 4)
	assert.Len(t, p.knownReporters, 4)

	for _, r := range reporters {
		_, ok := p.reportersInCurrentCycle[r]
		assert.True(t, ok, "reporter %s should be tracked", r)
	}
}

func TestRecordCycleRotation_WritesToDB(t *testing.T) {
	ctx := context.Background()
	p, db := setupCycleTestProcessor(t)
	blockTime := time.Now().UTC().Truncate(time.Second)

	// Record a cycle rotation
	p.recordCycleRotation(ctx, 12345, blockTime, "test-query-id-abc123")
	require.NoError(t, p.Flush(ctx))

	// Verify DB record
	records := fetchCycleRotations(t, db)
	require.Len(t, records, 1)
	assert.Equal(t, int64(12345), records[0].BlockHeight)
	assert.Equal(t, "test-query-id-abc123", records[0].QueryID)
}

func TestHandleCycleRotation_KnownReportersPersist(t *testing.T) {
	ctx := context.Background()
	p, _ := setupCycleTestProcessor(t)
	blockTime := time.Now()

	// First cycle
	p.handleCycleRotation(ctx, 100, blockTime, "query-id-1")
	p.markReporterReportedInCycle("tellor1reporter1")

	// Second cycle
	p.handleCycleRotation(ctx, 200, blockTime.Add(time.Minute), "query-id-2")
	p.markReporterReportedInCycle("tellor1reporter2")

	// Verify knownReporters accumulates across cycles
	assert.Len(t, p.knownReporters, 2)
	_, known1 := p.knownReporters["tellor1reporter1"]
	_, known2 := p.knownReporters["tellor1reporter2"]
	assert.True(t, known1, "reporter1 should be in known reporters")
	assert.True(t, known2, "reporter2 should be in known reporters")

	// But reportersInCurrentCycle only has current cycle's reporter
	assert.Len(t, p.reportersInCurrentCycle, 1)
	_, inCurrent := p.reportersInCurrentCycle["tellor1reporter2"]
	assert.True(t, inCurrent, "only reporter2 should be in current cycle")
}

// disputeRecord represents a row from the disputes table.
type disputeRecord struct {
	DisputeID   uint64
	BlockHeight uint64
	Status      string
}

// fetchDisputes queries all dispute records (open ones, via the failsafe accessor).
func fetchDisputes(t *testing.T, db blockdb.SQLDB) []disputeRecord {
	t.Helper()
	ctx := context.Background()

	// Use FINAL to collapse rows for the ReplacingMergeTree engine, matching
	// how the dispute monitor reads via blockdb.GetOpenDisputes.
	rows, err := db.Query(ctx, fmt.Sprintf(
		"SELECT %s, %s, %s FROM %s FINAL ORDER BY %s",
		blockdb.ColDisputeID, blockdb.ColBlockHeight, blockdb.ColStatus,
		blockdb.TableNameDisputes, blockdb.ColDisputeID,
	))
	require.NoError(t, err)
	defer rows.Close()

	var records []disputeRecord
	for rows.Next() {
		var rec disputeRecord
		require.NoError(t, rows.Scan(&rec.DisputeID, &rec.BlockHeight, &rec.Status))
		records = append(records, rec)
	}
	require.NoError(t, rows.Err())
	return records
}

// TestProcessBlock_NewDisputeEvent_PersistsToDB verifies that when the
// processor sees a new_dispute event in a block, it inserts the dispute into
// the disputes table so the dispute monitor's failsafe DB path can find it.
func TestProcessBlock_NewDisputeEvent_PersistsToDB(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	blockTime := time.Now().UTC().Truncate(time.Millisecond)
	blockEv := ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{Height: 12345, Time: blockTime},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{
			Events: []abci.Event{
				{
					Type: "new_dispute",
					Attributes: []abci.EventAttribute{
						{Key: AttrKeyDisputeID, Value: "42"},
						{Key: AttrKeyReporter, Value: "tellor1reporter"},
						{Key: AttrKeyDisputer, Value: "tellor1disputer"},
					},
				},
			},
		},
	}

	p.ProcessBlock(ctx, blockEv)
	require.NoError(t, p.Flush(ctx))

	records := fetchDisputes(t, db)
	require.Len(t, records, 1, "new_dispute event should produce one dispute row")
	assert.Equal(t, uint64(42), records[0].DisputeID)
	assert.Equal(t, uint64(12345), records[0].BlockHeight)
	assert.Equal(t, blockdb.DisputeStatusOpen, records[0].Status)

	// Verify the dispute monitor's accessor sees the open dispute.
	open, err := blockdb.GetOpenDisputes(ctx, db)
	require.NoError(t, err)
	assert.Equal(t, []uint64{42}, open)
}

// TestProcessBlock_NewDisputeEvent_MultipleDisputes verifies multiple
// new_dispute events across blocks are all persisted.
func TestProcessBlock_NewDisputeEvent_MultipleDisputes(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	makeBlock := func(height int64, disputeIDs ...string) ctypes.EventDataNewBlock {
		events := make([]abci.Event, 0, len(disputeIDs))
		for _, id := range disputeIDs {
			events = append(events, abci.Event{
				Type: "new_dispute",
				Attributes: []abci.EventAttribute{
					{Key: AttrKeyDisputeID, Value: id},
				},
			})
		}
		return ctypes.EventDataNewBlock{
			Block: &ctypes.Block{
				Header: ctypes.Header{Height: height, Time: time.Now()},
			},
			ResultFinalizeBlock: abci.ResponseFinalizeBlock{Events: events},
		}
	}

	p.ProcessBlock(ctx, makeBlock(100, "1", "2"))
	p.ProcessBlock(ctx, makeBlock(101, "3"))
	require.NoError(t, p.Flush(ctx))

	open, err := blockdb.GetOpenDisputes(ctx, db)
	require.NoError(t, err)
	assert.ElementsMatch(t, []uint64{1, 2, 3}, open)
}

// TestProcessBlock_NewDisputeEvent_InvalidIDIgnored verifies that a
// new_dispute event with a non-numeric dispute_id is dropped silently and
// does NOT halt processing of the rest of the block.
func TestProcessBlock_NewDisputeEvent_InvalidIDIgnored(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	p := New(ctx, cryptolog.New(), db)

	blockEv := ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header: ctypes.Header{Height: 200, Time: time.Now()},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{
			Events: []abci.Event{
				{
					Type: "new_dispute",
					Attributes: []abci.EventAttribute{
						{Key: AttrKeyDisputeID, Value: "not-a-number"},
					},
				},
				{
					Type: "new_dispute",
					Attributes: []abci.EventAttribute{
						{Key: AttrKeyDisputeID, Value: "7"},
					},
				},
			},
		},
	}

	p.ProcessBlock(ctx, blockEv)
	require.NoError(t, p.Flush(ctx))

	open, err := blockdb.GetOpenDisputes(ctx, db)
	require.NoError(t, err)
	assert.Equal(t, []uint64{7}, open, "only the valid dispute id should be stored")
}
