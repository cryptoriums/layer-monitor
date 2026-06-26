package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Default timeout for database operations.
const defaultDBTimeout = 5 * time.Second

// Table names.
const (
	TableNameReports        = "reports"
	TableNameTxs            = "txs"
	TableNameRewards        = "rewards"
	TableNameBlockSigns     = "block_signs"
	TableNameCycleRotations = "cycle_rotations"
	TableNameAddresses      = "addresses"
)

// Column names used across all tables.
// Each column name is defined once and reused wherever that column appears.
const (
	// Common columns (used in multiple tables)
	ColBlockHeight = "block_height"
	ColBlockTime   = "block_time"
	ColTimestamp   = "timestamp"
	ColSender      = "sender"
	ColRecipient   = "recipient"
	ColAmount      = "amount"
	ColType        = "type"
	ColQueryID     = "query_id"

	// Reports table columns
	ColReporter        = "reporter"
	ColPower           = "power"
	ColQueryType       = "query_type"
	ColAggregateMethod = "aggregate_method"
	ColValue           = "value"
	ColCyclelist       = "cyclelist"
	ColBlockNumber     = "block_number"
	ColMetaID          = "meta_id"

	// Txs table columns
	ColTxHash    = "tx_hash"
	ColGasUsed   = "gas_used"
	ColFeeAmount = "fee_amount"

	// Block signs table columns
	ColBlockTimestamp   = "block_timestamp"
	ColValidatorAddress = "validator_address"
	ColSigned           = "signed"

	// Addresses table columns
	ColName      = "name"
	ColAddress   = "address"
	ColUpdatedAt = "updated_at"
)

// Reward types for the unified rewards table.
const (
	RewardTypeValidatorCommission = "validator_commission"
	RewardTypeValidatorDelegator  = "validator_delegator"
	RewardTypeReporterTip         = "reporter_tip"
)

// Address names for the addresses table.
const (
	AddressNameReporter           = "reporter"            // tellor1... format
	AddressNameValidator          = "validator"           // tellorvaloper... format
	AddressNameValidatorConsensus = "validator_consensus" // tellorvalcons... format (for block_signs queries)
)

// SQLDB wraps *sql.DB to satisfy the Db interface with context helpers.
type SQLDB struct{ *sql.DB }

// Db defines the required database methods.
type Db interface {
	Exec(context.Context, string, ...any) (sql.Result, error)
	Query(context.Context, string, ...any) (*sql.Rows, error)
	Prepare(context.Context, string) (*sql.Stmt, error)
}

// New wraps the provided *sql.DB and ensures the monitor tables exist.
func New(ctx context.Context, raw *sql.DB) (SQLDB, error) {
	wrapped := SQLDB{DB: raw}
	if err := EnsureTables(ctx, wrapped); err != nil {
		return SQLDB{}, err
	}
	if err := MigrateSchema(ctx, wrapped); err != nil {
		return SQLDB{}, err
	}
	return wrapped, nil
}

// MigrateSchema applies additive schema changes to existing tables.
// It is idempotent — safe to call on every startup.
func MigrateSchema(ctx context.Context, database Db) error {
	ctx, cancel := context.WithTimeout(ctx, defaultDBTimeout)
	defer cancel()

	// Add block_time column to reports table so alerts can filter by when the
	// report was actually written to the chain, not the oracle query timestamp.
	q := fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s DateTime64(3, 'UTC') DEFAULT toDateTime64(0, 3, 'UTC')",
		TableNameReports, ColBlockTime,
	)
	if _, err := database.Exec(ctx, q); err != nil {
		return fmt.Errorf("migrate reports.block_time: %w", err)
	}
	return nil
}

// EnsureTables creates the required tables if they do not exist.
func EnsureTables(ctx context.Context, database Db) error {
	ctx, cancel := context.WithTimeout(ctx, defaultDBTimeout)
	defer cancel()

	if err := initReportsTable(ctx, database); err != nil {
		return fmt.Errorf("create %s table: %w", TableNameReports, err)
	}
	if err := initTxTable(ctx, database); err != nil {
		return fmt.Errorf("create %s table: %w", TableNameTxs, err)
	}
	if err := initRewardsTable(ctx, database); err != nil {
		return fmt.Errorf("create %s table: %w", TableNameRewards, err)
	}
	if err := initBlockSignsTable(ctx, database); err != nil {
		return fmt.Errorf("create %s table: %w", TableNameBlockSigns, err)
	}
	if err := initCycleRotationsTable(ctx, database); err != nil {
		return fmt.Errorf("create %s table: %w", TableNameCycleRotations, err)
	}
	if err := initAddressesTable(ctx, database); err != nil {
		return fmt.Errorf("create %s table: %w", TableNameAddresses, err)
	}
	return nil
}

func (s SQLDB) Exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return s.ExecContext(ctx, q, args...)
}

func (s SQLDB) Query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return s.QueryContext(ctx, q, args...)
}

func (s SQLDB) Prepare(ctx context.Context, q string) (*sql.Stmt, error) {
	return s.PrepareContext(ctx, q)
}

func initTxTable(ctx context.Context, database Db) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s UInt64,
			%s DateTime64(3, 'UTC'),
			%s FixedString(64),
			%s String,
			%s UInt64,
			%s Decimal(30, 6)
		)
		ENGINE = MergeTree
		PARTITION BY intDiv(%s, 100000)
		ORDER BY (%s, %s)
	`, TableNameTxs,
		ColBlockHeight, ColBlockTime, ColTxHash, ColSender, ColGasUsed, ColFeeAmount,
		ColBlockHeight, ColBlockHeight, ColTxHash)

	_, err := database.Exec(ctx, query)
	return err
}

// initRewardsTable creates unified rewards table for all reward types:
// - reporter_tip: Reporter tips from oracle reports
// - validator_commission: Validator commission from delegated stake
// - validator_delegator: Staking rewards for validators
func initRewardsTable(ctx context.Context, database Db) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s UInt64,
			%s DateTime64(3, 'UTC'),
			%s String,
			%s String,
			%s Decimal(38, 18),
			%s LowCardinality(String)
		)
		ENGINE = MergeTree
		PARTITION BY intDiv(%s, 100000)
		ORDER BY (%s, %s, %s)
	`, TableNameRewards,
		ColBlockHeight, ColBlockTime, ColSender, ColRecipient, ColAmount, ColType,
		ColBlockHeight, ColBlockHeight, ColType, ColRecipient)

	_, err := database.Exec(ctx, query)
	return err
}

func initBlockSignsTable(ctx context.Context, database Db) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s UInt64,
			%s DateTime64(3, 'UTC'),
			%s String,
			%s UInt8
		)
		ENGINE = MergeTree
		PARTITION BY toYYYYMM(%s)
		ORDER BY (%s, %s)
	`, TableNameBlockSigns,
		ColBlockHeight, ColBlockTimestamp, ColValidatorAddress, ColSigned,
		ColBlockTimestamp, ColBlockHeight, ColValidatorAddress)

	_, err := database.Exec(ctx, query)
	return err
}

func initReportsTable(ctx context.Context, database Db) error {
	columnDefs := []string{
		fmt.Sprintf("%s LowCardinality(String)", ColReporter),
		fmt.Sprintf("%s UInt64", ColPower),
		fmt.Sprintf("%s LowCardinality(String)", ColQueryType),
		fmt.Sprintf("%s String", ColQueryID),
		fmt.Sprintf("%s LowCardinality(String)", ColAggregateMethod),
		fmt.Sprintf("%s String", ColValue),
		fmt.Sprintf("%s DateTime64(3, 'UTC')", ColTimestamp),
		fmt.Sprintf("%s DateTime64(3, 'UTC')", ColBlockTime),
		fmt.Sprintf("%s UInt8", ColCyclelist),
		fmt.Sprintf("%s UInt64", ColBlockNumber),
		fmt.Sprintf("%s UInt64", ColMetaID),
	}

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s (
		%[2]s
		)
		ENGINE = MergeTree
		PARTITION BY toYYYYMM(%[3]s)
		ORDER BY (%[4]s, %[5]s)
		SETTINGS index_granularity = 8192
		`,
		TableNameReports,
		strings.Join(columnDefs, ",\n  "),
		ColTimestamp,
		ColBlockNumber,
		ColQueryID,
	)

	_, err := database.Exec(ctx, query)
	return err
}

// initCycleRotationsTable creates a table to track query cycle rotations.
// Used to calculate missed reports: total cycles - actual reports per reporter.
func initCycleRotationsTable(ctx context.Context, database Db) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s UInt64,
			%s String,
			%s DateTime64(3, 'UTC')
		)
		ENGINE = MergeTree
		PARTITION BY toYYYYMM(%s)
		ORDER BY (%s, %s)
		SETTINGS index_granularity = 8192
	`, TableNameCycleRotations,
		ColBlockHeight, ColQueryID, ColTimestamp,
		ColTimestamp, ColBlockHeight, ColQueryID)

	_, err := database.Exec(ctx, query)
	return err
}

// initAddressesTable creates a table to store wallet addresses.
// Used by monitor and Grafana to query reporter/validator addresses dynamically.
func initAddressesTable(ctx context.Context, database Db) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s String,
			%s String,
			%s DateTime DEFAULT now()
		)
		ENGINE = ReplacingMergeTree(%s)
		ORDER BY %s
	`, TableNameAddresses,
		ColName, ColAddress, ColUpdatedAt,
		ColUpdatedAt, ColName)

	_, err := database.Exec(ctx, query)
	return err
}

// ConfigureTTL applies a 2-month TTL to all tables using ALTER TABLE MODIFY TTL.
// It is idempotent — safe to call on startup even if TTL is already set.
// Call this from the production monitor startup AFTER EnsureTables.
// Do NOT call from tests (fixture data is older than 1 month and would be dropped immediately).
func ConfigureTTL(ctx context.Context, database Db) error {
	type ttlSpec struct {
		table string
		col   string
	}
	specs := []ttlSpec{
		{TableNameReports, ColTimestamp},
		{TableNameTxs, ColBlockTime},
		{TableNameRewards, ColBlockTime},
		{TableNameBlockSigns, ColBlockTimestamp},
		{TableNameCycleRotations, ColTimestamp},
		{TableNameAddresses, ColUpdatedAt},
	}
	for _, s := range specs {
		q := fmt.Sprintf("ALTER TABLE %s MODIFY TTL %s + INTERVAL 2 MONTH", s.table, s.col)
		if _, err := database.Exec(ctx, q); err != nil {
			return fmt.Errorf("configure TTL for %s: %w", s.table, err)
		}
	}
	return nil
}

// UpsertAddress inserts or updates an address in the addresses table.
func UpsertAddress(ctx context.Context, database Db, name, address string) error {
	if name == "" {
		return errors.New("address name is required")
	}
	if address == "" {
		return errors.New("address value is required")
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (%s, %s, %s)
		VALUES (?, ?, now())
	`, TableNameAddresses, ColName, ColAddress, ColUpdatedAt)

	if _, err := database.Exec(ctx, query, name, address); err != nil {
		return fmt.Errorf("upsert address %s: %w", name, err)
	}
	return nil
}

// GetAddress retrieves an address by name from the addresses table.
// Returns empty string if not found.
func GetAddress(ctx context.Context, database Db, name string) (string, error) {
	if name == "" {
		return "", errors.New("address name is required")
	}

	query := fmt.Sprintf(`
		SELECT %s FROM %s FINAL WHERE %s = ? LIMIT 1
	`, ColAddress, TableNameAddresses, ColName)

	rows, err := database.Query(ctx, query, name)
	if err != nil {
		return "", fmt.Errorf("query address %s: %w", name, err)
	}
	defer func() { _ = rows.Close() }()

	if rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			return "", fmt.Errorf("scan address %s: %w", name, err)
		}
		return address, nil
	}
	return "", nil
}

// ReporterAddr retrieves the reporter address from the addresses table.
// Returns empty string if not found.
func ReporterAddr(ctx context.Context, database Db) (string, error) {
	return GetAddress(ctx, database, AddressNameReporter)
}

// ValidatorAddr retrieves the validator address from the addresses table.
// Returns empty string if not found.
func ValidatorAddr(ctx context.Context, database Db) (string, error) {
	return GetAddress(ctx, database, AddressNameValidator)
}
