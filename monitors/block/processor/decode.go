package processor

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cryptoriums/layer-packages/encoding"
	"github.com/tellor-io/layer/app"
	"github.com/tellor-io/layer/x/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// ParseTimestamp parses timestamps stored on chain into UTC time.
func ParseTimestamp(val string) (time.Time, error) {
	if ts, err := time.Parse(time.RFC3339Nano, val); err == nil {
		return ts.UTC(), nil
	}
	ms, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms).UTC(), nil
}

// ParseReporterPower parses the reporter power column.
func ParseReporterPower(val string) (uint64, error) {
	return parseUint64(val)
}

// ParseBlockNumber parses the block_number column.
func ParseBlockNumber(val string) (uint64, error) {
	return parseUint64(val)
}

// ParseMetaID parses the meta_id column.
func ParseMetaID(val string) (uint64, error) {
	return parseUint64(val)
}

// parseUint64 converts decimal strings to uint64.
func parseUint64(val string) (uint64, error) {
	return strconv.ParseUint(val, 10, 64)
}

// EncodeQueryID converts 32-byte query IDs to a hex string.
func EncodeQueryID(queryID []byte) (string, error) {
	if len(queryID) != 32 {
		return "", fmt.Errorf("query_id must be 32 bytes, got %d", len(queryID))
	}
	return hex.EncodeToString(queryID), nil
}

// DecodeQueryID converts a hex string query ID to bytes.
func DecodeQueryID(val string) ([]byte, error) {
	if val == "" {
		return nil, fmt.Errorf("query_id is empty")
	}
	return hex.DecodeString(val)
}

// DecodeReportEvent extracts a MicroReport from a CometBFT event payload.
func DecodeReportEvent(height int64, ev abci.Event) (*types.MicroReport, error) {
	var report types.MicroReport

	for _, attr := range ev.Attributes {
		attrVal := attr.Value
		switch attr.Key {
		case AttrKeyReporter:
			report.Reporter = attrVal
		case AttrKeyPower:
			power, err := ParseReporterPower(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse reporter power: %w", err)
			}
			report.Power = power
		case AttrKeyQueryType:
			report.QueryType = attrVal
		case AttrKeyQueryID:
			queryIDBytes, err := DecodeQueryID(attrVal)
			if err != nil {
				return nil, fmt.Errorf("decode query_id: %w", err)
			}
			report.QueryId = queryIDBytes
		case AttrKeyAggMethod:
			report.AggregateMethod = attrVal
		case AttrKeyValue:
			report.Value = attrVal
		case AttrKeyTimestamp:
			ts, err := ParseTimestamp(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse timestamp: %w", err)
			}
			report.Timestamp = ts
		case AttrKeyCyclelist:
			report.Cyclelist = attrVal == "true"
		case AttrKeyBlockNumber:
			blockNumber, err := ParseBlockNumber(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse block number: %w", err)
			}
			report.BlockNumber = blockNumber
		case AttrKeyMetaID:
			metaId, err := ParseMetaID(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse meta_id: %w", err)
			}
			report.MetaId = metaId
		}
	}

	// Fallback: if block_number was not in the event, use the block height
	if report.BlockNumber == 0 && height > 0 {
		report.BlockNumber = uint64(height)
	}

	return &report, nil
}

// NewTxDecoder builds a Cosmos SDK TxDecoder capable of handling Layer transactions.
func NewTxDecoder() sdk.TxDecoder {
	return encoding.MakeTxDecoder()
}

// ParseVoteExtensionTx attempts to decode vote extension payloads emitted as JSON.
func ParseVoteExtensionTx(raw []byte) (*app.VoteExtTx, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || (trimmed[0] != '{' && trimmed[0] != '[') {
		return nil, false
	}

	var voteTx app.VoteExtTx
	if err := json.Unmarshal(trimmed, &voteTx); err != nil {
		return nil, false
	}

	if voteTx.BlockHeight == 0 && voteTx.ExtendedCommitInfo.Round == 0 && len(voteTx.ExtendedCommitInfo.Votes) == 0 &&
		len(voteTx.OpAndEVMAddrs.OperatorAddresses) == 0 &&
		len(voteTx.ValsetSigs.OperatorAddresses) == 0 &&
		len(voteTx.OracleAttestations.OperatorAddresses) == 0 {
		return nil, false
	}

	return &voteTx, true
}
