package tools

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	"github.com/cryptoriums/layer-monitor/encoding"
	"github.com/joho/godotenv"
	disputetypes "github.com/tellor-io/layer/x/dispute/types"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/tx"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
)

var (
	disputeReporter = flag.String("dispute-reporter", "", "Reporter address to query reports or dispute")
	disputeMetaID   = flag.Uint64("dispute-meta-id", 0, "Report meta ID to dispute")
	disputeQueryID  = flag.String("dispute-query-id", "", "Query ID hex of the report to dispute")
	disputeCategory = flag.String("dispute-category", "warning", "Dispute category (warning|minor|major)")
	disputeFee      = flag.String("dispute-fee", "1000000loya", "Dispute fee amount")
	disputeFromKey  = flag.String("dispute-from", "", "Key name to sign transaction")
	disputePayBond  = flag.Bool("dispute-pay-from-bond", false, "Pay fee from reporter bond")
	disputeExecute  = flag.Bool("dispute-execute", false, "Actually execute the dispute (default: dry-run)")
	disputeNode     = flag.String("dispute-node", "http://127.0.0.1:26657", "CometBFT RPC endpoint")
	disputeChainID  = flag.String("dispute-chain-id", "layer", "Chain ID")
	disputeKeyring  = flag.String("dispute-keyring", "test", "Keyring backend (os|file|test)")
	disputeHomeDir  = flag.String("dispute-home", "", "Node home directory (default: ~/.layer)")
)

const defaultDisputeGas = uint64(500000)

// ReportInfo holds report data for display.
type ReportInfo struct {
	Reporter    string
	MetaID      uint64
	QueryID     string
	QueryType   string
	Value       string
	Timestamp   time.Time
	BlockNumber uint64
	Power       uint64
}

// TestListReports lists recent reports from a reporter.
//
// Usage:
//
//	go test -v ./cryptoriums/tools/... -run TestListReports -args \
//	  -env=/root/.layer/.env \
//	  -dispute-reporter=tellor1abc...
func TestListReports(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping list reports in short mode")
	}

	if *envFile != "" {
		if err := godotenv.Load(*envFile); err != nil {
			t.Logf("Warning: could not load env file %s: %v", *envFile, err)
		}
	}

	reporterAddr := *disputeReporter
	if reporterAddr == "" {
		reporterAddr = os.Getenv("DISPUTE_REPORTER_ADDRESS")
	}
	if reporterAddr == "" {
		t.Skip("reporter address not set (use -dispute-reporter flag or DISPUTE_REPORTER_ADDRESS env var)")
	}

	apiURLs := parseAPIURLs()
	if len(apiURLs) == 0 {
		t.Fatal("API_URLS not set")
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 30 * time.Second}

	reports, err := queryReportsByReporter(ctx, httpClient, apiURLs, reporterAddr)
	if err != nil {
		t.Fatalf("failed to query reports: %v", err)
	}

	if len(reports) == 0 {
		t.Logf("No reports found for reporter: %s", reporterAddr)
		return
	}

	t.Log("")
	t.Log("==================== REPORTS ====================")
	t.Logf("Reporter: %s", reporterAddr)
	t.Logf("Total: %d reports", len(reports))
	t.Log("")
	t.Log("  # | Meta ID    | Query Type       | Block      | Query ID (first 32 chars)")
	t.Log("----|------------|------------------|------------|----------------------------------")

	for i, r := range reports {
		queryIDDisplay := r.QueryID
		if len(queryIDDisplay) > 32 {
			queryIDDisplay = queryIDDisplay[:32]
		}
		t.Logf("%3d | %10d | %-16s | %10d | %s",
			i+1, r.MetaID, truncateString(r.QueryType, 16), r.BlockNumber, queryIDDisplay)
	}

	t.Log("")
	t.Log("To dispute a report, run:")
	t.Logf("  go test -v ./cryptoriums/tools/... -run TestProposeDispute -args \\")
	t.Logf("    -env=/root/.layer/.env \\")
	t.Logf("    -dispute-reporter=%s \\", reporterAddr)
	t.Logf("    -dispute-meta-id=<META_ID> \\")
	t.Logf("    -dispute-query-id=<QUERY_ID> \\")
	t.Logf("    -dispute-from=<KEY_NAME>")
	t.Log("")
	t.Log("Add -dispute-execute to actually submit the transaction.")
	t.Log("=================================================")
}

// TestProposeDispute proposes a dispute against a reporter's report.
// By default this is a dry-run. Add -dispute-execute to actually submit.
//
// Usage (dry-run):
//
//	go test -v ./cryptoriums/tools/... -run TestProposeDispute -args \
//	  -env=/root/.layer/.env \
//	  -dispute-reporter=tellor1abc... \
//	  -dispute-meta-id=123 \
//	  -dispute-query-id=0x1234abcd... \
//	  -dispute-from=mykey
//
// Usage (execute):
//
//	go test -v ./cryptoriums/tools/... -run TestProposeDispute -args \
//	  -env=/root/.layer/.env \
//	  -dispute-reporter=tellor1abc... \
//	  -dispute-meta-id=123 \
//	  -dispute-query-id=0x1234abcd... \
//	  -dispute-from=mykey \
//	  -dispute-execute
func TestProposeDispute(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping propose dispute in short mode")
	}

	if *envFile != "" {
		if err := godotenv.Load(*envFile); err != nil {
			t.Logf("Warning: could not load env file %s: %v", *envFile, err)
		}
	}

	// Validate required flags
	disputedReporter := *disputeReporter
	if disputedReporter == "" {
		t.Fatal("-dispute-reporter flag is required")
	}

	metaID := *disputeMetaID
	if metaID == 0 {
		t.Fatal("-dispute-meta-id flag is required")
	}

	queryIDHex := *disputeQueryID
	if queryIDHex == "" {
		t.Fatal("-dispute-query-id flag is required")
	}

	fromKey := *disputeFromKey
	if fromKey == "" {
		fromKey = os.Getenv("DISPUTE_FROM_KEY")
	}
	if fromKey == "" {
		t.Fatal("-dispute-from flag or DISPUTE_FROM_KEY env var is required")
	}

	// Remove 0x prefix if present
	if len(queryIDHex) > 2 && queryIDHex[:2] == "0x" {
		queryIDHex = queryIDHex[2:]
	}

	// Parse category
	var category disputetypes.DisputeCategory
	switch *disputeCategory {
	case "warning":
		category = disputetypes.Warning
	case "minor":
		category = disputetypes.Minor
	case "major":
		category = disputetypes.Major
	default:
		t.Fatalf("invalid category: %s (must be warning, minor, or major)", *disputeCategory)
	}

	// Parse fee
	fee, err := sdk.ParseCoinNormalized(*disputeFee)
	if err != nil {
		t.Fatalf("invalid fee: %v", err)
	}

	// Get config
	homeDir := *disputeHomeDir
	if homeDir == "" {
		homeDir = os.Getenv("HOME") + "/.layer"
	}

	t.Log("")
	t.Log("==================== PROPOSE DISPUTE ====================")
	t.Logf("Disputed Reporter: %s", disputedReporter)
	t.Logf("Report Meta ID:    %d", metaID)
	t.Logf("Query ID:          %s", queryIDHex)
	t.Logf("Category:          %s", *disputeCategory)
	t.Logf("Fee:               %s", fee.String())
	t.Logf("Pay From Bond:     %t", *disputePayBond)
	t.Logf("From Key:          %s", fromKey)
	t.Log("")

	if !*disputeExecute {
		t.Log("[DRY RUN - Transaction will not be submitted]")
		t.Log("")
		t.Log("To actually submit the dispute, add -dispute-execute flag")
		t.Log("=========================================================")
		return
	}

	// Setup encoding
	cdc := encoding.MakeCodec()
	txConfig := encoding.MakeTxConfig()

	// Setup keyring
	reader := strings.NewReader("")

	kr, err := keyring.New("", *disputeKeyring, homeDir, reader, cdc)
	if err != nil {
		t.Fatalf("failed to open keyring: %v", err)
	}

	record, err := kr.Key(fromKey)
	if err != nil {
		t.Fatalf("key not found: %v", err)
	}

	fromAddr, err := record.GetAddress()
	if err != nil {
		t.Fatalf("failed to get address: %v", err)
	}

	t.Logf("Creator Address:   %s", fromAddr.String())

	// Setup RPC client
	rpcClient, err := rpchttp.New(*disputeNode, "/websocket")
	if err != nil {
		t.Fatalf("failed to create RPC client: %v", err)
	}

	// Build client context
	clientCtx := client.Context{}.
		WithChainID(*disputeChainID).
		WithHomeDir(homeDir).
		WithKeyringDir(homeDir).
		WithClient(rpcClient).
		WithCodec(cdc).
		WithTxConfig(txConfig).
		WithKeyring(kr).
		WithFrom(fromKey).
		WithFromName(fromKey).
		WithFromAddress(fromAddr).
		WithAccountRetriever(authtypes.AccountRetriever{}).
		WithBroadcastMode("sync")

	// Build the dispute message
	msg := &disputetypes.MsgProposeDispute{
		Creator:          fromAddr.String(),
		DisputedReporter: disputedReporter,
		ReportMetaId:     metaID,
		ReportQueryId:    queryIDHex,
		DisputeCategory:  category,
		Fee:              fee,
		PayFromBond:      *disputePayBond,
	}

	// Build and sign transaction
	txf := tx.Factory{}.
		WithChainID(*disputeChainID).
		WithKeybase(kr).
		WithGas(defaultDisputeGas).
		WithGasAdjustment(1.2).
		WithTxConfig(txConfig).
		WithAccountRetriever(clientCtx.AccountRetriever)

	// Prepare factory (get account number and sequence)
	txf, err = txf.Prepare(clientCtx)
	if err != nil {
		t.Fatalf("failed to prepare transaction: %v", err)
	}

	txBuilder, err := txf.BuildUnsignedTx(msg)
	if err != nil {
		t.Fatalf("failed to build transaction: %v", err)
	}

	if err := tx.Sign(clientCtx.CmdContext, txf, fromKey, txBuilder, true); err != nil {
		t.Fatalf("failed to sign transaction: %v", err)
	}

	txBytes, err := clientCtx.TxConfig.TxEncoder()(txBuilder.GetTx())
	if err != nil {
		t.Fatalf("failed to encode transaction: %v", err)
	}

	// Broadcast
	t.Log("Broadcasting transaction...")
	res, err := clientCtx.BroadcastTx(txBytes)
	if err != nil {
		t.Fatalf("failed to broadcast transaction: %v", err)
	}

	if res.Code != 0 {
		t.Fatalf("transaction failed: code=%d, log=%s", res.Code, res.RawLog)
	}

	t.Log("")
	t.Log("Transaction successful!")
	t.Logf("  TX Hash: %s", res.TxHash)
	t.Logf("  Code:    %d", res.Code)
	t.Log("=========================================================")
}

// TestQueryOpenDisputes queries current open disputes from the chain.
//
// Usage:
//
//	go test -v ./cryptoriums/tools/... -run TestQueryOpenDisputes -args -env=/root/.layer/.env
func TestQueryOpenDisputes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping query open disputes in short mode")
	}

	if *envFile != "" {
		if err := godotenv.Load(*envFile); err != nil {
			t.Logf("Warning: could not load env file %s: %v", *envFile, err)
		}
	}

	apiURLs := parseAPIURLs()
	if len(apiURLs) == 0 {
		t.Fatal("API_URLS not set")
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 30 * time.Second}

	disputes, err := queryOpenDisputes(ctx, httpClient, apiURLs)
	if err != nil {
		t.Fatalf("failed to query open disputes: %v", err)
	}

	t.Log("")
	t.Log("==================== OPEN DISPUTES ====================")
	if len(disputes) == 0 {
		t.Log("No open disputes found.")
	} else {
		t.Logf("Found %d open dispute(s):", len(disputes))
		for _, id := range disputes {
			t.Logf("  - Dispute ID: %d", id)
		}
	}
	t.Log("========================================================")
}

// queryReportsByReporter fetches reports from a reporter address via Layer API.
func queryReportsByReporter(ctx context.Context, httpClient *http.Client, apiURLs []string, reporter string) ([]ReportInfo, error) {
	path := fmt.Sprintf("/tellor-io/layer/oracle/get-reports-by-reporter/%s", reporter)
	resp, err := queryAPI(ctx, httpClient, apiURLs, path)
	if err != nil {
		return nil, err
	}

	var result struct {
		MicroReports []struct {
			Reporter        string `json:"reporter"`
			MetaId          string `json:"meta_id"`
			QueryId         string `json:"query_id"`
			QueryType       string `json:"query_type"`
			Value           string `json:"value"`
			Timestamp       string `json:"timestamp"`
			BlockNumber     string `json:"block_number"`
			Power           string `json:"power"`
			AggregateMethod string `json:"aggregate_method"`
		} `json:"microReports"`
	}

	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	reports := make([]ReportInfo, 0, len(result.MicroReports))
	for _, r := range result.MicroReports {
		var metaID uint64
		var blockNumber uint64
		var power uint64
		_, _ = fmt.Sscanf(r.MetaId, "%d", &metaID)
		_, _ = fmt.Sscanf(r.BlockNumber, "%d", &blockNumber)
		_, _ = fmt.Sscanf(r.Power, "%d", &power)

		// Parse timestamp
		var ts time.Time
		if r.Timestamp != "" {
			ts, _ = time.Parse(time.RFC3339, r.Timestamp)
		}

		// Convert query_id from base64 to hex if needed
		queryIDHex := r.QueryId
		if !strings.HasPrefix(queryIDHex, "0x") && len(queryIDHex) > 0 {
			// Try to decode as base64 and convert to hex
			if decoded, err := decodeBase64OrHex(queryIDHex); err == nil {
				queryIDHex = "0x" + hex.EncodeToString(decoded)
			}
		}

		reports = append(reports, ReportInfo{
			Reporter:    r.Reporter,
			MetaID:      metaID,
			QueryID:     queryIDHex,
			QueryType:   r.QueryType,
			Value:       r.Value,
			Timestamp:   ts,
			BlockNumber: blockNumber,
			Power:       power,
		})
	}

	return reports, nil
}

// queryOpenDisputes fetches open disputes from the Layer API.
func queryOpenDisputes(ctx context.Context, httpClient *http.Client, apiURLs []string) ([]uint64, error) {
	path := "/tellor-io/layer/dispute/open-disputes"
	resp, err := queryAPI(ctx, httpClient, apiURLs, path)
	if err != nil {
		return nil, err
	}

	var result struct {
		OpenDisputes struct {
			Ids []string `json:"ids"`
		} `json:"open_disputes"`
	}

	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	ids := make([]uint64, 0, len(result.OpenDisputes.Ids))
	for _, idStr := range result.OpenDisputes.Ids {
		var id uint64
		_, _ = fmt.Sscanf(idStr, "%d", &id)
		ids = append(ids, id)
	}

	return ids, nil
}

// decodeBase64OrHex tries to decode a string as base64, falling back to hex.
func decodeBase64OrHex(s string) ([]byte, error) {
	// Try hex first (with or without 0x prefix)
	hexStr := strings.TrimPrefix(s, "0x")
	if decoded, err := hex.DecodeString(hexStr); err == nil {
		return decoded, nil
	}

	// Try standard base64
	if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
		return decoded, nil
	}

	// Try URL-safe base64
	if decoded, err := base64.URLEncoding.DecodeString(s); err == nil {
		return decoded, nil
	}

	return nil, fmt.Errorf("could not decode as hex or base64: %s", s)
}

// truncateString truncates a string to maxLen characters.
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
