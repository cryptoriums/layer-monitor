//go:build ignore

// Generator for blocks_fixtures.json (the test fixture consumed by block_test.go).
// Run from this directory:  go run gen_fixtures.go
//
// It produces 4 consecutive blocks, each carrying one "new_report" event, so the
// deduplication test can replay the same blocks from multiple mock nodes and
// assert each report is stored exactly once.
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	abci "github.com/cometbft/cometbft/abci/types"
	ctypes "github.com/cometbft/cometbft/types"
)

func attr(k, v string) abci.EventAttribute { return abci.EventAttribute{Key: k, Value: v} }

func newReportEvent(height int64, reporter, querySeed string) abci.Event {
	qid := make([]byte, 32)
	copy(qid, querySeed)
	return abci.Event{
		Type: "new_report",
		Attributes: []abci.EventAttribute{
			attr("reporter", reporter),
			attr("power", "10"),
			attr("query_type", "SpotPrice"),
			attr("query_id", hex.EncodeToString(qid)),
			attr("aggregate_method", "weighted-median"),
			attr("value", fmt.Sprintf("%064x", height)),
			attr("timestamp", "1700000000000"),
			attr("cyclelist", "true"),
			attr("block_number", fmt.Sprintf("%d", height)),
			attr("meta_id", fmt.Sprintf("%d", height)),
		},
	}
}

func block(height int64, evs ...abci.Event) ctypes.EventDataNewBlock {
	return ctypes.EventDataNewBlock{
		Block: &ctypes.Block{Header: ctypes.Header{Height: height}},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{
			TxResults: []*abci.ExecTxResult{{Code: 0, Events: evs}},
		},
	}
}

func main() {
	const base = 1000
	fixtures := []ctypes.EventDataNewBlock{
		block(base+0, newReportEvent(base+0, "tellor1reporteraaa", "query-A")),
		block(base+1, newReportEvent(base+1, "tellor1reporterbbb", "query-B")),
		block(base+2, newReportEvent(base+2, "tellor1reporterccc", "query-C")),
		block(base+3, newReportEvent(base+3, "tellor1reporterddd", "query-D")),
	}

	data, err := json.MarshalIndent(fixtures, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("blocks_fixtures.json", append(data, '\n'), 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("wrote blocks_fixtures.json (%d blocks)\n", len(fixtures))
}
