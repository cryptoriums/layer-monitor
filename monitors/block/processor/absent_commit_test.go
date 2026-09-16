package processor

import (
	"context"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	ctypes "github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	blockdb "github.com/cryptoriums/layer-monitor/db"
	cryptolog "github.com/cryptoriums/layer-monitor/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// commitBlock builds a block whose LastCommit holds sigs, i.e. the signatures for
// height-1.
func commitBlock(height int64, sigs []ctypes.CommitSig) ctypes.EventDataNewBlock {
	return ctypes.EventDataNewBlock{
		Block: &ctypes.Block{
			Header:     ctypes.Header{Height: height, Time: time.Now()},
			LastCommit: &ctypes.Commit{Signatures: sigs},
		},
		ResultFinalizeBlock: abci.ResponseFinalizeBlock{},
	}
}

func valAddr(b byte) []byte {
	a := make([]byte, 20)
	for i := range a {
		a[i] = b
	}
	return a
}

// absentSig is what CometBFT actually puts in a commit for a validator that did not
// vote: NewCommitSigAbsent() leaves ValidatorAddress EMPTY. Tests that attach an
// address to an absent vote do not reproduce production, which is how the
// undercount survived.
func absentSig() ctypes.CommitSig {
	return ctypes.CommitSig{BlockIDFlag: ctypes.BlockIDFlagAbsent}
}

func commitSig(addr []byte) ctypes.CommitSig {
	return ctypes.CommitSig{BlockIDFlag: ctypes.BlockIDFlagCommit, ValidatorAddress: addr}
}

func newProcessorForAddr(t *testing.T, ctx context.Context, consAddr string) (*Processor, blockdb.SQLDB) {
	t.Helper()
	db := setupTestDB(t)
	p := NewWithConfig(ctx, cryptolog.New(), db, ProcessorConfig{
		ValidatorConsensusAddress: consAddr,
		Registerer:                prometheus.NewRegistry(),
	})
	return p, db
}

// Our validator absent from the commit must produce a signed=0 row. Previously the
// empty ValidatorAddress caused the entry to be skipped, so a missed block left no
// trace at all and the chain counter and the dashboard disagreed.
func TestAbsentOurValidator_IsRecorded(t *testing.T) {
	ctx := context.Background()
	ours := sdk.ConsAddress(valAddr(0x22)).String()
	p, tdb := newProcessorForAddr(t, ctx, ours)

	// Block 100: we sign at index 1, which teaches the processor our position.
	p.insertBlockSigns(commitBlock(100, []ctypes.CommitSig{
		commitSig(valAddr(0x11)),
		commitSig(valAddr(0x22)),
		commitSig(valAddr(0x33)),
	}))
	// Block 101: our slot is an absent vote with no address.
	p.insertBlockSigns(commitBlock(101, []ctypes.CommitSig{
		commitSig(valAddr(0x11)),
		absentSig(),
		commitSig(valAddr(0x33)),
	}))
	require.NoError(t, p.Flush(ctx))

	records := fetchBlockSigns(t, tdb)
	var found bool
	for _, r := range records {
		if r.BlockHeight == 100 && r.ValidatorAddress == ours {
			// height 101's commit carries signatures for height 100
			require.Equal(t, uint8(0), r.Signed, "our absent vote must be recorded as signed=0")
			found = true
		}
	}
	require.True(t, found, "an absent vote by our validator must produce a row")
}

// A commit whose size changed means the validator set changed, so the remembered
// index may now belong to someone else. It must be discarded rather than used to
// blame an absence on us.
func TestAbsent_StaleIndexDiscardedWhenSetSizeChanges(t *testing.T) {
	ctx := context.Background()
	ours := sdk.ConsAddress(valAddr(0x22)).String()
	p, tdb := newProcessorForAddr(t, ctx, ours)

	p.insertBlockSigns(commitBlock(100, []ctypes.CommitSig{
		commitSig(valAddr(0x11)),
		commitSig(valAddr(0x22)),
		commitSig(valAddr(0x33)),
	}))
	// Set grew and we are not present: index 1 now means a different validator.
	p.insertBlockSigns(commitBlock(101, []ctypes.CommitSig{
		commitSig(valAddr(0x11)),
		absentSig(),
		commitSig(valAddr(0x33)),
		commitSig(valAddr(0x44)),
	}))
	require.NoError(t, p.Flush(ctx))

	for _, r := range fetchBlockSigns(t, tdb) {
		if r.ValidatorAddress == ours && r.BlockHeight == 100 {
			t.Fatal("must not attribute an absence to us after the validator set changed size")
		}
	}
	require.Equal(t, -1, p.ourSigIndex, "a stale index must be dropped, not reused")
}

// Until we have been seen signing at least once there is no index to reason about,
// so an absence cannot be attributed and must not be recorded.
func TestAbsent_NotRecordedBeforeIndexIsLearned(t *testing.T) {
	ctx := context.Background()
	ours := sdk.ConsAddress(valAddr(0x22)).String()
	p, tdb := newProcessorForAddr(t, ctx, ours)

	p.insertBlockSigns(commitBlock(101, []ctypes.CommitSig{
		commitSig(valAddr(0x11)),
		absentSig(),
		commitSig(valAddr(0x33)),
	}))
	require.NoError(t, p.Flush(ctx))

	for _, r := range fetchBlockSigns(t, tdb) {
		if r.ValidatorAddress == ours {
			t.Fatal("no row should be attributed to us before our index is known")
		}
	}
}

// Someone else being absent is not our miss.
func TestAbsent_OtherValidatorIsNotOurMiss(t *testing.T) {
	ctx := context.Background()
	ours := sdk.ConsAddress(valAddr(0x22)).String()
	p, tdb := newProcessorForAddr(t, ctx, ours)

	p.insertBlockSigns(commitBlock(100, []ctypes.CommitSig{
		commitSig(valAddr(0x11)),
		commitSig(valAddr(0x22)),
		commitSig(valAddr(0x33)),
	}))
	// Index 0 is absent; we signed at index 1 as usual.
	p.insertBlockSigns(commitBlock(101, []ctypes.CommitSig{
		absentSig(),
		commitSig(valAddr(0x22)),
		commitSig(valAddr(0x33)),
	}))
	require.NoError(t, p.Flush(ctx))

	for _, r := range fetchBlockSigns(t, tdb) {
		if r.ValidatorAddress == ours && r.Signed == 0 {
			t.Fatal("another validator's absence must not be recorded as ours")
		}
	}
}
