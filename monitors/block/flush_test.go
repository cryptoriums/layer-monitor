package block

import (
	"context"
	"errors"
	"testing"
	"time"

	ctypes "github.com/cometbft/cometbft/types"
	cryptolog "github.com/cryptoriums/layer-monitor/log"
	"github.com/stretchr/testify/require"
)

type countingProcessor struct {
	processed int
	flushes   int
	flushErr  error
}

func (p *countingProcessor) ProcessBlock(context.Context, ctypes.EventDataNewBlock) {
	p.processed++
}

func (p *countingProcessor) Flush(context.Context) error {
	p.flushes++
	return p.flushErr
}

type rangeFetcher struct {
	latest int64
}

func (f rangeFetcher) LatestHeight(context.Context) (int64, error) {
	return f.latest, nil
}

func (f rangeFetcher) EarliestHeight(context.Context) (int64, error) {
	return 1, nil
}

func (f rangeFetcher) FetchBlock(_ context.Context, height int64) (ctypes.EventDataNewBlock, error) {
	return ctypes.EventDataNewBlock{Block: &ctypes.Block{Header: ctypes.Header{Height: height}}}, nil
}

func TestCatchUpBatchesWritesAcrossFetchBatches(t *testing.T) {
	proc := &countingProcessor{}
	monitor := &Monitor{
		cfg: Config{
			FetchWorkers:  10,
			FlushBlocks:   100,
			FlushInterval: time.Hour,
		},
		logger:    cryptolog.New(),
		fetcher:   rangeFetcher{latest: 25},
		processor: proc,
		lastFlush: time.Now(),
	}

	last, err := monitor.catchUp(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, int64(25), last)
	require.Equal(t, 25, proc.processed)
	require.Equal(t, 25, monitor.unflushedBlocks)
	require.Zero(t, proc.flushes, "fetch batches must not create ClickHouse inserts")

	monitor.cfg.FlushBlocks = 25
	require.NoError(t, monitor.flushIfDue(context.Background()))
	require.Equal(t, 1, proc.flushes)
	require.Zero(t, monitor.unflushedBlocks)
}

func TestFlushIfDueUsesMaximumBufferAge(t *testing.T) {
	proc := &countingProcessor{}
	monitor := &Monitor{
		cfg: Config{
			FlushBlocks:   100,
			FlushInterval: 30 * time.Second,
		},
		logger:          cryptolog.New(),
		processor:       proc,
		unflushedBlocks: 1,
		lastFlush:       time.Now().Add(-31 * time.Second),
	}

	require.NoError(t, monitor.flushIfDue(context.Background()))
	require.Equal(t, 1, proc.flushes)
	require.Zero(t, monitor.unflushedBlocks)
}

func TestFlushFailureRemainsPendingForRetry(t *testing.T) {
	wantErr := errors.New("write failed")
	proc := &countingProcessor{flushErr: wantErr}
	monitor := &Monitor{
		cfg: Config{
			FlushBlocks:   3,
			FlushInterval: time.Hour,
		},
		logger:          cryptolog.New(),
		processor:       proc,
		unflushedBlocks: 3,
		lastFlush:       time.Now(),
	}

	require.ErrorIs(t, monitor.flushIfDue(context.Background()), wantErr)
	require.Equal(t, 1, proc.flushes)
	require.Equal(t, 3, monitor.unflushedBlocks)

	proc.flushErr = nil
	require.NoError(t, monitor.flushIfDue(context.Background()))
	require.Equal(t, 2, proc.flushes)
	require.Zero(t, monitor.unflushedBlocks)
}
