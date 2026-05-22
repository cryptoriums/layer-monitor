package dispute

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"

	"cosmossdk.io/log"
)

var queryInterval = 50 * time.Millisecond

// noopDb satisfies blockdb.Db but always returns an error, so dispute tests
// rely solely on the HTTP API path without needing a real ClickHouse connection.
type noopDb struct{}

func (noopDb) Exec(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, fmt.Errorf("noop")
}

func (noopDb) Query(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, fmt.Errorf("noop")
}

func (noopDb) Prepare(_ context.Context, _ string) (*sql.Stmt, error) {
	return nil, fmt.Errorf("noop")
}

// mockOpenDisputesResponse returns a JSON response for open disputes.
// Uses camelCase field names to match protobuf JSON encoding.
func mockOpenDisputesResponse(ids []uint64) []byte {
	resp := map[string]interface{}{
		"openDisputes": map[string]interface{}{
			"ids": ids,
		},
	}
	b, _ := json.Marshal(resp)
	return b
}

func TestPanicsOnOpenDisputes_MultiServer(t *testing.T) {
	// Helper to create a server with given dispute IDs
	createServer := func(ids []uint64) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			_, _ = w.Write(mockOpenDisputesResponse(ids))
		}))
	}

	// Case 1: One server returns open disputes, one returns none
	serverOpen := createServer([]uint64{1, 2, 3})
	defer serverOpen.Close()
	serverNone := createServer([]uint64{})
	defer serverNone.Close()

	cfg := Config{Db: noopDb{}, LayerAPIURLs: []string{serverOpen.URL, serverNone.URL}, CheckInterval: queryInterval}
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.DebugLevel), log.ColorOption(false))
	monitor := New(logger, cfg, prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicCh := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
			close(doneCh)
		}()
		monitor.Run(ctx)
	}()
	select {
	case p := <-panicCh:
		if msg, ok := p.(string); ok {
			if !strings.Contains(msg, ReasonOpenDisputes) {
				t.Fatalf("Unexpected panic message: %v", msg)
			}
			t.Logf("Monitor panicked as expected (one open, one none): %v", msg)
		} else {
			t.Fatalf("Panic was not a string: %v", p)
		}
	case <-doneCh:
		t.Fatal("Monitor exited without panicking on open disputes (one open, one none)")
	case <-time.After(5 * queryInterval):
		t.Fatal("Timeout: Monitor did not panic on open disputes (one open, one none)")
	}

	// Case 2: Both servers return open disputes
	serverOpen2 := createServer([]uint64{4, 5})
	defer serverOpen2.Close()
	cfg2 := Config{Db: noopDb{}, LayerAPIURLs: []string{serverOpen.URL, serverOpen2.URL}, CheckInterval: queryInterval}
	monitor = New(logger, cfg2, prometheus.NewRegistry())
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	panicCh = make(chan interface{}, 1)
	doneCh = make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
			close(doneCh)
		}()
		monitor.Run(ctx)
	}()
	select {
	case p := <-panicCh:
		if msg, ok := p.(string); ok {
			if !strings.Contains(msg, "OPEN DISPUTES DETECTED") {
				t.Fatalf("Unexpected panic message: %v", msg)
			}
			t.Logf("Monitor panicked as expected (both open): %v", msg)
		} else {
			t.Fatalf("Panic was not a string: %v", p)
		}
	case <-doneCh:
		t.Fatal("Monitor exited without panicking on open disputes (both open)")
	case <-time.After(5 * queryInterval):
		t.Fatal("Timeout: Monitor did not panic on open disputes (both open)")
	}
}

func TestDoesNotPanicOnErrors(t *testing.T) {
	// Server that always returns error - should NOT panic (just log errors)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("error"))
	}))
	defer server.Close()

	cfg := Config{Db: noopDb{}, LayerAPIURLs: []string{server.URL}, CheckInterval: queryInterval}
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.DebugLevel), log.ColorOption(false))
	monitor := New(logger, cfg, prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicCh := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
			close(doneCh)
		}()
		monitor.Run(ctx)
	}()

	select {
	case p := <-panicCh:
		t.Fatalf("Monitor panicked on errors (should only panic on disputes): %v", p)
	case <-time.After(10 * queryInterval):
		// Success: did not panic on errors
		cancel()
	}
}

func TestDoesNotPanicWhenNoOpenDisputes(t *testing.T) {
	// Server that returns no open disputes
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write(mockOpenDisputesResponse([]uint64{}))
	}))
	defer server.Close()

	cfg := Config{Db: noopDb{}, LayerAPIURLs: []string{server.URL}, CheckInterval: queryInterval}
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.DebugLevel), log.ColorOption(false))
	monitor := New(logger, cfg, prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicCh := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
			close(doneCh)
		}()
		monitor.Run(ctx)
	}()

	select {
	case p := <-panicCh:
		t.Fatalf("Monitor panicked when there are no disputes: %v", p)
	case <-time.After(5 * queryInterval):
		// Success: did not panic
		cancel()
	}
}

func TestDoesNoPanicWhenDisputeIsIgnored(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write(mockOpenDisputesResponse([]uint64{42}))
	}))
	defer server.Close()

	cfg := Config{
		Db:             noopDb{},
		LayerAPIURLs:   []string{server.URL},
		IgnoreDisputes: []uint64{42},
		CheckInterval:  queryInterval,
	}
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.DebugLevel), log.ColorOption(false))
	monitor := New(logger, cfg, prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicCh := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
			close(doneCh)
		}()
		monitor.Run(ctx)
	}()

	select {
	case p := <-panicCh:
		t.Fatalf("Monitor panicked despite ignored dispute: %v", p)
	case <-doneCh:
		t.Fatal("Monitor exited unexpectedly while disputes were ignored")
	case <-time.After(5 * queryInterval):
		// Expected path: no panic within observation window.
		// Check gauge value BEFORE cancel to avoid race with final failed API call.
		if got := testutil.ToFloat64(monitor.openDisputesCount); got != 1 {
			t.Fatalf("unexpected open dispute gauge value: got %v, want 1", got)
		}
	}

	cancel()
	<-doneCh
}

func TestPanicsOnNonIgnoredDispute(t *testing.T) {
	// Server returns dispute ID 42, but we only ignore ID 99
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write(mockOpenDisputesResponse([]uint64{42}))
	}))
	defer server.Close()

	cfg := Config{
		Db:             noopDb{},
		LayerAPIURLs:   []string{server.URL},
		IgnoreDisputes: []uint64{99}, // Different ID
		CheckInterval:  queryInterval,
	}
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.DebugLevel), log.ColorOption(false))
	monitor := New(logger, cfg, prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicCh := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
			close(doneCh)
		}()
		monitor.Run(ctx)
	}()

	select {
	case p := <-panicCh:
		if msg, ok := p.(string); ok {
			if !strings.Contains(msg, "dispute_id=42") {
				t.Fatalf("Unexpected panic message: %v", msg)
			}
			t.Logf("Monitor panicked as expected on non-ignored dispute: %v", msg)
		} else {
			t.Fatalf("Panic was not a string: %v", p)
		}
	case <-doneCh:
		t.Fatal("Monitor exited without panicking on non-ignored dispute")
	case <-time.After(5 * queryInterval):
		t.Fatal("Timeout: Monitor did not panic on non-ignored dispute")
	}
}
