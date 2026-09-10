package health

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cosmossdk.io/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const testWallet = "tellor128m9knt3039k5rmaeu50q0g7y608g2w5etj2ra"

// statusBody is a minimal CometBFT /status payload.
func statusBody(height string, catchingUp bool) string {
	return fmt.Sprintf(`{"result":{"sync_info":{"latest_block_height":%q,"catching_up":%t}}}`, height, catchingUp)
}

// newTestMonitor builds a monitor against the given RPC and API base URLs using an
// isolated registry, so each test observes only its own metrics.
func newTestMonitor(t *testing.T, rpcURLs, apiURLs []string) *Monitor {
	t.Helper()
	m, err := New(log.NewNopLogger(), Config{
		LayerRPCURLs:  rpcURLs,
		LayerAPIURLs:  apiURLs,
		WalletAddress: testWallet,
	}, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func TestNewRequiresConfig(t *testing.T) {
	cases := map[string]Config{
		"no rpc urls": {LayerAPIURLs: []string{"http://api"}, WalletAddress: testWallet},
		"no api urls": {LayerRPCURLs: []string{"http://rpc"}, WalletAddress: testWallet},
		"no wallet":   {LayerRPCURLs: []string{"http://rpc"}, LayerAPIURLs: []string{"http://api"}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := New(log.NewNopLogger(), cfg, prometheus.NewRegistry()); err == nil {
				t.Fatal("expected an error for incomplete config")
			}
		})
	}
}

func TestStatusMetricsFromFirstEndpoint(t *testing.T) {
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, statusBody("22236534", false))
	}))
	defer rpc.Close()

	m := newTestMonitor(t, []string{rpc.URL}, []string{"http://unused"})
	before := time.Now().Unix()
	m.check(context.Background())

	if got := testutil.ToFloat64(m.height); got != 22236534 {
		t.Fatalf("height = %v, want 22236534", got)
	}
	if got := testutil.ToFloat64(m.nodeSynced); got != 1 {
		t.Fatalf("node_synced = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.rpcSourceIndex); got != 0 {
		t.Fatalf("rpc_source_index = %v, want 0 (first endpoint answered)", got)
	}
	if got := testutil.ToFloat64(m.lastSuccess); got < float64(before) {
		t.Fatalf("last_success_timestamp = %v, want >= %v", got, before)
	}
}

func TestCatchingUpNodeReportsNotSynced(t *testing.T) {
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, statusBody("100", true))
	}))
	defer rpc.Close()

	m := newTestMonitor(t, []string{rpc.URL}, []string{"http://unused"})
	m.check(context.Background())

	if got := testutil.ToFloat64(m.nodeSynced); got != 0 {
		t.Fatalf("node_synced = %v, want 0 while catching up", got)
	}
}

// The whole point of a list of endpoints is that our own node going down must not
// blind the exporter, and that the fallback is visible rather than silent.
func TestFailsOverToSecondEndpointAndReportsIndex(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer dead.Close()
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, statusBody("500", false))
	}))
	defer live.Close()

	m := newTestMonitor(t, []string{dead.URL, live.URL}, []string{"http://unused"})
	m.check(context.Background())

	if got := testutil.ToFloat64(m.height); got != 500 {
		t.Fatalf("height = %v, want 500 from the fallback endpoint", got)
	}
	if got := testutil.ToFloat64(m.rpcSourceIndex); got != 1 {
		t.Fatalf("rpc_source_index = %v, want 1 so the failover is visible", got)
	}
}

// A stale heartbeat is what the "exporter stale" alert fires on, so it must not
// advance when nothing could be read - otherwise a total outage looks healthy.
func TestHeartbeatDoesNotAdvanceWhenAllEndpointsFail(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer dead.Close()

	m := newTestMonitor(t, []string{dead.URL}, []string{"http://unused"})
	m.check(context.Background())

	if got := testutil.ToFloat64(m.lastSuccess); got != 0 {
		t.Fatalf("last_success_timestamp = %v, want 0 when every endpoint failed", got)
	}
	if got := testutil.ToFloat64(m.rpcSourceIndex); got != sourceNone {
		t.Fatalf("rpc_source_index = %v, want %d when every endpoint failed", got, sourceNone)
	}
}

// Malformed JSON must be treated as a failed endpoint, not as height 0 - reporting
// zero would look like a stalled chain and page someone at night.
func TestMalformedStatusIsTreatedAsFailure(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"result":{"sync_info":{"latest_block_height":"not-a-number"}}}`)
	}))
	defer bad.Close()

	m := newTestMonitor(t, []string{bad.URL}, []string{"http://unused"})
	m.check(context.Background())

	if got := testutil.ToFloat64(m.lastSuccess); got != 0 {
		t.Fatalf("last_success_timestamp = %v, want 0 for an unparseable status", got)
	}
}
