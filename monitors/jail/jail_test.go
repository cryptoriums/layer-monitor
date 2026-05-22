package jail

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/cryptoriums/layer-packages/encoding"
	cryptolog "github.com/cryptoriums/layer-packages/log"
	reportertypes "github.com/tellor-io/layer/x/reporter/types"

	"github.com/cosmos/cosmos-sdk/x/staking/types"
)

const testOurReporterAddr = "tellor1ourreporter"

// ============================================================================
// Test Helper Functions
// ============================================================================

// mockReportersResponse creates a mock API response for reporters endpoint
// using the actual protobuf types for proper codec compatibility.
func mockReportersResponse(addresses []string) []byte {
	cdc := encoding.MakeCodec()
	reporters := make([]*reportertypes.Reporter, 0, len(addresses))
	for _, addr := range addresses {
		reporters = append(reporters, &reportertypes.Reporter{
			Address: addr,
		})
	}
	resp := reportertypes.QueryReportersResponse{
		Reporters: reporters,
	}
	data, _ := cdc.MarshalJSON(&resp)
	return data
}

// mockValidatorsResponse creates a mock API response for validators endpoint
// using the actual protobuf types for proper codec compatibility.
func mockValidatorsResponse(validators []types.Validator) []byte {
	cdc := encoding.MakeCodec()
	resp := types.QueryValidatorsResponse{
		Validators: validators,
	}
	data, _ := cdc.MarshalJSON(&resp)
	return data
}

// createMockServer creates a mock HTTP server with configurable responses.
// reporters: list of all reporter addresses
// jailedReporters: list of jailed reporter addresses
// validators: list of validators with their jail status
func createMockServer(t *testing.T, reporters, jailedReporters []string, validators []types.Validator) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tellor-io/layer/reporter/reporters":
			_, _ = w.Write(mockReportersResponse(reporters))
		case "/tellor-io/layer/reporter/jailed-reporters":
			_, _ = w.Write(mockReportersResponse(jailedReporters))
		case "/cosmos/staking/v1beta1/validators":
			_, _ = w.Write(mockValidatorsResponse(validators))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(func() { server.Close() })
	return server
}

// createFailingServer creates a mock HTTP server that always returns errors.
func createFailingServer(t *testing.T, statusCode int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Server Error", statusCode)
	}))
	t.Cleanup(func() { server.Close() })
	return server
}

// newTestMonitor creates a monitor for testing with isolated registry.
func newTestMonitor(t *testing.T, cfg Config) *Monitor {
	t.Helper()
	logger := cryptolog.New()
	monitor, err := New(logger, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	return monitor
}

// ============================================================================
// Constructor Tests
// ============================================================================

func TestNew_ValidConfig(t *testing.T) {
	logger := cryptolog.New()
	cfg := Config{
		LayerAPIURLs:  []string{"http://localhost:1317"},
		WalletAddress: "tellor1abc123",
		CheckInterval: time.Minute,
	}

	monitor, err := New(logger, cfg, prometheus.NewRegistry())

	require.NoError(t, err)
	require.NotNil(t, monitor)
	assert.Equal(t, cfg.WalletAddress, monitor.cfg.WalletAddress)
	assert.Equal(t, cfg.CheckInterval, monitor.cfg.CheckInterval)
	assert.NotNil(t, monitor.httpClient)
}

func TestNew_MissingAPIURLs(t *testing.T) {
	logger := cryptolog.New()
	cfg := Config{
		LayerAPIURLs:  []string{},
		WalletAddress: "tellor1abc123",
	}

	_, err := New(logger, cfg, prometheus.NewRegistry())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "LayerAPIURLs is required")
}

func TestNew_DefaultCheckInterval(t *testing.T) {
	logger := cryptolog.New()
	cfg := Config{
		LayerAPIURLs:  []string{"http://localhost:1317"},
		WalletAddress: "tellor1abc123",
		CheckInterval: 0, // Should default to DefaultCheckInterval
	}

	monitor, err := New(logger, cfg, prometheus.NewRegistry())

	require.NoError(t, err)
	assert.Equal(t, DefaultCheckInterval, monitor.cfg.CheckInterval)
}

// ============================================================================
// Table-Driven Tests for Jail Status Scenarios
// ============================================================================

// makeValidator creates a validator with the given address and jail status.
func makeValidator(operatorAddr string, jailed bool, moniker string) types.Validator {
	return types.Validator{
		OperatorAddress: operatorAddr,
		Jailed:          jailed,
		Description:     types.Description{Moniker: moniker},
	}
}

func TestCheckJailStatus_Scenarios(t *testing.T) {
	ourReporterAddr := testOurReporterAddr
	ourValidatorAddr := "tellorvaloper1ourvalidator"
	otherReporterAddr := "tellor1other"
	otherValidatorAddr := "tellorvaloper1other"

	tests := []struct {
		name            string
		walletAddress   string
		reporters       []string
		jailedReporters []string
		validators      []types.Validator
		expectOurJailed bool
		description     string
	}{
		{
			name:            "no_jailed_entities",
			walletAddress:   ourReporterAddr,
			reporters:       []string{ourReporterAddr, otherReporterAddr},
			jailedReporters: []string{},
			validators:      []types.Validator{makeValidator(otherValidatorAddr, false, "other-val")},
			expectOurJailed: false,
			description:     "All reporters and validators are not jailed",
		},
		{
			name:            "our_reporter_jailed",
			walletAddress:   ourReporterAddr,
			reporters:       []string{ourReporterAddr, otherReporterAddr},
			jailedReporters: []string{ourReporterAddr},
			validators:      []types.Validator{},
			expectOurJailed: true,
			description:     "Our reporter is jailed",
		},
		{
			name:            "other_reporter_jailed_not_ours",
			walletAddress:   ourReporterAddr,
			reporters:       []string{ourReporterAddr, otherReporterAddr},
			jailedReporters: []string{otherReporterAddr},
			validators:      []types.Validator{},
			expectOurJailed: false,
			description:     "Another reporter is jailed but not ours",
		},
		{
			name:            "our_validator_jailed",
			walletAddress:   ourReporterAddr,
			reporters:       []string{ourReporterAddr},
			jailedReporters: []string{},
			validators: []types.Validator{
				makeValidator(ourValidatorAddr, true, "our-val"),
				makeValidator(otherValidatorAddr, false, "other-val"),
			},
			expectOurJailed: false, // Our wallet != validator addr format, so this won't trigger
			description:     "Validator is jailed (different address format)",
		},
		{
			name:            "multiple_jailed_reporters",
			walletAddress:   ourReporterAddr,
			reporters:       []string{ourReporterAddr, otherReporterAddr, "tellor1third"},
			jailedReporters: []string{ourReporterAddr, otherReporterAddr, "tellor1third"},
			validators:      []types.Validator{},
			expectOurJailed: true,
			description:     "Multiple reporters jailed including ours",
		},
		{
			name:            "empty_responses",
			walletAddress:   ourReporterAddr,
			reporters:       []string{},
			jailedReporters: []string{},
			validators:      []types.Validator{},
			expectOurJailed: false,
			description:     "Empty responses from all endpoints",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := createMockServer(t, tc.reporters, tc.jailedReporters, tc.validators)

			cfg := Config{
				LayerAPIURLs:  []string{server.URL},
				WalletAddress: tc.walletAddress,
				CheckInterval: time.Minute,
			}
			monitor := newTestMonitor(t, cfg)

			ctx := context.Background()
			monitor.checkJailStatus(ctx)

			// Verify metrics match expected jailed status
			if tc.expectOurJailed {
				assert.Equal(t, float64(1), testutil.ToFloat64(monitor.ourReporterJailed),
					"%s: our reporter should be jailed", tc.name)
			} else {
				assert.Equal(t, float64(0), testutil.ToFloat64(monitor.ourReporterJailed),
					"%s: our reporter should not be jailed", tc.name)
			}
		})
	}
}

// ============================================================================
// API Failure Tests
// ============================================================================

func TestCheckJailStatus_APIFailures(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
	}{
		{"internal_server_error", http.StatusInternalServerError},
		{"service_unavailable", http.StatusServiceUnavailable},
		{"bad_gateway", http.StatusBadGateway},
		{"not_found", http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := createFailingServer(t, tc.statusCode)

			cfg := Config{
				LayerAPIURLs:  []string{server.URL},
				WalletAddress: "tellor1abc",
				CheckInterval: time.Minute,
			}
			monitor := newTestMonitor(t, cfg)

			// Should handle API failure gracefully without panic
			ctx := context.Background()
			monitor.checkJailStatus(ctx)
		})
	}
}

// ============================================================================
// Multi-Node Failover Tests
// ============================================================================

func TestCheckJailStatus_MultipleAPINodes_Failover(t *testing.T) {
	// First server fails
	failServer := createFailingServer(t, http.StatusServiceUnavailable)

	// Second server works
	workingServer := createMockServer(t,
		[]string{"tellor1reporter1"},
		[]string{},
		[]types.Validator{},
	)

	cfg := Config{
		LayerAPIURLs:  []string{failServer.URL, workingServer.URL},
		WalletAddress: "tellor1abc",
		CheckInterval: time.Minute,
	}
	monitor := newTestMonitor(t, cfg)

	// Should fallback to working server
	ctx := context.Background()
	monitor.checkJailStatus(ctx)
	// Test passes if no panic occurs - the working server should be used
}

func TestCheckJailStatus_AllNodesDown(t *testing.T) {
	failServer1 := createFailingServer(t, http.StatusServiceUnavailable)
	failServer2 := createFailingServer(t, http.StatusInternalServerError)

	cfg := Config{
		LayerAPIURLs:  []string{failServer1.URL, failServer2.URL},
		WalletAddress: "tellor1abc",
		CheckInterval: time.Minute,
	}
	monitor := newTestMonitor(t, cfg)

	// Should handle all nodes failing gracefully
	ctx := context.Background()
	monitor.checkJailStatus(ctx)
	// Test passes if no panic occurs
}

// ============================================================================
// Run Loop Tests
// ============================================================================

func TestRun_ContextCancellation(t *testing.T) {
	server := createMockServer(t,
		[]string{},
		[]string{},
		[]types.Validator{},
	)

	cfg := Config{
		LayerAPIURLs:  []string{server.URL},
		WalletAddress: "tellor1abc",
		CheckInterval: 100 * time.Millisecond, // Short interval for test
	}
	monitor := newTestMonitor(t, cfg)

	// Run with cancellable context
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	err := monitor.Run(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRun_ImmediateCancellation(t *testing.T) {
	server := createMockServer(t,
		[]string{},
		[]string{},
		[]types.Validator{},
	)

	cfg := Config{
		LayerAPIURLs:  []string{server.URL},
		WalletAddress: "tellor1abc",
		CheckInterval: time.Hour, // Long interval - won't be reached
	}
	monitor := newTestMonitor(t, cfg)

	// Cancel immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := monitor.Run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

// ============================================================================
// Metric Verification Tests
// ============================================================================

func TestMetrics_NoJailedReporters(t *testing.T) {
	server := createMockServer(t,
		[]string{"tellor1abc", "tellor1def"},
		[]string{}, // No jailed reporters
		[]types.Validator{makeValidator("tellorvaloper1abc", false, "val1")},
	)

	cfg := Config{
		LayerAPIURLs:  []string{server.URL},
		WalletAddress: "tellor1abc",
		CheckInterval: time.Minute,
	}
	monitor := newTestMonitor(t, cfg)

	ctx := context.Background()
	monitor.checkJailStatus(ctx)

	// Verify metrics
	assert.Equal(t, float64(0), testutil.ToFloat64(monitor.ourReporterJailed), "our reporter should not be jailed")
	assert.Equal(t, float64(0), testutil.ToFloat64(monitor.ourValidatorJailed), "our validator should not be jailed")
	assert.Equal(t, float64(0), testutil.ToFloat64(monitor.totalJailedReporters), "total jailed reporters should be 0")
	assert.Equal(t, float64(0), testutil.ToFloat64(monitor.totalJailedValidators), "total jailed validators should be 0")
}

func TestMetrics_OurReporterJailed(t *testing.T) {
	ourAddress := testOurReporterAddr

	server := createMockServer(t,
		[]string{ourAddress, "tellor1other"},
		[]string{ourAddress}, // Our reporter is jailed
		[]types.Validator{},
	)

	cfg := Config{
		LayerAPIURLs:  []string{server.URL},
		WalletAddress: ourAddress,
		CheckInterval: time.Minute,
	}
	monitor := newTestMonitor(t, cfg)

	ctx := context.Background()
	monitor.checkJailStatus(ctx)

	// Verify our reporter jailed metric is set to 1
	assert.Equal(t, float64(1), testutil.ToFloat64(monitor.ourReporterJailed), "our reporter should be jailed")
	assert.Equal(t, float64(1), testutil.ToFloat64(monitor.totalJailedReporters), "total jailed reporters should be 1")
}

func TestMetrics_MultipleJailedReporters(t *testing.T) {
	server := createMockServer(t,
		[]string{"tellor1abc", "tellor1jailed1", "tellor1jailed2", "tellor1jailed3"},
		[]string{"tellor1jailed1", "tellor1jailed2", "tellor1jailed3"},
		[]types.Validator{},
	)

	cfg := Config{
		LayerAPIURLs:  []string{server.URL},
		WalletAddress: "tellor1abc", // Not jailed
		CheckInterval: time.Minute,
	}
	monitor := newTestMonitor(t, cfg)

	ctx := context.Background()
	monitor.checkJailStatus(ctx)

	// Verify metrics
	assert.Equal(t, float64(0), testutil.ToFloat64(monitor.ourReporterJailed), "our reporter should not be jailed")
	assert.Equal(t, float64(3), testutil.ToFloat64(monitor.totalJailedReporters), "total jailed reporters should be 3")
}

func TestMetrics_JailedValidators(t *testing.T) {
	server := createMockServer(t,
		[]string{},
		[]string{},
		[]types.Validator{
			makeValidator("tellorvaloper1val1", false, "val1"),
			makeValidator("tellorvaloper1val2", true, "val2"),
			makeValidator("tellorvaloper1val3", true, "val3"),
		},
	)

	cfg := Config{
		LayerAPIURLs:  []string{server.URL},
		WalletAddress: "tellor1abc",
		CheckInterval: time.Minute,
	}
	monitor := newTestMonitor(t, cfg)

	ctx := context.Background()
	monitor.checkJailStatus(ctx)

	// Verify metrics
	assert.Equal(t, float64(2), testutil.ToFloat64(monitor.totalJailedValidators), "total jailed validators should be 2")
}

func TestMetrics_ResetAfterUnjail(t *testing.T) {
	ourAddress := "tellor1ourreporter"

	// First check: our reporter is jailed
	jailedServer := createMockServer(t,
		[]string{ourAddress},
		[]string{ourAddress}, // Jailed
		[]types.Validator{},
	)

	cfg := Config{
		LayerAPIURLs:  []string{jailedServer.URL},
		WalletAddress: ourAddress,
		CheckInterval: time.Minute,
	}
	monitor := newTestMonitor(t, cfg)

	ctx := context.Background()
	monitor.checkJailStatus(ctx)

	// Verify jailed
	assert.Equal(t, float64(1), testutil.ToFloat64(monitor.ourReporterJailed), "our reporter should be jailed")
	assert.Equal(t, float64(1), testutil.ToFloat64(monitor.totalJailedReporters), "total jailed reporters should be 1")

	// Second check: our reporter is no longer jailed
	unjailedServer := createMockServer(t,
		[]string{ourAddress},
		[]string{}, // No longer jailed
		[]types.Validator{},
	)

	// Update monitor to use new server
	monitor.cfg.LayerAPIURLs = []string{unjailedServer.URL}
	monitor.checkJailStatus(ctx)

	// Verify unjailed - metrics should reset to 0
	assert.Equal(t, float64(0), testutil.ToFloat64(monitor.ourReporterJailed), "our reporter should no longer be jailed")
	assert.Equal(t, float64(0), testutil.ToFloat64(monitor.totalJailedReporters), "total jailed reporters should be 0")
}

// ============================================================================
// Malformed Response Tests
// ============================================================================

func TestCheckJailStatus_MalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"invalid json`))
	}))
	t.Cleanup(func() { server.Close() })

	cfg := Config{
		LayerAPIURLs:  []string{server.URL},
		WalletAddress: "tellor1abc",
		CheckInterval: time.Minute,
	}
	monitor := newTestMonitor(t, cfg)

	// Should handle malformed JSON gracefully
	ctx := context.Background()
	monitor.checkJailStatus(ctx)
}

func TestCheckJailStatus_EmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(``))
	}))
	t.Cleanup(func() { server.Close() })

	cfg := Config{
		LayerAPIURLs:  []string{server.URL},
		WalletAddress: "tellor1abc",
		CheckInterval: time.Minute,
	}
	monitor := newTestMonitor(t, cfg)

	// Should handle empty response gracefully
	ctx := context.Background()
	monitor.checkJailStatus(ctx)
}
