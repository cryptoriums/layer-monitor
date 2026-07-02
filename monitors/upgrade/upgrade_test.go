package upgrade

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/cryptoriums/layer-monitor/encoding"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"

	"cosmossdk.io/log"
	upgradetypes "cosmossdk.io/x/upgrade/types"

	"github.com/cosmos/cosmos-sdk/client/grpc/cmtservice"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/gogoproto/proto"

	govv1 "github.com/cosmos/cosmos-sdk/x/gov/types/v1"
)

// mockChain serves the three endpoints the upgrade monitor queries, returning proto-JSON
// marshalled from the real cosmos types. Fields are read live so a test can flip state
// (e.g. planErr) between checks.
type mockChain struct {
	t         *testing.T
	cdc       *codec.ProtoCodec
	plan      *upgradetypes.Plan            // scheduled plan (nil = none)
	height    int64                         // latest block height
	proposals *govv1.QueryProposalsResponse // gov proposals (nil = none)
	planErr   bool                          // when true, current_plan returns HTTP 500
}

func (m *mockChain) server() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/cosmos/upgrade/v1beta1/current_plan", func(w http.ResponseWriter, _ *http.Request) {
		if m.planErr {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		m.write(w, &upgradetypes.QueryCurrentPlanResponse{Plan: m.plan})
	})
	mux.HandleFunc("/cosmos/base/tendermint/v1beta1/blocks/latest", func(w http.ResponseWriter, _ *http.Request) {
		m.write(w, &cmtservice.GetLatestBlockResponse{
			SdkBlock: &cmtservice.Block{Header: cmtservice.Header{Height: m.height}},
		})
	})
	mux.HandleFunc("/cosmos/gov/v1/proposals", func(w http.ResponseWriter, _ *http.Request) {
		props := m.proposals
		if props == nil {
			props = &govv1.QueryProposalsResponse{}
		}
		m.write(w, props)
	})
	return httptest.NewServer(mux)
}

func (m *mockChain) write(w http.ResponseWriter, msg proto.Message) {
	b, err := m.cdc.MarshalJSON(msg)
	if err != nil {
		m.t.Fatalf("marshal mock response: %v", err)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func newTestMonitor(t *testing.T, url string) *Monitor {
	t.Helper()
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.ErrorLevel), log.ColorOption(false))
	m, err := New(logger, Config{LayerAPIURLs: []string{url}}, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func assertGauge(t *testing.T, name string, g prometheus.Gauge, want float64) {
	t.Helper()
	if got := testutil.ToFloat64(g); got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

// upgradeProposalResp builds a QueryProposalsResponse with one voting-period proposal that
// carries a MsgSoftwareUpgrade for planName.
func upgradeProposalResp(t *testing.T, planName string) *govv1.QueryProposalsResponse {
	t.Helper()
	anyMsg, err := codectypes.NewAnyWithValue(&upgradetypes.MsgSoftwareUpgrade{
		Plan: upgradetypes.Plan{Name: planName, Height: 999},
	})
	if err != nil {
		t.Fatalf("pack MsgSoftwareUpgrade: %v", err)
	}
	return &govv1.QueryProposalsResponse{
		Proposals: []*govv1.Proposal{
			{Id: 1, Status: govv1.StatusVotingPeriod, Messages: []*codectypes.Any{anyMsg}},
		},
	}
}

func TestUpgradeMetrics_NoUpgrade(t *testing.T) {
	mc := &mockChain{t: t, cdc: encoding.MakeCodec(), height: 100}
	srv := mc.server()
	defer srv.Close()

	m := newTestMonitor(t, srv.URL)
	m.check(context.Background())

	assertGauge(t, "pending", m.pending, 0)
	assertGauge(t, "proposed", m.proposed, 0)
	assertGauge(t, "plan_height", m.planHeight, 0)
	assertGauge(t, "blocks_remaining", m.blocksRemaining, 0)
	assertGauge(t, "gov_proposals_voting", m.proposalsVoting, 0)
}

func TestUpgradeMetrics_Scheduled(t *testing.T) {
	mc := &mockChain{t: t, cdc: encoding.MakeCodec(), height: 100, plan: &upgradetypes.Plan{Name: "v2", Height: 150}}
	srv := mc.server()
	defer srv.Close()

	m := newTestMonitor(t, srv.URL)
	m.check(context.Background())

	assertGauge(t, "pending", m.pending, 1)
	assertGauge(t, "plan_height", m.planHeight, 150)
	assertGauge(t, "blocks_remaining", m.blocksRemaining, 50)
	assertGauge(t, "proposed", m.proposed, 0)
}

func TestUpgradeMetrics_ScheduledPastHeight_RemainingZero(t *testing.T) {
	// Plan height already behind the current height: blocks_remaining floors at 0.
	mc := &mockChain{t: t, cdc: encoding.MakeCodec(), height: 200, plan: &upgradetypes.Plan{Name: "v2", Height: 150}}
	srv := mc.server()
	defer srv.Close()

	m := newTestMonitor(t, srv.URL)
	m.check(context.Background())

	assertGauge(t, "pending", m.pending, 1)
	assertGauge(t, "blocks_remaining", m.blocksRemaining, 0)
}

func TestUpgradeMetrics_ProposalInVoting(t *testing.T) {
	mc := &mockChain{t: t, cdc: encoding.MakeCodec(), height: 100, proposals: upgradeProposalResp(t, "v3")}
	srv := mc.server()
	defer srv.Close()

	m := newTestMonitor(t, srv.URL)
	m.check(context.Background())

	assertGauge(t, "proposed", m.proposed, 1)
	assertGauge(t, "gov_proposals_voting", m.proposalsVoting, 1)
	assertGauge(t, "pending", m.pending, 0)
}

func TestUpgradeMetrics_GovProposalInVoting_NotUpgrade(t *testing.T) {
	// A non-upgrade proposal in voting counts toward gov_proposals_voting but not proposed.
	props := &govv1.QueryProposalsResponse{
		Proposals: []*govv1.Proposal{{Id: 7, Status: govv1.StatusVotingPeriod}},
	}
	mc := &mockChain{t: t, cdc: encoding.MakeCodec(), height: 100, proposals: props}
	srv := mc.server()
	defer srv.Close()

	m := newTestMonitor(t, srv.URL)
	m.check(context.Background())

	assertGauge(t, "gov_proposals_voting", m.proposalsVoting, 1)
	assertGauge(t, "proposed", m.proposed, 0)
}

func TestUpgradeMetrics_QueryErrorKeepsPreviousPending(t *testing.T) {
	// A scheduled plan sets pending=1; a later current_plan query error must leave it at 1
	// rather than clearing a real pending upgrade.
	mc := &mockChain{t: t, cdc: encoding.MakeCodec(), height: 100, plan: &upgradetypes.Plan{Name: "v2", Height: 150}}
	srv := mc.server()
	defer srv.Close()

	m := newTestMonitor(t, srv.URL)
	m.check(context.Background())
	assertGauge(t, "pending", m.pending, 1)

	mc.planErr = true
	m.check(context.Background())
	assertGauge(t, "pending", m.pending, 1)
}
