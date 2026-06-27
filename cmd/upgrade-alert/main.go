// Command upgrade-alert watches a Tellor Layer node for upcoming chain upgrades and sends
// a Telegram alert with operator instructions (including how to stage the new cosmovisor
// binary). It is dependency-light (stdlib only) so it can run anywhere the node REST API
// is reachable.
//
// Detection uses two signals, polled every --interval:
//   - the scheduled upgrade plan: GET /cosmos/upgrade/v1beta1/current_plan
//   - governance software-upgrade proposals still in voting (earliest warning):
//     GET /cosmos/gov/v1/proposals
//
// Each distinct upgrade (keyed by plan name) is alerted at most once per phase so the
// channel is not spammed; a final "imminent" alert fires as the target height approaches.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	var (
		restURL     string
		interval    time.Duration
		botToken    string
		chatID      string
		binaryName  string
		daemonHome  string
		imminentWin int64
		runOnce     bool
	)
	flag.StringVar(&restURL, "rest", envOr("LAYER_REST", "http://localhost:1317"), "Layer REST (LCD) base URL")
	flag.DurationVar(&interval, "interval", 5*time.Minute, "How often to poll for upgrades")
	flag.StringVar(&botToken, "telegram-bot-token", os.Getenv("TELEGRAM_BOT_TOKEN"), "Telegram bot token (or env TELEGRAM_BOT_TOKEN)")
	flag.StringVar(&chatID, "telegram-chat-id", os.Getenv("TELEGRAM_CHAT_ID"), "Telegram chat/channel id (or env TELEGRAM_CHAT_ID)")
	flag.StringVar(&binaryName, "binary-name", "layerd", "Daemon binary name (for cosmovisor instructions)")
	flag.StringVar(&daemonHome, "daemon-home", "$DAEMON_HOME", "Daemon home dir (for cosmovisor instructions)")
	flag.Int64Var(&imminentWin, "imminent-blocks", 200, "Send an 'imminent' alert when the plan height is within this many blocks")
	flag.BoolVar(&runOnce, "run-once", false, "Check once and exit (for testing)")
	flag.Parse()

	if botToken == "" || chatID == "" {
		log.Fatal("telegram-bot-token and telegram-chat-id are required (flags or env TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
	}

	w := &watcher{
		rest:        strings.TrimRight(restURL, "/"),
		http:        &http.Client{Timeout: 15 * time.Second},
		tg:          &telegram{token: botToken, chatID: chatID, http: &http.Client{Timeout: 15 * time.Second}},
		binaryName:  binaryName,
		daemonHome:  daemonHome,
		imminentWin: imminentWin,
		alerted:     map[string]string{}, // plan name -> last phase alerted (scheduled|imminent)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if runOnce {
		if err := w.check(ctx); err != nil {
			log.Printf("check failed: %v", err)
			os.Exit(1)
		}
		return
	}

	log.Printf("upgrade-alert watching %s every %s", w.rest, interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	if err := w.check(ctx); err != nil {
		log.Printf("check failed: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down")
			return
		case <-ticker.C:
			if err := w.check(ctx); err != nil {
				log.Printf("check failed: %v", err)
			}
		}
	}
}

type watcher struct {
	rest        string
	http        *http.Client
	tg          *telegram
	binaryName  string
	daemonHome  string
	imminentWin int64
	alerted     map[string]string
}

// check runs one polling cycle: it inspects the scheduled plan and voting proposals and
// emits alerts for anything not yet announced.
func (w *watcher) check(ctx context.Context) error {
	height, err := w.currentHeight(ctx)
	if err != nil {
		return fmt.Errorf("current height: %w", err)
	}

	// 1) Scheduled plan (set once a proposal passes) — the authoritative upcoming upgrade.
	plan, err := w.currentPlan(ctx)
	if err != nil {
		return fmt.Errorf("current_plan: %w", err)
	}
	if plan != nil && plan.Name != "" {
		remaining := plan.Height - height
		phase := "scheduled"
		if remaining <= w.imminentWin && remaining > 0 {
			phase = "imminent"
		}
		if w.alerted[plan.Name] != phase {
			w.alerted[plan.Name] = phase
			w.send(w.planMessage(plan, height, remaining, phase))
		}
		return nil // a scheduled plan supersedes proposal-stage warnings
	}

	// 2) Governance software-upgrade proposals still in voting — earliest possible warning.
	props, err := w.upgradeProposalsInVoting(ctx)
	if err != nil {
		return fmt.Errorf("proposals: %w", err)
	}
	for _, p := range props {
		key := "prop:" + p.Plan.Name + ":" + p.ID
		if w.alerted[key] == "voting" {
			continue
		}
		w.alerted[key] = "voting"
		w.send(w.proposalMessage(p, height))
	}
	return nil
}

func (w *watcher) send(msg string) {
	if err := w.tg.send(msg); err != nil {
		log.Printf("telegram send failed: %v", err)
		return
	}
	log.Printf("alert sent")
}

// ---- chain queries ----

type plan struct {
	Name   string
	Height int64
	Info   string
}

func (w *watcher) currentPlan(ctx context.Context) (*plan, error) {
	var resp struct {
		Plan *struct {
			Name   string `json:"name"`
			Height string `json:"height"`
			Info   string `json:"info"`
		} `json:"plan"`
	}
	if err := w.getJSON(ctx, "/cosmos/upgrade/v1beta1/current_plan", &resp); err != nil {
		return nil, err
	}
	if resp.Plan == nil {
		return nil, nil
	}
	h, _ := strconv.ParseInt(resp.Plan.Height, 10, 64)
	return &plan{Name: resp.Plan.Name, Height: h, Info: resp.Plan.Info}, nil
}

func (w *watcher) currentHeight(ctx context.Context) (int64, error) {
	var resp struct {
		Block struct {
			Header struct {
				Height string `json:"height"`
			} `json:"header"`
		} `json:"block"`
	}
	if err := w.getJSON(ctx, "/cosmos/base/tendermint/v1beta1/blocks/latest", &resp); err != nil {
		return 0, err
	}
	return strconv.ParseInt(resp.Block.Header.Height, 10, 64)
}

type upgradeProposal struct {
	ID            string
	VotingEndTime string
	Plan          plan
}

func (w *watcher) upgradeProposalsInVoting(ctx context.Context) ([]upgradeProposal, error) {
	var resp struct {
		Proposals []struct {
			ID            string `json:"id"`
			Status        string `json:"status"`
			VotingEndTime string `json:"voting_end_time"`
			Messages      []struct {
				Type string `json:"@type"`
				Plan *struct {
					Name   string `json:"name"`
					Height string `json:"height"`
					Info   string `json:"info"`
				} `json:"plan"`
			} `json:"messages"`
		} `json:"proposals"`
	}
	if err := w.getJSON(ctx, "/cosmos/gov/v1/proposals?pagination.limit=200", &resp); err != nil {
		return nil, err
	}
	var out []upgradeProposal
	for _, p := range resp.Proposals {
		if p.Status != "PROPOSAL_STATUS_VOTING_PERIOD" {
			continue
		}
		for _, m := range p.Messages {
			if !strings.Contains(m.Type, "MsgSoftwareUpgrade") || m.Plan == nil {
				continue
			}
			h, _ := strconv.ParseInt(m.Plan.Height, 10, 64)
			out = append(out, upgradeProposal{
				ID:            p.ID,
				VotingEndTime: p.VotingEndTime,
				Plan:          plan{Name: m.Plan.Name, Height: h, Info: m.Plan.Info},
			})
		}
	}
	return out, nil
}

func (w *watcher) getJSON(ctx context.Context, path string, v interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.rest+path, nil)
	if err != nil {
		return err
	}
	res, err := w.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s -> HTTP %d", path, res.StatusCode)
	}
	return json.NewDecoder(res.Body).Decode(v)
}

// ---- message formatting ----

func (w *watcher) planMessage(p *plan, height, remaining int64, phase string) string {
	eta := etaString(remaining)
	head := "🟠 Tellor Layer upgrade SCHEDULED"
	if phase == "imminent" {
		head = "🔴 Tellor Layer upgrade IMMINENT"
	}
	b := &strings.Builder{}
	fmt.Fprintf(b, "%s\n\n", head)
	fmt.Fprintf(b, "Upgrade: %s\n", p.Name)
	fmt.Fprintf(b, "Target height: %d\n", p.Height)
	fmt.Fprintf(b, "Current height: %d\n", height)
	fmt.Fprintf(b, "Blocks remaining: %d (~%s)\n", remaining, eta)
	if p.Info != "" {
		fmt.Fprintf(b, "Info: %s\n", p.Info)
	}
	b.WriteString("\n")
	b.WriteString(w.runbook(p.Name))
	return b.String()
}

func (w *watcher) proposalMessage(p upgradeProposal, height int64) string {
	b := &strings.Builder{}
	fmt.Fprintf(b, "🟡 Tellor Layer upgrade PROPOSED (voting)\n\n")
	fmt.Fprintf(b, "Upgrade: %s (proposal #%s)\n", p.Plan.Name, p.ID)
	fmt.Fprintf(b, "Target height: %d\n", p.Plan.Height)
	fmt.Fprintf(b, "Current height: %d\n", height)
	if p.VotingEndTime != "" {
		fmt.Fprintf(b, "Voting ends: %s\n", p.VotingEndTime)
	}
	b.WriteString("\nIf this passes, the upgrade plan will be scheduled at the target height.\n\n")
	b.WriteString(w.runbook(p.Plan.Name))
	return b.String()
}

// runbook is the operator's how-to-update text, including staging the cosmovisor binary.
func (w *watcher) runbook(name string) string {
	return fmt.Sprintf(`How to update (cosmovisor auto-upgrade):
1. Build/download the new %[1]s binary for %[2]s and verify it:
     %[1]s version   # must print %[2]s
2. Stage it where cosmovisor expects it (named after the plan):
     mkdir -p %[3]s/cosmovisor/upgrades/%[2]s/bin
     cp /path/to/%[1]s %[3]s/cosmovisor/upgrades/%[2]s/bin/%[1]s
     chmod +x %[3]s/cosmovisor/upgrades/%[2]s/bin/%[1]s
3. Ensure cosmovisor env: DAEMON_NAME=%[1]s, DAEMON_HOME=%[3]s,
   DAEMON_RESTART_AFTER_UPGRADE=true (DAEMON_ALLOW_DOWNLOAD_BINARIES=false).
4. At the target height the node halts with: UPGRADE "%[2]s" NEEDED — cosmovisor then
   swaps current -> upgrades/%[2]s/bin and restarts automatically. Verify with:
     %[1]s version   # should print %[2]s`, w.binaryName, name, w.daemonHome)
}

func etaString(blocks int64) string {
	if blocks <= 0 {
		return "now / passed"
	}
	// tellor-1 averages ~1.8s/block; round to a friendly duration.
	d := time.Duration(float64(blocks)*1.8) * time.Second
	return d.Round(time.Minute).String()
}

// ---- telegram ----

type telegram struct {
	token  string
	chatID string
	http   *http.Client
}

func (t *telegram) send(text string) error {
	api := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.token)
	form := url.Values{}
	form.Set("chat_id", t.chatID)
	form.Set("text", text)
	form.Set("disable_web_page_preview", "true")
	res, err := t.http.PostForm(api, form)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram API HTTP %d", res.StatusCode)
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
