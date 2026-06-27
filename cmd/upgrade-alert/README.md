# upgrade-alert

Watches a Tellor Layer node for upcoming chain upgrades and sends a **Telegram** alert with
operator instructions (including how to stage the new cosmovisor binary). Stdlib-only, so it
runs anywhere the node REST API is reachable.

## What it detects (polled every `--interval`, default 5m)

1. **Proposed (earliest warning)** — governance `MsgSoftwareUpgrade` proposals still in the
   voting period: `GET /cosmos/gov/v1/proposals`.
2. **Scheduled (authoritative)** — the active upgrade plan once a proposal passes:
   `GET /cosmos/upgrade/v1beta1/current_plan` (`plan.name`, `plan.height`).
3. **Imminent** — a final alert when the plan height is within `--imminent-blocks` (default 200)
   of the current height.

Each upgrade is alerted at most once per phase (keyed by plan name) so the channel isn't spammed.

## Run

```bash
export TELEGRAM_BOT_TOKEN=123456:ABC...
export TELEGRAM_CHAT_ID=-1001234567890

go run ./cmd/upgrade-alert \
  --rest=http://localhost:1317 \
  --interval=5m \
  --binary-name=layerd \
  --daemon-home=/root/.layer

# one-shot (CI / manual test):
go run ./cmd/upgrade-alert --run-once
```

Flags: `--rest`, `--interval`, `--telegram-bot-token`, `--telegram-chat-id`, `--binary-name`,
`--daemon-home`, `--imminent-blocks`, `--run-once`. Token/chat also read from
`TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`.

## Example alert

```
🔴 Tellor Layer upgrade IMMINENT

Upgrade: v6.1.6
Target height: 21000000
Current height: 20999850
Blocks remaining: 150 (~5m0s)

How to update (cosmovisor auto-upgrade):
1. Build/download the new layerd binary for v6.1.6 and verify it:
     layerd version   # must print v6.1.6
2. Stage it where cosmovisor expects it (named after the plan):
     mkdir -p $DAEMON_HOME/cosmovisor/upgrades/v6.1.6/bin
     cp /path/to/layerd $DAEMON_HOME/cosmovisor/upgrades/v6.1.6/bin/layerd
     chmod +x $DAEMON_HOME/cosmovisor/upgrades/v6.1.6/bin/layerd
3. Ensure cosmovisor env: DAEMON_NAME=layerd, DAEMON_HOME=$DAEMON_HOME,
   DAEMON_RESTART_AFTER_UPGRADE=true (DAEMON_ALLOW_DOWNLOAD_BINARIES=false).
4. At the target height the node halts with: UPGRADE "v6.1.6" NEEDED — cosmovisor then
   swaps current -> upgrades/v6.1.6/bin and restarts automatically. Verify with:
     layerd version   # should print v6.1.6
```

## Note on framework integration

The Grafana stack already has a `telegram-alerts` contact point. This tool sends the rich,
instruction-bearing upgrade message directly via the Telegram Bot API because that content
(the cosmovisor runbook with the exact plan name) doesn't fit a Prometheus-metric alert. It
can run alongside Grafana as a small sidecar.
