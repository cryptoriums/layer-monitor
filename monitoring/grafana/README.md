# Missed Events Dashboard

This folder contains a Grafana dashboard for missed validation and reporting events.

## Metrics

The web service now exposes two Prometheus metrics:

- `layerc_web_tree_missed_current{event_type,scope}`
  - Current missed count from the latest tree cache refresh.
  - `event_type`: `validator_block` or `reporter_cycle`.
  - `scope`: `our` or `network`.

- `layerc_web_tree_missed_events_total{event_type,scope}`
  - Monotonic counter of newly observed missed events between tree refreshes.
  - Useful with `increase(...)` to identify when misses appeared.

## Dashboard import

1. Open Grafana.
2. Go to Dashboards > New > Import.
3. Upload `layer-monitor-missed-events-dashboard.json`.
4. Select your Prometheus datasource for `${DS_PROMETHEUS}`.

## Useful queries

- Current misses (our validator):
  - `layerc_web_tree_missed_current{event_type="validator_block",scope="our"}`
- Current misses (our reporter):
  - `layerc_web_tree_missed_current{event_type="reporter_cycle",scope="our"}`
- New misses in 15 minutes:
  - `increase(layerc_web_tree_missed_events_total{scope="our"}[15m])`
