# Local monitoring for neve

A throwaway, single-box Prometheus + Grafana setup for poking at a neve
instance's metrics and dashboards locally — no Grafana Cloud, no agent, no
ingestion pipeline. Reuses the dashboard in [`../grafana/neve-dashboard.json`](../grafana/neve-dashboard.json).

This is intentionally **not** a production observability stack — your org will
have its own metrics pipeline. This is just for development and demos.

## Prerequisites

```sh
brew install prometheus grafana jq
```

## 1. Point Prometheus at your box

Copy this scrape config into Homebrew's Prometheus config and set your host:

```sh
cp prometheus.yml /opt/homebrew/etc/prometheus.yml
# then edit /opt/homebrew/etc/prometheus.yml and replace NEVE_HOST with your
# box's IP/EIP (or 127.0.0.1:9999 if you tunnel — see the comment in the file)
```

neve serves `/metrics` on its RPC port (`8545`). The `node` job is optional host
stats from `prometheus-node-exporter` on the box (`:9100`); drop it if you
haven't installed node-exporter.

> **Reachability:** Prometheus must be able to hit those ports. Either open them
> in the box's security group, or scrape over an SSH tunnel
> (`ssh -N -L 9999:localhost:8545 neve`) to keep `/metrics` off the public net.

## 2. Start the services

```sh
brew services start prometheus    # http://localhost:9090  (data in /opt/homebrew/var/prometheus)
brew services start grafana       # http://localhost:3000  (admin / admin on first login)
```

Confirm both targets are healthy at <http://localhost:9090/targets>. Apply later
config edits with `brew services restart prometheus`.

## 3. Add the datasource + import the dashboard

In Grafana (http://localhost:3000):

1. **Connections → Data sources → Add → Prometheus**, URL `http://localhost:9090`, Save & test.
2. **Dashboards → New → Import → Upload JSON file** → pick `../grafana/neve-dashboard.json` → select the Prometheus datasource.

To update the dashboard after editing the JSON, push it via the API (avoids the
import name-collision and the in-UI v2-schema validation):

```sh
jq '{dashboard: (del(.__comment)), overwrite: true}' ../grafana/neve-dashboard.json \
| curl -fsS -u admin:YOUR_PASSWORD -X POST http://localhost:3000/api/dashboards/db \
    -H 'content-type: application/json' -d @-
```

## 4. Generate some traffic (optional)

To populate the throughput/latency panels, drive a mix of read-only RPC calls:

```sh
WORKERS=6 bash ../../benchmark/gen-traffic.sh http://NEVE_HOST:8545
```

See [`../../benchmark/gen-traffic.sh`](../../benchmark/gen-traffic.sh) for options.
