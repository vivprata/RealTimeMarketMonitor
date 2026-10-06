# Demo Runbook — Confluent + watsonx.data Stock Market Demo

## Prerequisites

- `.venv` is set up: `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`
- `FinalDemo/.env` is filled in with all credentials (see `.env.example`)
- Confluent Cloud Flink job is **Running** (see Step 2 below)
- watsonx.data Presto engine is **Active** on IBM Cloud

---

## Start the Demo

### Step 1 — Start simulator + dashboard (one command)

```bash
cd /Users/ibmvrp/Documents/ISL/POCs/ConfluentWxdPOCDemo/FinalDemo
./run_demo.sh
```

This starts:
- The **market simulator** (10-min warmup + random background shocks)
- The **Streamlit dashboard** at http://localhost:8501

#### Options

| Command | Behaviour |
|---|---|
| `./run_demo.sh` | Random background shocks ON (default) |
| `./run_demo.sh --no-random` | Button-only shocks (manual trigger only) |

#### Shock symbol options (manual start only)

```bash
# Shock a random symbol each time button is clicked
.venv/bin/python -m simulator.run_simulator --warmup 10 --seed-portfolio --shock-symbol random

# Shock a specific symbol (e.g. MSFT)
.venv/bin/python -m simulator.run_simulator --warmup 10 --seed-portfolio --shock-symbol MSFT
```

---

### Step 2 — Verify Flink job is Running (Confluent Cloud)

1. Go to **Confluent Cloud → default environment → Flink → Statements**
2. Confirm the `INSERT INTO market_signals` job shows **🟢 Running**
3. If stopped: open a **SQL workspace**, set catalog = `default`, database = `stock_demo_cluster`
4. Paste and run only the `INSERT INTO` statement from [`flink/01_market_signals.sql`](flink/01_market_signals.sql)

---

### Step 3 — Pre-flight health check (optional but recommended)

```bash
.venv/bin/python -m infra.health_check
```

Expected output:
```
✓ topic market_ticks exists
✓ topic market_signals exists
✓ topic portfolio_positions exists
✓ simulator producing market_ticks
✓ schemas registered (4 subjects)
✓ symbols table: 8 rows (expect 8)
✓ anomaly query OK (8 rows)
✓ currently ALL CLEAR
```

> Note: `market_signals NOT growing` warning can be safely ignored — the Flink
> upsert topic only emits when a signal **changes**, so offsets may not grow
> in the 5-second health check window.

---

### Step 4 — Open the dashboard

```
http://localhost:8501
```

#### Pages

| Page | What it shows |
|---|---|
| **Live Market Monitor** | Real-time prices from Kafka, portfolio value, streaming events |
| **Historical Analysis** | Historical OHLCV charts from S3 via watsonx.data |
| **Anomaly Detection** | Live σ score vs historical volatility — fires on shock |
| **Portfolio Rebalancing** | Live position weights + 6m/1y returns from S3 |

---

## Inject a Market Shock (Demo Highlight)

1. Click **⚡ INJECT MARKET SHOCK** button in the sidebar
2. Wait **~60–90 seconds** for Tableflow to commit the Iceberg snapshot
3. Open **Anomaly Detection** page
4. A **🚨 RISK ALERT** banner fires for the shocked symbol

The shock drives the target symbol **~-5.5% with 8× volume spike**.
Flink detects `HIGH_VOLUME_DROP` within seconds.
watsonx.data joins the live drop with historical volatility to compute the anomaly σ score.

---

## Stop the Demo

```bash
./stop_demo.sh
```

This stops both the simulator and the Streamlit dashboard.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| Dashboard shows no live data | Check simulator is running: `pgrep -f run_simulator` |
| Anomaly page shows no rows | Wait for Tableflow commit (~30–60s after shock) |
| watsonx.data query fails | Check `WXD_HOST`, `WXD_USER`, `WXD_PASSWORD` in `.env` |
| Flink job stopped | Re-run `INSERT INTO` from `flink/01_market_signals.sql` in Confluent Cloud |
| `market_signals NOT growing` in health check | False alarm — Flink upsert only emits on changes |
| 401 Unauthorized on watsonx.data | Regenerate IBM Cloud API key for `vivprata@in.ibm.com` |

---

## Key Configuration (FinalDemo/.env)

| Variable | Description |
|---|---|
| `CONFLUENT_BOOTSTRAP_SERVERS` | Confluent Kafka cluster endpoint |
| `CONFLUENT_API_KEY/SECRET` | Kafka cluster API credentials |
| `SCHEMA_REGISTRY_URL` | Confluent Schema Registry endpoint |
| `SCHEMA_REGISTRY_API_KEY/SECRET` | Schema Registry API credentials |
| `AWS_REGION` | `us-east-2` |
| `S3_BUCKET` | `confluent-bucket-us-east` |
| `S3_PREFIX` | `stock-demo` |
| `WXD_HOST` | `afc0481d-...dallas.lakehouse.ibmappdomain.cloud` |
| `WXD_PORT` | `443` |
| `WXD_USER` | `ibmlhapikey_vivprata@in.ibm.com` |
| `WXD_PASSWORD` | IBM Cloud API key |
| `WXD_CATALOG_STREAMING` | `confluent` |
| `WXD_SCHEMA_STREAMING` | `v1-root` |
| `WXD_CATALOG_HISTORICAL` | `confluent` |
| `WXD_SCHEMA_HISTORICAL` | `stock_demo` |

---

## Architecture

```
Simulator → Kafka (market_ticks)
              ↓ Flink SQL
          market_signals → Tableflow → Iceberg (S3: confluent-bucket-us-east)
                                            │
historical Parquet (S3: stock-demo/) ── watsonx.data (Presto)
                                            │
                                       Streamlit (http://localhost:8501)
```

---
*Made with IBM Bob*
