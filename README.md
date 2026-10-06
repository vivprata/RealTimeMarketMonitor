# Confluent + watsonx.data Stock Market Demo — FinalDemo

Real-time stock-market monitoring demo: a simulated market feed flows through
**Confluent Cloud (Kafka + Flink + Tableflow)** into **Apache Iceberg**, is
joined with **historical data in AWS S3** by **watsonx.data**, and visualized
in **Streamlit**.

---

## Quick start (after filling in `.env`)

```bash
# 1. Create + activate the virtual environment
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 2. Copy the config template and fill in your credentials
cp .env.example .env          # then edit .env

# 3. Create Kafka topics (idempotent)
.venv/bin/python -m infra.create_topics

# 4. Start the demo (simulator + dashboard)
./run_demo.sh                 # random background shocks ON
./run_demo.sh --no-random     # button-only shocks

# 5. Stop
./stop_demo.sh

# Pre-flight check (run a few minutes before presenting):
.venv/bin/python -m infra.health_check
```

Dashboard at **http://localhost:8501**

---

## One-time Confluent Cloud setup

See **`SETUP.md`** for a step-by-step UI guide to:
1. Create a Basic Kafka cluster → copy Bootstrap server
2. Create Cluster API Key + Secret
3. Enable Schema Registry → copy URL + API Key
4. Run `create_topics.py` to create the three Kafka topics
5. Deploy `flink/01_market_signals.sql` in the Flink SQL workspace

---

## .env values you need to fill in

| Variable | Where to find it |
|---|---|
| `CONFLUENT_BOOTSTRAP_SERVERS` | Cluster Settings → Bootstrap server |
| `CONFLUENT_API_KEY/SECRET` | Cluster → API Keys → Create key |
| `SCHEMA_REGISTRY_URL` | Environment → Schema Registry panel → Endpoint |
| `SCHEMA_REGISTRY_API_KEY/SECRET` | Schema Registry → API Keys → Create key |
| `AWS_*` | Same S3 bucket as before — no change |
| `WXD_*` | Same watsonx.data instance — only update `WXD_SCHEMA_STREAMING` to your new cluster ID (e.g. `lkc-xxxxxx`) |

---

## Architecture

```
Simulator → Kafka (market_ticks)
              ↓ Flink SQL
          market_signals → Tableflow → Iceberg (S3)
                                           │
historical Parquet (S3) ─────────── watsonx.data (Presto)
                                           │
                                      Streamlit
```

## Project layout

```
config/
  symbols.json            symbol universe + portfolio (single source of truth)
  settings.py             env-driven config: Kafka, S3, simulator
simulator/
  engine.py               pure price/volume engine + scenarios (no I/O)
  run_simulator.py        Confluent Cloud producer runner
infra/
  create_topics.py        create Kafka topics (idempotent)
  health_check.py         pre-demo validation script
dashboard/
  Live_Market_Monitor.py  Page 1: Live Market (main page)
  market_data.py          background Kafka consumer → thread-safe state
  shock_control.py        INJECT SHOCK button → signals simulator
  theme.py                shared CSS + UI components
  watsonx_client.py       Presto/watsonx.data query client
  pages/
    2_Historical_Analysis.py
    3_Anomaly_Detection.py
    4_Portfolio_Rebalancing.py
flink/
  01_market_signals.sql   Flink SQL: ticks → signals (deploy in Confluent Cloud)
  02_demo_queries.sql     ad-hoc demo queries
schemas/
  market_ticks.schema.json
  portfolio_positions.schema.json
  market_signals.schema.json
data/                     generated data (gitignored)
```

## Demo flow

1. **INJECT MARKET SHOCK** button (sidebar on Page 1) sends `SIGUSR1` to the
   simulator, which drives IBM into a ~-5.5% drop + 8× volume spike.
2. Flink detects `HIGH_VOLUME_DROP` within seconds and emits a signal.
3. ~30-60s later Tableflow commits the updated Iceberg snapshot.
4. Open **Anomaly Detection** — watsonx.data joins the live drop with IBM's
   historical volatility and fires a **🚨 RISK ALERT**.
5. Open **Portfolio & Rebalancing** to see the impact on portfolio weights.

---
*Made with IBM Bob*
