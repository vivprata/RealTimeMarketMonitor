# Phase 4 — Flink SQL (Confluent Cloud for Apache Flink)

Computes `market_signals` from `market_ticks` in real time.

## Files
- `01_market_signals.sql` — `market_signals` table + the continuous `INSERT`
  that computes price change / volume ratio / status (uses `$rowtime`, no ALTER).
- `02_demo_queries.sql` — read-only SELECTs to project during the live demo.

## Prerequisites
1. Topics exist (Phase 3) and the simulator is producing with **Schema Registry**
   (default), so `market_ticks` has an inferred schema Flink can read.
2. A Flink **compute pool** in the same environment/region as the Kafka cluster
   (`lkc-81qqg9m`, us-east-2).

## Deploy
1. Confluent Cloud → your environment → **Flink** → open a **SQL workspace**.
2. Set the workspace **catalog** = your environment and **database** = your
   Kafka cluster, so `market_ticks` / `market_signals` resolve.
3. Run the statements in `01_market_signals.sql` **in order**:
   - **No ALTER needed** — Confluent Flink tables already have a built-in
     `$rowtime` event-time column with a system watermark, which the OVER window
     uses. (If you tried `ALTER TABLE ... ADD WATERMARK` and got "All tables
     declare a system-provided watermark by default", that's why — it's already
     there. Use `ALTER TABLE ... MODIFY` only if you want a custom watermark.)
   - `CREATE TABLE market_signals (...)`
   - `INSERT INTO market_signals ...` — this is the long-running job.
4. In another cell, run queries from `02_demo_queries.sql` to watch output.

## The two lookback windows (important for the demo)
`01_market_signals.sql` uses **two nested OVER windows** (Confluent Flink only
allows one window per query, so they are computed in stacked subqueries):

```sql
-- price baseline (inner): keeps the PRE-shock price so the drop shows full size
RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
-- volume baseline (outer): short window so a sudden surge registers
RANGE BETWEEN INTERVAL '30' SECOND PRECEDING AND CURRENT ROW
```

- **5-minute price window** → `price_change_pct` is measured against the price
  from up to 5 minutes ago, so during the NVDA shock the baseline stays
  pre-shock and the drop shows its full magnitude (~-5%). Run the simulator with
  `--warmup` and let it trade normally for ~1 min before the shock so this
  window is primed (plan risk #1).
- **30-second volume window** → `volume_ratio = current_volume / 30s_avg`, so the
  shock's front-loaded volume burst spikes the ratio above 2 while price is
  already down >3%, letting `HIGH_VOLUME_DROP` fire.

Because these are **OVER (rolling) windows**, every incoming tick emits an
updated signal immediately — there is no tumbling-window delay.

## Notes
- Column `timestamp` is a reserved word — always backtick it (`` `timestamp` ``).
- `market_signals` is an **upsert** table keyed by `symbol`: consumers see the
  latest signal per symbol, which is exactly what the dashboard and Tableflow/
  Iceberg want.

---
*Made with IBM Bob*
