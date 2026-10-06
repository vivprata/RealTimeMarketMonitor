-- ============================================================================
-- Phase 4 — Flink SQL for Confluent Cloud for Apache Flink
-- File: flink/01_market_signals.sql
--
-- Computes a real-time signal per symbol from `market_ticks` and writes it to
-- `market_signals`. Designed for the demo:
--   * OVER (rolling) windows so EVERY tick emits an updated signal immediately
--     (no waiting for a tumbling window to close -> better live "wow" moment).
--   * TWO nested windows (see step 3): a 5-MINUTE price baseline (so the NVDA
--     drop shows full magnitude) and a 30-SECOND volume baseline (so a sudden
--     volume surge registers -> HIGH_VOLUME_DROP fires). Confluent Flink allows
--     only one window per query, so they are stacked in subqueries.
--
-- HOW TO RUN in Confluent Cloud:
--   Confluent Cloud > your environment > Flink > SQL workspace (compute pool).
--   Make sure the catalog/database point at the environment + Kafka cluster that
--   holds these topics. Then run the statements below in order.
--
-- NOTES on tables:
--   In Confluent Cloud for Apache Flink, each Kafka topic already appears as a
--   table (auto-created from Schema Registry). You do NOT need CREATE TABLE for
--   `market_ticks`, and you do NOT need ALTER — every table already has a
--   built-in `$rowtime` event-time column + system watermark, which the OVER
--   windows use. `market_signals` IS created here (step 2) because Flink must
--   own its typed schema.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1) Event time.
--    In Confluent Cloud for Apache Flink every table ALREADY has a built-in
--    event-time column `$rowtime` (the Kafka record timestamp) WITH a
--    system-provided watermark. So there is NOTHING to run in this step for the
--    OVER windows below — we just ORDER BY `$rowtime`.
--
--    NOTE: our producer sets the Kafka record time at produce time. During the
--    `--warmup` backfill, ticks are produced "now" (not back-dated in Kafka
--    record time), which is fine: the window fills over the first minute of a
--    fresh run. If you want the payload's own timestamp to be the event time
--    instead, MODIFY the watermark (not ADD):
--
--    ALTER TABLE `market_ticks` MODIFY (
--      `event_time` AS TO_TIMESTAMP(REPLACE(REPLACE(`timestamp`,'T',' '),'Z','')),
--      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
--    );
--    ...then use `event_time` instead of `$rowtime` in the OVER windows.
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 2) Create the output table market_signals (keyed by symbol).
--    IMPORTANT: this topic must be created by FLINK (not pre-created as a plain
--    Kafka topic), otherwise Flink sees it as [key: BYTES, val: BYTES] and the
--    INSERT fails with a schema mismatch. If it already exists as a plain topic,
--    delete the topic AND run DROP TABLE first:
--        DROP TABLE IF EXISTS `market_signals`;
--    Key column (`symbol`) must be FIRST in the schema (Confluent Flink rule).
-- ----------------------------------------------------------------------------
CREATE TABLE `market_signals` (
  `symbol`           STRING,
  `timestamp`        STRING,
  `price`            DOUBLE,
  `price_change_pct` DOUBLE,
  `volume_ratio`     DOUBLE,
  `status`           STRING,
  PRIMARY KEY (`symbol`) NOT ENFORCED
) DISTRIBUTED BY (`symbol`) INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'value.format'   = 'json-registry'
);


-- ----------------------------------------------------------------------------
-- 3) The signal computation (continuous INSERT).
--
--   price_change_pct : current price vs the OLDEST price in the lookback window,
--                      per symbol, as a percentage.
--   volume_ratio     : current tick volume / average tick volume over the window.
--   status           : rule-based classification (matches plan §8).
--
--   TWO windows are used (this matters for the demo):
--     * w_price = 5 MINUTE  -> stable PRE-shock price baseline, so the NVDA
--       drop shows its full magnitude (~-5%) instead of being dampened by a
--       sliding baseline. Prime it with the simulator's `--warmup`.
--     * w_vol   = 30 SECOND -> short volume baseline, so a sudden volume surge
--       registers as a high volume_ratio (a 5-min volume avg would dilute the
--       spike and HIGH_VOLUME_DROP would never fire).
-- ----------------------------------------------------------------------------
--   Confluent Flink requires ALL OVER aggregates in ONE query to share the SAME
--   window. So we NEST them: the inner query computes the 5-min price baseline;
--   the outer query computes the 30s volume average on the inner result. Each
--   level has exactly one OVER window.
--
--   Windows (why two):
--     * 5 MINUTE (inner) -> stable PRE-shock price baseline so the NVDA drop
--       shows full magnitude. Prime with the simulator's `--warmup`.
--     * 30 SECOND (outer) -> short volume baseline so a sudden surge registers
--       (a 5-min volume avg would dilute the spike; HIGH_VOLUME_DROP wouldn't fire).
-- ----------------------------------------------------------------------------
INSERT INTO `market_signals`
WITH priced AS (
  SELECT
    `symbol`,
    `timestamp`,
    `price`,
    `volume`,
    `$rowtime` AS `rt`,
    FIRST_VALUE(`price`) OVER w_price AS `price_window_start`
  FROM `market_ticks`
  WINDOW w_price AS (
    PARTITION BY `symbol`
    ORDER BY `$rowtime`
    RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
  )
),
windowed AS (
  SELECT
    `symbol`,
    `timestamp`,
    `price`,
    `volume`,
    `price_window_start`,
    AVG(CAST(`volume` AS DOUBLE)) OVER w_vol AS `avg_volume_window`
  FROM priced
  WINDOW w_vol AS (
    PARTITION BY `symbol`
    ORDER BY `rt`
    RANGE BETWEEN INTERVAL '30' SECOND PRECEDING AND CURRENT ROW
  )
)
SELECT
  `symbol`,
  `timestamp`,
  `price`,
  ROUND(
    CASE WHEN `price_window_start` > 0
         THEN (`price` - `price_window_start`) / `price_window_start` * 100
         ELSE 0 END, 4
  ) AS `price_change_pct`,
  ROUND(
    CASE WHEN `avg_volume_window` > 0
         THEN CAST(`volume` AS DOUBLE) / `avg_volume_window`
         ELSE 0 END, 4
  ) AS `volume_ratio`,
  CASE
    WHEN (`price` - `price_window_start`) / NULLIF(`price_window_start`, 0) * 100 < -3
         AND CAST(`volume` AS DOUBLE) / NULLIF(`avg_volume_window`, 0) > 2
      THEN 'HIGH_VOLUME_DROP'
    WHEN (`price` - `price_window_start`) / NULLIF(`price_window_start`, 0) * 100 <= -5
      THEN 'SIGNIFICANT_DROP'
    WHEN (`price` - `price_window_start`) / NULLIF(`price_window_start`, 0) * 100 <= -3
      THEN 'DOWN'
    WHEN (`price` - `price_window_start`) / NULLIF(`price_window_start`, 0) * 100 >= 3
      THEN 'UP'
    ELSE 'NORMAL'
  END AS `status`
FROM windowed;
