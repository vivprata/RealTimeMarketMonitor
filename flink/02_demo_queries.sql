-- ============================================================================
-- Phase 4 — Handy Flink SQL queries for the live demo (run in SQL workspace).
-- These are read-only SELECTs you can show during Part 4 / Part 5 of the demo.
-- ============================================================================

-- (a) Confirm raw ticks are flowing (Part 3/4). Ctrl-C / stop to end.
SELECT `timestamp`, `symbol`, `price`, `volume`
FROM `market_ticks`
WHERE `symbol` = 'NVDA';


-- (b) Watch the computed signals live (Part 4). During the NVDA shock this row
--     should flip to DOWN / HIGH_VOLUME_DROP.
SELECT `symbol`, `price`, `price_change_pct`, `volume_ratio`, `status`, `timestamp`
FROM `market_signals`;


-- (c) Only the "interesting" signals — nice to project during the shock (Part 5).
SELECT `symbol`, `price`, `price_change_pct`, `volume_ratio`, `status`, `timestamp`
FROM `market_signals`
WHERE `status` IN ('DOWN', 'SIGNIFICANT_DROP', 'HIGH_VOLUME_DROP');


-- (d) Sanity: current price per symbol (latest by event time).
SELECT `symbol`, `price`, `timestamp`
FROM `market_signals`;
