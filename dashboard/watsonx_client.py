"""
watsonx.data query client for the Streamlit dashboard.

watsonx.data's Presto engine speaks the Presto/Trino protocol over HTTPS with
basic auth, so we use the `trino` Python client. Queries are cached briefly so
the dashboard stays responsive and doesn't hammer the engine on every rerun.

Connection details come from .env (WXD_*). See .env.example.
"""

from __future__ import annotations

import os

import certifi
import pandas as pd

# macOS python.org builds: point requests/urllib at a real CA bundle.
os.environ.setdefault("SSL_CERT_FILE", certifi.where())
os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())

from config.settings import WatsonxConfig

# A live move at or above this many sigma (typical daily moves) for a symbol
# is flagged as a genuine anomaly. Calibrated so routine trading stays well
# under it and an injected shock clearly crosses it.
ANOMALY_SIGMA_THRESHOLD = 1.5

# Overexposure: flag any single holding above this fraction of the portfolio.
OVEREXPOSURE_THRESHOLD = 0.25

_CONN = None  # module-level reused connection (Presto connect+auth is costly)


def _connection():
    """Return a cached Presto connection, reconnecting if it was closed."""
    global _CONN
    import trino

    if _CONN is not None:
        return _CONN

    cfg = WatsonxConfig.from_env()
    cfg.validate()
    _CONN = trino.dbapi.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        http_scheme="https",
        auth=trino.auth.BasicAuthentication(cfg.user, cfg.password),
        # watsonx.data runs PrestoDB, which requires the X-Presto-User header.
        # The trino client otherwise sends X-Trino-User -> "User must be set" (400).
        http_headers={"X-Presto-User": cfg.user},
    )
    return _CONN


def run_query(sql: str) -> pd.DataFrame:
    """Execute a SQL query against watsonx.data Presto and return a DataFrame.

    Reuses a cached connection; reconnects once on a stale/broken connection.
    """
    global _CONN
    for attempt in (1, 2):
        conn = _connection()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            return pd.DataFrame(rows, columns=cols)
        except Exception:  # noqa: BLE001 -- reconnect once if the cached conn died
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            _CONN = None
            if attempt == 2:
                raise
    return pd.DataFrame()  # unreachable


# Tableflow stores the market_signals table under its Iceberg UUID.
# This maps the logical table name to the actual UUID in the catalog.
_STREAMING_TABLE_MAP = {
    "market_signals": "f5d3b8d1-045c-4932-ac70-a7cb529bcc02",
}


def qual_streaming(table: str) -> str:
    """Fully-qualified name for a streaming (Confluent Iceberg) table."""
    cfg = WatsonxConfig.from_env()
    physical = _STREAMING_TABLE_MAP.get(table, table)
    return f'confluent."{cfg.schema_streaming}"."{physical}"'


def qual_historical(table: str) -> str:
    """Fully-qualified name for a historical (Hive/Parquet) table."""
    cfg = WatsonxConfig.from_env()
    return f"{cfg.catalog_historical}.{cfg.schema_historical}.{table}"


# ---- Query builders -------------------------------------------------------

def q_current_vs_historical() -> str:
    """Live streaming price vs each symbol's historical baseline.

    anomaly_sigma = |deviation %| / (symbol's typical daily move %)
    The SAME move means different things per asset — only computable by joining
    the LIVE stream (Confluent → Iceberg) with HISTORY (S3) in watsonx.data.

    Deviation is measured vs Flink's own 5-minute price_change_pct, which is
    the rolling window change computed in real time. Anomalies only fire on
    genuine shocks (INJECT MARKET SHOCK) that push a symbol > 2σ in 5 minutes.
    """
    s = qual_streaming("market_signals")
    h = qual_historical("historical_risk_metrics")
    return f"""
        WITH latest AS (
          SELECT symbol, avg_volume, volatility, beta
          FROM {h}
          WHERE "date" = (SELECT max("date") FROM {h})
        )
        SELECT
          s.symbol,
          s.price AS current_price,
          s.price_change_pct,
          s.volume_ratio,
          s.status,
          l.avg_volume AS hist_avg_volume,
          l.volatility AS hist_volatility_30d,
          l.beta,
          (l.volatility / SQRT(252) * 100) AS daily_sigma_pct,
          LEAST(
            ABS(s.price_change_pct)
              / NULLIF(l.volatility / SQRT(252) * 100, 0),
            10.0
          ) AS anomaly_sigma
        FROM {s} s
        JOIN latest l ON s.symbol = l.symbol
        ORDER BY anomaly_sigma DESC
    """


def score_anomalies_from_kafka(live_signals: list[dict]) -> pd.DataFrame:
    """Score anomalies using LIVE Kafka signals — bypasses Tableflow/Iceberg lag.

    Instead of reading price_change_pct from the Iceberg table (which has
    30–60s Tableflow lag), this function accepts the signals already consumed
    from Kafka by the dashboard's background consumer thread and scores them
    directly against the historical volatility in S3.

    Args:
        live_signals: list of dicts with keys:
            symbol, price, price_change_pct, volume_ratio, status
            (comes from MarketState.signals via snapshot()['rows'])

    Returns:
        DataFrame with same columns as q_current_vs_historical():
        symbol, current_price, price_change_pct, volume_ratio, status,
        hist_avg_volume, hist_volatility_30d, beta, daily_sigma_pct, anomaly_sigma
    """
    import math
    if not live_signals:
        return pd.DataFrame()

    h = qual_historical("historical_risk_metrics")
    sql = f"""
        SELECT symbol, avg_volume, volatility, beta
        FROM {h}
        WHERE "date" = (SELECT max("date") FROM {h})
    """
    hist = run_query(sql)
    if hist.empty:
        return pd.DataFrame()

    hist_map = {r["symbol"]: r for _, r in hist.iterrows()}
    rows = []
    for sig in live_signals:
        sym = sig.get("symbol")
        # Use Flink's authoritative price_change_pct if available, else local
        change_pct = sig.get("flink_change_pct")
        if change_pct is None:
            change_pct = sig.get("change_pct", 0.0)
        vol_ratio = sig.get("flink_vol_ratio") or sig.get("vol_ratio", 0.0)
        status = sig.get("flink_status") or sig.get("status", "NORMAL")
        h_row = hist_map.get(sym)
        if h_row is None:
            continue
        volatility = float(h_row["volatility"])
        daily_sigma_pct = volatility / math.sqrt(252) * 100
        anomaly_sigma = min(abs(float(change_pct)) / daily_sigma_pct, 10.0) if daily_sigma_pct else 0.0
        rows.append({
            "symbol": sym,
            "current_price": sig.get("price", 0.0),
            "price_change_pct": float(change_pct),
            "volume_ratio": float(vol_ratio),
            "status": status,
            "hist_avg_volume": float(h_row["avg_volume"]),
            "hist_volatility_30d": volatility,
            "beta": float(h_row["beta"]),
            "daily_sigma_pct": daily_sigma_pct,
            "anomaly_sigma": anomaly_sigma,
        })

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("anomaly_sigma", ascending=False).reset_index(drop=True)
    return df


def q_portfolio_exposure() -> str:
    s = qual_streaming("market_signals")
    p = qual_historical("portfolio_positions")
    return f"""
        SELECT
          p.symbol, p.shares, p.purchase_price,
          s.price AS current_price,
          p.shares * s.price AS position_value,
          (s.price - p.purchase_price) * p.shares AS unrealized_pnl,
          s.status
        FROM {p} p
        JOIN {s} s ON p.symbol = s.symbol
        ORDER BY position_value DESC
    """


def q_portfolio_rebalance() -> str:
    """LIVE position weight (streaming price × shares) joined with 6-month and
    1-year historical returns (from S3), all in one watsonx.data cross-source query.
    """
    s = qual_streaming("market_signals")
    p = qual_historical("portfolio_positions")
    hp = qual_historical("historical_prices")
    return f"""
        WITH live AS (
          SELECT p.symbol, p.shares, p.purchase_price, s.price AS current_price,
                 p.shares * s.price AS position_value
          FROM {p} p JOIN {s} s ON p.symbol = s.symbol
        ),
        ranked AS (
          SELECT symbol, close,
                 ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY "date" DESC) AS rn
          FROM {hp}
        ),
        perf AS (
          SELECT
            symbol,
            MAX(CASE WHEN rn = 1   THEN close END) AS last_close,
            MAX(CASE WHEN rn = 126 THEN close END) AS close_6m,
            MAX(CASE WHEN rn = 252 THEN close END) AS close_1y
          FROM ranked
          WHERE rn IN (1, 126, 252)
          GROUP BY symbol
        ),
        tot AS (SELECT SUM(position_value) AS pv FROM live)
        SELECT
          l.symbol,
          l.shares,
          l.current_price,
          l.position_value,
          l.position_value / t.pv * 100                                       AS weight_pct,
          (l.current_price - l.purchase_price) / l.purchase_price * 100      AS total_return_pct,
          (perf.last_close - perf.close_6m) / perf.close_6m * 100            AS return_6m_pct,
          (perf.last_close - perf.close_1y) / perf.close_1y * 100            AS return_1y_pct
        FROM live l
        CROSS JOIN tot t
        JOIN perf ON perf.symbol = l.symbol
        ORDER BY weight_pct DESC
    """


def q_historical_prices(symbol: str, days: int) -> str:
    hp = qual_historical("historical_prices")
    return f"""
        SELECT "date", open, high, low, close, volume
        FROM {hp}
        WHERE symbol = '{symbol}'
        ORDER BY "date" DESC
        LIMIT {int(days)}
    """


def q_historical_risk_latest(symbol: str) -> str:
    """Latest volatility + overall max drawdown for a symbol (one scan)."""
    hr = qual_historical("historical_risk_metrics")
    return f"""
        SELECT
          (SELECT volatility FROM {hr}
             WHERE symbol = '{symbol}' ORDER BY "date" DESC LIMIT 1) AS volatility,
          (SELECT min(drawdown) FROM {hr}
             WHERE symbol = '{symbol}') AS max_drawdown
    """


def list_symbols() -> str:
    ref = qual_historical("company_reference")
    return f"SELECT symbol, company_name, sector FROM {ref} ORDER BY symbol"
