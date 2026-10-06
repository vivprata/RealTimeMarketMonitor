"""
Streamlit Page 4 — Portfolio & Rebalancing.

The business-decision view: combine LIVE position weights (streaming price ×
shares, via Confluent → Tableflow → Iceberg) with 6-month / 1-year performance
(historical Parquet in S3) — in ONE watsonx.data query — to answer:
  * Am I over-concentrated in any single holding? (overexposure alert)
  * Which holdings are laggards vs leaders?
  * Is my biggest position also a laggard? -> prime rebalancing candidate.

This turns the demo from "watch data" into "make a decision" — relatable to any
audience, and it uses both the real-time and historical sides together.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.market_data import start_consumer
from dashboard.theme import flow_strip, header, inject_css, metric_card
from dashboard.watsonx_client import OVEREXPOSURE_THRESHOLD, qual_historical, run_query

st.set_page_config(page_title="Portfolio & Rebalancing", page_icon="⚖️", layout="wide")
inject_css()
header(
    "Portfolio & Rebalancing",
    "Live position weights (Confluent → Iceberg) + 6-month / 1-year returns "
    "(S3 history), joined in watsonx.data — where should you rebalance?",
)
flow_strip(active={"Iceberg / S3", "watsonx.data"})

REFRESH_SECONDS = 30


@st.cache_resource
def _state():
    return start_consumer()


@st.cache_data(ttl=25)
def _rebalance(live_prices: tuple) -> pd.DataFrame:
    """Build portfolio rebalance view using live Kafka prices + S3 history.
    Bypasses the broken Iceberg snapshot entirely.
    Accepts a tuple of (symbol, price) pairs for st.cache_data hashability."""
    if not live_prices:
        return pd.DataFrame()
    # Convert tuple of (symbol, price) pairs back to dict for lookups
    prices: dict = dict(live_prices)

    hp = qual_historical("historical_prices")
    pp = qual_historical("portfolio_positions")

    # Get historical performance from S3
    perf_sql = f"""
        WITH ranked AS (
          SELECT symbol, close,
                 ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY "date" DESC) AS rn
          FROM {hp}
        ),
        perf AS (
          SELECT symbol,
            MAX(CASE WHEN rn = 1   THEN close END) AS last_close,
            MAX(CASE WHEN rn = 126 THEN close END) AS close_6m,
            MAX(CASE WHEN rn = 252 THEN close END) AS close_1y
          FROM ranked WHERE rn IN (1, 126, 252) GROUP BY symbol
        )
        SELECT p.symbol, p.shares, p.purchase_price,
               perf.last_close, perf.close_6m, perf.close_1y
        FROM {pp} p JOIN perf ON perf.symbol = p.symbol
    """
    try:
        hist = run_query(perf_sql)
    except Exception:
        return pd.DataFrame()

    rows = []
    total_value = sum(
        prices.get(r["symbol"], r["last_close"]) * r["shares"]
        for _, r in hist.iterrows()
    )
    for _, r in hist.iterrows():
        sym = r["symbol"]
        price = prices.get(sym, float(r["last_close"]))
        pos_val = price * r["shares"]
        rows.append({
            "symbol": sym,
            "shares": r["shares"],
            "purchase_price": float(r["purchase_price"]),
            "current_price": price,
            "position_value": pos_val,
            "weight_pct": pos_val / total_value * 100 if total_value else 0,
            "total_return_pct": (price - r["purchase_price"]) / r["purchase_price"] * 100,
            "return_6m_pct": (r["last_close"] - r["close_6m"]) / r["close_6m"] * 100 if r["close_6m"] else 0,
            "return_1y_pct": (r["last_close"] - r["close_1y"]) / r["close_1y"] * 100 if r["close_1y"] else 0,
        })
    return pd.DataFrame(rows).sort_values("weight_pct", ascending=False).reset_index(drop=True)


col_l, col_r = st.columns([3, 1])
with col_r:
    auto = st.toggle("Auto-refresh", value=True)
    if st.button("🔄 Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
with col_l:
    st.caption(
        "Weights use the **live** price from the stream; returns come from "
        "**historical** prices in S3 — combined by **watsonx.data** in one "
        f"query. Overexposure = any holding above {OVEREXPOSURE_THRESHOLD * 100:.0f}% "
        f"of the portfolio. Last loaded {datetime.now().strftime('%H:%M:%S')}."
    )

state = _state()
snap = state.snapshot()
live_prices = {r["symbol"]: r["price"] for r in snap.get("rows", []) if r.get("price")}
try:
    df = _rebalance(tuple(sorted(live_prices.items())))
except Exception as exc:  # noqa: BLE001
    st.warning(f"⚠️ Query error — retrying. ({exc})")
    df = pd.DataFrame()

if df.empty:
    st.info(
        "No rows yet. Ensure the Flink `market_signals` job is running and "
        "Tableflow has committed to Iceberg."
    )
    st.stop()

df = df.sort_values("weight_pct", ascending=False).reset_index(drop=True)
total_value = float((df["shares"] * df["current_price"]).sum())
over = df[df["weight_pct"] > OVEREXPOSURE_THRESHOLD * 100]

laggards = df.nsmallest(2, "return_6m_pct")["symbol"].tolist()
leaders = df.nlargest(2, "return_6m_pct")["symbol"].tolist()

# ---- Summary metrics ----
c1, c2, c3 = st.columns(3)
c1.markdown(
    metric_card(
        "Portfolio Value",
        f"${total_value:,.0f}",
        sub=f"{len(df)} holdings, live prices",
    ),
    unsafe_allow_html=True,
)
top = df.iloc[0]
c2.markdown(
    metric_card(
        "Largest Position",
        f"{top['symbol']} · {top['weight_pct']:.1f}%",
        sub="single-name concentration",
    ),
    unsafe_allow_html=True,
)
c3.markdown(
    metric_card(
        "Overexposed Holdings",
        str(len(over)),
        sub=f"> {OVEREXPOSURE_THRESHOLD * 100:.0f}% of portfolio",
    ),
    unsafe_allow_html=True,
)

# ---- Rebalancing recommendation banner ----
rec = None
for _, r in over.iterrows():
    if r["symbol"] in laggards:
        rec = r
        break

if rec is not None:
    add_names = ", ".join([s for s in leaders if s != rec["symbol"]][:2])
    st.markdown(
        f"<div style='padding:18px 22px;border-radius:14px;"
        f"background:linear-gradient(100deg,#7c2d12,#b45309);color:#fff;"
        f"margin:12px 0;box-shadow:0 6px 20px rgba(180,83,9,.3)'>"
        f"<div style='font-size:1.3rem;font-weight:800'>⚖️ REBALANCING SIGNAL — "
        f"trim {rec['symbol']}</div>"
        f"<div style='margin-top:4px;opacity:.95'>"
        f"<b>{rec['symbol']}</b> is your largest position at "
        f"<b>{rec['weight_pct']:.1f}%</b> (over the {OVEREXPOSURE_THRESHOLD * 100:.0f}% limit) "
        f"and has <b>lagged ({rec['return_6m_pct']:+.1f}% over 6 months)</b>. "
        f"Consider trimming it and rotating into leaders like <b>{add_names}</b>.</div>"
        f"</div>",
        unsafe_allow_html=True,
    )
elif len(over) > 0:
    o = over.iloc[0]
    st.markdown(
        f"<div style='padding:14px 20px;border-radius:12px;background:#fffbeb;"
        f"border:1px solid #fde68a;color:#92400e;margin:12px 0;font-weight:600'>"
        f"⚠ Concentration risk: <b>{o['symbol']}</b> is {o['weight_pct']:.1f}% of the "
        f"portfolio (> {OVEREXPOSURE_THRESHOLD * 100:.0f}%). It's performing well, so "
        f"monitor rather than trim.</div>",
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        "<div style='padding:14px 20px;border-radius:12px;background:#f0fdf4;"
        "border:1px solid #bbf7d0;color:#166534;margin:12px 0;font-weight:600'>"
        "✅ Well diversified — no single holding exceeds the concentration limit.</div>",
        unsafe_allow_html=True,
    )

left, right = st.columns([1, 1])

# ---- Weights bar chart ----
with left:
    st.markdown("#### Position weights")
    wdf = df[["symbol", "weight_pct"]].copy()
    bars = (
        alt.Chart(wdf)
        .mark_bar()
        .encode(
            x=alt.X("weight_pct:Q", title="Weight (%)"),
            y=alt.Y("symbol:N", title=None, sort="-x"),
            color=alt.condition(
                alt.datum.weight_pct > OVEREXPOSURE_THRESHOLD * 100,
                alt.value("#dc2626"),
                alt.value("#2563eb"),
            ),
            tooltip=[
                alt.Tooltip("symbol:N"),
                alt.Tooltip("weight_pct:Q", title="Weight", format=".1f"),
            ],
        )
        .properties(height=300)
    )
    rule = alt.Chart(
        pd.DataFrame({"x": [OVEREXPOSURE_THRESHOLD * 100]})
    ).mark_rule(color="#dc2626", strokeDash=[6, 4]).encode(x="x:Q")
    st.altair_chart(bars + rule, use_container_width=True)
    st.caption(
        f"Dashed line = {OVEREXPOSURE_THRESHOLD * 100:.0f}% concentration limit. "
        "Red bars are overexposed."
    )

# ---- Performance (leaders vs laggards) ----
with right:
    st.markdown("#### 6-month vs 1-year performance")
    perf = df[["symbol", "return_6m_pct", "return_1y_pct"]].melt(
        "symbol", var_name="period", value_name="ret"
    )
    perf["period"] = perf["period"].map(
        {"return_6m_pct": "6 months", "return_1y_pct": "1 year"}
    )
    chart = (
        alt.Chart(perf)
        .mark_bar()
        .encode(
            x=alt.X("ret:Q", title="Return (%)"),
            y=alt.Y("symbol:N", title=None, sort="-x"),
            color=alt.Color(
                "period:N",
                title=None,
                scale=alt.Scale(range=["#0ea5e9", "#1e3a8a"]),
            ),
            yOffset="period:N",
            tooltip=[
                "symbol:N",
                "period:N",
                alt.Tooltip("ret:Q", title="Return", format="+.1f"),
            ],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)
    st.caption(
        f"Leaders (6M): {', '.join(leaders)} · Laggards (6M): {', '.join(laggards)}"
    )

# ---- Full table ----
st.markdown("#### Holdings — weight & performance")
disp = pd.DataFrame(
    {
        "Symbol": df["symbol"],
        "Shares": df["shares"],
        "Price": df["current_price"],
        "Value": df["shares"] * df["current_price"],
        "Weight %": df["weight_pct"],
        "6M Return %": df["return_6m_pct"],
        "1Y Return %": df["return_1y_pct"],
        "Flag": [
            ("🔴 Overexposed" if w > OVEREXPOSURE_THRESHOLD * 100 else "")
            + (" · Laggard" if s in laggards else "")
            for w, s in zip(df["weight_pct"], df["symbol"])
        ],
    }
)
st.dataframe(
    disp,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Price": st.column_config.NumberColumn(format="$%.2f"),
        "Value": st.column_config.NumberColumn(format="$%.0f"),
        "Weight %": st.column_config.ProgressColumn(
            format="%.1f%%",
            min_value=0.0,
            max_value=float(max(40.0, df["weight_pct"].max())),
        ),
        "6M Return %": st.column_config.NumberColumn(format="%+.1f%%"),
        "1Y Return %": st.column_config.NumberColumn(format="%+.1f%%"),
    },
)

if auto:
    time.sleep(REFRESH_SECONDS)
    st.cache_data.clear()
    st.rerun()
