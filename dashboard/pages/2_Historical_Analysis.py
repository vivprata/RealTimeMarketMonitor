"""
Streamlit Page 2 — Historical Analysis.

Per-symbol historical stats + price chart, queried from watsonx.data
(Hive/Parquet over S3). Demonstrates the analytical/lakehouse side.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.theme import flow_strip, header, inject_css, metric_card
from dashboard.watsonx_client import (
    list_symbols,
    q_historical_prices,
    q_historical_risk_latest,
    run_query,
)

st.set_page_config(page_title="Historical Analysis", page_icon="📚", layout="wide")
inject_css()
header(
    "Historical Analysis",
    "Lakehouse analytics in IBM watsonx.data over historical Parquet in S3 "
    "(`hive_catalog`)",
)
flow_strip(active={"Iceberg / S3", "watsonx.data"})


@st.cache_data(ttl=600)
def _symbols() -> pd.DataFrame:
    return run_query(list_symbols())


@st.cache_data(ttl=300)
def _prices(symbol: str, days: int) -> pd.DataFrame:
    return run_query(q_historical_prices(symbol, days))


@st.cache_data(ttl=300)
def _risk(symbol: str) -> pd.DataFrame:
    return run_query(q_historical_risk_latest(symbol))


try:
    syms = _symbols()
except Exception as exc:  # noqa: BLE001
    st.error(f"Could not connect to watsonx.data. Check WXD_* in .env.\n\n{exc}")
    st.stop()

col1, col2 = st.columns(2)
with col1:
    symbol = st.selectbox("Symbol", syms["symbol"].tolist())
with col2:
    period = st.radio(
        "Period",
        [7, 30, 90, 365],
        format_func=lambda d: {7: "7 days", 30: "30 days", 90: "90 days", 365: "1 year"}[d],
        horizontal=True,
        index=1,
    )

try:
    prices = _prices(symbol, period)
    risk = _risk(symbol)
except Exception as exc:  # noqa: BLE001
    st.error(f"Query failed: {exc}")
    st.stop()

# Compute averages client-side from already-fetched rows (avoids a second scan).
avg_price = float(prices["close"].mean()) if not prices.empty else None
avg_volume = float(prices["volume"].mean()) if not prices.empty else None
volatility = (
    float(risk.iloc[0]["volatility"])
    if not risk.empty and risk.iloc[0]["volatility"] is not None
    else None
)
max_drawdown = (
    float(risk.iloc[0]["max_drawdown"])
    if not risk.empty and risk.iloc[0]["max_drawdown"] is not None
    else None
)

c1, c2, c3, c4 = st.columns(4)
c1.markdown(
    metric_card(
        "Average Price",
        f"${avg_price:,.2f}" if avg_price else "—",
        sub=f"over {period} days",
    ),
    unsafe_allow_html=True,
)
c2.markdown(
    metric_card(
        "Average Volume",
        f"{avg_volume / 1_000:,.0f}K" if avg_volume else "—",
    ),
    unsafe_allow_html=True,
)
c3.markdown(
    metric_card(
        "Volatility (annualized)",
        f"{volatility * 100:,.1f}%" if volatility is not None else "—",
    ),
    unsafe_allow_html=True,
)
c4.markdown(
    metric_card(
        "Max Drawdown",
        f"{max_drawdown * 100:,.1f}%" if max_drawdown is not None else "—",
        sub="worst peak-to-trough",
    ),
    unsafe_allow_html=True,
)

st.markdown(f"#### {symbol} — historical close ({period} days)")
if not prices.empty and len(prices) >= 2:
    cdf = prices[["date", "close"]].copy()
    cdf["date"] = pd.to_datetime(cdf["date"], errors="coerce")
    cdf = cdf.dropna().sort_values("date")
    lo, hi = float(cdf["close"].min()), float(cdf["close"].max())
    rng = hi - lo
    pad = rng * 0.1 if rng > 0.02 else max(hi * 0.001, 0.05)
    base = alt.Chart(cdf).encode(
        x=alt.X("date:T", title=None),
        y=alt.Y(
            "close:Q",
            title="Close ($)",
            scale=alt.Scale(domain=[lo - pad, hi + pad], zero=False, nice=False),
            axis=alt.Axis(format="$.2f"),
        ),
    )
    line = base.mark_line(color="#2563eb", strokeWidth=2.5, interpolate="monotone")
    pts = base.mark_circle(size=22, color="#2563eb").encode(
        tooltip=[
            alt.Tooltip("date:T", title="Date"),
            alt.Tooltip("close:Q", title="Close", format="$.2f"),
        ]
    )
    st.altair_chart((line + pts).properties(height=340), use_container_width=True)
    with st.expander("Raw rows"):
        st.dataframe(prices, use_container_width=True, hide_index=True)
elif not prices.empty:
    st.info("Only one data point for this period — pick a longer period.")
else:
    st.info("No historical rows returned.")
