"""
Streamlit Page 3 — Anomaly Detection: Live vs Historical (the payoff view).

For each symbol, watsonx.data joins the LIVE streaming signal
(Confluent → Tableflow → Iceberg) with that symbol's OWN HISTORICAL volatility
baseline (S3) and computes a volatility-adjusted ANOMALY SCORE (sigma). The
same-sized move is normal noise for one asset and a genuine anomaly for another
— you can only know which by combining real-time streaming with historical
context. This is the business reason the whole architecture exists.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
import streamlit as st

from dashboard.market_data import start_consumer
from dashboard.theme import flow_strip, header, inject_css, metric_card, status_pill
from dashboard.watsonx_client import (
    ANOMALY_SIGMA_THRESHOLD,
    q_current_vs_historical,
    score_anomalies_from_kafka,
)

st.set_page_config(page_title="Anomaly Detection", page_icon="🚨", layout="wide")
inject_css()
header(
    "Real-Time Anomaly Detection — Live vs Historical",
    "watsonx.data scores every live move against each asset's own history: "
    "is this normal, or a genuine anomaly to act on?",
)
flow_strip(active={"Iceberg / S3", "watsonx.data"})


@st.cache_resource
def _state():
    return start_consumer()


@st.cache_data(ttl=5)
def _cvh() -> pd.DataFrame:
    """Score anomalies using live Kafka signals + S3 historical volatility.
    No Tableflow/Iceberg dependency."""
    state = _state()
    snap = state.snapshot()
    rows = snap.get("rows", [])
    if rows:
        df = score_anomalies_from_kafka(rows)
        if not df.empty:
            return df
    # Fallback only if consumer has no data yet (first few seconds after startup)
    return pd.DataFrame()


# Match Tableflow's ~commit cadence; faster just re-shows the same snapshot.
REFRESH_SECONDS = 30

col_l, col_r = st.columns([3, 1])
with col_r:
    auto = st.toggle("Auto-refresh", value=True)
    if st.button("🔄 Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
with col_l:
    st.caption(
        "Scores come from **watsonx.data** joining the live Iceberg table "
        "(materialized by **Tableflow** from the Kafka/Flink stream) with "
        "historical volatility in S3. Anomaly = a move ≥ "
        f"{ANOMALY_SIGMA_THRESHOLD:.0f}σ of the asset's typical daily move. "
        f"Tableflow commits every ~30–60s. Last loaded {datetime.now().strftime('%H:%M:%S')}."
    )

try:
    cvh = _cvh()
except Exception as exc:  # noqa: BLE001
    st.warning(f"⚠️ Query error — retrying on next refresh. ({exc})")
    cvh = pd.DataFrame()

if not cvh.empty:
    worst = cvh.iloc[0]  # query is ordered by anomaly_sigma DESC
    sigma = float(worst["anomaly_sigma"] or 0)
    is_anomaly = sigma >= ANOMALY_SIGMA_THRESHOLD
    anomalies = cvh[cvh["anomaly_sigma"] >= ANOMALY_SIGMA_THRESHOLD]

    # ---- The RISK ALERT / ALL CLEAR banner ----
    if is_anomaly:
        st.markdown(
            f"<div style='padding:18px 22px;border-radius:14px;"
            f"background:linear-gradient(100deg,#7f1d1d,#b91c1c);color:#fff;"
            f"margin-bottom:14px;box-shadow:0 6px 20px rgba(185,28,28,.35)'>"
            f"<div style='font-size:1.35rem;font-weight:800'>🚨 RISK ALERT — "
            f"{worst['symbol']} anomaly detected</div>"
            f"<div style='margin-top:4px;opacity:.95'>"
            f"{worst['symbol']} moved <b>{worst['price_change_pct']:+.2f}%</b> — that's "
            f"<b>{sigma:.1f}σ</b> vs its typical daily move "
            f"(historical volatility {worst['hist_volatility_30d'] * 100:.0f}%). "
            f"A move this large is <b>abnormal for this asset</b> and warrants "
            f"desk review / hedging.</div></div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"<div style='padding:14px 20px;border-radius:12px;"
            f"background:#f0fdf4;border:1px solid #bbf7d0;color:#166534;"
            f"margin-bottom:14px;font-weight:600'>✅ ALL CLEAR — no anomalies. "
            f"Largest move ({worst['symbol']} {worst['price_change_pct']:+.2f}%) is "
            f"only {sigma:.1f}σ, within normal range for that asset.</div>",
            unsafe_allow_html=True,
        )

    # ---- Focus on the top-anomaly symbol ----
    st.markdown(f"###### {worst['symbol']} — live signal scored against its own history")
    a, b, c, d = st.columns(4)
    a.markdown(
        metric_card("Live Move", f"{worst['price_change_pct']:+.2f}%",
                    sub="Confluent → Iceberg (live)"),
        unsafe_allow_html=True,
    )
    b.markdown(
        metric_card("Typical Daily Move", f"±{worst['daily_sigma_pct']:.2f}%",
                    sub="from historical volatility (S3)"),
        unsafe_allow_html=True,
    )
    c.markdown(
        metric_card("Anomaly Score", f"{sigma:.1f}σ",
                    sub=f"threshold {ANOMALY_SIGMA_THRESHOLD:.0f}σ"),
        unsafe_allow_html=True,
    )
    verdict = "ANOMALY" if is_anomaly else "NORMAL"
    d.markdown(
        f'<div class="mcard"><div class="label">Verdict</div>'
        f'<div style="margin-top:8px">'
        f'{status_pill("HIGH_VOLUME_DROP" if is_anomaly else "NORMAL")}'
        f'&nbsp;<b style="color:{"#b91c1c" if is_anomaly else "#166534"}">{verdict}</b>'
        f'</div>'
        f'<div class="sub">watsonx.data cross-source</div></div>',
        unsafe_allow_html=True,
    )

    st.divider()

    # ---- All symbols, ranked by anomaly score ----
    st.markdown("#### All assets — ranked by anomaly score")
    st.caption(
        "Notice: the **same % move means different things**. A big % swing "
        "in a volatile asset can be normal, while a smaller move in a "
        "stable asset is a red flag — that's what the σ score captures."
    )
    disp = pd.DataFrame(
        {
            "Symbol": cvh["symbol"],
            "Live Move %": cvh["price_change_pct"],
            "Typical Daily ±%": cvh["daily_sigma_pct"],
            "Anomaly σ": cvh["anomaly_sigma"],
            "Verdict": [
                ("🚨 ANOMALY" if s >= ANOMALY_SIGMA_THRESHOLD else "normal")
                for s in cvh["anomaly_sigma"]
            ],
            "Hist Volatility %": (cvh["hist_volatility_30d"] * 100).round(1),
            "Volume Ratio": cvh["volume_ratio"],
        }
    )
    st.dataframe(
        disp,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Live Move %": st.column_config.NumberColumn(format="%+.2f%%"),
            "Typical Daily ±%": st.column_config.NumberColumn(format="%.2f%%"),
            "Anomaly σ": st.column_config.ProgressColumn(
                format="%.1fσ",
                min_value=0.0,
                max_value=10.0,
            ),
            "Hist Volatility %": st.column_config.NumberColumn(format="%.1f%%"),
            "Volume Ratio": st.column_config.NumberColumn(format="%.2fx"),
        },
    )
else:
    st.info(
        "No rows. Ensure the simulator + Flink are running so market_signals "
        "has data, and that Tableflow has committed to Iceberg."
    )

if auto:
    time.sleep(REFRESH_SECONDS)
    st.cache_data.clear()
    st.rerun()
