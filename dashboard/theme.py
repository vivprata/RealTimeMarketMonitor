"""
Shared UI theme + helpers for the demo dashboard.

Centralizes CSS, the branded header, the architecture "flow strip", metric
cards and status pills so all pages share a polished, partner-ready look.
"""

from __future__ import annotations

import streamlit as st

# Brand palette
NAVY = "#0b1e3f"
BLUE = "#2563eb"
CYAN = "#06b6d4"
GREEN = "#16a34a"
AMBER = "#d97706"
RED = "#dc2626"
DEEP_RED = "#7f1d1d"

STATUS_COLORS = {
    "HIGH_VOLUME_DROP": (DEEP_RED, "white"),
    "SIGNIFICANT_DROP": ("#991b1b", "white"),
    "DOWN": (AMBER, "white"),
    "UP": (GREEN, "white"),
    "NORMAL": ("#e2e8f0", "#0b1e3f"),
}

_CSS = f"""
<style>
.block-container {{ padding-top: 2.2rem; padding-bottom: 2rem; }}

.demo-header {{
  background: linear-gradient(100deg, {NAVY} 0%, #12306b 55%, {BLUE} 100%);
  border-radius: 16px; padding: 20px 26px; margin-bottom: 14px;
  color: #fff; box-shadow: 0 8px 26px rgba(11,30,63,.28);
}}
.demo-header h1 {{ font-size: 1.55rem; margin: 0; font-weight: 700;
  letter-spacing: .2px; color: #ffffff !important; }}
.demo-header p  {{ margin: 6px 0 0; opacity: .9; font-size: .93rem;
  color: #ffffff !important; }}

.flow {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin: 4px 0 16px; }}
.flow .node {{
  background:#fff; border:1px solid #e2e8f0; border-radius:10px;
  padding:6px 12px; font-size:.8rem; font-weight:600; color:{NAVY};
  box-shadow:0 1px 3px rgba(0,0,0,.06);
}}
.flow .node.active {{ background:{BLUE}; color:#fff; border-color:{BLUE}; }}
.flow .node.lake   {{ background:{CYAN}; color:#062a35; border-color:{CYAN}; }}
.flow .arrow {{ color:#94a3b8; font-weight:700; }}

.mcard {{
  background:#fff; border:1px solid #e9edf3; border-radius:14px;
  padding:14px 16px; box-shadow:0 2px 10px rgba(15,23,42,.05); height:100%;
}}
.mcard .label {{ font-size:.72rem; text-transform:uppercase; letter-spacing:.08em;
  color:#64748b; font-weight:700; }}
.mcard .value {{ font-size:1.7rem; font-weight:800; color:{NAVY};
  line-height:1.15; margin-top:2px; }}
.mcard .delta-pos {{ color:{GREEN}; font-weight:700; font-size:.9rem; }}
.mcard .delta-neg {{ color:{RED}; font-weight:700; font-size:.9rem; }}
.mcard .sub {{ color:#94a3b8; font-size:.78rem; margin-top:2px; }}

.pill {{ display:inline-block; padding:3px 10px; border-radius:999px;
  font-size:.74rem; font-weight:700; letter-spacing:.02em; }}

.livedot {{ height:9px; width:9px; background:{GREEN}; border-radius:50%;
  display:inline-block; margin-right:6px; box-shadow:0 0 0 0 rgba(22,163,74,.7);
  animation: pulse 1.6s infinite; }}
@keyframes pulse {{
  0%   {{ box-shadow:0 0 0 0 rgba(22,163,74,.6); }}
  70%  {{ box-shadow:0 0 0 8px rgba(22,163,74,0); }}
  100% {{ box-shadow:0 0 0 0 rgba(22,163,74,0); }}
}}
section[data-testid="stSidebar"] {{ background: #f8fafc; }}
section[data-testid="stSidebar"] a,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] li {{ color: #0b1e3f !important; font-weight: 600; }}
</style>
"""

_FLOW_STAGES = ["Kafka", "Flink", "Tableflow", "Iceberg / S3", "watsonx.data", "Streamlit"]


def inject_css() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def header(title: str, subtitle: str) -> None:
    st.markdown(
        f'<div class="demo-header"><h1>{title}</h1><p>{subtitle}</p></div>',
        unsafe_allow_html=True,
    )


def flow_strip(active: set[str] | None = None) -> None:
    """Render the architecture flow strip; highlight the stages in `active`."""
    active = active or set()
    parts = []
    for i, stage in enumerate(_FLOW_STAGES):
        cls = "node"
        if stage in active:
            cls += " lake" if stage in ("Iceberg / S3", "watsonx.data") else " active"
        parts.append(f'<span class="{cls}">{stage}</span>')
        if i < len(_FLOW_STAGES) - 1:
            parts.append('<span class="arrow">&rsaquo;</span>')
    st.markdown(f'<div class="flow">{"".join(parts)}</div>', unsafe_allow_html=True)


def metric_card(label: str, value: str, delta: str | None = None,
                delta_positive: bool | None = None, sub: str | None = None) -> str:
    """Return HTML for a styled metric card (place inside a column)."""
    delta_html = ""
    if delta is not None:
        cls = "delta-pos" if delta_positive else "delta-neg"
        delta_html = f'<div class="{cls}">{delta}</div>'
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    return (
        f'<div class="mcard"><div class="label">{label}</div>'
        f'<div class="value">{value}</div>{delta_html}{sub_html}</div>'
    )


def status_pill(status: str) -> str:
    bg, fg = STATUS_COLORS.get(status, ("#e2e8f0", "#0b1e3f"))
    return f'<span class="pill" style="background:{bg};color:{fg}">{status}</span>'


def live_badge(text: str) -> str:
    return (
        f'<span class="livedot"></span>'
        f'<span style="font-size:.82rem;color:#475569;font-weight:600">{text}</span>'
    )
