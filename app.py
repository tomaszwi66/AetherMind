"""
app.py - AetherMind Streamlit UI
Premium dark interface with cyan-violet design system.

Four views:
  1. Timeline    - events by date with filters
  2. Ask Memory  - RAG Q&A chat interface
  3. Reflections - daily AI reflections calendar
  4. Stats       - charts, patterns, streaks

Run:
    streamlit run app.py
"""

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import altair as alt
import pandas as pd
import streamlit as st
import yaml

import storage

# ─── Always run from the project directory ────────────────────────────────────
# Fixes "Events: 0 after restart" when Streamlit is launched from a different
# working directory (e.g. new CMD window defaults to C:\Users\PRO\).
os.chdir(Path(__file__).parent.resolve())

# ─── Page config (must be first Streamlit call) ───────────────────────────────

_favicon = Path("favicon.ico")
_icon_png = Path("icon.png")

if _favicon.exists():
    try:
        from PIL import Image as _PILImage
        _page_icon = _PILImage.open(_favicon)
    except Exception:
        _page_icon = "🧠"
elif _icon_png.exists():
    try:
        from PIL import Image as _PILImage
        _page_icon = _PILImage.open(_icon_png)
    except Exception:
        _page_icon = "🧠"
else:
    _page_icon = "🧠"

st.set_page_config(
    page_title="AetherMind",
    page_icon=_page_icon,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Premium CSS injection ────────────────────────────────────────────────────

PREMIUM_CSS = """
<style>
/* ── Google Fonts ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ── Cosmic Dark: deep space blue-purple base ── */
html, body, .stApp {
    background-color: #06061A !important;
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
    color: #E8EEFF !important;
}

/* ── Hide only specific Streamlit chrome, never the sidebar toggle ── */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }

/* Hide deploy button and toolbar decorations specifically */
.stDeployButton { display: none !important; }
[data-testid="stToolbar"] { display: none !important; }
[data-testid="stDecoration"] { display: none !important; }
[data-testid="stStatusWidget"] { display: none !important; }

/* Header: transparent, no height tricks - just clear the background */
header[data-testid="stHeader"] {
    background: transparent !important;
}

/* ── SIDEBAR: always visible, never collapsible ── */
section[data-testid="stSidebar"] {
    transform: translateX(0px) !important;
    visibility: visible !important;
    opacity: 1 !important;
    min-width: 240px !important;
    width: 240px !important;
}
/* Hide the collapse/expand chevron buttons so sidebar can never be toggled */
[data-testid="stSidebarCollapseButton"],
[data-testid="collapsedControl"] {
    display: none !important;
    visibility: hidden !important;
}

/* ── Sidebar: deep space glass ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #080820 0%, #0A0A28 100%) !important;
    border-right: 1px solid rgba(0,200,232,0.12) !important;
    min-width: 240px !important;
    max-width: 260px !important;
}
[data-testid="stSidebarContent"] { background: transparent !important; padding: 0 !important; }
[data-testid="stSidebar"] .stMarkdown p {
    color: #5A5A78 !important;
    font-size: 0.68rem !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
}
[data-testid="stSidebar"] hr {
    border: none !important;
    border-top: 1px solid rgba(255,255,255,0.06) !important;
    margin: 12px 0 !important;
}

/* Sidebar metrics: gradient text */
[data-testid="stSidebar"] [data-testid="stMetricValue"] {
    font-size: 1.8rem !important;
    font-weight: 700 !important;
    background: linear-gradient(135deg, #00E5FF, #A855F7) !important;
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
    background-clip: text !important;
}
[data-testid="stSidebar"] [data-testid="stMetricLabel"] {
    font-size: 0.65rem !important;
    color: #5A5A78 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
}
[data-testid="stSidebar"] [data-testid="metric-container"] {
    background: rgba(0,229,255,0.04) !important;
    border: 1px solid rgba(0,229,255,0.1) !important;
    border-radius: 12px !important;
    padding: 12px 16px !important;
    margin-bottom: 8px !important;
}

/* Sidebar nav: glowing orbital active state */
[data-testid="stSidebar"] .stRadio [data-testid="stWidgetLabel"] {
    color: #5A5A78 !important;
    font-size: 0.65rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
}
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label {
    display: flex !important;
    align-items: center !important;
    gap: 10px !important;
    padding: 9px 14px !important;
    border-radius: 10px !important;
    margin: 2px 0 !important;
    cursor: pointer !important;
    font-size: 0.875rem !important;
    color: #8890BB !important;
    font-weight: 400 !important;
    transition: all 0.2s ease !important;
    border: 1px solid transparent !important;
}
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label:hover {
    background: rgba(0,229,255,0.05) !important;
    color: #C8D0FF !important;
    border-color: rgba(0,229,255,0.12) !important;
}
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label:has(input:checked) {
    background: linear-gradient(135deg, rgba(0,229,255,0.12), rgba(168,85,247,0.12)) !important;
    border-color: rgba(0,229,255,0.25) !important;
    color: #00E5FF !important;
    font-weight: 600 !important;
    box-shadow: 0 0 16px rgba(0,229,255,0.1), inset 0 0 16px rgba(0,229,255,0.04) !important;
}
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] input[type="radio"] {
    display: none !important;
}
/* Radio indicator dot - the circle next to the label text */
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label > div:first-child {
    border: 1.5px solid rgba(255,255,255,0.18) !important;
    background: transparent !important;
    box-shadow: none !important;
}
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label:has(input:checked) > div:first-child {
    border-color: #00E5FF !important;
    background: rgba(0,229,255,0.2) !important;
    box-shadow: 0 0 6px rgba(0,229,255,0.4) !important;
}

/* Sidebar button */
[data-testid="stSidebar"] .stButton > button {
    background: rgba(0,229,255,0.06) !important;
    border: 1px solid rgba(0,229,255,0.15) !important;
    border-radius: 10px !important;
    color: #8890BB !important;
    font-size: 0.82rem !important;
    font-weight: 500 !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(0,229,255,0.1) !important;
    color: #00E5FF !important;
    border-color: rgba(0,229,255,0.3) !important;
    box-shadow: 0 0 12px rgba(0,229,255,0.12) !important;
    transform: none !important;
}

/* ── Main content ── */
.main .block-container {
    padding: 2rem 2.5rem 2rem 2.5rem !important;
    max-width: 1200px !important;
}

/* ── Typography ── */
.stApp h1 {
    font-size: 1.75rem !important;
    font-weight: 700 !important;
    color: #E8EEFF !important;
    letter-spacing: -0.025em !important;
    margin-bottom: 1.5rem !important;
}
.stApp h2 {
    font-size: 1.0rem !important;
    font-weight: 600 !important;
    color: #A0A8D8 !important;
    letter-spacing: -0.01em !important;
}
.stApp h3 { font-size: 0.9rem !important; color: #7880AA !important; }
.stApp h4 {
    font-size: 0.72rem !important;
    font-weight: 600 !important;
    color: #5A5A78 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
}
.stCaption, [data-testid="stCaptionContainer"] {
    color: #5A5A78 !important;
    font-size: 0.76rem !important;
}

/* ── Inputs: glass panels ── */
[data-baseweb="input"] {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 10px !important;
    backdrop-filter: blur(8px) !important;
}
[data-baseweb="input"]:focus-within {
    border-color: rgba(0,229,255,0.5) !important;
    box-shadow: 0 0 0 3px rgba(0,229,255,0.08), 0 0 20px rgba(0,229,255,0.1) !important;
}
[data-baseweb="input"] input {
    background: transparent !important;
    color: #E8EEFF !important;
    font-size: 0.875rem !important;
}
[data-testid="stDateInput"] label,
[data-testid="stSlider"] label,
[data-testid="stMultiSelect"] label,
[data-testid="stSelectbox"] label {
    color: #5A5A78 !important;
    font-size: 0.68rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
}

/* ── Selects / dropdowns ── */
[data-baseweb="select"] > div,
[data-baseweb="select"] > div > div {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 10px !important;
    color: #C8D0FF !important;
    backdrop-filter: blur(8px) !important;
}
[data-baseweb="select"] > div:hover { border-color: rgba(0,229,255,0.25) !important; }
[data-baseweb="popover"] [data-baseweb="menu"], [data-baseweb="popover"] ul {
    background: #0E0E28 !important;
    border: 1px solid rgba(0,229,255,0.15) !important;
    border-radius: 12px !important;
    box-shadow: 0 8px 32px rgba(0,0,0,0.5), 0 0 40px rgba(0,229,255,0.06) !important;
}
[data-baseweb="menu"] [role="option"] { background: transparent !important; color: #A0A8D8 !important; }
[data-baseweb="menu"] [role="option"]:hover {
    background: rgba(0,229,255,0.08) !important;
    color: #00E5FF !important;
}

/* ── Multiselect chips: iridescent gradient pills ── */
[data-baseweb="tag"] {
    background: linear-gradient(135deg, rgba(0,229,255,0.15), rgba(168,85,247,0.15)) !important;
    border: 1px solid rgba(0,229,255,0.3) !important;
    border-radius: 20px !important;
    color: #A0F0FF !important;
    font-size: 0.76rem !important;
    font-weight: 500 !important;
    padding: 3px 10px !important;
}
[data-baseweb="tag"]:hover {
    background: linear-gradient(135deg, rgba(0,229,255,0.25), rgba(168,85,247,0.25)) !important;
    box-shadow: 0 0 12px rgba(0,229,255,0.25) !important;
}
[data-baseweb="tag"] span { color: #A0F0FF !important; }
[data-baseweb="tag"] button { color: rgba(0,229,255,0.5) !important; }
[data-baseweb="tag"] button:hover { color: #00E5FF !important; }
.stMultiSelect [data-baseweb="select"] > div {
    min-height: 44px !important;
}

/* ── Slider: glowing futuristic track ── */
[data-testid="stSlider"] > div > div > div {
    background: rgba(255,255,255,0.08) !important;
    height: 3px !important;
    border-radius: 3px !important;
}
[data-testid="stSlider"] > div > div > div > div {
    background: linear-gradient(90deg, #00E5FF, #A855F7) !important;
    height: 3px !important;
}
[data-testid="stSlider"] [role="slider"] {
    background: #00E5FF !important;
    border: 2px solid #06061A !important;
    box-shadow: 0 0 12px rgba(0,229,255,0.8), 0 0 24px rgba(0,229,255,0.3) !important;
    width: 16px !important;
    height: 16px !important;
    border-radius: 50% !important;
}
[data-testid="stSlider"] [data-testid="stThumbValue"] {
    color: #00E5FF !important;
    font-size: 0.75rem !important;
    font-weight: 600 !important;
}

/* ── Expanders: glass panels ── */
[data-testid="stExpander"] {
    background: rgba(255,255,255,0.02) !important;
    border: 1px solid rgba(255,255,255,0.08) !important;
    border-radius: 14px !important;
    margin-bottom: 8px !important;
    overflow: hidden !important;
    backdrop-filter: blur(8px) !important;
}
[data-testid="stExpander"]:hover {
    border-color: rgba(0,229,255,0.2) !important;
    box-shadow: 0 0 20px rgba(0,229,255,0.06) !important;
}
[data-testid="stExpander"] > details > summary {
    padding: 14px 18px !important;
    font-size: 0.88rem !important;
    font-weight: 600 !important;
    color: #A0A8D8 !important;
    cursor: pointer !important;
    list-style: none !important;
}
[data-testid="stExpander"] > details > summary:hover { color: #E8EEFF !important; }
[data-testid="stExpander"] > details[open] > summary {
    color: #00E5FF !important;
    border-bottom: 1px solid rgba(255,255,255,0.06) !important;
}
[data-testid="stExpander"] > details > div { padding: 16px 18px !important; }

/* ── Buttons ── */
.stButton > button {
    background: linear-gradient(135deg, rgba(0,229,255,0.1), rgba(168,85,247,0.1)) !important;
    border: 1px solid rgba(0,229,255,0.25) !important;
    border-radius: 10px !important;
    color: #00E5FF !important;
    font-size: 0.83rem !important;
    font-weight: 500 !important;
    padding: 8px 18px !important;
    transition: all 0.2s ease !important;
    letter-spacing: 0.02em !important;
}
.stButton > button:hover {
    background: linear-gradient(135deg, rgba(0,229,255,0.18), rgba(168,85,247,0.18)) !important;
    border-color: rgba(0,229,255,0.5) !important;
    box-shadow: 0 0 20px rgba(0,229,255,0.2), 0 0 40px rgba(0,229,255,0.08) !important;
    color: #E8EEFF !important;
    transform: translateY(-1px) !important;
}
.stButton > button:active { transform: translateY(0) !important; }

/* ── Alerts ── */
[data-testid="stAlert"] {
    background: rgba(168,85,247,0.08) !important;
    border: 1px solid rgba(168,85,247,0.2) !important;
    border-radius: 12px !important;
    color: #C4B5FD !important;
}

/* ── Metrics ── */
[data-testid="metric-container"] {
    background: rgba(255,255,255,0.03) !important;
    border: 1px solid rgba(255,255,255,0.08) !important;
    border-radius: 14px !important;
    padding: 16px 20px !important;
    backdrop-filter: blur(8px) !important;
}
[data-testid="stMetricValue"] {
    font-size: 2rem !important;
    font-weight: 700 !important;
    color: #E8EEFF !important;
    -webkit-text-fill-color: #E8EEFF !important;
}
[data-testid="stMetricLabel"] {
    color: #5A5A78 !important;
    font-size: 0.68rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border: 1px solid rgba(255,255,255,0.08) !important;
    border-radius: 12px !important;
}
.stDataFrame thead tr th {
    background: rgba(255,255,255,0.03) !important;
    color: #5A5A78 !important;
    font-size: 0.68rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
    border-bottom: 1px solid rgba(255,255,255,0.08) !important;
}

/* ── Chat ── */
[data-testid="stChatInput"] {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 14px !important;
}
[data-testid="stChatInput"]:focus-within {
    border-color: rgba(0,229,255,0.4) !important;
    box-shadow: 0 0 20px rgba(0,229,255,0.1) !important;
}
[data-testid="stChatInput"] textarea { background: transparent !important; color: #E8EEFF !important; }
[data-testid="stChatMessage"] {
    background: rgba(255,255,255,0.03) !important;
    border: 1px solid rgba(255,255,255,0.08) !important;
    border-radius: 14px !important;
    padding: 14px 18px !important;
    margin-bottom: 10px !important;
}

/* ── Dividers ── */
hr {
    border: none !important;
    border-top: 1px solid rgba(255,255,255,0.06) !important;
    margin: 20px 0 !important;
}

/* ── Code ── */
code {
    background: rgba(0,229,255,0.08) !important;
    color: #00E5FF !important;
    border-radius: 5px !important;
    padding: 1px 6px !important;
    font-size: 0.83em !important;
}

/* ── Charts ── */
.vega-embed { background: transparent !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: rgba(0,229,255,0.2); border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: rgba(0,229,255,0.4); }
</style>
"""

# ─── Light mode CSS override ─────────────────────────────────────────────────

LIGHT_CSS = """
<style>
/* ── Base: light background, dark text ── */
html, body, .stApp {
    background-color: #F5F6FA !important;
    color: #111827 !important;
}

/* ── All text elements ── */
.stApp p, .stApp span, .stApp div, .stApp label,
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] span {
    color: #111827 !important;
}

/* ── Headers ── */
.stApp h1 { color: #0F172A !important; }
.stApp h2 { color: #1E293B !important; }
.stApp h3 { color: #374151 !important; }
.stApp h4 { color: #6B7280 !important; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: #FFFFFF !important;
    border-right: 1px solid #E5E7EB !important;
}
[data-testid="stSidebarContent"] { background: transparent !important; }
[data-testid="stSidebar"] .stMarkdown p {
    color: #9CA3AF !important;
}
[data-testid="stSidebar"] hr { border-color: #E5E7EB !important; }

/* Sidebar metrics */
[data-testid="stSidebar"] [data-testid="metric-container"] {
    background: #F9FAFB !important;
    border: 1px solid #E5E7EB !important;
}
[data-testid="stSidebar"] [data-testid="stMetricValue"] {
    background: linear-gradient(135deg, #0284C7, #7C3AED) !important;
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
    background-clip: text !important;
}
[data-testid="stSidebar"] [data-testid="stMetricLabel"] {
    color: #6B7280 !important;
}

/* Sidebar radio */
[data-testid="stSidebar"] .stRadio [data-testid="stWidgetLabel"] {
    color: #9CA3AF !important;
}
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label {
    color: #374151 !important;
    background: transparent !important;
}
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label:hover {
    background: rgba(2, 132, 199, 0.06) !important;
    color: #0F172A !important;
}
/* Radio indicator dot - hide in light mode, selection shown by label background */
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label > div:first-child,
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label > div:first-child > div,
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label > span:first-child {
    display: none !important;
    visibility: hidden !important;
}

/* Sidebar button (theme toggle) */
[data-testid="stSidebar"] .stButton > button {
    background: #F3F4F6 !important;
    border: 1px solid #E5E7EB !important;
    color: #374151 !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: #E5E7EB !important;
    color: #111827 !important;
    box-shadow: none !important;
}

/* ── Main area ── */
.main .block-container { background: transparent !important; }

/* ── Inputs and date pickers ── */
[data-baseweb="input"] {
    background: #FFFFFF !important;
    border: 1px solid #D1D5DB !important;
}
[data-baseweb="input"]:focus-within {
    border-color: #0284C7 !important;
    box-shadow: 0 0 0 2px rgba(2,132,199,0.15) !important;
}
[data-baseweb="input"] input {
    background: transparent !important;
    color: #111827 !important;
}
[data-testid="stDateInput"] label,
[data-testid="stSlider"] label,
[data-testid="stMultiSelect"] label,
[data-testid="stSelectbox"] label {
    color: #6B7280 !important;
}

/* ── Dropdowns/Selects - the black boxes in your screenshot ── */
[data-baseweb="select"] > div,
[data-baseweb="select"] > div > div {
    background: #FFFFFF !important;
    border: 1px solid #D1D5DB !important;
    color: #111827 !important;
}
[data-baseweb="select"] svg { color: #6B7280 !important; fill: #6B7280 !important; }
[data-baseweb="select"] [data-testid="stMarkdownContainer"] p { color: #111827 !important; }
[data-baseweb="select"] placeholder { color: #9CA3AF !important; }

/* Dropdown menu popup */
[data-baseweb="popover"] [data-baseweb="menu"],
[data-baseweb="popover"] ul {
    background: #FFFFFF !important;
    border: 1px solid #E5E7EB !important;
    box-shadow: 0 4px 16px rgba(0,0,0,0.08) !important;
}
[data-baseweb="menu"] li,
[data-baseweb="menu"] [role="option"] {
    background: #FFFFFF !important;
    color: #111827 !important;
}
/* Target ALL nested spans/divs inside options - PREMIUM_CSS bleeds #A0A8D8 through */
[data-baseweb="menu"] li *,
[data-baseweb="menu"] [role="option"] *,
[data-baseweb="popover"] [data-baseweb="menu"] * {
    color: #111827 !important;
}
[data-baseweb="menu"] [role="option"]:hover,
[data-baseweb="menu"] [role="option"]:hover * {
    background: #F3F4F6 !important;
    color: #0F172A !important;
}

/* ── Multiselect tags (cyan chips) ── */
[data-baseweb="tag"] {
    background: rgba(2, 132, 199, 0.1) !important;
    border: 1px solid rgba(2, 132, 199, 0.3) !important;
    color: #0284C7 !important;
}
[data-baseweb="tag"] span { color: #0284C7 !important; }
[data-baseweb="tag"] button { color: rgba(2,132,199,0.7) !important; }

/* ── Slider ── */
[data-testid="stSlider"] > div > div > div { background: #E5E7EB !important; }
[data-testid="stSlider"] [data-testid="stThumbValue"] { color: #0284C7 !important; }

/* ── Expanders: html body prefix beats emotion-cache dark from config.toml ── */
html body [data-testid="stExpander"] {
    background: #FFFFFF !important;
    border: 1px solid #E5E7EB !important;
    border-radius: 12px !important;
    backdrop-filter: none !important;
}
html body [data-testid="stExpander"] details,
html body [data-testid="stExpander"] details > div {
    background: #FFFFFF !important;
    color: #374151 !important;
}
html body [data-testid="stExpander"] details > summary {
    background: #FFFFFF !important;
    color: #1E293B !important;
}
html body [data-testid="stExpander"] details[open] > summary {
    color: #0284C7 !important;
    border-bottom: 1px solid #E5E7EB !important;
}

/* ── st.json() viewer - dark bg fix ── */
html body [data-testid="stJson"],
html body [data-testid="stJson"] > div,
html body .stJson,
html body .stJson > div {
    background: #F8F9FA !important;
    color: #111827 !important;
    border-radius: 8px !important;
}
html body [data-testid="stJson"] span,
html body [data-testid="stJson"] div {
    background: transparent !important;
}

/* ── Date picker calendar popup ──
   html body prefix: specificity 12 beats emotion-cache class (10)
   even when Streamlit re-injects its CSS after ours              */
html body [data-baseweb="calendar"] {
    background: #FFFFFF !important;
    border-radius: 12px !important;
    box-shadow: 0 4px 24px rgba(0,0,0,0.1) !important;
}
/* Nuke ALL background on every descendant including pseudo-elements */
html body [data-baseweb="calendar"] *,
html body [data-baseweb="calendar"] *::before,
html body [data-baseweb="calendar"] *::after {
    background-color: transparent !important;
    background: transparent !important;
    color: #111827 !important;
}
/* Restore white for the top-level wrapper so the container shows */
html body [data-baseweb="calendar"] > div {
    background-color: #FFFFFF !important;
}
/* Day buttons */
html body [data-baseweb="calendar"] button {
    color: #374151 !important;
}
html body [data-baseweb="calendar"] button:hover {
    background-color: rgba(2,132,199,0.1) !important;
    color: #0284C7 !important;
}
/* Selected day: keep cyan circle */
html body [data-baseweb="calendar"] [aria-selected="true"] button,
html body [data-baseweb="calendar"] [aria-selected="true"] button::before,
html body [data-baseweb="calendar"] [aria-selected="true"] button::after {
    background-color: #0284C7 !important;
    color: #FFFFFF !important;
    border-radius: 50% !important;
}
/* Out-of-month empty cells */
html body [data-baseweb="calendar"] button[disabled],
html body [data-baseweb="calendar"] [aria-disabled="true"] button {
    color: #D1D5DB !important;
}

/* ── Metrics ── */
[data-testid="metric-container"] {
    background: #FFFFFF !important;
    border: 1px solid #E5E7EB !important;
}
[data-testid="stMetricValue"] { color: #0F172A !important; -webkit-text-fill-color: #0F172A !important; }
[data-testid="stMetricLabel"] { color: #6B7280 !important; }

/* ── Buttons ── */
.stButton > button {
    background: rgba(2, 132, 199, 0.08) !important;
    border: 1px solid rgba(2, 132, 199, 0.25) !important;
    color: #0284C7 !important;
    box-shadow: none !important;
}
.stButton > button:hover {
    background: rgba(2, 132, 199, 0.14) !important;
    border-color: rgba(2, 132, 199, 0.4) !important;
    color: #0F172A !important;
    box-shadow: none !important;
    transform: none !important;
}

/* ── Alert / info boxes ── */
[data-testid="stAlert"] {
    background: rgba(2,132,199,0.06) !important;
    border: 1px solid rgba(2,132,199,0.2) !important;
    color: #0369A1 !important;
}

/* ── Caption ── */
.stCaption, [data-testid="stCaptionContainer"] { color: #9CA3AF !important; }

/* ── Code ── */
code {
    background: rgba(2,132,199,0.08) !important;
    color: #0369A1 !important;
}

/* ── Dividers ── */
hr { border-color: #E5E7EB !important; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] { border-color: #E5E7EB !important; }
.stDataFrame thead tr th {
    background: #F9FAFB !important;
    color: #6B7280 !important;
    border-bottom: 1px solid #E5E7EB !important;
}

/* ── Chat ── */
[data-testid="stChatMessage"] {
    background: #FFFFFF !important;
    border: 1px solid #E5E7EB !important;
}
/* stBottom: Streamlit's fixed bottom bar - dark from config.toml theme */
[data-testid="stBottom"],
[data-testid="stBottom"] > div {
    background-color: #F5F6FA !important;
}
/* Chat input: use descendant div selector (no >) to catch all nesting levels */
[data-testid="stChatInput"] {
    background: #FFFFFF !important;
    border: 1px solid #D1D5DB !important;
    border-radius: 14px !important;
    overflow: hidden !important;
}
[data-testid="stChatInput"] div {
    background-color: #FFFFFF !important;
    background: #FFFFFF !important;
}
[data-testid="stChatInput"] textarea {
    background: transparent !important;
    background-color: transparent !important;
    color: #111827 !important;
}
[data-testid="stChatInput"] textarea::placeholder {
    color: #9CA3AF !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar-track { background: #F5F6FA; }
::-webkit-scrollbar-thumb { background: #D1D5DB; }
::-webkit-scrollbar-thumb:hover { background: #9CA3AF; }

/* ── Vega-Embed chart controls (Stats view: ..., fullscreen) ── */
.vega-embed summary,
.vega-embed .vega-actions,
.vega-embed .vega-actions a,
.vega-embed .vega-actions button {
    background: #FFFFFF !important;
    color: #374151 !important;
    border-color: #E5E7EB !important;
}
.vega-embed .vega-actions a:hover {
    background: #F3F4F6 !important;
    color: #111827 !important;
}
.vega-tooltip, .vega-tooltip * {
    background: #FFFFFF !important;
    color: #111827 !important;
    border: 1px solid #E5E7EB !important;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08) !important;
}
[data-testid="StyledFullScreenButton"] button,
[data-testid="StyledFullScreenButton"] svg {
    background: rgba(255,255,255,0.9) !important;
    color: #374151 !important;
    fill: #374151 !important;
    border-radius: 6px !important;
}

</style>
"""

# ─── Apply CSS based on theme ─────────────────────────────────────────────────

if "theme" not in st.session_state:
    st.session_state.theme = "dark"

st.markdown(PREMIUM_CSS, unsafe_allow_html=True)
if st.session_state.theme == "light":
    st.markdown(LIGHT_CSS, unsafe_allow_html=True)

# ─── Load config + connections ────────────────────────────────────────────────

@st.cache_resource
def load_config():
    with open("config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


@st.cache_resource
def get_db(config):
    return storage.init_db(config["paths"]["sqlite_db"])


@st.cache_resource
def get_qdrant(config):
    client = storage.get_qdrant_client(config["qdrant"]["storage_path"])
    storage.ensure_collection(client, config)
    return client


@st.cache_resource
def get_embedding_model(config):
    from index import load_model
    return load_model(config)


# ─── Design tokens ───────────────────────────────────────────────────────────

TYPE_COLORS = {
    "work":     "#3B82F6",   # blue
    "note":     "#F59E0B",   # amber
    "health":   "#10B981",   # emerald
    "social":   "#EC4899",   # pink
    "location": "#8B5CF6",   # violet
    "unknown":  "#6B7280",   # gray
}

TYPE_ICONS = {
    "work":     "💻",
    "note":     "📝",
    "health":   "💪",
    "social":   "👥",
    "location": "📍",
    "unknown":  "❔",
}

TYPE_BG = {
    "work":     "rgba(59, 130, 246, 0.08)",
    "note":     "rgba(245, 158, 11, 0.08)",
    "health":   "rgba(16, 185, 129, 0.08)",
    "social":   "rgba(236, 72, 153, 0.08)",
    "location": "rgba(139, 92, 246, 0.08)",
    "unknown":  "rgba(107, 114, 128, 0.08)",
}


# ─── Sidebar ─────────────────────────────────────────────────────────────────

def sidebar(config) -> str:
    with st.sidebar:
        # Logo + Brand
        logo_path = Path("icon.png")
        if logo_path.exists():
            col_logo, col_brand = st.columns([1, 2.5])
            with col_logo:
                st.image(str(logo_path), width=44)
            with col_brand:
                st.markdown(
                    '<div style="padding: 10px 0 0 2px; font-size: 1.05rem; '
                    'font-weight: 700; color: #E2E8F0; letter-spacing: -0.01em;">AetherMind</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.markdown(
                '<div style="padding: 16px 16px 12px; font-size: 1.1rem; '
                'font-weight: 700; color: #E2E8F0;">AetherMind</div>',
                unsafe_allow_html=True,
            )

        st.markdown("---")

        # Stats
        conn = get_db(config)
        total = storage.get_events_count(conn)
        indexed = storage.get_indexed_count(conn)

        col1, col2 = st.columns(2)
        col1.metric("Events", total)
        col2.metric("Indexed", indexed)

        st.markdown("---")

        # Navigation
        view = st.radio(
            "Navigate",
            ["📅  Timeline", "💬  Ask Memory", "🌙  Reflections", "📊  Stats"],
        )

        st.markdown("---")

        # Theme toggle
        is_dark = st.session_state.theme == "dark"
        toggle_label = "☀️  Light mode" if is_dark else "🌙  Dark mode"
        if st.button(toggle_label, use_container_width=True):
            st.session_state.theme = "light" if is_dark else "dark"
            st.rerun()

    return view


# ─── Event card renderer ──────────────────────────────────────────────────────

def _render_event_card(ev: dict):
    is_light = st.session_state.get("theme", "dark") == "light"
    color = TYPE_COLORS.get(ev["type"], "#6B7280")
    icon = TYPE_ICONS.get(ev["type"], "❔")
    time_str = ev["timestamp"][11:16] if len(ev["timestamp"]) > 10 else "00:00"
    tags = ev.get("tags", [])
    importance = ev.get("importance", 0.5)
    source = ev.get("source", "")
    summary = ev.get("summary", "")

    # Theme-aware styles
    if is_light:
        card_bg = "rgba(255,255,255,0.9)"
        card_border = f"rgba(0,0,0,0.07)"
        text_color = "#0F1020"
        time_color = "#9CA3AF"
        tag_bg = "rgba(2,132,199,0.08)"
        tag_border = "rgba(2,132,199,0.2)"
        tag_color = "#0284C7"
        source_bg = "rgba(0,0,0,0.05)"
        source_border = "rgba(0,0,0,0.1)"
        source_color = "#6B7280"
        glow = f"0 2px 16px rgba(0,0,0,0.06)"
    else:
        card_bg = "rgba(255,255,255,0.03)"
        card_border = "rgba(255,255,255,0.08)"
        text_color = "#E8EEFF"
        time_color = "#5A5A78"
        tag_bg = "rgba(168,85,247,0.1)"
        tag_border = "rgba(168,85,247,0.2)"
        tag_color = "#C4B5FD"
        source_bg = "rgba(255,255,255,0.05)"
        source_border = "rgba(255,255,255,0.1)"
        source_color = "#8890BB"
        glow_intensity = importance * 0.15
        glow = f"0 4px 24px rgba(0,229,255,{glow_intensity:.2f}), 0 1px 4px rgba(0,0,0,0.4)"

    # Tag pills
    tag_html = ""
    for tag in tags[:7]:
        tag_html += (
            f'<span style="display:inline-block; padding: 2px 10px; margin: 0 4px 4px 0; '
            f'background: {tag_bg}; border: 1px solid {tag_border}; '
            f'border-radius: 20px; font-size: 0.71rem; color: {tag_color}; font-weight: 500;">'
            f'{tag}</span>'
        )

    # Importance dots
    filled = round(importance * 5)
    dot_html = ""
    for i in range(5):
        if is_light:
            c = f"rgba(2,132,199,{1.0 if i < filled else 0.2})"
        else:
            c = f"rgba(0,229,255,{1.0 if i < filled else 0.15})"
        dot_html += f'<span style="color:{c}; font-size:0.5rem; line-height:1;">&#9679;</span>'

    source_badge = (
        f'<span style="font-size: 0.67rem; color: {source_color}; background: {source_bg}; '
        f'border: 1px solid {source_border}; border-radius: 5px; padding: 1px 7px; '
        f'letter-spacing: 0.04em;">{source}</span>'
    )

    # Left accent line - iridescent gradient
    left_accent = (
        'linear-gradient(180deg, #00E5FF, #A855F7)' if not is_light
        else f'linear-gradient(180deg, {color}, {color}88)'
    )

    st.markdown(
        f"""
        <div style="
            position: relative;
            background: {card_bg};
            border: 1px solid {card_border};
            border-radius: 14px;
            padding: 14px 16px 12px 20px;
            margin: 6px 0;
            box-shadow: {glow};
            backdrop-filter: blur(12px);
            overflow: hidden;
        ">
            <div style="
                position: absolute; left: 0; top: 10%; bottom: 10%;
                width: 2px; border-radius: 2px;
                background: {left_accent};
                box-shadow: 0 0 8px rgba(0,229,255,0.5);
            "></div>
            <div style="display:flex; align-items:flex-start; justify-content:space-between; margin-bottom:8px;">
                <div style="display:flex; align-items:center; gap:8px; flex:1; min-width:0;">
                    <span style="font-size:0.68rem; color:{time_color}; font-variant-numeric:tabular-nums; flex-shrink:0; font-weight:500;">{time_str}</span>
                    <span style="font-size:0.93rem; font-weight:600; color:{text_color}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">
                        {icon} {summary}
                    </span>
                </div>
                <div style="display:flex; align-items:center; gap:7px; flex-shrink:0; margin-left:12px;">
                    <span style="display:flex; gap:2px; align-items:center;">{dot_html}</span>
                    {source_badge}
                </div>
            </div>
            <div style="padding-left:0; margin-top:4px;">{tag_html if tag_html else ""}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("Details", expanded=False):
        st.json(ev)


# ─── View 1: Timeline ─────────────────────────────────────────────────────────

def view_timeline(config):
    st.markdown(
        '<h1 style="display:flex;align-items:center;gap:12px;">'
        '<span style="font-size:1.5rem;">📅</span> Timeline</h1>',
        unsafe_allow_html=True,
    )
    conn = get_db(config)

    # Filter row
    col1, col2 = st.columns([1, 3])
    with col1:
        end_date = st.date_input("To", value=date.today())
        start_date = st.date_input("From", value=date.today() - timedelta(days=30))

    with col2:
        type_filter = st.multiselect(
            "Event types",
            ["work", "note", "health", "social", "location", "unknown"],
            default=["work", "note", "health", "social", "location"],
        )
        source_filter = st.multiselect(
            "Sources",
            ["git", "notes", "calendar", "google_calendar", "google_timeline", "manual"],
            default=[],
        )

    min_importance = st.slider("Min importance", 0.0, 1.0, 0.0, 0.05)

    if start_date > end_date:
        st.error("Start date must be before end date.")
        return

    start_unix = int(datetime.combine(start_date, datetime.min.time()).timestamp())
    end_unix = int(datetime.combine(end_date, datetime.max.time()).timestamp())
    events = storage.get_events_for_range(conn, start_unix, end_unix)

    if type_filter:
        events = [e for e in events if e["type"] in type_filter]
    if source_filter:
        events = [e for e in events if e["source"] in source_filter]
    events = [e for e in events if e.get("importance", 0) >= min_importance]

    if not events:
        st.markdown(
            '<div style="text-align:center; padding: 48px; color: #6B6B82; font-size: 0.9rem;">'
            'No events found for this date range and filters.</div>',
            unsafe_allow_html=True,
        )
        return

    st.markdown(
        f'<div style="font-size: 0.78rem; color: #6B6B82; margin-bottom: 16px;">'
        f'{len(events)} events found</div>',
        unsafe_allow_html=True,
    )

    by_date: dict[str, list] = {}
    for ev in sorted(events, key=lambda e: e["timestamp"], reverse=True):
        d = ev["timestamp"][:10]
        by_date.setdefault(d, []).append(ev)

    for day, day_events in by_date.items():
        label = f"**{day}**  ·  {len(day_events)} event{'s' if len(day_events) != 1 else ''}"
        with st.expander(label, expanded=(day == str(date.today()))):
            for ev in day_events:
                _render_event_card(ev)


# ─── View 2: Ask Memory ───────────────────────────────────────────────────────

def view_ask(config):
    st.markdown(
        '<h1 style="display:flex;align-items:center;gap:12px;">'
        '<span style="font-size:1.5rem;">💬</span> Ask Memory</h1>',
        unsafe_allow_html=True,
    )

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sources"):
                with st.expander(f"Sources - {len(msg['sources'])} events"):
                    for ev in msg["sources"][:6]:
                        c = TYPE_COLORS.get(ev.get("type", "unknown"), "#6B7280")
                        st.markdown(
                            f'<div style="border-left: 2px solid {c}; padding: 4px 10px; '
                            f'margin: 3px 0; font-size: 0.82rem; color: #A0A0B8;">'
                            f'<b style="color:#E2E8F0;">{ev["timestamp"][:10]}</b> &nbsp;'
                            f'<span style="color:#6B6B82;">{ev["type"]}/{ev["source"]}</span>'
                            f' &nbsp; {ev["summary"][:110]}</div>',
                            unsafe_allow_html=True,
                        )

    col1, col2 = st.columns([5, 1])
    with col1:
        question = st.chat_input("Ask your memory...")
    with col2:
        if st.button("Clear", use_container_width=True):
            st.session_state.chat_history = []
            st.rerun()

    if question:
        st.session_state.chat_history.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.markdown(question)

        conn = get_db(config)
        qdrant = get_qdrant(config)
        indexed = storage.get_indexed_count(conn)

        if indexed == 0:
            answer = "No memories indexed yet. Run `python run_pipeline.py` first."
            sources = []
        else:
            with st.spinner("Searching memory..."):
                try:
                    model = get_embedding_model(config)
                    from ask import retrieve_and_answer
                    answer, sources = retrieve_and_answer(question, conn, qdrant, model, config)
                except Exception as e:
                    answer = f"Error: {e}"
                    sources = []

        st.session_state.chat_history.append(
            {"role": "assistant", "content": answer, "sources": sources}
        )
        with st.chat_message("assistant"):
            st.markdown(answer)
            if sources:
                with st.expander(f"Sources - {len(sources)} events"):
                    for ev in sources[:6]:
                        c = TYPE_COLORS.get(ev.get("type", "unknown"), "#6B7280")
                        st.markdown(
                            f'<div style="border-left: 2px solid {c}; padding: 4px 10px; '
                            f'margin: 3px 0; font-size: 0.82rem; color: #A0A0B8;">'
                            f'<b style="color:#E2E8F0;">{ev["timestamp"][:10]}</b> &nbsp;'
                            f'<span style="color:#6B6B82;">{ev["type"]}/{ev["source"]}</span>'
                            f' &nbsp; {ev["summary"][:110]}</div>',
                            unsafe_allow_html=True,
                        )


# ─── View 3: Reflections ──────────────────────────────────────────────────────

def view_reflections(config):
    st.markdown(
        '<h1 style="display:flex;align-items:center;gap:12px;">'
        '<span style="font-size:1.5rem;">🌙</span> Daily Reflections</h1>',
        unsafe_allow_html=True,
    )
    conn = get_db(config)

    col1, _ = st.columns([1, 3])
    with col1:
        selected_date = st.date_input("Select date", value=date.today())

    reflection = storage.get_reflection_for_date(conn, str(selected_date))

    if not reflection:
        st.markdown(
            f'<div style="background: rgba(139,92,246,0.06); border: 1px solid rgba(139,92,246,0.18); '
            f'border-radius: 10px; padding: 20px 22px; color: #C4B5FD; font-size: 0.88rem;">'
            f'No reflection for <b>{selected_date}</b>. '
            f'Run: <code>python reflect.py --date {selected_date}</code></div>',
            unsafe_allow_html=True,
        )
    else:
        for field in ("wins", "risks", "patterns"):
            if isinstance(reflection.get(field), str):
                try:
                    reflection[field] = json.loads(reflection[field])
                except Exception:
                    reflection[field] = []

        MOOD_COLORS = {
            "focused": "#3B82F6", "productive": "#10B981", "social": "#F59E0B",
            "tired": "#EF4444", "stressed": "#EF4444", "creative": "#8B5CF6",
            "mixed": "#6B7280", "unknown": "#6B7280",
        }
        mood = reflection.get("mood", "unknown")
        mood_color = MOOD_COLORS.get(mood, "#6B7280")

        col1, col2, col3 = st.columns(3)
        col1.metric("Theme", reflection.get("theme", "-"))
        col2.metric("Mood", mood)
        col3.metric("Events", reflection.get("event_count", 0))

        st.markdown(
            f'<div style="background: #0F0F1A; border: 1px solid #1A1A2E; '
            f'border-radius: 12px; padding: 18px 20px; margin: 16px 0; '
            f'line-height: 1.7; color: #C8C8DC; font-size: 0.9rem;">'
            f'{reflection.get("summary", "-")}</div>',
            unsafe_allow_html=True,
        )

        col1, col2, col3 = st.columns(3)

        def _list_card(col, title, items, color, bg):
            with col:
                st.markdown(
                    f'<div style="background: {bg}; border: 1px solid rgba({color},0.2); '
                    f'border-radius: 10px; padding: 14px 16px;">'
                    f'<div style="font-size: 0.72rem; font-weight: 600; '
                    f'text-transform: uppercase; letter-spacing: 0.07em; '
                    f'color: rgba({color},1); margin-bottom: 10px;">{title}</div>'
                    + "".join(
                        f'<div style="font-size: 0.85rem; color: #C8C8DC; '
                        f'padding: 4px 0; border-bottom: 1px solid rgba(255,255,255,0.04);">'
                        f'- {item}</div>'
                        for item in (items or [])
                    )
                    + "</div>",
                    unsafe_allow_html=True,
                )

        _list_card(col1, "Wins", reflection.get("wins", []), "16,185,129", "rgba(16,185,129,0.06)")
        _list_card(col2, "Risks", reflection.get("risks", []), "239,68,68", "rgba(239,68,68,0.06)")
        _list_card(col3, "Patterns", reflection.get("patterns", []), "139,92,246", "rgba(139,92,246,0.06)")

    # Recent table
    st.markdown('<hr style="margin: 28px 0 20px;"/>', unsafe_allow_html=True)
    st.markdown(
        '<h2 style="font-size:0.85rem; text-transform:uppercase; '
        'letter-spacing:0.08em; color:#8B8BA7;">Recent Reflections</h2>',
        unsafe_allow_html=True,
    )
    end = str(date.today())
    start = str(date.today() - timedelta(days=30))
    recent = storage.get_reflections_for_range(conn, start, end)
    if recent:
        df_data = []
        for r in recent:
            for field in ("wins", "risks", "patterns"):
                if isinstance(r.get(field), str):
                    try:
                        r[field] = json.loads(r[field])
                    except Exception:
                        r[field] = []
            df_data.append({
                "Date": r["date"],
                "Theme": r.get("theme", "-"),
                "Mood": r.get("mood", "-"),
                "Events": r.get("event_count", 0),
                "Summary": r.get("summary", "")[:100],
            })
        st.dataframe(pd.DataFrame(df_data), use_container_width=True, hide_index=True)
    else:
        st.markdown(
            '<div style="color: #6B6B82; font-size: 0.88rem; padding: 12px 0;">No reflections in the last 30 days.</div>',
            unsafe_allow_html=True,
        )


# ─── View 4: Stats ────────────────────────────────────────────────────────────

_CHART_CONFIG = {
    "background": "transparent",
    "view": {"stroke": "transparent"},
    "axis": {
        "domainColor": "#2A2A40",
        "gridColor": "#1A1A2E",
        "labelColor": "#8B8BA7",
        "titleColor": "#8B8BA7",
        "labelFontSize": 11,
        "titleFontSize": 11,
    },
    "legend": {"labelColor": "#8B8BA7", "titleColor": "#8B8BA7", "labelFontSize": 11},
    "title": {"color": "#E2E8F0", "fontSize": 13, "fontWeight": 600},
}


def view_stats(config):
    st.markdown(
        '<h1 style="display:flex;align-items:center;gap:12px;">'
        '<span style="font-size:1.5rem;">📊</span> Stats & Patterns</h1>',
        unsafe_allow_html=True,
    )
    conn = get_db(config)

    total = storage.get_events_count(conn)
    indexed = storage.get_indexed_count(conn)

    if total == 0:
        st.markdown(
            '<div style="text-align:center; padding: 48px; color: #6B6B82;">No events yet. Run the pipeline to import your data.</div>',
            unsafe_allow_html=True,
        )
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Events", total)
    col2.metric("Indexed", indexed)
    col3.metric("Unindexed", total - indexed)

    end_unix = int(datetime.now().timestamp())
    start_unix = int((datetime.now() - timedelta(days=90)).timestamp())
    events = storage.get_events_for_range(conn, start_unix, end_unix)

    if not events:
        st.markdown(
            '<div style="color: #6B6B82; font-size: 0.88rem; padding: 12px 0;">No recent events to chart.</div>',
            unsafe_allow_html=True,
        )
        return

    df = pd.DataFrame(events)
    df["date"] = df["timestamp"].str[:10]

    st.markdown('<hr style="margin: 24px 0 16px;"/>', unsafe_allow_html=True)
    st.markdown(
        '<h2 style="font-size:0.85rem; text-transform:uppercase; letter-spacing:0.08em; '
        'color:#8B8BA7; margin-bottom:12px;">Events per Day - Last 90 Days</h2>',
        unsafe_allow_html=True,
    )
    daily_counts = df.groupby("date").size().reset_index(name="count")
    daily_counts["date"] = pd.to_datetime(daily_counts["date"])
    chart = (
        alt.Chart(daily_counts)
        .mark_bar(
            color="#00C8E8",
            opacity=0.85,
            cornerRadiusTopLeft=3,
            cornerRadiusTopRight=3,
        )
        .encode(
            x=alt.X("date:T", title="Date", axis=alt.Axis(format="%b %d")),
            y=alt.Y("count:Q", title="Events"),
            tooltip=["date:T", "count:Q"],
            color=alt.condition(
                alt.datum.count > daily_counts["count"].mean() if len(daily_counts) > 0 else alt.value("#00C8E8"),
                alt.value("#00C8E8"),
                alt.value("#1E5A6A"),
            ),
        )
        .properties(height=180)
        .configure(**_CHART_CONFIG)
    )
    st.altair_chart(chart, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            '<h2 style="font-size:0.85rem; text-transform:uppercase; letter-spacing:0.08em; '
            'color:#8B8BA7; margin-bottom:12px;">Event Type Distribution</h2>',
            unsafe_allow_html=True,
        )
        type_counts = df.groupby("type").size().reset_index(name="count")
        pie = (
            alt.Chart(type_counts)
            .mark_arc(innerRadius=50, outerRadius=95, padAngle=0.03)
            .encode(
                theta=alt.Theta("count:Q"),
                color=alt.Color(
                    "type:N",
                    scale=alt.Scale(
                        domain=list(TYPE_COLORS.keys()),
                        range=list(TYPE_COLORS.values()),
                    ),
                    legend=alt.Legend(orient="bottom", columns=3),
                ),
                tooltip=["type:N", "count:Q"],
            )
            .properties(width=320, height=280)
            .configure(**_CHART_CONFIG)
        )
        st.altair_chart(pie)

    with col2:
        st.markdown(
            '<h2 style="font-size:0.85rem; text-transform:uppercase; letter-spacing:0.08em; '
            'color:#8B8BA7; margin-bottom:12px;">Top Tags</h2>',
            unsafe_allow_html=True,
        )
        from collections import Counter
        all_tags = []
        for tags_list in df["tags"]:
            if isinstance(tags_list, list):
                all_tags.extend(tags_list)
        tag_counts = Counter(all_tags).most_common(12)
        if tag_counts:
            tag_df = pd.DataFrame(tag_counts, columns=["tag", "count"])
            bar = (
                alt.Chart(tag_df)
                .mark_bar(color="#8B5CF6", opacity=0.85, cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
                .encode(
                    x=alt.X("count:Q", title="Count"),
                    y=alt.Y("tag:N", sort="-x", title=None),
                    tooltip=["tag:N", "count:Q"],
                )
                .properties(height=260)
                .configure(**_CHART_CONFIG)
            )
            st.altair_chart(bar, use_container_width=True)

    st.markdown('<hr style="margin: 8px 0 16px;"/>', unsafe_allow_html=True)
    st.markdown(
        '<h2 style="font-size:0.85rem; text-transform:uppercase; letter-spacing:0.08em; '
        'color:#8B8BA7; margin-bottom:12px;">Events by Source</h2>',
        unsafe_allow_html=True,
    )
    source_counts = df.groupby("source").size().reset_index(name="count")
    source_chart = (
        alt.Chart(source_counts)
        .mark_bar(color="#10B981", opacity=0.85, cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("source:N", title=None),
            y=alt.Y("count:Q", title="Events"),
            tooltip=["source:N", "count:Q"],
        )
        .properties(height=160)
        .configure(**_CHART_CONFIG)
    )
    st.altair_chart(source_chart, use_container_width=True)

    # Git streak
    git_events = df[df["source"] == "git"]
    if not git_events.empty:
        git_dates = set(git_events["date"].unique())
        streak = 0
        d = date.today()
        while str(d) in git_dates:
            streak += 1
            d -= timedelta(days=1)
        st.metric("Git Commit Streak", f"{streak} days")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    try:
        config = load_config()
    except FileNotFoundError:
        st.error("config.yaml not found. Run `streamlit run app.py` from the AetherMind directory.")
        return

    view = sidebar(config)

    if "Timeline" in view:
        view_timeline(config)
    elif "Ask" in view:
        view_ask(config)
    elif "Reflection" in view:
        view_reflections(config)
    elif "Stats" in view:
        view_stats(config)


if __name__ == "__main__":
    main()
