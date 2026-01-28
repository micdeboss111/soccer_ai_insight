# ============================================================
# FOOTBALL-DATA.ORG — HISTORICAL INGESTION ENGINE (ALL-IN-ONE)
# Features:
#  - Auto competition discovery (via your API key)
#  - Incremental ingestion (only missing seasons)
#  - Auto-merge (never duplicates)
#  - Refresh last 7 days
#  - Local CSV cache (fd_history.csv)
#
# REQUIREMENTS:
#   export FOOTBALL_DATA_TOKEN=your_api_key
# ============================================================

import os
import time
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

BASE_URL = "https://api.football-data.org/v4"

# -------------------------
# AUTH
# -------------------------
def _get_fd_token() -> str:
    token = os.getenv("FOOTBALL_DATA_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing FOOTBALL_DATA_TOKEN environment variable.")
    return token

# -------------------------
# COMPETITIONS
# -------------------------
@st.cache_data(show_spinner=False, ttl=60 * 15)
def fetch_competitions_df() -> pd.DataFrame:
    headers = {"X-Auth-Token": _get_fd_token()}
    r = requests.get(f"{BASE_URL}/competitions", headers=headers, timeout=30)
    r.raise_for_status()
    comps = r.json().get("competitions", [])
    rows = []
    for c in comps:
        rows.append({
            "code": c.get("code"),
            "name": c.get("name"),
            "area": (c.get("area") or {}).get("name"),
            "type": c.get("type"),
        })
    return (
        pd.DataFrame(rows)
        .dropna(subset=["code", "name"])
        .sort_values(["area", "name"])
        .reset_index(drop=True)
    )

# -------------------------
# RAW FETCHERS
# -------------------------
def fetch_competition_matches(code: str, season: int) -> Dict[str, Any]:
    headers = {"X-Auth-Token": _get_fd_token()}
    params = {"season": int(season)}
    r = requests.get(
        f"{BASE_URL}/competitions/{code}/matches",
        headers=headers,
        params=params,
        timeout=30
    )
    r.raise_for_status()
    return r.json()

def fetch_matches_by_date(
    date_from: str,
    date_to: str,
    competitions: Optional[str] = None
) -> Dict[str, Any]:
    headers = {"X-Auth-Token": _get_fd_token()}
    params = {"dateFrom": date_from, "dateTo": date_to, "status": "FINISHED"}
    if competitions:
        params["competitions"] = competitions
    r = requests.get(
        f"{BASE_URL}/matches",
        headers=headers,
        params=params,
        timeout=30
    )
    r.raise_for_status()
    return r.json()

# -------------------------
# NORMALIZATION
# -------------------------
def _normalize_payload(payload: Dict[str, Any], season_hint: Optional[int] = None) -> pd.DataFrame:
    rows = []
    for m in payload.get("matches", []):
        if m.get("status") != "FINISHED":
            continue
        ft = (m.get("score") or {}).get("fullTime") or {}
        if ft.get("home") is None or ft.get("away") is None:
            continue

        dt = pd.to_datetime(m.get("utcDate"), utc=True, errors="coerce")
        if pd.isna(dt):
            continue
        dt = dt.tz_convert(None)

        rows.append({
            "Date": dt,
            "HomeTeam": (m.get("homeTeam") or {}).get("name"),
            "AwayTeam": (m.get("awayTeam") or {}).get("name"),
            "FTHG": int(ft["home"]),
            "FTAG": int(ft["away"]),
            "Competition": (m.get("competition") or {}).get("name"),
            "CompCode": (m.get("competition") or {}).get("code"),
            "Season": season_hint if season_hint is not None else dt.year,
        })

    return pd.DataFrame(rows)

def _standardize(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Date","HomeTeam","AwayTeam","FTHG","FTAG","Competition","CompCode","Season"])

    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out = out.dropna(subset=["Date","HomeTeam","AwayTeam","FTHG","FTAG"])
    out["FTHG"] = out["FTHG"].astype(int)
    out["FTAG"] = out["FTAG"].astype(int)
    out["HomeTeam"] = out["HomeTeam"].astype(str).str.strip()
    out["AwayTeam"] = out["AwayTeam"].astype(str).str.strip()
    out["CompCode"] = out["CompCode"].astype(str).str.strip()
    out["Competition"] = out["Competition"].astype(str).str.strip()
    out["Season"] = pd.to_numeric(out["Season"], errors="coerce").fillna(-1).astype(int)
    return out.sort_values("Date").reset_index(drop=True)

# -------------------------
# MERGE / DEDUP
# -------------------------
def merge_dedup(existing: Optional[pd.DataFrame], incoming: Optional[pd.DataFrame]) -> pd.DataFrame:
    existing = _standardize(existing)
    incoming = _standardize(incoming)
    combined = pd.concat([existing, incoming], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["CompCode","Season","Date","HomeTeam","AwayTeam","FTHG","FTAG"],
        keep="last"
    )
    return combined.sort_values("Date").reset_index(drop=True)

def existing_comp_seasons(df: Optional[pd.DataFrame]) -> set:
    df = _standardize(df)
    return set(zip(df["CompCode"], df["Season"]))

# -------------------------
# INCREMENTAL INGEST
# -------------------------
def ingest_missing_seasons(
    df_hist: Optional[pd.DataFrame],
    codes: List[str],
    seasons: List[int],
    sleep_s: float
) -> pd.DataFrame:
    have = existing_comp_seasons(df_hist)
    parts = []
    for code in codes:
        for season in seasons:
            if (code, season) in have:
                continue
            payload = fetch_competition_matches(code, season)
            part = _normalize_payload(payload, season_hint=season)
            if not part.empty:
                parts.append(part)
            time.sleep(sleep_s)
    incoming = pd.concat(parts, ignore_index=True) if parts else None
    return merge_dedup(df_hist, incoming)

def refresh_last_7_days(df_hist: Optional[pd.DataFrame], codes: List[str]) -> pd.DataFrame:
    date_to = datetime.utcnow().date()
    date_from = date_to - timedelta(days=7)
    payload = fetch_matches_by_date(
        str(date_from),
        str(date_to),
        competitions=",".join(codes)
    )
    incoming = _normalize_payload(payload)
    return merge_dedup(df_hist, incoming)

# -------------------------
# CACHE
# -------------------------
def load_cache(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    return _standardize(pd.read_csv(path))

def save_cache(df: pd.DataFrame, path: str):
    _standardize(df).to_csv(path, index=False)

# ============================================================
# STREAMLIT UI BLOCK
# ============================================================

st.subheader("🗂️ Historical Data Builder (football-data.org)")

cache_path = st.text_input("Local cache file", value="fd_history.csv")

# Load competitions
try:
    comps = fetch_competitions_df()
    st.dataframe(comps, use_container_width=True)
    available_codes = comps["code"].tolist()
except Exception as e:
    st.error(f"Failed to load competitions: {e}")
    available_codes = []

default_codes = [c for c in ["PL","PD","BL1","SA","FL1","CL","BSA","MLS"] if c in available_codes]

selected_codes = st.multiselect(
    "Select competitions",
    options=available_codes,
    default=default_codes
)

current_year = datetime.utcnow().year
seasons = st.multiselect(
    "Select seasons (start year)",
    options=list(range(current_year-10, current_year+1)),
    default=[current_year-3, current_year-2, current_year-1]
)

sleep_s = st.slider("API delay (seconds)", 3.0, 10.0, 6.5, 0.5)

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📥 Load Cache"):
        df = load_cache(cache_path)
        if df is not None:
            st.session_state["fd_hist"] = df
            st.success(f"Loaded {len(df):,} matches")

with col2:
    if st.button("➕ Ingest Missing Seasons"):
        df = st.session_state.get("fd_hist", load_cache(cache_path))
        df = ingest_missing_seasons(df, selected_codes, seasons, sleep_s)
        st.session_state["fd_hist"] = df
        save_cache(df, cache_path)
        st.success(f"Dataset now {len(df):,} matches")

with col3:
    if st.button("🔄 Refresh Last 7 Days"):
        df = st.session_state.get("fd_hist", load_cache(cache_path))
        df = refresh_last_7_days(df, selected_codes)
        st.session_state["fd_hist"] = df
        save_cache(df, cache_path)
        st.success(f"Updated dataset: {len(df):,} matches")

if "fd_hist" in st.session_state:
    df = st.session_state["fd_hist"]
    st.caption(f"Data ready: {df['Date'].min().date()} → {df['Date'].max().date()}")
    st.dataframe(df.tail(100), use_container_width=True)

    if st.checkbox("Use this dataset for training", value=True):
        df = df[["Date","HomeTeam","AwayTeam","FTHG","FTAG"]].copy()
        st.info("Models will now train on ingested historical data.")
