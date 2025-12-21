import os
import math
import csv
import requests
import pytz
from datetime import datetime, timedelta, time as dtime

# =========================
# DEPLOY MARKER
# =========================
DEPLOY_MARKER = "V-C3-EVAL-THU-SUN-001"
print(f"DEPLOY MARKER: {DEPLOY_MARKER}")

# =========================
# TIMEZONES
# =========================
LONDON = pytz.timezone("Europe/London")
UTC = pytz.utc

# =========================
# CONFIG
# =========================
# Run logic: only do "work" Thu–Sun. If Render cron runs daily, the script will self-skip Mon–Wed.
RUN_DAYS = {3, 4, 5, 6}  # Thu=3 Fri=4 Sat=5 Sun=6 (Python weekday: Mon=0)

# Elo model tuning
HOME_ADV_ELO = 55
DRAW_BASE = 0.24
DRAW_TIGHTNESS = 260.0

# Provider: SportsDB is primary. API-Football optional (requires key + later mapping if you want deeper usage).
USE_API_FOOTBALL_IF_AVAILABLE = False

# SportsDB API key:
# - Free user shown as "123" in your screenshot
SPORTSDB_API_KEY = os.environ.get("SPORTSDB_API_KEY", "123")

# API-Football key (optional)
API_FOOTBALL_KEY = os.environ.get("API_FOOTBALL_KEY", "")

# Leagues to track (auto-resolve in SportsDB; fallback hardcoded IDs)
LEAGUE_NAMES = ["Premier League", "EFL Championship"]
SPORTSDB_LEAGUE_ID_FALLBACK = {
    "Premier League": 4328,
    "EFL Championship": 4329,  # English League Championship
}

# =========================
# TELEGRAM
# =========================
TELEGRAM_MAX_LEN = 3800  # keep comfortably under Telegram 4096

def send_telegram_message(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram not configured (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID)")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=20)
    print("Telegram response:", r.status_code, (r.text or "")[:300])

def send_telegram_chunks(big_text: str) -> None:
    big_text = (big_text or "").strip()
    if not big_text:
        return

    chunks = []
    while len(big_text) > TELEGRAM_MAX_LEN:
        cut = big_text.rfind("\n", 0, TELEGRAM_MAX_LEN)
        if cut == -1:
            cut = TELEGRAM_MAX_LEN
        chunks.append(big_text[:cut].strip())
        big_text = big_text[cut:].strip()
    chunks.append(big_text)

    for i, ch in enumerate(chunks, start=1):
        send_telegram_message(f"{ch}\n\n(Part {i}/{len(chunks)})")

# =========================
# HELPERS
# =========================
def safe_int(x):
    try:
        if x is None or x == "":
            return 0
        return int(float(x))
    except Exception:
        return 0

def get_json_or_none(r: requests.Response):
    try:
        return r.json()
    except Exception:
        preview = (r.text or "")[:200].replace("\n", " ")
        print(f"Non-JSON response ({r.status_code}): {preview}")
        return None

def norm(s: str) -> str:
    return (s or "").strip().lower()

def parse_event_dt_utc(date_str: str, time_str: str):
    """
    SportsDB returns dateEvent 'YYYY-MM-DD' and strTime sometimes 'HH:MM:SS' or 'HH:MM' or None.
    """
    if not date_str:
        return None
    if not time_str:
        time_str = "00:00:00"
    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        except Exception:
            return None
    return UTC.localize(dt)

# =========================
# ELO MODEL
# =========================
def win_prob_from_elo(elo_a: float, elo_b: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((elo_b - elo_a) / 400.0))

def probs_1x2(elo_home: float, elo_away: float):
    p_home_raw = win_prob_from_elo(elo_home + HOME_ADV_ELO, elo_away)
    diff = abs((elo_home + HOME_ADV_ELO) - elo_away)
    p_draw = DRAW_BASE * math.exp(-diff / DRAW_TIGHTNESS)
    p_draw = max(0.08, min(0.35, p_draw))
    remaining = 1.0 - p_draw
    p_home = remaining * p_home_raw
    p_
