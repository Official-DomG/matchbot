print("DEPLOY MARKER: V123-CHAMP-001")

import os
import time
import math
import requests
import pytz
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, List, Tuple

# -----------------------------
# Timezones
# -----------------------------
LONDON = pytz.timezone("Europe/London")
UTC = pytz.utc

# -----------------------------
# Leagues (TheSportsDB IDs)
# -----------------------------
LEAGUES: List[Tuple[str, int]] = [
    ("Premier League", 4328),
    ("EFL Championship", 4329),   # swapped from Champions League
]

# -----------------------------
# Model params
# -----------------------------
HOME_ADV_ELO = 55
DRAW_BASE = 0.24
DRAW_TIGHTNESS = 260.0

EDGE_THRESHOLD = 0.05
MIN_PROB_TO_BET = 0.40

# -----------------------------
# Odds API sport keys (if you use The Odds API)
# -----------------------------
ODDS_SPORT_KEYS = {
    "Premier League": "soccer_epl",
    "EFL Championship": "soccer_efl_championship",
}

# -----------------------------
# Environment variables
# -----------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SPORTSDB_API_KEY = os.getenv("SPORTSDB_API_KEY", "1").strip()  # "1" is TheSportsDB test key (very limited)
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "").strip()

# -----------------------------
# HTTP helpers (anti-crash)
# -----------------------------
def send_telegram_message(text: str) -> None:
    """
    Sends a Telegram message if env vars are present.
    Safe: never raises.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[Telegram] Skipped (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID)")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        requests.post(url, json=payload, timeout=15)
    except Exception as e:
        print("[Telegram] Send failed:", repr(e))


def safe_json(r: requests.Response, label: str) -> Optional[Dict[str, Any]]:
    """
    Robust JSON parsing:
    - Logs status and response preview
    - Returns None instead of raising
    """
    try:
        return r.json()
    except Exception:
        preview = (r.text or "")[:350].replace("\n", " ")
        print(f"[{label}] Non-JSON response")
        print("Status:", r.status_code)
        print("Preview:", preview)
        return None


def get_with_retry(url: str, label: str, params: Optional[Dict[str, Any]] = None, tries: int = 3) -> Optional[Dict[str, Any]]:
    """
    GET with retries + JSON safety.
    Returns parsed JSON dict or None.
    """
    last_r = None
    for i in range(tries):
        try:
            last_r = requests.get(url, params=params, timeout=20)
            # Basic sanity gates BEFORE json parsing
            if last_r.status_code != 200:
                print(f"[{label}] Bad status: {last_r.status_code} | URL: {last_r.url}")
            if not last_r.text or not last_r.text.strip():
                print(f"[{label}] Empty body | URL: {last_r.url}")
            # Attempt JSON parse regardless; safe_json handles failures
            data = safe_json(last_r, label)
            if data is not None:
                return data
        except Exception as e:
            print(f"[{label}] Request error:", repr(e))
        time.sleep(2 * (i + 1))
    return None


# -----------------------------
# Core model bits
# -----------------------------
def elo_to_probs(elo_home: float, elo_away: float) -> Tuple[float, float, float]:
    """
    Convert Elo difference to W/D/L probabilities with a draw model.
    """
    diff = (elo_home + HOME_ADV_ELO) - elo_away

    # Win probability ignoring draws
    p_home_nodraw = 1.0 / (1.0 + 10 ** (-diff / 400.0))
    p_away_nodraw = 1.0 - p_home_nodraw

    # Draw probability increases when teams are close
    p_draw = DRAW_BASE * math.exp(-(abs(diff) / DRAW_TIGHTNESS))
    p_draw = max(0.05, min(0.35, p_draw))  # clamp

    # Re-scale win probs to fit remaining mass
    rem = 1.0 - p_draw
    p_home = p_home_nodraw * rem
    p_away = p_away_nodraw * rem

    # Normalize (numerical safety)
    s = p_home + p_draw + p_away
    p_home, p_draw, p_away = p_home / s, p_draw / s, p_away / s
    return p_home, p_draw, p_away


# -----------------------------
# TheSportsDB: fixtures
# -----------------------------
def sportsdb_next_events_by_league(league_id: int) -> List[Dict[str, Any]]:
    """
    TheSportsDB endpoint: eventsnextleague.php?id=<league_id>
    """
    url = "https://www.thesportsdb.com/api/v1/json/{}/eventsnextleague.php".format(SPORTSDB_API_KEY)
    data = get_with_retry(url, "TheSportsDB", params={"id": league_id})
    if not data:
        return []
    events = data.get("events") or []
    if not isinstance(events, list):
        return []
    return events


def parse_event_time_utc(event: Dict[str, Any]) -> Optional[datetime]:
    """
    TheSportsDB typically provides dateEvent + strTime (often UTC).
    We treat missing time as 00:00.
    """
    date_str = (event.get("dateEvent") or "").strip()  # e.g. "2025-12-20"
    time_str = (event.get("strTime") or "").strip()    # e.g. "15:00:00"
    if not date_str:
        return None
    if not time_str:
        time_str = "00:00:00"
    try:
        dt = datetime.fromisoformat(f"{date_str}T{time_str}")
        # Treat as UTC if no tz info
        if dt.tzinfo is None:
            dt = UTC.localize(dt)
        else:
            dt = dt.astimezone(UTC)
        return dt
    except Exception:
        return None


# -----------------------------
# Placeholder Elo source
# -----------------------------
def get_team_elo(team_name: str, league_name: str) -> float:
    """
    Replace this with your Elo table / file / API.
    For now, deterministic fallback to avoid crashes.
    """
    # Simple stable hash -> range
    h = abs(hash((team_name.lower().strip(), league_name))) % 400
    return 1400.0 + (h - 200)  # approx 1200-1600


# -----------------------------
# Selection logic
# -----------------------------
def top_soonest_fixtures(limit: int = 5) -> List[Dict[str, Any]]:
    now_utc = datetime.now(UTC)

    all_events: List[Dict[str, Any]] = []
    for league_name, league_id in LEAGUES:
        events = sportsdb_next_events_by_league(league_id)
        for e in events:
            e["_league_name"] = league_name
            dt_utc = parse_event_time_utc(e)
            if not dt_utc:
                continue
            if dt_utc < now_utc - timedelta(hours=1):
                continue
            e["_dt_utc"] = dt_utc
            all_events.append(e)

    all_events.sort(key=lambda x: x["_dt_utc"])
    return all_events[:limit]


def format_probs_line(event: Dict[str, Any]) -> str:
    league = event.get("_league_name", "Unknown League")
    home = (event.get("strHomeTeam") or "Home").strip()
    away = (event.get("strAwayTeam") or "Away").strip()

    dt_london = event["_dt_utc"].astimezone(LONDON)
    dt_str = dt_london.strftime("%Y-%m-%d %H:%M")

    # Elo -> probs
    elo_home = get_team_elo(home, league)
    elo_away = get_team_elo(away, league)
    pH, pD, pA = elo_to_probs(elo_home, elo_away)

    return f"{dt_str} {league} — {home} vs {away} | H:{pH:.0%} D:{pD:.0%} A:{pA:.0%}"


# -----------------------------
# Main run
# -----------------------------
def run_bot_once() -> None:
    run_time = datetime.now(LONDON).strftime("%Y-%m-%d %H:%M")
    lines = [
        f"MatchBot — Probabilities (Mode A3)",
        f"Run (London): {run_time}",
        "",
        "Top 5 soonest fixtures:",
    ]

    fixtures = top_soonest_fixtures(limit=5)
    if not fixtures:
        lines.append("No fixtures returned (API may be down).")
        msg = "\n".join(lines)
        print(msg)
        send_telegram_message(msg)
        return

    for e in fixtures:
        lines.append(format_probs_line(e))

    msg = "\n".join(lines)
    print(msg)
    send_telegram_message(msg)


if __name__ == "__main__":
    try:
        run_bot_once()
    except Exception as e:
        # Last-resort guard so the bot never dies silently
        err = f"MatchBot crashed ❌\n{type(e).__name__}: {e}"
        print(err)
        send_telegram_message(err)
        raise
