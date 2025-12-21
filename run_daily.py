import os
import math
import csv
import requests
import pytz
from datetime import datetime, timedelta, time as dtime

# =========================
# DEPLOY MARKER
# =========================
DEPLOY_MARKER = "V-C4-ALLMATCHES-THU-SUN-001"
print(f"DEPLOY MARKER: {DEPLOY_MARKER}")

# =========================
# TIMEZONES
# =========================
LONDON = pytz.timezone("Europe/London")
UTC = pytz.utc

# =========================
# CONFIG
# =========================
# Thu=3 Fri=4 Sat=5 Sun=6 (Mon=0)
RUN_DAYS = {3, 4, 5, 6}

# SportsDB key: default to 123 for free users
SPORTSDB_API_KEY = os.environ.get("SPORTSDB_API_KEY", "123")

# Leagues (stable IDs)
LEAGUES = [
    ("Premier League", 4328),
    ("EFL Championship", 4329),
]

# Elo model tuning
HOME_ADV_ELO = 55
DRAW_BASE = 0.24
DRAW_TIGHTNESS = 260.0

# Telegram limits
TELEGRAM_MAX_LEN = 3800  # keep under 4096


# =========================
# TELEGRAM
# =========================
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
    SportsDB returns dateEvent 'YYYY-MM-DD' and strTime sometimes 'HH:MM:SS' or None.
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
    p_away = remaining * (1.0 - p_home_raw)

    s = p_home + p_draw + p_away
    return (p_home / s, p_draw / s, p_away / s)


def pick_from_probs(p_home: float, p_draw: float, p_away: float) -> str:
    if p_home >= p_draw and p_home >= p_away:
        return "HOME"
    if p_away >= p_home and p_away >= p_draw:
        return "AWAY"
    return "DRAW"


def actual_outcome(home_goals: int, away_goals: int) -> str:
    if home_goals > away_goals:
        return "HOME"
    if away_goals > home_goals:
        return "AWAY"
    return "DRAW"


def find_team_rating(ratings: dict, team_name: str):
    if team_name in ratings:
        return ratings[team_name]
    low = norm(team_name)
    for k, v in (ratings or {}).items():
        if norm(k) == low:
            return v
    return None


# =========================
# SPORTSD B (PRIMARY)
# =========================
def sportsdb_get(url: str):
    r = requests.get(url, timeout=25)
    if r.status_code != 200:
        print(f"SportsDB HTTP {r.status_code}: {url}")
        return None
    return get_json_or_none(r)


def sportsdb_fetch_table_ratings(league_id: int):
    """
    Builds a basic Elo-ish rating from points-per-game + goal-diff-per-game.
    """
    url = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}/lookuptable.php?l={league_id}"
    data = sportsdb_get(url)
    table = (data or {}).get("table") or []
    if not table:
        return {}

    rows = []
    for t in table:
        team = t.get("strTeam") or ""
        played = safe_int(t.get("intPlayed"))
        points = safe_int(t.get("intPoints"))
        gd = safe_int(t.get("intGoalDifference"))
        if not team or played <= 0:
            continue
        rows.append((team, points / played, gd / played))

    if not rows:
        return {}

    avg_ppg = sum(x[1] for x in rows) / len(rows)
    avg_gdpg = sum(x[2] for x in rows) / len(rows)

    ratings = {}
    for team, ppg, gdpg in rows:
        elo = 1500 + (ppg - avg_ppg) * 420 + (gdpg - avg_gdpg) * 65
        elo = max(1200, min(1800, elo))
        ratings[team] = float(elo)

    return ratings


def sportsdb_fetch_events_for_day(date_yyyy_mm_dd: str):
    """
    CRITICAL FIX:
    Instead of eventsnextleague (limited), pull ALL soccer events by day,
    then filter by league id.
    """
    url = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}/eventsday.php?d={date_yyyy_mm_dd}&s=Soccer"
    data = sportsdb_get(url)
    return (data or {}).get("events") or []


# =========================
# CSV OUTPUT (NO PANDAS)
# =========================
def write_csv(rows: list, filepath: str):
    if not rows:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    fields = sorted({k for r in rows for k in r.keys()})
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


# =========================
# MAIN (C4)
# =========================
def main():
    now_london = datetime.now(LONDON)
    wd = now_london.weekday()

    header = [
        "MatchBot — C4 (All Weekend Matches) Thu–Sun",
        f"Run (London): {now_london.strftime('%Y-%m-%d %H:%M')}",
        f"Deploy: {DEPLOY_MARKER}",
        ""
    ]

    # Self-skip Mon–Wed (if Render cron is daily)
    if wd not in RUN_DAYS:
        msg = "\n".join(header + [
            "Today is outside Thu–Sun.",
            "Skipping run (expected if your Render cron is still daily)."
        ])
        send_telegram_chunks(msg)
        print("Skip: outside Thu–Sun")
        return

    # Upcoming window: now -> end of Sunday (London)
    end_sun = (now_london + timedelta(days=(6 - wd))).date()
    window_end_l
