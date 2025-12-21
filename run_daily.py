import os
import math
import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta, date

# ----------------------------
# Config
# ----------------------------
DEPLOY_MARKER = "V-C-RESULTS-THU-SUN-001"

LONDON = pytz.timezone("Europe/London")
UTC = pytz.utc

# SportsDB League IDs (stable)
LEAGUES = [
    ("Premier League", 4328),
    ("EFL Championship", 4329),
]

# Prob model tuning
HOME_ADV_ELO = 55
DRAW_BASE = 0.24
DRAW_TIGHTNESS = 260.0

# ----------------------------
# Helpers
# ----------------------------
def env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()

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

def send_telegram_message(text: str) -> None:
    token = env("TELEGRAM_BOT_TOKEN")
    chat_id = env("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram not configured (missing TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=20)
    print("Telegram response:", r.status_code, r.text)

def sportsdb_base() -> str:
    key = env("SPORTSDB_API_KEY", "1")
    return f"https://www.thesportsdb.com/api/v1/json/{key}"

def sportsdb_get(path: str, params: dict):
    url = f"{sportsdb_base()}/{path}"
    r = requests.get(url, params=params, timeout=25)
    if r.status_code != 200:
        print(f"SportsDB failed {r.status_code}: {url} params={params}")
        return None
    return get_json_or_none(r)

def fetch_events_by_day(league_id: int, day: date):
    # Events for a given league on a given date
    data = sportsdb_get("eventsday.php", {"d": day.strftime("%Y-%m-%d"), "l": str(league_id)})
    if not data:
        return []
    return (data.get("events") or [])

def fetch_table_ratings(league_id: int) -> dict:
    # League table -> derived Elo rating
    data = sportsdb_get("lookuptable.php", {"l": str(league_id)})
    if not data:
        return {}
    table = data.get("table") or []
    if not table:
        return {}

    rows = []
    for t in table:
        team = (t.get("strTeam") or "").strip()
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

def parse_event_dt_london(ev: dict):
    # SportsDB: dateEvent "YYYY-MM-DD", strTime usually "HH:MM:SS" (UTC-ish in many feeds).
    # We treat it as UTC if time exists, then convert to London.
    date_str = ev.get("dateEvent")
    time_str = ev.get("strTime") or "00:00:00"
    if not date_str:
        return None
    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
        dt_utc = UTC.localize(dt)
        return dt_utc.astimezone(LONDON)
    except Exception:
        return None

def event_is_played(ev: dict) -> bool:
    # If scores exist, treat as played
    hs = ev.get("intHomeScore")
    aws = ev.get("intAwayScore")
    return (hs is not None and hs != "") and (aws is not None and aws != "")

def fmt_score(ev: dict) -> str:
    hs = safe_int(ev.get("intHomeScore"))
    aws = safe_int(ev.get("intAwayScore"))
    return f"{hs}-{aws}"

def end_of_sunday_london(now_london: datetime) -> datetime:
    # Sunday = 6 when Monday=0
    days_until_sun = (6 - now_london.weekday()) % 7
    sun = (now_london + timedelta(days=days_until_sun)).date()
    # 23:59:59 London
    return LONDON.localize(datetime(sun.year, sun.month, sun.day, 23, 59, 59))

# ----------------------------
# Main
# ----------------------------
def main():
    now_london = datetime.now(LONDON)
    print(f"DEPLOY MARKER: {DEPLOY_MARKER}")
    print("MatchBot starting (London):", now_london)

    # Safety guard: only do work Thu-Sun (Thu=3, Fri=4, Sat=5, Sun=6)
    if now_london.weekday() not in (3, 4, 5, 6):
        print("Not a Thu–Sun run day. Exiting cleanly.")
        return

    window_end = end_of_sunday_london(now_london)

    # We build:
    # - upcoming fixtures: now -> Sunday end
    # - results: yesterday + today
    today = now_london.date()
    yesterday = today - timedelta(days=1)

    upcoming_rows = []
    results_rows = []

    for comp_name, league_id in LEAGUES:
        ratings = fetch_table_ratings(league_id)

        # Pull events day-by-day across the window (keeps it simple + reliable)
        cursor = today
        while cursor <= window_end.date():
            events = fetch_events_by_day(league_id, cursor)

            for ev in events:
                home = (ev.get("strHomeTeam") or "").strip()
                away = (ev.get("strAwayTeam") or "").strip()
                if not home or not away:
                    continue

                dt_london = parse_event_dt_london(ev)
                if not dt_london:
                    # fallback: date-only
                    dt_london = LONDON.localize(datetime(cursor.year, cursor.month, cursor.day, 12, 0, 0))

                # RESULTS bucket (yesterday + today, only if played)
                if cursor in (yesterday, today) and event_is_played(ev):
                    results_rows.append({
                        "competition": comp_name,
                        "kickoff_london": dt_london.strftime("%Y-%m-%d %H:%M"),
                        "home_team": home,
                        "away_team": away,
                        "score": fmt_score(ev),
                    })

                # UPCOMING bucket (now -> end of Sunday, only if not played)
                if (now_london <= dt_london <= window_end) and (not event_is_played(ev)):
                    elo_h = ratings.get(home, 1500.0)
                    elo_a = ratings.get(away, 1500.0)
                    p_home, p_draw, p_away = probs_1x2(elo_h, elo_a)

                    upcoming_rows.append({
                        "competition": comp_name,
                        "kickoff_london": dt_london.strftime("%Y-%m-%d %H:%M"),
                        "home_team": home,
                        "away_team": away,
                        "p_home": round(p_home, 4),
                        "p_draw": round(p_draw, 4),
                        "p_away": round(p_away, 4),
                    })

            cursor += timedelta(days=1)

    # Sort outputs
    upcoming_rows.sort(key=lambda x: x["kickoff_london"])
    results_rows.sort(key=lambda x: x["kickoff_london"], reverse=True)

    # Build Telegram message (single message per run)
    msg = []
    msg.append("MatchBot — Fixtures + Results (Thu–Sun)")
    msg.append(f"Run (London): {now_london.strftime('%Y-%m-%d %H:%M')}")
    msg.append(f"Window: now → {window_end.strftime('%a %Y-%m-%d %H:%M')} (London)")
    msg.append(f"Deploy: {DEPLOY_MARKER}")
    msg.append("")

    # Results section
    msg.append("RESULTS (yesterday + today):")
    if results_rows:
        # show up to 12 results
        for r in results_rows[:12]:
            msg.append(f"- {r['kickoff_london']} {r['competition']} — {r['home_team']} {r['score']} {r['away_team']}")
    else:
        msg.append("- No completed matches found (yet).")
    msg.append("")

    # Upcoming section (with probabilities)
    msg.append("UPCOMING (now → Sunday):")
    if upcoming_rows:
        # show up to 12 upcoming
        for r in upcoming_rows[:12]:
            msg.append(
                f"- {r['kickoff_london']} {r['competition']} — {r['home_team']} vs {r['away_team']} | "
                f"H:{int(r['p_home']*100)}% D:{int(r['p_draw']*100)}% A:{int(r['p_away']*100)}%"
            )
    else:
        msg.append("- No upcoming fixtures found in the window.")
    msg.append("")

    send_telegram_message("\n".join(msg).strip())

    # Optional: write CSV report for debugging
    try:
        out_dir = "/tmp/matchbot_reports"
        os.makedirs(out_dir, exist_ok=True)
        stamp = now_london.strftime("%Y-%m-%d")
        if upcoming_rows:
            pd.DataFrame(upcoming_rows).to_csv(os.path.join(out_dir, f"upcoming_{stamp}.csv"), index=False)
        if results_rows:
            pd.DataFrame(results_rows).to_csv(os.path.join(out_dir, f"results_{stamp}.csv"), index=False)
        print("CSV reports written to", out_dir)
    except Exception as e:
        print("CSV write skipped:", type(e).__name__, str(e))

if __name__ == "__main__":
    try:
        main()
        print("MatchBot finished successfully")
    except Exception as e:
        err = f"MatchBot crashed ❌\n{type(e).__name__}: {e}"
        print(err)
        send_telegram_message(err)
        raise
