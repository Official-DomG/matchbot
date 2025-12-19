import os
import math
import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta

LONDON = pytz.timezone("Europe/London")
UTC = pytz.utc

LEAGUES = [
    ("Premier League", 4328),
    ("Champions League", 4480),
]

HOME_ADV_ELO = 55
DRAW_BASE = 0.24
DRAW_TIGHTNESS = 260.0

EDGE_THRESHOLD = 0.05
MIN_PROB_TO_BET = 0.40

ODDS_SPORT_KEYS = {
    "Premier League": "soccer_epl",
    "Champions League": "soccer_uefa_champs_league",
}

def send_telegram_message(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram not configured (missing env vars)")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=20)
    print("Telegram response:", r.status_code, r.text)

def safe_int(x):
    try:
        if x is None or x == "":
            return 0
        return int(float(x))
    except Exception:
        return 0

def fetch_next_events(league_id: int):
    url = f"https://www.thesportsdb.com/api/v1/json/1/eventsnextleague.php?id={league_id}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            print(f"Events API returned {r.status_code} for league {league_id}")
            return []
        return (r.json() or {}).get("events") or []
    except Exception as e:
        print(f"Failed to fetch events for league {league_id}: {e}")
        return []

def fetch_table_ratings(league_id: int):
    url = f"https://www.thesportsdb.com/api/v1/json/1/lookuptable.php?l={league_id}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    table = (r.json() or {}).get("table") or []
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

def parse_event_dt_utc(date_str: str, time_str: str):
    if not date_str:
        return None
    if not time_str:
        time_str = "00:00:00"
    dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
    return UTC.localize(dt)

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

def find_team_rating(ratings: dict, team_name: str):
    if team_name in ratings:
        return ratings[team_name]
    low = team_name.lower().strip()
    for k, v in ratings.items():
        if k.lower().strip() == low:
            return v
    return None

def main():
    now_london = datetime.now(LONDON)
    now_utc = now_london.astimezone(UTC)
    cutoff_utc = now_utc + timedelta(hours=72)

    print("MatchBot starting:", now_london)

    try:
        all_rows = []

        for comp_name, league_id in LEAGUES:
            events = fetch_next_events(league_id)
            ratings = fetch_table_ratings(league_id)

            for ev in events:
                home = ev.get("strHomeTeam") or ""
                away = ev.get("strAwayTeam") or ""
                dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
                if dt_utc is None or not (now_utc <= dt_utc <= cutoff_utc):
                    continue

                dt_london = dt_utc.astimezone(LONDON)

                elo_h = find_team_rating(ratings, home) or 1500.0
                elo_a = find_team_rating(ratings, away) or 1500.0

                p_home, p_draw, p_away = probs_1x2(elo_h, elo_a)

                all_rows.append({
                    "competition": comp_name,
                    "kickoff_london": dt_london.strftime("%Y-%m-%d %H:%M"),
                    "home_team": home,
                    "away_team": away,
                    "p_home": round(p_home, 4),
                    "p_draw": round(p_draw, 4),
                    "p_away": round(p_away, 4),
                })

        # Always send a useful summary even if empty
        msg_lines = [
            "MatchBot — Probabilities (Mode A3)",
            f"Run (London): {now_london.strftime('%Y-%m-%d %H:%M')}",
            ""
        ]

        if not all_rows:
            msg_lines.append("No fixtures found in the next 72 hours for selected leagues.")
        else:
            all_rows.sort(key=lambda x: x["kickoff_london"])
            msg_lines.append("Top 5 soonest fixtures:")
            for r in all_rows[:5]:
                msg_lines.append(
                    f"{r['kickoff_london']} {r['competition']} — {r['home_team']} vs {r['away_team']} | "
                    f"H:{int(r['p_home']*100)}% D:{int(r['p_draw']*100)}% A:{int(r['p_away']*100)}%"
                )

        send_telegram_message("\n".join(msg_lines))

        # Optional: write CSV
        if all_rows:
            out_dir = "/tmp/matchbot_reports"
            os.makedirs(out_dir, exist_ok=True)
            date_str = now_london.strftime("%Y-%m-%d")
            filepath = os.path.join(out_dir, f"daily_report_{date_str}.csv")
            pd.DataFrame(all_rows).to_csv(filepath, index=False)
            print(f"Daily report written to {filepath}")

    except Exception as e:
        # If anything crashes, you still get the error in Telegram
        err_text = f"MatchBot crashed ❌\n{type(e).__name__}: {e}"
        print(err_text)
        send_telegram_message(err_text)
        raise

if __name__ == "__main__":
    main()
