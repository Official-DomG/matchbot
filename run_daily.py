import os
import mathimport os
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

# Layer D tuning
HOME_ADV_ELO = 55
DRAW_BASE = 0.24
DRAW_TIGHTNESS = 260.0

# Layer E tuning (value rules)
EDGE_THRESHOLD = 0.05      # 5% edge minimum
MIN_PROB_TO_BET = 0.40     # avoid “value” on longshots for MVP

# Odds API sport keys
ODDS_SPORT_KEYS = {
    "Premier League": "soccer_epl",
    # Champions League key name can change; if this returns empty, we'll adjust in next step.
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
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return (r.json() or {}).get("events") or []

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

def norm_team(s: str) -> str:
    return (s or "").lower().replace("&", "and").replace(".", "").strip()

def fetch_odds_for_sport(sport_key: str):
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        print("Missing ODDS_API_KEY env var; skipping odds.")
        return []

    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "regions": "uk",
        "markets": "h2h",
        "oddsFormat": "decimal",
        "apiKey": api_key,
    }
    r = requests.get(url, params=params, timeout=25)
    if r.status_code != 200:
        print("Odds API failed:", r.status_code, r.text)
        return []
    return r.json() or []

def pick_best_bookmaker_h2h(bookmakers):
    # choose the bookmaker with the most complete h2h market; keep it simple
    for b in bookmakers or []:
        markets = b.get("markets") or []
        for m in markets:
            if m.get("key") == "h2h":
                return b, m.get("outcomes") or []
    return None, []

def odds_map_from_events(odds_events):
    """
    Returns mapping keyed by normalized 'home|away' with odds dict:
    {'home': x, 'draw': y, 'away': z, 'bookmaker': name}
    """
    out = {}
    for ev in odds_events or []:
        home = ev.get("home_team") or ""
        away = ev.get("away_team") or ""
        key = f"{norm_team(home)}|{norm_team(away)}"
        bm, outcomes = pick_best_bookmaker_h2h(ev.get("bookmakers"))
        if not outcomes:
            continue
        # outcomes are like [{"name": "Team A", "price": 2.1}, {"name":"Draw","price":3.4}, ...]
        odds = {"home": None, "draw": None, "away": None, "bookmaker": (bm or {}).get("title") if bm else ""}
        for o in outcomes:
            name = (o.get("name") or "").strip()
            price = o.get("price")
            if price is None:
                continue
            if name.lower() == "draw":
                odds["draw"] = float(price)
            elif norm_team(name) == norm_team(home):
                odds["home"] = float(price)
            elif norm_team(name) == norm_team(away):
                odds["away"] = float(price)
        if odds["home"] and odds["draw"] and odds["away"]:
            out[key] = odds
    return out

def implied_probs_from_odds(odds_home, odds_draw, odds_away):
    # includes overround; we normalize to sum to 1
    ih = 1.0 / odds_home
    idr = 1.0 / odds_draw
    ia = 1.0 / odds_away
    s = ih + idr + ia
    return ih / s, idr / s, ia / s

def main():
    now_london = datetime.now(LONDON)
    now_utc = now_london.astimezone(UTC)
    cutoff_utc = now_utc + timedelta(hours=72)

    print("MatchBot starting:", now_london)

    all_rows = []

    # Build rows: fixtures + probabilities + (optional) odds/value
    for comp_name, league_id in LEAGUES:
        ratings = fetch_table_ratings(league_id)
        sport_key = ODDS_SPORT_KEYS.get(comp_name)

        # Odds (optional)
        odds_map = {}
        if sport_key:
            odds_events = fetch_odds_for_sport(sport_key)
            odds_map = odds_map_from_events(odds_events)

        # Fixtures
        events = fetch_next_events(league_id)

        for ev in events:
            home = ev.get("strHomeTeam") or ""
            away = ev.get("strAwayTeam") or ""
            dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
            if dt_utc is None or not (now_utc <= dt_utc <= cutoff_utc):
                continue

            dt_london = dt_utc.astimezone(LONDON)

            elo_h = find_team_rating(ratings, home) if ratings else None
            elo_a = find_team_rating(ratings, away) if ratings else None
            if elo_h is None:
                elo_h = 1500.0
            if elo_a is None:
                elo_a = 1500.0

            p_home, p_draw, p_away = probs_1x2(elo_h, elo_a)

            # Attach odds if we can match
            key = f"{norm_team(home)}|{norm_team(away)}"
            odds = odds_map.get(key)

            value_flag = ""
            value_side = ""
            value_prob = ""
            value_edge = ""
            value_odds = ""
            bookmaker = ""

            if odds:
                bookmaker = odds.get("bookmaker", "")
                ih, idr, ia = implied_probs_from_odds(odds["home"], odds["draw"], odds["away"])
                edge_home = p_home - ih
                edge_draw = p_draw - idr
                edge_away = p_away - ia

                candidates = [
                    ("HOME", p_home, edge_home, odds["home"]),
                    ("DRAW", p_draw, edge_draw, odds["draw"]),
                    ("AWAY", p_away, edge_away, odds["away"]),
                ]
                candidates.sort(key=lambda x: x[2], reverse=True)
                best_side, best_prob, best_edge, best_odds = candidates[0]

                if best_edge >= EDGE_THRESHOLD and best_prob >= MIN_PROB_TO_BET:
                    value_flag = "VALUE"
                    value_side = best_side
                    value_prob = best_prob
                    value_edge = best_edge
                    value_odds = best_odds

            all_rows.append({
                "competition": comp_name,
                "kickoff_london": dt_london.strftime("%Y-%m-%d %H:%M"),
                "home_team": home,
                "away_team": away,
                "elo_home": round(elo_h, 1),
                "elo_away": round(elo_a, 1),
                "p_home": round(p_home, 4),
                "p_draw": round(p_draw, 4),
                "p_away": round(p_away, 4),
                "odds_home": odds["home"] if odds else "",
                "odds_draw": odds["draw"] if odds else "",
                "odds_away": odds["away"] if odds else "",
                "bookmaker": bookmaker,
                "value_flag": value_flag,
                "value_side": value_side,
                "value_prob": round(value_prob, 4) if value_prob != "" else "",
                "value_edge": round(value_edge, 4) if value_edge != "" else "",
                "value_odds": value_odds,
            })

    # Sort by kickoff
    all_rows.sort(key=lambda x: x["kickoff_london"])

    # Write CSV
    output_dir = "/tmp/matchbot_reports"
    os.makedirs(output_dir, exist_ok=True)
    date_str = now_london.strftime("%Y-%m-%d")
    filepath = os.path.join(output_dir, f"daily_report_{date_str}.csv")

    df = pd.DataFrame(all_rows)
    df.to_csv(filepath, index=False)
    print(f"Daily report written to {filepath}")

    # --- Telegram Summary (Mode B): VALUE picks + Top 5 fixtures probs ---
    msg_lines = [
        "MatchBot — Daily Summary (Mode B)",
        f"Run (London): {now_london.strftime('%Y-%m-%d %H:%M')}",
        ""
    ]

    value_all = [r for r in all_rows if str(r.get("value_flag", "")).upper() == "VALUE"]
    value_all.sort(key=lambda x: float(r.get("value_edge") or 0) if isinstance(r, dict) else 0, reverse=True)

    if value_all:
        msg_lines.append("Top VALUE picks (max 5):")
        for r in value_all[:5]:
            msg_lines.append(
                f"{r['kickoff_london']} {r['competition']} — {r['home_team']} vs {r['away_team']}\n"
                f"{r['value_side']} @ {r['value_odds']} | "
                f"P:{int(float(r['value_prob'])*100)}% | "
                f"Edge:+{int(float(r['value_edge'])*100)}% | "
                f"Book: {r.get('bookmaker','')}"
            )
    else:
        msg_lines.append("No VALUE picks today (thresholds held).")

    msg_lines.append("")
    msg_lines.append("Top 5 fixtures (probabilities):")

    soonest = all_rows[:5]
    if soonest:
        for r in soonest:
            msg_lines.append(
                f"{r['kickoff_london']} {r['competition']} — {r['home_team']} vs {r['away_team']} | "
                f"H:{int(float(r['p_home'])*100)}% "
                f"D:{int(float(r['p_draw'])*100)}% "
                f"A:{int(float(r['p_away'])*100)}%"
            )
    else:
        msg_lines.append("No fixtures found in the next 72 hours.")

    send_telegram_message("\n\n".join(msg_lines).strip())

if __name__ == "__main__":
    main()
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

# Layer D tuning
HOME_ADV_ELO = 55
DRAW_BASE = 0.24
DRAW_TIGHTNESS = 260.0

# Layer E tuning (value rules)
EDGE_THRESHOLD = 0.05      # 5% edge minimum
MIN_PROB_TO_BET = 0.40     # avoid “value” on longshots for MVP

# Odds API sport keys
ODDS_SPORT_KEYS = {
    "Premier League": "soccer_epl",
    # Champions League key name can change; if this returns empty, we'll adjust in next step.
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
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return (r.json() or {}).get("events") or []

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

def norm_team(s: str) -> str:
    return (s or "").lower().replace("&", "and").replace(".", "").strip()

def fetch_odds_for_sport(sport_key: str):
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        print("Missing ODDS_API_KEY env var; skipping odds.")
        return []

    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "regions": "uk",
        "markets": "h2h",
        "oddsFormat": "decimal",
        "apiKey": api_key,
    }
    r = requests.get(url, params=params, timeout=25)
    if r.status_code != 200:
        print("Odds API failed:", r.status_code, r.text)
        return []
    return r.json() or []

def pick_best_bookmaker_h2h(bookmakers):
    # choose the bookmaker with the most complete h2h market; keep it simple
    for b in bookmakers or []:
        markets = b.get("markets") or []
        for m in markets:
            if m.get("key") == "h2h":
                return b, m.get("outcomes") or []
    return None, []

def odds_map_from_events(odds_events):
    """
    Returns mapping keyed by normalized 'home|away' with odds dict:
    {'home': x, 'draw': y, 'away': z, 'bookmaker': name}
    """
    out = {}
    for ev in odds_events or []:
        home = ev.get("home_team") or ""
        away = ev.get("away_team") or ""
        key = f"{norm_team(home)}|{norm_team(away)}"
        bm, outcomes = pick_best_bookmaker_h2h(ev.get("bookmakers"))
        if not outcomes:
            continue
        # outcomes are like [{"name": "Team A", "price": 2.1}, {"name":"Draw","price":3.4}, ...]
        odds = {"home": None, "draw": None, "away": None, "bookmaker": (bm or {}).get("title") if bm else ""}
        for o in outcomes:
            name = (o.get("name") or "").strip()
            price = o.get("price")
            if price is None:
                continue
            if name.lower() == "draw":
                odds["draw"] = float(price)
            elif norm_team(name) == norm_team(home):
                odds["home"] = float(price)
            elif norm_team(name) == norm_team(away):
                odds["away"] = float(price)
        if odds["home"] and odds["draw"] and odds["away"]:
            out[key] = odds
    return out

def implied_probs_from_odds(odds_home, odds_draw, odds_away):
    # includes overround; we normalize to sum to 1
    ih = 1.0 / odds_home
    idr = 1.0 / odds_draw
    ia = 1.0 / odds_away
    s = ih + idr + ia
    return ih / s, idr / s, ia / s

def main():
    now_london = datetime.now(LONDON)
    now_utc = now_london.astimezone(UTC)
    cutoff_utc = now_utc + timedelta(hours=72)

    print("MatchBot starting:", now_london)

    all_rows = []

    # Build rows: fixtures + probabilities + (optional) odds/value
    for comp_name, league_id in LEAGUES:
        ratings = fetch_table_ratings(league_id)
        sport_key = ODDS_SPORT_KEYS.get(comp_name)

        # Odds (optional)
        odds_map = {}
        if sport_key:
            odds_events = fetch_odds_for_sport(sport_key)
            odds_map = odds_map_from_events(odds_events)

        # Fixtures
        events = fetch_next_events(league_id)

        for ev in events:
            home = ev.get("strHomeTeam") or ""
            away = ev.get("strAwayTeam") or ""
            dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
            if dt_utc is None or not (now_utc <= dt_utc <= cutoff_utc):
                continue

            dt_london = dt_utc.astimezone(LONDON)

            elo_h = find_team_rating(ratings, home) if ratings else None
            elo_a = find_team_rating(ratings, away) if ratings else None
            if elo_h is None:
                elo_h = 1500.0
            if elo_a is None:
                elo_a = 1500.0

            p_home, p_draw, p_away = probs_1x2(elo_h, elo_a)

            # Attach odds if we can match
            key = f"{norm_team(home)}|{norm_team(away)}"
            odds = odds_map.get(key)

            value_flag = ""
            value_side = ""
            value_prob = ""
            value_edge = ""
            value_odds = ""
            bookmaker = ""

            if odds:
                bookmaker = odds.get("bookmaker", "")
                ih, idr, ia = implied_probs_from_odds(odds["home"], odds["draw"], odds["away"])
                edge_home = p_home - ih
                edge_draw = p_draw - idr
                edge_away = p_away - ia

                candidates = [
                    ("HOME", p_home, edge_home, odds["home"]),
                    ("DRAW", p_draw, edge_draw, odds["draw"]),
                    ("AWAY", p_away, edge_away, odds["away"]),
                ]
                candidates.sort(key=lambda x: x[2], reverse=True)
                best_side, best_prob, best_edge, best_odds = candidates[0]

                if best_edge >= EDGE_THRESHOLD and best_prob >= MIN_PROB_TO_BET:
                    value_flag = "VALUE"
                    value_side = best_side
                    value_prob = best_prob
                    value_edge = best_edge
                    value_odds = best_odds

            all_rows.append({
                "competition": comp_name,
                "kickoff_london": dt_london.strftime("%Y-%m-%d %H:%M"),
                "home_team": home,
                "away_team": away,
                "elo_home": round(elo_h, 1),
                "elo_away": round(elo_a, 1),
                "p_home": round(p_home, 4),
                "p_draw": round(p_draw, 4),
                "p_away": round(p_away, 4),
                "odds_home": odds["home"] if odds else "",
                "odds_draw": odds["draw"] if odds else "",
                "odds_away": odds["away"] if odds else "",
                "bookmaker": bookmaker,
                "value_flag": value_flag,
                "value_side": value_side,
                "value_prob": round(value_prob, 4) if value_prob != "" else "",
                "value_edge": round(value_edge, 4) if value_edge != "" else "",
                "value_odds": value_odds,
            })

    # Sort by kickoff
    all_rows.sort(key=lambda x: x["kickoff_london"])

    # Write CSV
    output_dir = "/tmp/matchbot_reports"
    os.makedirs(output_dir, exist_ok=True)
    date_str = now_london.strftime("%Y-%m-%d")
    filepath = os.path.join(output_dir, f"daily_report_{date_str}.csv")

    df = pd.DataFrame(all_rows)
    df.to_csv(filepath, index=False)
    print(f"Daily report written to {filepath}")

    # --- Telegram Summary (Mode B): VALUE picks + Top 5 fixtures probs ---
    msg_lines = [
        "MatchBot — Daily Summary (Mode B)",
        f"Run (London): {now_london.strftime('%Y-%m-%d %H:%M')}",
        ""
    ]

    value_all = [r for r in all_rows if str(r.get("value_flag", "")).upper() == "VALUE"]
    value_all.sort(key=lambda x: float(x.get("value_edge") or 0), reverse=True)

    if value_all:
        msg_lines.append("Top VALUE picks (max 5):")
        for r in value_all[:5]:
            msg_lines.append(
                f"{r['kickoff_london']} {r['competition']} — {r['home_team']} vs {r['away_team']}\n"
                f"{r['value_side']} @ {r['value_odds']} | "
                f"P:{int(float(r['value_prob'])*100)}% | "
                f"Edge:+{int(float(r['value_edge'])*100)}% | "
                f"Book: {r.get('bookmaker','')}"
            )
    else:
        msg_lines.append("No VALUE picks today (thresholds held).")

    msg_lines.append("")
    msg_lines.append("Top 5 fixtures (probabilities):")

    soonest = all_rows[:5]
    if soonest:
        for r in soonest:
            msg_lines.append(
                f"{r['kickoff_london']} {r['competition']} — {r['home_team']} vs {r['away_team']} | "
                f"H:{int(float(r['p_home'])*100)}% "
                f"D:{int(float(r['p_draw'])*100)}% "
                f"A:{int(float(r['p_away'])*100)}%"
            )
    else:
        msg_lines.append("No fixtures found in the next 72 hours.")

    send_telegram_message("\n\n".join(msg_lines).strip())

if __name__ == "__main__":
    main()
