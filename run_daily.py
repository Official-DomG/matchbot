import os
import math
import csv
import requests
import pytz
from datetime import datetime, timedelta, time as dtime

# =========================
# DEPLOY MARKER
# =========================
DEPLOY_MARKER = "V-C4-CONF-THU-SUN-001"
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

# Provider: SportsDB primary for your setup.
SPORTSDB_API_KEY = os.environ.get("SPORTSDB_API_KEY", "123")

# Leagues to track (auto-resolve in SportsDB; fallback hardcoded IDs)
LEAGUE_NAMES = ["Premier League", "EFL Championship"]
SPORTSDB_LEAGUE_ID_FALLBACK = {
    "Premier League": 4328,
    "EFL Championship": 4329,  # English League Championship
}

# =========================
# C4: CONFIDENCE FILTERING
# =========================
# Only send upcoming picks where max(H/D/A) >= this value.
MIN_CONF_UPCOMING = float(os.environ.get("MIN_CONF_UPCOMING", "0.60"))  # 55% default
MAX_UPCOMING_POST = int(os.environ.get("MAX_UPCOMING_POST", "10"))      # cap message size

# =========================
# TELEGRAM
# =========================
TELEGRAM_MAX_LEN = 3800  # stay under 4096

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

def sportsdb_resolve_league_id(league_name: str) -> int:
    """
    Resolve league id by listing all soccer leagues in England and matching by name.
    Fallback to known IDs.
    """
    fallback = SPORTSDB_LEAGUE_ID_FALLBACK.get(league_name)
    try:
        url = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}/search_all_leagues.php?c=England&s=Soccer"
        data = sportsdb_get(url)
        leagues = (data or {}).get("countries") or []
        target = norm(league_name)

        aliases = {
            "premier league": {"premier league", "english premier league"},
            "efl championship": {"efl championship", "english league championship", "championship"},
        }
        allowed = aliases.get(target, {target})

        for item in leagues:
            nm = norm(item.get("strLeague") or "")
            if nm in allowed:
                lid = safe_int(item.get("idLeague"))
                if lid:
                    return lid
    except Exception as e:
        print("League resolve failed:", type(e).__name__, e)

    return fallback or 0

def sportsdb_fetch_table_ratings(league_id: int):
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

def sportsdb_fetch_next_events(league_id: int):
    url = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}/eventsnextleague.php?id={league_id}"
    data = sportsdb_get(url)
    return (data or {}).get("events") or []

def sportsdb_fetch_past_events(league_id: int):
    url = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}/eventspastleague.php?id={league_id}"
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
        f"MatchBot — C4 (Confidence Filtered Picks) Thu–Sun",
        f"Run (London): {now_london.strftime('%Y-%m-%d %H:%M')}",
        f"Deploy: {DEPLOY_MARKER}",
        f"Upcoming filter: min_conf={int(MIN_CONF_UPCOMING*100)}% | max_posts={MAX_UPCOMING_POST}",
        ""
    ]

    if wd not in RUN_DAYS:
        msg = "\n".join(header + [
            "Today is outside Thu–Sun.",
            "Skipping run (expected if your Render cron is still daily)."
        ])
        send_telegram_chunks(msg)
        print("Skip: outside Thu–Sun")
        return

    # Window: now -> end of Sunday (London)
    end_sun = (now_london + timedelta(days=(6 - wd))).date()
    window_end_london = LONDON.localize(datetime.combine(end_sun, dtime(23, 59, 59)))
    window_start_utc = now_london.astimezone(UTC)
    window_end_utc = window_end_london.astimezone(UTC)

    # Results window: yesterday 00:00 -> now (London)
    yday = (now_london - timedelta(days=1)).date()
    results_start_london = LONDON.localize(datetime.combine(yday, dtime(0, 0, 0)))
    results_start_utc = results_start_london.astimezone(UTC)
    results_end_utc = now_london.astimezone(UTC)

    league_ids = {}
    for name in LEAGUE_NAMES:
        lid = sportsdb_resolve_league_id(name)
        if not lid:
            lid = SPORTSDB_LEAGUE_ID_FALLBACK.get(name, 0)
        league_ids[name] = lid

    upcoming_rows = []
    result_rows = []
    eval_rows = []

    total_eval = 0
    correct_eval = 0

    for league_name, league_id in league_ids.items():
        if not league_id:
            continue

        ratings = sportsdb_fetch_table_ratings(league_id)

        # UPCOMING (SportsDB "next" list filtered to time window)
        next_events = sportsdb_fetch_next_events(league_id)
        for ev in next_events:
            dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
            if not dt_utc:
                continue
            if not (window_start_utc <= dt_utc <= window_end_utc):
                continue

            home = ev.get("strHomeTeam") or ""
            away = ev.get("strAwayTeam") or ""
            dt_london = dt_utc.astimezone(LONDON)

            elo_h = find_team_rating(ratings, home) or 1500.0
            elo_a = find_team_rating(ratings, away) or 1500.0
            p_home, p_draw, p_away = probs_1x2(elo_h, elo_a)

            conf = max(p_home, p_draw, p_away)
            pick = pick_from_probs(p_home, p_draw, p_away)

            # C4 filter applied here
            if conf < MIN_CONF_UPCOMING:
                continue

            upcoming_rows.append({
                "type": "UPCOMING",
                "league": league_name,
                "kickoff_london": dt_london.strftime("%Y-%m-%d %H:%M"),
                "home": home,
                "away": away,
                "p_home": round(p_home, 4),
                "p_draw": round(p_draw, 4),
                "p_away": round(p_away, 4),
                "confidence": round(conf, 4),
                "pick": pick,
                "source": "SportsDB",
            })

        # RESULTS (SportsDB "past" list filtered to yesterday->now)
        past_events = sportsdb_fetch_past_events(league_id)
        for ev in past_events:
            hg = safe_int(ev.get("intHomeScore"))
            ag = safe_int(ev.get("intAwayScore"))

            status = norm(ev.get("strStatus") or "")
            if status and status not in {"match finished", "ft", "finished", "aet", "pen"}:
                continue

            dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
            if not dt_utc:
                continue
            if not (results_start_utc <= dt_utc <= results_end_utc):
                continue

            home = ev.get("strHomeTeam") or ""
            away = ev.get("strAwayTeam") or ""
            dt_london = dt_utc.astimezone(LONDON)

            elo_h = find_team_rating(ratings, home) or 1500.0
            elo_a = find_team_rating(ratings, away) or 1500.0
            p_home, p_draw, p_away = probs_1x2(elo_h, elo_a)

            model_pick = pick_from_probs(p_home, p_draw, p_away)
            actual = actual_outcome(hg, ag)
            hit = (model_pick == actual)

            total_eval += 1
            if hit:
                correct_eval += 1

            result_rows.append({
                "type": "RESULT",
                "league": league_name,
                "kickoff_london": dt_london.strftime("%Y-%m-%d %H:%M"),
                "home": home,
                "away": away,
                "score": f"{hg}-{ag}",
                "source": "SportsDB",
            })

            eval_rows.append({
                "type": "EVAL",
                "league": league_name,
                "kickoff_london": dt_london.strftime("%Y-%m-%d %H:%M"),
                "home": home,
                "away": away,
                "score": f"{hg}-{ag}",
                "model_pick": model_pick,
                "actual": actual,
                "hit": "YES" if hit else "NO",
                "p_home": round(p_home, 4),
                "p_draw": round(p_draw, 4),
                "p_away": round(p_away, 4),
                "source": "SportsDB",
            })

    # Sorting
    upcoming_rows.sort(key=lambda r: (-r["confidence"], r["kickoff_london"]))
    result_rows.sort(key=lambda r: r["kickoff_london"])
    eval_rows.sort(key=lambda r: r["kickoff_london"])

    # Build Telegram message
    lines = header + [
        f"Window (Upcoming): now → Sun {window_end_london.strftime('%Y-%m-%d %H:%M')} (London)",
        f"Window (Results):  {results_start_london.strftime('%Y-%m-%d %H:%M')} → now (London)",
        ""
    ]

    # RESULTS
    lines.append("RESULTS (yesterday + today):")
    if not result_rows:
        lines.append("- No completed matches found (yet).")
    else:
        for r in result_rows[:25]:
            lines.append(f"- {r['kickoff_london']} {r['league']} — {r['home']} {r['score']} {r['away']}")

    lines.append("")
    # EVAL
    lines.append("C3 EVALUATION (model pick vs actual):")
    if total_eval == 0:
        lines.append("- No matches to evaluate in this results window.")
    else:
        acc = (correct_eval / total_eval) * 100.0
        lines.append(f"- Accuracy (this window): {correct_eval}/{total_eval} = {acc:.1f}%")
        lines.append("")
        lines.append("Last 10 evaluated matches:")
        for r in eval_rows[-10:]:
            lines.append(
                f"- {r['kickoff_london']} {r['league']} — {r['home']} {r['score']} {r['away']} | "
                f"Pick:{r['model_pick']} Actual:{r['actual']} Hit:{r['hit']}"
            )

    lines.append("")
    # UPCOMING (C4-filtered)
    lines.append(f"UPCOMING (filtered ≥ {int(MIN_CONF_UPCOMING*100)}% confidence):")
    if not upcoming_rows:
        lines.append("- No upcoming fixtures passed the confidence filter in the window.")
    else:
        for r in upcoming_rows[:MAX_UPCOMING_POST]:
            lines.append(
                f"- {r['kickoff_london']} {r['league']} — {r['home']} vs {r['away']} | "
                f"H:{int(r['p_home']*100)}% D:{int(r['p_draw']*100)}% A:{int(r['p_away']*100)}% | "
                f"Conf:{int(r['confidence']*100)}% | Pick:{r['pick']}"
            )

    # Send message
    send_telegram_chunks("\n".join(lines))

    # Write CSVs
    out_dir = "/tmp/matchbot_reports"
    date_str = now_london.strftime("%Y-%m-%d")
    write_csv(result_rows, os.path.join(out_dir, f"results_{date_str}.csv"))
    write_csv(eval_rows, os.path.join(out_dir, f"eval_{date_str}.csv"))
    write_csv(upcoming_rows, os.path.join(out_dir, f"upcoming_filtered_{date_str}.csv"))
    print("CSV reports written to", out_dir)

if __name__ == "__main__":
    main()
