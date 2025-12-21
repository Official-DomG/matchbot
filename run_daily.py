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

# Window for UPCOMING: now -> end of Sunday (London time)
# Window for RESULTS: yesterday 00:00 -> now (London time)  (can be adjusted easily)

# Elo model tuning
HOME_ADV_ELO = 55
DRAW_BASE = 0.24
DRAW_TIGHTNESS = 260.0

# Provider: SportsDB is reliable for your setup. API-Football is optional fallback (requires key).
USE_API_FOOTBALL_IF_AVAILABLE = True

# SportsDB key:
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
    SportsDB returns dateEvent 'YYYY-MM-DD' and strTime sometimes 'HH:MM:SS' or None.
    """
    if not date_str:
        return None
    if not time_str:
        time_str = "00:00:00"
    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Sometimes time is "HH:MM"
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
    """
    Basic C3 pick: choose the highest probability.
    Returns: "HOME" / "DRAW" / "AWAY"
    """
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
    Try to resolve league id by listing all soccer leagues in England and matching by name.
    Fallback to known IDs.
    """
    fallback = SPORTSDB_LEAGUE_ID_FALLBACK.get(league_name)
    try:
        url = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}/search_all_leagues.php?c=England&s=Soccer"
        data = sportsdb_get(url)
        leagues = (data or {}).get("countries") or []
        target = norm(league_name)
        # Allow common label variants
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

def sportsdb_fetch_next_events(league_id: int):
    url = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}/eventsnextleague.php?id={league_id}"
    data = sportsdb_get(url)
    return (data or {}).get("events") or []

def sportsdb_fetch_past_events(league_id: int):
    # last ~15
    url = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}/eventspastleague.php?id={league_id}"
    data = sportsdb_get(url)
    return (data or {}).get("events") or []

# =========================
# API-FOOTBALL (OPTIONAL, SECONDARY)
# You can ignore this if you want SportsDB only.
# =========================
def apifootball_headers():
    return {"x-apisports-key": API_FOOTBALL_KEY}

def apifootball_get(url: str, params: dict):
    r = requests.get(url, headers=apifootball_headers(), params=params, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"API-Football HTTP {r.status_code}: {(r.text or '')[:200]}")
    data = get_json_or_none(r)
    if not data:
        raise RuntimeError("API-Football returned non-JSON/empty response")
    return data

# NOTE: We are not relying on API-Football league IDs here (to keep this stable).
# If you later want full API-Football integration, we can add a mapping.

# =========================
# CSV OUTPUT (NO PANDAS)
# =========================
def write_csv(rows: list, filepath: str):
    if not rows:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    # stable field order
    fields = sorted({k for r in rows for k in r.keys()})
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)

# =========================
# MAIN (C3)
# =========================
def main():
    now_london = datetime.now(LONDON)
    wd = now_london.weekday()

    header = [
        f"MatchBot — C3 (Fixtures + Results + Eval) Thu–Sun",
        f"Run (London): {now_london.strftime('%Y-%m-%d %H:%M')}",
        f"Deploy: {DEPLOY_MARKER}",
        ""
    ]

    if wd not in RUN_DAYS:
        msg = "\n".join(header + [
            "Today is outside Thu–Sun.",
            "Skipping run (this is expected if your Render cron is still daily)."
        ])
        send_telegram_chunks(msg)
        print("Skip: outside Thu–Sun")
        return

    # Window: now -> end of Sunday
    end_sun = (now_london + timedelta(days=(6 - wd))).date()
    window_end_london = LONDON.localize(datetime.combine(end_sun, dtime(23, 59, 59)))
    window_start_utc = now_london.astimezone(UTC)
    window_end_utc = window_end_london.astimezone(UTC)

    # Results window: yesterday 00:00 -> now
    yday = (now_london - timedelta(days=1)).date()
    results_start_london = LONDON.localize(datetime.combine(yday, dtime(0, 0, 0)))
    results_start_utc = results_start_london.astimezone(UTC)
    results_end_utc = now_london.astimezone(UTC)

    print("Window upcoming UTC:", window_start_utc, "->", window_end_utc)
    print("Window results  UTC:", results_start_utc, "->", results_end_utc)

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

        # Ratings (for probabilities + eval)
        ratings = sportsdb_fetch_table_ratings(league_id)

        # UPCOMING (from SportsDB "next" list filtered by time window)
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
            pick = pick_from_probs(p_home, p_draw, p_away)

            upcoming_rows.append({
                "type": "UPCOMING",
                "league": league_name,
                "kickoff_london": dt_london.strftime("%Y-%m-%d %H:%M"),
                "home": home,
                "away": away,
                "p_home": round(p_home, 4),
                "p_draw": round(p_draw, 4),
                "p_away": round(p_away, 4),
                "pick": pick,
                "source": "SportsDB",
            })

        # RESULTS (from SportsDB "past" list filtered to yesterday->now)
        past_events = sportsdb_fetch_past_events(league_id)
        for ev in past_events:
            # A completed match should have scores
            hg = safe_int(ev.get("intHomeScore"))
            ag = safe_int(ev.get("intAwayScore"))
            # Some entries can be 0/0 even if not played; we need a strong completion check
            status = norm(ev.get("strStatus") or "")
            if status and status not in {"match finished", "ft", "finished", "aet", "pen"}:
                # allow empty status (SportsDB can be inconsistent), otherwise skip non-finished
                continue

            dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
            if not dt_utc:
                continue
            if not (results_start_utc <= dt_utc <= results_end_utc):
                continue

            home = ev.get("strHomeTeam") or ""
            away = ev.get("strAwayTeam") or ""
            dt_london = dt_utc.astimezone(LONDON)

            # Model prediction for evaluation
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

    # Sort upcoming by confidence then kickoff
    def conf(row):
        return max(row["p_home"], row["p_draw"], row["p_away"])

    upcoming_rows.sort(key=lambda r: (-conf(r), r["kickoff_london"]))
    result_rows.sort(key=lambda r: r["kickoff_london"])
    eval_rows.sort(key=lambda r: r["kickoff_london"])

    # Build Telegram message
    lines = header + [
        f"Window (Upcoming): now → Sun {window_end_london.strftime('%Y-%m-%d %H:%M')} (London)",
        f"Window (Results):  {results_start_london.strftime('%Y-%m-%d %H:%M')} → now (London)",
        ""
    ]

    # RESULTS block
    lines.append("RESULTS (yesterday + today):")
    if not result_rows:
        lines.append("- No completed matches found (yet).")
    else:
        for r in result_rows[:25]:
            lines.append(f"- {r['kickoff_london']} {r['league']} — {r['home']} {r['score']} {r['away']}")

    lines.append("")
    # EVAL block
    lines.append("C3 EVALUATION (model pick vs actual):")
    if total_eval == 0:
        lines.append("- No matches to evaluate in this results window.")
    else:
        acc = (correct_eval / total_eval) * 100.0
        lines.append(f"- Accuracy (this window): {correct_eval}/{total_eval} = {acc:.1f}%")

        # Optional: show the last 10 evaluated matches
        lines.append("")
        lines.append("Last 10 evaluated matches:")
        for e in eval_rows[-10:]:
            lines.append(
                f"- {e['kickoff_london']} {e['league']} — {e['home']} {e['score']} {e['away']} | "
                f"Pick:{e['model_pick']} Actual:{e['actual']} Hit:{e['hit']}"
            )

    lines.append("")
    # UPCOMING block
    lines.append("UPCOMING (now → Sunday):")
    if not upcoming_rows:
        lines.append("- No upcoming fixtures found in the window.")
    else:
        for u in upcoming_rows[:25]:
            lines.append(
                f"- {u['kickoff_london']} {u['league']} — {u['home']} vs {u['away']} | "
                f"H:{int(u['p_home']*100)}% D:{int(u['p_draw']*100)}% A:{int(u['p_away']*100)}% | "
                f"Pick:{u['pick']}"
            )

    # Send Telegram output
    send_telegram_chunks("\n".join(lines))

    # Write CSVs
    out_dir = "/tmp/matchbot_reports"
    date_tag = now_london.strftime("%Y-%m-%d")
    write_csv(upcoming_rows, f"{out_dir}/upcoming_{date_tag}.csv")
    write_csv(result_rows,   f"{out_dir}/results_{date_tag}.csv")
    write_csv(eval_rows,     f"{out_dir}/eval_{date_tag}.csv")
    print("CSV reports written to", out_dir)

if __name__ == "__main__":
    try:
        main()
        print("MatchBot finished successfully")
    except Exception as e:
        err = f"MatchBot crashed ❌\n{type(e).__name__}: {e}"
        print(err)
        send_telegram_chunks(err)
        raise
