import os
import math
import csv
import requests
import pytz
from datetime import datetime, timedelta, time as dtime

# =========================
# DEPLOY MARKER
# =========================
DEPLOY_MARKER = "V-C4-ALLMATCHES-FALLBACK-LIVE-001"
print(f"DEPLOY MARKER: {DEPLOY_MARKER}")

# =========================
# TIMEZONES
# =========================
LONDON = pytz.timezone("Europe/London")
UTC = pytz.utc

# =========================
# CONFIG
# =========================
# Thu=3 Fri=4 Sat=5 Sun=6 (Python weekday: Mon=0)
RUN_DAYS = {3, 4, 5, 6}

# Elo model tuning
HOME_ADV_ELO = 55
DRAW_BASE = 0.24
DRAW_TIGHTNESS = 260.0

# SportsDB key (free key often "123")
SPORTSDB_API_KEY = os.environ.get("SPORTSDB_API_KEY", "123")

# Leagues to track (auto-resolve; fallback IDs)
LEAGUE_NAMES = ["Premier League", "EFL Championship"]
SPORTSDB_LEAGUE_ID_FALLBACK = {
    "Premier League": 4328,
    "EFL Championship": 4329,  # English League Championship
}

# Telegram
TELEGRAM_MAX_LEN = 3800  # keep under 4096

# Output CSV
OUT_DIR = "/tmp/matchbot_reports"
CSV_PREFIX = "daily_report"  # keep filename family consistent

# =========================
# LIVE / IN-PLAY HEURISTICS
# =========================
LIVE_GRACE = timedelta(minutes=10)            # allow minor API clock drift
LIVE_MAX_DURATION = timedelta(hours=2, minutes=55)  # 90 + HT + stoppage (safe upper bound)

# =========================
# FALLBACK (when weekend is empty)
# =========================
FALLBACK_IF_EMPTY = True
FALLBACK_UPCOMING_DAYS = int(os.environ.get("FALLBACK_UPCOMING_DAYS", "7"))
FALLBACK_RESULTS_HOURS = int(os.environ.get("FALLBACK_RESULTS_HOURS", "48"))

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
    SportsDB usually returns dateEvent 'YYYY-MM-DD' and strTime 'HH:MM:SS' or 'HH:MM' or None.
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
def sportsdb_get(url: str, params: dict | None = None):
    r = requests.get(url, params=params, timeout=25)
    if r.status_code != 200:
        print(f"SportsDB HTTP {r.status_code}: {r.url}")
        return None
    return get_json_or_none(r)

def sportsdb_resolve_league_meta(league_name: str):
    """
    Returns (idLeague, canonical strLeague) for use with eventsday.php.
    """
    fallback_id = SPORTSDB_LEAGUE_ID_FALLBACK.get(league_name, 0)
    fallback_name = None

    try:
        url = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}/search_all_leagues.php"
        data = sportsdb_get(url, params={"c": "England", "s": "Soccer"})
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
                canon = item.get("strLeague") or None
                if lid:
                    return lid, canon

    except Exception as e:
        print("League resolve failed:", type(e).__name__, e)

    # Reasonable fallbacks for eventsday 'l='
    if league_name == "Premier League":
        fallback_name = "English Premier League"
    elif league_name == "EFL Championship":
        fallback_name = "English League Championship"

    return fallback_id, fallback_name

def sportsdb_fetch_table_ratings(league_id: int):
    url = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}/lookuptable.php"
    data = sportsdb_get(url, params={"l": str(league_id)})
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

def sportsdb_fetch_events_day(date_yyyy_mm_dd: str, league_filter_name: str | None):
    """
    eventsday.php returns events for a day.
    We pass sport Soccer + league name when available.
    """
    url = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}/eventsday.php"
    params = {"d": date_yyyy_mm_dd, "s": "Soccer"}
    if league_filter_name:
        params["l"] = league_filter_name
    data = sportsdb_get(url, params=params)
    return (data or {}).get("events") or []

# =========================
# STATUS CLASSIFICATION
# =========================
def is_finished(ev: dict) -> bool:
    status = norm(ev.get("strStatus") or "")
    finished_markers = {"match finished", "ft", "finished", "aet", "pen"}
    if status in finished_markers:
        return True
    # if status missing but scores exist, treat as finished
    return (ev.get("intHomeScore") is not None) and (ev.get("intAwayScore") is not None)

def is_liveish(ev: dict, now_utc: datetime) -> bool:
    status = norm(ev.get("strStatus") or "")
    live_markers = {"in play", "live", "1h", "2h", "ht", "1st half", "2nd half"}
    if status in live_markers:
        return True

    dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
    if not dt_utc:
        return False

    live_start = dt_utc - LIVE_GRACE
    live_end = dt_utc + LIVE_MAX_DURATION
    if live_start <= now_utc <= live_end:
        return not is_finished(ev)

    return False

# =========================
# CORE COLLECTION
# =========================
def collect_for_dates(leagues_meta: list, dates: list, now_utc: datetime,
                      results_start_utc: datetime | None = None, results_end_utc: datetime | None = None,
                      upcoming_start_utc: datetime | None = None, upcoming_end_utc: datetime | None = None):
    """
    Collects RESULT, LIVE, UPCOMING rows for given date list using eventsday.php.
    - If results_* provided, RESULT/EVAL are limited to that UTC range.
    - If upcoming_* provided, UPCOMING are limited to that UTC range.
    """
    upcoming_rows = []
    live_rows = []
    result_rows = []
    eval_rows = []
    all_csv_rows = []

    total_eval = 0
    correct_eval = 0

    for lg in leagues_meta:
        league_name = lg["name"]
        league_id = lg["id"]
        league_canon = lg["canon"]
        if not league_id:
            continue

        ratings = sportsdb_fetch_table_ratings(league_id)

        seen_event_ids = set()
        for day in dates:
            events = sportsdb_fetch_events_day(day, league_canon)
            for ev in events:
                # Safety filter if league filter fails
                ev_league_id = safe_int(ev.get("idLeague"))
                if ev_league_id and ev_league_id != league_id:
                    continue

                ev_id = str(ev.get("idEvent") or "")
                if ev_id and ev_id in seen_event_ids:
                    continue
                if ev_id:
                    seen_event_ids.add(ev_id)

                home = ev.get("strHomeTeam") or ""
                away = ev.get("strAwayTeam") or ""

                dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
                if not dt_utc:
                    continue

                dt_london_str = dt_utc.astimezone(LONDON).strftime("%Y-%m-%d %H:%M")

                elo_h = find_team_rating(ratings, home) or 1500.0
                elo_a = find_team_rating(ratings, away) or 1500.0
                p_home, p_draw, p_away = probs_1x2(elo_h, elo_a)
                pick = pick_from_probs(p_home, p_draw, p_away)

                hg_raw = ev.get("intHomeScore")
                ag_raw = ev.get("intAwayScore")
                hg = None if hg_raw in (None, "") else safe_int(hg_raw)
                ag = None if ag_raw in (None, "") else safe_int(ag_raw)

                finished = is_finished(ev)
                liveish = is_liveish(ev, now_utc)

                base_row = {
                    "league": league_name,
                    "kickoff_london": dt_london_str,
                    "home": home,
                    "away": away,
                    "p_home": round(p_home, 4),
                    "p_draw": round(p_draw, 4),
                    "p_away": round(p_away, 4),
                    "pick": pick,
                    "source": "SportsDB",
                    "idEvent": ev_id,
                    "status": (ev.get("strStatus") or ""),
                }

                # RESULTS/EVAL within range
                if finished and hg is not None and ag is not None:
                    if results_start_utc and dt_utc < results_start_utc:
                        continue
                    if results_end_utc and dt_utc > results_end_utc:
                        continue

                    actual = actual_outcome(hg, ag)
                    hit = (pick == actual)
                    total_eval += 1
                    if hit:
                        correct_eval += 1

                    r = base_row | {
                        "type": "RESULT",
                        "score": f"{hg}-{ag}",
                        "actual": actual,
                        "hit": "YES" if hit else "NO",
                    }
                    result_rows.append(r)
                    eval_rows.append(r)
                    all_csv_rows.append(r)

                # LIVE
                elif liveish:
                    r = base_row | {
                        "type": "LIVE",
                        "score": "" if (hg is None or ag is None) else f"{hg}-{ag}",
                    }
                    live_rows.append(r)
                    all_csv_rows.append(r)

                # UPCOMING within range
                else:
                    if upcoming_start_utc and dt_utc < upcoming_start_utc:
                        continue
                    if upcoming_end_utc and dt_utc > upcoming_end_utc:
                        continue

                    r = base_row | {"type": "UPCOMING"}
                    upcoming_rows.append(r)
                    all_csv_rows.append(r)

    # Sorting
    def kickoff_key(r):
        return r.get("kickoff_london") or "9999-99-99 99:99"

    def conf_key(r):
        return max(r.get("p_home", 0), r.get("p_draw", 0), r.get("p_away", 0))

    result_rows.sort(key=kickoff_key)
    eval_rows.sort(key=kickoff_key)
    live_rows.sort(key=lambda r: (-conf_key(r), kickoff_key(r)))
    upcoming_rows.sort(key=kickoff_key)

    return {
        "upcoming_rows": upcoming_rows,
        "live_rows": live_rows,
        "result_rows": result_rows,
        "eval_rows": eval_rows,
        "all_csv_rows": all_csv_rows,
        "total_eval": total_eval,
        "correct_eval": correct_eval,
    }

# =========================
# MAIN
# =========================
def main():
    now_london = datetime.now(LONDON)
    now_utc = now_london.astimezone(UTC)
    wd = now_london.weekday()

    header = [
        "MatchBot — C4 (All matches Thu–Sun + Live + Eval + Fallback)",
        f"Run (London): {now_london.strftime('%Y-%m-%d %H:%M')}",
        f"Deploy: {DEPLOY_MARKER}",
        ""
    ]

    # If outside Thu–Sun, still send a heartbeat
    if wd not in RUN_DAYS:
        msg = "\n".join(header + [
            "Today is outside Thu–Sun.",
            "Skipping main window (expected if your Render cron still runs daily)."
        ])
        send_telegram_chunks(msg)
        print("Skip: outside Thu–Sun")
        return

    # Thu->Sun window for THIS run
    start_thu_date = (now_london.date() - timedelta(days=(wd - 3)))
    end_sun_date = (now_london.date() + timedelta(days=(6 - wd)))

    # Dates list Thu/Fri/Sat/Sun
    dates = []
    d = start_thu_date
    while d <= end_sun_date:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    # Resolve leagues meta
    leagues_meta = []
    for name in LEAGUE_NAMES:
        lid, canon = sportsdb_resolve_league_meta(name)
        leagues_meta.append({"name": name, "id": lid, "canon": canon})

    # Define windows:
    # Results: Thu 00:00 -> now (within the Thu–Sun window)
    results_start_london = LONDON.localize(datetime.combine(start_thu_date, dtime(0, 0, 0)))
    results_start_utc = results_start_london.astimezone(UTC)
    results_end_utc = now_utc

    # Upcoming: now -> end of Sunday 23:59:59
    window_end_london = LONDON.localize(datetime.combine(end_sun_date, dtime(23, 59, 59)))
    window_end_utc = window_end_london.astimezone(UTC)

    payload = collect_for_dates(
        leagues_meta=leagues_meta,
        dates=dates,
        now_utc=now_utc,
        results_start_utc=results_start_utc,
        results_end_utc=results_end_utc,
        upcoming_start_utc=now_utc,
        upcoming_end_utc=window_end_utc,
    )

    upcoming_rows = payload["upcoming_rows"]
    live_rows = payload["live_rows"]
    result_rows = payload["result_rows"]
    eval_rows = payload["eval_rows"]
    all_csv_rows = payload["all_csv_rows"]
    total_eval = payload["total_eval"]
    correct_eval = payload["correct_eval"]

    fallback_note = None

    # =========================
    # FALLBACK: if weekend window has nothing, switch to rolling windows
    # =========================
    if FALLBACK_IF_EMPTY and (not upcoming_rows) and (not live_rows) and (not result_rows):
        rolling_end_london = now_london + timedelta(days=FALLBACK_UPCOMING_DAYS)
        rolling_start_utc = now_utc
        rolling_end_utc = rolling_end_london.astimezone(UTC)

        res_start_utc_fb = (now_london - timedelta(hours=FALLBACK_RESULTS_HOURS)).astimezone(UTC)
        res_end_utc_fb = now_utc

        # Build day list for next N days
        days = []
        d = now_london.date()
        while d <= rolling_end_london.date():
            days.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)

        payload_fb = collect_for_dates(
            leagues_meta=leagues_meta,
            dates=days,
            now_utc=now_utc,
            results_start_utc=res_start_utc_fb,
            results_end_utc=res_end_utc_fb,
            upcoming_start_utc=rolling_start_utc,
            upcoming_end_utc=rolling_end_utc,
        )

        upcoming_rows = payload_fb["upcoming_rows"]
        live_rows = payload_fb["live_rows"]
        result_rows = payload_fb["result_rows"]
        eval_rows = payload_fb["eval_rows"]
        all_csv_rows = payload_fb["all_csv_rows"]
        total_eval = payload_fb["total_eval"]
        correct_eval = payload_fb["correct_eval"]

        fallback_note = (
            f"Fallback active: Thu–Sun window had no matches. "
            f"Showing next {FALLBACK_UPCOMING_DAYS} days + last {FALLBACK_RESULTS_HOURS}h results."
        )

    # =========================
    # TELEGRAM MESSAGE
    # =========================
    lines = header + [
        f"Window: Thu {start_thu_date} → Sun {end_sun_date} (London)",
        f"Leagues: {', '.join([l['name'] for l in leagues_meta if l['id']])}",
        ""
    ]

    if fallback_note:
        lines.append(fallback_note)
        lines.append("")

    # RESULTS
    lines.append("RESULTS (completed):")
    if not result_rows:
        lines.append("- None found (yet).")
    else:
        for r in result_rows[:40]:
            lines.append(f"- {r['kickoff_london']} {r['league']} — {r['home']} {r['score']} {r['away']}")

    # EVAL
    lines.append("")
    lines.append("EVALUATION (model pick vs actual):")
    if total_eval == 0:
        lines.append("- No completed matches to evaluate.")
    else:
        acc = (correct_eval / total_eval) * 100.0
        lines.append(f"- Accuracy: {correct_eval}/{total_eval} = {acc:.1f}%")
        lines.append("Last 10 evaluated matches:")
        for r in eval_rows[-10:]:
            lines.append(
                f"- {r['kickoff_london']} {r['league']} — {r['home']} {r['score']} {r['away']} | "
                f"Pick:{r['pick']} Actual:{r['actual']} Hit:{r['hit']}"
            )

    # LIVE
    lines.append("")
    lines.append("LIVE / IN-PLAY (matches happening now):")
    if not live_rows:
        lines.append("- None in-play right now (or SportsDB didn’t flag them).")
    else:
        for r in live_rows[:40]:
            h = int(r["p_home"] * 100)
            d = int(r["p_draw"] * 100)
            a = int(r["p_away"] * 100)
            score = r.get("score") or "?"
            lines.append(
                f"- {r['kickoff_london']} {r['league']} — {r['home']} vs {r['away']} "
                f"(Score:{score}) | H:{h}% D:{d}% A:{a}% | Pick:{r['pick']}"
            )

    # UPCOMING
    lines.append("")
    lines.append("UPCOMING:")
    if not upcoming_rows:
        lines.append("- No upcoming fixtures found in the window.")
    else:
        for r in upcoming_rows[:80]:
            h = int(r["p_home"] * 100)
            d = int(r["p_draw"] * 100)
            a = int(r["p_away"] * 100)
            lines.append(
                f"- {r['kickoff_london']} {r['league']} — {r['home']} vs {r['away']} | "
                f"H:{h}% D:{d}% A:{a}% | Pick:{r['pick']}"
            )

    send_telegram_chunks("\n".join(lines))

    # =========================
    # CSV OUTPUT
    # =========================
    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = now_london.strftime("%Y-%m-%d_%H%M")
    filepath = os.path.join(OUT_DIR, f"{CSV_PREFIX}_{stamp}.csv")
    write_csv(all_csv_rows, filepath)
    print(f"CSV reports written to {OUT_DIR}")
    print("MatchBot finished successfully")

if __name__ == "__main__":
    main()
```0
