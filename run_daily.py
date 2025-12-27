# -*- coding: utf-8 -*-

import os
import math
import csv
import requests
import pytz
from datetime import datetime, timedelta, time as dtime

# =========================
# DEPLOY MARKER (ASCII ONLY)
# =========================
DEPLOY_MARKER = "V-C4-ALLMATCHES-THU-SUN-ASCII-001"
print("DEPLOY MARKER:", DEPLOY_MARKER)

# =========================
# TIMEZONES
# =========================
LONDON = pytz.timezone("Europe/London")
UTC = pytz.utc

# =========================
# CONFIG
# =========================
# If your Render cron is still daily, we self-skip Mon-Wed.
RUN_DAYS = {3, 4, 5, 6}  # Thu=3 Fri=4 Sat=5 Sun=6 (Mon=0)

SPORTSDB_API_KEY = os.environ.get("SPORTSDB_API_KEY", "123")

LEAGUE_NAMES = ["Premier League", "EFL Championship"]
SPORTSDB_LEAGUE_ID_FALLBACK = {
    "Premier League": 4328,
    "EFL Championship": 4329,
}

HOME_ADV_ELO = 55
DRAW_BASE = 0.24
DRAW_TIGHTNESS = 260.0

TELEGRAM_MAX_LEN = 3800

# If a match started recently, still include it in "UPCOMING/LIVE" block.
LIVE_GRACE_MINUTES = 120  # show matches that started up to 2h ago

# Results window: look back this many hours (covers late kickoffs)
RESULTS_LOOKBACK_HOURS = 36

# Output directory on Render
OUT_DIR = "/tmp/matchbot_reports"
# =========================
# TELEGRAM
# =========================
def send_telegram_message(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("Telegram not configured")
        return

    url = "https://api.telegram.org/bot{}/sendMessage".format(token)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True
    }

    r = requests.post(url, json=payload, timeout=20)
    print("Telegram response:", r.status_code)


def send_telegram_chunks(text: str) -> None:
    text = (text or "").strip()
    if not text:
        return

    chunks = []
    while len(text) > TELEGRAM_MAX_LEN:
        cut = text.rfind("\n", 0, TELEGRAM_MAX_LEN)
        if cut == -1:
            cut = TELEGRAM_MAX_LEN
        chunks.append(text[:cut])
        text = text[cut:].lstrip()

    chunks.append(text)

    for i, chunk in enumerate(chunks, start=1):
        suffix = "\n\n(Part {}/{})".format(i, len(chunks))
        send_telegram_message(chunk + suffix)


# =========================
# BASIC HELPERS
# =========================
def safe_int(x):
    try:
        if x is None or x == "":
            return 0
        return int(float(x))
    except Exception:
        return 0


def get_json_or_none(resp):
    try:
        return resp.json()
    except Exception:
        preview = (resp.text or "")[:200].replace("\n", " ")
        print("Non-JSON response:", preview)
        return None


def norm(s: str) -> str:
    return (s or "").strip().lower()


def parse_event_dt_utc(date_str: str, time_str: str):
    if not date_str:
        return None

    if not time_str:
        time_str = "00:00:00"

    try:
        dt = datetime.strptime(
            "{} {}".format(date_str, time_str),
            "%Y-%m-%d %H:%M:%S"
        )
    except ValueError:
        try:
            dt = datetime.strptime(
                "{} {}".format(date_str, time_str),
                "%Y-%m-%d %H:%M"
            )
        except Exception:
            return None

    return UTC.localize(dt)
    # =========================
# PREDICTION MODEL (ELO-ish)
# =========================
def win_prob_from_elo(elo_a: float, elo_b: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((elo_b - elo_a) / 400.0))


def probs_1x2(elo_home: float, elo_away: float):
    # Home advantage baked in
    p_home_raw = win_prob_from_elo(elo_home + HOME_ADV_ELO, elo_away)

    # Draw probability decays as teams are further apart
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
    if not ratings or not team_name:
        return None

    if team_name in ratings:
        return ratings[team_name]

    low = norm(team_name)
    for k, v in ratings.items():
        if norm(k) == low:
            return v

    return None


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
# SPORTSD B PROVIDER
# =========================
def sportsdb_get(endpoint: str, params: dict | None = None):
    url = "https://www.thesportsdb.com/api/v1/json/{}/{}".format(SPORTSDB_API_KEY, endpoint)
    r = requests.get(url, params=params, timeout=25)
    if r.status_code != 200:
        print("SportsDB HTTP", r.status_code, "URL:", r.url)
        return None
    return get_json_or_none(r)


def sportsdb_resolve_league_meta(league_name: str):
    """
    Returns (league_id, canon_league_name_for_eventsday)
    We try to resolve from search_all_leagues, fallback to known IDs + name.
    """
    fallback_id = SPORTSDB_LEAGUE_ID_FALLBACK.get(league_name, 0)

    # eventsday often likes: "English Premier League" / "English League Championship"
    fallback_canon = None
    if league_name == "Premier League":
        fallback_canon = "English Premier League"
    elif league_name == "EFL Championship":
        fallback_canon = "English League Championship"

    try:
        data = sportsdb_get("search_all_leagues.php", params={"c": "England", "s": "Soccer"})
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
                canon = item.get("strLeague") or fallback_canon
                if lid:
                    return lid, canon

    except Exception as e:
        print("League resolve failed:", type(e).__name__, str(e))

    return fallback_id, fallback_canon


def sportsdb_fetch_table_ratings(league_id: int):
    """
    Convert table standings to a stable rating:
    Elo-ish = 1500 + (ppg - avg_ppg)*420 + (gdpg - avg_gdpg)*65, clamped.
    """
    data = sportsdb_get("lookuptable.php", params={"l": str(league_id)})
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


def sportsdb_fetch_events_day(date_yyyy_mm_dd: str, league_canon_name: str | None):
    """
    Pull all soccer matches for a day, optionally filtered by league name.
    """
    params = {"d": date_yyyy_mm_dd, "s": "Soccer"}
    if league_canon_name:
        params["l"] = league_canon_name

    data = sportsdb_get("eventsday.php", params=params)
    return (data or {}).get("events") or []
    # =========================
# STATUS / BUCKETING
# =========================
def is_finished(ev: dict) -> bool:
    status = norm(ev.get("strStatus") or "")
    finished_markers = {"match finished", "ft", "finished", "aet", "pen"}
    if status in finished_markers:
        return True

    # If status is empty, but scores exist, treat as finished.
    return (ev.get("intHomeScore") is not None) and (ev.get("intAwayScore") is not None)


def is_live_now(ev: dict, now_utc: datetime) -> bool:
    """
    Live if kickoff <= now <= kickoff + (90+HT+stoppage) approx, OR status indicates live.
    We do not depend on reliable "minute" fields (SportsDB can be inconsistent).
    """
    status = norm(ev.get("strStatus") or "")
    live_markers = {"in play", "live", "1h", "2h", "ht", "1st half", "2nd half"}
    if status in live_markers:
        return True

    dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
    if not dt_utc:
        return False

    # Consider live if started within the last LIVE_GRACE_MINUTES and not finished.
    grace_start = now_utc - timedelta(minutes=LIVE_GRACE_MINUTES)
    if grace_start <= dt_utc <= now_utc and (not is_finished(ev)):
        return True

    return False


# =========================
# CORE COLLECTOR
# =========================
def collect_matches_for_range(leagues_meta: list,
                              dates: list,
                              now_utc: datetime,
                              upcoming_start_utc: datetime,
                              upcoming_end_utc: datetime,
                              results_start_utc: datetime,
                              results_end_utc: datetime):
    """
    Returns:
      upcoming_rows, live_rows, result_rows, eval_rows, all_csv_rows, correct_eval, total_eval
    """
    upcoming_rows = []
    live_rows = []
    result_rows = []
    eval_rows = []
    all_csv_rows = []

    correct_eval = 0
    total_eval = 0

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
                # Safety filter if league filtering fails
                ev_league_id = safe_int(ev.get("idLeague"))
                if ev_league_id and ev_league_id != league_id:
                    continue

                ev_id = str(ev.get("idEvent") or "")
                if ev_id and ev_id in seen_event_ids:
                    continue
                if ev_id:
                    seen_event_ids.add(ev_id)

                dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
                if not dt_utc:
                    continue

                dt_london_str = dt_utc.astimezone(LONDON).strftime("%Y-%m-%d %H:%M")

                home = ev.get("strHomeTeam") or ""
                away = ev.get("strAwayTeam") or ""

                elo_h = find_team_rating(ratings, home) or 1500.0
                elo_a = find_team_rating(ratings, away) or 1500.0

                p_home, p_draw, p_away = probs_1x2(elo_h, elo_a)
                pick = pick_from_probs(p_home, p_draw, p_away)

                hg_raw = ev.get("intHomeScore")
                ag_raw = ev.get("intAwayScore")
                hg = None if hg_raw in (None, "") else safe_int(hg_raw)
                ag = None if ag_raw in (None, "") else safe_int(ag_raw)

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

                # RESULT/EVAL within results window
                if is_finished(ev) and hg is not None and ag is not None:
                    if results_start_utc <= dt_utc <= results_end_utc:
                        actual = actual_outcome(hg, ag)
                        hit = (pick == actual)
                        total_eval += 1
                        if hit:
                            correct_eval += 1

                        r = dict(base_row)
                        r["type"] = "RESULT"
                        r["score"] = "{}-{}".format(hg, ag)
                        r["actual"] = actual
                        r["hit"] = "YES" if hit else "NO"

                        result_rows.append(r)
                        eval_rows.append(r)
                        all_csv_rows.append(r)
                    continue

                # LIVE right now
                if is_live_now(ev, now_utc):
                    r = dict(base_row)
                    r["type"] = "LIVE"
                    r["score"] = "" if (hg is None or ag is None) else "{}-{}".format(hg, ag)
                    live_rows.append(r)
                    all_csv_rows.append(r)
                    continue

                # UPCOMING within upcoming window
                if upcoming_start_utc <= dt_utc <= upcoming_end_utc:
                    r = dict(base_row)
                    r["type"] = "UPCOMING"
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

    return upcoming_rows, live_rows, result_rows, eval_rows, all_csv_rows, correct_eval, total_eval
 # =========================
# STATUS / BUCKETING
# =========================
def is_finished(ev: dict) -> bool:
    status = norm(ev.get("strStatus") or "")
    finished_markers = {"match finished", "ft", "finished", "aet", "pen"}
    if status in finished_markers:
        return True

    # If status is empty, but scores exist, treat as finished.
    return (ev.get("intHomeScore") is not None) and (ev.get("intAwayScore") is not None)


def is_live_now(ev: dict, now_utc: datetime) -> bool:
    """
    Live if kickoff <= now <= kickoff + (90+HT+stoppage) approx, OR status indicates live.
    We do not depend on reliable "minute" fields (SportsDB can be inconsistent).
    """
    status = norm(ev.get("strStatus") or "")
    live_markers = {"in play", "live", "1h", "2h", "ht", "1st half", "2nd half"}
    if status in live_markers:
        return True

    dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
    if not dt_utc:
        return False

    # Consider live if started within the last LIVE_GRACE_MINUTES and not finished.
    grace_start = now_utc - timedelta(minutes=LIVE_GRACE_MINUTES)
    if grace_start <= dt_utc <= now_utc and (not is_finished(ev)):
        return True

    return False


# =========================
# CORE COLLECTOR
# =========================
def collect_matches_for_range(leagues_meta: list,
                              dates: list,
                              now_utc: datetime,
                              upcoming_start_utc: datetime,
                              upcoming_end_utc: datetime,
                              results_start_utc: datetime,
                              results_end_utc: datetime):
    """
    Returns:
      upcoming_rows, live_rows, result_rows, eval_rows, all_csv_rows, correct_eval, total_eval
    """
    upcoming_rows = []
    live_rows = []
    result_rows = []
    eval_rows = []
    all_csv_rows = []

    correct_eval = 0
    total_eval = 0

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
                # Safety filter if league filtering fails
                ev_league_id = safe_int(ev.get("idLeague"))
                if ev_league_id and ev_league_id != league_id:
                    continue

                ev_id = str(ev.get("idEvent") or "")
                if ev_id and ev_id in seen_event_ids:
                    continue
                if ev_id:
                    seen_event_ids.add(ev_id)

                dt_utc = parse_event_dt_utc(ev.get("dateEvent"), ev.get("strTime"))
                if not dt_utc:
                    continue

                dt_london_str = dt_utc.astimezone(LONDON).strftime("%Y-%m-%d %H:%M")

                home = ev.get("strHomeTeam") or ""
                away = ev.get("strAwayTeam") or ""

                elo_h = find_team_rating(ratings, home) or 1500.0
                elo_a = find_team_rating(ratings, away) or 1500.0

                p_home, p_draw, p_away = probs_1x2(elo_h, elo_a)
                pick = pick_from_probs(p_home, p_draw, p_away)

                hg_raw = ev.get("intHomeScore")
                ag_raw = ev.get("intAwayScore")
                hg = None if hg_raw in (None, "") else safe_int(hg_raw)
                ag = None if ag_raw in (None, "") else safe_int(ag_raw)

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

                # RESULT/EVAL within results window
                if is_finished(ev) and hg is not None and ag is not None:
                    if results_start_utc <= dt_utc <= results_end_utc:
                        actual = actual_outcome(hg, ag)
                        hit = (pick == actual)
                        total_eval += 1
                        if hit:
                            correct_eval += 1

                        r = dict(base_row)
                        r["type"] = "RESULT"
                        r["score"] = "{}-{}".format(hg, ag)
                        r["actual"] = actual
                        r["hit"] = "YES" if hit else "NO"

                        result_rows.append(r)
                        eval_rows.append(r)
                        all_csv_rows.append(r)
                    continue

                # LIVE right now
                if is_live_now(ev, now_utc):
                    r = dict(base_row)
                    r["type"] = "LIVE"
                    r["score"] = "" if (hg is None or ag is None) else "{}-{}".format(hg, ag)
                    live_rows.append(r)
                    all_csv_rows.append(r)
                    continue

                # UPCOMING within upcoming window
                if upcoming_start_utc <= dt_utc <= upcoming_end_utc:
                    r = dict(base_row)
                    r["type"] = "UPCOMING"
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

    return upcoming_rows, live_rows, result_rows, eval_rows, all_csv_rows, correct_eval, total_eval 
# =========================
# MAIN
# =========================
def main():
    now_london = datetime.now(LONDON)
    now_utc = now_london.astimezone(UTC)
    wd = now_london.weekday()
    header_lines = [
        "MatchBot C4 - Thu-Sun + Live + Eval + Fallback (ASCII)",
        "Run (London): {}".format(now_london.strftime("%Y-%m-%d %H:%M")),
        "Deploy: {}".format(DEPLOY_MARKER),
        ""
    ]

    # Heartbeat outside Thu-Sun
    if wd not in RUN_DAYS:
        msg = "\n".join(header_lines + [
            "Today is outside Thu-Sun. Script is skipping main run.",
        ])
        send_telegram_chunks(msg)
        print("Skip outside Thu-Sun")
        return

    # Thu-Sun window for this run
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

    # Results window: look back X hours (covers late kickoffs)
    results_start_utc = (now_utc - timedelta(hours=RESULTS_LOOKBACK_HOURS))
    results_end_utc = now_utc

    # Upcoming window: now -> end of Sunday
    window_end_london = LONDON.localize(datetime.combine(end_sun_date, dtime(23, 59, 59)))
    upcoming_start_utc = now_utc
    upcoming_end_utc = window_end_london.astimezone(UTC)

    upcoming_rows, live_rows, result_rows, eval_rows, all_csv_rows, correct_eval, total_eval = collect_matches_for_range(
        leagues_meta=leagues_meta,
        dates=dates,
        now_utc=now_utc,
        upcoming_start_utc=upcoming_start_utc,
        upcoming_end_utc=upcoming_end_utc,
        results_start_utc=results_start_utc,
        results_end_utc=results_end_utc,
    )

    fallback_note = None

    # Fallback: if nothing at all in Thu-Sun window, show next 7 days and last 48h results
    if FALLBACK_IF_EMPTY and (not upcoming_rows) and (not live_rows) and (not result_rows):
        fb_end_london = now_london + timedelta(days=FALLBACK_UPCOMING_DAYS)
        fb_end_utc = fb_end_london.astimezone(UTC)

        fb_results_start_utc = now_utc - timedelta(hours=FALLBACK_RESULTS_HOURS)
        fb_results_end_utc = now_utc

        # Days list for fallback period
        fb_dates = []
        d = now_london.date()
        while d <= fb_end_london.date():
            fb_dates.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)

        upcoming_rows, live_rows, result_rows, eval_rows, all_csv_rows, correct_eval, total_eval = collect_matches_for_range(
            leagues_meta=leagues_meta,
            dates=fb_dates,
            now_utc=now_utc,
            upcoming_start_utc=now_utc,
            upcoming_end_utc=fb_end_utc,
            results_start_utc=fb_results_start_utc,
            results_end_utc=fb_results_end_utc,
        )

        fallback_note = "Fallback active: no matches in Thu-Sun window. Showing next {} days + last {}h results.".format(
            FALLBACK_UPCOMING_DAYS, FALLBACK_RESULTS_HOURS
        )

    # Build Telegram text
    lines = list(header_lines)
    lines.append("Window: Thu {} -> Sun {} (London)".format(start_thu_date, end_sun_date))
    lines.append("Leagues: {}".format(", ".join([l["name"] for l in leagues_meta if l["id"]])))
    lines.append("")

    if fallback_note:
        lines.append(fallback_note)
        lines.append("")

    # RESULTS
    lines.append("RESULTS (last {}h):".format(RESULTS_LOOKBACK_HOURS if not fallback_note else FALLBACK_RESULTS_HOURS))
    if not result_rows:
        lines.append("- None found.")
    else:
        for r in result_rows[:40]:
            lines.append("- {} {} - {} {} {}".format(
                r["kickoff_london"], r["league"], r["home"], r["score"], r["away"]
            ))

    # EVAL
    lines.append("")
    lines.append("EVALUATION (pick vs actual):")
    if total_eval == 0:
        lines.append("- No completed matches to evaluate.")
    else:
        acc = (correct_eval / float(total_eval)) * 100.0
        lines.append("- Accuracy: {}/{} = {:.1f}%".format(correct_eval, total_eval, acc))
        lines.append("Last 10 evaluated:")
        for r in eval_rows[-10:]:
            lines.append("- {} {} - {} {} {} | Pick:{} Actual:{} Hit:{}".format(
                r["kickoff_london"], r["league"], r["home"], r["score"], r["away"],
                r.get("pick", "?"), r.get("actual", "?"), r.get("hit", "?")
            ))

    # LIVE
    lines.append("")
    lines.append("LIVE (matches happening now):")
    if not live_rows:
        lines.append("- None live right now (or provider did not flag).")
    else:
        for r in live_rows[:40]:
            h = int(r["p_home"] * 100)
            d_ = int(r["p_draw"] * 100)
            a = int(r["p_away"] * 100)
            score = r.get("score") or "?"
            lines.append("- {} {} - {} vs {} (Score:{}) | H:{} D:{} A:{} | Pick:{}".format(
                r["kickoff_london"], r["league"], r["home"], r["away"], score, h, d_, a, r["pick"]
            ))

    # UPCOMING
    lines.append("")
    lines.append("UPCOMING:")
    if not upcoming_rows:
        lines.append("- None upcoming in window.")
    else:
        for r in upcoming_rows[:80]:
            h = int(r["p_home"] * 100)
            d_ = int(r["p_draw"] * 100)
            a = int(r["p_away"] * 100)
            lines.append("- {} {} - {} vs {} | H:{} D:{} A:{} | Pick:{}".format(
                r["kickoff_london"], r["league"], r["home"], r["away"], h, d_, a, r["pick"]
            ))

    send_telegram_chunks("\n".join(lines))

    # CSV output (ephemeral on Render)
    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = now_london.strftime("%Y-%m-%d_%H%M")
    csv_path = os.path.join(OUT_DIR, "{}_{}.csv".format(CSV_PREFIX, stamp))
    write_csv(all_csv_rows, csv_path)
    print("CSV written:", csv_path)
    print("MatchBot finished successfully")


if __name__ == "__main__":
    main()
