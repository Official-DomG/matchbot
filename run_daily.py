import os
import math
import requests
import pytz
from datetime import datetime, date

# ----------------------------
# Config
# ----------------------------
DEPLOY_MARKER = "V300-AF+SPORTSDB-THU-SUN"

LONDON = pytz.timezone("Europe/London")
UTC = pytz.utc

# Leagues we care about (names used for filtering SportsDB "eventsday")
LEAGUES = [
    ("Premier League", 39),        # API-Football league id
    ("EFL Championship", 40),      # API-Football league id
]

# Run only on Thu/Fri/Sat/Sun (London local)
# Python weekday: Mon=0 ... Sun=6
ALLOWED_WEEKDAYS = {3, 4, 5, 6}

# Simple 1X2 model tuning
HOME_ADV_ELO = 55
DRAW_BASE = 0.24
DRAW_TIGHTNESS = 260.0

TELEGRAM_MAX_LEN = 3800

# ----------------------------
# Telegram
# ----------------------------
def send_telegram_message(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram not configured (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID).")
        print(text)
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    # Split long messages safely
    chunks = []
    t = (text or "").strip()
    while len(t) > TELEGRAM_MAX_LEN:
        cut = t.rfind("\n", 0, TELEGRAM_MAX_LEN)
        if cut == -1:
            cut = TELEGRAM_MAX_LEN
        chunks.append(t[:cut].strip())
        t = t[cut:].strip()
    if t:
        chunks.append(t)

    for i, chunk in enumerate(chunks, start=1):
        payload = {"chat_id": chat_id, "text": chunk, "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=25)
        print(f"Telegram chunk {i}/{len(chunks)}:", r.status_code, r.text[:200])

# ----------------------------
# Model
# ----------------------------
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

def pct(x: float) -> str:
    return f"{int(round(x * 100))}%"

# ----------------------------
# API-Football (RapidAPI)
# ----------------------------
RAPIDAPI_HOST = os.environ.get("RAPIDAPI_HOST", "api-football-v1.p.rapidapi.com")
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY")
_SEASON_CACHE = {}

def af_headers() -> dict:
    if not RAPIDAPI_KEY:
        raise RuntimeError("Missing RAPIDAPI_KEY env var")
    return {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }

def api_get(path: str, params: dict) -> dict:
    url = f"https://{RAPIDAPI_HOST}/v3{path}"
    r = requests.get(url, headers=af_headers(), params=params, timeout=30)
    if r.status_code != 200:
        preview = (r.text or "")[:250].replace("\n", " ")
        raise RuntimeError(f"API-Football failed {r.status_code} {path}: {preview}")
    return r.json()

def get_current_season(league_id: int) -> int:
    if league_id in _SEASON_CACHE:
        return _SEASON_CACHE[league_id]

    data = api_get("/leagues", {"id": league_id})
    resp = data.get("response") or []
    if not resp:
        raise RuntimeError(f"No league data for league id {league_id}")

    seasons = (resp[0].get("seasons") or [])
    current = next((s for s in seasons if s.get("current") is True), None)

    if current and current.get("year"):
        season_year = int(current["year"])
    else:
        years = [s.get("year") for s in seasons if s.get("year")]
        if not years:
            raise RuntimeError(f"Could not determine season for league id {league_id}")
        season_year = int(max(years))

    _SEASON_CACHE[league_id] = season_year
    return season_year

def fetch_fixtures_api_football(league_id: int, season: int, target_ymd: str):
    data = api_get("/fixtures", {"league": league_id, "season": season, "date": target_ymd, "timezone": "UTC"})
    resp = data.get("response") or []

    out = []
    for item in resp:
        fx = item.get("fixture") or {}
        teams = item.get("teams") or {}
        goals = item.get("goals") or {}

        kickoff_iso = fx.get("date")
        if not kickoff_iso:
            continue

        kickoff_utc = datetime.fromisoformat(kickoff_iso.replace("Z", "+00:00")).astimezone(UTC)
        kickoff_london = kickoff_utc.astimezone(LONDON)

        status = (fx.get("status") or {}).get("short") or ""
        home = ((teams.get("home") or {}).get("name")) or ""
        away = ((teams.get("away") or {}).get("name")) or ""

        out.append({
            "kickoff_utc": kickoff_utc,
            "kickoff_london": kickoff_london,
            "home": home,
            "away": away,
            "status": status,
            "home_goals": goals.get("home"),
            "away_goals": goals.get("away"),
            "source": "API-Football",
        })

    out.sort(key=lambda x: x["kickoff_utc"])
    return out

def fetch_standings_ratings_api_football(league_id: int, season: int) -> dict:
    data = api_get("/standings", {"league": league_id, "season": season})
    resp = data.get("response") or []
    if not resp:
        return {}

    league = resp[0].get("league") or {}
    standings = league.get("standings") or []
    if not standings or not standings[0]:
        return {}

    rows = []
    for row in standings[0]:
        team = ((row.get("team") or {}).get("name")) or ""
        all_stats = row.get("all") or {}
        played = int(all_stats.get("played") or 0)
        points = int(row.get("points") or 0)
        gd = int(row.get("goalsDiff") or 0)
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
        ratings[team.strip().lower()] = float(elo)

    return ratings

def find_team_rating(ratings: dict, team_name: str):
    if not team_name:
        return None
    return ratings.get(team_name.strip().lower())

# ----------------------------
# SportsDB (fallback)
# ----------------------------
SPORTSDB_KEY = os.environ.get("SPORTSDB_KEY", "1")  # if you have a better key, set it in Render
SPORTSDB_BASE = "https://www.thesportsdb.com/api/v1/json"

def sportsdb_get(path: str, params: dict) -> dict | None:
    url = f"{SPORTSDB_BASE}/{SPORTSDB_KEY}/{path}"
    try:
        r = requests.get(url, params=params, timeout=25)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def fetch_fixtures_sportsdb_for_date(target_ymd: str, league_name: str):
    """
    Uses eventsday.php to pull all soccer events for the day, then filters by strLeague.
    This is a fallback only.
    """
    data = sportsdb_get("eventsday.php", {"d": target_ymd, "s": "Soccer"})
    if not data:
        return []

    events = data.get("events") or []
    out = []
    for ev in events:
        if (ev.get("strLeague") or "").strip().lower() != league_name.strip().lower():
            continue

        home = ev.get("strHomeTeam") or ""
        away = ev.get("strAwayTeam") or ""

        # SportsDB uses dateEvent + strTime (often UTC-ish); treat as UTC if present
        date_str = ev.get("dateEvent")
        time_str = ev.get("strTime") or "00:00:00"
        try:
            kickoff_utc = UTC.localize(datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S"))
        except Exception:
            continue

        kickoff_london = kickoff_utc.astimezone(LONDON)

        # Scores may exist if played
        hg = ev.get("intHomeScore")
        ag = ev.get("intAwayScore")

        # status approximation
        status = "FT" if (hg is not None and ag is not None) else "NS"

        out.append({
            "kickoff_utc": kickoff_utc,
            "kickoff_london": kickoff_london,
            "home": home,
            "away": away,
            "status": status,
            "home_goals": hg,
            "away_goals": ag,
            "source": "SportsDB",
        })

    out.sort(key=lambda x: x["kickoff_utc"])
    return out

# ----------------------------
# Formatting
# ----------------------------
def fmt_score(status: str, hg, ag) -> str:
    if hg is None and ag is None:
        return ""
    if status and status != "NS":
        return f" [{hg}-{ag} {status}]"
    return f" [{hg}-{ag}]"

# ----------------------------
# Main
# ----------------------------
def main():
    now_london = datetime.now(LONDON)

    # Only run Thu/Fri/Sat/Sun
    if now_london.weekday() not in ALLOWED_WEEKDAYS:
        print(f"Skipping run (weekday {now_london.weekday()} not allowed).")
        return

    target_ymd = now_london.strftime("%Y-%m-%d")

    msg_lines = [
        "MatchBot — Fixtures & Results (Prem + Championship)",
        f"Date (London): {target_ymd}",
        f"Run (London): {now_london.strftime('%Y-%m-%d %H:%M')}",
        f"Deploy: {DEPLOY_MARKER}",
        "",
    ]

    any_found = 0

    try:
        for comp_name, league_id in LEAGUES:
            # Primary: API-Football
            fixtures = []
            ratings = {}

            api_ok = False
            try:
                season = get_current_season(league_id)
                ratings = fetch_standings_ratings_api_football(league_id, season)
                fixtures = fetch_fixtures_api_football(league_id, season, target_ymd)
                api_ok = True
            except Exception as e:
                print(f"API-Football failed for {comp_name}: {type(e).__name__}: {e}")

            # Fallback: SportsDB (fixtures only)
            if not fixtures:
                fixtures = fetch_fixtures_sportsdb_for_date(target_ymd, comp_name)

            msg_lines.append(f"{comp_name}")
            msg_lines.append(f"Source: {'API-Football' if api_ok else 'SportsDB / mixed'}")

            if not fixtures:
                msg_lines.append("No fixtures found.")
                msg_lines.append("")
                continue

            for fx in fixtures:
                home = fx["home"]
                away = fx["away"]
                t_str = fx["kickoff_london"].strftime("%H:%M")
                status = fx["status"]
                score_txt = fmt_score(status, fx["home_goals"], fx["away_goals"])

                elo_h = find_team_rating(ratings, home) or 1500.0
                elo_a = find_team_rating(ratings, away) or 1500.0
                p_home, p_draw, p_away = probs_1x2(elo_h, elo_a)

                msg_lines.append(
                    f"{t_str} — {home} vs {away}{score_txt} | "
                    f"H:{pct(p_home)} D:{pct(p_draw)} A:{pct(p_away)}"
                )
                any_found += 1

            msg_lines.append("")

        if any_found == 0:
            msg_lines.append("No fixtures found today for the selected leagues.")

        send_telegram_message("\n".join(msg_lines).strip())

    except Exception as e:
        err = f"MatchBot crashed ❌\n{type(e).__name__}: {e}"
        print(err)
        send_telegram_message(err)
        raise

if __name__ == "__main__":
    main()
