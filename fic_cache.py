"""
fic_cache.py — Step 1: Two-source player pool builder.

SOURCE 1: MLB Stats API — batters with career BA >= .250 (min 4 AB) vs today's probable pitcher.
SOURCE 2: Baseball Musings Hot Streaks — batters currently on hit streaks (streak BA used as S1).

Both sources are merged and deduplicated before passing to Steps 2, 3, 4.
"""
import json, os, time, re, requests
from datetime import date as _date
from bs4 import BeautifulSoup

CACHE_DIR = os.environ.get("CACHE_DIR", "/tmp")
MLB_API   = "https://statsapi.mlb.com/api/v1"
BM_URL    = "https://www.baseballmusings.com/cgi-bin/CurStreak.py"
BM_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36"}


def _cache_path(run_date: str) -> str:
    return os.path.join(CACHE_DIR, f"fic_step1_{run_date.replace('-','')}.json")


def _short_name(full_name: str) -> str:
    parts = full_name.strip().split()
    if not parts:
        return full_name
    return f"{parts[0][0]}. {' '.join(parts[1:])}"


def _parse_avg(avg_str) -> float:
    try:
        s = str(avg_str or "0").strip()
        if s in ("", "-.--", "-.-", "---"):
            return 0.0
        return float(f"0{s}") if s.startswith(".") else float(s)
    except (ValueError, TypeError):
        return 0.0


# ── SOURCE 1: MLB Stats API ───────────────────────────────────────────

def _get_schedule_with_pitchers(run_date: str) -> list:
    r = requests.get(f"{MLB_API}/schedule",
        params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"},
        timeout=15)
    matchups = []
    for date_data in r.json().get("dates", []):
        for game in date_data.get("games", []):
            home   = game["teams"]["home"]
            away   = game["teams"]["away"]
            home_p = home.get("probablePitcher", {})
            away_p = away.get("probablePitcher", {})
            if home_p and away_p:
                matchups.append({"team_id": away["team"]["id"], "team_name": away["team"]["name"],
                                 "pitcher_id": home_p["id"], "pitcher_name": home_p["fullName"],
                                 "pitcher_short": _short_name(home_p["fullName"])})
                matchups.append({"team_id": home["team"]["id"], "team_name": home["team"]["name"],
                                 "pitcher_id": away_p["id"], "pitcher_name": away_p["fullName"],
                                 "pitcher_short": _short_name(away_p["fullName"])})
    return matchups


def _get_position_players(team_id: int) -> list:
    r = requests.get(f"{MLB_API}/teams/{team_id}/roster",
        params={"rosterType": "active"}, timeout=10)
    return [p for p in r.json().get("roster", []) if p.get("position", {}).get("code") != "1"]


def _get_career_vs_pitcher(batter_id: int, pitcher_id: int) -> dict:
    r = requests.get(f"{MLB_API}/people/{batter_id}/stats",
        params={"stats": "vsPlayerTotal", "group": "hitting", "opposingPlayerId": pitcher_id},
        timeout=8)
    for sg in r.json().get("stats", []):
        if "vsPlayer" in sg.get("type", {}).get("displayName", ""):
            for sp in sg.get("splits", []):
                s = sp.get("stat", {})
                return {"ab": s.get("atBats", 0), "hits": s.get("hits", 0),
                        "hr": s.get("homeRuns", 0), "ba": _parse_avg(s.get("avg"))}
    return {"ab": 0, "hits": 0, "hr": 0, "ba": 0.0}


def _get_mlb_api_players(run_date, min_ab, min_ba, emit):
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 1: MLB Stats API — career BA vs today's pitchers...")
    matchups = _get_schedule_with_pitchers(run_date)
    log(f"   {len(matchups)//2} games with probable pitchers")

    results = []
    seen = set()

    for m in matchups:
        log(f"   Scanning {m['team_name']} vs {m['pitcher_name']}...")
        try:
            roster = _get_position_players(m["team_id"])
        except Exception:
            continue
        time.sleep(0.1)

        for player in roster:
            batter_id   = player["person"]["id"]
            batter_name = player["person"]["fullName"]
            pos         = player.get("position", {}).get("abbreviation", "")
            key         = (batter_id, m["pitcher_id"])
            if key in seen:
                continue
            seen.add(key)
            try:
                stats = _get_career_vs_pitcher(batter_id, m["pitcher_id"])
            except Exception:
                time.sleep(0.2)
                continue
            if stats["ab"] >= min_ab and stats["ba"] >= min_ba:
                results.append({"batter": _short_name(batter_name), "pos": pos,
                                 "pitcher": m["pitcher_short"], "ab": stats["ab"],
                                 "h": stats["hits"], "hr": stats["hr"], "ba": stats["ba"],
                                 "source": "mlb_api"})
            time.sleep(0.08)

    log(f"✅ Source 1: {len(results)} players from MLB Stats API")
    return results


# ── SOURCE 2: Baseball Musings Hot Streaks ────────────────────────────

def _get_bm_players(run_date, emit):
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 2: Baseball Musings Hot Streaks...")

    # Get today's schedule for pitcher lookup
    matchups_by_team = {}  # team_id -> pitcher info
    team_id_cache    = {}  # player_id -> team_id
    try:
        r = requests.get(f"{MLB_API}/schedule",
            params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"},
            timeout=15)
        for date_data in r.json().get("dates", []):
            for game in date_data.get("games", []):
                home   = game["teams"]["home"]
                away   = game["teams"]["away"]
                home_p = home.get("probablePitcher", {})
                away_p = away.get("probablePitcher", {})
                if home_p:
                    matchups_by_team[away["team"]["id"]] = _short_name(home_p.get("fullName", ""))
                if away_p:
                    matchups_by_team[home["team"]["id"]] = _short_name(away_p.get("fullName", ""))
    except Exception as e:
        log(f"   ⚠️ Schedule fetch failed: {e}")
        return []

    # Scrape Baseball Musings
    try:
        r = requests.get(BM_URL, headers=BM_HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        tables = soup.find_all("table")
        table = tables[1] if len(tables) > 1 else tables[0]
    except Exception as e:
        log(f"   ⚠️ Baseball Musings fetch failed: {e}")
        return []

    bm_players = []
    for row in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) < 10:
            continue
        full_name = cols[0]
        ba_str    = cols[9]
        try:
            ba = float(f"0{ba_str}") if ba_str.startswith(".") else float(ba_str)
            ab = int(cols[2])
        except (ValueError, IndexError):
            continue
        if ba < 0.250:
            continue
        bm_players.append({"full_name": full_name, "ab_streak": ab, "ba_streak": ba})

    log(f"   {len(bm_players)} hot streak players found on Baseball Musings")

    # Look up each player's team and opposing pitcher
    results = []
    for bm in bm_players:
        full_name = bm["full_name"]
        try:
            r = requests.get(f"{MLB_API}/people/search",
                params={"names": full_name, "sportId": 1}, timeout=8)
            people = r.json().get("people", [])
            matched = [p for p in people if p.get("active") and
                       p.get("fullName","").lower() == full_name.lower()]
            if not matched:
                matched = [p for p in people if p.get("active")]
            if not matched:
                continue

            pid = matched[0]["id"]
            r2  = requests.get(f"{MLB_API}/people/{pid}",
                params={"hydrate": "currentTeam"}, timeout=8)
            info     = r2.json()["people"][0]
            team_id  = info.get("currentTeam", {}).get("id")
            pos      = info.get("primaryPosition", {}).get("abbreviation", "")

            if not team_id or team_id not in matchups_by_team:
                continue  # not playing today

            pitcher_short = matchups_by_team[team_id]
            short         = _short_name(full_name)

            results.append({"batter": short, "pos": pos,
                             "pitcher": pitcher_short, "ab": bm["ab_streak"],
                             "h": int(bm["ab_streak"] * bm["ba_streak"]),
                             "hr": 0, "ba": bm["ba_streak"],
                             "source": "baseball_musings"})
        except Exception:
            pass
        time.sleep(0.1)

    log(f"✅ Source 2: {len(results)} hot streak players playing today")
    return results


# ── MERGE & DEDUPLICATE ───────────────────────────────────────────────

def _merge_players(mlb_players, bm_players):
    """Merge two lists, deduplicating by short batter name (keep higher BA)."""
    merged = {p["batter"]: p for p in mlb_players}
    for p in bm_players:
        name = p["batter"]
        if name not in merged or p["ba"] > merged[name]["ba"]:
            merged[name] = p
    return sorted(merged.values(), key=lambda x: x["ba"], reverse=True)


# ── PUBLIC API ────────────────────────────────────────────────────────

def get_step1_players_or_scrape(run_date=None, min_ab=4, min_ba=0.250, emit=None):
    if run_date is None:
        run_date = _date.today().strftime("%Y-%m-%d")

    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    path = _cache_path(run_date)
    if os.path.exists(path):
        with open(path) as f:
            players = json.load(f)
        log(f"✅ Loaded {len(players)} players from cache")
        return players

    log("🔍 Building Step 1 player pool from 2 sources...")

    # Source 1: MLB Stats API
    mlb_players = _get_mlb_api_players(run_date, min_ab, min_ba, emit)

    # Source 2: Baseball Musings hot streaks
    bm_players  = _get_bm_players(run_date, emit)

    # Merge
    combined = _merge_players(mlb_players, bm_players)

    log(f"✅ Combined pool: {len(combined)} unique players "
        f"({len(mlb_players)} MLB API + {len(bm_players)} hot streaks, deduped)")

    with open(path, "w") as f:
        json.dump(combined, f)

    return combined
