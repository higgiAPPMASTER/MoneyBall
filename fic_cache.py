"""
fic_cache.py — Step 1: Batter vs Pitcher career stats via MLB Stats API.
Replaces Fantasy Info Central (FIC) which is blocked on cloud servers.
Uses official MLB Stats API — free, always available, no IP blocking.
"""
import json, os, time, re, requests
from datetime import date as _date

CACHE_DIR = os.environ.get("CACHE_DIR", "/tmp")
MLB_API   = "https://statsapi.mlb.com/api/v1"


def _cache_path(run_date: str) -> str:
    return os.path.join(CACHE_DIR, f"fic_step1_{run_date.replace('-','')}.json")


def _short_name(full_name: str) -> str:
    """Convert 'Aaron Judge' → 'A. Judge', 'Jazz Chisholm Jr.' → 'J. Chisholm Jr.'"""
    parts = full_name.strip().split()
    if not parts:
        return full_name
    return f"{parts[0][0]}. {' '.join(parts[1:])}"


def _parse_avg(avg_str) -> float:
    """Parse MLB API avg string: '.286' → 0.286, handle None/empty."""
    try:
        s = str(avg_str or "0").strip()
        if s in ("", "-.--", "-.-", "---"):
            return 0.0
        return float(f"0{s}") if s.startswith(".") else float(s)
    except (ValueError, TypeError):
        return 0.0


def _get_schedule_with_pitchers(run_date: str) -> list:
    """Return list of {team_id, team_name, pitcher_id, pitcher_name, pitcher_short} dicts."""
    r = requests.get(
        f"{MLB_API}/schedule",
        params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"},
        timeout=15,
    )
    matchups = []
    for date_data in r.json().get("dates", []):
        for game in date_data.get("games", []):
            home    = game["teams"]["home"]
            away    = game["teams"]["away"]
            home_p  = home.get("probablePitcher", {})
            away_p  = away.get("probablePitcher", {})
            if home_p and away_p:
                matchups.append({
                    "team_id":       away["team"]["id"],
                    "team_name":     away["team"]["name"],
                    "pitcher_id":    home_p["id"],
                    "pitcher_name":  home_p["fullName"],
                    "pitcher_short": _short_name(home_p["fullName"]),
                })
                matchups.append({
                    "team_id":       home["team"]["id"],
                    "team_name":     home["team"]["name"],
                    "pitcher_id":    away_p["id"],
                    "pitcher_name":  away_p["fullName"],
                    "pitcher_short": _short_name(away_p["fullName"]),
                })
    return matchups


def _get_position_players(team_id: int) -> list:
    """Return active roster position players (excludes pitchers)."""
    r = requests.get(
        f"{MLB_API}/teams/{team_id}/roster",
        params={"rosterType": "active"},
        timeout=10,
    )
    return [
        p for p in r.json().get("roster", [])
        if p.get("position", {}).get("code") != "1"  # exclude pitchers
    ]


def _get_career_vs_pitcher(batter_id: int, pitcher_id: int) -> dict:
    """Return {ab, hits, hr, ba} for batter's career stats against pitcher."""
    r = requests.get(
        f"{MLB_API}/people/{batter_id}/stats",
        params={"stats": "vsPlayerTotal", "group": "hitting",
                "opposingPlayerId": pitcher_id},
        timeout=8,
    )
    for sg in r.json().get("stats", []):
        if "vsPlayer" in sg.get("type", {}).get("displayName", ""):
            for sp in sg.get("splits", []):
                s = sp.get("stat", {})
                return {
                    "ab":   s.get("atBats", 0),
                    "hits": s.get("hits",   0),
                    "hr":   s.get("homeRuns", 0),
                    "ba":   _parse_avg(s.get("avg")),
                }
    return {"ab": 0, "hits": 0, "hr": 0, "ba": 0.0}


def get_step1_players_or_scrape(
    run_date:  str   = None,
    min_ab:    int   = 4,
    min_ba:    float = 0.250,
    emit             = None,
) -> list:
    """
    Return list of batters with career BA ≥ min_ba against today's probable
    pitchers, sorted by BA descending.
    Results are cached — MLB API is only called once per day.
    """
    if run_date is None:
        run_date = _date.today().strftime("%Y-%m-%d")

    def log(msg):
        if emit:
            emit({"type": "log", "msg": msg})

    # ── Cache hit ──────────────────────────────────────────────────────
    path = _cache_path(run_date)
    if os.path.exists(path):
        with open(path) as f:
            players = json.load(f)
        log(f"✅ Loaded {len(players)} players from cache")
        return players

    # ── Build fresh data from MLB Stats API ───────────────────────────
    log("⬇️  Fetching today's matchups from MLB Stats API...")
    matchups = _get_schedule_with_pitchers(run_date)
    log(f"   {len(matchups)//2} games with probable pitchers found")

    results  = []
    seen     = set()   # avoid duplicate batter+pitcher combos

    for m in matchups:
        log(f"   Scanning {m['team_name']} batters vs {m['pitcher_name']}...")
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
                results.append({
                    "batter":  _short_name(batter_name),
                    "pos":     pos,
                    "pitcher": m["pitcher_short"],
                    "ab":      stats["ab"],
                    "h":       stats["hits"],
                    "hr":      stats["hr"],
                    "ba":      stats["ba"],
                })

            time.sleep(0.08)

    results.sort(key=lambda x: x["ba"], reverse=True)

    with open(path, "w") as f:
        json.dump(results, f)

    log(f"✅ Found {len(results)} qualifying batters (min {min_ab} AB, min {min_ba:.3f} BA)")
    return results
