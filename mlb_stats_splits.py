
"""
mlb_stats_splits.py - Steps 2 & 3 via MLB Stats API game logs.
Replaces statmuse_fetch.py - no scraping, no credentials needed.

Step 2: Last 10 H/A game logs vs today's specific opponent (up to 3 seasons back).
Step 3: Last 10 H/A game logs in the current season (all opponents).
Both require min 3 qualifying AB-games; return None ba if insufficient.
"""
import requests
import time
from datetime import date
from concurrent.futures import ThreadPoolExecutor

# ── Module-level cache: (player_id, season) → splits list ──────────
_CACHE: dict = {}


def _get_game_logs(player_id: int, season: int) -> list:
    """Fetch and cache hitting game-log splits for one player/season."""
    key = (player_id, season)
    if key in _CACHE:
        return _CACHE[key]
    for attempt in range(3):
        try:
            r = requests.get(
                f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats",
                params={"stats": "gameLog", "season": season,
                        "group": "hitting", "gameType": "R"},
                timeout=12,
            )
            r.raise_for_status()
            splits = r.json().get("stats", [{}])[0].get("splits", [])
            _CACHE[key] = splits
            return splits
        except Exception:
            if attempt < 2:
                time.sleep(0.4)
    _CACHE[key] = []
    return []


def _team_name_match(api_name: str, espn_name: str) -> bool:
    a = api_name.lower().strip()
    b = espn_name.lower().strip()
    if a == b:
        return True
    a_last = a.split()[-1] if a else ""
    b_last = b.split()[-1] if b else ""
    if a_last and a_last == b_last:
        return True
    return (a in b) or (b in a)


def _build_result(hits: int, ab: int, games: int, min_games: int = 3) -> dict:
    if games < min_games or ab == 0:
        return {"ba": None, "display": f"N/A ({games}G)", "flag": "❌ skip",
                "ab": ab, "h": hits, "games": games}
    ba = hits / ab
    display = f".{int(ba * 1000):03d} ({games}G)"
    return {"ba": ba, "display": display, "flag": f"✅ {display}",
            "ab": ab, "h": hits, "games": games}


def prefetch_game_logs(player_ids: list, seasons: list = None):
    """Pre-fetch and cache game logs for multiple players concurrently."""
    if seasons is None:
        current_year = date.today().year
        seasons = list(range(current_year, current_year - 5, -1))
    tasks = [(pid, s) for pid in player_ids if pid
             for s in seasons if (pid, s) not in _CACHE]
    if not tasks:
        return
    def _fetch_one(args):
        _get_game_logs(args[0], args[1])
    with ThreadPoolExecutor(max_workers=30) as ex:
        list(ex.map(_fetch_one, tasks))


def fetch_step2_ba(player_id, side: str, opp_name: str,
                   max_games: int = 10, min_games: int = 3) -> dict:
    """Step 2 — Last 10 H/A game logs vs today's opponent (5 seasons back)."""
    if not player_id:
        return {"ba": None, "display": "N/A", "flag": "❌ skip",
                "ab": 0, "h": 0, "games": 0}
    current_year = date.today().year
    seasons = list(range(current_year, current_year - 5, -1))
    matching = []
    for season in seasons:
        splits = _get_game_logs(player_id, season)
        for sp in reversed(splits):
            is_home = sp.get("isHome", False)
            if (side.upper() == "HOME") != is_home:
                continue
            opp = sp.get("opponent", {}).get("name", "")
            if not _team_name_match(opp, opp_name):
                continue
            stat = sp.get("stat", {})
            ab = int(stat.get("atBats", 0) or 0)
            h  = int(stat.get("hits",   0) or 0)
            if ab < 1:
                continue
            matching.append({"h": h, "ab": ab})
            if len(matching) >= max_games:
                break
        if len(matching) >= max_games:
            break
    return _build_result(sum(g["h"] for g in matching),
                         sum(g["ab"] for g in matching), len(matching), min_games)


def fetch_step3_ba(player_id, side: str, season: int = None,
                   max_games: int = 10, min_games: int = 3) -> dict:
    """Step 3 — Last 10 H/A game logs in current season (all opponents)."""
    if not player_id:
        return {"ba": None, "display": "N/A", "flag": "❌ skip",
                "ab": 0, "h": 0, "games": 0}
    if season is None:
        season = date.today().year
    splits = _get_game_logs(player_id, season)
    matching = []
    for sp in reversed(splits):
        is_home = sp.get("isHome", False)
        if (side.upper() == "HOME") != is_home:
            continue
        stat = sp.get("stat", {})
        ab = int(stat.get("atBats", 0) or 0)
        h  = int(stat.get("hits",   0) or 0)
        if ab < 1:
            continue
        matching.append({"h": h, "ab": ab})
        if len(matching) >= max_games:
            break
    return _build_result(sum(g["h"] for g in matching),
                         sum(g["ab"] for g in matching), len(matching), min_games)
