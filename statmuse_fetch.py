"""
statmuse_fetch.py — Steps 2 & 3 via MLB Stats API game logs.

STEP 2: Last 10 H/A games vs today's specific opponent (career, all seasons).
STEP 3: Last 10 H/A games in 2026 season (vs any team).

Uses game logs with isHome flag for exact Home/Away split.
Min 3 qualifying games required. Falls back to however many are available.
"""
import time, requests

MLB_API    = "https://statsapi.mlb.com/api/v1"
SEASON     = "2026"
LAST_N     = 10    # use last N qualifying games
MIN_GAMES  = 3     # minimum qualifying games to return a result
S2_SEASONS = [2021, 2022, 2023, 2024, 2025, 2026]  # seasons to search for Step 2


# ── Player & Team ID lookup (cached) ─────────────────────────────────
_player_cache = {}
_team_cache   = {}


def _get_player_id(first: str, last: str) -> int | None:
    key = f"{first}-{last}".lower()
    if key in _player_cache:
        return _player_cache[key]
    try:
        full = f"{first.capitalize()} {last.replace('-', ' ').title()}"
        r = requests.get(f"{MLB_API}/people/search",
            params={"names": full, "sportId": 1}, timeout=8)
        for p in r.json().get("people", []):
            if p.get("active"):
                _player_cache[key] = p["id"]
                return p["id"]
    except Exception:
        pass
    return None


def _get_team_id(opp_slug: str) -> int | None:
    key = opp_slug.lower()
    if key in _team_cache:
        return _team_cache[key]
    try:
        name_guess = opp_slug.replace("-", " ").lower()
        r = requests.get(f"{MLB_API}/teams",
            params={"sportId": 1, "season": SEASON}, timeout=8)
        for team in r.json().get("teams", []):
            team_name = team.get("name", "").lower()
            team_slug = team_name.replace(" ", "-")
            if team_slug == key or name_guess in team_name or team_name in name_guess:
                _team_cache[key] = team["id"]
                return team["id"]
    except Exception:
        pass
    return None


def _get_game_logs(player_id: int, season: int) -> list:
    """Fetch game log for one season. Returns list of split dicts."""
    try:
        r = requests.get(f"{MLB_API}/people/{player_id}/stats",
            params={"stats": "gameLog", "group": "hitting", "season": season},
            timeout=12)
        data = r.json().get("stats", [])
        return data[0].get("splits", []) if data else []
    except Exception:
        return []


def _calc_ba(games: list) -> dict:
    """
    Given a list of game splits (most-recent-first), take the last LAST_N
    qualifying games (AB > 0), sum AB & H, return result dict.
    """
    qualifying = [g for g in games if g.get("stat", {}).get("atBats", 0) > 0]
    recent     = qualifying[:LAST_N]      # last N games

    if len(recent) < MIN_GAMES:
        return None   # not enough data

    total_ab = sum(g["stat"]["atBats"] for g in recent)
    total_h  = sum(g["stat"]["hits"]   for g in recent)

    if total_ab == 0:
        return None

    ba      = round(total_h / total_ab, 3)
    display = f".{int(ba * 1000):03d}"
    flag    = "✅" if len(recent) >= LAST_N else f"✅ ({len(recent)}g)"
    return {"ba": ba, "score_ba": ba, "display": display,
            "flag": flag, "games": len(recent), "ab": total_ab}


# ── Step 2: Last 10 H/A games vs today's opponent ────────────────────

def fetch_step2_ba(first: str, last: str, side: str, opp: str, session=None) -> dict:
    """
    STEP 2: BA in last 10 Home (or Away) games vs today's specific opponent.
    Searches career game logs across all seasons, filtered by opponent + isHome.
    """
    player_id = _get_player_id(first, last)
    time.sleep(0.1)
    if not player_id:
        return _na_result()

    team_id = _get_team_id(opp)
    time.sleep(0.1)
    if not team_id:
        return _na_result()

    is_home = (side == "HOME")

    # Collect all qualifying games newest → oldest
    all_games = []
    for season in reversed(S2_SEASONS):   # newest season first
        splits = _get_game_logs(player_id, season)
        # Game log comes oldest→newest, reverse for recency
        for sp in reversed(splits):
            if (sp.get("opponent", {}).get("id") == team_id and
                    sp.get("isHome") == is_home):
                all_games.append(sp)
        time.sleep(0.08)

    result = _calc_ba(all_games)
    if result is None:
        return _na_result()
    return {**result, "url": "mlb_api"}


# ── Step 3: Last 10 H/A games in 2026 season ─────────────────────────

def fetch_step3_ba(first: str, last: str, side: str, session=None) -> dict:
    """
    STEP 3: BA in last 10 Home (or Away) games in the 2026 season (any opponent).
    """
    player_id = _get_player_id(first, last)
    time.sleep(0.1)
    if not player_id:
        return _na_result()

    is_home   = (side == "HOME")
    splits    = _get_game_logs(player_id, int(SEASON))
    time.sleep(0.08)

    # Filter by home/away, reverse for recency
    matching = [sp for sp in reversed(splits) if sp.get("isHome") == is_home]

    result = _calc_ba(matching)
    if result is None:
        return _na_result()
    return {**result, "url": "mlb_api"}


# ── Helpers ───────────────────────────────────────────────────────────

def _na_result() -> dict:
    return {"ba": None, "score_ba": 0.0, "display": "N/A",
            "flag": "❌ N/A", "games": 0, "url": "mlb_api"}


def fetch_statmuse_ba(first, last, side, opp=None, session=None):
    if opp:
        return fetch_step2_ba(first, last, side, opp, session)
    else:
        return fetch_step3_ba(first, last, side, session)


# ── Test ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== Step 2: Last 10 H/A games vs opponent ===")
    r = fetch_step2_ba("riley", "greene", "AWAY", "new-york-mets")
    print(f"Greene AWAY vs Mets (last {r.get('games','?')} games): {r['display']}  {r['flag']}")

    r2 = fetch_step2_ba("freddie", "freeman", "HOME", "new-york-mets")
    print(f"Freeman HOME vs Mets (last {r2.get('games','?')} games): {r2['display']}  {r2['flag']}")

    r3 = fetch_step2_ba("aaron", "judge", "HOME", "texas-rangers")
    print(f"Judge HOME vs Rangers (last {r3.get('games','?')} games): {r3['display']}  {r3['flag']}")

    print()
    print("=== Step 3: Last 10 H/A games in 2026 ===")
    r4 = fetch_step3_ba("riley", "greene", "AWAY")
    print(f"Greene AWAY 2026 (last {r4.get('games','?')} games): {r4['display']}  {r4['flag']}")

    r5 = fetch_step3_ba("aaron", "judge", "HOME")
    print(f"Judge HOME 2026 (last {r5.get('games','?')} games): {r5['display']}  {r5['flag']}")

    r6 = fetch_step3_ba("freddie", "freeman", "AWAY")
    print(f"Freeman AWAY 2026 (last {r6.get('games','?')} games): {r6['display']}  {r6['flag']}")
