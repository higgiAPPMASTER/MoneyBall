"""
statmuse_fetch.py — Steps 2 & 3 via MLB Stats API (NO StatMuse needed!)

STEP 2: Career BA vs today's opponent (vsTeamTotal) — min 4 AB
STEP 3: 2026 season Home or Away BA (homeAndAway) — min 10 AB

100% official MLB API — no scraping, no login, no IP blocking, always works.
"""
import time, requests

MLB_API = "https://statsapi.mlb.com/api/v1"
SEASON  = "2026"
S2_MIN_AB = 4    # minimum career AB vs opponent
S3_MIN_AB = 10   # minimum season AB in home/away split


# ── Player & Team ID lookup ───────────────────────────────────────────

_player_cache = {}   # "first-last" -> player_id
_team_cache   = {}   # team_name_lower -> team_id


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
    """Convert opponent slug like 'new-york-mets' to MLB team ID."""
    key = opp_slug.lower()
    if key in _team_cache:
        return _team_cache[key]
    try:
        # Convert slug to name: 'new-york-mets' -> 'new york mets'
        name_guess = opp_slug.replace("-", " ")
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


# ── Step 2: Career BA vs today's opponent ────────────────────────────

def fetch_step2_ba(first: str, last: str, side: str, opp: str, session=None) -> dict:
    """
    STEP 2: Career batting average vs today's opposing team.
    Uses vsTeamTotal from MLB Stats API.
    """
    player_id = _get_player_id(first, last)
    time.sleep(0.1)

    if not player_id:
        return _na_result()

    team_id = _get_team_id(opp)
    time.sleep(0.1)

    if not team_id:
        return _na_result()

    try:
        r = requests.get(f"{MLB_API}/people/{player_id}/stats",
            params={"stats": "vsTeamTotal", "group": "hitting",
                    "opposingTeamId": team_id}, timeout=10)
        for sg in r.json().get("stats", []):
            for sp in sg.get("splits", []):
                s  = sp.get("stat", {})
                ab = s.get("atBats", 0)
                ba = _parse_avg(s.get("avg"))
                if ab >= S2_MIN_AB:
                    return {"ba": ba, "score_ba": ba, "display": s.get("avg", "N/A"),
                            "flag": "✅", "games": ab, "url": "mlb_api"}
    except Exception:
        pass

    return _na_result()


# ── Step 3: 2026 Season H/A BA ───────────────────────────────────────

def fetch_step3_ba(first: str, last: str, side: str, session=None) -> dict:
    """
    STEP 3: 2026 season batting average in Home or Away games.
    Uses homeAndAway from MLB Stats API.
    """
    player_id = _get_player_id(first, last)
    time.sleep(0.1)

    if not player_id:
        return _na_result()

    is_home = (side == "HOME")

    try:
        r = requests.get(f"{MLB_API}/people/{player_id}/stats",
            params={"stats": "homeAndAway", "group": "hitting",
                    "season": SEASON}, timeout=10)
        for sg in r.json().get("stats", []):
            for sp in sg.get("splits", []):
                if sp.get("isHome") == is_home:
                    s  = sp.get("stat", {})
                    ab = s.get("atBats", 0)
                    ba = _parse_avg(s.get("avg"))
                    if ab >= S3_MIN_AB:
                        return {"ba": ba, "score_ba": ba, "display": s.get("avg", "N/A"),
                                "flag": "✅", "games": ab, "url": "mlb_api"}
    except Exception:
        pass

    return _na_result()


# ── Helpers ───────────────────────────────────────────────────────────

def _parse_avg(avg_str) -> float:
    try:
        s = str(avg_str or "0").strip()
        if s in ("", "-.--", "-.-", "---"):
            return 0.0
        return float(f"0{s}") if s.startswith(".") else float(s)
    except (ValueError, TypeError):
        return 0.0


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
    tests = [
        ("Freeman HOME vs Mets",  "freddie", "freeman",  "HOME", "new-york-mets"),
        ("Freeman AWAY vs Mets",  "freddie", "freeman",  "AWAY", "new-york-mets"),
        ("Judge HOME 2026",       "aaron",   "judge",    "HOME", None),
    ]
    for label, first, last, side, opp in tests:
        if opp:
            r = fetch_step2_ba(first, last, side, opp)
            print(f"S2 {label}: {r['display']} ({r.get('games','?')} AB) {r['flag']}")
        else:
            r = fetch_step3_ba(first, last, side)
            print(f"S3 {label}: {r['display']} ({r.get('games','?')} AB) {r['flag']}")
