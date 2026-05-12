"""
Lineup Check — MLB Stats API (confirmed) + Rotowire (projected fallback)

Priority:
  1. MLB Stats API confirmed lineups  — most authoritative when posted
  2. Rotowire projected lineups       — available hours before game time
  3. TBD                              — neither source has data for this team

Status returned: IN_LINEUP / NOT_IN_LINEUP / TBD
"""
import re, requests
from bs4 import BeautifulSoup

MLB_API    = "https://statsapi.mlb.com/api/v1"
RW_URL     = "https://www.rotowire.com/baseball/daily-lineups.php"
RW_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36"
}


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _normalize(name: str) -> str:
    subs = {
        'á':'a','à':'a','ä':'a','â':'a','ã':'a',
        'é':'e','è':'e','ë':'e','ê':'e',
        'í':'i','ì':'i','ï':'i','î':'i',
        'ó':'o','ò':'o','ö':'o','ô':'o','õ':'o',
        'ú':'u','ù':'u','ü':'u','û':'u',
        'ñ':'n','ç':'c',
    }
    n = name.lower().strip()
    for a, p in subs.items():
        n = n.replace(a, p)
    return n


def _names_match(full_name: str, rw_name: str) -> bool:
    """
    Fuzzy match between full MLB name and Rotowire display name.
    Handles: 'Dominic Smith' vs 'Dom Smith', 'D. Smith', 'Dominic Smith'
    """
    fn = _normalize(full_name)
    rw = _normalize(rw_name)
    if fn == rw:
        return True
    # Last name match with first initial
    fn_parts = fn.split()
    rw_parts = rw.split()
    if not fn_parts or not rw_parts:
        return False
    # Both last names must match
    if fn_parts[-1] != rw_parts[-1]:
        return False
    # First initial check
    if len(fn_parts) >= 2 and len(rw_parts) >= 2:
        if fn_parts[0][0] == rw_parts[0][0]:
            return True
    # Last name only (fallback — less reliable)
    if fn_parts[-1] == rw_parts[-1] and len(fn_parts[-1]) > 4:
        return True
    return False


# ─────────────────────────────────────────────────────────────────────
# MLB Stats API — confirmed lineups
# ─────────────────────────────────────────────────────────────────────

def _build_mlb_lineups(date_str: str) -> tuple:
    """Returns (id_map, name_map, teams_with_lineups)"""
    if len(date_str) == 8:
        fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    else:
        fmt = date_str

    id_map             = {}
    name_map           = {}
    teams_with_lineups = set()

    try:
        url  = f"{MLB_API}/schedule?sportId=1&date={fmt}&hydrate=lineups"
        data = requests.get(url, timeout=12).json()
        for date_obj in data.get("dates", []):
            for game in date_obj.get("games", []):
                lineups   = game.get("lineups", {})
                home_pl   = lineups.get("homePlayers", [])
                away_pl   = lineups.get("awayPlayers", [])
                home_name = game["teams"]["home"]["team"]["name"]
                away_name = game["teams"]["away"]["team"]["name"]
                if home_pl:
                    teams_with_lineups.add(home_name)
                if away_pl:
                    teams_with_lineups.add(away_name)
                for p in home_pl + away_pl:
                    pid  = p.get("id")
                    name = p.get("fullName", "").lower().strip()
                    if pid:  id_map[int(pid)] = True
                    if name: name_map[name]   = True
    except Exception as e:
        print(f"[lineup_check] MLB API error: {e}")

    return id_map, name_map, teams_with_lineups


# ─────────────────────────────────────────────────────────────────────
# Rotowire — projected lineups (fallback)
# ─────────────────────────────────────────────────────────────────────

def _get_abbrev_map() -> dict:
    """Returns {abbrev: full_team_name} from MLB Stats API."""
    try:
        r = requests.get(f"{MLB_API}/teams", params={"sportId": 1}, timeout=10)
        return {t["abbreviation"]: t["name"]
                for t in r.json().get("teams", []) if t.get("abbreviation")}
    except Exception:
        return {}


def _build_rotowire_lineups(abbrev_map: dict) -> tuple:
    """
    Scrape Rotowire daily lineups page.
    Returns:
      rw_lineups : {team_name: [player_full_name, ...]}
      rw_teams   : set of team names found on Rotowire today
    """
    rw_lineups = {}
    rw_teams   = set()

    try:
        r    = requests.get(RW_URL, headers=RW_HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        for box in soup.select(".lineup__box"):
            # Grab team abbreviations — away first, then home
            abbrevs     = [el.get_text(strip=True) for el in box.select(".lineup__abbrev")]
            player_lists = box.select(".lineup__list")

            for abbrev, plist in zip(abbrevs, player_lists):
                team_name = abbrev_map.get(abbrev.upper(), "")
                if not team_name:
                    continue
                rw_teams.add(team_name)
                players = []
                for li in plist.select("li.lineup__player, li"):
                    link = li.select_one("a")
                    if link:
                        name = link.get_text(strip=True)
                        if name and len(name) > 3:
                            players.append(name)
                if players:
                    rw_lineups[team_name] = players

        print(f"[lineup_check] Rotowire: {len(rw_teams)} teams found")
    except Exception as e:
        print(f"[lineup_check] Rotowire error: {e}")

    return rw_lineups, rw_teams


# ─────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────

def build_lineup_map(date_str: str) -> tuple:
    """
    Build full lineup data from both sources.

    Returns
    -------
    (id_map, name_map, teams_with_lineups, rw_lineups, rw_teams)
      id_map             : {player_id: True}            — MLB confirmed
      name_map           : {full_name_lower: True}      — MLB confirmed
      teams_with_lineups : set of team names            — MLB confirmed
      rw_lineups         : {team_name: [player_names]}  — Rotowire projected
      rw_teams           : set of team names            — Rotowire projected
    """
    id_map, name_map, teams_with_lineups = _build_mlb_lineups(date_str)
    abbrev_map                           = _get_abbrev_map()
    rw_lineups, rw_teams                 = _build_rotowire_lineups(abbrev_map)

    n_mlb = len(teams_with_lineups)
    n_rw  = len(rw_teams)
    print(f"[lineup_check] MLB confirmed: {n_mlb} teams  |  Rotowire projected: {n_rw} teams")

    return id_map, name_map, teams_with_lineups, rw_lineups, rw_teams


def get_lineup_status(player_id, full_name, team_name,
                      id_map, name_map, teams_with_lineups,
                      rw_lineups=None, rw_teams=None) -> str:
    """
    Returns 'IN_LINEUP' / 'NOT_IN_LINEUP' / 'TBD'

    Checks MLB confirmed first, then Rotowire projected, then TBD.
    """
    rw_lineups = rw_lineups or {}
    rw_teams   = rw_teams   or set()

    # ── MLB Stats API confirmed lineup ────────────────────────────
    if team_name in teams_with_lineups:
        if player_id and int(player_id) in id_map:
            return "IN_LINEUP"
        if full_name and _normalize(full_name) in name_map:
            return "IN_LINEUP"
        return "NOT_IN_LINEUP"

    # ── Rotowire projected lineup ─────────────────────────────────
    if team_name in rw_teams:
        rw_names = rw_lineups.get(team_name, [])
        for rw_name in rw_names:
            if _names_match(full_name or "", rw_name):
                return "IN_LINEUP"
        return "NOT_IN_LINEUP"

    # ── Neither source has data yet ───────────────────────────────
    return "TBD"


def check_top9_lineups(top9, roster, date_str):
    """Legacy helper kept for compatibility."""
    print("[lineup_check] Fetching lineups...")
    id_map, name_map, teams_with_lineups, rw_lineups, rw_teams = build_lineup_map(date_str)
    posted = len(teams_with_lineups) + len(rw_teams - teams_with_lineups)
    print(f"[lineup_check] Coverage: {posted} teams")
    for p in top9:
        name      = p.get("name", "")
        info      = roster.get(name, {})
        player_id = info.get("player_id")
        full_name = p.get("full_name") or info.get("full_name", name)
        team_name = info.get("team_name", "")
        status    = get_lineup_status(player_id, full_name, team_name,
                                      id_map, name_map, teams_with_lineups,
                                      rw_lineups, rw_teams)
        p["lineup_status"] = status
        print(f"  {full_name:<25} -> {status}")
    return top9
