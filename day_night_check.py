"""
Step 4 — Day/Night BA Filter (ESPN Splits API)
================================================
Uses ESPN's official splits API for REAL 2026 Day/Night batting averages.

Endpoint: https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/<ID>/splits
Player Search: https://site.web.api.espn.com/apis/search/v2?query=<name>&sport=mlb

Rules:
  - Day game (start before 5 PM ET = before 21:00 UTC) → check 2026 Day BA
  - Night game (5 PM ET or later = 21:00+ UTC)         → check 2026 Night BA
  - If BA < .200 → DISQUALIFIED (filter only, no score impact)
  - N/A or insufficient data → does NOT disqualify
"""

import requests
import time
import re
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
}
MIN_DAY_NIGHT_BA = 0.200


def get_game_time_type(team_name, date_str):
    """
    Returns 'day', 'night', or 'unknown' for a team's game on date_str ('YYYYMMDD').
    Day = start before 21:00 UTC (5 PM ET)
    Night = 21:00 UTC or later
    Uses partial matching so 'Kansas City Royals' matches 'Royals' etc.
    """
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_str}"
        r = requests.get(url, headers=HEADERS, timeout=10)
        team_lower = team_name.lower()
        for event in r.json().get("events", []):
            comps = event.get("competitions", [{}])[0]
            team_names = [t["team"]["displayName"] for t in comps.get("competitors", [])]
            # Exact match first, then partial match
            matched = (team_name in team_names or
                       any(team_lower in tn.lower() or tn.lower() in team_lower
                           for tn in team_names))
            if matched:
                game_date = event.get("date", "")
                if game_date:
                    dt = datetime.fromisoformat(game_date.replace("Z", "+00:00"))
                    return "day" if dt.hour < 21 else "night"
        return "unknown"
    except:
        return "unknown"


def find_espn_player_id(player_full_name):
    """
    Search ESPN for a player and return numeric ESPN ID.
    Numeric ID is found in `uid` field as 's:1~l:10~a:<NUMERIC_ID>'.
    """
    try:
        url = f"https://site.web.api.espn.com/apis/search/v2?query={player_full_name.replace(' ', '+')}&limit=5&sport=mlb"
        r = requests.get(url, headers=HEADERS, timeout=8)
        data = r.json()
        # Strip accents for comparison
        def norm(t):
            for a, p in {'á':'a','é':'e','í':'i','ó':'o','ú':'u','ñ':'n'}.items():
                t = t.replace(a, p)
            return t.lower()
        target = norm(player_full_name)

        for result in data.get("results", []):
            if result.get("type") != "player":
                continue
            for content in result.get("contents", []):
                if norm(content.get("displayName", "")) == target:
                    # Extract numeric ID from uid 's:1~l:10~a:33481'
                    uid = content.get("uid", "")
                    m = re.search(r"a:(\d+)", uid)
                    if m:
                        return m.group(1)
                    # Fallback: try the link URL
                    link = content.get("link", {}).get("web", "")
                    m = re.search(r"/id/(\d+)", link)
                    if m:
                        return m.group(1)
        return None
    except:
        return None


def fetch_day_night_ba(espn_id, game_type):
    """
    Fetch 2026 Day or Night BA from ESPN splits API.
    Returns dict: { ba, display, flag, dq, ab }
    """
    if not espn_id:
        return {"ba": None, "ab": None, "display": "N/A", "flag": "❌ no ESPN ID", "dq": False}

    label = "Day" if game_type == "day" else "Night"
    try:
        url = f"https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{espn_id}/splits"
        r = requests.get(url, headers=HEADERS, timeout=10)
        data = r.json()

        for cat in data.get("splitCategories", []):
            if cat.get("displayName") == "Breakdown":
                splits = cat.get("splits", [])
                # First try: exact match (Day or Night)
                for s in splits:
                    if s.get("displayName") == label:
                        stats = s.get("stats", [])
                        if len(stats) > 12:
                            try:
                                ba = float(stats[12])
                                ab = int(stats[0])
                                dq = ba < MIN_DAY_NIGHT_BA
                                return {
                                    "ba": ba, "ab": ab,
                                    "display": stats[12],
                                    "flag": f"❌ {stats[12]}<.200" if dq else "✅",
                                    "dq": dq
                                }
                            except (ValueError, TypeError):
                                pass
                # Second try: fallback to other split if primary not available
                other_label = "Night" if label == "Day" else "Day"
                for s in splits:
                    if s.get("displayName") == other_label:
                        stats = s.get("stats", [])
                        if len(stats) > 12:
                            try:
                                ba = float(stats[12])
                                ab = int(stats[0])
                                dq = ba < MIN_DAY_NIGHT_BA
                                return {
                                    "ba": ba, "ab": ab,
                                    "display": stats[12],
                                    "flag": f"❌ {stats[12]}<.200" if dq else f"✅ ({other_label} used)",
                                    "dq": dq
                                }
                            except: pass
                return {"ba": None, "ab": None, "display": "N/A", "flag": "❌ no data", "dq": False}
    except:
        return {"ba": None, "ab": None, "display": "N/A", "flag": "❌ ERR", "dq": False}


def run_day_night_filter(qualified_players, date_str, roster):
    """
    qualified_players: list of player dicts surviving Steps 1-3
    date_str: 'YYYYMMDD'
    roster: dict from mlb_roster.build_player_roster() — has full_name, team_name
    Returns: (still_qualified, newly_disqualified)
    """
    print(f"\n=== Step 4: ESPN Day/Night BA Filter (2026, min .200) ===\n")
    print(f"{'Player':<22} {'Game':<6} {'BA':>6} {'AB':>4} {'Flag':<22} Status")
    print("-" * 75)

    still_qualified = []
    newly_disqualified = []

    for p in qualified_players:
        name = p["name"]
        info = roster.get(name, {})
        team = info.get("team_name", "")
        full_name = info.get("full_name", name)

        # Day or Night?
        game_type = get_game_time_type(team, date_str)
        time.sleep(0.2)

        # ESPN player ID
        espn_id = find_espn_player_id(full_name)
        time.sleep(0.3)

        if game_type == "unknown" or not espn_id:
            print(f"{name:<22} {'?':<6} {'N/A':>6} {'?':>4} {'❌ skip':<22} OK (skipped)")
            still_qualified.append({**p, "dn_ba": None, "dn_flag": "❌ N/A", "dn_type": "?", "dn_display": "N/A"})
            continue

        result = fetch_day_night_ba(espn_id, game_type)
        time.sleep(0.3)

        label = "DAY" if game_type == "day" else "NIGHT"
        ab_str = str(result["ab"]) if result["ab"] is not None else "—"
        print(f"{name:<22} {label:<6} {result['display']:>6} {ab_str:>4} {result['flag']:<22} {'❌ DQ' if result['dq'] else 'OK'}")

        player_with_dn = {
            **p,
            "dn_ba": result["ba"],
            "dn_ab": result["ab"],
            "dn_flag": result["flag"],
            "dn_type": label,
            "dn_display": result["display"],
            "espn_id": espn_id
        }

        if result["dq"]:
            newly_disqualified.append({
                **player_with_dn,
                "dq_reason": f"Step 4 {label} BA {result['display']} < .200 ({result['ab']} AB)"
            })
        else:
            still_qualified.append(player_with_dn)

    print(f"\n{'='*75}")
    print(f"Passed Step 4: {len(still_qualified)} | DQ'd: {len(newly_disqualified)}")
    return still_qualified, newly_disqualified


if __name__ == "__main__":
    # Test with May 5 players using full names from MLB API
    test_players = [
        {"name": "J. Chourio",    "full_name": "Jackson Chourio",    "team_name": "Milwaukee Brewers"},
        {"name": "L. Arraez",     "full_name": "Luis Arraez",        "team_name": "San Francisco Giants"},
        {"name": "Y. Diaz",       "full_name": "Yandy Diaz",         "team_name": "Tampa Bay Rays"},
        {"name": "N. Arenado",    "full_name": "Nolan Arenado",      "team_name": "Arizona Diamondbacks"},
        {"name": "A. Judge",      "full_name": "Aaron Judge",        "team_name": "New York Yankees"},
        {"name": "K. Schwarber",  "full_name": "Kyle Schwarber",     "team_name": "Philadelphia Phillies"},
    ]
    qualified = [{"name": p["name"]} for p in test_players]
    roster = {p["name"]: {"full_name": p["full_name"], "team_name": p["team_name"]} for p in test_players}

    run_day_night_filter(qualified, "20260505", roster)
