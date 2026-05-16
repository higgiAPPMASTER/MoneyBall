"""
MLB Roster & StatMuse Slug Lookup — with Pitcher-Based Disambiguation
======================================================================
When multiple players share an initial + last name (e.g. "Y. Diaz" = Yandy or Yainer),
we use the opposing PITCHER's team from FIC to determine which team the batter is on.

Example:
  "Y. Diaz" facing "K. Gausman" (Blue Jays)
  → Batter is on the team OPPOSING the Blue Jays today
  → ESPN: TOR @ TB → Rays are facing Blue Jays
  → Y. Diaz = Yandy Diaz (Rays) ✅ NOT Yainer Diaz (Astros)
"""

import requests
import time
import re

MLB_API = "https://statsapi.mlb.com/api/v1"


def name_to_slug(full_name):
    """Convert 'Bobby Witt Jr.' to 'bobby-witt-jr' for StatMuse URLs."""
    replacements = {
        'á':'a','à':'a','ä':'a','â':'a','ã':'a',
        'é':'e','è':'e','ë':'e','ê':'e',
        'í':'i','ì':'i','ï':'i','î':'i',
        'ó':'o','ò':'o','ö':'o','ô':'o','õ':'o',
        'ú':'u','ù':'u','ü':'u','û':'u',
        'ñ':'n','ç':'c',
    }
    name = full_name.lower()
    for accented, plain in replacements.items():
        name = name.replace(accented, plain)
    return re.sub(r"[^a-z0-9]+", "-", name).strip("-")


def normalize(text):
    """Strip accents for comparison: Díaz → diaz, Iván → ivan."""
    replacements = {
        'á':'a','à':'a','ä':'a','â':'a','ã':'a',
        'é':'e','è':'e','ë':'e','ê':'e',
        'í':'i','ì':'i','ï':'i','î':'i',
        'ó':'o','ò':'o','ö':'o','ô':'o','õ':'o',
        'ú':'u','ù':'u','ü':'u','û':'u',
        'ñ':'n','ç':'c',
    }
    t = text.lower()
    for a, p in replacements.items():
        t = t.replace(a, p)
    return t


# Suffixes that appear after last name and break MLB API lookups
_SUFFIXES = re.compile(r'\s+(Jr\.?|Sr\.?|II|III|IV|V)$', re.IGNORECASE)

def parse_short_name(short_name):
    parts = short_name.strip().split(".", 1)
    if len(parts) == 2:
        first_initial = parts[0].strip()
        last = parts[1].strip()
        last = _SUFFIXES.sub('', last).strip()  # strip Jr./Sr./II/III etc.
        return first_initial, last
    return "", short_name.strip()


def get_pitcher_team(pitcher_last_name, date_str):
    """
    Look up a pitcher's current team via MLB API.
    Returns team display name or None.
    """
    try:
        r = requests.get(
            f"{MLB_API}/people/search",
            params={"names": pitcher_last_name, "sportId": 1},
            timeout=8
        )
        people = r.json().get("people", [])
        for person in people:
            if not person.get("active", False):
                continue
            pid = person["id"]
            r2 = requests.get(
                f"{MLB_API}/people/{pid}",
                params={"hydrate": "currentTeam"},
                timeout=8
            )
            p = r2.json()["people"][0]
            # Check if pitcher (position code 1)
            pos = p.get("primaryPosition", {}).get("code", "")
            if pos == "1":  # pitcher
                return p.get("currentTeam", {}).get("name", "")
        return None
    except:
        return None


def get_schedule(date_str):
    """
    Fetch ESPN schedule. Returns:
    - todays_teams: set of all team names playing today
    - matchups: dict { team_name: opponent_name }
    """
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_str}"
        r = requests.get(url, timeout=10)
        data = r.json()
        todays_teams = set()
        matchups = {}
        for event in data.get("events", []):
            comps = event.get("competitions", [{}])[0]
            teams = comps.get("competitors", [])
            if len(teams) == 2:
                t1 = teams[0]["team"]["displayName"]
                t2 = teams[1]["team"]["displayName"]
                todays_teams.add(t1)
                todays_teams.add(t2)
                matchups[t1] = t2
                matchups[t2] = t1
        return todays_teams, matchups
    except:
        return set(), {}


def lookup_player(short_name, todays_teams=None, matchups=None, pitcher_name=None, date_str=None):
    """
    Look up a player by short name with pitcher-based disambiguation.

    short_name:   e.g. 'Y. Diaz'
    pitcher_name: last name of today's opposing pitcher from FIC (e.g. 'Gausman')
    date_str:     'YYYYMMDD' for schedule lookup
    Returns dict: { full_name, slug, team_name, team_abbr, player_id }
    """
    first_initial, last_name = parse_short_name(short_name)

    try:
        r = requests.get(
            f"{MLB_API}/people/search",
            params={"names": last_name, "sportId": 1},
            timeout=10
        )
        people = r.json().get("people", [])

        # Filter: active players matching initial + last name
        # Use normalize() so Díaz matches Diaz, Iván matches Ivan etc.
        candidates = []
        for person in people:
            if not person.get("active", False):
                continue
            p_last = person.get("lastName", "")
            p_first = person.get("firstName", "")
            if (normalize(p_last) == normalize(last_name) and
                    p_first.upper().startswith(first_initial.upper())):
                candidates.append(person)

        if not candidates:
            for person in people:
                if (person.get("active", False) and
                        normalize(person.get("lastName", "")) == normalize(last_name)):
                    candidates.append(person)

        if not candidates:
            return None

        # Get full details for all candidates
        full_candidates = []
        for person in candidates:
            pid = person["id"]
            r2 = requests.get(
                f"{MLB_API}/people/{pid}",
                params={"hydrate": "currentTeam"},
                timeout=10
            )
            p = r2.json()["people"][0]
            team = p.get("currentTeam", {})
            full_candidates.append({
                "player_id": pid,
                "full_name": p["fullName"],
                "slug": name_to_slug(p["fullName"]),
                "team_name": team.get("name", ""),
                "team_abbr": team.get("abbreviation", ""),
                "team_id": team.get("id", ""),
            })
            time.sleep(0.2)

        # PRE-FILTER: when we have today's schedule, immediately discard any
        # candidate whose team is not playing today (catches minor-league players,
        # wrong-name matches, etc.).
        if todays_teams:
            # Fuzzy match — handles minor name differences between MLB API and ESPN
            def _team_in_today(cand_team):
                ct = cand_team.lower()
                return any(ct in t.lower() or t.lower() in ct for t in todays_teams)
            mlb_today = [c for c in full_candidates if _team_in_today(c["team_name"])]
            if not mlb_today:
                return None
            full_candidates = mlb_today   # work only with valid MLB players

        # Only one candidate remaining — easy
        if len(full_candidates) == 1:
            return full_candidates[0]

        # DISAMBIGUATION STEP 1: Use pitcher's team to pinpoint batter's team
        # Logic: pitcher plays for team X → batter faces X → batter is on X's opponent
        if pitcher_name and matchups and date_str:
            pitcher_team = get_pitcher_team(pitcher_name, date_str)
            time.sleep(0.2)
            if pitcher_team and pitcher_team in matchups:
                batter_team = matchups[pitcher_team]   # team opposing the pitcher
                matched = [c for c in full_candidates if c["team_name"] == batter_team]
                if len(matched) == 1:
                    return matched[0]

        # DISAMBIGUATION STEP 2: still multiple valid candidates — return first
        return full_candidates[0]

    except:
        return None


def build_player_roster(short_names, date_str=None, pitcher_map=None):
    """
    Look up all players dynamically.
    short_names:  list of short names e.g. ['Y. Diaz', 'J. Chourio', ...]
    date_str:     'YYYYMMDD'
    pitcher_map:  dict { short_name: pitcher_last_name } from FIC data
    """
    todays_teams, matchups = get_schedule(date_str) if date_str else (set(), {})

    roster = {}
    print(f"Looking up {len(short_names)} players via MLB Stats API...")
    print(f"{'Player':<22} {'Full Name':<28} {'Slug':<30} {'Team'}")
    print("-" * 95)

    for short in short_names:
        pitcher = (pitcher_map or {}).get(short, "")
        pitcher_last = pitcher.split()[-1] if pitcher else None
        result = lookup_player(short, todays_teams, matchups, pitcher_last, date_str)

        if result:
            roster[short] = result
            print(f"{short:<22} {result['full_name']:<28} {result['slug']:<30} {result['team_name']}")
        else:
            # No MLB player found on a team playing today —
            # store empty record so the pipeline skips this player cleanly.
            roster[short] = {
                "player_id": None, "full_name": short,
                "slug": "", "team_name": "",
                "team_abbr": "", "team_id": "",
            }
            print(f"{short:<22} ⚠️  NOT FOUND in today's MLB schedule — will be skipped")
        time.sleep(0.25)

    found = len([v for v in roster.values() if v["player_id"]])
    fallback = len([v for v in roster.values() if not v["player_id"]])
    print(f"\n✅ Done: {found} found, {fallback} fallback")
    return roster


if __name__ == "__main__":
    print("=== Testing Disambiguation: Y. Diaz on May 5, 2026 ===\n")
    todays_teams, matchups = get_schedule("20260505")
    # FIC shows Y. Diaz facing K. Gausman (Blue Jays)
    result = lookup_player("Y. Diaz", todays_teams, matchups,
                           pitcher_name="Gausman", date_str="20260505")
    if result:
        print(f"✅ Resolved: {result['full_name']} ({result['team_name']})")
        print(f"   Slug: {result['slug']}")
    else:
        print("❌ Not found")
