"""
under_picks.py — "Under Picks" section for MoneyBall.

Fetches DraftKings batter_hits lines from The Odds API.
Filters for players with line = 1.5 (not 0.5).
Runs them through 3 UNDER-focused filters:
  Step U1: Career BA vs today's pitcher  < .200  (min 4 AB required)
  Step U2: Lifetime H/A BA vs opponent   < .225  (min 3 games required)
  Step U3: 2026 season H/A BA            < .250  (min 3 games required)
Players passing ALL three filters = Under Picks.
"""

import os, re, time, requests
from statmuse_fetch import fetch_step2_ba, fetch_step3_ba

ODDS_API_KEY  = os.environ.get("ODDS_API_KEY", "")
ODDS_BASE     = "https://api.the-odds-api.com/v4"
MLB_API       = "https://statsapi.mlb.com/api/v1"

MIN_AB_U1    = 4      # min career AB vs pitcher to evaluate Step U1
UNDER_S1_MAX = 0.200  # career BA vs pitcher must be BELOW this
UNDER_S2_MAX = 0.225  # lifetime H/A vs opponent must be BELOW this
UNDER_S3_MAX = 0.250  # 2026 H/A BA must be BELOW this


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _short_name(full_name: str) -> str:
    parts = full_name.strip().split()
    return f"{parts[0][0]}. {' '.join(parts[1:])}" if len(parts) >= 2 else full_name


def _parse_avg(val) -> float:
    try:
        s = str(val or "0").strip()
        if s in ("", "-.--", "-.-", "---"):
            return 0.0
        return float(f"0{s}") if s.startswith(".") else float(s)
    except (ValueError, TypeError):
        return 0.0


def _normalize(text: str) -> str:
    """Lowercase + strip accents for fuzzy matching."""
    subs = {
        'á':'a','à':'a','ä':'a','â':'a','ã':'a',
        'é':'e','è':'e','ë':'e','ê':'e',
        'í':'i','ì':'i','ï':'i','î':'i',
        'ó':'o','ò':'o','ö':'o','ô':'o','õ':'o',
        'ú':'u','ù':'u','ü':'u','û':'u',
        'ñ':'n','ç':'c',
    }
    t = text.lower()
    for a, p in subs.items():
        t = t.replace(a, p)
    return t


def _name_to_slug(full_name: str) -> str:
    """'Freddie Freeman' → 'freddie-freeman', 'Bobby Witt Jr.' → 'bobby-witt-jr'"""
    return re.sub(r"[^a-z0-9]+", "-", _normalize(full_name)).strip("-")


def _match_team(mlb_team_name: str, team_schedule: dict) -> dict:
    """
    Match an MLB Stats API team name to a team_schedule entry.
    Falls back to normalized and partial matching.
    """
    if mlb_team_name in team_schedule:
        return team_schedule[mlb_team_name]
    norm = _normalize(mlb_team_name)
    for key, val in team_schedule.items():
        if _normalize(key) == norm:
            return val
    for key, val in team_schedule.items():
        if norm in _normalize(key) or _normalize(key) in norm:
            return val
    return {}


# ─────────────────────────────────────────────────────────────────────
# The Odds API — fetch DraftKings 1.5 hit-line players
# ─────────────────────────────────────────────────────────────────────

def _get_today_events() -> list:
    """Fetch today's MLB events from The Odds API."""
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY, "dateFormat": "iso"},
            timeout=15,
        )
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _get_event_players_15(event_id: str) -> list:
    """Return player names with batter_hits line = 1.5 on DraftKings for one event."""
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/baseball_mlb/events/{event_id}/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": "batter_hits",
                "bookmakers": "draftkings",
                "oddsFormat": "american",
            },
            timeout=15,
        )
        if not r.ok:
            return []
        players = set()
        for bm in r.json().get("bookmakers", []):
            if bm.get("key") != "draftkings":
                continue
            for mkt in bm.get("markets", []):
                if mkt.get("key") != "batter_hits":
                    continue
                for outcome in mkt.get("outcomes", []):
                    if outcome.get("point") == 1.5:
                        name = outcome.get("description", "").strip()
                        if name:
                            players.add(name)
        return list(players)
    except Exception:
        return []


def get_dk_15_hit_players(emit=None) -> list:
    """Returns full names of players with a 1.5 DK batter_hits line today."""
    def log(msg):
        if emit:
            emit({"type": "log", "msg": msg})

    if not ODDS_API_KEY:
        log("⚠️  ODDS_API_KEY not set — Under Picks skipped")
        return []

    log("⬇️  Fetching DraftKings batter_hits lines (1.5 O/U only)…")
    events = _get_today_events()
    if not events:
        log("⚠️  No MLB events returned from Odds API — Under Picks skipped")
        return []

    log(f"   {len(events)} MLB events found")
    all_players = []
    for ev in events:
        players = _get_event_players_15(ev["id"])
        all_players.extend(players)
        time.sleep(0.2)

    unique = list(dict.fromkeys(p for p in all_players if p))
    log(f"✅ {len(unique)} players have a 1.5 hits line on DraftKings")
    return unique


# ─────────────────────────────────────────────────────────────────────
# MLB Stats API helpers
# ─────────────────────────────────────────────────────────────────────

def _build_team_pitcher_map(run_date: str) -> dict:
    """
    Returns {team_id (int): {pitcher_id, pitcher_short}} from today's MLB schedule.
    Away batters face the home pitcher and vice versa.
    """
    try:
        r = requests.get(
            f"{MLB_API}/schedule",
            params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"},
            timeout=15,
        )
        result = {}
        for dd in r.json().get("dates", []):
            for game in dd.get("games", []):
                home   = game["teams"]["home"]
                away   = game["teams"]["away"]
                home_p = home.get("probablePitcher", {})
                away_p = away.get("probablePitcher", {})
                if home_p:   # away batters face home pitcher
                    result[away["team"]["id"]] = {
                        "pitcher_id":    home_p["id"],
                        "pitcher_short": _short_name(home_p["fullName"]),
                    }
                if away_p:   # home batters face away pitcher
                    result[home["team"]["id"]] = {
                        "pitcher_id":    away_p["id"],
                        "pitcher_short": _short_name(away_p["fullName"]),
                    }
        return result
    except Exception:
        return {}


def _lookup_player_full(full_name: str) -> dict:
    """
    Look up an MLB player by full name via MLB Stats API.
    Returns {player_id, team_id, team_name, pos, slug, full_name} or None.
    """
    try:
        r = requests.get(
            f"{MLB_API}/people/search",
            params={"names": full_name, "sportId": 1},
            timeout=8,
        )
        people = r.json().get("people", [])
        active = [p for p in people if p.get("active")]
        if not active:
            return None
        exact  = [p for p in active if p.get("fullName", "").lower() == full_name.lower()]
        person = exact[0] if exact else active[0]
        pid    = person["id"]

        r2   = requests.get(f"{MLB_API}/people/{pid}",
                            params={"hydrate": "currentTeam"}, timeout=8)
        info = r2.json()["people"][0]
        team = info.get("currentTeam", {})

        return {
            "player_id": pid,
            "team_id":   team.get("id"),
            "team_name": team.get("name", ""),
            "pos":       info.get("primaryPosition", {}).get("abbreviation", ""),
            "slug":      _name_to_slug(info.get("fullName", full_name)),
            "full_name": info.get("fullName", full_name),
        }
    except Exception:
        return None


def _career_ba_vs_pitcher(batter_id: int, pitcher_id: int) -> dict:
    """Career BA vs a specific pitcher from MLB Stats vsPlayerTotal."""
    try:
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
                        "hits": s.get("hits", 0),
                        "ba":   _parse_avg(s.get("avg")),
                    }
    except Exception:
        pass
    return {"ab": 0, "hits": 0, "ba": 0.0}


# ─────────────────────────────────────────────────────────────────────
# Main Under Picks runner
# ─────────────────────────────────────────────────────────────────────

def run_under_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    """
    Build the Under Picks list.

    Parameters
    ----------
    run_date      : "YYYY-MM-DD"
    team_schedule : {team_display_name: {side, opponent, opp_slug}} — from pipeline.py
    emit          : SSE callback

    Returns
    -------
    List of under-pick player dicts.
    """
    if emit is None:
        emit = lambda _: None

    def log(msg):
        emit({"type": "log", "msg": msg})

    emit({"type": "section", "msg": "Under Picks — DraftKings 1.5 Hits Line Analysis"})

    # Step 1: fetch DK 1.5 hit-line players
    dk_players = get_dk_15_hit_players(emit)
    if not dk_players:
        log("⚠️  No DK 1.5 hit-line players found — Under Picks section empty")
        return []

    # Step 2: build team → pitcher map from MLB schedule
    log("⬇️  Building pitcher map from MLB Stats API…")
    pitcher_map = _build_team_pitcher_map(run_date)
    log(f"   {len(pitcher_map)} teams with probable pitchers")

    under_picks = []
    total = len(dk_players)

    for i, full_name in enumerate(dk_players):
        emit({"type": "under_progress", "current": i + 1,
              "total": total, "name": full_name})

        # ── Resolve player via MLB Stats API ───────────────────────
        info = _lookup_player_full(full_name)
        time.sleep(0.15)
        if not info or not info.get("player_id"):
            log(f"  — {full_name}: not found in MLB Stats API")
            continue

        team_name = info["team_name"]
        team_id   = info["team_id"]
        sched     = _match_team(team_name, team_schedule)
        side      = sched.get("side", "")
        opp_name  = sched.get("opponent", "")
        opp_slug  = sched.get("opp_slug", "")

        if not side:
            log(f"  — {full_name}: no game today ({team_name})")
            continue

        # ── Step U1: Career BA vs today's pitcher (< .200) ─────────
        pitch_info    = pitcher_map.get(team_id, {})
        pitcher_id    = pitch_info.get("pitcher_id")
        pitcher_short = pitch_info.get("pitcher_short", "TBD")

        if not pitcher_id:
            log(f"  — {full_name}: no probable pitcher listed — skip")
            continue

        career  = _career_ba_vs_pitcher(info["player_id"], pitcher_id)
        time.sleep(0.1)
        s1_ab   = career["ab"]
        s1_ba   = career["ba"]
        s1_disp = f".{int(s1_ba * 1000):03d}" if (s1_ba > 0 or s1_ab > 0) else ".000"

        if s1_ab < MIN_AB_U1:
            log(f"  — {full_name}: S1 only {s1_ab} AB vs {pitcher_short} (need {MIN_AB_U1}+) — skip")
            continue
        if s1_ba >= UNDER_S1_MAX:
            log(f"  ❌ {full_name}: S1 {s1_disp} ≥ .200 vs {pitcher_short} — not an under pick")
            continue

        # ── Step U2: Lifetime H/A BA vs today's opponent (< .225) ──
        slug_parts = info["slug"].split("-")
        first_s    = slug_parts[0]
        last_s     = "-".join(slug_parts[1:])

        s2 = fetch_step2_ba(first_s, last_s, side, opp_slug)
        time.sleep(0.25)

        if s2["ba"] is None:
            log(f"  — {full_name}: S2 no data {side} vs {opp_name} — skip")
            continue
        if s2["ba"] >= UNDER_S2_MAX:
            log(f"  ❌ {full_name}: S2 {s2['display']} ≥ .225 {side} vs {opp_name} — not under pick")
            continue

        # ── Step U3: 2026 H/A BA (< .250) ──────────────────────────
        s3 = fetch_step3_ba(first_s, last_s, side)
        time.sleep(0.25)

        if s3["ba"] is None:
            log(f"  — {full_name}: S3 no 2026 data — skip")
            continue
        if s3["ba"] >= UNDER_S3_MAX:
            log(f"  ❌ {full_name}: S3 {s3['display']} ≥ .250 {side} — not under pick")
            continue

        # ── Passed all 3! ───────────────────────────────────────────
        log(f"  ✅ UNDER PICK: {full_name}  S1:{s1_disp} vs {pitcher_short}"
            f"  S2:{s2['display']}  S3:{s3['display']}  {side} vs {opp_name}")
        emit({
            "type":    "under_pick_found",
            "name":    full_name,
            "pos":     info["pos"],
            "side":    side,
            "opp":     opp_name,
            "pitcher": pitcher_short,
            "s1":      s1_disp,
            "s1_ab":   s1_ab,
            "s2":      s2["display"],
            "s3":      s3["display"],
        })

        # under_score: lower combined BA = better under pick
        under_score = round((s1_ba + (s2["ba"] or 0) + (s3["ba"] or 0)) * 1000)

        under_picks.append({
            "name":        full_name,
            "pos":         info["pos"],
            "side":        side,
            "opp":         opp_name,
            "pitcher":     pitcher_short,
            "s1_ba":       s1_ba,
            "s1_ab":       s1_ab,
            "s1_disp":     s1_disp,
            "s2":          s2,
            "s3":          s3,
            "dk_line":     1.5,
            "under_score": under_score,
        })

    # Rank by under_score ascending — LOWEST combined BA = Rank 1 (coldest bat)
    under_picks.sort(key=lambda x: x["under_score"])

    log(f"✅ Under Picks complete — {len(under_picks)} players passed all 3 under filters (ranked coldest → warmest)")
    return under_picks
