"""
pitcher_k.py — Pitcher Strikeout Picks for MoneyBall.

Simplified algorithm:
  Step 1 : Get pitcher K lines from The Odds API (pitcher_strikeouts market).
  Step 2 : Pull career H/A game logs vs today's specific opponent (all seasons).
           Calculate avg Ks in those H/A starts.
  Pick   : Compare avg K to the line.
           avg > line  → lean OVER
           avg < line  → lean UNDER
           Shows ALL pitchers with at least MIN_STARTS qualifying starts vs that team.
"""

import os, time, requests

ODDS_API_KEY  = os.environ.get("ODDS_API_KEY", "")
ODDS_BASE     = "https://api.the-odds-api.com/v4"
MLB_API       = "https://statsapi.mlb.com/api/v1"

MIN_STARTS       = 2       # minimum qualifying H/A starts vs opponent to show a pick
MIN_IP_START     = 3.0     # min innings pitched to count as a qualifying start
K_SEASONS        = [2021, 2022, 2023, 2024, 2025, 2026]
SEASON           = "2026"
BOTTOM_K_TEAMS_N = 5       # cut pitchers facing the bottom N lowest K-rate teams

_pitcher_id_cache = {}
_team_id_cache    = {}


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    subs = {'á':'a','à':'a','ä':'a','é':'e','è':'e','ë':'e',
            'í':'i','ì':'i','ó':'o','ò':'o','ö':'o','ú':'u',
            'ù':'u','ü':'u','ñ':'n','ç':'c'}
    t = text.lower()
    for a, p in subs.items():
        t = t.replace(a, p)
    return t


def _teams_match(t1: str, t2: str) -> bool:
    """Fuzzy team name match."""
    n1, n2 = _normalize(t1), _normalize(t2)
    if n1 == n2 or n1 in n2 or n2 in n1:
        return True
    stop = {"of", "the", "los", "las", "san", "new", "de"}
    w1 = set(n1.split()) - stop
    w2 = set(n2.split()) - stop
    return len(w1 & w2) >= 2


def _ip_to_float(ip_str) -> float:
    """'6.2' → 6.667  (MLB fractional innings)"""
    try:
        parts = str(ip_str).split(".")
        full   = int(parts[0])
        thirds = int(parts[1]) if len(parts) > 1 else 0
        return full + thirds / 3.0
    except Exception:
        return 0.0


# ─────────────────────────────────────────────────────────────────────
# Team K-rate filter — bottom 5 strikeout teams (hardest to K as batters)
# ─────────────────────────────────────────────────────────────────────

def _get_bottom_k_teams(season: str, n: int = BOTTOM_K_TEAMS_N):
    """
    Fetch all team batting stats and find the bottom N teams by K/game.
    These teams strike out the LEAST as batters — toughest for pitcher K props.
    Returns:
      dq_set  : set of team names to DQ against
      ranked  : list of {name, k_per_g} for display in logs
    """
    try:
        r = requests.get(
            f"{MLB_API}/teams/stats",
            params={
                "season":   season,
                "sportId":  1,
                "group":    "hitting",
                "stats":    "season",
            },
            timeout=12,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        teams_data = []
        for sp in splits:
            stat = sp.get("stat", {})
            ks   = stat.get("strikeOuts", 0)
            gp   = stat.get("gamesPlayed", 1)
            if gp < 5:
                continue   # skip teams with tiny sample
            k_per_g = round(ks / gp, 2)
            teams_data.append({
                "name":    sp.get("team", {}).get("name", ""),
                "k_per_g": k_per_g,
            })
        # Sort ascending — fewest Ks per game first
        teams_data.sort(key=lambda x: x["k_per_g"])
        bottom_n = teams_data[:n]
        dq_set   = {t["name"] for t in bottom_n}
        return dq_set, bottom_n
    except Exception:
        return set(), []


# ─────────────────────────────────────────────────────────────────────
# Odds API
# ─────────────────────────────────────────────────────────────────────

def _get_today_events() -> list:
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY, "dateFormat": "iso"},
            timeout=15,
        )
        return r.json() if r.ok and isinstance(r.json(), list) else []
    except Exception:
        return []


def _get_k_lines_for_event(event_id: str) -> list:
    """Returns pitcher K lines — tries both pitcher_strikeouts and pitcher_strikeouts_alternate."""
    # Per Odds API: regular O/U = pitcher_strikeouts, milestone X+ = pitcher_strikeouts_alternate
    markets_to_try = ["pitcher_strikeouts", "pitcher_strikeouts_alternate"]
    bookmakers = "draftkings,fanduel,betmgm,caesars,pointsbetus,betrivers"
    lines = {}
    for market in markets_to_try:
        try:
            r = requests.get(
                f"{ODDS_BASE}/sports/baseball_mlb/events/{event_id}/odds",
                params={
                    "apiKey":     ODDS_API_KEY,
                    "regions":    "us,us2",
                    "markets":    market,
                    "bookmakers": bookmakers,
                    "oddsFormat": "american",
                },
                timeout=15,
            )
            if not r.ok:
                continue
            for bm in r.json().get("bookmakers", []):
                for mkt in bm.get("markets", []):
                    if mkt.get("key") not in (market, "pitcher_strikeouts", "pitcher_strikeouts_alternate"):
                        continue
                    for oc in mkt.get("outcomes", []):
                        name  = oc.get("description") or oc.get("name", "")
                        side  = oc.get("name", "")
                        point = oc.get("point")
                        price = oc.get("price")
                        if not name or point is None:
                            continue
                        key = _normalize(name)
                        if key not in lines:
                            lines[key] = {"name": name, "line": float(point),
                                          "over_odds": None, "under_odds": None}
                        if side == "Over":
                            lines[key]["over_odds"] = price
                        elif side == "Under":
                            lines[key]["under_odds"] = price
        except Exception:
            continue
    return list(lines.values())


# ─────────────────────────────────────────────────────────────────────
# MLB Stats API
# ─────────────────────────────────────────────────────────────────────

def _get_pitcher_id(full_name: str) -> int | None:
    key = _normalize(full_name)
    if key in _pitcher_id_cache:
        return _pitcher_id_cache[key]
    try:
        last = full_name.strip().split()[-1]
        r = requests.get(f"{MLB_API}/people/search",
            params={"names": last, "sportId": 1}, timeout=8)
        norm = _normalize(full_name)
        candidates = r.json().get("people", [])
        # Exact name + active pitcher
        for p in candidates:
            if (_normalize(p.get("fullName", "")) == norm and p.get("active") and
                    p.get("primaryPosition", {}).get("code") == "1"):
                _pitcher_id_cache[key] = p["id"]
                return p["id"]
        # Last-name match among active pitchers
        for p in candidates:
            if (_normalize(p.get("lastName", "")) == _normalize(last) and
                    p.get("active") and
                    p.get("primaryPosition", {}).get("code") == "1"):
                _pitcher_id_cache[key] = p["id"]
                return p["id"]
    except Exception:
        pass
    return None


def _get_pitcher_team(pitcher_id: int) -> str:
    try:
        r = requests.get(f"{MLB_API}/people/{pitcher_id}",
            params={"hydrate": "currentTeam"}, timeout=8)
        return r.json()["people"][0].get("currentTeam", {}).get("name", "")
    except Exception:
        return ""


def _get_team_id(team_name: str) -> int | None:
    key = _normalize(team_name)
    if key in _team_id_cache:
        return _team_id_cache[key]
    try:
        r = requests.get(f"{MLB_API}/teams",
            params={"sportId": 1, "season": SEASON}, timeout=8)
        for t in r.json().get("teams", []):
            if _teams_match(t.get("name", ""), team_name):
                _team_id_cache[key] = t["id"]
                return t["id"]
    except Exception:
        pass
    return None


def _get_pitching_logs(pitcher_id: int, season: int) -> list:
    try:
        r = requests.get(f"{MLB_API}/people/{pitcher_id}/stats",
            params={"stats": "gameLog", "group": "pitching",
                    "season": season, "gameType": "R"},
            timeout=12)
        data = r.json().get("stats", [])
        return data[0].get("splits", []) if data else []
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────
# Core: career H/A K avg vs specific opponent
# ─────────────────────────────────────────────────────────────────────

def career_ha_ks_vs_opp(pitcher_id: int, side: str, opp_name: str) -> dict:
    """
    Pull all career H/A qualifying starts vs today's opponent across all seasons.
    Returns {avg_k, starts, k_list, min_k, max_k} or None if not enough data.
    """
    opp_id = _get_team_id(opp_name)
    time.sleep(0.1)
    if not opp_id:
        return None

    is_home   = (side == "HOME")
    k_list    = []

    for season in reversed(K_SEASONS):   # newest first so most recent context is prioritized
        splits = _get_pitching_logs(pitcher_id, season)
        time.sleep(0.08)
        for sp in reversed(splits):      # reverse so newest game is first within season
            opp_id_sp = sp.get("opponent", {}).get("id")
            if opp_id_sp != opp_id:
                continue
            if sp.get("isHome") != is_home:
                continue
            ip = _ip_to_float(sp.get("stat", {}).get("inningsPitched", "0"))
            if ip < MIN_IP_START:
                continue   # filter out relief appearances
            ks = sp.get("stat", {}).get("strikeOuts", 0)
            k_list.append(ks)

    if len(k_list) < MIN_STARTS:
        return {"avg_k": None, "starts": len(k_list), "k_list": k_list,
                "min_k": None, "max_k": None}

    avg_k = round(sum(k_list) / len(k_list), 1)
    return {
        "avg_k":  avg_k,
        "starts": len(k_list),
        "k_list": k_list,
        "min_k":  min(k_list),
        "max_k":  max(k_list),
    }


# ─────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────

def run_pitcher_k_picks(run_date: str, team_schedule: dict, emit=None) -> dict:
    """
    Pitcher K Picks pipeline.
    Returns {"picks": [...confirmed direction picks], "all": [...all analyzed]}
    """
    if emit is None:
        emit = lambda _: None

    if not ODDS_API_KEY:
        emit({"type": "log", "msg": "⚠️ No ODDS_API_KEY — Pitcher K Picks skipped"})
        return {"picks": [], "all": []}

    emit({"type": "section", "msg": "⚾ Pitcher K Picks — Fetching lines from Odds API"})

    # ── Get events & lines ───────────────────────────────────────────
    events = _get_today_events()
    if not events:
        emit({"type": "log", "msg": "⚠️ No MLB events today — Pitcher K Picks skipped"})
        return {"picks": [], "all": []}

    all_lines = []
    for event in events:
        k_lines = _get_k_lines_for_event(event["id"])
        for l in k_lines:
            l["home_team"] = event.get("home_team", "")
            l["away_team"] = event.get("away_team", "")
        all_lines.extend(k_lines)
        time.sleep(0.15)

    if not all_lines:
        emit({"type": "log", "msg": "⚠️ No pitcher K lines posted yet — check back closer to game time"})
        return {"picks": [], "all": []}

    emit({"type": "log", "msg": f"✅ {len(all_lines)} pitcher K lines found"})

    # ── Bottom-K teams filter (lowest K/game as batters) ───────────────
    bottom_k_set, bottom_k_list = _get_bottom_k_teams(SEASON)
    if bottom_k_set:
        team_lines = ", ".join(
            f"{t['name']} ({t['k_per_g']} K/G)" for t in bottom_k_list
        )
        emit({"type": "log", "msg": f"✅ Bottom {BOTTOM_K_TEAMS_N} K teams (DQ zone): {team_lines}"})
    else:
        emit({"type": "log", "msg": "⚠️ Could not load team K stats — K-team filter skipped"})

    emit({"type": "section", "msg": "⚾ Pitcher K Picks — Pulling career H/A history vs opponent"})

    # ── Analyze each pitcher ─────────────────────────────────────────
    all_results = []
    for pl in all_lines:
        name = pl["name"]
        line = pl["line"]

        pid = _get_pitcher_id(name)
        time.sleep(0.15)
        if not pid:
            emit({"type": "log", "msg": f"  ⚠️ {name} — player not found, skipping"})
            continue

        # Determine H/A context
        pitcher_team = _get_pitcher_team(pid)
        time.sleep(0.1)
        if _teams_match(pitcher_team, pl["home_team"]):
            side = "HOME"
            opp  = pl["away_team"]
        else:
            side = "AWAY"
            opp  = pl["home_team"]

        # ── Bottom-K team DQ check ──────────────────────────────────
        opp_k_info = next((t for t in bottom_k_list if _teams_match(t["name"], opp)), None)
        if bottom_k_set and opp_k_info:
            dq_note = f"Opp {opp} is bottom {BOTTOM_K_TEAMS_N} K team ({opp_k_info['k_per_g']} K/G)"
            emit({"type": "log", "msg": f"  ❌ {name} — {dq_note}"})
            all_results.append({
                "name": name, "team": pitcher_team, "opp": opp, "side": side,
                "line": line, "over_odds": pl.get("over_odds"), "under_odds": pl.get("under_odds"),
                "avg_k": None, "starts": 0, "min_k": None, "max_k": None,
                "k_history": "—", "pick": None, "pick_note": dq_note,
            })
            continue

        emit({"type": "log",
              "msg": f"  {name}  K line: {line}  {side} vs {opp} — pulling history..."})

        # Career H/A K avg vs today's opponent
        hist = career_ha_ks_vs_opp(pid, side, opp)

        avg_k  = hist["avg_k"]  if hist else None
        starts = hist["starts"] if hist else 0
        k_list = hist["k_list"] if hist else []
        min_k  = hist["min_k"]  if hist else None
        max_k  = hist["max_k"]  if hist else None

        # Determine pick direction
        if avg_k is None:
            pick     = None
            pick_note = f"N/A — only {starts} qualifying start{'s' if starts != 1 else ''} vs {opp}"
            emit({"type": "log", "msg": f"    — No pick: {pick_note}"})
        elif avg_k > line:
            pick      = "OVER"
            pick_note = f"avg {avg_k} K > line {line}"
            emit({"type": "log", "msg": f"    ✅ OVER — avg {avg_k} K > line {line}  ({starts} starts)"})
        elif avg_k < line:
            pick      = "UNDER"
            pick_note = f"avg {avg_k} K < line {line}"
            emit({"type": "log", "msg": f"    ✅ UNDER — avg {avg_k} K < line {line}  ({starts} starts)"})
        else:
            pick      = None
            pick_note = f"avg {avg_k} K exactly on the line — no pick"
            emit({"type": "log", "msg": f"    — Avg exactly on line {line} — no pick"})

        # Build a compact K history string e.g. "8, 7, 5, 9, 6"
        k_history = ", ".join(str(k) for k in k_list) if k_list else "—"

        all_results.append({
            "name":       name,
            "team":       pitcher_team,
            "opp":        opp,
            "side":       side,
            "line":       line,
            "over_odds":  pl.get("over_odds"),
            "under_odds": pl.get("under_odds"),
            "avg_k":      avg_k,
            "starts":     starts,
            "min_k":      min_k,
            "max_k":      max_k,
            "k_history":  k_history,
            "pick":       pick,
            "pick_note":  pick_note,
        })

    # Confirmed picks = any pitcher with enough history and avg ≠ line
    confirmed = [r for r in all_results if r["pick"]]
    no_pick   = [r for r in all_results if not r["pick"]]

    # Sort confirmed: biggest gap between avg and line first
    confirmed.sort(
        key=lambda r: abs((r["avg_k"] or 0) - r["line"]),
        reverse=True
    )

    cnt = len(confirmed)
    emit({"type": "log",
          "msg": f"✅ Pitcher K Picks done — {cnt} pick{'s' if cnt != 1 else ''} (avg vs line)"})

    return {"picks": confirmed, "all": all_results}
