"""
pitcher_k.py — Pitcher Strikeout Picks for MoneyBall.

Algorithm:
  Step 1 : Get K lines from The Odds API (pitcher_strikeouts market).
  Step 2 : Career H/A K hit rate vs today's specific opponent (min 2 starts).
  Step 3 : Last 10 H/A starts (any opponent) K hit rate.
  Pick   : OVER if both S2 & S3 >= 70% over the line.
           UNDER if both S2 & S3 >= 70% under the line.
           No pick if split is too close.
"""

import os, re, time, requests

ODDS_API_KEY  = os.environ.get("ODDS_API_KEY", "")
ODDS_BASE     = "https://api.the-odds-api.com/v4"
MLB_API       = "https://statsapi.mlb.com/api/v1"

HIT_THRESH    = 70.0   # % required to make a pick
MIN_S2_GAMES  = 2      # min career H/A starts vs opponent for Step 2
MIN_S3_GAMES  = 5      # min H/A starts (general) for Step 3 to be valid
MIN_IP_START  = 3.0    # min innings pitched to count as a qualifying start
LAST_N        = 10     # last N qualifying H/A starts for Step 3
K_SEASONS     = [2021, 2022, 2023, 2024, 2025, 2026]
SEASON        = "2026"

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
    """Fuzzy team name match — handles 'Los Angeles Angels' vs 'LA Angels' etc."""
    n1, n2 = _normalize(t1), _normalize(t2)
    if n1 == n2 or n1 in n2 or n2 in n1:
        return True
    w1, w2 = set(n1.split()), set(n2.split())
    # Ignore generic words
    stop = {"of", "the", "los", "las", "san", "new"}
    w1 -= stop; w2 -= stop
    return len(w1 & w2) >= 2


def _ip_to_float(ip_str) -> float:
    """'6.2' -> 6.667  (MLB fractional innings notation)"""
    try:
        parts = str(ip_str).split(".")
        full   = int(parts[0])
        thirds = int(parts[1]) if len(parts) > 1 else 0
        return full + thirds / 3.0
    except Exception:
        return 0.0


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
    """
    Fetch pitcher_strikeouts outcomes for one event.
    Returns list of {name, line, over_odds, under_odds}.
    """
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/baseball_mlb/events/{event_id}/odds",
            params={
                "apiKey":     ODDS_API_KEY,
                "regions":    "us,us2",
                "markets":    "pitcher_strikeouts",
                "bookmakers": "draftkings,fanduel,betmgm",
                "oddsFormat": "american",
            },
            timeout=15,
        )
        if not r.ok:
            return []

        lines = {}
        for bm in r.json().get("bookmakers", []):
            for mkt in bm.get("markets", []):
                if mkt.get("key") != "pitcher_strikeouts":
                    continue
                for oc in mkt.get("outcomes", []):
                    name  = oc.get("description") or oc.get("name", "")
                    side  = oc.get("name", "")   # "Over" or "Under"
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
        return list(lines.values())
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────
# MLB Stats API — pitcher lookup + game logs
# ─────────────────────────────────────────────────────────────────────

def _get_pitcher_id(full_name: str) -> int | None:
    key = _normalize(full_name)
    if key in _pitcher_id_cache:
        return _pitcher_id_cache[key]
    try:
        last  = full_name.strip().split()[-1]
        r = requests.get(f"{MLB_API}/people/search",
            params={"names": last, "sportId": 1}, timeout=8)
        norm = _normalize(full_name)
        candidates = r.json().get("people", [])
        # Exact name + pitcher position
        for p in candidates:
            if (_normalize(p.get("fullName","")) == norm and
                    p.get("active") and
                    p.get("primaryPosition", {}).get("code") == "1"):
                _pitcher_id_cache[key] = p["id"]
                return p["id"]
        # Relax to last-name match for active pitchers
        for p in candidates:
            if (_normalize(p.get("lastName","")) == _normalize(last) and
                    p.get("active") and
                    p.get("primaryPosition", {}).get("code") == "1"):
                _pitcher_id_cache[key] = p["id"]
                return p["id"]
    except Exception:
        pass
    return None


def _get_pitcher_team(pitcher_id: int) -> str:
    """Return pitcher's current team display name."""
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
            if _teams_match(t.get("name",""), team_name):
                _team_id_cache[key] = t["id"]
                return t["id"]
    except Exception:
        pass
    return None


def _get_pitching_logs(pitcher_id: int, season: int) -> list:
    """Return pitching game-log splits for one season (regular season only)."""
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
# Hit-rate calculation
# ─────────────────────────────────────────────────────────────────────

def _calc_hit_rate(games: list, line: float, label: str = "") -> dict | None:
    """
    From a list of game-log splits, extract qualifying starts and
    calculate over/under hit rate vs the K line.
    Returns None if not enough qualifying starts.
    """
    ks_list = []
    for g in games:
        stat = g.get("stat", {})
        ip   = _ip_to_float(stat.get("inningsPitched", "0"))
        if ip < MIN_IP_START:
            continue   # skip relief/very short outings
        ks_list.append(stat.get("strikeOuts", 0))

    if not ks_list:
        return None

    n_over  = sum(1 for k in ks_list if k > line)
    n_push  = sum(1 for k in ks_list if k == line)
    n_under = sum(1 for k in ks_list if k < line)
    total   = len(ks_list)

    # Pushes split 50/50
    over_pct  = round((n_over  + n_push * 0.5) / total * 100, 1)
    under_pct = round((n_under + n_push * 0.5) / total * 100, 1)
    avg_k     = round(sum(ks_list) / total, 1)

    return {
        "over_pct":  over_pct,
        "under_pct": under_pct,
        "avg_k":     avg_k,
        "games":     total,
        "label":     label,
    }


# ─────────────────────────────────────────────────────────────────────
# Step 2 — career H/A vs today's specific opponent
# ─────────────────────────────────────────────────────────────────────

def step2_vs_opp(pitcher_id: int, side: str, opp_name: str, line: float) -> dict | None:
    opp_id  = _get_team_id(opp_name)
    time.sleep(0.1)
    if not opp_id:
        return None

    is_home   = (side == "HOME")
    all_games = []

    for season in reversed(K_SEASONS):   # newest season first
        splits = _get_pitching_logs(pitcher_id, season)
        time.sleep(0.08)
        for sp in reversed(splits):      # reverse so newest game is first
            if (sp.get("opponent", {}).get("id") == opp_id and
                    sp.get("isHome") == is_home):
                all_games.append(sp)

    result = _calc_hit_rate(all_games, line, label="vs_opp")
    if result is None or result["games"] < MIN_S2_GAMES:
        return {"over_pct": None, "under_pct": None, "avg_k": None,
                "games": result["games"] if result else 0, "label": "vs_opp"}
    return result


# ─────────────────────────────────────────────────────────────────────
# Step 3 — last 10 H/A starts (any opponent)
# ─────────────────────────────────────────────────────────────────────

def step3_general_ha(pitcher_id: int, side: str, line: float) -> dict | None:
    is_home   = (side == "HOME")
    collected = []

    for season in [int(SEASON), int(SEASON) - 1]:
        splits = _get_pitching_logs(pitcher_id, season)
        time.sleep(0.08)
        for sp in reversed(splits):   # newest first
            if sp.get("isHome") == is_home:
                ip = _ip_to_float(sp.get("stat", {}).get("inningsPitched", "0"))
                if ip >= MIN_IP_START:
                    collected.append(sp)
            if len(collected) >= LAST_N:
                break
        if len(collected) >= LAST_N:
            break

    result = _calc_hit_rate(collected, line, label="general_ha")
    if result is None or result["games"] < MIN_S3_GAMES:
        return {"over_pct": None, "under_pct": None, "avg_k": None,
                "games": result["games"] if result else 0, "label": "general_ha"}
    return result


# ─────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────

def run_pitcher_k_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    """
    Full Pitcher K Picks pipeline.
    team_schedule: {team_name: {"side": "HOME"|"AWAY", "opponent": "..."}}
    Returns list of pick dicts (only confirmed picks with ≥70% both steps).
    """
    if emit is None:
        emit = lambda _: None

    if not ODDS_API_KEY:
        emit({"type": "log", "msg": "⚠️ No ODDS_API_KEY — Pitcher K Picks skipped"})
        return []

    emit({"type": "section", "msg": "⚾ Pitcher K Picks — Fetching K lines from Odds API"})

    # ── Fetch events & K lines ───────────────────────────────────────
    events = _get_today_events()
    if not events:
        emit({"type": "log", "msg": "⚠️ No MLB events found — Pitcher K Picks skipped"})
        return []

    all_lines = []
    for event in events:
        k_lines = _get_k_lines_for_event(event["id"])
        for l in k_lines:
            l["home_team"] = event.get("home_team", "")
            l["away_team"] = event.get("away_team", "")
        all_lines.extend(k_lines)
        time.sleep(0.15)

    if not all_lines:
        emit({"type": "log", "msg": "⚠️ No pitcher K lines posted yet — check back later"})
        return []

    emit({"type": "log", "msg": f"✅ {len(all_lines)} pitcher K lines found today"})
    emit({"type": "section", "msg": "⚾ Pitcher K Picks — Running Step 2 & 3 analysis"})

    # ── Analyze each pitcher ─────────────────────────────────────────
    all_results = []
    for pl in all_lines:
        name = pl["name"]
        line = pl["line"]

        # Resolve to MLB player ID
        pid = _get_pitcher_id(name)
        time.sleep(0.15)
        if not pid:
            emit({"type": "log", "msg": f"  ⚠️ {name} — could not resolve player ID, skipping"})
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

        emit({"type": "log", "msg": f"  Analyzing {name}  line: {line} Ks  {side} vs {opp}"})

        # Step 2: vs opponent H/A
        s2 = step2_vs_opp(pid, side, opp, line)
        # Step 3: last 10 H/A general
        s3 = step3_general_ha(pid, side, line)

        s2_over = s2.get("over_pct") if s2 else None
        s3_over = s3.get("over_pct") if s3 else None
        s2_g    = s2.get("games", 0) if s2 else 0
        s3_g    = s3.get("games", 0) if s3 else 0

        # Determine pick
        pick = None
        if s2_over is not None and s3_over is not None:
            if s2_over >= HIT_THRESH and s3_over >= HIT_THRESH:
                pick = "OVER"
            elif (100 - s2_over) >= HIT_THRESH and (100 - s3_over) >= HIT_THRESH:
                pick = "UNDER"

        # Build display strings
        def _pct_disp(res, direction):
            if res is None or res.get("over_pct") is None:
                g = res.get("games",0) if res else 0
                return f"N/A ({g}g)"
            pct = res["over_pct"] if direction == "over" else res["under_pct"]
            return f"{pct}% ({res['games']}g avg {res['avg_k']}K)"

        if pick == "OVER":
            s2_disp = _pct_disp(s2, "over")
            s3_disp = _pct_disp(s3, "over")
        elif pick == "UNDER":
            s2_disp = _pct_disp(s2, "under")
            s3_disp = _pct_disp(s3, "under")
        else:
            s2_disp = _pct_disp(s2, "over")
            s3_disp = _pct_disp(s3, "over")

        emit({"type": "log", "msg":
              f"    {'✅ ' + pick if pick else '— no pick'}  "
              f"S2(vs {opp[:12]}): {s2_disp}  S3(L{s3_g} H/A): {s3_disp}"})

        avg_k_s3 = s3.get("avg_k") if s3 else None

        all_results.append({
            "name":       name,
            "team":       pitcher_team,
            "opp":        opp,
            "side":       side,
            "line":       line,
            "over_odds":  pl.get("over_odds"),
            "under_odds": pl.get("under_odds"),
            "s2":         s2,
            "s2_disp":    s2_disp,
            "s3":         s3,
            "s3_disp":    s3_disp,
            "avg_k":      avg_k_s3,
            "pick":       pick,
        })

    # ── Sort confirmed picks by avg confidence ───────────────────────
    confirmed = [r for r in all_results if r["pick"]]
    no_pick   = [r for r in all_results if not r["pick"]]

    def _confidence(r):
        d = r["pick"]
        s2_pct = (r["s2"]["over_pct"] if d == "OVER" else r["s2"]["under_pct"]) if r["s2"] and r["s2"].get("over_pct") else 0
        s3_pct = (r["s3"]["over_pct"] if d == "OVER" else r["s3"]["under_pct"]) if r["s3"] and r["s3"].get("over_pct") else 0
        return (s2_pct + s3_pct) / 2

    confirmed.sort(key=_confidence, reverse=True)

    total_picks = len(confirmed)
    emit({"type": "log", "msg": f"✅ Pitcher K Picks done — {total_picks} pick{'s' if total_picks != 1 else ''} found"})

    # Return confirmed picks first, then no-pick pitchers (for full table view)
    return {"picks": confirmed, "all": all_results}
