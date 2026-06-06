
"""
pipeline.py — MLB Daily Picks master pipeline (web-optimized).
Runs all 4 steps with real-time progress via emit callback.
"""
import os, sys, time, json, math, requests
from concurrent.futures import ThreadPoolExecutor as _TPEx

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fic_cache        import get_step1_players_or_scrape, slate_has_tbd
from mlb_roster       import build_player_roster
from mlb_stats_splits import fetch_step2_ba, fetch_step3_ba, prefetch_game_logs


































# ── Step 4: L10 H/A hit-consistency (added) ──────────────────────────────
def fetch_step4_consistency(player_id, side: str, opp_name: str = "",
                            max_games: int = 10) -> dict:
    """S4 — Last 10 career H/A games vs THIS opponent (5 seasons back),
       counting games with 1+ hit. Used as ranking tiebreaker."""
    if not player_id:
        return {"hits_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        current_year = _dt.today().year
        seasons = list(range(current_year, current_year - 5, -1))
        matching = []
        for season in seasons:
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                is_home = sp.get("isHome", False)
                if (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                h  = int(stat.get("hits",   0) or 0)
                if ab < 1:
                    continue
                matching.append(1 if h >= 1 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        hits_games = sum(matching)
        if games == 0:
            return {"hits_games": 0, "games": 0, "display": "N/A", "score": 0}
        score = round(hits_games / games * 100)
        return {"hits_games": hits_games, "games": games,
                "display": f"{hits_games}/{games}", "score": score}
    except Exception:
        return {"hits_games": 0, "games": 0, "display": "ERR", "score": 0}


def _recent_hit_log(player_id, n: int = 5) -> list:
    """Last n games (any opponent), newest-first: date, hits, total bases, opp, H/A.
       Mirrors the pitcher recent-form log so hitter cards/under picks can show
       a 'recent form' click-through popup."""
    if not player_id:
        return []
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        games = []
        for season in range(cy, cy - 2, -1):        # current + prior season for recency
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):             # splits oldest-first → iterate newest-first
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                games.append({
                    "d":   (sp.get("date") or "")[5:],
                    "h":   int(stat.get("hits", 0) or 0),
                    "tb":  int(stat.get("totalBases", 0) or 0),
                    "opp": (sp.get("opponent", {}) or {}).get("name", ""),
                    "ha":  "H" if sp.get("isHome") else "A",
                })
                if len(games) >= n:
                    break
            if len(games) >= n:
                break
        return games
    except Exception:
        return []



from day_night_check  import get_game_time_type, find_espn_player_id, fetch_day_night_ba

TOP_N_ERA_PITCHERS = 30
MIN_IP_STARTER     = 30.0
MIN_GS_STARTER     = 5


def _get_active_player_ids(season: str) -> set:
    """IDs on every team's CURRENT active roster.
       A player on the IL (or optioned to the minors) is dropped from the
       active roster, so this set is used to exclude injured pitchers."""
    try:
        from concurrent.futures import ThreadPoolExecutor
        teams = requests.get(
            "https://statsapi.mlb.com/api/v1/teams",
            params={"sportId": 1, "season": season}, timeout=14,
        ).json().get("teams", [])
        team_ids = [t["id"] for t in teams if t.get("sport", {}).get("id") == 1]

        def _roster(tid):
            try:
                r = requests.get(
                    f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster",
                    params={"rosterType": "active"}, timeout=10,
                ).json()
                return {p["person"]["id"] for p in r.get("roster", [])}
            except Exception:
                return set()

        ids = set()
        with ThreadPoolExecutor(max_workers=10) as ex:
            for s in ex.map(_roster, team_ids):
                ids |= s
        return ids
    except Exception:
        return set()


def _get_top_era_starters(season: str, n: int = TOP_N_ERA_PITCHERS,
                          min_ip: float = MIN_IP_STARTER, min_gs: int = MIN_GS_STARTER):
    """Top-N lowest-ERA active STARTING pitchers.
       Scans ALL pitchers (playerPool=All), keeps only starters (started the
       majority of their appearances, >= min_gs starts and >= min_ip IP),
       drops anyone not on a current active roster (i.e. on the IL / optioned),
       then sorts by ERA ascending and returns the top N. This includes elite
       arms (e.g. Wheeler) who fall below MLB's innings qualifier, while
       excluding relievers, small samples, and injured pitchers."""
    try:
        active_ids = _get_active_player_ids(season)
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/stats",
            params={"stats": "season", "group": "pitching", "gameType": "R",
                    "season": season, "sportId": 1, "limit": 800,
                    "sortStat": "earnedRunAverage", "order": "asc",
                    "playerPool": "All"},
            timeout=14,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        starters = []
        for sp in splits:
            stat = sp.get("stat", {})
            pl   = sp.get("player", {})
            try:
                ip  = float(stat.get("inningsPitched", 0))
                era = float(stat.get("era", 99.0))
            except (ValueError, TypeError):
                continue
            gs = int(stat.get("gamesStarted", 0) or 0)
            g  = int(stat.get("gamesPlayed", 0) or 0)
            if gs < min_gs or ip < min_ip:          # small sample → skip
                continue
            if gs < g * 0.5:                         # mostly reliever → skip
                continue
            if active_ids and pl.get("id") not in active_ids:  # on IL → skip
                continue
            starters.append({"name": pl.get("fullName", ""), "era": era, "ip": ip})
        starters.sort(key=lambda p: p["era"])
        top_n = starters[:n]
        last_name_set = set()
        for p in top_n:
            parts = p["name"].lower().split()
            if parts:
                last_name_set.add(parts[-1])
        return last_name_set, top_n
    except Exception:
        return set(), []


def _pitcher_last_name(pitcher_raw: str) -> str:
    name = pitcher_raw.strip()
    if "." in name:
        last = name.split(".")[-1].strip()
    else:
        parts = name.split()
        last  = parts[-1] if parts else name
    return last.lower()


def _build_blurb(r):
    parts = []
    side_str = "home" if r.get("side") == "HOME" else "away"
    pitcher  = (r.get("pitcher") or "").strip()
    opp      = (r.get("opp") or "").strip()
    s1 = r.get("s1")
    if s1 and s1 > 0:
        label = f"vs {pitcher}" if pitcher else "vs today's pitcher"
        parts.append(f"Career .{round(s1 * 1000):03d} {label}")
    s4 = r.get("s4") or {}
    if s4.get("games", 0) >= 1:
        hits_g = s4.get("hits_games", 0)
        games  = s4.get("games", 0)
        opp_str = f" vs {opp}" if opp else ""
        parts.append(f"{hits_g} out of {games} {side_str} games with a hit{opp_str}")
    s3 = r.get("s3") or {}
    s3_ba = s3.get("ba")
    if s3_ba and s3_ba > 0 and "✅" in (s3.get("flag") or ""):
        parts.append(f".{round(s3_ba * 1000):03d} BA last 10 {side_str} games")
    return " · ".join(parts)


def _wilson_lb(hits: int, games: int, z: float = 1.96) -> float:
    """Lower bound of a 95% Wilson confidence interval for the S4 hit rate.
    Rewards sample size: a proven 9/10 outranks a lucky 4/4, while strong big
    samples (10/10) stay on top. Drives the final HIT-list rank order."""
    if not games:
        return 0.0
    p = hits / games
    den = 1.0 + z * z / games
    centre = p + z * z / (2 * games)
    margin = z * math.sqrt(p * (1 - p) / games + z * z / (4 * games * games))
    return (centre - margin) / den


def _fetch_bullpen_fatigue(run_date: str) -> dict:
    """
    Returns {team_full_name: {"bp_ip": float, "games": int, "taxed": bool}}
    for every MLB team that played in the last 3 days.
    Uses one MLB Stats API schedule call with boxscore hydration.
    Taxed = bullpen threw >= 9 IP across last 3 days (~3 IP/day avg).
    """
    from datetime import datetime, timedelta
    BP_TAXED = 9.0
    today   = datetime.strptime(run_date, "%Y-%m-%d")
    d_start = (today - timedelta(days=3)).strftime("%Y-%m-%d")
    d_end   = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        url  = (f"https://statsapi.mlb.com/api/v1/schedule?sportId=1"
                f"&startDate={d_start}&endDate={d_end}&hydrate=boxscore")
        data = requests.get(url, timeout=20).json()
    except Exception:
        return {}

    team_ip: dict = {}   # team_name -> {"bp_ip": float, "games": int}

    def _parse_ip(raw) -> float:
        try:
            parts = str(raw).split(".")
            full  = int(parts[0]) if parts[0] else 0
            outs  = int(parts[1]) if len(parts) > 1 and parts[1] else 0
            return full + outs / 3.0
        except Exception:
            return 0.0

    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            if game.get("status", {}).get("abstractGameState", "") != "Final":
                continue
            box = game.get("boxscore", {})
            if not box:
                continue
            for side in ("home", "away"):
                td = box.get("teams", {}).get(side, {})
                team_name = td.get("team", {}).get("name", "")
                if not team_name:
                    continue
                pitchers = td.get("pitchers", [])   # ordered list of player IDs
                players  = td.get("players", {})    # "ID<n>": {...}
                bp_ip = 0.0
                for idx, pid in enumerate(pitchers):
                    if idx == 0:         # starter — skip
                        continue
                    pdata = players.get(f"ID{pid}", {})
                    ip_raw = (pdata.get("stats", {})
                                   .get("pitching", {})
                                   .get("inningsPitched", "0"))
                    bp_ip += _parse_ip(ip_raw)
                acc = team_ip.setdefault(team_name, {"bp_ip": 0.0, "games": 0})
                acc["bp_ip"]  += bp_ip
                acc["games"]  += 1

    return {
        name: {"bp_ip": round(d["bp_ip"], 1),
               "games": d["games"],
               "taxed": d["bp_ip"] >= BP_TAXED}
        for name, d in team_ip.items()
    }


# ── Platoon split helpers ────────────────────────────────────────────────
_PITCHER_HAND_CACHE: dict = {}
_PLATOON_CACHE:      dict = {}

def _get_pitcher_hand(pitcher_id):
    """Pitcher throwing hand: 'R', 'L', or None."""
    if not pitcher_id:
        return None
    if pitcher_id in _PITCHER_HAND_CACHE:
        return _PITCHER_HAND_CACHE[pitcher_id]
    try:
        r = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}",
            params={"fields": "people,pitchHand,code"}, timeout=8)
        hand = (r.json().get("people", [{}])[0]
                        .get("pitchHand", {}).get("code"))
        _PITCHER_HAND_CACHE[pitcher_id] = hand
        return hand
    except Exception:
        _PITCHER_HAND_CACHE[pitcher_id] = None
        return None

def _get_batter_platoon(batter_id):
    """Career platoon splits: bat_hand (R/L/S), vs_r {ba,ab}, vs_l {ba,ab}."""
    if not batter_id:
        return {}
    if batter_id in _PLATOON_CACHE:
        return _PLATOON_CACHE[batter_id]
    try:
        ph = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{batter_id}",
            params={"fields": "people,batSide,code"}, timeout=8)
        bat_hand = (ph.json().get("people", [{}])[0]
                              .get("batSide", {}).get("code"))  # R, L, or S
        sr = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{batter_id}/stats",
            params={"stats": "career", "group": "hitting",
                    "sitCodes": "vr,vl", "gameType": "R"}, timeout=10)
        splits = sr.json().get("stats", [{}])[0].get("splits", [])
        vs_r = vs_l = None
        for sp in splits:
            code = sp.get("split", {}).get("code", "")
            st   = sp.get("stat", {})
            ab   = int(st.get("atBats", 0) or 0)
            h    = int(st.get("hits",   0) or 0)
            ba   = round(h / ab, 3) if ab > 0 else None
            if code == "vr":   vs_r = {"ba": ba, "ab": ab}
            elif code == "vl": vs_l = {"ba": ba, "ab": ab}
        result = {"bat_hand": bat_hand, "vs_r": vs_r, "vs_l": vs_l}
        _PLATOON_CACHE[batter_id] = result
        return result
    except Exception:
        _PLATOON_CACHE[batter_id] = {}
        return {}

def run_pipeline(run_date: str, emit=None) -> dict:
    if emit is None:
        emit = lambda _: None

    t_start   = time.time()
    date_espn = run_date.replace("-", "")

    def log(msg, type_="log"):
        emit({"type": type_, "msg": msg})

    # ── STEP 1 ────────────────────────────────────────────────────────
    emit({"type": "section", "msg": "Step 1 — Loading player list from MLB Stats API"})
    step1 = get_step1_players_or_scrape(run_date, emit=emit)
    top30 = step1
    pitcher_map = {p["batter"]: p["pitcher"] for p in top30}
    emit({"type": "step1_done", "msg": f"✅ {len(top30)} players loaded", "count": len(top30)})

    # ── ESPN Schedule ─────────────────────────────────────────────────
    emit({"type": "section", "msg": "ESPN — Fetching today's schedule"})
    espn_r = requests.get(
        f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_espn}",
        timeout=15).json()
    team_schedule = {}
    for event in espn_r.get("events", []):
        comps = event.get("competitions", [{}])[0]
        g_start = event.get("date", "")   # ISO UTC first-pitch time (e.g. 2026-05-30T23:05Z)
        home = away = None
        for t in comps.get("competitors", []):
            if t["homeAway"] == "home": home = t["team"]
            else:                       away = t["team"]
        if home and away:
            team_schedule[home["displayName"]] = {
                "side": "HOME", "opponent": away["displayName"],
                "opp_slug": away["displayName"].lower().replace(" ", "-"),
                "game_start": g_start}
            team_schedule[away["displayName"]] = {
                "side": "AWAY", "opponent": home["displayName"],
                "opp_slug": home["displayName"].lower().replace(" ", "-"),
                "game_start": g_start}
    log(f"✅ {len(team_schedule) // 2} games found today")

    # ── Roster Lookup ─────────────────────────────────────────────────
    emit({"type": "section", "msg": "Roster — Resolving player teams via MLB Stats API"})
    log(f"Looking up {len(top30)} players (this takes ~30 seconds)…")
    roster = build_player_roster([p["batter"] for p in top30], date_espn, pitcher_map)
    found = len([v for v in roster.values() if v.get("player_id")])
    log(f"✅ Resolved {found}/{len(top30)} players")

    # ── STEPS 2 & 3 ───────────────────────────────────────────────────
    emit({"type": "section", "msg": "Steps 2 & 3 — Fetching MLB Stats API H/A game logs"})
    all_player_ids = [roster.get(p["batter"], {}).get("player_id") or p.get("player_id") for p in top30]
    log(f"  Pre-fetching game logs for {len(all_player_ids)} players (parallel)...")
    prefetch_game_logs(all_player_ids)
    log("  ✅ Game logs cached — running splits...")

    results = []
    for i, p in enumerate(top30):
        name  = p["batter"]
        info  = roster.get(name, {})
        # Fallback: fic_cache already resolved player_id + team_name. Use them
        # when build_player_roster failed to match the abbreviated name (e.g.
        # "H. Lee", "A. Rosario" — common surnames with multiple MLB players).
        if not info.get("player_id") and p.get("player_id"):
            info = dict(info)
            info["player_id"] = p["player_id"]
        if not info.get("team_name") and p.get("team_name"):
            info = dict(info)
            info["team_name"] = p.get("team_name", "")
        slug  = info.get("slug", "")
        team  = info.get("team_name", "")
        sched = team_schedule.get(team, {})
        if not sched and team:
            tl = team.lower()
            for k, v in team_schedule.items():
                if tl in k.lower() or k.lower() in tl:
                    sched = v
                    break
        side     = sched.get("side", "")
        opp_name = sched.get("opponent", "")
        game_start = sched.get("game_start", "")

        emit({"type": "progress", "current": i + 1, "total": len(top30), "name": name})

        player_id = info.get("player_id")
        if not side or not player_id:
            emit({"type": "player_skip", "name": name, "reason": "no game today"})
            continue

        s2 = fetch_step2_ba(player_id, side, opp_name)
        s3 = fetch_step3_ba(player_id, side)

        dq = []
        if s2["ba"] is None:
            dq.append("S2 N/A")
        elif "✅" in s2["flag"] and s2["ba"] < 0.250:
            dq.append(f"S2 {s2['display']}")
        if s3["ba"] is None:
            dq.append("S3 N/A")
        elif "✅" in s3["flag"] and s3["ba"] < 0.250:
            dq.append(f"S3 {s3['display']}")

        s2s   = round(s2["ba"] * 1000) if s2["ba"] and "✅" in s2["flag"] else 0
        s3s   = round(s3["ba"] * 1000) if s3["ba"] and "✅" in s3["flag"] else 0
        total = round(p["ba"] * 1000) + s2s + s3s if not dq else 0

        player_result = {
            "name": name, "pos": p["pos"], "s1": p["ba"],
            "team": team, "opp": opp_name, "side": side, "slug": slug,
            "full_name": info.get("full_name", name),
            "pitcher": pitcher_map.get(name, ""),
            "s2": s2, "s3": s3, "total": total,
            "dq": bool(dq), "dq_reason": " & ".join(dq),
            "player_id": player_id,
            "game_start": game_start,
        }
        results.append(player_result)

        if dq:
            emit({"type": "player_dq", "name": name,
                  "s1": f"{p['ba']:.3f}", "s2": s2["display"], "s3": s3["display"],
                  "reason": " & ".join(dq)})
        else:
            emit({"type": "player_ok", "name": name,
                  "s1": f"{p['ba']:.3f}", "s2": s2["display"], "s3": s3["display"],
                  "opp": opp_name, "side": side, "total": total})

    # ── STEP 4 ────────────────────────────────────────────────────────
    emit({"type": "section", "msg": "Step 4 — ESPN Day/Night BA filter"})
    qualified, dn_dq = [], []
    for r in [x for x in results if not x["dq"]]:
        team      = roster.get(r["name"], {}).get("team_name", "")
        full_name = r.get("full_name", r["name"])
        gtype     = get_game_time_type(team, date_espn)
        eid       = find_espn_player_id(full_name) or r.get("player_id")
        dn = (fetch_day_night_ba(eid, gtype)
              if eid and gtype != "unknown"
              else {"display": "N/A", "flag": "❌ skip", "dq": False, "ba": None, "ab": None})
        label = "DAY" if gtype == "day" else "NIGHT"
        r["dn"] = dn
        r["dn_label"] = label
        if dn["dq"]:
            r["dq"] = True
            r["dq_reason"] = f"Step 4 {label} {dn['display']} < .200"
            dn_dq.append(r)
            emit({"type": "dn_dq", "name": r["name"], "label": label, "display": dn["display"]})
        else:
            qualified.append(r)
            emit({"type": "dn_ok", "name": r["name"], "label": label, "display": dn["display"]})

    # ── STEP 5 ────────────────────────────────────────────────────────
    emit({"type": "section", "msg": f"Step 5 — Pitcher ERA filter (top {TOP_N_ERA_PITCHERS} lowest ERA)"})
    era_qualified, era_dq = [], []
    top_era_lastnames, top_era_list = _get_top_era_starters(run_date[:4])
    # Fetch MLB schedule probable pitchers as fallback when FIC pitcher data is missing
    try:
        from under_picks import _get_probable_pitchers as _mlb_probs
        mlb_probable = _mlb_probs(run_date)  # team_name -> {"name": ..., "id": ...}
    except Exception:
        mlb_probable = {}
    if top_era_lastnames:
        emit({"type": "log", "msg": f"✅ Top {TOP_N_ERA_PITCHERS} ERA: " +
              ", ".join(f"{p['name']} ({p['era']:.2f})" for p in top_era_list)})
        for r in qualified:
            pitcher_raw = r.get("pitcher", "")
            # Fallback: if FIC has no pitcher, look up from MLB schedule by opponent team
            if not pitcher_raw or pitcher_raw.strip().lower() in ("", "tbd", "unknown"):
                opp = r.get("opp", "").lower()
                stop = {"the", "of", "los", "san", "new", "de"}
                o_words = set(opp.split()) - stop
                for t, pinfo in mlb_probable.items():
                    t_words = set(t.lower().split()) - stop
                    if t_words & o_words:
                        pitcher_raw = pinfo.get("name", "")
                        r["pitcher"] = pitcher_raw
                        break
            pitcher_last = _pitcher_last_name(pitcher_raw)
            if pitcher_last and pitcher_last in top_era_lastnames:
                matched_era = next((p["era"] for p in top_era_list
                                    if p["name"].lower().endswith(pitcher_last)), None)
                era_str = f" ERA {matched_era:.2f}" if matched_era else ""
                r["dq"] = True
                r["dq_reason"] = f"Facing top-ERA pitcher {pitcher_raw}{era_str}"
                era_dq.append(r)
                emit({"type": "log", "msg": f"  ❌ {r['name']} — facing {pitcher_raw}{era_str}"})
            else:
                era_qualified.append(r)
    else:
        emit({"type": "log", "msg": "⚠️ ERA rankings unavailable — skipping Step 5"})
        era_qualified = qualified

    # ── LINEUP CHECK ─────────────────────────────────────────────
    emit({"type": "section", "msg": "Lineup Check — MLB Stats API + Rotowire"})
    dq_lineup = []
    try:
        from lineup_check import build_lineup_map, get_lineup_status
        id_map, name_map, teams_confirmed, rw_lineups, rw_teams = build_lineup_map(run_date)
        projected = len(rw_teams - teams_confirmed)
        emit({"type": "log", "msg": f"✅ Coverage: {len(teams_confirmed)} confirmed + {projected} projected teams"})
        lineup_qualified = []
        for r in era_qualified:
            info      = roster.get(r["name"], {})
            player_id = info.get("player_id") or r.get("player_id")
            full_name = r.get("full_name") or info.get("full_name", r["name"])
            team_name = info.get("team_name", "")
            status    = get_lineup_status(player_id, full_name, team_name,
                                          id_map, name_map, teams_confirmed,
                                          rw_lineups, rw_teams)
            r["lineup_status"] = status
            if status == "NOT_IN_LINEUP":
                r["dq"] = True
                r["dq_reason"] = "Not in lineup"
                dq_lineup.append(r)
                emit({"type": "lineup_ok", "name": r["name"], "status": "NOT_IN_LINEUP"})
            else:
                lineup_qualified.append(r)
                emit({"type": "lineup_ok", "name": r["name"], "status": status})
        emit({"type": "log", "msg": f"✅ Lineup: {len(lineup_qualified)} in/TBD, {len(dq_lineup)} removed"})
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ Lineup check skipped: {exc}"})
        lineup_qualified = era_qualified
        for r in lineup_qualified:
            r.setdefault("lineup_status", "TBD")

    # ── Platoon Splits ─────────────────────────────────────────────────────
    emit({"type": "section", "msg": "Platoon — Fetching batter/pitcher handedness"})
    try:
        pit_id_map = {tn: pi.get("id") for tn, pi in mlb_probable.items() if pi.get("id")}
        _pltn_bids = list({r.get("player_id") for r in lineup_qualified if r.get("player_id")})
        _pltn_pids = list({pid for pid in pit_id_map.values() if pid})
        with _TPEx(max_workers=8) as _ex:
            list(_ex.map(_get_batter_platoon, _pltn_bids))
        with _TPEx(max_workers=8) as _ex:
            list(_ex.map(_get_pitcher_hand, _pltn_pids))
        _stop_w = {"the", "of", "los", "san", "new", "de"}
        def _match_opp(opp):
            opp_l = opp.lower()
            for tn, pid in pit_id_map.items():
                if tn.lower() == opp_l: return pid
            for tn, pid in pit_id_map.items():
                if (set(tn.lower().split()) - _stop_w) & (set(opp_l.split()) - _stop_w):
                    return pid
            return None
        enriched = 0
        for r in lineup_qualified:
            batter_id = r.get("player_id")
            pit_id    = _match_opp(r.get("opp", ""))
            pl        = _get_batter_platoon(batter_id)
            bat_hand  = pl.get("bat_hand")
            pit_hand  = _get_pitcher_hand(pit_id) if pit_id else None
            if bat_hand and pit_hand:
                eff = ("L" if pit_hand == "R" else "R") if bat_hand == "S" else bat_hand
                split = pl.get("vs_r" if pit_hand == "R" else "vs_l") or {}
                ba   = split.get("ba")
                ab   = split.get("ab", 0)
                adv  = (eff == "L" and pit_hand == "R") or (eff == "R" and pit_hand == "L")
                ba_d = (".%03d" % int(ba * 1000)) if ba is not None else "N/A"
                r["platoon"] = {
                    "bat_hand": bat_hand, "pit_hand": pit_hand,
                    "ba": ba, "ab": ab, "adv": adv,
                    "display": f"{ba_d} ({ab}AB)",
                    "label": f"{'L' if bat_hand=='S' else bat_hand}HB vs {pit_hand}HP",
                }
                enriched += 1
            else:
                r["platoon"] = None
        emit({"type": "log", "msg": f"✅ Platoon: {enriched}/{len(lineup_qualified)} enriched"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Platoon enrichment skipped: {_exc}"})
        for r in lineup_qualified:
            r.setdefault("platoon", None)

    # ── S4 (L10 H/A consistency ≥50%) — filter then re-rank ──────────
    emit({"type": "section", "msg": "S4 (L10 H/A consistency ≥50%) + S5 (D/N BA) — filter & re-rank"})
    s4_qualified, s4_dq = [], []
    for r in lineup_qualified:
        info       = roster.get(r["name"], {})
        player_id  = info.get("player_id") or r.get("player_id")
        s4         = fetch_step4_consistency(player_id, r["side"], r.get("opp", ""))
        r["s4"]    = s4
        # DQ if S4 has qualifying games but hit rate < 60%
        if s4["games"] > 0 and s4["score"] < 60:
            r["dq"] = True
            r["dq_reason"] = f"S4 {s4['display']} ({s4['score']}%) < 60% H/A hit rate vs opp"
            s4_dq.append(r)
            emit({"type": "log", "msg": f"  ❌ {r['name']}: S4 {s4['display']} ({s4['score']}%) < 60% — DQ"})
            continue
        dn_ba      = (r.get("dn", {}) or {}).get("ba")
        s5_score   = round(dn_ba * 1000) if dn_ba else 0
        r["s5"]    = {"ba": dn_ba, "score": s5_score,
                      "display": f"{dn_ba:.3f}" if dn_ba else "N/A"}
        # S5 (day/night BA) IS added to the total — total = (S1+S2+S3+S5)×1000 — and
        # also filters (DQ below the cutoff). S4 (H/A hit rate vs opp) is NOT in the
        # total; it drives the final HIT-list order (see sort below).
        r["total"] = (r.get("total", 0) or 0) + s5_score
        emit({"type": "log",
              "msg": f"  ✅ {r['name']}: S4 {s4['display']} ({s4['score']}%) ranking signal | "
                     f"S5 {r['s5']['display']} (+{s5_score}) → total {r['total']}"})
        r["blurb"] = _build_blurb(r)
        s4_qualified.append(r)

    emit({"type": "log", "msg": f"S4 filter: {len(s4_qualified)} pass, {len(s4_dq)} DQ'd (<50%)"})
    # Rank by model total (points) — S4 hit rate is displayed on the card for
    # consistency reference only and does not affect ordering.
    all_ranked = sorted(
        s4_qualified,
        key=lambda x: x.get("total", 0),
        reverse=True,
    )
    top9     = all_ranked[:10]
    also_ran = all_ranked[10:]

    # Recent form: last 5 games (date/opp/hits/total-bases) for the click-through popup
    for _hp in top9 + also_ran:
        _hp["recent_hit_log"] = _recent_hit_log(_hp.get("player_id"))

    # ── Under Picks ───────────────────────────────────────────────────
    try:
        from under_picks import run_under_picks
        under_picks_list = run_under_picks(run_date, team_schedule, emit=emit,
                                           top_era=top_era_lastnames, top_era_list=top_era_list)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ Under Picks skipped: {exc}"})
        under_picks_list = []

    # ── Enrich top9 + also_ran with hit odds (0.5 line "to record a hit") ──
    try:
        from under_picks import HIT_ODDS as _HIT_ODDS, _norm_name as _nn
        # Build last-name index for fallback (only use when unambiguous)
        _last_idx: dict = {}
        for _k, _v in _HIT_ODDS.items():
            _parts = _k.split()
            if _parts:
                _last = _parts[-1]
                _last_idx[_last] = (_last_idx[_last] + [(_k, _v)]) if _last in _last_idx else [(_k, _v)]

        def _name_variants(raw: str):
            """Generate all name variants to try against HIT_ODDS."""
            s = _nn(raw)
            if not s: return []
            variants = [s]
            parts = s.split()
            if len(parts) >= 2:
                last  = parts[-1]
                first_parts = parts[:-1]  # everything before last name
                # single initial: "J. Last"
                variants.append(f"{first_parts[0][0]}. {last}")
                # multi-word first: "Jung Hoo Lee" → "J.H. Lee"
                if len(first_parts) >= 2:
                    initials = ".".join(p[0] for p in first_parts) + "."
                    variants.append(f"{initials} {last}")
                # hyphen variant: "ha seong kim" → "ha-seong kim"
                if len(first_parts) >= 2:
                    variants.append(f"{'-'.join(first_parts)} {last}")
                # no-space variant: "junghoo lee"
                if len(first_parts) >= 2:
                    variants.append(f"{''.join(first_parts)} {last}")
            return variants

        def _lookup_odds(p):
            candidates = []
            for field in ("full_name", "name"):
                candidates.extend(_name_variants(p.get(field, "")))
            # 1. exact match on any variant
            for v in candidates:
                if v in _HIT_ODDS: return _HIT_ODDS[v]
            # 2. unambiguous last-name fallback (skip common last names)
            seen_last = set()
            for v in candidates:
                parts = v.split()
                last = parts[-1] if parts else ""
                if not last or last in seen_last: continue
                seen_last.add(last)
                matches = _last_idx.get(last, [])
                if len(matches) == 1: return matches[0][1]
            return None

        for _p in top9 + also_ran:
            _p["hit_odds"] = _lookup_odds(_p)
        emit({"type": "log", "msg": f"  ✅ Hit odds matched for {sum(1 for p in top9+also_ran if p.get('hit_odds') is not None)}/{len(top9)+len(also_ran)} picks"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Hit odds enrichment skipped: {_exc}"})
    # Inject team + first-pitch time into each under pick (reverse-lookup from team_schedule)
    for _up in under_picks_list:
        _side, _opp = _up.get("side", ""), _up.get("opp", "")
        for _t, _sched in team_schedule.items():
            if _sched.get("side") == _side and _sched.get("opponent") == _opp:
                _up["team"] = _t
                _up["game_start"] = _sched.get("game_start", "")
                break
        _up.setdefault("team", "")
        _up.setdefault("game_start", "")
        _up["recent_hit_log"] = _recent_hit_log(_up.get("batter_id"))

    # ── Pitcher K Picks ───────────────────────────────────────────────
    try:
        from pitcher_k import run_pitcher_k_picks
        pitcher_k_result = run_pitcher_k_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ Pitcher K Picks skipped: {exc}"})
        pitcher_k_result = {"picks": [], "all": []}

    # Stamp first-pitch time on pitcher K picks so the frontend can hide started games.
    # Pitcher team names come from the MLB Stats API; match them to the ESPN schedule
    # (exact key first, else substring) to pull that game's start time.
    def _game_start_for(team_name):
        if not team_name:
            return ""
        s = team_schedule.get(team_name)
        if s:
            return s.get("game_start", "")
        tl = team_name.lower()
        for _k, _v in team_schedule.items():
            if tl in _k.lower() or _k.lower() in tl:
                return _v.get("game_start", "")
        return ""
    for _pk in (pitcher_k_result.get("picks", []) + pitcher_k_result.get("all", [])):
        _pk["game_start"] = _game_start_for(_pk.get("team", ""))

    # Stamp first-pitch time on the 3 pitcher prop categories (hits allowed / outs /
    # earned runs) too, so the frontend can hide games that already started.
    pitcher_props = pitcher_k_result.get("props", {}) or {}
    for _mkt, _bucket in pitcher_props.items():
        for _pp in (_bucket.get("picks", []) + _bucket.get("all", [])):
            _pp["game_start"] = _game_start_for(_pp.get("team", ""))

    # ── Runs Picks (Batter Runs Scored, Over/Under 0.5) ───────────────
    try:
        from under_picks import run_runs_picks
        runs_picks_list = run_runs_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ Runs Picks skipped: {exc}"})
        runs_picks_list = []
    for _rp in runs_picks_list:
        _rp["game_start"] = _game_start_for(_rp.get("team", ""))

    # ── TB Under Picks (batter total bases Under 1.5) ─────────────────────
    try:
        from under_picks import run_tb_under_picks
        tb_picks_list = run_tb_under_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ TB Under picks skipped: {exc}"})
        tb_picks_list = []
    for _tp in tb_picks_list:
        _tp["game_start"] = _game_start_for(_tp.get("team", ""))

    # ── RBI Picks (Batter RBIs, Over/Under 0.5) ───────────────────────────
    try:
        from under_picks import run_rbi_picks
        rbi_picks_list = run_rbi_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ RBI picks skipped: {exc}"})
        rbi_picks_list = []
    for _xp in rbi_picks_list:
        _xp["game_start"] = _game_start_for(_xp.get("team", ""))

    # ── Game-environment re-ranking inputs (weather + home-plate umpire) ──
    # Build the shared target list ONCE; ballpark/weather and umpire each stamp
    # onto it (Phase A), then a single combined Phase B re-ranks every category
    # (reorder only — qualification gates are never touched, so the same picks
    # appear, just in a different order). Either factor missing -> neutral 1.0.
    _rr_targets = list(top9) + list(also_ran) + list(under_picks_list) + list(runs_picks_list) + list(tb_picks_list) + list(rbi_picks_list)
    _rr_targets += pitcher_k_result.get("picks", []) + pitcher_k_result.get("all", [])
    for _b in pitcher_props.values():
        _rr_targets += _b.get("picks", []) + _b.get("all", [])

    # ── Phase A1: ballpark + weather env chip (per HOME park) ──
    # Per-game factor = Savant park factor x Open-Meteo temp/wind. Silent on
    # failure (env=None -> no chip, neutral re-rank).
    try:
        from ballpark import game_env
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Ballpark env unavailable: {_exc}"})
        game_env = None
    if game_env is not None:
        def _home_team(team_name):
            if not team_name:
                return ""
            s = team_schedule.get(team_name)
            if s:
                return team_name if s.get("side") == "HOME" else s.get("opponent", "")
            tl = team_name.lower()
            for _k, _v in team_schedule.items():
                if tl in _k.lower() or _k.lower() in tl:
                    return _k if _v.get("side") == "HOME" else _v.get("opponent", "")
            return ""
        _env_cache = {}
        def _env_for(team_name, game_start):
            home = _home_team(team_name)
            if not home:
                return None
            if home not in _env_cache:
                try:
                    _env_cache[home] = game_env(home, game_start)
                except Exception:
                    _env_cache[home] = None
            return _env_cache[home]
        for _pp in _rr_targets:
            try:
                _pp["env"] = _env_for(_pp.get("team", ""), _pp.get("game_start", ""))
            except Exception:
                _pp["env"] = None
        emit({"type": "log", "msg": f"  ✅ Ballpark/weather env computed for {len(_env_cache)} stadium(s)"})

    # ── Phase A2: home-plate umpire effect (per game) ──
    # Each game's HP umpire + how their games trend on K/BB/runs vs league
    # (statsapi only, cached per day). Silent on failure / unposted officials.
    try:
        from umpire import build_today as _ump_build, lookup as _ump_lookup
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Umpire effect unavailable: {_exc}"})
        _ump_build = None
        _ump_lookup = None
    if _ump_build is not None:
        try:
            _ump_map = _ump_build(run_date, emit=emit) or {}
        except Exception as _exc:
            emit({"type": "log", "msg": f"⚠️ Umpire effect skipped: {_exc}"})
            _ump_map = {}
        if _ump_map:
            for _pp in _rr_targets:
                try:
                    _pp["ump"] = _ump_lookup(_ump_map, _pp.get("team", ""))
                except Exception:
                    _pp["ump"] = None

    # ── Phase A3: bullpen fatigue (chip-only, last 3 days) ──
    # Hitters get bp_opp = opponent team's bullpen usage.
    # Pitchers get bp_own = their own team's bullpen usage (affects how deep
    # they might pitch if the bullpen is taxed). Chip displayed in main.py;
    # no re-ranking here — signal only.
    try:
        _bp_map = _fetch_bullpen_fatigue(run_date)
        if _bp_map:
            def _bp_for(team_name: str):
                if not team_name:
                    return None
                d = _bp_map.get(team_name)
                if d:
                    return d
                tl = team_name.lower()
                for _k, _v in _bp_map.items():
                    if tl in _k.lower() or _k.lower() in tl:
                        return _v
                return None

            _hitter_bp = (list(top9) + list(also_ran)
                          + list(under_picks_list) + list(runs_picks_list))
            _pitcher_bp: list = (list(pitcher_k_result.get("picks", []))
                                 + list(pitcher_k_result.get("all", [])))
            for _bk in pitcher_props.values():
                _pitcher_bp += list(_bk.get("picks", [])) + list(_bk.get("all", []))

            for _pp in _hitter_bp:
                try:
                    _pp["bp_opp"] = _bp_for(_pp.get("opp", ""))
                except Exception:
                    _pp["bp_opp"] = None
            for _pp in _pitcher_bp:
                try:
                    _pp["bp_own"] = _bp_for(_pp.get("team", ""))
                except Exception:
                    _pp["bp_own"] = None
            emit({"type": "log",
                  "msg": f"  ✅ Bullpen fatigue attached ({len(_bp_map)} teams)"})
    except Exception as _bp_exc:
        emit({"type": "log", "msg": f"⚠️ Bullpen fatigue skipped: {_bp_exc}"})

    # ── Phase B: combined env + umpire RE-RANKING (reorder only) ──
    # Offense axis = weather/park env × umpire RUN factor (a wide strike zone
    # suppresses offense; a tight zone inflates it). Pitcher Walks uses the
    # umpire WALK factor. Pitcher Strikeouts are re-ranked CLIENT-SIDE in
    # main.py (the only category the frontend re-sorts) via the K factor.
    # Outs stay excluded. Both factors default to 1.0 -> base order preserved.
    def _envf(p):
        e = p.get("env")
        try:
            f = float(e.get("factor")) if e else 1.0
        except Exception:
            f = 1.0
        return f if f and f > 0 else 1.0
    def _umpf(p, comp):
        u = p.get("ump")
        try:
            f = float(u.get(comp)) if u else 1.0
        except Exception:
            f = 1.0
        return f if f and f > 0 else 1.0
    def _offf(p):                       # combined offense multiplier
        return _envf(p) * _umpf(p, "rFactor")

    # Hitters ("to record a hit", all OVER): points × offense factor, then
    # re-split the headline Top-10 vs Money Ball from the same pool.
    _hit_pool = list(top9) + list(also_ran)
    _hit_pool.sort(
        key=lambda x: x.get("total", 0) * _offf(x),
        reverse=True,
    )
    top9     = _hit_pool[:10]
    also_ran = _hit_pool[10:]

    # Under 1.5 hits / TB (all UNDER): under_score is lower=colder=better, so
    # × offense factor pushes hitter-park / tight-zone unders DOWN the board.
    under_picks_list.sort(key=lambda p: (p.get("under_score", 0) * _offf(p),
                                         p.get("name", "")))

    # Runs 0.5: OVERs boosted (-wilson × offense), UNDERs penalized (score ×
    # offense). OVER block stays ahead of UNDER block, same as run_runs_picks.
    runs_picks_list.sort(key=lambda p: (
        0 if p.get("pick") == "OVER" else 1,
        (-p.get("wilson", 0) * _offf(p)) if p.get("pick") == "OVER"
        else (p.get("score", 0) * _offf(p)),
        -p.get("games", 0),
    ))

    # Pitcher props — Hits Allowed + Earned Runs use the offense axis; Walks use
    # the umpire walk factor (wide zone -> fewer walks -> Under boosted). Outs +
    # K excluded here. OVER × factor, UNDER × 1/factor.
    for _mkt, _bk in pitcher_props.items():
        if _mkt in ("pitcher_hits_allowed", "pitcher_earned_runs"):
            _bk["picks"].sort(
                key=lambda x: abs((x.get("blended") if x.get("blended") is not None else 0)
                                  - (x.get("line") or 0))
                              * (_offf(x) if x.get("pick") == "OVER" else 1.0 / _offf(x)),
                reverse=True,
            )
        elif _mkt == "pitcher_walks":
            _bk["picks"].sort(
                key=lambda x: abs((x.get("blended") if x.get("blended") is not None else 0)
                                  - (x.get("line") or 0))
                              * (_umpf(x, "bbFactor") if x.get("pick") == "OVER"
                                 else 1.0 / _umpf(x, "bbFactor")),
                reverse=True,
            )
    emit({"type": "log", "msg": "  ✅ Env + umpire re-ranking applied (reorder only, gates untouched)"})

    elapsed = round(time.time() - t_start, 1)
    result = {
        "date": run_date, "top9": top9, "also_ran": also_ran,
        "under_picks": under_picks_list, "runs_picks": runs_picks_list, "tb_picks": tb_picks_list, "rbi_picks": rbi_picks_list,
        "all_qualified": era_qualified,
        "dq_s1_s3": [x for x in results if x["dq"] and x not in dn_dq and x not in era_dq and x not in dq_lineup and x not in s4_dq],
        "dq_step4": dn_dq, "dq_step5": era_dq, "dq_lineup": dq_lineup, "dq_s4": s4_dq, "pitcher_k": pitcher_k_result,
        "pitcher_props": pitcher_props,
        "stats": {"step1_count": len(top30), "games": len(team_schedule) // 2,
                  "elapsed": elapsed, "picks": len(top9),
                  "under_count": len(under_picks_list),
                  "runs_count": len(runs_picks_list),
                  "tb_count": len(tb_picks_list),
                  "rbi_count": len(rbi_picks_list),
                  "pitcher_k_count": len(pitcher_k_result.get("picks", [])),
                  "prop_counts": {m: len(b.get("picks", [])) for m, b in pitcher_props.items()},
                  "has_tbd": slate_has_tbd(run_date)},
    }
    emit({"type": "done", "result": result})
    return result
