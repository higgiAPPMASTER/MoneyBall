
"""
pipeline.py — MLB Daily Picks master pipeline (web-optimized).
Runs all 4 steps with real-time progress via emit callback.
"""
import os, sys, time, json, requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fic_cache        import get_step1_players_or_scrape
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



from day_night_check  import get_game_time_type, find_espn_player_id, fetch_day_night_ba

TOP_N_ERA_PITCHERS = 20
MIN_IP_STARTER     = 20.0


def _get_top_era_starters(season: str, n: int = TOP_N_ERA_PITCHERS, min_ip: float = MIN_IP_STARTER):
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/stats",
            params={"stats": "season", "group": "pitching", "gameType": "R",
                    "season": season, "sportId": 1, "limit": 300,
                    "sortStat": "earnedRunAverage", "order": "asc"},
            timeout=14,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        qualified = []
        for sp in splits:
            stat = sp.get("stat", {})
            try:
                ip  = float(stat.get("inningsPitched", 0))
                era = float(stat.get("era", 99.0))
            except (ValueError, TypeError):
                continue
            if ip >= min_ip:
                qualified.append({"name": sp.get("player", {}).get("fullName", ""),
                                   "era": era, "ip": ip})
        top_n = qualified[:n]
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
        home = away = None
        for t in comps.get("competitors", []):
            if t["homeAway"] == "home": home = t["team"]
            else:                       away = t["team"]
        if home and away:
            team_schedule[home["displayName"]] = {
                "side": "HOME", "opponent": away["displayName"],
                "opp_slug": away["displayName"].lower().replace(" ", "-")}
            team_schedule[away["displayName"]] = {
                "side": "AWAY", "opponent": home["displayName"],
                "opp_slug": home["displayName"].lower().replace(" ", "-")}
    log(f"✅ {len(team_schedule) // 2} games found today")

    # ── Roster Lookup ─────────────────────────────────────────────────
    emit({"type": "section", "msg": "Roster — Resolving player teams via MLB Stats API"})
    log(f"Looking up {len(top30)} players (this takes ~30 seconds)…")
    roster = build_player_roster([p["batter"] for p in top30], date_espn, pitcher_map)
    found = len([v for v in roster.values() if v.get("player_id")])
    log(f"✅ Resolved {found}/{len(top30)} players")

    # ── STEPS 2 & 3 ───────────────────────────────────────────────────
    emit({"type": "section", "msg": "Steps 2 & 3 — Fetching MLB Stats API H/A game logs"})
    all_player_ids = [roster.get(p["batter"], {}).get("player_id") for p in top30]
    log(f"  Pre-fetching game logs for {len(all_player_ids)} players (parallel)...")
    prefetch_game_logs(all_player_ids)
    log("  ✅ Game logs cached — running splits...")

    results = []
    for i, p in enumerate(top30):
        name  = p["batter"]
        info  = roster.get(name, {})
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
        elif "✅" in s2["flag"] and s2["ba"] < 0.225:
            dq.append(f"S2 {s2['display']}")
        if s3["ba"] is None:
            dq.append("S3 N/A")
        elif "✅" in s3["flag"] and s3["ba"] < 0.225:
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
            player_id = info.get("player_id")
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

    # ── S4 (L10 H/A consistency ≥50%) — filter then re-rank ──────────
    emit({"type": "section", "msg": "S4 (L10 H/A consistency ≥50%) + S5 (D/N BA) — filter & re-rank"})
    s4_qualified, s4_dq = [], []
    for r in lineup_qualified:
        info       = roster.get(r["name"], {})
        player_id  = info.get("player_id")
        s4         = fetch_step4_consistency(player_id, r["side"], r.get("opp", ""))
        r["s4"]    = s4
        # DQ if S4 has qualifying games but hit rate < 50%
        if s4["games"] > 0 and s4["score"] < 50:
            r["dq"] = True
            r["dq_reason"] = f"S4 {s4['display']} ({s4['score']}%) < 50% H/A hit rate vs opp"
            s4_dq.append(r)
            emit({"type": "log", "msg": f"  ❌ {r['name']}: S4 {s4['display']} ({s4['score']}%) < 50% — DQ"})
            continue
        dn_ba      = (r.get("dn", {}) or {}).get("ba")
        s5_score   = round(dn_ba * 1000) if dn_ba else 0
        r["s5"]    = {"ba": dn_ba, "score": s5_score,
                      "display": f"{dn_ba:.3f}" if dn_ba else "N/A"}
        s4_pts = (s4.get("score", 0) or 0) * 10
        r["total"] = (r.get("total", 0) or 0) + s5_score + s4_pts
        emit({"type": "log",
              "msg": f"  ✅ {r['name']}: S4 {s4['display']} (+{s4_pts}) | "
                     f"S5 {r['s5']['display']} (+{s5_score}) → total {r['total']}"})
        s4_qualified.append(r)

    emit({"type": "log", "msg": f"S4 filter: {len(s4_qualified)} pass, {len(s4_dq)} DQ'd (<50%)"})
    all_ranked = sorted(s4_qualified, key=lambda x: x["total"], reverse=True)
    top9     = all_ranked[:10]
    also_ran = all_ranked[10:]

    # ── Under Picks ───────────────────────────────────────────────────
    try:
        from under_picks import run_under_picks
        under_picks_list = run_under_picks(run_date, team_schedule, emit=emit)
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
    # Inject team into each under pick (reverse-lookup from team_schedule)
    for _up in under_picks_list:
        _side, _opp = _up.get("side", ""), _up.get("opp", "")
        for _t, _sched in team_schedule.items():
            if _sched.get("side") == _side and _sched.get("opponent") == _opp:
                _up["team"] = _t
                break
        _up.setdefault("team", "")

    # ── Pitcher K Picks ───────────────────────────────────────────────
    try:
        from pitcher_k import run_pitcher_k_picks
        pitcher_k_result = run_pitcher_k_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ Pitcher K Picks skipped: {exc}"})
        pitcher_k_result = {"picks": [], "all": []}

    elapsed = round(time.time() - t_start, 1)
    result = {
        "date": run_date, "top9": top9, "also_ran": also_ran,
        "under_picks": under_picks_list, "all_qualified": era_qualified,
        "dq_s1_s3": [x for x in results if x["dq"] and x not in dn_dq and x not in era_dq and x not in dq_lineup and x not in s4_dq],
        "dq_step4": dn_dq, "dq_step5": era_dq, "dq_lineup": dq_lineup, "dq_s4": s4_dq, "pitcher_k": pitcher_k_result,
        "stats": {"step1_count": len(top30), "games": len(team_schedule) // 2,
                  "elapsed": elapsed, "picks": len(top9),
                  "under_count": len(under_picks_list),
                  "pitcher_k_count": len(pitcher_k_result.get("picks", []))},
    }
    emit({"type": "done", "result": result})
    return result
