"""
pipeline.py — MLB Daily Picks master pipeline (web-optimized).
Runs all 4 steps with real-time progress via emit callback.
"""
import os, sys, time, json, requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fic_cache      import get_step1_players_or_scrape
from mlb_roster     import build_player_roster
from statmuse_fetch import fetch_step2_ba, fetch_step3_ba
from day_night_check import get_game_time_type, find_espn_player_id, fetch_day_night_ba


def _get_top5_era_teams(season: str) -> set:
    """
    Fetch current team ERA rankings from MLB Stats API.
    Returns the TOP 5 ERA teams (best pitching = lowest ERA = ranks 1-5).
    Players facing these teams are CUT in Step 5.
    Returns empty set on failure so the filter is skipped gracefully.
    """
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/teams/stats",
            params={"season": season, "sportId": 1, "group": "pitching",
                    "stats": "season", "sortStat": "earnedRunAverage", "order": "asc"},
            timeout=12,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if len(splits) < 5:
            return set()
        # Top 5 = indices 0-4 (0-based) = BEST ERA teams (lowest ERA)
        return {sp["team"]["name"] for sp in splits[:5]}
    except Exception:
        return set()


def run_pipeline(run_date: str, emit=None) -> dict:
    """
    Run the full 4-step MLB Daily Picks pipeline.

    Parameters
    ----------
    run_date : str  "YYYY-MM-DD"
    emit     : callable(dict)  — progress callback for SSE streaming

    Returns
    -------
    dict  {date, top9, all_qualified, dq_s1_s3, dq_step4, stats}
    """
    if emit is None:
        emit = lambda _: None

    t_start    = time.time()
    date_espn  = run_date.replace("-", "")

    def log(msg, type_="log"):
        emit({"type": type_, "msg": msg})

    # ── STEP 1: Fantasy Info Central ──────────────────────────────────
    emit({"type": "section", "msg": "Step 1 — Loading player list from Fantasy Info Central"})
    step1 = get_step1_players_or_scrape(run_date, emit=emit)
    top30 = step1  # no limit — use all qualifying players
    pitcher_map = {p["batter"]: p["pitcher"] for p in top30}
    emit({"type": "step1_done", "msg": f"✅ {len(top30)} players loaded", "count": len(top30)})

    # ── ESPN Schedule ─────────────────────────────────────────────────
    emit({"type": "section", "msg": "ESPN — Fetching today's schedule"})
    espn_r = requests.get(
        f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_espn}",
        timeout=15
    ).json()
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
    games = len(team_schedule) // 2
    log(f"✅ {games} games found today")

    # ── MLB Roster Lookup ─────────────────────────────────────────────
    emit({"type": "section", "msg": "Roster — Resolving player teams via MLB Stats API"})
    log(f"Looking up {len(top30)} players (this takes ~30 seconds)…")
    roster = build_player_roster([p["batter"] for p in top30], date_espn, pitcher_map)
    found = len([v for v in roster.values() if v.get("player_id")])
    log(f"✅ Resolved {found}/{len(top30)} players")

    # ── STEPS 2 & 3: StatMuse splits ─────────────────────────────────
    emit({"type": "section", "msg": "Steps 2 & 3 — Fetching StatMuse batting splits"})
    results = []

    for i, p in enumerate(top30):
        name     = p["batter"]
        info     = roster.get(name, {})
        slug     = info.get("slug", "")
        team     = info.get("team_name", "")
        sched    = team_schedule.get(team, {})
        side     = sched.get("side", "")
        opp_slug = sched.get("opp_slug", "")
        opp_name = sched.get("opponent", "")

        emit({"type": "progress", "current": i + 1, "total": len(top30), "name": name})

        if not side or not slug:
            emit({"type": "player_skip", "name": name, "reason": "no game today"})
            continue

        parts = slug.split("-")
        first = parts[0]
        last  = "-".join(parts[1:])

        s2 = fetch_step2_ba(first, last, side, opp_slug)
        time.sleep(0.25)
        s3 = fetch_step3_ba(first, last, side)
        time.sleep(0.25)

        dq = []
        # DQ if N/A (no data = no pick)
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
            "opp": opp_name, "side": side, "slug": slug,
            "full_name": info.get("full_name", name),
            "s2": s2, "s3": s3, "total": total,
            "dq": bool(dq), "dq_reason": " & ".join(dq)
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

    # ── STEP 4: Day / Night filter ────────────────────────────────────
    emit({"type": "section", "msg": "Step 4 — ESPN Day/Night BA filter"})
    qualified = []
    dn_dq     = []

    for r in [x for x in results if not x["dq"]]:
        team      = roster.get(r["name"], {}).get("team_name", "")
        full_name = r.get("full_name", r["name"])
        gtype     = get_game_time_type(team, date_espn)
        eid       = find_espn_player_id(full_name)
        dn = (
            fetch_day_night_ba(eid, gtype)
            if eid and gtype != "unknown"
            else {"display": "N/A", "flag": "❌ skip", "dq": False, "ba": None, "ab": None}
        )
        label = "DAY" if gtype == "day" else "NIGHT"
        r["dn"]       = dn
        r["dn_label"] = label

        if dn["dq"]:
            r["dq"]        = True
            r["dq_reason"] = f"Step 4 {label} {dn['display']} < .200"
            dn_dq.append(r)
            emit({"type": "dn_dq",  "name": r["name"], "label": label, "display": dn["display"]})
        else:
            qualified.append(r)
            emit({"type": "dn_ok",  "name": r["name"], "label": label, "display": dn["display"]})

    # ── STEP 5: Team ERA filter ──────────────────────────────────────
    # Only keep players facing bottom 15 ERA teams (weakest pitching)
    emit({"type": "section", "msg": "Step 5 — Team ERA filter (bottom 15 ERA opponents only)"})
    era_qualified = []
    era_dq        = []
    top5_era  = _get_top5_era_teams(run_date[:4])

    if top5_era:
        emit({"type": "log", "msg": f"✅ ERA rankings loaded — top 5 teams to avoid: {', '.join(sorted(top5_era))}"})
        for r in qualified:
            opp = r.get("opp", "")
            if opp in top5_era:
                r["dq"]        = True
                r["dq_reason"] = f"Opp {opp} is a top-5 ERA team"
                era_dq.append(r)
                emit({"type": "era_dq", "name": r["name"], "opp": opp})
            else:
                era_qualified.append(r)
                emit({"type": "era_ok", "name": r["name"], "opp": opp})
    else:
        # ERA fetch failed — skip this filter so picks still run
        emit({"type": "log", "msg": "⚠️ Could not load ERA rankings — skipping Step 5"})
        era_qualified = qualified

    # ── LINEUP CHECK (Step 6) ─────────────────────────────────────────
    emit({"type": "section", "msg": "Step 6 — Lineup Check (confirmed starters only)"})
    from lineup_check import build_lineup_map, get_lineup_status
    id_map, name_map, teams_with_lineups = build_lineup_map(run_date)
    n_posted = len(teams_with_lineups)
    emit({"type": "log", "msg": f"✅ Lineups posted for {n_posted} teams"})

    lineup_qualified = []
    lineup_dq        = []
    for r in era_qualified:
        name      = r["name"]
        info      = roster.get(name, {})
        player_id = info.get("player_id")
        full_name = r.get("full_name", name)
        team_name = info.get("team_name", "")
        status    = get_lineup_status(player_id, full_name, team_name,
                                      id_map, name_map, teams_with_lineups)
        r["lineup_status"] = status
        if status == "NOT_IN_LINEUP":
            r["dq"]        = True
            r["dq_reason"] = "Not in today's lineup"
            lineup_dq.append(r)
            emit({"type": "lineup_dq", "name": name, "team": team_name})
        else:
            lineup_qualified.append(r)
            emit({"type": "lineup_ok", "name": name, "status": status})
    emit({"type": "log", "msg": f"✅ {len(lineup_qualified)} players IN or TBD lineup, {len(lineup_dq)} scratched"})

    # ── FINAL TOP 9 ───────────────────────────────────────────────────
    all_ranked = sorted(lineup_qualified, key=lambda x: x["total"], reverse=True)
    top9         = all_ranked[:9]
    also_ran     = all_ranked[9:]   # picks #10+ who passed all 6 steps

    # ── UNDER PICKS (runs after main pipeline) ────────────────────────
    try:
        from under_picks import run_under_picks
        lineup_data      = (id_map, name_map, teams_with_lineups)
        under_picks_list = run_under_picks(run_date, team_schedule,
                                           lineup_data=lineup_data, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ Under Picks skipped: {exc}"})
        under_picks_list = []

    elapsed = round(time.time() - t_start, 1)

    result = {
        "date":          run_date,
        "top9":          top9,
        "also_ran":      also_ran,
        "under_picks":   under_picks_list,
        "all_qualified": lineup_qualified,
        "dq_s1_s3":      [x for x in results if x["dq"] and x not in dn_dq and x not in era_dq and x not in lineup_dq],
        "dq_step4":      dn_dq,
        "dq_step5":      era_dq,
        "dq_lineup":     lineup_dq,
        "stats": {
            "step1_count":    len(top30),
            "games":          games,
            "elapsed":        elapsed,
            "picks":          len(top9),
            "under_count":    len(under_picks_list),
            "lineups_posted": n_posted,
        },
    }

    emit({"type": "done", "result": result})
    return result
