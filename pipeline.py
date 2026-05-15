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


TOP_N_ERA_PITCHERS = 10    # DQ batters facing the top-N lowest ERA starters
MIN_IP_STARTER     = 20.0  # minimum innings pitched to count as a qualified starter


def _get_top_era_starters(season: str, n: int = TOP_N_ERA_PITCHERS, min_ip: float = MIN_IP_STARTER):
    """
    Fetch the top-N lowest ERA qualified starters from MLB Stats API.
    Returns:
      last_name_set : set of lowercased last names for fast matching against FIC pitcher names
      top_list      : list of {"name", "era", "ip"} for display in logs
    """
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/stats",
            params={
                "stats":    "season",
                "group":    "pitching",
                "gameType": "R",
                "season":   season,
                "sportId":  1,
                "limit":    300,
                "sortStat": "earnedRunAverage",
                "order":    "asc",
            },
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
                full_name = sp.get("player", {}).get("fullName", "")
                qualified.append({"name": full_name, "era": era, "ip": ip})

        top_n = qualified[:n]   # already sorted lowest ERA first

        # Build last-name lookup set (lowercase) for matching "P. Skenes" -> "skenes"
        last_name_set = set()
        for p in top_n:
            parts = p["name"].lower().split()
            if parts:
                last_name_set.add(parts[-1])

        return last_name_set, top_n
    except Exception:
        return set(), []


def _pitcher_last_name(pitcher_raw: str) -> str:
    """Normalize FIC pitcher string to a lowercase last name.
    Examples: 'P. Skenes' -> 'skenes'  |  'Paul Skenes' -> 'skenes'
    """
    name = pitcher_raw.strip()
    if "." in name:
        last = name.split(".")[-1].strip()
    else:
        parts = name.split()
        last  = parts[-1] if parts else name
    return last.lower()


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

        pid = info.get("player_id")
        s2 = fetch_step2_ba(first, last, side, opp_slug, player_id=pid)
        time.sleep(0.25)
        s3 = fetch_step3_ba(first, last, side, player_id=pid)
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
            "pitcher": pitcher_map.get(name, ""),
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

    # ── STEP 5: Individual Pitcher ERA filter ────────────────────────
    # DQ any batter whose starting pitcher is in the top-10 lowest ERA starters
    emit({"type": "section", "msg": f"Step 5 — Pitcher ERA filter (top {TOP_N_ERA_PITCHERS} lowest ERA starters removed)"})
    era_qualified = []
    era_dq        = []
    top_era_lastnames, top_era_list = _get_top_era_starters(run_date[:4])

    if top_era_lastnames:
        # Log the top-10 list so users can see who's being filtered
        pitcher_lines = ", ".join(
            f"{p['name']} ({p['era']:.2f})" for p in top_era_list
        )
        emit({"type": "log", "msg": f"✅ Top {TOP_N_ERA_PITCHERS} lowest ERA starters (DQ zone): {pitcher_lines}"})

        for r in qualified:
            pitcher_raw  = r.get("pitcher", "")
            pitcher_last = _pitcher_last_name(pitcher_raw)
            if pitcher_last and pitcher_last in top_era_lastnames:
                # Find the ERA for display
                matched_era = next(
                    (p["era"] for p in top_era_list
                     if p["name"].lower().endswith(pitcher_last)),
                    None
                )
                era_str = f" ERA {matched_era:.2f}" if matched_era is not None else ""
                r["dq"]        = True
                r["dq_reason"] = f"Facing top-ERA pitcher {pitcher_raw}{era_str}"
                era_dq.append(r)
                emit({"type": "era_dq", "name": r["name"],
                      "pitcher": pitcher_raw, "era": era_str.strip()})
                emit({"type": "log",
                      "msg": f"  ❌ {r['name']} — facing {pitcher_raw}{era_str} (top {TOP_N_ERA_PITCHERS} ERA)"})
            else:
                era_qualified.append(r)
                emit({"type": "era_ok", "name": r["name"], "pitcher": pitcher_raw})
    else:
        # ERA fetch failed — skip this filter so picks still run
        emit({"type": "log", "msg": "⚠️ Could not load pitcher ERA rankings — skipping Step 5"})
        era_qualified = qualified

    # ── FINAL TOP 9 ───────────────────────────────────────────────────
    all_ranked = sorted(era_qualified, key=lambda x: x["total"], reverse=True)
    top9         = all_ranked[:9]
    also_ran     = all_ranked[9:]   # picks #10+ who passed all 5 steps

    # ── UNDER PICKS (runs after main pipeline) ────────────────────────
    try:
        from under_picks import run_under_picks
        under_picks_list = run_under_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ Under Picks skipped: {exc}"})
        under_picks_list = []

    # ── PITCHER K PICKS ─────────────────────────────────────────────
    try:
        from pitcher_k import run_pitcher_k_picks
        pitcher_k_result = run_pitcher_k_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ Pitcher K Picks skipped: {exc}"})
        pitcher_k_result = {"picks": [], "all": []}

    elapsed = round(time.time() - t_start, 1)

    result = {
        "date":          run_date,
        "top9":          top9,
        "also_ran":      also_ran,
        "under_picks":   under_picks_list,
        "all_qualified": era_qualified,
        "dq_s1_s3":      [x for x in results if x["dq"] and x not in dn_dq and x not in era_dq],
        "dq_step4":      dn_dq,
        "dq_step5":      era_dq,
        "pitcher_k":     pitcher_k_result,
        "stats": {
            "step1_count":  len(top30),
            "games":        games,
            "elapsed":      elapsed,
            "picks":        len(top9),
            "under_count":  len(under_picks_list),
            "pitcher_k_count": len(pitcher_k_result.get("picks", [])),
        },
    }

    emit({"type": "done", "result": result})
    return result
