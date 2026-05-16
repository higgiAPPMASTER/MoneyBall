"""
pipeline.py — MoneyBall MLB Daily Picks Pipeline
=================================================
Step 1  Career BA vs today's starter       MIN_AB=4, ≥ .250  (+Baseball Musings hot streaks)
Step 2  H/A game logs vs today's opponent  min 3 games, ≥ .225
Step 3  2026 H/A season logs               min 3 games, ≥ .225
Step 4  Day/Night split — MLB Stats API    ≥ .200   ← replaces ESPN, NEVER crashes
Step 5  Avoid facing top-5 ERA teams
Score = (S1 + S2 + S3) × 1000  ──→  Top 9 picks + Also Ran + Under Picks + Pitcher K
"""
import re, time, requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Constants ─────────────────────────────────────────────────────────────────
MLB      = "https://statsapi.mlb.com/api/v1"
MIN_AB   = 4        # Step 1 minimum AB vs pitcher
S1_MIN   = 0.250    # Step 1 minimum career BA vs pitcher
S2_MIN   = 0.225    # Step 2 minimum H/A BA vs today's opponent
S3_MIN   = 0.225    # Step 3 minimum 2026 H/A BA
DN_MIN   = 0.200    # Step 4 minimum Day/Night BA
ERA_CUT  = 5        # Step 5 — avoid top-N ERA pitching staffs

_dn_cache: dict = {}   # (player_id, label) → dn dict — cleared each run


# ─────────────────────────────  UTILITIES  ───────────────────────────────────

def _cur_year() -> int:
    return datetime.now().year


def _req(url: str, **kw) -> dict:
    """Safe GET — never raises, always returns a dict."""
    try:
        return requests.get(url, timeout=13, **kw).json()
    except Exception:
        return {}


def _ba(h: int, ab: int):
    return round(h / ab, 3) if ab > 0 else None


# ─────────────────────────  DATA HELPERS  ───────────────────────────────────

def _schedule(date_str: str) -> list:
    """Return today's games with probable pitchers and lineups."""
    d = _req(f"{MLB}/schedule", params={
        "date": date_str, "sportId": 1,
        "hydrate": "probablePitcher,team,lineups",
    })
    games = []
    for dd in d.get("dates", []):
        for g in dd.get("games", []):
            ht = g["teams"]["home"]
            at = g["teams"]["away"]
            games.append({
                "gamePk":       g.get("gamePk"),
                "game_time":    g.get("gameDate", ""),
                "home_id":      ht.get("team", {}).get("id"),
                "home_name":    ht.get("team", {}).get("name", ""),
                "away_id":      at.get("team", {}).get("id"),
                "away_name":    at.get("team", {}).get("name", ""),
                "home_pitcher": ht.get("probablePitcher"),
                "away_pitcher": at.get("probablePitcher"),
                "home_lineup":  [p.get("id") for p in ht.get("lineup", [])],
                "away_lineup":  [p.get("id") for p in at.get("lineup", [])],
            })
    return games


def _roster(team_id: int) -> list:
    d = _req(f"{MLB}/teams/{team_id}/roster", params={
        "rosterType": "active", "season": _cur_year(),
    })
    return [
        {
            "id":   p["person"]["id"],
            "name": p["person"].get("fullName", ""),
            "pos":  p.get("position", {}).get("abbreviation", ""),
        }
        for p in d.get("roster", [])
    ]


def _vs_pitcher(batter_id: int, pitcher_id: int):
    """Career regular-season BA vs a specific pitcher. Returns (ba, ab)."""
    d = _req(f"{MLB}/people/{batter_id}/stats", params={
        "stats": "vsPlayer", "opposingPlayerId": pitcher_id,
        "group": "hitting", "gameType": "R",
    })
    splits = d.get("stats", [{}])[0].get("splits", [])
    if not splits:
        return None, 0
    stat = splits[0].get("stat", {})
    ab   = int(stat.get("atBats", 0))
    h    = int(stat.get("hits", 0))
    return _ba(h, ab), ab


def _game_logs(player_id: int, season: int) -> list:
    d = _req(f"{MLB}/people/{player_id}/stats", params={
        "stats": "gameLog", "group": "hitting",
        "season": season, "hydrate": "opponent",
    })
    return d.get("stats", [{}])[0].get("splits", [])


def _fetch_all_logs(player_id: int) -> tuple:
    """Fetch 3 seasons of game logs for one player (used in parallel pool)."""
    yr = _cur_year()
    return player_id, {s: _game_logs(player_id, s) for s in [yr, yr - 1, yr - 2]}


def _calc_ba(logs: list, side: str, opp_id=None, n: int = 10):
    """
    Filter logs by side (HOME/AWAY) and optionally opponent,
    take last n. Returns {"ba", "games", "display"} or None.
    """
    want_home = (side == "HOME")
    filtered  = [
        g for g in logs
        if g.get("isHome", False) == want_home
        and (opp_id is None or g.get("opponent", {}).get("id") == opp_id)
    ]
    if not filtered:
        return None
    recent = filtered[-n:]
    h  = sum(int(g.get("stat", {}).get("hits",   0)) for g in recent)
    ab = sum(int(g.get("stat", {}).get("atBats", 0)) for g in recent)
    if ab == 0:
        return None
    ba = _ba(h, ab)
    return {"ba": ba, "games": len(recent), "display": f"{ba:.3f}" if ba else "N/A"}


def _dn_split(player_id: int, game_time_utc: str) -> dict:
    """
    Day/Night BA split via MLB Stats API sitCodes (d / n).
    ✅ ALWAYS returns a safe dict — NEVER returns None — no more crashes.
    No ESPN dependency.
    """
    label = "NIGHT"
    try:
        if game_time_utc:
            hr    = datetime.strptime(game_time_utc, "%Y-%m-%dT%H:%M:%SZ").hour
            label = "DAY" if 5 < hr < 21 else "NIGHT"
    except Exception:
        pass

    key = (player_id, label)
    if key in _dn_cache:
        return _dn_cache[key]

    sit  = "d" if label == "DAY" else "n"
    data = _req(f"{MLB}/people/{player_id}/stats", params={
        "stats":    "statSplits",
        "group":    "hitting",
        "season":   _cur_year(),
        "sitCodes": sit,
    })
    splits   = data.get("stats", [{}])[0].get("splits", [])
    fallback = {"ba": None, "display": "N/A", "label": label, "dq": False}

    if not splits:
        _dn_cache[key] = fallback
        return fallback

    stat = splits[0].get("stat", {})
    ab   = int(stat.get("atBats", 0))
    h    = int(stat.get("hits",   0))

    if ab == 0:
        _dn_cache[key] = fallback
        return fallback

    ba     = _ba(h, ab)
    result = {
        "ba":      ba,
        "display": f"{ba:.3f}" if ba else "N/A",
        "label":   label,
        "dq":      ba is not None and ba < DN_MIN,
    }
    _dn_cache[key] = result
    return result


def _top_era_team_ids(n: int = ERA_CUT) -> set:
    """Return team IDs with the n lowest (best) ERAs — Step 5 filter."""
    d = _req(f"{MLB}/teams/stats", params={
        "stats": "season", "group": "pitching",
        "season": _cur_year(), "sportId": 1,
    })
    rows = []
    for split in d.get("stats", [{}])[0].get("splits", []):
        try:
            era = float(split.get("stat", {}).get("era", "99.99"))
        except Exception:
            era = 99.99
        tid = split.get("team", {}).get("id")
        if tid:
            rows.append((era, tid))
    rows.sort()
    return {tid for _, tid in rows[:n]}


# ── FantasyPros last-7-day hot hitters ───────────────────────────────────────
FP_HOT_BA = 0.300   # minimum 7-day BA to be considered hot
FP_HOT_AB = 5       # minimum 7-day AB

_FP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.google.com/",
}


def _fp_hot_hitters(min_ba: float = FP_HOT_BA, min_ab: int = FP_HOT_AB) -> dict:
    """
    Scrape FantasyPros /mlb/stats/hitters.php?range=7 for last-7-day hot hitters.
    Returns {player_name_lower: {"ba": float, "ab": int}}
    for every player hitting >= min_ba BA with >= min_ab AB in last 7 days.
    Falls back to empty dict on any failure — pipeline continues normally.
    """
    url = "https://www.fantasypros.com/mlb/stats/hitters.php?range=7"
    try:
        r    = requests.get(url, headers=_FP_HEADERS, timeout=15)
        html = r.text

        # Find the stats table (id="data" is FP's standard)
        tbl = re.search(r'<table[^>]+id=["\']data["\'][^>]*>(.*?)</table>', html, re.DOTALL)
        if not tbl:
            # fallback: any table with class containing "sortable" or "stats"
            tbl = re.search(
                r'<table[^>]+class=["\'][^"\']*(sortable|stats)[^"\']["\'][^>]*>(.*?)</table>',
                html, re.DOTALL,
            )
        if not tbl:
            return {}
        table_html = tbl.group(0)

        # Identify AB and AVG column positions from the header row
        ab_idx, ba_idx = 3, 15   # FP hitters default column positions
        thead = re.search(r'<thead>(.*?)</thead>', table_html, re.DOTALL)
        if thead:
            ths  = re.findall(r'<th[^>]*>(.*?)</th>', thead.group(1), re.DOTALL)
            keys = [re.sub(r'<[^>]+>', '', h).strip().upper() for h in ths]
            if "AB"  in keys: ab_idx = keys.index("AB")
            if "AVG" in keys: ba_idx = keys.index("AVG")
            elif "BA" in keys: ba_idx = keys.index("BA")

        # Parse each player row
        tbody = re.search(r'<tbody>(.*?)</tbody>', table_html, re.DOTALL)
        body  = tbody.group(1) if tbody else table_html
        hot: dict = {}

        for row in re.findall(r'<tr[^>]*>(.*?)</tr>', body, re.DOTALL):
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if len(cells) <= max(ab_idx, ba_idx):
                continue

            # Player name lives inside an <a> tag in the first cell
            nm = re.search(r'<a[^>]*>([^<]+)</a>', cells[0])
            if not nm:
                continue
            name = nm.group(1).strip()

            try:
                ab = int(re.sub(r'<[^>]+>', '', cells[ab_idx]).strip())
            except (ValueError, IndexError):
                continue
            try:
                ba = float(re.sub(r'<[^>]+>', '', cells[ba_idx]).strip())
            except (ValueError, IndexError):
                continue

            if ab >= min_ab and ba >= min_ba:
                hot[name.lower()] = {"ba": ba, "ab": ab}

        return hot

    except Exception as exc:
        print(f"[pipeline] FantasyPros scrape error: {exc}")
        return {}


def _baseball_musings_fallback(date_str: str) -> dict:
    """Baseball Musings hit-streak fallback — used only when FantasyPros is unavailable."""
    try:
        d = date_str.replace("-", "%2F")
        r = requests.get(
            f"http://baseballmusings.com/cgi-bin/DayStreak.py?DateToCheck={d}",
            timeout=10,
        )
        names: dict = {}
        for m in re.finditer(
            r"<td[^>]*>([A-Z][a-zA-Zé'àáíóúñü\-]+(?: [A-Z][a-zA-Zé'àáíóúñü\-\.]+)+)</td>",
            r.text,
        ):
            n = m.group(1).strip().lower()
            if len(n) > 3:
                names[n] = {"ba": None, "ab": 0}
        return names
    except Exception:
        return {}


# ─────────────────────────  UNDER PICKS  ─────────────────────────────────────

def _build_under_picks(games: list, log_cache: dict, emit=None) -> list:
    """
    Cold bats against today's pitchers — players on DK 1.5 hits O/U line
    who fail S1 / S2 / S3 thresholds (i.e. expected to go UNDER 1.5 hits).
    """
    def log(msg):
        if emit:
            emit({"type": "log", "msg": msg})

    # Build name → game-context map from all rosters
    player_map: dict = {}
    for g in games:
        for side in ("HOME", "AWAY"):
            if side == "HOME":
                team_id  = g["home_id"];  opp = g["away_name"]; opp_id = g["away_id"]
                pitcher  = g["away_pitcher"]; lu_ids = g["home_lineup"]
            else:
                team_id  = g["away_id"];   opp = g["home_name"]; opp_id = g["home_id"]
                pitcher  = g["home_pitcher"]; lu_ids = g["away_lineup"]

            pitcher_name = pitcher.get("fullName", "TBD") if pitcher else "TBD"
            pitcher_id   = pitcher.get("id")              if pitcher else None

            for pl in _roster(team_id):
                if pl["pos"] in ("P", "TWP"):
                    continue
                player_map[pl["name"].lower()] = {
                    "player_id":  pl["id"],  "name":      pl["name"],
                    "pos":        pl["pos"],  "side":      side,
                    "opp":        opp,        "opp_id":    opp_id,
                    "pitcher":    pitcher_name, "pitcher_id": pitcher_id,
                    "lineup_ids": lu_ids,
                }

    # Fetch DraftKings 1.5 hit lines
    log("⬇️ Fetching DraftKings 1.5 hit lines…")
    dk_names: list = []
    try:
        r = requests.get(
            "https://sportsbook.draftkings.com//sites/US-SB/api/v5/eventgroups/"
            "84240/categories/743/subcategories/4519",
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        for cat in r.json().get("eventGroup", {}).get("offerCategories", []):
            for sub in cat.get("offerSubcategoryDescriptors", []):
                for row in sub.get("offerSubcategory", {}).get("offers", []):
                    for offer in row:
                        label = offer.get("label", "")
                        for oc in offer.get("outcomes", []):
                            if oc.get("line") == 1.5 and str(oc.get("label", "")).lower() == "over":
                                dk_names.append(label.lower())
    except Exception as exc:
        log(f"⚠️ DK fetch skipped ({exc}) — no under picks this run")
        return []

    if not dk_names:
        log("ℹ️ No DK 1.5 hit lines found today")
        return []

    log(f"  {len(dk_names)} players on DK 1.5 hit line — screening for cold bats…")
    results: list = []

    for dk in dk_names:
        # Fuzzy match to our roster map
        info = None
        for key, val in player_map.items():
            if dk in key or key in dk:
                info = val
                break
        if not info:
            continue

        pid    = info["player_id"]
        side   = info["side"]
        opp_id = info["opp_id"]

        # Fetch logs if not cached (player wasn't in main pool)
        if pid not in log_cache:
            yr = _cur_year()
            log_cache[pid] = {s: _game_logs(pid, s) for s in [yr, yr - 1, yr - 2]}

        logs   = log_cache[pid]
        all_lg = [g for sl in logs.values() for g in sl]

        # S1: career BA vs today's pitcher (cold = <.250 with ≥4 AB, or N/A passes)
        s1_ba, s1_ab = None, 0
        if info["pitcher_id"]:
            s1_ba, s1_ab = _vs_pitcher(pid, info["pitcher_id"])
        s1_disp = f"{s1_ba:.3f}" if s1_ba is not None else "N/A"
        if s1_ba is not None and s1_ab >= MIN_AB and s1_ba >= 0.250:
            continue  # too warm

        # S2: H/A vs opponent (cold = <.225)
        s2 = _calc_ba(all_lg, side, opp_id=opp_id) or {"ba": None, "games": 0, "display": "N/A"}
        if s2["ba"] is not None and s2["games"] >= 3 and s2["ba"] >= 0.225:
            continue

        # S3: 2026 H/A (cold = <.250)
        cur = logs.get(_cur_year(), [])
        s3  = _calc_ba(cur, side) or {"ba": None, "games": 0, "display": "N/A"}
        if s3["ba"] is not None and s3["games"] >= 3 and s3["ba"] >= 0.250:
            continue

        # Lineup status
        lu_ids = info.get("lineup_ids", [])
        lu_st  = ("IN_LINEUP" if pid in lu_ids else "NOT_IN_LINEUP") if lu_ids else "TBD"

        score  = round(((s1_ba or 0) + (s2.get("ba") or 0) + (s3.get("ba") or 0)) * 1000)
        results.append({
            "name": info["name"], "pos": info["pos"],
            "side": side,         "opp": info["opp"],
            "pitcher":  info["pitcher"],
            "s1_disp":  s1_disp,  "s1_ab": s1_ab,
            "s2": s2,             "s3": s3,
            "lineup_status": lu_st, "under_score": score,
        })
        if emit:
            emit({"type": "under_pick_found", "name": info["name"],
                  "s1": s1_disp, "s2": s2["display"], "s3": s3["display"],
                  "side": side, "opp": info["opp"]})

    results.sort(key=lambda x: x["under_score"])   # coldest bat first
    return results


# ─────────────────────────  MAIN PIPELINE  ───────────────────────────────────

def run_pipeline(date_str: str, emit=None) -> dict:
    t0 = time.time()
    _dn_cache.clear()

    def log(msg: str):
        if emit:
            emit({"type": "log", "msg": msg})

    def section(msg: str):
        if emit:
            emit({"type": "section", "msg": msg})

    # ── Schedule ──────────────────────────────────────────────────────────────
    section("Fetching today's schedule")
    games = _schedule(date_str)
    if not games:
        log("❌ No games found for this date")
        return _empty_result()
    log(f"✅ {len(games)} games today")

    # ── Hot hitters (FantasyPros) ─────────────────────────────────────────────
    log(f"🔥 FantasyPros last-7-day hot hitters (.{int(FP_HOT_BA*1000)}+ BA, {FP_HOT_AB}+ AB)…")
    hot = _fp_hot_hitters()
    if hot:
        log(f"  ✅ {len(hot)} hot hitters found on FantasyPros")
    else:
        log("  ⚠️  FantasyPros unavailable — falling back to Baseball Musings streaks")
        hot = _baseball_musings_fallback(date_str)
        log(f"  {len(hot)} streak players from Baseball Musings")

    log("📊 Fetching team ERA rankings…")
    bad_era = _top_era_team_ids()
    log(f"  Top-{ERA_CUT} ERA pitching staffs identified — batters vs these teams excluded")

    # ── Step 1: Build player pool ─────────────────────────────────────────────
    section(f"Step 1 — Career BA vs today's pitcher (min {MIN_AB} AB, min {S1_MIN:.3f})")
    pool: list = []

    for g in games:
        for side in ("HOME", "AWAY"):
            if side == "HOME":
                bat_id   = g["home_id"];   opp_name = g["away_name"]
                opp_id   = g["away_id"];   pitcher  = g["away_pitcher"]
                lu_ids   = g["home_lineup"]
            else:
                bat_id   = g["away_id"];   opp_name = g["home_name"]
                opp_id   = g["home_id"];   pitcher  = g["home_pitcher"]
                lu_ids   = g["away_lineup"]

            if not pitcher:
                log(f"  — No probable pitcher for {opp_name} — skipping side")
                continue
            if opp_id in bad_era:
                log(f"  ⚔️  {opp_name} — top-{ERA_CUT} ERA team, skipping")
                continue

            pid_p  = pitcher["id"]
            name_p = pitcher.get("fullName", "TBD")
            roster = _roster(bat_id)

            for pl in roster:
                if pl["pos"] in ("P", "TWP"):
                    continue

                pid  = pl["id"]
                name = pl["name"]

                ba, ab   = _vs_pitcher(pid, pid_p)
                name_key = name.lower()
                in_hot   = name_key in hot
                hot_data = hot.get(name_key, {})

                # Qualify: meets S1 career threshold  OR  is a FantasyPros hot hitter
                if not ((ab >= MIN_AB and ba is not None and ba >= S1_MIN) or in_hot):
                    continue

                if in_hot and hot_data.get("ba"):
                    log(f"  🔥 HOT ADD: {name} — {hot_data['ba']:.3f} BA last 7d ({hot_data['ab']} AB)")

                pool.append({
                    "player_id":    pid,
                    "name":         name,
                    "pos":          pl["pos"],
                    "side":         side,
                    "opp":          opp_name,
                    "opp_id":       opp_id,
                    "pitcher_id":   pid_p,
                    "pitcher_name": name_p,
                    "s1":           ba,
                    "s1_ab":        ab,
                    "game_time":    g["game_time"],
                    "lineup_ids":   lu_ids,
                    "hot_streak":   in_hot,
                    "fp7d_ba":      hot_data.get("ba"),
                    "fp7d_ab":      hot_data.get("ab", 0),
                })

    log(f"✅ Step 1: {len(pool)} candidates in pool")
    if emit:
        emit({"type": "step1_done", "msg": f"✅ {len(pool)} players in pool after Step 1"})

    if not pool:
        log("❌ No candidates found — probable pitchers may not be posted yet")
        return _empty_result()

    # ── Prefetch game logs in parallel ────────────────────────────────────────
    section("Steps 2 & 3 — H/A game log splits")
    log("📂 Fetching game logs in parallel…")
    log_cache: dict = {}
    unique_ids = list({c["player_id"] for c in pool})

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_all_logs, pid): pid for pid in unique_ids}
        for fut in as_completed(futs):
            try:
                pid, logs = fut.result()
                log_cache[pid] = logs
            except Exception:
                pass

    # ── Steps 2 & 3 ───────────────────────────────────────────────────────────
    passed:   list = []
    dq_s1_s3: list = []
    total          = len(pool)

    for i, c in enumerate(pool):
        if emit:
            emit({"type": "progress", "current": i + 1, "total": total, "name": c["name"]})

        pid    = c["player_id"]
        side   = c["side"]
        opp_id = c["opp_id"]
        yr     = _cur_year()
        logs   = log_cache.get(pid, {})
        all_lg = [g for sl in logs.values() for g in sl]

        # Step 2: H/A vs today's opponent
        s2 = _calc_ba(all_lg, side, opp_id=opp_id) or {"ba": None, "games": 0, "display": "N/A"}
        if s2["ba"] is not None and s2["games"] >= 3 and s2["ba"] < S2_MIN:
            dq_s1_s3.append({**c, "s2": s2, "s3": {"display": "—"},
                             "dq_reason": f"S2 {s2['display']} < .225"})
            if emit:
                emit({"type": "player_dq", "name": c["name"],
                      "s1": f"{c['s1']:.3f}" if c["s1"] else "—",
                      "s2": s2["display"], "s3": "—",
                      "reason": f"S2 {s2['display']} < .225"})
            continue

        # Step 3: current-season H/A
        cur = logs.get(yr, [])
        s3  = _calc_ba(cur, side) or {"ba": None, "games": 0, "display": "N/A"}
        if s3["ba"] is not None and s3["games"] >= 3 and s3["ba"] < S3_MIN:
            dq_s1_s3.append({**c, "s2": s2, "s3": s3,
                             "dq_reason": f"S3 {s3['display']} < .225"})
            if emit:
                emit({"type": "player_dq", "name": c["name"],
                      "s1": f"{c['s1']:.3f}" if c["s1"] else "—",
                      "s2": s2["display"], "s3": s3["display"],
                      "reason": f"S3 {s3['display']} < .225"})
            continue

        score = round(((c["s1"] or 0) + (s2.get("ba") or 0) + (s3.get("ba") or 0)) * 1000)
        if emit:
            emit({"type": "player_ok", "name": c["name"],
                  "s1": f"{c['s1']:.3f}" if c["s1"] else "—",
                  "s2": s2["display"], "s3": s3["display"],
                  "side": side, "opp": c["opp"], "total": score})
        passed.append({**c, "s2": s2, "s3": s3, "total": score})

    passed.sort(key=lambda x: x["total"], reverse=True)
    log(f"✅ Steps 2 & 3: {len(passed)} passed, {len(dq_s1_s3)} DQ'd")

    # ── Step 4: Day/Night — MLB Stats API ─────────────────────────────────────
    section("Step 4 — Day/Night split (MLB Stats API, DQ if < .200)")
    after_dn: list = []
    dq_step4: list = []

    for p in passed:
        dn = _dn_split(p["player_id"], p["game_time"])   # ← always a safe dict, never None
        if dn["dq"]:
            dq_step4.append({**p, "dn": dn, "dn_label": dn["label"],
                             "dq_reason": f"D/N {dn['display']} < .200"})
            if emit:
                emit({"type": "dn_dq", "name": p["name"],
                      "label": dn["label"], "display": dn["display"]})
        else:
            after_dn.append({**p, "dn": dn, "dn_label": dn["label"]})
            if emit:
                emit({"type": "dn_ok", "name": p["name"],
                      "label": dn["label"], "display": dn["display"]})

    log(f"✅ Step 4: {len(after_dn)} passed, {len(dq_step4)} DQ'd")

    # ── Lineup check ──────────────────────────────────────────────────────────
    section("Lineup Check — Confirming today's starting lineups")
    after_lu:  list = []
    dq_lineup: list = []
    ln_posted       = 0

    for p in after_dn:
        lu = p.get("lineup_ids", [])
        if lu:
            ln_posted += 1
        if lu and p["player_id"] not in lu:
            dq_lineup.append({**p, "lineup_status": "NOT_IN_LINEUP",
                              "dq_reason": "Not in lineup"})
            if emit:
                emit({"type": "lineup_ok", "name": p["name"], "status": "NOT_IN_LINEUP"})
        else:
            st = "IN_LINEUP" if (lu and p["player_id"] in lu) else "TBD"
            after_lu.append({**p, "lineup_status": st})
            if emit:
                emit({"type": "lineup_ok", "name": p["name"], "status": st})

    log(f"✅ Lineup: {len(after_lu)} confirmed/TBD, {len(dq_lineup)} not in lineup")

    top9     = after_lu[:9]
    also_ran = after_lu[9:]

    # ── Under picks ───────────────────────────────────────────────────────────
    section("Under Picks — Cold bats on DraftKings 1.5 hit line")
    under = _build_under_picks(games, log_cache, emit=emit)
    log(f"✅ Under picks: {len(under)} found")

    # ── Pitcher K picks ───────────────────────────────────────────────────────
    section("Pitcher K Picks — Strikeout Over/Under")
    try:
        from pitcher_k import get_pitcher_k_picks
        pk = get_pitcher_k_picks(date_str, emit=emit)
    except Exception as exc:
        log(f"⚠️ Pitcher K error: {exc}")
        pk = {"picks": [], "all": []}

    elapsed = round(time.time() - t0, 1)
    log(f"✅ Pipeline complete in {elapsed}s")

    return {
        "top9":        top9,
        "also_ran":    also_ran,
        "dq_s1_s3":   dq_s1_s3,
        "dq_step4":    dq_step4,
        "dq_lineup":   dq_lineup,
        "under_picks": under,
        "pitcher_k":   pk,
        "stats": {
            "picks":           len(top9),
            "games":           len(games),
            "elapsed":         elapsed,
            "step1_count":     len(pool),
            "under_count":     len(under),
            "pitcher_k_count": len(pk.get("picks", [])),
            "lineups_posted":  ln_posted,
        },
    }


def _empty_result() -> dict:
    return {
        "top9": [], "also_ran": [], "dq_s1_s3": [], "dq_step4": [],
        "dq_lineup": [], "under_picks": [], "pitcher_k": {"picks": [], "all": []},
        "stats": {
            "picks": 0, "games": 0, "elapsed": 0, "step1_count": 0,
            "under_count": 0, "pitcher_k_count": 0, "lineups_posted": 0,
        },
    }
