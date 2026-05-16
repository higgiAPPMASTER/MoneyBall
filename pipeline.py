"""
pipeline.py — MoneyBall MLB Daily Picks Pipeline
=================================================
Step 1  Career BA vs today's starter (MLB Stats API)   MIN_AB=4, >= .250
        + FantasyPros last-7-day hot hitters (>= .300 BA, >= 5 AB) auto-seeded
        + Baseball Musings streak fallback if FantasyPros unavailable
Step 2  H/A game logs vs today's opponent              min 3 games, >= .225
Step 3  Current-season H/A game logs                   min 3 games, >= .225
Step 4  Day/Night split via MLB Stats API              >= .200  (replaces ESPN — never crashes)
Step 5  Avoid facing top-5 ERA pitching staffs
Score   = (S1 + S2 + S3) x 1000  ->  Top 9 picks + Also Ran
Extras: Under Picks (DK 1.5 hit line), Pitcher K picks
"""
import re, time, requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Constants ─────────────────────────────────────────────────────────────────
MLB      = "https://statsapi.mlb.com/api/v1"
MIN_AB   = 4        # Step 1 min AB vs pitcher
S1_MIN   = 0.250    # Step 1 min career BA vs pitcher
S2_MIN   = 0.225    # Step 2 min H/A BA vs opponent
S3_MIN   = 0.225    # Step 3 min season H/A BA
DN_MIN   = 0.200    # Step 4 min Day/Night BA
ERA_CUT  = 5        # Step 5 avoid top-N ERA teams
FP_BA    = 0.300    # FantasyPros 7-day min BA
FP_AB    = 5        # FantasyPros 7-day min AB

_dn_cache: dict = {}   # cleared each run

_FP_HDRS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.google.com/",
}

_DK_HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://sportsbook.draftkings.com/",
}

_DK_URLS = [
    "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/84240/categories/743/subcategories/4519",
    "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/84240/categories/1000/subcategories/4519",
    "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/84240",
]


# ─────────────────────────────  UTILS  ──────────────────────────────────────

def _yr() -> int:
    return datetime.now().year


def _req(url: str, **kw) -> dict:
    """Safe GET — never raises."""
    try:
        return requests.get(url, timeout=13, **kw).json()
    except Exception:
        return {}


def _ba(h: int, ab: int):
    return round(h / ab, 3) if ab > 0 else None


# ─────────────────────────  HOT HITTERS  ────────────────────────────────────

def _fp_hot_hitters() -> dict:
    """
    FantasyPros /mlb/stats/hitters.php?range=7
    Returns {name_lower: {ba, ab}} for players >= FP_BA with >= FP_AB ABs last 7 days.
    Uses curl_cffi (bypasses Cloudflare on Render) with requests fallback.
    """
    url = "https://www.fantasypros.com/mlb/stats/hitters.php?range=7"
    try:
        try:
            from curl_cffi import requests as cffi
            r = cffi.get(url, impersonate="chrome120", timeout=15)
        except ImportError:
            r = requests.get(url, headers=_FP_HDRS, timeout=15)
        if r.status_code != 200:
            return {}
        html = r.text

        tbl = re.search(r'<table[^>]+id=["\']data["\'][^>]*>(.*?)</table>', html, re.DOTALL)
        if not tbl:
            tbl = re.search(r'<table[^>]+class=["\'][^"\']*sortable[^"\']*["\'][^>]*>(.*?)</table>', html, re.DOTALL)
        if not tbl:
            return {}
        table_html = tbl.group(0)

        ab_idx, ba_idx = 3, 15
        thead = re.search(r'<thead>(.*?)</thead>', table_html, re.DOTALL)
        if thead:
            ths  = re.findall(r'<th[^>]*>(.*?)</th>', thead.group(1), re.DOTALL)
            keys = [re.sub(r'<[^>]+>', '', h).strip().upper() for h in ths]
            if "AB"  in keys: ab_idx = keys.index("AB")
            if "AVG" in keys: ba_idx = keys.index("AVG")
            elif "BA" in keys: ba_idx = keys.index("BA")

        tbody = re.search(r'<tbody>(.*?)</tbody>', table_html, re.DOTALL)
        body  = tbody.group(1) if tbody else table_html
        hot: dict = {}

        for row in re.findall(r'<tr[^>]*>(.*?)</tr>', body, re.DOTALL):
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if len(cells) <= max(ab_idx, ba_idx):
                continue
            nm = re.search(r'<a[^>]*>([^<]+)</a>', cells[0])
            if not nm:
                continue
            name = nm.group(1).strip()
            try:
                ab = int(re.sub(r'<[^>]+>', '', cells[ab_idx]).strip())
                ba = float(re.sub(r'<[^>]+>', '', cells[ba_idx]).strip())
            except (ValueError, IndexError):
                continue
            if ab >= FP_AB and ba >= FP_BA:
                hot[name.lower()] = {"ba": ba, "ab": ab}
        return hot
    except Exception as exc:
        print(f"[pipeline] FantasyPros error: {exc}")
        return {}


def _musings_fallback(date_str: str) -> dict:
    """Baseball Musings streak page as fallback."""
    try:
        d = date_str.replace("-", "%2F")
        r = requests.get(
            f"http://baseballmusings.com/cgi-bin/DayStreak.py?DateToCheck={d}",
            timeout=10,
        )
        names: dict = {}
        for m in re.finditer(
            r"<td[^>]*>([A-Z][a-zA-Z\u00e9'\u00e0\u00e1\u00ed\u00f3\u00fa\u00f1\u00fc\-]+"
            r"(?: [A-Z][a-zA-Z\u00e9'\u00e0\u00e1\u00ed\u00f3\u00fa\u00f1\u00fc\-\.]+)+)</td>",
            r.text,
        ):
            n = m.group(1).strip().lower()
            if len(n) > 3:
                names[n] = {"ba": None, "ab": 0}
        return names
    except Exception:
        return {}


# ─────────────────────────  MLB DATA  ───────────────────────────────────────

def _schedule(date_str: str) -> list:
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
    d = _req(f"{MLB}/teams/{team_id}/roster",
             params={"rosterType": "active", "season": _yr()})
    return [
        {"id":   p["person"]["id"],
         "name": p["person"].get("fullName", ""),
         "pos":  p.get("position", {}).get("abbreviation", "")}
        for p in d.get("roster", [])
    ]


def _vs_pitcher(batter_id: int, pitcher_id: int):
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


def _fetch_all_logs(player_id: int):
    yr = _yr()
    return player_id, {s: _game_logs(player_id, s) for s in [yr, yr - 1, yr - 2]}


def _calc_ba(logs: list, side: str, opp_id=None, n: int = 10):
    """Filter by H/A + optional opponent, last n games. Returns dict or None."""
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
    # NOTE: use "is not None" check — 0.000 BA is NOT None
    return {"ba": ba, "games": len(recent), "display": f"{ba:.3f}" if ba is not None else "N/A"}


def _dn_split(player_id: int, game_time_utc: str) -> dict:
    """
    Day/Night BA via MLB Stats API sitCodes (d/n).
    ALWAYS returns a safe dict — NEVER returns None.
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

    sit      = "d" if label == "DAY" else "n"
    data     = _req(f"{MLB}/people/{player_id}/stats", params={
        "stats": "statSplits", "group": "hitting",
        "season": _yr(), "sitCodes": sit,
    })
    fallback = {"ba": None, "display": "N/A", "label": label, "dq": False}
    splits   = data.get("stats", [{}])[0].get("splits", [])
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
        "display": f"{ba:.3f}" if ba is not None else "N/A",
        "label":   label,
        "dq":      ba is not None and ba < DN_MIN,
    }
    _dn_cache[key] = result
    return result


def _top_era_ids(n: int = ERA_CUT) -> set:
    d = _req(f"{MLB}/teams/stats", params={
        "stats": "season", "group": "pitching",
        "season": _yr(), "sportId": 1,
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


# ─────────────────────────  UNDER PICKS  ────────────────────────────────────

def _fetch_dk_lines(log_fn) -> list:
    """Try multiple DK endpoints to find players on 1.5 hit O/U line."""
    for url in _DK_URLS:
        try:
            r = requests.get(url, headers=_DK_HDRS, timeout=10)
            if r.status_code != 200 or not r.text.strip():
                continue
            data = r.json()
            names = []
            def _walk(obj):
                if isinstance(obj, dict):
                    label = obj.get("label", "")
                    for oc in obj.get("outcomes", []):
                        try:
                            lv = float(oc.get("line", 0))
                        except (TypeError, ValueError):
                            lv = 0
                        if lv == 1.5 and str(oc.get("label", "")).lower() == "over" and label:
                            names.append(label.lower())
                    for v in obj.values():
                        _walk(v)
                elif isinstance(obj, list):
                    for item in obj:
                        _walk(item)
            _walk(data)
            if names:
                log_fn(f"  ✅ {len(names)} players on DK 1.5 hit line")
                return names
        except Exception as exc:
            log_fn(f"  ⚠️ DK endpoint failed: {exc}")
    log_fn("  ⚠️ DK unavailable — no under picks this run")
    return []


def _build_under_picks(games: list, log_cache: dict, emit=None) -> list:
    def log(msg):
        if emit:
            emit({"type": "log", "msg": msg})

    # Build name→game map from rosters
    player_map: dict = {}
    for g in games:
        for side in ("HOME", "AWAY"):
            if side == "HOME":
                tid = g["home_id"];  opp = g["away_name"]; oid = g["away_id"]
                pit = g["away_pitcher"]; lu = g["home_lineup"]
            else:
                tid = g["away_id"];   opp = g["home_name"]; oid = g["home_id"]
                pit = g["home_pitcher"]; lu = g["away_lineup"]
            pname = pit.get("fullName", "TBD") if pit else "TBD"
            pid_p = pit.get("id") if pit else None
            for pl in _roster(tid):
                if pl["pos"] in ("P", "TWP"):
                    continue
                player_map[pl["name"].lower()] = {
                    "player_id":  pl["id"],   "name":      pl["name"],
                    "pos":        pl["pos"],   "side":      side,
                    "opp":        opp,         "opp_id":    oid,
                    "pitcher":    pname,       "pitcher_id": pid_p,
                    "lineup_ids": lu,
                }

    log("⬇️ Fetching DraftKings 1.5 hit lines…")
    dk_names = _fetch_dk_lines(log)
    if not dk_names:
        return []

    results = []
    for dk in dk_names:
        info = None
        for key, val in player_map.items():
            if dk in key or key in dk:
                info = val
                break
        if not info:
            continue

        pid  = info["player_id"]
        side = info["side"]
        oid  = info["opp_id"]
        logs = log_cache.get(pid) or {}
        if not logs:
            yr = _yr()
            logs = {s: _game_logs(pid, s) for s in [yr, yr - 1, yr - 2]}
            log_cache[pid] = logs
        all_lg = [g for sl in logs.values() for g in sl]

        # S1 career vs pitcher
        s1_ba, s1_ab = None, 0
        if info["pitcher_id"]:
            s1_ba, s1_ab = _vs_pitcher(pid, info["pitcher_id"])
        s1_disp = f"{s1_ba:.3f}" if s1_ba is not None else "N/A"
        if s1_ba is not None and s1_ab >= MIN_AB and s1_ba >= 0.250:
            continue

        # S2 H/A vs opponent
        s2 = _calc_ba(all_lg, side, opp_id=oid) or {"ba": None, "games": 0, "display": "N/A"}
        if s2["ba"] is not None and s2["games"] >= 3 and s2["ba"] >= 0.225:
            continue

        # S3 season H/A
        cur = logs.get(_yr(), [])
        s3  = _calc_ba(cur, side) or {"ba": None, "games": 0, "display": "N/A"}
        if s3["ba"] is not None and s3["games"] >= 3 and s3["ba"] >= 0.250:
            continue

        lu_ids = info.get("lineup_ids", [])
        lu_st  = ("IN_LINEUP" if pid in lu_ids else "NOT_IN_LINEUP") if lu_ids else "TBD"
        score  = round(((s1_ba or 0) + (s2.get("ba") or 0) + (s3.get("ba") or 0)) * 1000)
        results.append({
            "name": info["name"], "pos": info["pos"],
            "side": side, "opp": info["opp"],
            "pitcher": info["pitcher"],
            "s1_disp": s1_disp, "s1_ab": s1_ab,
            "s2": s2, "s3": s3,
            "lineup_status": lu_st, "under_score": score,
        })
        if emit:
            emit({"type": "under_pick_found", "name": info["name"],
                  "s1": s1_disp, "s2": s2["display"], "s3": s3["display"],
                  "side": side, "opp": info["opp"]})

    results.sort(key=lambda x: x["under_score"])
    return results


# ─────────────────────────  MAIN PIPELINE  ──────────────────────────────────

def run_pipeline(date_str: str, emit=None) -> dict:
    t0 = time.time()
    _dn_cache.clear()

    def log(msg):
        if emit:
            emit({"type": "log", "msg": msg})

    def section(msg):
        if emit:
            emit({"type": "section", "msg": msg})

    # ── Schedule ──────────────────────────────────────────────────────────────
    section("Fetching today's schedule")
    games = _schedule(date_str)
    if not games:
        log("❌ No games found for this date")
        return _empty()
    log(f"✅ {len(games)} games today")

    # ── Hot hitters ───────────────────────────────────────────────────────────
    log(f"🔥 FantasyPros last-7-day hot hitters (>= .{int(FP_BA*1000)} BA, >= {FP_AB} AB)…")
    hot = _fp_hot_hitters()
    if hot:
        log(f"  ✅ {len(hot)} hot hitters found")
    else:
        log("  ⚠️ FantasyPros unavailable — trying Baseball Musings fallback…")
        hot = _musings_fallback(date_str)
        log(f"  {'✅' if hot else '⚠️ 0'} {len(hot)} streak players from Baseball Musings")

    # ── ERA filter ────────────────────────────────────────────────────────────
    log("📊 Fetching team ERA rankings…")
    bad_era = _top_era_ids()
    log(f"  Top-{ERA_CUT} ERA pitching staffs identified — skipping those matchups")

    # ── Step 1 ────────────────────────────────────────────────────────────────
    section(f"Step 1 — Career BA vs pitcher (min {MIN_AB} AB, min {S1_MIN:.3f})")
    pool: list = []

    for g in games:
        for side in ("HOME", "AWAY"):
            if side == "HOME":
                bat_id = g["home_id"]; opp_name = g["away_name"]
                opp_id = g["away_id"]; pitcher  = g["away_pitcher"]
                lu_ids = g["home_lineup"]
            else:
                bat_id = g["away_id"];  opp_name = g["home_name"]
                opp_id = g["home_id"];  pitcher  = g["home_pitcher"]
                lu_ids = g["away_lineup"]

            if not pitcher:
                log(f"  — No probable pitcher for {opp_name}")
                continue
            if opp_id in bad_era:
                log(f"  ⚔️  {opp_name} — top-{ERA_CUT} ERA team, skipping")
                continue

            pid_p  = pitcher["id"]
            name_p = pitcher.get("fullName", "TBD")

            for pl in _roster(bat_id):
                if pl["pos"] in ("P", "TWP"):
                    continue
                pid      = pl["id"]
                name     = pl["name"]
                ba, ab   = _vs_pitcher(pid, pid_p)
                name_key = name.lower()
                in_hot   = name_key in hot
                hot_data = hot.get(name_key, {})

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

    log(f"✅ Step 1: {len(pool)} candidates")
    if emit:
        emit({"type": "step1_done", "msg": f"✅ {len(pool)} players in pool after Step 1"})
    if not pool:
        log("❌ Empty pool — probable pitchers may not be posted yet")
        return _empty()

    # ── Prefetch game logs (parallel) ─────────────────────────────────────────
    section("Steps 2 & 3 — H/A game log splits")
    log("📂 Fetching game logs in parallel…")
    log_cache: dict = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_all_logs, pid): pid
                for pid in {c["player_id"] for c in pool}}
        for fut in as_completed(futs):
            try:
                pid, logs = fut.result()
                log_cache[pid] = logs
            except Exception:
                pass

    # ── Steps 2 & 3 ──────────────────────────────────────────────────────────
    passed:   list = []
    dq_s1_s3: list = []
    total          = len(pool)

    for i, c in enumerate(pool):
        if emit:
            emit({"type": "progress", "current": i + 1, "total": total, "name": c["name"]})

        pid    = c["player_id"]
        side   = c["side"]
        opp_id = c["opp_id"]
        yr     = _yr()
        logs   = log_cache.get(pid, {})
        all_lg = [g for sl in logs.values() for g in sl]

        # Step 2
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

        # Step 3
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
    log(f"✅ Steps 2&3: {len(passed)} passed, {len(dq_s1_s3)} DQ'd")

    # ── Step 4: Day/Night via MLB Stats API ───────────────────────────────────
    section("Step 4 — Day/Night split (MLB Stats API, DQ if < .200)")
    after_dn: list = []
    dq_step4: list = []

    for p in passed:
        dn = _dn_split(p["player_id"], p["game_time"])   # always a safe dict, never None
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
    section("Lineup Check")
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
    section("Under Picks — Cold bats on DK 1.5 hit line")
    under = _build_under_picks(games, log_cache, emit=emit)
    log(f"✅ Under picks: {len(under)} found")

    # ── Pitcher K ─────────────────────────────────────────────────────────────
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


def _empty() -> dict:
    return {
        "top9": [], "also_ran": [], "dq_s1_s3": [], "dq_step4": [],
        "dq_lineup": [], "under_picks": [], "pitcher_k": {"picks": [], "all": []},
        "stats": {"picks": 0, "games": 0, "elapsed": 0, "step1_count": 0,
                  "under_count": 0, "pitcher_k_count": 0, "lineups_posted": 0},
    }
