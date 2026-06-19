"""
fic_cache.py — Step 1: Player pool builder.

SOURCE 1 (PRIMARY): MLB Stats API — career BA vs today's probable pitcher.
        Filter: min 4 AB, min .250 BA. Parallelised (8 threads, fast).

SOURCE 2: MLB Stats API — active hitting streaks (full team scan).
        Scans every hitter on a team playing today, computes current
        hit streak from the official game log. Filter: MIN_STREAK+ games.
        No website scraping.

SOURCE 3: MLB Stats API last-7-day hot hitters — .300+ BA, 5+ AB, always works.

All sources merged + deduplicated, sorted by BA descending.
"""
import json, os, requests
from datetime import date as _date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

CACHE_DIR  = os.environ.get("CACHE_DIR", "/tmp")
MLB_API    = "https://statsapi.mlb.com/api/v1"
MIN_STREAK = 5   # minimum current hit-streak length to qualify (Source 2)


def _cache_path(run_date: str) -> str:
    # v3: threshold lowered to .225
    return os.path.join(CACHE_DIR, f"fic_step1_v3_{run_date.replace('-','')}.json")


def _short_name(full_name: str) -> str:
    parts = full_name.strip().split()
    return f"{parts[0][0]}. {' '.join(parts[1:])}" if parts else full_name


def _parse_avg(s) -> float:
    try:
        s = str(s or "0").strip().replace(",", "")
        if s in ("", "-.--", "-.-", "---", "N/A"):
            return 0.0
        return float(f"0{s}") if s.startswith(".") else float(s)
    except (ValueError, TypeError):
        return 0.0


# ── SOURCE 1: MLB Stats API — career BA vs today's pitcher ─────────────

def _get_schedule_with_pitchers(run_date: str) -> list:
    """Return matchup list: one entry per batting team with probable pitcher."""
    try:
        r = requests.get(f"{MLB_API}/schedule",
            params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"},
            timeout=15)
        out = []
        for dd in r.json().get("dates", []):
            for g in dd.get("games", []):
                ht = g["teams"]["home"]
                at = g["teams"]["away"]
                hp = ht.get("probablePitcher", {})
                ap = at.get("probablePitcher", {})
                away_t = at.get("team", {})
                home_t = ht.get("team", {})
                if hp and away_t.get("id"):
                    out.append({"team_id": away_t["id"],
                                "team_name": away_t.get("name", ""),
                                "pitcher_id": hp["id"],
                                "pitcher_short": _short_name(hp["fullName"])})
                if ap and home_t.get("id"):
                    out.append({"team_id": home_t["id"],
                                "team_name": home_t.get("name", ""),
                                "pitcher_id": ap["id"],
                                "pitcher_short": _short_name(ap["fullName"])})
        return out
    except Exception:
        return []


def _get_all_games(run_date: str) -> list:
    """All games today with team ids + probable pitcher (None if TBD)."""
    try:
        r = requests.get(f"{MLB_API}/schedule",
            params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"},
            timeout=15)
        out = []
        for dd in r.json().get("dates", []):
            for g in dd.get("games", []):
                ht = g["teams"]["home"]; at = g["teams"]["away"]
                hp = ht.get("probablePitcher", {}); ap = at.get("probablePitcher", {})
                out.append({
                    "home_id":   ht.get("team", {}).get("id"),
                    "away_id":   at.get("team", {}).get("id"),
                    "home_name": ht.get("team", {}).get("name", ""),
                    "away_name": at.get("team", {}).get("name", ""),
                    "home_pitcher_id": hp.get("id"),
                    "away_pitcher_id": ap.get("id"),
                    "home_pitcher_short": _short_name(hp["fullName"]) if hp.get("fullName") else None,
                    "away_pitcher_short": _short_name(ap["fullName"]) if ap.get("fullName") else None,
                })
        return out
    except Exception:
        return []


def slate_has_tbd(run_date=None) -> bool:
    """True if any game today is still missing a probable pitcher (TBD)."""
    if run_date is None:
        run_date = _date.today().strftime("%Y-%m-%d")
    games = _get_all_games(run_date)
    if not games:
        return False
    return any((not g["home_pitcher_id"]) or (not g["away_pitcher_id"]) for g in games)


def _check_batter(batter_id, batter_name, pos, pitcher_id, pitcher_short,
                  min_ab, min_ba, team_name="") -> dict | None:
    """Check one batter vs one pitcher. Returns player dict or None."""
    try:
        r = requests.get(f"{MLB_API}/people/{batter_id}/stats",
            params={"stats": "vsPlayerTotal", "group": "hitting",
                    "opposingPlayerId": pitcher_id}, timeout=8)
        for sg in r.json().get("stats", []):
            if "vsPlayer" in sg.get("type", {}).get("displayName", ""):
                for sp in sg.get("splits", []):
                    s  = sp.get("stat", {})
                    ab = s.get("atBats", 0)
                    h  = s.get("hits", 0)
                    hr = s.get("homeRuns", 0)
                    ba = _parse_avg(s.get("avg"))
                    if ab >= min_ab and ba >= min_ba:
                        return {
                            "batter":    _short_name(batter_name),
                            "full_name": batter_name,
                            "player_id": batter_id,
                            "team_name": team_name,
                            "pos":       pos,
                            "pitcher":   pitcher_short,
                            "ab": ab, "h": h, "hr": hr, "ba": ba,
                            "source": "mlb_api",
                        }
    except Exception:
        pass
    return None


def _get_mlb_api_players(run_date: str, min_ab: int, min_ba: float,
                          emit=None) -> list:
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 1: MLB Stats API — career BA vs today's pitchers (parallel)...")
    matchups = _get_schedule_with_pitchers(run_date)
    games    = len(_get_all_games(run_date))
    log(f"   {games} games today")

    # Build all (batter, pitcher) tasks from active rosters
    tasks = []
    seen  = set()
    for m in matchups:
        try:
            r = requests.get(f"{MLB_API}/teams/{m['team_id']}/roster",
                params={"rosterType": "active"}, timeout=10)
            for pl in r.json().get("roster", []):
                if pl.get("position", {}).get("code") == "1":
                    continue   # skip pitchers
                bid = pl["person"]["id"]
                key = (bid, m["pitcher_id"])
                if key in seen:
                    continue
                seen.add(key)
                tasks.append((
                    bid,
                    pl["person"]["fullName"],
                    pl.get("position", {}).get("abbreviation", ""),
                    m["pitcher_id"],
                    m["pitcher_short"],
                    m.get("team_name", ""),
                ))
        except Exception:
            pass

    log(f"   Checking {len(tasks)} batter-pitcher combos (8 threads)...")
    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {
            ex.submit(_check_batter, bid, name, pos, pid, pshort, min_ab, min_ba, tname): None
            for bid, name, pos, pid, pshort, tname in tasks
        }
        for fut in as_completed(futs, timeout=90):
            try:
                r = fut.result()
                if r:
                    results.append(r)
            except Exception:
                pass

    log(f"✅ Source 1: {len(results)} players (min {min_ab} AB, min {min_ba:.3f} BA)")
    return results


# ── SOURCE 2: MLB Stats API — active hitting streaks (full team scan) ──

def _check_streak(batter_id, batter_name, pos, pitcher_short,
                  season, min_streak, team_name="") -> dict | None:
    """Compute current hit streak from the game log. Returns dict or None."""
    try:
        r = requests.get(f"{MLB_API}/people/{batter_id}/stats",
            params={"stats": "gameLog", "group": "hitting",
                    "season": season, "sportId": 1}, timeout=8)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        streak = 0
        s_h = 0
        s_ab = 0
        for sp in reversed(splits):          # most recent game first
            st = sp.get("stat", {})
            ab = int(st.get("atBats", 0) or 0)
            h  = int(st.get("hits", 0) or 0)
            if ab == 0:
                continue                     # no AB (pinch run / DNP) — skip, don't break
            if h >= 1:
                streak += 1
                s_h    += h
                s_ab   += ab
            else:
                break
        if streak >= min_streak:
            ba = round(s_h / s_ab, 3) if s_ab else 0.0
            return {
                "batter":    _short_name(batter_name),
                "full_name": batter_name,
                "player_id": batter_id,
                "team_name": team_name,
                "pos":       pos,
                "pitcher":   pitcher_short,
                "ab": s_ab, "h": s_h, "hr": 0, "ba": ba,
                "streak": streak,
                "source": "mlb_streak",
            }
    except Exception:
        pass
    return None


def _get_streak_players(run_date: str, emit=None, min_streak=MIN_STREAK) -> list:
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 2: MLB Stats API — active hitting streaks (full team scan)...")
    season = run_date[:4]

    # Every team playing today, regardless of whether the opposing pitcher is
    # announced — streaks don't depend on the pitcher.
    team_opp = []
    for g in _get_all_games(run_date):
        if g["home_id"]:
            team_opp.append((g["home_id"], g.get("home_name", ""), g["away_pitcher_short"] or ""))
        if g["away_id"]:
            team_opp.append((g["away_id"], g.get("away_name", ""), g["home_pitcher_short"] or ""))

    tasks = []
    seen  = set()
    for team_id, team_name, opp_pitcher in team_opp:
        try:
            r = requests.get(f"{MLB_API}/teams/{team_id}/roster",
                params={"rosterType": "active"}, timeout=10)
            for pl in r.json().get("roster", []):
                if pl.get("position", {}).get("code") == "1":
                    continue   # skip pitchers
                bid = pl["person"]["id"]
                if bid in seen:
                    continue
                seen.add(bid)
                tasks.append((
                    bid,
                    pl["person"]["fullName"],
                    pl.get("position", {}).get("abbreviation", ""),
                    opp_pitcher,
                    team_name,
                ))
        except Exception:
            pass

    log(f"   Scanning {len(tasks)} hitters for active streaks (8 threads)...")
    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {
            ex.submit(_check_streak, bid, name, pos, pshort, season, min_streak, tname): None
            for bid, name, pos, pshort, tname in tasks
        }
        for fut in as_completed(futs, timeout=120):
            try:
                r = fut.result()
                if r:
                    results.append(r)
            except Exception:
                pass

    log(f"✅ Source 2: {len(results)} players on {min_streak}+ game hitting streaks")
    return results


# ── SOURCE 3: MLB last-7-day hot hitters ─────────────────────────────

def _get_recent_hot_hitters(run_date: str, emit=None) -> list:
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 3: MLB last-7-day hot hitters (.300+ BA, 5+ AB)...")
    try:
        end   = _date.fromisoformat(run_date)
        start = (end - timedelta(days=7)).strftime("%Y-%m-%d")
        r = requests.get(f"{MLB_API}/stats", params={
            "stats": "byDateRange", "group": "hitting",
            "startDate": start, "endDate": run_date,
            "playerPool": "All", "sportId": 1,
            "season": run_date[:4], "limit": 500,
        }, timeout=15)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
    except Exception as exc:
        log(f"   ⚠️ Source 3 failed: {exc}")
        return []

    playing_teams   = set()
    pitcher_by_team = {}
    try:
        r2 = requests.get(f"{MLB_API}/schedule",
            params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"},
            timeout=10)
        for dd in r2.json().get("dates", []):
            for g in dd.get("games", []):
                ht = g["teams"]["home"]; at = g["teams"]["away"]
                hp = ht.get("probablePitcher", {}); ap = at.get("probablePitcher", {})
                playing_teams.update([ht["team"]["id"], at["team"]["id"]])
                if hp: pitcher_by_team[at["team"]["id"]] = _short_name(hp.get("fullName",""))
                if ap: pitcher_by_team[ht["team"]["id"]] = _short_name(ap.get("fullName",""))
    except Exception:
        pass

    results = []
    for sp in splits:
        stat = sp.get("stat", {})
        ab   = int(stat.get("atBats", 0))
        h    = int(stat.get("hits",   0))
        if ab < 5:
            continue
        ba = round(h / ab, 3) if ab > 0 else 0.0
        if ba < 0.300:
            continue
        fname   = sp.get("player", {}).get("fullName", "")
        team_id = sp.get("team", {}).get("id")
        if not fname or not team_id:
            continue
        if playing_teams and team_id not in playing_teams:
            continue
        pos = sp.get("player", {}).get("primaryPosition", {}).get("abbreviation", "")
        results.append({
            "batter":    _short_name(fname),
            "full_name": fname,
            "player_id": sp.get("player", {}).get("id"),
            "team_name": sp.get("team", {}).get("name", ""),
            "pos":       pos,
            "pitcher":   pitcher_by_team.get(team_id, ""),
            "ab":        ab, "h": h,
            "hr":        int(stat.get("homeRuns", 0)),
            "ba":        ba,
            "source":    "mlb_recent_7d",
        })

    log(f"✅ Source 3: {len(results)} hot hitters playing today")
    return results


# ── MERGE ─────────────────────────────────────────────────────────────

def _merge(*sources) -> list:
    merged: dict = {}
    for src in sources:
        for p in src:
            name = p["batter"]
            if name not in merged or p["ba"] > merged[name]["ba"]:
                merged[name] = p
    return sorted(merged.values(), key=lambda x: x["ba"], reverse=True)


# ── PUBLIC API ────────────────────────────────────────────────────────

def get_step1_players_or_scrape(run_date=None, min_ab=4, min_ba=0.225, emit=None):
    if run_date is None:
        run_date = _date.today().strftime("%Y-%m-%d")

    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    path = _cache_path(run_date)
    if os.path.exists(path):
        with open(path) as f:
            players = json.load(f)
        log(f"✅ Loaded {len(players)} players from cache")
        return players

    log("🔍 Building Step 1 player pool...")

    s1 = _get_mlb_api_players(run_date, min_ab, min_ba, emit)
    s2 = _get_streak_players(run_date, emit)
    s3 = _get_recent_hot_hitters(run_date, emit)

    combined = _merge(s1, s2, s3)
    # Tag Source-1 career-qualified (>= .250 career BA vs today's pitcher, min 4
    # AB). The flag survives the merge even when a hotter streak/last-7 copy won
    # the higher-BA tiebreak, so the hit list can be gated to it. Streak/hot-only
    # batters stay in the pool (they still nudge ranking via the merged BA) but
    # are flagged False so they can't reach the hit list unless they ALSO clear
    # .250 vs the pitcher.
    _s1_names = {p["batter"] for p in s1}
    for _p in combined:
        _p["career_qualified"] = _p["batter"] in _s1_names
    log(f"✅ Step 1 pool: {len(combined)} players "
        f"(career vs pitcher: {len(s1)} + streaks: {len(s2)} + last 7d hot: {len(s3)}, deduped)")

    # Don't freeze the pool while any starter is still TBD — let it rebuild so
    # a late-named starter gets picked up automatically on the next run.
    if slate_has_tbd(run_date):
        log("   ⏳ Some starters still TBD — pool not cached, will rebuild next run")
    else:
        with open(path, "w") as f:
            json.dump(combined, f)

    return combined
