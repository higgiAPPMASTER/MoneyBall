
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

# ── BvP pitch-type matchup ────────────────────────────────────────────────
# For each opposing pitcher: load their pitch-usage % from Statcast.
# For each hitter: load their wOBA vs each pitch type from Statcast.
# Compute a weighted wOBA (weighted by pitcher's usage %) → ranking nudge.
# All data is season-level (cached once per run); silently skipped on Savant
# throttle so no picks are lost when the endpoint returns an empty 200.

_PITCH_TYPES    = ["FF", "SL", "SI", "CH", "CU", "FC", "FS", "ST"]
_ARSENAL_CACHE: dict = {}   # pitcher_id (int) -> {pitch_type: usage_pct}
_BATTER_PITCH_CACHE: dict = {}  # batter_id (int) -> {pitch_type: woba}
_PITCH_LOADED:  set  = set()   # years already fetched

_SAVANT_HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
}

_BATTER_SAV_CACHE: dict = {}   # {year: {player_id: {xba, hard_hit_pct}}}
LEAGUE_HARD_HIT   = 35.0       # MLB avg hard-hit rate % (exit velo >= 95 mph), 2024-2025
LEAGUE_XBA        = 0.245      # MLB avg expected batting average (xBA), 2024-2025

def _fetch_batter_savant(year: str) -> dict:
    """Bulk-fetch hitter xBA + hard-hit% from Baseball Savant. Cached per year."""
    if _BATTER_SAV_CACHE.get(year):
        return _BATTER_SAV_CACHE[year]
    import csv, io
    out = {}
    for _attempt in range(2):
        try:
            r = requests.get("https://baseballsavant.mlb.com/leaderboard/custom",
                params={"year": str(year), "type": "batter", "filter": "", "min": "30",
                        "selections": "xba,hard_hit_percent", "csv": "true"},
                headers=_SAVANT_HDRS, timeout=15)
            for row in csv.DictReader(io.StringIO(r.text.lstrip("\ufeff"))):
                try:
                    pid  = int(row.get("player_id") or 0)
                    xba  = row.get("xba") or ""
                    hh   = (row.get("hard_hit_percent") or row.get("hard_hit_pct") or "")
                    if not pid: continue
                    entry: dict = {}
                    if xba not in (None, ""): entry["xba"]          = float(xba)
                    if hh  not in (None, ""): entry["hard_hit_pct"] = float(hh)
                    if entry: out[pid] = entry
                except Exception:
                    continue
        except Exception:
            pass
        if out: break
        time.sleep(1.0)
    if out:
        _BATTER_SAV_CACHE[year] = out
    return out

def _batter_sav_lookup(player_id) -> dict:
    """Return {xba, hard_hit_pct} for this batter — current season, prior-year fallback."""
    if not player_id: return {}
    pid = int(player_id)
    from datetime import date as _d
    yr = str(_d.today().year)
    cur = _BATTER_SAV_CACHE.get(yr, {})
    if pid in cur: return cur[pid]
    prev = _BATTER_SAV_CACHE.get(str(int(yr) - 1), {})
    return prev.get(pid, {})

def _xba_hardhit_adj(player_id, current_ba) -> tuple:
    """(xba_adj, hardhit_adj) ranking nudges from Statcast quality-of-contact metrics.
    xBA gap >= .020 above current BA => positive (due for positive regression).
    Hard-hit % vs 35% avg => +/-50 pts. Ranking-only — never affects totals."""
    sav = _batter_sav_lookup(player_id)
    xba_adj = 0
    if sav.get("xba") is not None and current_ba is not None:
        gap = sav["xba"] - current_ba
        xba_adj = int(max(-100, min(100, round(gap * 2000))))
    hardhit_adj = 0
    if sav.get("hard_hit_pct") is not None:
        hardhit_adj = int(max(-50, min(50, round((sav["hard_hit_pct"] - LEAGUE_HARD_HIT) * 2))))
    return xba_adj, hardhit_adj

def _fetch_one_pt(args):
    """Fetch one (pitch_type, player_type, year) combination from Savant."""
    import csv, io
    pt, ptype, year = args
    try:
        r = requests.get(
            "https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats",
            params={"type": ptype, "pitchType": pt, "year": str(year),
                    "position": "", "team": "", "min": "1",
                    "stat": "p_run_exp", "sort": "1", "sortDir": "desc", "csv": "true"},
            headers=_SAVANT_HDRS, timeout=15)
        txt = r.text.lstrip("\ufeff").strip()
        if not txt or txt.startswith("<"):
            return  # throttled / HTML error
        for row in csv.DictReader(io.StringIO(txt)):
            try:
                pid = int(row.get("player_id") or 0)
                if not pid:
                    continue
                if ptype == "pitcher":
                    pct = float(row.get("pitch_usage") or row.get("pitch_percent") or 0)
                    _ARSENAL_CACHE.setdefault(pid, {})[pt] = pct
                else:
                    w = row.get("woba") or row.get("est_woba") or ""
                    if w:
                        _BATTER_PITCH_CACHE.setdefault(pid, {})[pt] = float(w)
            except Exception:
                continue
    except Exception:
        pass

def _load_pitch_data(year: str) -> None:
    """Parallel-fetch pitcher arsenal + batter-vs-pitch-type wOBA for `year`.
    Capped at 4 workers to be gentle on Savant. No-op if already loaded."""
    if year in _PITCH_LOADED:
        return
    combos = [(pt, ptype, year)
              for pt in _PITCH_TYPES
              for ptype in ("pitcher", "batter")]
    with _TPEx(max_workers=4) as ex:
        list(ex.map(_fetch_one_pt, combos))
    _PITCH_LOADED.add(year)

def _fetch_batting_order(run_date: str) -> dict:
    """Fetch today's confirmed lineups from MLB Stats API.
    Returns {player_id (int): batting_spot (1-9)}.
    Lineup order = position in homePlayers/awayPlayers array (0-indexed → +1).
    Returns {} silently on any error or when lineups aren't posted yet."""
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": run_date, "hydrate": "lineups"},
            timeout=12)
        out = {}
        for date_entry in r.json().get("dates", []):
            for game in date_entry.get("games", []):
                lu = game.get("lineups", {})
                for side in ("homePlayers", "awayPlayers"):
                    for idx, p in enumerate(lu.get(side, [])):
                        pid = p.get("id")
                        if pid:
                            out[int(pid)] = idx + 1   # 1-indexed batting spot
        return out
    except Exception:
        return {}

def _lineup_adj(spot) -> int:
    """Ranking-only nudge based on batting order spot.
    More PAs and protection = higher. Never affects displayed total."""
    if spot is None:
        return 0
    if spot == 1:   return  75   # leadoff — most PAs
    if spot == 2:   return  50
    if spot <= 5:   return  25   # heart of order
    if spot <= 7:   return -25   # bottom third
    return -75                   # 8-9 hole

_ROT_RANK_CACHE: dict = {}   # run_date -> {pitcher_id(int): {"rank","gs","rookie"}}
def _build_rotation_ranks(run_date: str) -> dict:
    """Rank each team's starters by season games-started via the MLB Stats API.
    Returns {pitcher_id: {"rank": 1.., "gs": n, "rookie": bool}}. The most-
    started pitcher on a team is SP1 (the ace), next SP2, etc. Official feed
    only (no scraping). Cached per run_date for the life of the process."""
    if run_date in _ROT_RANK_CACHE:
        return _ROT_RANK_CACHE[run_date]
    season = run_date[:4]
    rank_map: dict = {}
    team_ids = set()
    try:
        sched = requests.get(
            f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={run_date}",
            timeout=15).json()
        for d in sched.get("dates", []):
            for g in d.get("games", []):
                teams = g.get("teams", {}) or {}
                for _sh in ("home", "away"):
                    tid = ((teams.get(_sh) or {}).get("team") or {}).get("id")
                    if tid:
                        team_ids.add(tid)
    except Exception:
        team_ids = set()
    if not team_ids:
        _ROT_RANK_CACHE[run_date] = rank_map
        return rank_map

    def _team_rows(tid):
        try:
            r = requests.get(
                "https://statsapi.mlb.com/api/v1/stats?stats=season&group=pitching"
                f"&season={season}&gameType=R&teamId={tid}&playerPool=all&limit=200",
                timeout=15).json()
            rows = []
            for blk in r.get("stats", []):
                for s in blk.get("splits", []):
                    st = s.get("stat", {}) or {}
                    pl = s.get("player", {}) or {}
                    gs = st.get("gamesStarted", 0) or 0
                    pid = pl.get("id")
                    if gs and pid:
                        rows.append((int(gs), int(pid)))
            rows.sort(key=lambda x: x[0], reverse=True)
            return rows
        except Exception:
            return []

    try:
        with _TPEx(max_workers=8) as _ex:
            all_rows = list(_ex.map(_team_rows, list(team_ids)))
    except Exception:
        all_rows = [_team_rows(t) for t in team_ids]

    pids = []
    for rows in all_rows:
        for i, (gs, pid) in enumerate(rows, 1):
            rank_map[pid] = {"rank": i, "gs": gs, "rookie": False}
            pids.append(pid)

    # Rookie flag — MLB debut this season or last (batched people lookup).
    try:
        cutoff = int(season) - 1
        for i in range(0, len(pids), 40):
            chunk = pids[i:i + 40]
            r = requests.get(
                "https://statsapi.mlb.com/api/v1/people?personIds=" +
                ",".join(str(x) for x in chunk),
                timeout=15).json()
            for person in r.get("people", []):
                pid = person.get("id")
                deb = (person.get("mlbDebutDate") or "")[:4]
                if pid in rank_map and deb.isdigit() and int(deb) >= cutoff:
                    rank_map[pid]["rookie"] = True
    except Exception:
        pass

    _ROT_RANK_CACHE[run_date] = rank_map
    return rank_map


def _pitch_adj(batter_id, pitcher_id) -> int:
    """Ranking-only nudge [-150, +150] from BvP pitch-type matchup.
    High = batter excels vs pitcher's primary pitches; Low = struggles.
    Returns 0 when data is missing for either player (no penalty applied)."""
    if not batter_id or not pitcher_id:
        return 0
    arsenal = _ARSENAL_CACHE.get(int(pitcher_id), {})
    splits  = _BATTER_PITCH_CACHE.get(int(batter_id), {})
    if not arsenal or not splits:
        return 0
    # Weighted avg batter wOBA over pitcher's top-3 pitch types by usage
    total_wgt = wgt_woba = 0.0
    for pt, pct in sorted(arsenal.items(), key=lambda x: -x[1])[:3]:
        w = splits.get(pt)
        if w is not None and pct > 0:
            total_wgt += pct
            wgt_woba  += pct * w
    if total_wgt < 5:   # not enough shared pitch-type data
        return 0
    weighted_woba = wgt_woba / total_wgt
    # League-avg wOBA ~.310; scale deviation to ±150 ranking pts
    return int(max(-150, min(150, round((weighted_woba - 0.310) * 1500))))


































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


def fetch_series_splits(player_id, today_opp: str, run_date: str, side: str = "") -> dict:
    """G1/G2/G3+ BA splits — current season only, filtered by home/away."""
    _EMPTY = {"today_pos": 1, "g1_ba": None, "g1_ab": 0,
               "g2_ba": None, "g2_ab": 0, "g3_ba": None, "g3_ab": 0, "ha": side or ""}
    if not player_id:
        return _EMPTY
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        want_home = (side.upper() == "HOME") if side else None
        all_games = []
        for sp in _get_game_logs(player_id, cy):
            stat = sp.get("stat", {})
            ab = int(stat.get("atBats", 0) or 0)
            if ab < 1:
                continue
            if want_home is not None and sp.get("isHome") != want_home:
                continue
            raw = (sp.get("date") or "")[:10]
            try:
                gdate = _dt.fromisoformat(raw)
            except Exception:
                continue
            all_games.append({
                "date": gdate,
                "hits": int(stat.get("hits", 0) or 0),
                "ab": ab,
                "opp": (sp.get("opponent", {}) or {}).get("name", ""),
            })
        if not all_games:
            return _EMPTY

        all_games.sort(key=lambda g: g["date"])

        # Group consecutive games vs same opponent (≤4 day gap) into series
        pos_stats = {1: [0, 0], 2: [0, 0], 3: [0, 0]}
        i = 0
        while i < len(all_games):
            g = all_games[i]; series = [g]; j = i + 1
            while j < len(all_games):
                prev = all_games[j - 1]; curr = all_games[j]
                if curr["opp"] == g["opp"] and (curr["date"] - prev["date"]).days <= 4:
                    series.append(curr); j += 1
                else:
                    break
            for pos, sg in enumerate(series, 1):
                k = min(pos, 3)
                pos_stats[k][0] += sg["hits"]
                pos_stats[k][1] += sg["ab"]
            i = j

        def _ba(h, a): return round(h / a, 3) if a >= 5 else None

        # Today's series position — count recent consecutive games vs same opp
        try:
            today_d = _dt.fromisoformat(run_date[:10])
        except Exception:
            today_d = _dt.today()
        opp_norm = set((today_opp or "").lower().split()) - {"the", "at", "vs"}
        def _match(opp_str):
            if not opp_norm: return False
            return bool(opp_norm & (set((opp_str or "").lower().split()) - {"the", "at", "vs"}))
        recent = [g for g in reversed(all_games)
                  if _match(g["opp"]) and 0 < (today_d - g["date"]).days <= 5]
        streak = 0; prev_d = today_d
        for g in recent:
            if (prev_d - g["date"]).days <= 2:
                streak += 1; prev_d = g["date"]
            else:
                break

        return {
            "today_pos": min(streak + 1, 3),
            "g1_ba": _ba(pos_stats[1][0], pos_stats[1][1]), "g1_ab": pos_stats[1][1],
            "g2_ba": _ba(pos_stats[2][0], pos_stats[2][1]), "g2_ab": pos_stats[2][1],
            "g3_ba": _ba(pos_stats[3][0], pos_stats[3][1]), "g3_ab": pos_stats[3][1],
            "ha": side or "",
        }
    except Exception:
        return _EMPTY


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


# ── Matchup-value (Log5 + EV) helpers ──────────────────────────────────────
# Blend a batter's season hit ability with the OPPOSING pitcher's hits-allowed
# level (Log5), convert to P(1+ hit), and compare to the posted "to record a
# hit" price to surface +EV value. Frontend uses ev/edge for the green badge,
# the value re-rank (default), and the +EV-only toggle.
_BATTER_RATE_CACHE = {}
_PITCHER_BAA_CACHE = {}


def _ml_implied(am):
    """Implied probability from American odds."""
    try:
        am = float(am)
    except Exception:
        return None
    return (-am) / ((-am) + 100.0) if am < 0 else 100.0 / (am + 100.0)


def _ml_ev(p, am):
    """Expected value per 1u stake at American odds `am` given win prob `p`."""
    try:
        am = float(am)
    except Exception:
        return None
    dec = 1.0 + (100.0 / (-am)) if am < 0 else 1.0 + (am / 100.0)
    return p * (dec - 1.0) - (1.0 - p)


def _set_ev(p, p_win, am):
    """Attach ev / edge / ev_prob to pick `p` for win-prob `p_win` at odds `am`.
    Always defines the three keys (None when not computable) so the frontend can
    render uniformly. `edge` = our prob minus the book's implied prob."""
    p["ev"] = None
    p["edge"] = None
    p["ev_prob"] = None
    if p_win is None or am is None:
        return
    ev = _ml_ev(p_win, am)
    if ev is None:
        return
    p["ev"] = round(ev, 4)
    p["ev_prob"] = round(float(p_win), 4)
    imp = _ml_implied(am)
    if imp is not None:
        p["edge"] = round(float(p_win) - imp, 4)


def _pois_cdf(k, mean):
    """P(X <= k) for X ~ Poisson(mean). Small means only (cheap loop). Used to
    turn a pitcher count projection into an over/under win probability."""
    try:
        if mean is None or mean <= 0:
            return None
        if k < 0:
            return 0.0
        import math
        k = int(k)
        term = math.exp(-mean)
        cum = term
        for i in range(1, k + 1):
            term *= mean / i
            cum += term
        return min(cum, 1.0)
    except Exception:
        return None


def _log5(B, P, Lg):
    """Log5 matchup-adjusted batting average (Bill James)."""
    try:
        n = B * P / Lg
        d = n + ((1 - B) * (1 - P) / (1 - Lg))
        return n / d if d > 0 else B
    except Exception:
        return B


def _get_batter_season_rate(pid, season):
    """Season BA + expected AB/game for a batter, cached. Returns dict or None."""
    key = (pid, season)
    if key in _BATTER_RATE_CACHE:
        return _BATTER_RATE_CACHE[key]
    out = None
    try:
        r = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{pid}/stats",
            params={"stats": "season", "season": season, "group": "hitting"},
            timeout=12,
        )
        st = r.json()["stats"][0]["splits"][0]["stat"]
        ba = float(st.get("avg") or 0)
        ab = int(st.get("atBats") or 0)
        g = int(st.get("gamesPlayed") or 0)
        est_ab = round(ab / g, 2) if g > 0 else 3.9
        if ba > 0:
            out = {"ba": ba, "est_ab": est_ab}
    except Exception:
        out = None
    _BATTER_RATE_CACHE[key] = out
    return out


def _get_pitcher_baa(pid, season):
    """Opposing pitcher's batting-average-against (season), cached. float or None."""
    key = (pid, season)
    if key in _PITCHER_BAA_CACHE:
        return _PITCHER_BAA_CACHE[key]
    baa = None
    try:
        r = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{pid}/stats",
            params={"stats": "season", "season": season, "group": "pitching"},
            timeout=12,
        )
        v = r.json()["stats"][0]["splits"][0]["stat"].get("avg")
        if v not in (None, "-", ".---", ""):
            baa = float(v)
    except Exception:
        baa = None
    _PITCHER_BAA_CACHE[key] = baa
    return baa


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

    # Pre-load batter Savant xBA + hard-hit% leaderboards (cached for the run).
    _yr = run_date[:4]
    _fetch_batter_savant(_yr)
    _fetch_batter_savant(str(int(_yr) - 1))
    emit({"type": "log", "msg": f"  ✅ Batter Savant loaded: {len(_BATTER_SAV_CACHE.get(_yr, {}))} hitters (xBA + hard-hit%)"})

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

        _sav_dat = _batter_sav_lookup(player_id)
        _xba_adj, _hh_adj = _xba_hardhit_adj(player_id, p.get("ba"))
        player_result = {
            "name": name, "pos": p["pos"], "s1": p["ba"],
            "team": team, "opp": opp_name, "side": side, "slug": slug,
            "full_name": info.get("full_name", name),
            "pitcher": pitcher_map.get(name, ""),
            "s2": s2, "s3": s3, "total": total,
            "dq": bool(dq), "dq_reason": " & ".join(dq),
            "player_id": player_id,
            "game_start": game_start,
            "xba": _sav_dat.get("xba"), "hard_hit_pct": _sav_dat.get("hard_hit_pct"),
            "xba_adj": _xba_adj, "hardhit_adj": _hh_adj,
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
    emit({"type": "section", "msg": f"Step 5 — Pitcher ERA reference (top {TOP_N_ERA_PITCHERS} lowest ERA · display only, never removes a hitter)"})
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
                # DISPLAY ONLY — tag a reference chip for the card. Top 30 ERA is a
                # reference, NOT a gate: the hitter is never removed from the pool.
                r["facing_top_era"] = pitcher_raw
                r["top_era_val"] = matched_era
                emit({"type": "log", "msg": f"  • {r['name']} — facing top-ERA {pitcher_raw}{era_str} (reference only)"})
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

    _bat_order: dict = {}   # player_id -> batting spot (1-9); populated below
    # ── Batting Order Position ─────────────────────────────────────────────
    emit({"type": "section", "msg": "Lineup context — fetching batting order spots"})
    try:
        _bat_order = _fetch_batting_order(run_date)
        _spot_n = 0
        for r in lineup_qualified:
            pid   = r.get("player_id")
            spot  = _bat_order.get(int(pid)) if pid else None
            r["lineup_spot"] = spot
            r["lineup_adj"]  = _lineup_adj(spot)
            if spot:
                _spot_n += 1
        emit({"type": "log", "msg": f"  Batting order: {_spot_n}/{len(lineup_qualified)} hitters placed"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"  Batting order skipped: {_exc}"})
        for r in lineup_qualified:
            r.setdefault("lineup_spot", None)
            r.setdefault("lineup_adj", 0)

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
            r["pit_id"] = pit_id   # stored for pitch-type matchup ranking
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

    # ── BvP pitch-type matchup ranking nudge ──────────────────────────
    # Loads Statcast pitch-arsenal + batter-vs-pitch-type wOBA once per season.
    # Silently skipped on Savant throttle — no picks are lost.
    emit({"type": "section", "msg": "BvP pitch-type matchup — loading Statcast arsenal"})
    try:
        _load_pitch_data(run_date[:4])
        _adj_n = 0
        for r in lineup_qualified:
            adj = _pitch_adj(r.get("player_id"), r.get("pit_id"))
            r["pitch_adj"] = adj
            if adj != 0:
                _adj_n += 1
        emit({"type": "log", "msg": f"  Pitch-type adj applied to {_adj_n}/{len(lineup_qualified)} hitters"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"  Pitch-type adj skipped: {_exc}"})
        for r in lineup_qualified:
            r.setdefault("pitch_adj", 0)

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
        key=lambda x: x.get("total", 0) + x.get("pitch_adj", 0) + x.get("lineup_adj", 0),
        reverse=True,
    )
    top9     = all_ranked[:10]
    also_ran = all_ranked[10:]

    # Recent form: last 5 games (date/opp/hits/total-bases) for the click-through popup
    for _hp in top9 + also_ran:
        _hp["recent_hit_log"] = _recent_hit_log(_hp.get("player_id"))

    # Series game-position splits (G1/G2/G3+)
    for _hp in top9 + also_ran:
        _hp["series_splits"] = fetch_series_splits(
            _hp.get("player_id"), _hp.get("opp", ""), run_date, _hp.get("side", "")
        )

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
        from under_picks import (HIT_ODDS as _HIT_ODDS, HIT_ODDS_BOOK as _HIT_ODDS_BOOK,
                                  _norm_name as _nn, _book_label as _hit_book_label)
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
                if v in _HIT_ODDS: return v
            # 2. unambiguous last-name fallback (skip common last names)
            seen_last = set()
            for v in candidates:
                parts = v.split()
                last = parts[-1] if parts else ""
                if not last or last in seen_last: continue
                seen_last.add(last)
                matches = _last_idx.get(last, [])
                if len(matches) == 1: return matches[0][0]
            return None

        for _p in top9 + also_ran:
            # Hit picks are always the "to record a hit" OVER on the 0.5 line, so the
            # displayed-side book is whichever book posted that best price.
            _mk = _lookup_odds(_p)
            _p["hit_odds"] = _HIT_ODDS.get(_mk) if _mk else None
            _p["book"]     = _hit_book_label(_HIT_ODDS_BOOK.get(_mk)) if _mk else ""
        emit({"type": "log", "msg": f"  ✅ Hit odds matched for {sum(1 for p in top9+also_ran if p.get('hit_odds') is not None)}/{len(top9)+len(also_ran)} picks"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Hit odds enrichment skipped: {_exc}"})

    # ── Matchup-value (Log5 + EV) enrichment for hit picks ──────────────
    # Adds matchup_prob / season_ba / proj_baa / impl_prob / ev / edge to every
    # hit pick. Frontend re-ranks by ev (default keeps ALL plays) + shows a green
    # edge badge, with a "+EV only" toggle. No play is dropped server-side.
    try:
        _SEASON_YR = str(run_date)[:4]
        _LG_BA = 0.244
        _pid_map = {tn: pi.get("id") for tn, pi in mlb_probable.items() if pi.get("id")}
        _stopw = {"the", "of", "los", "san", "new", "de"}

        def _opp_pid(opp):
            ol = (opp or "").lower()
            if not ol:
                return None
            for tn, pid in _pid_map.items():
                if tn.lower() == ol:
                    return pid
            for tn, pid in _pid_map.items():
                if (set(tn.lower().split()) - _stopw) & (set(ol.split()) - _stopw):
                    return pid
            return None

        _ev_pool = list(top9) + list(also_ran)
        _bids = {p.get("player_id") for p in _ev_pool if p.get("player_id")}
        _opids = {x for x in (_opp_pid(p.get("opp", "")) for p in _ev_pool) if x}
        with _TPEx(max_workers=8) as _ex:
            list(_ex.map(lambda i: _get_batter_season_rate(i, _SEASON_YR), _bids))
        with _TPEx(max_workers=8) as _ex:
            list(_ex.map(lambda i: _get_pitcher_baa(i, _SEASON_YR), _opids))

        _ev_n = 0
        for _p in _ev_pool:
            _p["matchup_prob"] = None
            _p["ev"] = None
            _p["edge"] = None
            _p["impl_prob"] = None
            _p["proj_baa"] = None
            _br = _get_batter_season_rate(_p.get("player_id"), _SEASON_YR)
            if not _br:
                continue
            _opid = _opp_pid(_p.get("opp", ""))
            _baa = _get_pitcher_baa(_opid, _SEASON_YR) if _opid else None
            _ba, _est_ab = _br["ba"], _br["est_ab"]
            _adj = _log5(_ba, _baa, _LG_BA) if _baa else _ba
            _mp = 1.0 - (1.0 - _adj) ** _est_ab
            _p["matchup_prob"] = round(_mp, 4)
            _p["season_ba"] = round(_ba, 3)
            _p["est_ab"] = _est_ab
            if _baa:
                _p["proj_baa"] = round(_baa, 3)
            _am = _p.get("hit_odds")
            if _am is not None:
                _imp = _ml_implied(_am)
                _e = _ml_ev(_mp, _am)
                _p["impl_prob"] = round(_imp, 4) if _imp is not None else None
                _p["ev"] = round(_e, 4) if _e is not None else None
                _p["edge"] = round(_mp - _imp, 4) if _imp is not None else None
                _ev_n += 1
        emit({"type": "log", "msg": f"  ✅ Matchup EV computed for {_ev_n}/{len(_ev_pool)} hit picks"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Matchup EV enrichment skipped: {_exc}"})

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
        _up["series_splits"]  = fetch_series_splits(_up.get("batter_id"), _up.get("opp", ""), run_date, _up.get("side", ""))

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
        _rp["game_start"]    = _game_start_for(_rp.get("team", ""))
        _rp["series_splits"] = fetch_series_splits(_rp.get("batter_id"), _rp.get("opp", ""), run_date, _rp.get("side", ""))

    # ── TB Under Picks (batter total bases Under 1.5) ─────────────────────
    try:
        from under_picks import run_tb_under_picks
        tb_picks_list = run_tb_under_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ TB Under picks skipped: {exc}"})
        tb_picks_list = []
    for _tp in tb_picks_list:
        _tp["game_start"]    = _game_start_for(_tp.get("team", ""))
        _tp["series_splits"] = fetch_series_splits(_tp.get("batter_id"), _tp.get("opp", ""), run_date, _tp.get("side", ""))

    # ── TB Over Picks (batter total bases Over 1.5) ───────────────────────
    try:
        from under_picks import run_tb_over_picks
        tb_over_picks_list = run_tb_over_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ TB Over picks skipped: {exc}"})
        tb_over_picks_list = []
    for _tov in tb_over_picks_list:
        _tov["game_start"]    = _game_start_for(_tov.get("team", ""))
        _tov["series_splits"] = fetch_series_splits(_tov.get("batter_id"), _tov.get("opp", ""), run_date, _tov.get("side", ""))

    # ── RBI Picks (Batter RBIs, Over/Under 0.5) ───────────────────────────
    try:
        from under_picks import run_rbi_picks
        rbi_picks_list = run_rbi_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ RBI picks skipped: {exc}"})
        rbi_picks_list = []
    for _xp in rbi_picks_list:
        _xp["game_start"]    = _game_start_for(_xp.get("team", ""))
        _xp["series_splits"] = fetch_series_splits(_xp.get("batter_id"), _xp.get("opp", ""), run_date, _xp.get("side", ""))

    # ── Batter Walks Picks (Batter Walks, Over/Under 0.5) ─────────────────
    try:
        from under_picks import run_walks_picks
        walks_picks_list = run_walks_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ Batter Walks picks skipped: {exc}"})
        walks_picks_list = []
    for _wp in walks_picks_list:
        _wp["game_start"]    = _game_start_for(_wp.get("team", ""))
        _wp["series_splits"] = fetch_series_splits(_wp.get("batter_id"), _wp.get("opp", ""), run_date, _wp.get("side", ""))

    # ── HRR Picks (Hits+Runs+RBI Over 1.5) ────────────────────────────────
    try:
        from under_picks import run_hrr_picks
        hrr_picks_list = run_hrr_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ HRR picks skipped: {exc}"})
        hrr_picks_list = []
    for _hp in hrr_picks_list:
        _hp["game_start"]    = _game_start_for(_hp.get("team", ""))
        _hp["series_splits"] = fetch_series_splits(_hp.get("batter_id"), _hp.get("opp", ""), run_date, _hp.get("side", ""))

    # ── Pitcher series position (G1/G2/G3) ────────────────────────────────
    # Pitchers have no batting logs, so derive each game's series slot from the
    # hitters in that same game: a pitcher's start IS game N of his team's
    # series, and his team's hitters already carry today_pos vs the opponent.
    # Use the MAX today_pos seen for the team (the hitter who played every game
    # reflects the true series position). Stamp a minimal series_splits so the
    # frontend G# badge + strategy dot render on pitcher cards too.
    try:
        _team_pos: dict = {}
        for _lst in (top9, also_ran, under_picks_list, runs_picks_list,
                     tb_picks_list, tb_over_picks_list, rbi_picks_list,
                     walks_picks_list, hrr_picks_list):
            for _hh in _lst:
                _tm  = (_hh.get("team") or "").strip()
                _pos = ((_hh.get("series_splits") or {}).get("today_pos")) or 0
                if _tm and _pos and _pos > _team_pos.get(_tm, 0):
                    _team_pos[_tm] = _pos

        def _pos_for_team(team_name):
            if not team_name:
                return 0
            _p = _team_pos.get(team_name)
            if _p:
                return _p
            _tl = team_name.lower()
            for _k, _v in _team_pos.items():
                if _tl in _k.lower() or _k.lower() in _tl:
                    return _v
            return 0

        _pit_all = list(pitcher_k_result.get("picks", [])) + list(pitcher_k_result.get("all", []))
        for _b in pitcher_props.values():
            _pit_all += list(_b.get("picks", [])) + list(_b.get("all", []))
        _stamped = 0
        for _pp in _pit_all:
            _pos = _pos_for_team(_pp.get("team", ""))
            if _pos:
                _pp["series_splits"] = {"today_pos": _pos}
                _stamped += 1
        emit({"type": "log", "msg": f"  ✅ Pitcher series position stamped ({_stamped} picks, {len(_team_pos)} teams)"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Pitcher series position skipped: {_exc}"})

    # ── EV enrichment for ALL non-hit categories ────────────────────────
    # Each pick gets ev / edge / ev_prob from our model probability vs the
    # posted price for the SIDE we picked. Binary 0.5/1.5 batter markets use the
    # empirical vs-opp rate (score); Under-1.5-hits uses a binomial off season
    # BA; pitcher count markets use a Poisson off the opponent-adjusted
    # projection. Frontend shows a badge; the "+EV only" toggle filters on
    # ev>0. No play is dropped server-side. Hit picks already enriched above.
    try:
        _SY = str(run_date)[:4]

        def _ev_ou(p, p_over, over_am, under_am):
            """Two-sided market: P(OVER)=p_over, attach for the picked side."""
            if p.get("pick") == "UNDER":
                _set_ev(p, (1.0 - p_over) if p_over is not None else None, under_am)
            else:
                _set_ev(p, p_over, over_am)

        for _p in rbi_picks_list:
            _s = _p.get("score")
            _ev_ou(_p, (_s / 100.0) if _s is not None else None,
                   _p.get("over_odds"), _p.get("under_odds"))
        for _p in walks_picks_list:
            _s = _p.get("score")
            _ev_ou(_p, (_s / 100.0) if _s is not None else None,
                   _p.get("over_odds"), _p.get("under_odds"))
        for _p in runs_picks_list:
            _s = _p.get("score")
            _ev_ou(_p, (_s / 100.0) if _s is not None else None,
                   _p.get("over_odds"), _p.get("under_odds"))
        for _p in hrr_picks_list:
            _s = _p.get("score")
            _ev_ou(_p, (_s / 100.0) if _s is not None else None,
                   _p.get("hrr_over_odds"), _p.get("hrr_under_odds"))
        for _p in tb_over_picks_list:                 # OVER only
            _s = _p.get("score")
            _set_ev(_p, (_s / 100.0) if _s is not None else None,
                    _p.get("tb_over_odds"))
        for _p in tb_picks_list:                      # TB UNDER (score = % OVER)
            _s = _p.get("score")
            _po = (_s / 100.0) if _s is not None else None
            _set_ev(_p, (1.0 - _po) if _po is not None else None,
                    _p.get("tb_under_odds"))

        # Under-1.5-hits: P(<=1 hit) = binomial(0)+binomial(1) off season BA.
        for _p in under_picks_list:
            _br = _get_batter_season_rate(_p.get("batter_id"), _SY)
            if _br:
                _b, _n = _br["ba"], _br["est_ab"]
                _pu = (1.0 - _b) ** _n + _n * _b * (1.0 - _b) ** (_n - 1)
                _set_ev(_p, min(max(_pu, 0.0), 1.0), _p.get("under_odds"))
            else:
                _set_ev(_p, None, None)

        # Pitcher count markets — Poisson off the projection (fallback blend).
        import math as _math
        def _ev_pois(p, mean):
            ln = p.get("line")
            if mean is None or ln is None:
                _set_ev(p, None, None)
                return
            if p.get("pick") == "UNDER":
                _cdf = _pois_cdf(int(_math.floor(ln)), mean)
                _set_ev(p, _cdf, p.get("under_odds"))
            else:
                _alt = p.get("sugg_line")
                if _alt is not None and p.get("pick") == "OVER":
                    _c = _pois_cdf(int(_math.floor(_alt)), mean)
                    _set_ev(p, (1.0 - _c) if _c is not None else None,
                            p.get("sugg_odds") or p.get("over_odds"))
                else:
                    _c = _pois_cdf(int(_math.floor(ln)), mean)
                    _set_ev(p, (1.0 - _c) if _c is not None else None,
                            p.get("over_odds"))

        for _pk in (pitcher_k_result.get("picks", [])
                    + pitcher_k_result.get("all", [])):
            _ev_pois(_pk, _pk.get("proj_k") if _pk.get("proj_k") is not None
                     else _pk.get("blended_avg_k"))
        for _mkt, _bk in pitcher_props.items():
            for _pp in _bk.get("picks", []):
                _ev_pois(_pp, _pp.get("proj") if _pp.get("proj") is not None
                         else _pp.get("blended"))
        emit({"type": "log", "msg": "  ✅ EV computed for all non-hit categories"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Category EV enrichment skipped: {_exc}"})

    # ── Game-environment re-ranking inputs (weather + home-plate umpire) ──
    # Build the shared target list ONCE; ballpark/weather and umpire each stamp
    # onto it (Phase A), then a single combined Phase B re-ranks every category
    # (reorder only — qualification gates are never touched, so the same picks
    # appear, just in a different order). Either factor missing -> neutral 1.0.
    _rr_targets = list(top9) + list(also_ran) + list(under_picks_list) + list(runs_picks_list) + list(tb_picks_list) + list(tb_over_picks_list) + list(rbi_picks_list) + list(walks_picks_list) + list(hrr_picks_list)
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

    # ── Platoon adj for hits picks ────────────────────────────────────────
    # Favorable handedness matchup: LHB vs RHP or RHB vs LHP → +50 boost.
    # Unfavorable: -25 drag. 0 when data absent.
    def _plat_adj(r):
        pl = r.get("platoon") or {}
        adv = pl.get("adv")
        if adv is True:   return 50
        if adv is False:  return -25
        return 0
    for _hp in list(top9) + list(also_ran):
        _hp["platoon_adj"] = _plat_adj(_hp)

    # ── Attach pitch_adj + lineup_adj to ALL non-hits hitter categories ──
    # Each non-hits pick already carries batter_id from under_picks.py.
    # We look up the opp probable pitcher from mlb_probable to get pit_id,
    # then call the same _pitch_adj / _lineup_adj functions used for hits.
    # Falls back to 0 when data is absent (cache miss, no probable pitcher).
    def _opp_pit_id(opp_name: str):
        """Return probable pitcher MLB id for opp team, or None."""
        if not opp_name:
            return None
        ol = opp_name.lower()
        sw = {"the", "of", "los", "san", "new", "de"}
        for tn, pi in mlb_probable.items():
            if not pi.get("id"):
                continue
            if tn.lower() == ol:
                return pi["id"]
        for tn, pi in mlb_probable.items():
            if not pi.get("id"):
                continue
            twords = set(tn.lower().split()) - sw
            owords = set(ol.split()) - sw
            if twords and owords and twords & owords:
                return pi["id"]
        return None

    _nonhit_all = (
        under_picks_list + runs_picks_list +
        tb_picks_list + tb_over_picks_list +
        rbi_picks_list + walks_picks_list + hrr_picks_list
    )
    _nh_enriched = 0
    for _np in _nonhit_all:
        bid  = _np.get("batter_id")
        pit  = _opp_pit_id(_np.get("opp", ""))
        padj = _pitch_adj(bid, pit) if bid else 0
        spot = _bat_order.get(int(bid)) if bid else None
        ladj = _lineup_adj(spot)
        _np.setdefault("pitch_adj",  padj)
        _np.setdefault("lineup_adj", ladj)
        if padj or ladj:
            _nh_enriched += 1
    emit({"type": "log", "msg": f"  ✅ Pitch/lineup adj attached to {_nh_enriched}/{len(_nonhit_all)} non-hits picks"})

    # ── Rotation rank (SP1..SP5) — drives the card depth-chart dot ──────────
    # Rank each team's pitchers by season games-started (most-started = ace,
    # SP1). Hitters get opp_rot_rank (the arm they face); pitchers get rot_rank
    # (their own). Frontend tiers: SP1 ace, SP2-3 mid (neutral), SP4+/rookie
    # back-end. MLB Stats API only — no scraping.
    try:
        _rot = _build_rotation_ranks(run_date)
    except Exception as _rexc:
        emit({"type": "log", "msg": f"⚠️ Rotation ranks skipped: {_rexc}"})
        _rot = {}

    def _rot_get(pid):
        try:
            return _rot.get(int(pid)) if pid else None
        except Exception:
            return None

    def _set_opp_rot(pick, pid):
        info = _rot_get(pid)
        if info:
            pick["opp_rot_rank"]   = info.get("rank")
            pick["opp_rot_rookie"] = info.get("rookie", False)

    def _set_own_rot(pick, pid):
        info = _rot_get(pid)
        if info:
            pick["rot_rank"]   = info.get("rank")
            pick["rot_rookie"] = info.get("rookie", False)

    for _hp in list(top9) + list(also_ran):
        _set_opp_rot(_hp, _hp.get("pit_id"))
    for _np in _nonhit_all:
        _set_opp_rot(_np, _np.get("pit_id") or _opp_pit_id(_np.get("opp", "")))
    _pk_all = list(pitcher_k_result.get("picks", [])) + list(pitcher_k_result.get("all", []))
    for _mkt, _bucket in (pitcher_k_result.get("props", {}) or {}).items():
        _pk_all += list(_bucket.get("picks", [])) + list(_bucket.get("all", []))
    for _pk in _pk_all:
        _set_own_rot(_pk, _pk.get("pid"))
    _rot_hit = sum(1 for x in (list(top9) + list(also_ran) + _nonhit_all) if x.get("opp_rot_rank"))
    _rot_pit = sum(1 for x in _pk_all if x.get("rot_rank"))
    emit({"type": "log", "msg": f"  ✅ Rotation rank: {len(_rot)} pitchers ranked, attached to {_rot_hit} hitters + {_rot_pit} pitcher picks"})

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

    # Hitters ("to record a hit", all OVER): points × offense factor, plus
    # pitch-type BvP, lineup-spot, and platoon-matchup adjustments.
    # Re-split the headline Top-10 vs Money Ball from the same pool.
    _hit_pool = list(top9) + list(also_ran)
    _hit_pool.sort(
        key=lambda x: (
            x.get("total", 0)
            + x.get("pitch_adj", 0)
            + x.get("lineup_adj", 0)
            + x.get("platoon_adj", 0)
            + x.get("xba_adj", 0)
            + x.get("hardhit_adj", 0)
        ) * _offf(x),
        reverse=True,
    )
    top9     = _hit_pool[:10]
    also_ran = _hit_pool[10:]

    # Under 1.5 hits (all UNDER): under_score lower=colder=better.
    # Good BvP (high pitch_adj) or high lineup spot = more PAs = worse under
    # → ADD adj to push those picks DOWN the board.
    under_picks_list.sort(key=lambda p: (
        (p.get("under_score", 0) + p.get("pitch_adj", 0) + p.get("lineup_adj", 0)) * _offf(p),
        p.get("name", ""),
    ))

    # Runs 0.5: OVERs boosted (-wilson × offense - adjs), UNDERs penalized
    # (score × offense + adjs). OVER block stays ahead of UNDER block.
    runs_picks_list.sort(key=lambda p: (
        0 if p.get("pick") == "OVER" else 1,
        (-(p.get("wilson", 0) * _offf(p))
         - p.get("pitch_adj", 0) - p.get("lineup_adj", 0))
        if p.get("pick") == "OVER"
        else (p.get("score", 0) * _offf(p)
              + p.get("pitch_adj", 0) + p.get("lineup_adj", 0)),
        -p.get("games", 0),
    ))

    # RBI 0.5: OVERs ranked by wilson × offense + adjs; UNDERs by score × offense + adjs.
    rbi_picks_list.sort(key=lambda p: (
        0 if p.get("pick") == "OVER" else 1,
        -((p.get("wilson", 0) * _offf(p))
          + p.get("pitch_adj", 0) + p.get("lineup_adj", 0))
        if p.get("pick") == "OVER"
        else (p.get("score", 0) * _offf(p)
              + p.get("pitch_adj", 0) + p.get("lineup_adj", 0)),
        -p.get("games", 0),
    ))

    # Batter Walks 0.5: same signed-adj pattern as RBI.
    walks_picks_list.sort(key=lambda p: (
        0 if p.get("pick") == "OVER" else 1,
        -((p.get("wilson", 0) * _offf(p))
          + p.get("pitch_adj", 0) + p.get("lineup_adj", 0))
        if p.get("pick") == "OVER"
        else (p.get("score", 0) * _offf(p)
              + p.get("pitch_adj", 0) + p.get("lineup_adj", 0)),
        -p.get("games", 0),
    ))

    # HRR 1.5: same signed-adj pattern as RBI.
    hrr_picks_list.sort(key=lambda p: (
        0 if p.get("pick") == "OVER" else 1,
        -((p.get("wilson", 0) * _offf(p))
          + p.get("pitch_adj", 0) + p.get("lineup_adj", 0))
        if p.get("pick") == "OVER"
        else (p.get("score", 0) * _offf(p)
              + p.get("pitch_adj", 0) + p.get("lineup_adj", 0)),
        -p.get("games", 0),
    ))

    # TB Over 1.5 (all OVER): wilson × offense + adjs, best at top.
    tb_over_picks_list.sort(
        key=lambda p: (
            p.get("wilson", 0) * _offf(p)
            + p.get("pitch_adj", 0) + p.get("lineup_adj", 0)
        ),
        reverse=True,
    )

    # TB Under 1.5 (all UNDER): score + adjs × offense; lower = colder = better.
    tb_picks_list.sort(
        key=lambda p: (
            p.get("score", 0) + p.get("pitch_adj", 0) + p.get("lineup_adj", 0)
        ) * _offf(p),
    )

    # Pitcher props — Hits Allowed + Earned Runs use the offense axis; Walks use
    # the umpire walk factor (wide zone -> fewer walks -> Under boosted). Outs +
    # K excluded here. OVER × factor, UNDER × 1/factor.
    def _prop_val(x):
        """Use the fully-adjusted projection when available; fall back to blend."""
        p = x.get("proj")
        return p if p is not None else (x.get("blended") or 0)

    for _mkt, _bk in pitcher_props.items():
        if _mkt in ("pitcher_hits_allowed", "pitcher_earned_runs"):
            _bk["picks"].sort(
                key=lambda x: abs(_prop_val(x) - (x.get("line") or 0))
                              * (_offf(x) if x.get("pick") == "OVER" else 1.0 / _offf(x)),
                reverse=True,
            )
        elif _mkt == "pitcher_walks":
            _bk["picks"].sort(
                key=lambda x: abs(_prop_val(x) - (x.get("line") or 0))
                              * (_umpf(x, "bbFactor") if x.get("pick") == "OVER"
                                 else 1.0 / _umpf(x, "bbFactor")),
                reverse=True,
            )
    emit({"type": "log", "msg": "  ✅ Env + umpire re-ranking applied (reorder only, gates untouched)"})

    elapsed = round(time.time() - t_start, 1)
    result = {
        "date": run_date, "top9": top9, "also_ran": also_ran,
        "under_picks": under_picks_list, "runs_picks": runs_picks_list, "tb_picks": tb_picks_list, "tb_over_picks": tb_over_picks_list, "rbi_picks": rbi_picks_list, "walks_picks": walks_picks_list, "hrr_picks": hrr_picks_list,
        "all_qualified": era_qualified,
        "dq_s1_s3": [x for x in results if x["dq"] and x not in dn_dq and x not in era_dq and x not in dq_lineup and x not in s4_dq],
        "dq_step4": dn_dq, "dq_step5": era_dq, "dq_lineup": dq_lineup, "dq_s4": s4_dq, "pitcher_k": pitcher_k_result,
        "pitcher_props": pitcher_props,
        "stats": {"step1_count": len(top30), "games": len(team_schedule) // 2,
                  "elapsed": elapsed, "picks": len(top9),
                  "under_count": len(under_picks_list),
                  "runs_count": len(runs_picks_list),
                  "tb_count": len(tb_picks_list),
                  "tb_over_count": len(tb_over_picks_list),
                  "rbi_count": len(rbi_picks_list),
                  "walks_count": len(walks_picks_list),
                  "hrr_count": len(hrr_picks_list),
                  "pitcher_k_count": len(pitcher_k_result.get("picks", [])),
                  "prop_counts": {m: len(b.get("picks", [])) for m, b in pitcher_props.items()},
                  "has_tbd": slate_has_tbd(run_date)},
    }
    emit({"type": "done", "result": result})
    return result
