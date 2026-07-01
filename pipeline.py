
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

_ROT_RANK_CACHE: dict = {}    # run_date -> {pitcher_id(int): {"rank","gs","recent","rookie"}}
_ROT_EDITOR_CACHE: dict = {}  # run_date -> {tid(str): {"name","pitchers":[...],"has_override"}}

# ── Manual rotation override (admin-set, persisted in Supabase) ──────────
# Render's filesystem is ephemeral, so the admin's hand-ranked rotation order
# lives in the shared mpa_track_ledger table as a single special row:
#   app=mlb, date=__rotation__, category=__rotation__, side=ALL,
#   detail = { "<team_id>": [[pid, "Name"], ...], ... }   (list order = rank 1..n)
# pipeline.py cannot import main.py, so it carries its own tiny read-only client.
_SB_URL_RAW_PL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
_SB_URL_PL = (f"https://{_SB_URL_RAW_PL}.supabase.co"
              if _SB_URL_RAW_PL and not _SB_URL_RAW_PL.startswith("http")
              else _SB_URL_RAW_PL)
_SB_KEY_PL = os.environ.get("SUPABASE_SERVICE_KEY", "")
_ROT_OVR_CAT = "__rotation__"

def _load_rot_override() -> dict:
    """Read the admin rotation-override row. Returns
    {team_id(str): {"order":[(pid,name),...], "inj":[(pid,name),...],
                    "tier":{pid:1|2|3}}} or {}. tier is a per-pitcher tier label
    override (1 ace / 2 mid / 3 back-end); absent/0 = auto (rank-derived).
    Back-compat: a legacy plain-list detail value is read as order-only (no INJ)."""
    if not (_SB_URL_PL and _SB_KEY_PL):
        return {}
    try:
        hdrs = {"apikey": _SB_KEY_PL, "Authorization": f"Bearer {_SB_KEY_PL}"}
        rsp = requests.get(
            f"{_SB_URL_PL}/rest/v1/mpa_track_ledger", headers=hdrs,
            params={"app": "eq.mlb", "category": f"eq.{_ROT_OVR_CAT}",
                    "side": "eq.ALL", "date": f"eq.{_ROT_OVR_CAT}",
                    "select": "detail", "limit": "1"}, timeout=10)
        if rsp.status_code == 200 and rsp.json():
            det = (rsp.json()[0] or {}).get("detail") or {}

            def _pairs(lst):
                norm = []
                for it in (lst or []):
                    if isinstance(it, (list, tuple)) and it:
                        norm.append((int(it[0]),
                                     str(it[1]) if len(it) > 1 else ""))
                    elif isinstance(it, dict) and it.get("id") is not None:
                        norm.append((int(it["id"]), str(it.get("name", ""))))
                return norm

            out = {}
            for tid, val in det.items():
                tier = {}
                if isinstance(val, dict):
                    order = _pairs(val.get("order"))
                    inj = _pairs(val.get("inj"))
                    for pid_, n_ in (val.get("tier") or {}).items():
                        try:
                            ni = int(n_)
                        except Exception:
                            continue
                        if ni in (1, 2, 3):
                            tier[int(pid_)] = ni
                else:
                    order = _pairs(val)   # legacy: ordered list, no INJ bucket
                    inj = []
                if order or inj or tier:
                    out[str(tid)] = {"order": order, "inj": inj, "tier": tier}
            return out
    except Exception as e:
        print(f"[rot] override load failed: {e}")
    return {}

def _build_rotation_ranks(run_date: str) -> dict:
    """Rank each team's CURRENT rotation via the MLB Stats API (official feed
    only, no scraping). Returns {pitcher_id: {"rank","gs","recent","rookie"}}.

    Membership = RECURRING listed starters from the probable-pitcher schedule:
    a pitcher is in the rotation only if he made >=2 starts in the trailing window
    OR has an upcoming probable start. A single trailing start with nothing ahead
    is a one-off (bullpen-day opener, spot starter, an arm that just hit the IL, or
    a demotion to AAA) and is dropped. Pure relievers never appear at all (they are
    never listed as a probable starter). A reliever recently promoted into the
    rotation qualifies on his recurring recent starts even though his season line
    still reads like a reliever. Within the rotation the order is a POWER RANKING by
    season ERA asc (lowest ERA = toughest arm = SP1), NOT games-started, so the
    staff's best arm reads as the ace; games-started / recent starts break ties.
    An admin override still wins over this auto order.

    Rookie = 10 or fewer career games pitched (0-10 rookie, 11+ established).
    Cached per run_date for the life of the process."""
    if run_date in _ROT_RANK_CACHE:
        return _ROT_RANK_CACHE[run_date]
    from datetime import datetime as _DT, timedelta as _TD
    season = run_date[:4]
    rank_map: dict = {}
    editor: dict = {}
    try:
        _d0 = _DT.strptime(run_date, "%Y-%m-%d")
    except Exception:
        _ROT_RANK_CACHE[run_date] = rank_map
        _ROT_EDITOR_CACHE[run_date] = editor
        return rank_map
    win_start = (_d0 - _TD(days=21)).strftime("%Y-%m-%d")
    win_end   = (_d0 + _TD(days=10)).strftime("%Y-%m-%d")

    team_ids = set()
    team_names: dict = {}
    try:
        sched = requests.get(
            f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={run_date}",
            timeout=15).json()
        for d in sched.get("dates", []):
            for g in d.get("games", []):
                teams = g.get("teams", {}) or {}
                for _sh in ("home", "away"):
                    tm = ((teams.get(_sh) or {}).get("team") or {})
                    tid = tm.get("id")
                    if tid:
                        team_ids.add(tid)
                        if tm.get("name"):
                            team_names[tid] = tm.get("name")
    except Exception:
        team_ids = set()
    if not team_ids:
        _ROT_RANK_CACHE[run_date] = rank_map
        _ROT_EDITOR_CACHE[run_date] = editor
        return rank_map
    tid_list = list(team_ids)

    def _team_rotation(tid):
        # Membership = the probable-pitcher schedule (the listed starter of record
        # per game), split into trailing starts vs upcoming starts. Pure relievers
        # never appear here (they are never a probable starter); the past/upcoming
        # split then separates RECURRING starters from one-off bullpen openers, IL
        # guys, and demotions.
        past: dict = {}   # pid -> starts made in the trailing window
        upc:  dict = {}   # pid -> upcoming probable starts (game date >= run_date)
        names: dict = {}
        try:
            r = requests.get(
                "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
                f"&teamId={tid}&startDate={win_start}&endDate={win_end}"
                "&hydrate=probablePitcher", timeout=15).json()
            for d in r.get("dates", []):
                gdate = d.get("date", "")
                for g in d.get("games", []):
                    teams = g.get("teams", {}) or {}
                    for _sh in ("home", "away"):
                        t = teams.get(_sh) or {}
                        if ((t.get("team") or {}).get("id")) != tid:
                            continue
                        pp = (t.get("probablePitcher") or {})
                        pid = pp.get("id")
                        if pid:
                            pid = int(pid)
                            if gdate >= run_date:
                                upc[pid] = upc.get(pid, 0) + 1
                            else:
                                past[pid] = past.get(pid, 0) + 1
                            if pp.get("fullName"):
                                names[pid] = pp.get("fullName")
        except Exception:
            past, upc = {}, {}
        recent = {p: past.get(p, 0) + upc.get(p, 0)
                  for p in set(past) | set(upc)}
        # Season ERA powers the ranking (best arm = SP1); GS only breaks ties.
        seas: dict = {}   # pid -> season games started
        era:  dict = {}   # pid -> season ERA (float; 999.0 when none / 0 IP)
        try:
            r = requests.get(
                "https://statsapi.mlb.com/api/v1/stats?stats=season&group=pitching"
                f"&season={season}&gameType=R&teamId={tid}&playerPool=all&limit=200",
                timeout=15).json()
            for blk in r.get("stats", []):
                for s in blk.get("splits", []):
                    pl = (s.get("player") or {})
                    pid = pl.get("id")
                    stt = (s.get("stat") or {})
                    if pid:
                        pid = int(pid)
                        seas[pid] = int(stt.get("gamesStarted", 0) or 0)
                        try:
                            ip = float(stt.get("inningsPitched", 0) or 0)
                            era[pid] = float(stt.get("era")) if ip > 0 else 999.0
                        except (TypeError, ValueError):
                            era[pid] = 999.0
                        if pl.get("fullName") and pid not in names:
                            names[pid] = pl.get("fullName")
        except Exception:
            seas = {}
        # CURRENT ROTATION = recurring listed starters: >=2 trailing starts OR an
        # upcoming probable start. A single trailing start with nothing upcoming is
        # a one-off (bullpen opener, spot starter, an arm that just hit the IL, or
        # a demotion) and is dropped. Newly promoted arms (a reliever moved into
        # the rotation) qualify on their recurring recent starts even though their
        # season line still reads like a reliever. If nothing recurs, fall back to
        # anyone listed as a probable starter in the window.
        cand = [p for p in recent if past.get(p, 0) >= 2 or upc.get(p, 0) >= 1]
        if not cand:
            cand = list(recent.keys())
        # POWER RANKING: season ERA asc (lowest ERA = toughest arm = SP1);
        # games-started / recent-start count break ties.
        def _key(p):
            return (era.get(p, 999.0), -seas.get(p, 0), -recent.get(p, 0))
        cand.sort(key=_key)
        return [(p, names.get(p, ""), seas.get(p, 0), recent.get(p, 0))
                for p in cand]

    try:
        with _TPEx(max_workers=8) as _ex:
            results = list(_ex.map(_team_rotation, tid_list))
    except Exception:
        results = [_team_rotation(t) for t in tid_list]

    override = _load_rot_override()

    pids = []
    for tid, rotation in zip(tid_list, results):
        auto = [(pid, nm) for (pid, nm, sgs, rc) in rotation]
        meta = {pid: (sgs, rc) for (pid, nm, sgs, rc) in rotation}
        name_by_pid = {pid: nm for (pid, nm) in auto if nm}
        ovr = override.get(str(tid))
        tier_ovr = (ovr.get("tier") or {}) if ovr else {}
        inj_order = []   # [(pid,name)] parked in the INJ bucket (gets no rank)
        if ovr:
            # Admin order wins. Pinned starters take ranks 1..k in the saved
            # order; arms moved to the INJ bucket (injured / optioned) are
            # dropped from the ranking entirely so the real starters re-rank
            # 1..k. Any auto-detected starter not pinned or INJ is appended.
            inj_order = ovr.get("inj") or []
            inj_pids = set(pid for pid, nm in inj_order)
            for pid, nm in (ovr.get("order") or []) + inj_order:
                if nm:
                    name_by_pid[pid] = nm
            seen = set(); final = []
            for pid, nm in (ovr.get("order") or []):
                if pid in seen or pid in inj_pids:
                    continue
                seen.add(pid); final.append(pid)
            for pid, nm in auto:
                if pid not in seen and pid not in inj_pids:
                    seen.add(pid); final.append(pid)
            has_ovr = True
        else:
            inj_pids = set()
            final = [pid for pid, nm in auto]
            has_ovr = False
        ed_pitchers = []
        for i, pid in enumerate(final, 1):
            sgs, rc = meta.get(pid, (0, 0))
            rank_map[pid] = {"rank": i, "gs": sgs, "recent": rc,
                             "rookie": False, "tier": tier_ovr.get(pid, 0)}
            pids.append(pid)
            ed_pitchers.append({"id": pid,
                                "name": name_by_pid.get(pid, str(pid)),
                                "rank": i, "override": has_ovr,
                                "tier": tier_ovr.get(pid, 0)})
        ed_injured = [{"id": pid, "name": name_by_pid.get(pid, str(pid))}
                      for pid, nm in inj_order]
        editor[str(tid)] = {"name": team_names.get(tid, str(tid)),
                            "pitchers": ed_pitchers, "injured": ed_injured,
                            "has_override": has_ovr}

    # Rookie flag — 10 or fewer career games pitched (batched career hydrate).
    try:
        for i in range(0, len(pids), 40):
            chunk = pids[i:i + 40]
            r = requests.get(
                "https://statsapi.mlb.com/api/v1/people?personIds=" +
                ",".join(str(x) for x in chunk) +
                "&hydrate=stats(group=[pitching],type=[career])",
                timeout=15).json()
            for person in r.get("people", []):
                pid = person.get("id")
                cg = None
                for blk in person.get("stats", []):
                    for s in blk.get("splits", []):
                        v = (s.get("stat") or {}).get("gamesPitched")
                        if v is not None:
                            cg = v
                if pid in rank_map and cg is not None and int(cg) <= 10:
                    rank_map[pid]["rookie"] = True
    except Exception:
        pass

    _ROT_RANK_CACHE[run_date] = rank_map
    _ROT_EDITOR_CACHE[run_date] = editor
    return rank_map


def rotation_editor_data(run_date: str) -> dict:
    """Per-team rotation (names + ranks + override flag) for the admin Rotation
    Order panel. Builds (and caches) the ranks first if not already done."""
    if run_date not in _ROT_EDITOR_CACHE:
        _build_rotation_ranks(run_date)
    data = _ROT_EDITOR_CACHE.get(run_date, {}) or {}
    teams = []
    for tid, info in data.items():
        teams.append({"team_id": tid,
                      "team_name": info.get("name", tid),
                      "has_override": info.get("has_override", False),
                      "pitchers": info.get("pitchers", []),
                      "injured": info.get("injured", [])})
    teams.sort(key=lambda t: (t["team_name"] or ""))
    return {"date": run_date, "teams": teams}


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
        from under_picks import _team_match as _tm
        def _match(opp_str):
            if not (today_opp or "").strip(): return False
            return _tm(opp_str, today_opp)
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


_SERIES_GAMES_CACHE: dict = {}

def _fetch_series_games(run_date: str) -> dict:
    """Per-team series game number (G1/G2/G3…) for today's slate — authoritative
    seriesGameNumber from the MLB Stats API schedule (one cached call). Some teams
    open a series today (G1) while others are mid-series (G2/G3).
    Returns {team_name: {"g": game_no, "of": games_in_series}}."""
    if run_date in _SERIES_GAMES_CACHE:
        return _SERIES_GAMES_CACHE[run_date]
    out: dict = {}
    try:
        j = requests.get("https://statsapi.mlb.com/api/v1/schedule",
                         params={"sportId": 1, "date": run_date}, timeout=15).json()
        for _d in j.get("dates", []):
            for _g in _d.get("games", []):
                _gno = _g.get("seriesGameNumber")
                if not _gno:
                    continue
                _gof = _g.get("gamesInSeries") or 0
                for _sh in ("home", "away"):
                    _nm = ((((_g.get("teams") or {}).get(_sh) or {}).get("team")) or {}).get("name")
                    if _nm:
                        out[_nm] = {"g": int(_gno), "of": int(_gof)}
    except Exception:
        pass
    if out:
        _SERIES_GAMES_CACHE[run_date] = out
    return out


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
    # Career line uses the TRUE head-to-head vs today's pitcher (same source the
    # popup's "Career vs" block shows), NOT the pool-entry BA — a hot-streak or
    # last-7-day average must never be mislabeled "Career vs pitcher". Omit the
    # line entirely when there's no real head-to-head sample (0 AB) or the
    # starter is still TBD.
    if pitcher and pitcher != "TBD":
        _vline = ""
        try:
            from under_picks import _get_s1_vs_pitcher, _get_s1_vs_pitcher_ha
            _hd = (_get_s1_vs_pitcher_ha(r.get("player_id"), r.get("pit_id")) or {}).get(side_str) or {}
            if _hd.get("ab"):
                _vline = f"{side_str.capitalize()} .{int(_hd['ba'] * 1000):03d} vs {pitcher} ({_hd['ab']} AB)"
            else:
                _vsp = _get_s1_vs_pitcher(r.get("player_id"), r.get("pit_id"))
                if _vsp and (_vsp.get("ab") or 0) > 0 and _vsp.get("ba") is not None:
                    _vline = f"Career .{int(_vsp['ba'] * 1000):03d} vs {pitcher} ({_vsp['ab']} AB)"
        except Exception:
            _vline = ""
        if _vline:
            parts.append(_vline)
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


# v1 probability calibration -- deflate the families the EV audit flagged as
# overconfident so the displayed win% / EV badge / ledger match reality.
# 14-day audit (Jun 2026): score-derived (RBI/Runs/HRR/Walks/TB) + Poisson
# pitcher markets ran HOT -- overs ~ predicted 72% / actual 46%; unders ~
# predicted 85% / actual 67%. A per-side affine map (monotonic, so it never
# reorders a list) pulls the model prob toward reality. Log5 hits, the binomial
# under-1.5-hits, and HR were already well-calibrated and are NOT passed through.
_PROB_CAL = {"OVER": (0.70, -0.044), "UNDER": (0.70, 0.075)}


def _cal_prob(pw, side):
    """Calibrate a raw model win-prob for `side` ('OVER'/'UNDER'). Clamped."""
    if pw is None:
        return None
    _a, _b = _PROB_CAL.get((side or "OVER").upper(), _PROB_CAL["OVER"])
    return max(0.05, min(0.95, _a * float(pw) + _b))


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


def _fetch_bullpen_stats(run_date: str) -> dict:
    """
    Per-team bullpen profile = FATIGUE (last 3 days IP) + QUALITY (reliever ERA,
    recent 14-day blended 60/40 with season-long).

    Returns {team_full_name: {
        "bp_ip": float, "games": int, "taxed": bool,   # fatigue (last 3 days)
        "era_l14": float|None, "g14": int,             # recent reliever ERA
        "era_szn": float|None,                         # season reliever ERA
        "era": float|None,                             # blended (display + nudge)
        "factor": float, "lean": str                   # offense nudge + label
    }}

    Bullpen = every pitcher AFTER the starter in each box score. The recent
    window is pulled per game from /game/{pk}/boxscore — schedule
    hydrate=boxscore returns 0 players so it cannot be used. Season reliever
    ERA comes from one league-wide team statSplits (sitCodes=rp) call. Silent
    on failure (-> {}); None-safe downstream (no chip / neutral 1.0 nudge).
    """
    from datetime import datetime, timedelta
    BP_TAXED   = 9.0       # >= 9 IP across last 3 days = taxed pen
    BP_CAP     = 0.08      # max +/- 8% offense nudge
    BP_K       = 0.03      # nudge per ERA run above/below league
    BP_GATE    = 0.75      # ERA gap vs league to label weak / elite
    W_RECENT   = 0.6
    W_SEASON   = 0.4
    MIN_OUTS14 = 30        # >= 10 IP before a 14-day ERA is trusted

    today    = datetime.strptime(run_date, "%Y-%m-%d")
    d_recent = (today - timedelta(days=3)).strftime("%Y-%m-%d")   # fatigue start
    d_start  = (today - timedelta(days=14)).strftime("%Y-%m-%d")  # ERA window start
    d_end    = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    def _parse_outs(raw) -> int:
        try:
            parts = str(raw).split(".")
            full  = int(parts[0]) if parts[0] else 0
            outs  = int(parts[1]) if len(parts) > 1 and parts[1] else 0
            return full * 3 + outs
        except Exception:
            return 0

    # ── recent window: gamePk + date for every Final regular-season game ──
    try:
        sched = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "startDate": d_start, "endDate": d_end,
                    "gameType": "R"}, timeout=20).json()
    except Exception:
        sched = {}
    game_days = []   # (gamePk, "YYYY-MM-DD")
    for de in sched.get("dates", []):
        gd = de.get("date", "")
        for g in de.get("games", []):
            if g.get("status", {}).get("abstractGameState", "") == "Final":
                game_days.append((g.get("gamePk"), gd))

    def _box(pk):
        try:
            return requests.get(
                f"https://statsapi.mlb.com/api/v1/game/{pk}/boxscore",
                timeout=15).json()
        except Exception:
            return None

    boxes = []
    if game_days:
        try:
            with _TPEx(max_workers=12) as ex:
                boxes = list(ex.map(lambda gp: (_box(gp[0]), gp[1]), game_days))
        except Exception:
            boxes = []

    teams: dict = {}   # name -> accumulators
    for box, gd in boxes:
        if not box:
            continue
        for side in ("home", "away"):
            td   = box.get("teams", {}).get(side, {})
            name = td.get("team", {}).get("name", "")
            if not name:
                continue
            pitchers = td.get("pitchers", [])
            players  = td.get("players", {})
            er = 0; outs = 0; ip3 = 0.0
            for idx, pid in enumerate(pitchers):
                if idx == 0:          # starter — skip
                    continue
                ps = (players.get(f"ID{pid}", {})
                             .get("stats", {}).get("pitching", {}))
                o   = _parse_outs(ps.get("inningsPitched", "0"))
                er  += int(ps.get("earnedRuns", 0) or 0)
                outs += o
                if gd >= d_recent:
                    ip3 += o / 3.0
            acc = teams.setdefault(name, {"er": 0, "outs": 0, "g14": 0,
                                          "bp_ip": 0.0, "games": 0})
            acc["er"]   += er
            acc["outs"] += outs
            acc["g14"]  += 1
            if gd >= d_recent:
                acc["bp_ip"] += ip3
                acc["games"] += 1

    # ── season reliever ERA (one league-wide statSplits call) ──
    season = run_date[:4]
    szn_era: dict = {}
    szn_tot_er = 0; szn_tot_outs = 0
    try:
        sjs = requests.get(
            "https://statsapi.mlb.com/api/v1/teams/stats",
            params={"season": season, "sportIds": 1, "stats": "statSplits",
                    "group": "pitching", "sitCodes": "rp", "gameType": "R"},
            timeout=20).json()
        for s in sjs.get("stats", [{}])[0].get("splits", []):
            nm = s.get("team", {}).get("name", "")
            st = s.get("stat", {})
            try:
                e = float(st.get("era"))
            except Exception:
                e = None
            if nm and e is not None:
                szn_era[nm] = e
            szn_tot_outs += _parse_outs(st.get("inningsPitched", "0"))
            szn_tot_er   += int(st.get("earnedRuns", 0) or 0)
    except Exception:
        pass

    # ── league baselines (same 60/40 blend as the per-team ERA) ──
    lg_er   = sum(d["er"] for d in teams.values())
    lg_outs = sum(d["outs"] for d in teams.values())
    LG_l14  = (lg_er * 27.0 / lg_outs) if lg_outs else None
    LG_szn  = (szn_tot_er * 27.0 / szn_tot_outs) if szn_tot_outs else None
    if LG_l14 is not None and LG_szn is not None:
        LG = W_RECENT * LG_l14 + W_SEASON * LG_szn
    elif LG_l14 is not None:
        LG = LG_l14
    elif LG_szn is not None:
        LG = LG_szn
    else:
        LG = 4.10

    out: dict = {}
    for nm in (set(teams) | set(szn_era)):
        d = teams.get(nm, {"er": 0, "outs": 0, "g14": 0, "bp_ip": 0.0, "games": 0})
        era_l14 = (d["er"] * 27.0 / d["outs"]) if d["outs"] >= MIN_OUTS14 else None
        era_szn = szn_era.get(nm)
        if era_l14 is not None and era_szn is not None:
            era = W_RECENT * era_l14 + W_SEASON * era_szn
        elif era_l14 is not None:
            era = era_l14
        else:
            era = era_szn
        if era is None:
            factor, lean = 1.0, "avg"
        else:
            delta = era - LG
            if delta >= 0:                      # weak pen -> boost overs
                factor = 1.0 + min(BP_CAP, delta * BP_K)
                lean   = "weak" if delta >= BP_GATE else "avg"
            else:                               # elite pen -> fade overs
                factor = 1.0 - min(BP_CAP, (-delta) * BP_K)
                lean   = "elite" if (-delta) >= BP_GATE else "avg"
        out[nm] = {
            "bp_ip":   round(d["bp_ip"], 1),
            "games":   d["games"],
            "taxed":   d["bp_ip"] >= BP_TAXED,
            "era_l14": round(era_l14, 2) if era_l14 is not None else None,
            "g14":     d["g14"],
            "era_szn": round(era_szn, 2) if era_szn is not None else None,
            "era":     round(era, 2) if era is not None else None,
            "factor":  round(factor, 4),
            "lean":    lean,
        }
    return out


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
    # HIT LIST ONLY: keep batters who clear .250 career BA vs today's pitcher
    # (min 4 AB) — i.e. Source-1 career-qualified. Streak / last-7 hot batters
    # that don't clear that bar are dropped from the hit candidates (they still
    # influenced ranking via the merged pool BA). No other category uses top30.
    top30 = [p for p in step1 if p.get("career_qualified")]
    pitcher_map = {p["batter"]: p["pitcher"] for p in top30}
    emit({"type": "step1_done",
          "msg": f"✅ {len(top30)} hitters clear .225 vs pitcher (min 4 AB) — "
                 f"from pool of {len(step1)}",
          "count": len(top30)})

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
        # Penalty for no direct career matchup vs today's pitcher.
        # career_vsp=True = real career AB (Source 1, min 3 AB / .225 BA).
        # Everyone else (hand-split S4, streak S2, hot-hitter S3) gets docked
        # 300 pts — equivalent to ~.300 BA worth of signal — so players who
        # have never faced this pitcher don't outrank those who have.
        NO_VSP_PENALTY = 300
        _vsp_penalty = 0 if p.get("career_vsp") else NO_VSP_PENALTY
        total = max(0, round(p["ba"] * 1000) + s2s + s3s - _vsp_penalty) if not dq else 0

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
            "career_vsp": bool(p.get("career_vsp")),
            "vsp_penalty": _vsp_penalty,
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
                from under_picks import _team_match as _tm
                opp = r.get("opp", "")
                for t, pinfo in mlb_probable.items():
                    if _tm(t, opp):
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
        from under_picks import _team_match as _tm
        def _match_opp(opp):
            for tn, pid in pit_id_map.items():
                if _tm(tn, opp): return pid
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

    # ── S4 (L10 H/A consistency) — display + re-rank only, no DQ ─────
    emit({"type": "section", "msg": "S4 (L10 H/A consistency) + S5 (D/N BA) — re-rank only"})
    s4_qualified, s4_dq = [], []
    for r in lineup_qualified:
        info       = roster.get(r["name"], {})
        player_id  = info.get("player_id") or r.get("player_id")
        s4         = fetch_step4_consistency(player_id, r["side"], r.get("opp", ""))
        r["s4"]    = s4
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

    # ── Record-a-Hit: merge career backbone (pool A) with the OVER engine ──
    # Pool A above is the career-vs-pitcher model. Here we (1) layer the shared
    # Over signals (3-window hit-rate convergence + hot-hand) onto every career
    # pick as display + a small hot nudge — career picks are NEVER dropped — and
    # (2) add pool B: hot hitters with a posted 0.5 hit line but NO career
    # history vs today's pitcher, qualified by the same Over engine at a 60% cut.
    # Pool B is appended to also_ran, so every downstream pass (EV, env, bullpen,
    # final reorder) covers it automatically with one record schema.
    try:
        from under_picks import run_hit_picks, hit_over_signals
        _pool_a_ids = {p.get("player_id") for p in (top9 + also_ran) if p.get("player_id")}
        for _hp in top9 + also_ran:
            _sig = hit_over_signals(_hp.get("player_id"), _hp.get("side", ""), _hp.get("opp", ""))
            _hp["rate_disp"]  = _sig["rate_disp"]
            _hp["basis"]      = _sig["basis"]
            _hp["h2h_disp"]   = _sig["h2h_disp"]
            _hp["h2h_games"]  = _sig["h2h_games"]
            _hp["l10_disp"]   = _sig["l10_disp"]
            _hp["recent_l10"] = _sig["recent_l10"]
            _hp["recent_l5"]  = _sig["recent_l5"]
            _hp["conv_flag"]  = _sig["conv_flag"]
            _hp["cold_flag"]  = _sig["cold_flag"]
            _hp["hot_disp"]   = _sig["hot_disp"]
            _hp["hot_bonus"]  = _sig["hot_bonus"]
            _hp["over_score"] = _sig["over_score"]
            _hp["total"]      = (_hp.get("total") or 0) + _sig["hot_bonus"]
        _pool_b = run_hit_picks(run_date, team_schedule, exclude_ids=_pool_a_ids, emit=emit)
        for _pb in _pool_b:
            _pb["player_id"]    = _pb.get("batter_id")
            _pb["full_name"]    = _pb.get("name")
            _pb["pos"]          = ""
            _pb["over_sourced"] = True
            _pb["dq"]           = False
            _pb["total"]        = round((_pb.get("score") or 0) * 10)   # 0-1000, ranks vs pool A
            _pb["recent_hit_log"] = _recent_hit_log(_pb.get("player_id"))
            _pb["series_splits"]  = fetch_series_splits(
                _pb.get("player_id"), _pb.get("opp", ""), run_date, _pb.get("side", ""))
        also_ran.extend(_pool_b)
        emit({"type": "log", "msg": f"  ✅ Record-a-Hit pool B added: {len(_pool_b)} hot hitters (no career vs pitcher)"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Record-a-Hit pool B skipped: {_exc}"})

    # ── Matchup-value (Log5 + EV) enrichment for hit picks ──────────────
    # Adds matchup_prob / season_ba / proj_baa / impl_prob / ev / edge to every
    # hit pick. Frontend re-ranks by ev (default keeps ALL plays) + shows a green
    # edge badge, with a "+EV only" toggle. No play is dropped server-side.
    try:
        _SEASON_YR = str(run_date)[:4]
        _LG_BA = 0.244
        _pid_map = {tn: pi.get("id") for tn, pi in mlb_probable.items() if pi.get("id")}
        from under_picks import _team_match as _tm

        def _opp_pid(opp):
            if not opp:
                return None
            for tn, pid in _pid_map.items():
                if _tm(tn, opp):
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

    # Pool B record-a-hit picks (hot hitters with no career vs pitcher, folded
    # into also_ran above) are the only hit picks that never got a first-pitch
    # stamp — career picks get it at build time, every other category gets it
    # here. Without game_start the frontend's started-game filter can never drop
    # them, so they stay on the board after their game starts/finishes while
    # every other category clears. Backfill any hit pick missing it.
    for _hp in (top9 + also_ran):
        if not _hp.get("game_start"):
            _hp["game_start"] = _game_start_for(_hp.get("team", ""))

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

    # ── HRR Special (parlay confluence board, OVER only) ──────────────────
    # Stricter, separate board for parlays. Gates 1-3 (.275 vs pitcher / 65%
    # vs team H/A / 65% L10 H/A) run inside run_hrr_special_picks; the 4th gate
    # (.275 day/night BA for today's game type) is applied AFTER the day/night
    # stamp below. The regular HRR board above is untouched.
    try:
        from under_picks import run_hrr_special_picks
        hrr_special_list = run_hrr_special_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ HRR Special picks skipped: {exc}"})
        hrr_special_list = []
    for _hsp in hrr_special_list:
        _hsp["game_start"] = _game_start_for(_hsp.get("team", ""))

    # ── HR Picks (Batter Home Runs, Over/Under 0.5) ───────────────────────
    try:
        from under_picks import run_hr_picks
        hr_picks_list = run_hr_picks(run_date, team_schedule, emit=emit)
    except Exception as exc:
        emit({"type": "log", "msg": f"⚠️ HR picks skipped: {exc}"})
        hr_picks_list = []
    for _hrp in hr_picks_list:
        _hrp["game_start"]    = _game_start_for(_hrp.get("team", ""))
        _hrp["series_splits"] = fetch_series_splits(_hrp.get("batter_id"), _hrp.get("opp", ""), run_date, _hrp.get("side", ""))

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
                     walks_picks_list, hrr_picks_list, hr_picks_list):
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

    # ── Series game number (G1/G2/G3) — authoritative from MLB schedule ──
    # seriesGameNumber is a property of the GAME, so stamp it per TEAM onto EVERY
    # pick (hitters + pitchers) — consistent across teammates, unlike the
    # per-player today_pos heuristic above. Drives the compact G# chip on cards.
    try:
        _sgames = _fetch_series_games(run_date)

        def _series_for_team(team_name):
            if not team_name or not _sgames:
                return None
            s = _sgames.get(team_name)
            if s:
                return s
            tl = team_name.lower()
            for _k, _v in _sgames.items():
                if tl in _k.lower() or _k.lower() in tl:
                    return _v
            return None

        _sg_all = (list(top9) + list(also_ran) + list(under_picks_list)
                   + list(runs_picks_list) + list(tb_picks_list)
                   + list(tb_over_picks_list) + list(rbi_picks_list)
                   + list(walks_picks_list) + list(hrr_picks_list)
                   + list(hr_picks_list))
        _sg_pit = list(pitcher_k_result.get("picks", [])) + list(pitcher_k_result.get("all", []))
        for _b in pitcher_props.values():
            _sg_pit += list(_b.get("picks", [])) + list(_b.get("all", []))
        _sg_n = 0
        for _p in _sg_all + _sg_pit:
            _si = _series_for_team(_p.get("team", ""))
            if _si:
                _p["series_game"] = _si["g"]
                _p["series_of"]   = _si["of"]
                _sg_n += 1
        emit({"type": "log", "msg": f"  ✅ Series game# stamped ({_sg_n} picks, {len(_sgames)} teams)"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Series game# skipped: {_exc}"})

    # ── Day/Night BA for ALL hitter picks (display on every card) ────────
    # Hit-pool picks already carry ESPN day/night BA (Step 4 / S5). The other
    # categories don't, so stamp each hitter's day/night BA (MLB Stats API,
    # batched by personId) for today's game time onto every pick so the card
    # can show it. Display only — ordering and gates are untouched.
    try:
        from datetime import datetime as _DT

        def _dn_gtype(team_name):
            s = team_schedule.get(team_name)
            if not s and team_name:
                tl = team_name.lower()
                for _k, _v in team_schedule.items():
                    if tl in _k.lower() or _k.lower() in tl:
                        s = _v
                        break
            gs = (s or {}).get("game_start", "") or ""
            if not gs:
                return "unknown"
            try:
                _dt = _DT.fromisoformat(gs.replace("Z", "+00:00"))
                return "night" if (_dt.hour >= 21 or _dt.hour <= 5) else "day"
            except Exception:
                return "unknown"

        _dn_lists = [under_picks_list, runs_picks_list, tb_picks_list,
                     tb_over_picks_list, rbi_picks_list, walks_picks_list,
                     hrr_picks_list, hr_picks_list, hrr_special_list]

        # reuse hit-pool day/night where the same player already has it so a
        # player shows the SAME number on every card
        _dn_existing = {}
        for _r in list(top9) + list(also_ran):
            _bid = _r.get("batter_id") or _r.get("player_id")
            if _bid and _r.get("s5"):
                _dn_existing[int(_bid)] = (_r.get("dn_label"), _r.get("s5"))

        _dn_need = set()
        for _lst in _dn_lists:
            for _r in _lst:
                _bid = _r.get("batter_id") or _r.get("player_id")
                if _bid and int(_bid) not in _dn_existing and not _r.get("s5"):
                    _dn_need.add(int(_bid))

        _dn_map = {}
        _dn_ids = list(_dn_need)
        _dn_season = str(run_date)[:4]
        for _i in range(0, len(_dn_ids), 40):
            _chunk = _dn_ids[_i:_i + 40]
            try:
                _u = ("https://statsapi.mlb.com/api/v1/people?personIds="
                      + ",".join(str(x) for x in _chunk)
                      + "&hydrate=stats(group=[hitting],type=[statSplits],"
                      + "sitCodes=[d,n],season=" + _dn_season + ")")
                _j = requests.get(_u, timeout=15).json()
                for _per in _j.get("people", []):
                    _pid = _per.get("id")
                    _sp = {}
                    for _st in _per.get("stats", []):
                        for _s in _st.get("splits", []):
                            _code = (_s.get("split") or {}).get("code")
                            _stat = _s.get("stat") or {}
                            if _code in ("d", "n"):
                                _sp[_code] = (_stat.get("avg"), _stat.get("atBats"))
                    if _pid is not None:
                        _dn_map[int(_pid)] = _sp
            except Exception:
                continue

        def _dn_fmt(_avg):
            try:
                _f = float(_avg)
            except Exception:
                return None, "N/A"
            _d = "%.3f" % _f
            if _d.startswith("0."):
                _d = _d[1:]
            return _f, _d

        _dn_n = 0
        for _lst in _dn_lists:
            for _r in _lst:
                if _r.get("s5"):
                    continue
                _bid = _r.get("batter_id") or _r.get("player_id")
                _bid = int(_bid) if _bid else None
                if _bid and _bid in _dn_existing:
                    _lbl, _s5 = _dn_existing[_bid]
                    _r["dn_label"] = _lbl
                    _r["s5"] = _s5
                    _dn_n += 1
                    continue
                _gt = _dn_gtype(_r.get("team", ""))
                if _gt == "unknown":
                    continue
                _code = "d" if _gt == "day" else "n"
                _avg, _ab = (_dn_map.get(_bid or -1, {}).get(_code) or (None, None))
                _ba, _disp = _dn_fmt(_avg)
                _r["dn_label"] = "DAY" if _gt == "day" else "NIGHT"
                _r["s5"] = {"ba": _ba,
                            "score": (round(_ba * 1000) if _ba else 0),
                            "display": _disp, "ab": _ab}
                _dn_n += 1
        emit({"type": "log",
              "msg": f"  ✅ Day/Night BA stamped ({_dn_n} hitter picks, {len(_dn_map)} fetched)"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Day/Night BA stamp skipped: {_exc}"})

    # ── HRR Special GATE 4 — day/night BA (today's game type) must clear .275 ─
    # The stamp above set s5.ba to each batter's BA in TODAY's game type. A
    # special pick must hit >= .275 there too; if we can't measure it (no split
    # / no AB in that game type), it can't qualify for the confluence board.
    try:
        _hsp_before = len(hrr_special_list)
        hrr_special_list = [p for p in hrr_special_list
                            if (p.get("s5") or {}).get("ba") is not None
                            and p["s5"]["ba"] >= 0.250]
        emit({"type": "log",
              "msg": f"  ✅ HRR Special day/night gate (>=.270): "
                     f"{len(hrr_special_list)}/{_hsp_before} cleared"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ HRR Special day/night gate skipped: {_exc}"})

    # ── EV enrichment for ALL non-hit categories ────────────────────────
    # Each pick gets ev / edge / ev_prob from our model probability vs the
    # posted price for the SIDE we picked. Binary 0.5/1.5 batter markets use the
    # empirical vs-opp rate (score); Under-1.5-hits uses a binomial off season
    # BA; pitcher count markets use a Poisson off the opponent-adjusted
    # projection. Frontend shows a badge; the "+EV only" toggle filters on
    # ev>0. No play is dropped server-side. Hit picks already enriched above.
    try:
        _SY = str(run_date)[:4]

        def _ev_ou(p, p_over, over_am, under_am, cal=False):
            """Two-sided market: P(OVER)=p_over, attach for the picked side.
            cal=True routes the win-prob through _cal_prob (overconfident cats)."""
            if p.get("pick") == "UNDER":
                _pw = (1.0 - p_over) if p_over is not None else None
                _set_ev(p, _cal_prob(_pw, "UNDER") if cal else _pw, under_am)
            else:
                _pw = _cal_prob(p_over, "OVER") if cal else p_over
                _set_ev(p, _pw, over_am)

        for _p in rbi_picks_list:
            _s = _p.get("score")
            _ev_ou(_p, (_s / 100.0) if _s is not None else None,
                   _p.get("over_odds"), _p.get("under_odds"), cal=True)
        for _p in walks_picks_list:
            _s = _p.get("score")
            _ev_ou(_p, (_s / 100.0) if _s is not None else None,
                   _p.get("over_odds"), _p.get("under_odds"), cal=True)
        for _p in runs_picks_list:
            _s = _p.get("score")
            _ev_ou(_p, (_s / 100.0) if _s is not None else None,
                   _p.get("over_odds"), _p.get("under_odds"), cal=True)
        for _p in hrr_picks_list:
            _s = _p.get("score")
            _ev_ou(_p, (_s / 100.0) if _s is not None else None,
                   _p.get("hrr_over_odds"), _p.get("hrr_under_odds"), cal=True)
        # HR EV is computed AFTER the ballpark/weather (env) stamp below so the
        # park factor folds into P(HR); see the dedicated HR EV block after Phase A1.
        for _p in tb_over_picks_list:                 # OVER only
            _s = _p.get("score")
            _set_ev(_p, _cal_prob((_s / 100.0) if _s is not None else None, "OVER"),
                    _p.get("tb_over_odds"))
        for _p in tb_picks_list:                      # TB UNDER (score = % UNDER = P(win))
            _s = _p.get("score")
            _set_ev(_p, _cal_prob((_s / 100.0) if _s is not None else None, "UNDER"),
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
                _set_ev(p, _cal_prob(_cdf, "UNDER"), p.get("under_odds"))
            else:
                _alt = p.get("sugg_line")
                if _alt is not None and p.get("pick") == "OVER":
                    _c = _pois_cdf(int(_math.floor(_alt)), mean)
                    _pw = (1.0 - _c) if _c is not None else None
                    _set_ev(p, _cal_prob(_pw, "OVER"),
                            p.get("sugg_odds") or p.get("over_odds"))
                else:
                    _c = _pois_cdf(int(_math.floor(ln)), mean)
                    _pw = (1.0 - _c) if _c is not None else None
                    _set_ev(p, _cal_prob(_pw, "OVER"),
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
    _rr_targets = list(top9) + list(also_ran) + list(under_picks_list) + list(runs_picks_list) + list(tb_picks_list) + list(tb_over_picks_list) + list(rbi_picks_list) + list(walks_picks_list) + list(hrr_picks_list) + list(hr_picks_list)
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
            from under_picks import _team_match as _tm
            for _k, _v in team_schedule.items():
                if _tm(team_name, _k):
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

    # ── HR EV — model prob (batter power x pitcher HR-allowed x platoon, carried
    # on `wilson`) folded with the ballpark/weather factor (known only now, after
    # the env stamp). HR is intentionally NOT routed through _cal_prob — it is a
    # fresh model and will be recalibrated from the ledger once data accrues.
    try:
        for _p in hr_picks_list:
            _pb = _p.get("wilson")
            if _pb is None:
                _s = _p.get("score")
                _pb = (_s / 100.0) if _s is not None else None
            if _pb is None:
                _set_ev(_p, None, None)
                continue
            _env = _p.get("env") or {}
            _pf = _env.get("factor")
            if _pf:
                _pb = _pb * max(0.80, min(1.35, 1.0 + (float(_pf) - 1.0) * 1.5))
            _pb = max(0.01, min(0.85, _pb))
            if _p.get("pick") == "UNDER":
                _set_ev(_p, 1.0 - _pb, _p.get("under_odds"))
            else:
                _set_ev(_p, _pb, _p.get("over_odds"))
        emit({"type": "log", "msg": "  ✅ HR EV computed (model prob x ballpark/weather)"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ HR EV skipped: {_exc}"})

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

    # ── Phase A3: bullpen — fatigue (last 3d) + QUALITY (reliever ERA) ──
    # Hitters get bp_opp = opponent bullpen profile; its quality `factor` nudges
    # the OVER/hitter side in Phase B (weak pen -> overs up, elite pen -> down;
    # unders inverse). Pitchers get bp_own = own bullpen (fatigue display only,
    # no re-rank). Chips rendered in main.py.
    try:
        _bp_map = _fetch_bullpen_stats(run_date)
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
                          + list(under_picks_list) + list(runs_picks_list)
                          + list(rbi_picks_list) + list(hrr_picks_list)
                          + list(tb_picks_list) + list(tb_over_picks_list)
                          + list(walks_picks_list) + list(hr_picks_list))
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
                  "msg": f"  ✅ Bullpen fatigue+quality attached ({len(_bp_map)} teams)"})
    except Exception as _bp_exc:
        emit({"type": "log", "msg": f"⚠️ Bullpen stats skipped: {_bp_exc}"})

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
        from under_picks import _team_match as _tm
        ol = opp_name.lower()
        for tn, pi in mlb_probable.items():
            if pi.get("id") and tn.lower() == ol:
                return pi["id"]
        # Nickname match ONLY (never a shared-city word) so a Mets opponent can
        # never resolve to the Yankees' starter.
        for tn, pi in mlb_probable.items():
            if pi.get("id") and _tm(tn, opp_name):
                return pi["id"]
        return None

    def _opp_pit_name(opp_name: str):
        """Probable pitcher NAME for opp team (mirrors _opp_pit_id), or None."""
        if not opp_name:
            return None
        from under_picks import _team_match as _tm
        ol = opp_name.lower()
        for tn, pi in mlb_probable.items():
            if pi.get("name") and tn.lower() == ol:
                return pi["name"]
        # Nickname match ONLY (never a shared-city word).
        for tn, pi in mlb_probable.items():
            if pi.get("name") and _tm(tn, opp_name):
                return pi["name"]
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
            pick["opp_rot_tier"]   = info.get("tier", 0)

    def _set_own_rot(pick, pid):
        info = _rot_get(pid)
        if info:
            pick["rot_rank"]   = info.get("rank")
            pick["rot_rookie"] = info.get("rookie", False)
            pick["rot_tier"]   = info.get("tier", 0)

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

    # ── Batter vs today's starter (head-to-head career line) for popups ──
    # Every hitter popup shows the batter's career record vs the arm he faces
    # (the user wanted this on ALL hitter props). _get_s1_vs_pitcher is cached and
    # already called during scoring, so reusing it here is ~free. Hits carry
    # player_id + pit_id; non-hits carry batter_id (resolve the arm via opp).
    try:
        from under_picks import _get_s1_vs_pitcher as _vsp_fn

        def _set_vs_pit(pick, bid, pid):
            if not bid or not pid:
                return
            vp = _vsp_fn(bid, pid)
            if vp:
                pick["vs_pit"] = {"display": vp.get("display", "N/A"),
                                  "ab": vp.get("ab", 0), "hr": vp.get("hr", 0)}

        _vp_n = 0
        for _hp in list(top9) + list(also_ran):
            _set_vs_pit(_hp, _hp.get("batter_id") or _hp.get("player_id"), _hp.get("pit_id"))
            if _hp.get("vs_pit"):
                _vp_n += 1
        for _np in _nonhit_all + list(hr_picks_list):
            _pid = _np.get("pit_id") or _opp_pit_id(_np.get("opp", ""))
            _set_vs_pit(_np, _np.get("batter_id"), _pid)
            # Batter Walks picks don't carry the opposing starter — stamp it so
            # the facing-pitcher + career-vs-pitcher popup blocks render.
            if (not _np.get("pitcher")) or _np.get("pitcher") == "TBD":
                _pn = _opp_pit_name(_np.get("opp", ""))
                if _pn:
                    _np["pitcher"] = _pn
            if _np.get("vs_pit"):
                _vp_n += 1
        emit({"type": "log", "msg": f"  ✅ Batter-vs-pitcher line stamped ({_vp_n} hitter picks)"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Batter-vs-pitcher line skipped: {_exc}"})

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
    def _bpf(p):                        # opponent-bullpen offense factor
        b = p.get("bp_opp")
        try:
            f = float(b.get("factor")) if b else 1.0
        except Exception:
            f = 1.0
        return f if f and f > 0 else 1.0
    def _offf(p):                       # combined offense multiplier
        return _envf(p) * _umpf(p, "rFactor") * _bpf(p)

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

    # Top-10 hit list ("Top Plays to Record a Hit") = model's best 10 by score,
    # ANY odds. No max-juice / price gate here by design (user requirement).

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

    # HR 0.5: OVERs by blended likelihood (wilson=blended prob) × offense;
    # UNDERs ranked biggest-hitter-first by under juice — least-juiced under
    # (closest to even, e.g. -150 before -500) on top = strongest power threats
    # the market fades. (Selection/juice cap lives in under_picks.run_hr_picks.)
    hr_picks_list.sort(key=lambda p: (
        0 if p.get("pick") == "OVER" else 1,
        -((p.get("wilson", 0) * _offf(p))
          + p.get("pitch_adj", 0) + p.get("lineup_adj", 0))
        if p.get("pick") == "OVER"
        else -(p.get("under_odds") if p.get("under_odds") is not None else -100000),
        -p.get("games", 0),
    ))

    # Cap each side (OVER/UNDER) to at most HR_MAX_PER_TEAM picks per team so a
    # single hitter-friendly matchup (e.g. a Coors-style blowup) can't flood the
    # HR list with one club. Ranked order is preserved; the weakest extras drop.
    HR_MAX_PER_TEAM = 3
    _hr_team_seen, _hr_capped = {}, []
    for _hp in hr_picks_list:
        _k = (_hp.get("pick"), _hp.get("team"))
        _n = _hr_team_seen.get(_k, 0)
        if _n >= HR_MAX_PER_TEAM:
            continue
        _hr_team_seen[_k] = _n + 1
        _hr_capped.append(_hp)
    hr_picks_list = _hr_capped

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
        "under_picks": under_picks_list, "runs_picks": runs_picks_list, "tb_picks": tb_picks_list, "tb_over_picks": tb_over_picks_list, "rbi_picks": rbi_picks_list, "walks_picks": walks_picks_list, "hrr_picks": hrr_picks_list, "hrr_special_picks": hrr_special_list, "hr_picks": hr_picks_list,
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
                  "hrr_special_count": len(hrr_special_list),
                  "hr_count": len(hr_picks_list),
                  "pitcher_k_count": len(pitcher_k_result.get("picks", [])),
                  "prop_counts": {m: len(b.get("picks", [])) for m, b in pitcher_props.items()},
                  "has_tbd": slate_has_tbd(run_date)},
    }
    emit({"type": "done", "result": result})
    return result
