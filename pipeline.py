
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
        # ACTIVE-ROSTER check: instantly drop arms that just hit the IL / were
        # optioned (their trailing starts otherwise keep them ranked for up to
        # 3 weeks). A pitcher with an UPCOMING probable start is kept even if
        # not yet on the active roster (being activated off the IL to start).
        # Fail open: if the roster fetch errors, skip the filter entirely.
        active_ids: set = set()
        roster_ok = False
        try:
            rr = requests.get(
                f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster"
                "?rosterType=active", timeout=15).json()
            for e in rr.get("roster", []):
                pid_a = ((e.get("person") or {}).get("id"))
                if pid_a:
                    active_ids.add(int(pid_a))
            roster_ok = len(active_ids) > 0
        except Exception:
            roster_ok = False
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
        if roster_ok:
            # Trailing-only qualifiers must be on the active roster; an arm
            # with an upcoming probable start always stays.
            cand = [p for p in cand
                    if upc.get(p, 0) >= 1 or p in active_ids]
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


def _last10_ha_ba(player_id, side: str, n: int = 10):
    """BA over the player's last n games at today's home/away site (matching
       `side`), current + prior season, newest-first. Returns (ba, ab, games)."""
    if not player_id:
        return (None, 0, 0)
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        want_home = (side or "").upper() == "HOME"
        cy = _dt.today().year
        _h = _ab = _g = 0
        for season in range(cy, cy - 2, -1):
            for sp in reversed(_get_game_logs(player_id, season)):
                if bool(sp.get("isHome")) != want_home:
                    continue
                stat = sp.get("stat", {}) or {}
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                _h  += int(stat.get("hits", 0) or 0)
                _ab += ab
                _g  += 1
                if _g >= n:
                    break
            if _g >= n:
                break
        if _ab < 1:
            return (None, 0, 0)
        return (round(_h / _ab, 3), _ab, _g)
    except Exception:
        return (None, 0, 0)


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
    # Pool B (hot hitters with NO career vs today's pitcher): the career / s3 / s4
    # lines above all come back empty, so build a recent-form write-up from the
    # shared Over-engine signals so EVERY record-a-hit card still carries a blurb.
    if not parts and r.get("over_sourced"):
        h2h = (r.get("h2h_disp") or "").strip()
        if h2h and (r.get("h2h_games") or 0) >= 1 and opp:
            parts.append(f"{h2h.replace('/', ' of ')} {side_str} games with a hit vs {opp}")
        r10 = (r.get("recent_l10") or "").strip()
        if r10:
            parts.append(f"{r10.replace('/', ' of ')} recent games with a hit")
        hot = (r.get("hot_disp") or "").strip()
        if hot:
            parts.append(hot)
        if parts:
            lead = "Hot bat"
            if pitcher and pitcher != "TBD":
                lead += f", no career vs {pitcher}"
            parts.insert(0, lead)
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

def _gp_num(v, d=None):
    try:
        f = float(v)
        return f if f == f else d
    except Exception:
        return d


# MLB team id -> rough timezone as HOURS WEST of Eastern (ET 0 / CT 1 / MT 2 / PT 3).
# Used only to gauge travel timezone shift for the Game Predictor's rest factor.
_TEAM_TZ = {
    108: 3, 109: 2, 110: 0, 111: 0, 112: 1, 113: 0, 114: 0, 115: 2, 116: 0, 117: 1,
    118: 1, 119: 3, 120: 0, 121: 0, 133: 3, 134: 0, 135: 3, 136: 3, 137: 3, 138: 1,
    139: 0, 140: 1, 141: 0, 142: 1, 143: 0, 144: 0, 145: 1, 146: 0, 147: 0, 158: 1,
}


def _fetch_team_rest(run_date: str) -> dict:
    """ONE MLB Stats API schedule call over the trailing week. For each team,
    find its most recent game BEFORE run_date and derive: days rest, whether it
    changed ballparks since (travel), and the timezone shift of that move.
    Returns {team_name: {days_rest, traveled, tz_shift}}. Free; silent on
    failure (-> {}). The predictor degrades to neutral when a team is missing."""
    import datetime
    try:
        d0 = datetime.date.fromisoformat(run_date[:10])
    except Exception:
        return {}
    start = (d0 - datetime.timedelta(days=7)).isoformat()
    end   = d0.isoformat()
    try:
        j = requests.get("https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "startDate": start, "endDate": end},
            timeout=15).json()
    except Exception:
        return {}
    games = {}   # team_name -> [(date, home_team_id, tz), ...]
    for day in (j.get("dates", []) or []):
        gdate = day.get("date", "")
        for g in (day.get("games", []) or []):
            teams = g.get("teams", {}) or {}
            home = ((teams.get("home", {}) or {}).get("team", {}) or {})
            away = ((teams.get("away", {}) or {}).get("team", {}) or {})
            hid  = home.get("id")
            tz   = _TEAM_TZ.get(hid)
            for tm in (home, away):
                nm = tm.get("name", "")
                if nm and gdate:
                    games.setdefault(nm, []).append((gdate, hid, tz))
    out = {}
    today = d0.isoformat()
    for nm, lst in games.items():
        prev    = sorted([x for x in lst if x[0] <  today])
        tonight = [x for x in lst if x[0] == today]
        if not prev or not tonight:
            continue
        pdate, phome, ptz = prev[-1]
        _, thome, ttz = tonight[-1]
        try:
            days = (datetime.date.fromisoformat(today) - datetime.date.fromisoformat(pdate)).days
        except Exception:
            days = None
        tz_shift = (abs(ttz - ptz) if (ttz is not None and ptz is not None) else 0)
        traveled = (phome is not None and thome is not None and phome != thome)
        out[nm] = {"days_rest": days, "traveled": traveled, "tz_shift": tz_shift}
    return out


def _build_game_predictions(team_schedule, hitter_pool, pitcher_pool, run_date, emit=None):
    """Game Predictor — team win model. Aggregates the SAME player-level signals
    the app already computes for props into two team run projections, then turns
    the run gap into a win probability. Six buckets:
      1 Lineup vs starter  — team hitters' Log5 matchup_prob vs the slate mean.
      2 Starter vs lineup  — the opposing starter's ERA / projected K.
      3 Recent form        — count of hot (hot_bonus>0) vs cold (cold_flag) bats.
      4 Bullpen            — opponent bullpen quality factor (+ taxed flag).
      5 Park & weather     — env run factor (same for both teams).
      6 Home-plate umpire  — umpire run factor (same for both teams).
    Every factor degrades to neutral (1.0 / even) when its signal is missing, and
    the whole thing is wrapped per-game in try/except so one bad game can't sink
    the slate. Returns a list of prediction dicts the frontend renders."""
    try:
        from under_picks import _team_match
    except Exception:
        def _team_match(a, b):
            return (a or "").strip().lower() == (b or "").strip().lower()

    # game O/U totals (reuse the pitcher_k fetch; cached for the run)
    _lookup_total = None
    try:
        from pitcher_k import _fetch_game_totals, _lookup_game_total
        _fetch_game_totals(run_date)
        _lookup_total = _lookup_game_total
    except Exception:
        _lookup_total = None

    # game moneylines — market win% baseline (de-vig h2h; cached for the run)
    _lookup_ml = None
    try:
        from pitcher_k import _fetch_game_moneylines, _lookup_game_ml
        _fetch_game_moneylines(run_date)
        _lookup_ml = _lookup_game_ml
    except Exception:
        _lookup_ml = None

    def _amer_prob(o):
        try:
            o = float(o)
        except Exception:
            return None
        return (100.0 / (o + 100.0)) if o > 0 else ((-o) / ((-o) + 100.0))

    # travel & rest — one MLB schedule call over the trailing week (free)
    rest_by_team = {}
    try:
        rest_by_team = _fetch_team_rest(run_date)
    except Exception:
        rest_by_team = {}

    LEAGUE_RPG = 4.40      # league runs per game per team
    LG_SP_ERA  = 4.15      # league starter ERA baseline
    LG_PK      = 4.80      # league mean projected K for a start
    EXP        = 1.83      # Pythagorean exponent (run diff -> win rate)
    HFA        = 1.03      # home-field run bump

    # ── starter stats per team (own starter: ERA + projected K) ──
    sp_by_team = {}
    for sp in (pitcher_pool or []):
        t = (sp.get("team") or "").strip()
        if not t:
            continue
        d = sp_by_team.setdefault(t, {"era": None, "proj_k": None, "name": "",
                                      "r_er": None, "r_outs": None})
        e = _gp_num(sp.get("era")); pk = _gp_num(sp.get("proj_k"))
        rer = _gp_num(sp.get("recent_avg_er")); rou = _gp_num(sp.get("recent_avg_outs"))
        if d["era"]    is None and e   is not None: d["era"]    = e
        if d["proj_k"] is None and pk  is not None: d["proj_k"] = pk
        if d["r_er"]   is None and rer is not None: d["r_er"]   = rer
        if d["r_outs"] is None and rou is not None: d["r_outs"] = rou
        if not d["name"] and sp.get("name"):        d["name"]   = sp.get("name")

    # ── hitters per team + slate-mean matchup prob (self-calibrating baseline) ──
    hit_by_team, all_mp = {}, []
    for h in (hitter_pool or []):
        t = (h.get("team") or "").strip()
        if not t:
            continue
        hit_by_team.setdefault(t, []).append(h)
        mp = _gp_num(h.get("matchup_prob"))
        if mp is not None:
            all_mp.append(mp)
    base_mp = (sum(all_mp) / len(all_mp)) if all_mp else 0.0

    def _lookup(team, table):
        if team in table:
            return table[team]
        for k, v in table.items():
            if _team_match(team, k):
                return v
        return None

    def _offense(team):
        hs = _lookup(team, hit_by_team) or []
        mps = [m for m in (_gp_num(h.get("matchup_prob")) for h in hs) if m is not None]
        hot  = sum(1 for h in hs if (_gp_num(h.get("hot_bonus"), 0) or 0) > 0 or h.get("hot"))
        cold = sum(1 for h in hs if h.get("cold_flag"))
        if mps and base_mp > 0:
            avg  = sum(mps) / len(mps)
            tilt = 1.0 + 0.70 * (avg - base_mp)
        else:
            avg, tilt = None, 1.0
        tilt *= (1.0 + max(-0.06, min(0.06, (hot - cold) * 0.015)))
        # lineup availability: dock offense when projected regulars are OUT
        out_n = len({(h.get("player_id"), h.get("name")) for h in hs
                     if h.get("lineup_status") == "NOT_IN_LINEUP"})
        lin_pen = min(0.08, out_n * 0.02)
        tilt *= (1.0 - lin_pen)
        # platoon: team hitters' BA vs the starter hand they face tonight
        p_bas, p_adv, seen_pl = [], 0, set()
        for h in hs:
            pid = h.get("player_id")
            if pid in seen_pl:
                continue
            pl = h.get("platoon") or {}
            ba = _gp_num(pl.get("ba"))
            if ba is not None and (pl.get("ab") or 0) >= 10:
                p_bas.append(ba)
                if pl.get("adv"):
                    p_adv += 1
                seen_pl.add(pid)
        if p_bas:
            plat_ba = sum(p_bas) / len(p_bas)
            plat_tilt = max(-0.05, min(0.05, (plat_ba - 0.245) * 0.60))
        else:
            plat_ba, plat_tilt = None, 0.0
        tilt *= (1.0 + plat_tilt)
        return {"mult": max(0.85, min(1.18, tilt)), "avg": avg,
                "hot": hot, "cold": cold, "n": len(hs),
                "out_n": out_n, "plat_ba": plat_ba,
                "plat_n": len(p_bas), "plat_adv": p_adv}

    def _starter(team):
        s = _lookup(team, sp_by_team) or {}
        era = s.get("era"); pk = s.get("proj_k")
        # recent form: turn recent ER + outs into a recent ERA, blend with season
        r_er = s.get("r_er"); r_outs = s.get("r_outs")
        recent_era = None
        if r_er is not None and r_outs is not None and r_outs >= 6:
            recent_era = round(r_er * 9.0 / (r_outs / 3.0), 2)
        eff_era = era
        if era is not None and recent_era is not None:
            eff_era = 0.65 * era + 0.35 * recent_era
        elif era is None and recent_era is not None:
            eff_era = recent_era
        m = (eff_era / LG_SP_ERA) if eff_era is not None else 1.0
        if pk is not None:
            m *= (1.0 + (LG_PK - pk) * 0.010)   # more Ks -> lower run environment
        return {"mult": max(0.80, min(1.22, m)), "era": era, "recent_era": recent_era,
                "eff_era": (round(eff_era, 2) if eff_era is not None else None),
                "proj_k": pk, "name": s.get("name", "")}

    def _pen(team):
        """Opponent bullpen the given team's hitters face (bp_opp). mult
        multiplies the FACING team's runs — a gassed pen lifts it."""
        for h in (_lookup(team, hit_by_team) or []):
            b = h.get("bp_opp")
            if b:
                f = _gp_num(b.get("factor"), 1.0)
                mult = f if (f and f > 0) else 1.0
                ip = _gp_num(b.get("bp_ip"))
                taxed = bool(b.get("taxed"))
                # fatigue: taxed pen (>=9 IP last 3d) or heavy usage bumps runs
                fatigue = 0.0
                if taxed:
                    fatigue = 0.05
                elif ip is not None and ip >= 6:
                    fatigue = min(0.04, (ip - 6) * 0.008)
                mult *= (1.0 + fatigue)
                return {"mult": mult, "era": _gp_num(b.get("era")), "ip": ip,
                        "taxed": taxed, "fatigue": fatigue}
        return {"mult": 1.0, "era": None, "ip": None, "taxed": False, "fatigue": 0.0}

    def _rest_info(team):
        return _lookup(team, rest_by_team) or {}

    def _rest_mult(ri):
        """Small offense multiplier for travel/rest. An off-day helps; a
        doubleheader nightcap and a ballpark change (esp. crossing time zones)
        hurt. Capped tight (0.96-1.02) — the edge here is real but small."""
        m = 1.0
        dr = ri.get("days_rest")
        if dr is not None:
            if dr >= 2:   m += 0.015
            elif dr == 0: m -= 0.020
        if ri.get("traveled"):
            tz = ri.get("tz_shift", 0) or 0
            m -= (0.010 + (0.015 if tz >= 2 else (0.005 if tz == 1 else 0.0)))
        return max(0.96, min(1.02, m))

    def _rest_s(ri):
        dr = ri.get("days_rest")
        parts = []
        if dr is None:
            parts.append("n/a")
        else:
            parts.append("off-day" if dr >= 2 else ("DH legs" if dr == 0 else "1d"))
        if ri.get("traveled"):
            tz = ri.get("tz_shift", 0) or 0
            parts.append("cross-country" if tz >= 2 else (("+%d tz" % tz) if tz == 1 else "travel"))
        return " · ".join(parts) if parts else "n/a"

    def _game_const(picks):
        env = ump = 1.0
        for h in picks:
            e = h.get("env")
            if e and _gp_num(e.get("factor")):
                env = _gp_num(e.get("factor"), 1.0); break
        for h in picks:
            u = h.get("ump")
            if u and _gp_num(u.get("rFactor")):
                ump = _gp_num(u.get("rFactor"), 1.0); break
        return env, ump

    def _tier(favw):
        return "STRONG" if favw >= 60 else ("MODERATE" if favw >= 55 else "LEAN")

    out = []
    for home, sched in (team_schedule or {}).items():
        if (sched or {}).get("side") != "HOME":
            continue
        away = sched.get("opponent", "")
        if not away:
            continue
        try:
            h_abbr = sched.get("abbr", "") or home.split()[-1][:3].upper()
            a_abbr = sched.get("opp_abbr", "") or away.split()[-1][:3].upper()
            home_hs = _lookup(home, hit_by_team) or []
            away_hs = _lookup(away, hit_by_team) or []
            env, ump = _game_const(list(home_hs) + list(away_hs))

            h_off, a_off = _offense(home), _offense(away)
            h_sp,  a_sp  = _starter(home), _starter(away)   # each team's OWN starter
            away_pen = _pen(home)   # away team's pen (home hitters face it)
            home_pen = _pen(away)   # home team's pen (away hitters face it)
            h_rest, a_rest = _rest_info(home), _rest_info(away)
            h_rm,   a_rm   = _rest_mult(h_rest), _rest_mult(a_rest)

            projH = LEAGUE_RPG * h_off["mult"] * a_sp["mult"] * away_pen["mult"] * env * ump * HFA * h_rm
            projA = LEAGUE_RPG * a_off["mult"] * h_sp["mult"] * home_pen["mult"] * env * ump * a_rm
            projH = max(1.5, min(9.0, projH)); projA = max(1.5, min(9.0, projA))

            ph, pa = projH ** EXP, projA ** EXP
            winH = ph / (ph + pa) if (ph + pa) else 0.5
            winH_pct, winA_pct = round(winH * 100), round((1 - winH) * 100)
            fav_home = winH >= 0.5
            favw = max(winH_pct, winA_pct)
            conf = _tier(favw)
            pick_name  = home if fav_home else away
            pick_abbr  = h_abbr if fav_home else a_abbr
            edge_runs  = round(abs(projH - projA), 1)

            # ── run total O/U ──
            proj_total = round(projH + projA, 1)
            total_line = None
            try:
                if _lookup_total:
                    _tl = _lookup_total(home, away)
                    total_line = round(float(_tl), 1) if _tl is not None else None
            except Exception:
                total_line = None
            total_pick = total_conf = ""
            total_edge = None
            if total_line is not None:
                total_edge = round(proj_total - total_line, 1)
                total_pick = "OVER" if proj_total >= total_line else "UNDER"
                _ae = abs(total_edge)
                total_conf = "STRONG" if _ae >= 1.5 else ("MODERATE" if _ae >= 0.75 else "LEAN")

            # ── market moneyline (de-vig h2h) vs model win% ──
            mkt_home_pct = mkt_away_pct = None
            mkt_edge = None          # model% - market% for the PICK side (>0 = value)
            mkt_pick_abbr = ""       # market's favourite
            value_flag = False
            try:
                if _lookup_ml:
                    _ml = _lookup_ml(home, away)
                    if _ml:
                        _hpr = _amer_prob(_ml[0]); _apr = _amer_prob(_ml[1])
                        if _hpr and _apr and (_hpr + _apr) > 0:
                            _mh = _hpr / (_hpr + _apr)
                            mkt_home_pct = round(_mh * 100)
                            mkt_away_pct = round((1 - _mh) * 100)
                            mkt_pick_abbr = h_abbr if _mh >= 0.5 else a_abbr
                            _model_pk = winH_pct if fav_home else winA_pct
                            _mkt_pk   = mkt_home_pct if fav_home else mkt_away_pct
                            mkt_edge  = _model_pk - _mkt_pk
                            value_flag = mkt_edge >= 4
            except Exception:
                pass

            LG = LG_SP_ERA
            def _eabbr(mag):
                return "even" if abs(mag) < 1e-9 else (h_abbr if mag > 0 else a_abbr)
            hp = (h_off["hot"] - h_off["cold"]); ap = (a_off["hot"] - a_off["cold"])
            mag_line = ((h_off["avg"] or 0) - (a_off["avg"] or 0))
            mag_sp   = ((a_sp["era"] if a_sp["era"] is not None else LG)
                        - (h_sp["era"] if h_sp["era"] is not None else LG))
            mag_form = (hp - ap)
            mag_pen  = ((away_pen["era"] if away_pen["era"] is not None else LG)
                        - (home_pen["era"] if home_pen["era"] is not None else LG))
            # new factors
            r_h = h_sp["recent_era"]; r_a = a_sp["recent_era"]
            mag_recent = ((r_a if r_a is not None else LG) - (r_h if r_h is not None else LG))
            mag_fat = (away_pen["fatigue"] - home_pen["fatigue"])   # >0 home benefits
            mag_lin = (a_off["out_n"] - h_off["out_n"])             # >0 home healthier
            mag_plat = ((h_off["plat_ba"] or 0) - (a_off["plat_ba"] or 0))
            mag_rest = (h_rm - a_rm)   # >0 home fresher

            def _pct(v):  return (str(round(v * 100)) + "% hit prob") if v is not None else "—"
            def _era(v):  return ("%.2f ERA" % v) if v is not None else "n/a"
            def _pen_s(p):
                s = _era(p["era"])
                if p["taxed"]: s += " · taxed"
                return s
            def _rform(sp):
                if sp["recent_era"] is None:
                    return "n/a"
                s = "%.2f ERA L5" % sp["recent_era"]
                if sp["era"] is not None:
                    d = sp["recent_era"] - sp["era"]
                    s += (" hot" if d <= -0.5 else (" cold" if d >= 0.5 else ""))
                return s
            def _fat(p):
                if p["taxed"]:
                    return "taxed %.0f IP/3d" % (p["ip"] or 0)
                if p["ip"] is not None:
                    return "%.0f IP/3d" % p["ip"]
                return "rested"
            def _lin(o):
                return "full" if o["out_n"] == 0 else ("%d regular%s out" % (o["out_n"], "" if o["out_n"] == 1 else "s"))
            def _plat(o):
                if o["plat_ba"] is None:
                    return "n/a"
                return ".%03d vs hand (%d)" % (int(o["plat_ba"] * 1000), o["plat_n"])
            def _mlpct(v):
                return (str(v) + "% ML") if v is not None else "n/a"

            factors = [
                {"name": "Lineup vs starter", "home": _pct(h_off["avg"]),
                 "away": _pct(a_off["avg"]), "edge": _eabbr(mag_line) if abs(mag_line) >= 0.005 else "even"},
                {"name": "Starter vs lineup",
                 "home": (_era(h_sp["era"]) + (" · %d K" % round(h_sp["proj_k"]) if h_sp["proj_k"] is not None else "")),
                 "away": (_era(a_sp["era"]) + (" · %d K" % round(a_sp["proj_k"]) if a_sp["proj_k"] is not None else "")),
                 "edge": _eabbr(mag_sp) if abs(mag_sp) >= 0.15 else "even"},
                {"name": "Starter recent form", "home": _rform(h_sp), "away": _rform(a_sp),
                 "edge": (_eabbr(mag_recent) if (r_h is not None and r_a is not None and abs(mag_recent) >= 0.40) else "even")},
                {"name": "Recent form (L10)", "home": ("%d up / %d down" % (h_off["hot"], h_off["cold"])),
                 "away": ("%d up / %d down" % (a_off["hot"], a_off["cold"])),
                 "edge": _eabbr(mag_form) if abs(mag_form) >= 1 else "even"},
                {"name": "Bullpen quality", "home": _pen_s(home_pen), "away": _pen_s(away_pen),
                 "edge": _eabbr(mag_pen) if abs(mag_pen) >= 0.15 else "even"},
                {"name": "Bullpen fatigue", "home": _fat(home_pen), "away": _fat(away_pen),
                 "edge": _eabbr(mag_fat) if abs(mag_fat) >= 0.01 else "even"},
                {"name": "Lineup availability", "home": _lin(h_off), "away": _lin(a_off),
                 "edge": _eabbr(mag_lin) if abs(mag_lin) >= 1 else "even"},
                {"name": "Platoon vs starter", "home": _plat(h_off), "away": _plat(a_off),
                 "edge": _eabbr(mag_plat) if abs(mag_plat) >= 0.010 else "even"},
                {"name": "Travel & rest", "home": _rest_s(h_rest), "away": _rest_s(a_rest),
                 "edge": _eabbr(mag_rest) if abs(mag_rest) >= 0.005 else "even"},
                {"name": "Park & weather",
                 "home": ("run factor %.2f" % env), "away": ("run factor %.2f" % env), "edge": "even"},
                {"name": "Home-plate umpire",
                 "home": ("run factor %.2f" % ump), "away": ("run factor %.2f" % ump), "edge": "even"},
                {"name": "Market (de-vig)", "home": _mlpct(mkt_home_pct), "away": _mlpct(mkt_away_pct),
                 "edge": (mkt_pick_abbr if mkt_home_pct is not None else "even")},
            ]

            # ── top-3 drivers (favouring the pick) ──
            loser_abbr = a_abbr if fav_home else h_abbr
            sign = 1 if fav_home else -1
            cands = []
            if value_flag and mkt_edge is not None:
                cands.append((100 + abs(mkt_edge), "value: model +%d%% vs market on %s" % (mkt_edge, pick_abbr)))
            if abs(mag_line) >= 0.005:
                strong = h_off if fav_home else a_off
                cands.append((abs(mag_line) * 50 * (1 if sign * mag_line > 0 else 0.01),
                              pick_abbr + " bats " + _pct(strong["avg"]) + " vs starter"))
            if abs(mag_sp) >= 0.15:
                sp = h_sp if fav_home else a_sp
                cands.append((abs(mag_sp) * 3 * (1 if sign * mag_sp > 0 else 0.01),
                              pick_abbr + " arm " + _era(sp["era"]) + " suppresses " + loser_abbr))
            if r_h is not None and r_a is not None and abs(mag_recent) >= 0.40 and _eabbr(mag_recent) == pick_abbr:
                sp2 = h_sp if fav_home else a_sp
                cands.append((abs(mag_recent) * 4, pick_abbr + " starter hot (%.2f ERA L5)" % sp2["recent_era"]))
            if abs(mag_form) >= 1:
                nhot = (h_off["hot"] if fav_home else a_off["hot"])
                ncold = (a_off["cold"] if fav_home else h_off["cold"])
                lab = (str(nhot) + " " + pick_abbr + " bats hot L10") if nhot >= ncold else (str(ncold) + " " + loser_abbr + " bats cold L10")
                cands.append((abs(mag_form) * 2 * (1 if sign * mag_form > 0 else 0.01), lab))
            if abs(mag_pen) >= 0.15:
                wp = away_pen if fav_home else home_pen   # the loser's pen the winner attacks
                lab = "weak " + loser_abbr + " pen (" + _era(wp["era"]) + ")" + (" taxed" if wp["taxed"] else "")
                cands.append((abs(mag_pen) * 3 * (1 if sign * mag_pen > 0 else 0.01), lab))
            if abs(mag_fat) >= 0.01 and _eabbr(mag_fat) == pick_abbr:
                tp = away_pen if fav_home else home_pen   # the tired pen the winner attacks
                cands.append((abs(mag_fat) * 60, "gassed " + loser_abbr + " pen (" + _fat(tp) + ")"))
            if abs(mag_lin) >= 1 and _eabbr(mag_lin) == pick_abbr:
                lo = a_off if fav_home else h_off   # loser's missing bats
                cands.append((abs(mag_lin) * 3, loser_abbr + " " + _lin(lo)))
            if abs(mag_plat) >= 0.010 and _eabbr(mag_plat) == pick_abbr:
                po = h_off if fav_home else a_off
                cands.append((abs(mag_plat) * 40, pick_abbr + " lineup " + _plat(po)))
            if abs(mag_rest) >= 0.005 and _eabbr(mag_rest) == pick_abbr:
                lo = a_rest if fav_home else h_rest
                cands.append((abs(mag_rest) * 25, loser_abbr + " " + _rest_s(lo)))
            if env >= 1.05:
                cands.append((0.5, "hitter-friendly park lifts both"))
            elif env <= 0.95:
                cands.append((0.5, "pitcher park trims both"))
            cands.sort(key=lambda x: x[0], reverse=True)
            drivers = [c[1] for c in cands[:3]] or ["near coin-flip · thin edge"]

            fav_ct = sum(1 for f in factors if f["edge"] == pick_abbr)
            verdict = ("%d of %d buckets favour %s, %d even. Model lands on %s %d%%, projected %.1f \u2013 %.1f "
                       "\u2014 a %s-tier call.") % (
                fav_ct, len(factors), pick_name, sum(1 for f in factors if f["edge"] == "even"),
                pick_name, favw, (projH if fav_home else projA), (projA if fav_home else projH), conf)
            if mkt_edge is not None:
                verdict += (" Market implies %s %d%% \u2014 model %s it by %d%%." % (
                    pick_name, (mkt_home_pct if fav_home else mkt_away_pct),
                    ("beats" if mkt_edge > 0 else ("trails" if mkt_edge < 0 else "matches")), abs(mkt_edge)))

            out.append({
                "away": away, "away_abbr": a_abbr, "home": home, "home_abbr": h_abbr,
                "away_sp": a_sp["name"], "home_sp": h_sp["name"],
                "proj_away": round(projA, 1), "proj_home": round(projH, 1),
                "win_away": winA_pct, "win_home": winH_pct,
                "pick": pick_name, "pick_abbr": pick_abbr, "pick_home": fav_home,
                "conf": conf, "edge_runs": edge_runs, "game_start": sched.get("game_start", ""),
                "proj_total": proj_total, "total_line": total_line,
                "total_pick": total_pick, "total_edge": total_edge, "total_conf": total_conf,
                "mkt_home_pct": mkt_home_pct, "mkt_away_pct": mkt_away_pct,
                "mkt_edge": mkt_edge, "mkt_pick_abbr": mkt_pick_abbr, "value_flag": value_flag,
                "drivers": drivers, "factors": factors, "verdict": verdict,
            })
        except Exception as _exc:
            if emit:
                emit({"type": "log", "msg": "  \u26a0\ufe0f Game Predictor skipped %s@%s: %s" % (away, home, _exc)})
            continue

    out.sort(key=lambda g: max(g["win_home"], g["win_away"]), reverse=True)
    return out


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
                "abbr": home.get("abbreviation", ""), "opp_abbr": away.get("abbreviation", ""),
                "game_start": g_start}
            team_schedule[away["displayName"]] = {
                "side": "AWAY", "opponent": home["displayName"],
                "opp_slug": home["displayName"].lower().replace(" ", "-"),
                "abbr": away.get("abbreviation", ""), "opp_abbr": home.get("abbreviation", ""),
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
            _pb["blurb"]          = _build_blurb(_pb)   # recent-form write-up (pool B branch)
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
                     hrr_picks_list, hr_picks_list, hrr_special_list,
                     [p for p in also_ran if p.get("over_sourced")]]

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

    # ── Triple Split Club — hitters batting > .275 in ALL THREE of today's
    # applicable splits: Home/Away (season), Day/Night (today's game type), and
    # Series-Game (G1/G2/G3+ of today's series). "To record a hit" board:
    # display + parlay legs + its own forward-only W/L record. Day/Night (s5.ba)
    # and Series (series_splits.g#_ba) are already stamped on every batter pick,
    # so only Home/Away needs a fetch — and only for hitters who already clear
    # the first two gates (one small batched statSplits sitCodes=[h,a] call).
    triple_split_list = []
    _fss_stage = []  # 5 Star Split pool: day/night + series survivors, PRE-home-gate
    _tsc_by_id = {}  # shared player pool; populated inside TSC try, reused by Hot Hitters
    try:
        _TSC_MIN = 0.275
        _tsc_hit_ids = set()
        for _lst in (top9, also_ran):
            for _r in _lst:
                _b = _r.get("batter_id") or _r.get("player_id")
                if _b:
                    _tsc_hit_ids.add(int(_b))
        _tsc_lists = [top9, also_ran, under_picks_list, runs_picks_list,
                      tb_picks_list, tb_over_picks_list, rbi_picks_list,
                      walks_picks_list, hrr_picks_list, hr_picks_list]
        _tsc_by_id = {}
        for _lst in _tsc_lists:
            for _r in _lst:
                _b = _r.get("batter_id") or _r.get("player_id")
                if not _b:
                    continue
                _b = int(_b)
                if _b not in _tsc_by_id:
                    _tsc_by_id[_b] = _r

        def _tsc_series_ba(_r):
            _ss = _r.get("series_splits") or {}
            _g = _r.get("series_game") or _ss.get("today_pos") or 1
            try:
                _g = int(_g)
            except Exception:
                _g = 1
            _g = 1 if _g < 1 else (3 if _g > 3 else _g)
            return _ss.get("g%d_ba" % _g), _g

        # Gate A (day/night today) + Gate B (series game) off already-stamped data
        _tsc_stage = []
        for _b, _r in _tsc_by_id.items():
            _dn = (_r.get("s5") or {}).get("ba")
            _sb, _gno = _tsc_series_ba(_r)
            if _dn is None or _sb is None:
                continue
            try:
                _dn = float(_dn); _sb = float(_sb)
            except Exception:
                continue
            if _dn > _TSC_MIN and _sb > _TSC_MIN:
                _tsc_stage.append((_b, _r, _dn, _sb, _gno))

        # Gate C (Home/Away season BA) — one batched call over survivors only
        _tsc_ha = {}
        _tsc_ids = [x[0] for x in _tsc_stage]
        _tsc_season = str(run_date)[:4]
        for _i in range(0, len(_tsc_ids), 40):
            _chunk = _tsc_ids[_i:_i + 40]
            try:
                _u = ("https://statsapi.mlb.com/api/v1/people?personIds="
                      + ",".join(str(x) for x in _chunk)
                      + "&hydrate=stats(group=[hitting],type=[statSplits],"
                      + "sitCodes=[h,a],season=" + _tsc_season + ")")
                _j = requests.get(_u, timeout=15).json()
                for _per in _j.get("people", []):
                    _pid = _per.get("id")
                    _sp = {}
                    for _st in _per.get("stats", []):
                        for _s in _st.get("splits", []):
                            _code = (_s.get("split") or {}).get("code")
                            _stat = _s.get("stat") or {}
                            if _code in ("h", "a"):
                                _sp[_code] = _stat.get("avg")
                    if _pid is not None:
                        _tsc_ha[int(_pid)] = _sp
            except Exception:
                continue

        def _tsc_d3(v):
            _d = "%.3f" % v
            return _d[1:] if _d.startswith("0.") else _d

        # 5 Star Split draws from the day/night + series survivors BEFORE the
        # full-season home/away gate below (that gate is Triple-Split-ONLY).
        # 5 Star runs its OWN last-10 home/away location gate later, so it must
        # NOT be pre-filtered by the season home gate.
        for _b, _r, _dn, _sb, _gno in _tsc_stage:
            _fss_stage.append({
                "name": _r.get("name", ""),
                "full_name": _r.get("full_name", _r.get("name", "")),
                "batter_id": _b,
                "player_id": _r.get("player_id"),
                "team": _r.get("team", ""),
                "opp": _r.get("opp", ""),
                "pitcher": _r.get("pitcher", ""),
                "side": (_r.get("side") or "").upper(),
                "dn_label": _r.get("dn_label", ""),
                "s5": _r.get("s5"),
                "series_splits": _r.get("series_splits"),
                "series_game": _r.get("series_game"),
                "series_of": _r.get("series_of"),
                "series_gno": _gno,
                "game_start": _r.get("game_start"),
                "recent_hit_log": _r.get("recent_hit_log"),
                "dn_ba": _dn, "dn_disp": _tsc_d3(_dn),
                "series_ba": _sb, "series_disp": _tsc_d3(_sb),
            })

        for _b, _r, _dn, _sb, _gno in _tsc_stage:
            _side = (_r.get("side") or "").upper()
            _code = "h" if _side == "HOME" else ("a" if _side == "AWAY" else None)
            if not _code:
                continue
            _hraw = (_tsc_ha.get(_b) or {}).get(_code)
            try:
                _hav = float(_hraw) if _hraw is not None else None
            except Exception:
                _hav = None
            if _hav is None or _hav <= _TSC_MIN:
                continue
            # "record a hit" price: reuse the pick's hit_odds when present (hit
            # pool), else look it up from the shared HIT_ODDS market.
            _ho = _r.get("hit_odds")
            _bk = _r.get("book", "") if _ho is not None else ""
            if _ho is None:
                try:
                    _mk = _lookup_odds(_r)
                    if _mk:
                        _ho = _HIT_ODDS.get(_mk)
                        _bk = _hit_book_label(_HIT_ODDS_BOOK.get(_mk))
                except Exception:
                    pass
            _from_hit = _b in _tsc_hit_ids
            triple_split_list.append({
                "name": _r.get("name", ""),
                "full_name": _r.get("full_name", _r.get("name", "")),
                "batter_id": _b,
                "player_id": _r.get("player_id"),
                "team": _r.get("team", ""),
                "opp": _r.get("opp", ""),
                "pitcher": _r.get("pitcher", ""),
                "side": _side,
                "dn_label": _r.get("dn_label", ""),
                "s5": _r.get("s5"),
                "series_splits": _r.get("series_splits"),
                "series_game": _r.get("series_game"),
                "series_of": _r.get("series_of"),
                "series_gno": _gno,
                "game_start": _r.get("game_start"),
                "recent_hit_log": _r.get("recent_hit_log"),
                "hit_odds": _ho,
                "book": _bk,
                "ha_ba": _hav, "ha_disp": _tsc_d3(_hav),
                "dn_ba": _dn, "dn_disp": _tsc_d3(_dn),
                "series_ba": _sb, "series_disp": _tsc_d3(_sb),
                "tsc_min": min(_dn, _sb, _hav),
                "matchup_prob": (_r.get("matchup_prob") if _from_hit else None),
                "ev": (_r.get("ev") if _from_hit else None),
                "ev_prob": (_r.get("ev_prob") if _from_hit else None),
                "edge": (_r.get("edge") if _from_hit else None),
            })
        triple_split_list.sort(key=lambda x: (x["tsc_min"], x["dn_ba"]), reverse=True)
        triple_split_list = triple_split_list[:20]
        emit({"type": "log",
              "msg": f"  🔱 Triple Split Club: {len(triple_split_list)} hitters clear "
                     f">.275 in all 3 splits (H/A + D/N + series)"})
    except Exception as _exc:
        triple_split_list = []
        emit({"type": "log", "msg": f"⚠️ Triple Split Club skipped: {_exc}"})

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

    # ── 5 Star Split — Triple Split qualifiers (>.275 in all three of today's
    # splits: Home/Away + Day/Night + Series-game) that ALSO clear two
    # consistency gates: vs-team ≥60% of season games with a hit AND last-10
    # games ≥60% with a hit. For each qualifier we pick the single best
    # PRODUCTION market off its last-10 over-rate (TB O1.5 / Runs O0.5 /
    # RBI O0.5 / HRR O1.5; tiebreak HRR>TB>Runs>RBI). Own forward-only W/L
    # record; the batter's career line vs today's starter (vs_pit, stamped
    # above) rides along as DISPLAY-ONLY reference — never a gate or pick factor.
    five_star_split_list = []
    try:
        from mlb_stats_splits import _get_game_logs as _fss_logs
        from under_picks import _team_match as _fss_tm
        from datetime import date as _fss_dt
        _FSS_CY = _fss_dt.today().year

        # id -> original pick (carries vs_pit + pitcher name stamped above)
        _fss_orig = {}
        for _lst in (top9, also_ran, under_picks_list, runs_picks_list,
                     tb_picks_list, tb_over_picks_list, rbi_picks_list,
                     walks_picks_list, hrr_picks_list, hr_picks_list):
            for _r in _lst:
                _b = _r.get("batter_id") or _r.get("player_id")
                if _b and int(_b) not in _fss_orig:
                    _fss_orig[int(_b)] = _r

        # per-market OVER odds for the chosen play (best-effort; None when the
        # player isn't a pick on that market's own board)
        _fss_odds = {"tb": {}, "runs": {}, "rbi": {}, "hrr": {}}

        def _fss_fill(_lst, _mkt, _oddkey, _over_only):
            for _r in _lst:
                _b = _r.get("batter_id") or _r.get("player_id")
                if not _b:
                    continue
                if _over_only and (_r.get("pick") or "OVER").upper() != "OVER":
                    continue
                # carry the market's own EV (already stamped by the EV pass above)
                # so the 5 Star Split record shows EV like every sibling board
                _fss_odds[_mkt][int(_b)] = {
                    "odds": _r.get(_oddkey), "book": _r.get("book", ""),
                    "ev": _r.get("ev"), "ev_prob": _r.get("ev_prob"), "edge": _r.get("edge"),
                }
        _fss_fill(tb_over_picks_list, "tb", "tb_over_odds", False)
        _fss_fill(runs_picks_list, "runs", "over_odds", True)
        _fss_fill(rbi_picks_list, "rbi", "over_odds", True)
        _fss_fill(hrr_picks_list, "hrr", "hrr_over_odds", True)

        _FSS_LBL = {"tb": "Total Bases", "runs": "Runs", "rbi": "RBI", "hrr": "H+R+RBI"}
        _FSS_LINE = {"tb": 1.5, "runs": 0.5, "rbi": 0.5, "hrr": 1.5}
        _FSS_TIE = {"hrr": 0, "tb": 1, "runs": 2, "rbi": 3}

        for _ts in _fss_stage:
            _bid = _ts.get("batter_id")
            if not _bid:
                continue
            # 5 Star location gate — last-10 Home/Away BA > .275 (its OWN gate;
            # Triple Split Club uses full-season home/away, 5 Star uses the
            # player's last 10 games at today's site instead).
            _fss_hav, _fss_hab, _fss_hg = _last10_ha_ba(int(_bid), _ts.get("side", ""))
            if _fss_hav is None or _fss_hav <= 0.275:
                continue
            _fss_hadisp = "%.3f" % _fss_hav
            _fss_hadisp = _fss_hadisp[1:] if _fss_hadisp.startswith("0.") else _fss_hadisp
            # fetch 3 seasons so vs-team hit% reflects real career history,
            # not just today's partial season (e.g. Buxton 3/3 CLE = only 2026)
            _gl_raw_all = []
            for _fss_yr in range(_FSS_CY, _FSS_CY - 3, -1):
                try:
                    _gl_raw_all.extend(_fss_logs(int(_bid), _fss_yr) or [])
                except Exception:
                    pass
            _gl = []
            for _sp in _gl_raw_all:
                _st = _sp.get("stat", {}) or {}
                if int(_st.get("atBats", 0) or 0) < 1:
                    continue
                _gd = (_sp.get("date") or "")[:10]
                if not _gd:
                    continue
                _gl.append({
                    "date":    _gd,
                    "opp":     (_sp.get("opponent", {}) or {}).get("name", ""),
                    "is_home": bool(_sp.get("isHome")),
                    "h":   int(_st.get("hits", 0) or 0),
                    "tb":  int(_st.get("totalBases", 0) or 0),
                    "r":   int(_st.get("runs", 0) or 0),
                    "rbi": int(_st.get("rbi", 0) or 0),
                })
            _gl.sort(key=lambda g: g["date"])
            if not _gl:
                continue

            # Gate 4 — vs-team at today's venue, last 10 games: ≥60% with a hit.
            # Venue-matched + capped at 10 so away hot streaks don't pass a home gate.
            _opp = _ts.get("opp", "")
            _want_home = (_ts.get("side", "").upper() == "HOME")
            _vt = [g for g in _gl if _opp and _fss_tm(g["opp"], _opp)
                   and g["is_home"] == _want_home][-10:]
            if not _vt:
                continue
            _vt_hit = sum(1 for g in _vt if g["h"] >= 1)
            _vt_pct = 100.0 * _vt_hit / len(_vt)
            if _vt_pct < 60:
                continue

            # Gate 5 — last 10 venue-matched games THIS SEASON: ≥60% with a hit.
            # Current season only, venue-matched (home games for home players, etc.)
            _cy_str = str(_FSS_CY)
            _l10 = [g for g in _gl if g["date"][:4] == _cy_str
                    and g["is_home"] == _want_home][-10:]
            _n = len(_l10)
            if _n == 0:
                continue
            _l10_hit = sum(1 for g in _l10 if g["h"] >= 1)
            _l10_pct = 100.0 * _l10_hit / _n
            if _l10_pct < 60:
                continue

            # Pick — highest last-10 over-rate; tiebreak HRR>TB>Runs>RBI
            _rates = {
                "tb":   100.0 * sum(1 for g in _l10 if g["tb"] >= 2) / _n,
                "runs": 100.0 * sum(1 for g in _l10 if g["r"] >= 1) / _n,
                "rbi":  100.0 * sum(1 for g in _l10 if g["rbi"] >= 1) / _n,
                "hrr":  100.0 * sum(1 for g in _l10 if (g["h"] + g["r"] + g["rbi"]) >= 2) / _n,
            }
            _pk = sorted(_rates.items(), key=lambda kv: (-kv[1], _FSS_TIE[kv[0]]))[0][0]
            _meta = _fss_odds[_pk].get(int(_bid), {})
            _od, _bk = _meta.get("odds"), _meta.get("book", "")
            _orig = _fss_orig.get(int(_bid), {})

            five_star_split_list.append({
                "name": _ts.get("name", ""),
                "full_name": _ts.get("full_name", _ts.get("name", "")),
                "batter_id": _bid,
                "player_id": _ts.get("player_id"),
                "team": _ts.get("team", ""),
                "opp": _opp,
                "pitcher": _ts.get("pitcher", "") or _orig.get("pitcher", ""),
                "side": _ts.get("side", ""),
                "dn_label": _ts.get("dn_label", ""),
                "s5": _ts.get("s5"),
                "series_splits": _ts.get("series_splits"),
                "series_game": _ts.get("series_game"),
                "series_of": _ts.get("series_of"),
                "series_gno": _ts.get("series_gno"),
                "game_start": _ts.get("game_start"),
                "recent_hit_log": _ts.get("recent_hit_log"),
                # gate values (all pass) — Home/Away shown is the FSS last-10 split
                "ha_ba": _fss_hav, "ha_disp": _fss_hadisp,
                "dn_ba": _ts.get("dn_ba"), "dn_disp": _ts.get("dn_disp"),
                "series_ba": _ts.get("series_ba"), "series_disp": _ts.get("series_disp"),
                "vt_pct": round(_vt_pct), "vt_g": len(_vt), "vt_hit_g": _vt_hit,
                "l10_hit_pct": round(_l10_pct), "l10_g": _n, "l10_hit_g": _l10_hit,
                # chosen production pick (side is always OVER) + all L10 rates
                "pick_market": _pk,
                "stat_label": _FSS_LBL[_pk],
                "line": _FSS_LINE[_pk],
                "bet_side": "OVER",
                "pick_rate": round(_rates[_pk]),
                "rates": {_k: round(_v) for _k, _v in _rates.items()},
                "odds": _od, "book": _bk,
                # EV carried from the chosen market's own board (None when the
                # player isn't a posted pick on that market)
                "ev": _meta.get("ev"), "ev_prob": _meta.get("ev_prob"), "edge": _meta.get("edge"),
                # vs-pitcher career line — DISPLAY-ONLY reference
                "vs_pit": _orig.get("vs_pit"),
            })

        five_star_split_list.sort(key=lambda x: (x["pick_rate"], x["l10_hit_pct"]), reverse=True)
        five_star_split_list = five_star_split_list[:20]
        emit({"type": "log",
              "msg": f"  ⭐ 5 Star Split: {len(five_star_split_list)} hitters clear all "
                     f"gates (day/night + series + last-10 H/A>.275 + vs-team≥60% + L10≥60%)"})
    except Exception as _exc:
        five_star_split_list = []
        emit({"type": "log", "msg": f"⚠️ 5 Star Split skipped: {_exc}"})

    # ── Triple Split Club Hot Hitters ──────────────────────────────────────────
    # Last-10 version of TSC — ALL three splits use recent game logs:
    #   Gate 1: last-10 H/A BA > .270 (venue-matched)
    #   Gate 2: full-season day/night BA > .270 (reuses s5 already on pick)
    #   Gate 3: last-10 G# H/A BA > .270 (same series position, game logs)
    hot_split_list = []
    try:
        if _tsc_by_id:
            from mlb_stats_splits import _get_game_logs as _tsch_gl
            from datetime import datetime as _tsch_dt
            _TSCH_MIN = 0.270
            _TSCH_CY = int(str(run_date)[:4])

            def _tsch_d3(v):
                _d = "%.3f" % v
                return _d[1:] if _d.startswith("0.") else _d

            def _tsch_gno(_r):
                _ss = _r.get("series_splits") or {}
                _g = _r.get("series_game") or _ss.get("today_pos") or 1
                try:
                    _g = int(_g)
                except Exception:
                    _g = 1
                return 1 if _g < 1 else (3 if _g > 3 else _g)

            def _tsch_l10_ser(player_id, side, series_pos):
                """Last-10 H/A games matching series position; returns (ba, games)."""
                _want = (side == "HOME")
                _all = []
                for _yr in range(_TSCH_CY, _TSCH_CY - 2, -1):
                    for _sp in _tsch_gl(player_id, _yr):
                        if bool(_sp.get("isHome")) != _want:
                            continue
                        _st = _sp.get("stat", {}) or {}
                        _ab = int(_st.get("atBats", 0) or 0)
                        if _ab < 1:
                            continue
                        _all.append({
                            "date": _sp.get("date", ""),
                            "ab": _ab, "h": int(_st.get("hits", 0) or 0),
                            "opp": (_sp.get("opponent", {}) or {}).get("name", ""),
                        })
                _all.sort(key=lambda x: x["date"])
                # annotate series position
                for _i, _gg in enumerate(_all):
                    _pos = 1
                    for _j in range(_i - 1, -1, -1):
                        _prev = _all[_j]
                        if _prev["opp"].lower() != _gg["opp"].lower():
                            break
                        try:
                            _gap = (_tsch_dt.strptime(_gg["date"], "%Y-%m-%d") -
                                    _tsch_dt.strptime(_prev["date"], "%Y-%m-%d")).days
                        except Exception:
                            break
                        if _gap > 4:
                            break
                        _pos += 1
                    _gg["spos"] = _pos
                _match = [_gg for _gg in reversed(_all) if _gg["spos"] == series_pos][:10]
                if not _match:
                    return None, 0
                _h2 = sum(_gg["h"] for _gg in _match)
                _ab2 = sum(_gg["ab"] for _gg in _match)
                return (round(_h2 / _ab2, 3) if _ab2 > 0 else None), len(_match)

            for _b, _r in _tsc_by_id.items():
                _side = (_r.get("side") or "").upper()
                if _side not in ("HOME", "AWAY"):
                    continue
                # Gate 2: full-season D/N BA (already stamped on pick as s5.ba)
                try:
                    _dn = float((_r.get("s5") or {}).get("ba") or 0) or None
                except Exception:
                    _dn = None
                if _dn is None or _dn <= _TSCH_MIN:
                    continue
                # Gate 1: last-10 H/A BA
                _l10_hav, _l10_ab, _l10_hg = _last10_ha_ba(int(_b), _side)
                if _l10_hav is None or _l10_hav <= _TSCH_MIN:
                    continue
                # Gate 3: last-10 G# H/A BA
                _gno = _tsch_gno(_r)
                _l10_ser, _l10_ser_g = _tsch_l10_ser(int(_b), _side, _gno)
                if _l10_ser is None or _l10_ser <= _TSCH_MIN:
                    continue
                # Passed all gates — look up hit odds
                _ho = _r.get("hit_odds")
                _bk = _r.get("book", "") if _ho is not None else ""
                if _ho is None:
                    try:
                        _mk = _lookup_odds(_r)
                        if _mk:
                            _ho = _HIT_ODDS.get(_mk)
                            _bk = _hit_book_label(_HIT_ODDS_BOOK.get(_mk))
                    except Exception:
                        pass
                _from_hit = int(_b) in _tsc_hit_ids
                hot_split_list.append({
                    "name": _r.get("name", ""),
                    "full_name": _r.get("full_name", _r.get("name", "")),
                    "batter_id": _b,
                    "player_id": _r.get("player_id"),
                    "team": _r.get("team", ""),
                    "opp": _r.get("opp", ""),
                    "pitcher": _r.get("pitcher", ""),
                    "side": _side,
                    "dn_label": _r.get("dn_label", ""),
                    "s5": _r.get("s5"),
                    "series_splits": _r.get("series_splits"),
                    "series_game": _r.get("series_game"),
                    "series_of": _r.get("series_of"),
                    "series_gno": _gno,
                    "game_start": _r.get("game_start"),
                    "recent_hit_log": _r.get("recent_hit_log"),
                    "hit_odds": _ho,
                    "book": _bk,
                    "ha_ba": _l10_hav, "ha_disp": _tsch_d3(_l10_hav), "ha_g": _l10_hg,
                    "dn_ba": _dn,    "dn_disp": _tsch_d3(_dn),
                    "ser_ba": _l10_ser, "ser_disp": _tsch_d3(_l10_ser), "ser_g": _l10_ser_g,
                    "tsch_min": min(_l10_hav, _dn, _l10_ser),
                    "matchup_prob": (_r.get("matchup_prob") if _from_hit else None),
                    "ev":      (_r.get("ev")      if _from_hit else None),
                    "ev_prob": (_r.get("ev_prob") if _from_hit else None),
                    "edge":    (_r.get("edge")    if _from_hit else None),
                })
            hot_split_list.sort(key=lambda x: x["tsch_min"], reverse=True)
            hot_split_list = hot_split_list[:20]
            emit({"type": "log",
                  "msg": f"  🔥 Hot Hitters: {len(hot_split_list)} hitters clear "
                         f">.270 in L10 H/A, D/N full-season & L10 G#"})
    except Exception as _exc:
        hot_split_list = []
        emit({"type": "log", "msg": f"⚠️ Hot Hitters skipped: {_exc}"})

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

    # ── Game Predictor — team win model (aggregates player-level signals) ──
    game_predictions = []
    try:
        _gp_hit = (list(top9) + list(also_ran) + list(under_picks_list)
                   + list(runs_picks_list) + list(tb_picks_list) + list(tb_over_picks_list)
                   + list(rbi_picks_list) + list(walks_picks_list) + list(hrr_picks_list)
                   + list(hr_picks_list))
        _gp_pit = (list(pitcher_k_result.get("all", [])) + list(pitcher_k_result.get("picks", [])))
        for _bk in pitcher_props.values():
            _gp_pit += list(_bk.get("all", [])) + list(_bk.get("picks", []))
        game_predictions = _build_game_predictions(team_schedule, _gp_hit, _gp_pit, run_date, emit)
        emit({"type": "log", "msg": f"  ✅ Game Predictor: {len(game_predictions)} game(s) modeled"})
    except Exception as _exc:
        emit({"type": "log", "msg": f"⚠️ Game Predictor skipped: {_exc}"})

    elapsed = round(time.time() - t_start, 1)
    result = {
        "date": run_date, "top9": top9, "also_ran": also_ran,
        "under_picks": under_picks_list, "runs_picks": runs_picks_list, "tb_picks": tb_picks_list, "tb_over_picks": tb_over_picks_list, "rbi_picks": rbi_picks_list, "walks_picks": walks_picks_list, "hrr_picks": hrr_picks_list, "hrr_special_picks": hrr_special_list, "triple_split_picks": triple_split_list, "five_star_split_picks": five_star_split_list, "hot_split_picks": hot_split_list, "hr_picks": hr_picks_list,
        "all_qualified": era_qualified,
        "game_predictions": game_predictions,
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
                  "triple_split_count": len(triple_split_list),
                  "five_star_count": len(five_star_split_list),
                  "hr_count": len(hr_picks_list),
                  "pitcher_k_count": len(pitcher_k_result.get("picks", [])),
                  "prop_counts": {m: len(b.get("picks", [])) for m, b in pitcher_props.items()},
                  "has_tbd": slate_has_tbd(run_date)},
    }
    emit({"type": "done", "result": result})
    return result
