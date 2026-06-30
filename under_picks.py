
"""
under_picks.py — Under Picks via The Odds API (batter_hits 1.5 line).
Replaces the DraftKings scraper. Requires ODDS_API_KEY env var.

Algorithm per candidate (must pass ALL gates; cutoff < .250 each):
  S1  Career BA vs today's probable pitcher                    — N/A / 0 AB passes; DQ if >= .250
  S2  BA over last 10 (or fewer) H/A games vs TODAY'S opponent — data required AND < .250
  S3  BA over last 10 (or fewer) H/A games vs ANY opponent     — data required AND < .250
  L7  BA over last 7 games (general, any side/opp)             — N/A passes; DQ if >= .250
  All four gates apply to every player — no bypasses.
  Facing a top-30 ERA ace shows a display chip on the card only (does not affect qualification).
  Qualifiers ranked coldest first (lowest S2 + S3 + L7 combined BA).
"""
import os
import requests
import time
from datetime import date, datetime, timezone

from concurrent.futures import ThreadPoolExecutor, as_completed

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

_PLAYER_MAP:    dict = {}
_PITCHER_CACHE: dict = {}

# ── Best-price book selection ──────────────────────────────────────────────
# Show the BEST price across ALL sportsbooks; big US books win on ties.
# Priority: DraftKings / FanDuel / BetMGM / Caesars first, then mid-tier,
# then smaller books (Fliff, ESPN BET, offshore, etc.) at the bottom.
_PRIORITY_BOOKS = ("draftkings", "fanduel", "betmgm", "williamhill_us", "caesars",
                   "betrivers", "ballybet", "bet365", "espnbet",
                   "bet99", "thescore", "fliff", "mybookieag", "betonlineag", "bovada")
_BOOK_PRIORITY = {b: i for i, b in enumerate(_PRIORITY_BOOKS)}
_BOOK_LABEL = {"bet99":"Bet99","thescore":"theScore","bet365":"Bet365","draftkings":"DK","fanduel":"FanDuel","betmgm":"BetMGM","caesars":"Caesars","williamhill_us":"Caesars","betrivers":"BetRivers","ballybet":"Bally Bet","espnbet":"ESPN BET","fliff":"Fliff","mybookieag":"MyBookie","betonlineag":"BetOnline","bovada":"Bovada"}
def _book_label(k):
    return _BOOK_LABEL.get(k, (k or "").replace("_"," ").title())
def _take_odds(entry, price_field, book_field, price, book_key):
    """All books: keep the best American price; tie-break by book priority (big books first)."""
    if price is None:
        return
    cur = entry.get(price_field)
    cur_book = entry.get(book_field)
    if cur is None or price > cur or (price == cur and _BOOK_PRIORITY.get(book_key, 999) < _BOOK_PRIORITY.get(cur_book, 999)):
        entry[price_field] = price
        entry[book_field] = book_key
def _take_odds_any(entry, price_field, book_field, price, book_key):
    """Alias — all books accepted (same as _take_odds)."""
    _take_odds(entry, price_field, book_field, price, book_key)

# Populated by _fetch_hits_lines: normalized player name -> Over price on the 0.5 hits line
# (i.e. the standard "to record a hit" prop). Read by pipeline.py to enrich top9 picks.
import unicodedata as _ud
import re as _re
def _norm_name(s: str) -> str:
    s = "".join(c for c in _ud.normalize("NFKD", s or "") if not _ud.combining(c)).lower().strip()
    # strip suffixes: jr, sr, ii, iii, iv
    s = _re.sub(r'\b(jr\.?|sr\.?|ii|iii|iv)$', '', s).strip().rstrip(',').strip()
    # normalize hyphens to space (ha-seong → ha seong)
    s = s.replace('-', ' ')
    # collapse multiple spaces
    s = _re.sub(r'\s+', ' ', s).strip()
    return s
HIT_ODDS: dict = {}
# Parallel to HIT_ODDS: normalized name -> source book key for the displayed
# (best-among-MY_BOOKS) 0.5-hit Over price. Read by pipeline.py to stamp pick["book"].
HIT_ODDS_BOOK: dict = {}
# Parallel to HIT_ODDS: normalized name -> {name, home_team, away_team} for EVERY
# hitter with a posted 0.5 "to record a hit" line. Lets run_hit_picks build the
# broadened pool-B candidate set (hot hitters with no career-vs-pitcher history).
HIT_TEAMS: dict = {}
# Populated by _fetch_hits_lines: normalized name -> {name, line, home_team,
# away_team, over, under} for the batter_runs_scored (Over/Under ~0.5) market.
# Read by run_runs_picks. Parallel to HIT_ODDS; first game seen per name wins.
RUNS_ODDS: dict = {}
# Populated by _fetch_hits_lines: normalized name → {name, line, home_team,
# away_team, tb_under_odds} for players with a posted batter_total_bases
# Under 1.5 price. Read by run_tb_under_picks. Cleared each call.
TB_ODDS: dict = {}
# Populated by _fetch_hits_lines: normalized name → {name, tb_over_odds, …}
# for players with a posted batter_total_bases Over 1.5 price.
TB_OVER_ODDS: dict = {}
# Populated by _fetch_hits_lines: normalized name → {name, line, home_team,
# away_team, over, under} for the batter_rbis (Over/Under ~0.5) market.
# Read by run_rbi_picks. Cleared each call.
RBI_ODDS: dict = {}
# Populated by _fetch_hits_lines: normalized name → {name, hrr_over_odds,
# hrr_under_odds, …} for players with a posted batter_hits_runs_rbis Over/Under
# 1.5 price. Read by run_hrr_picks. Cleared each call.
HRR_ODDS: dict = {}
# Populated by _fetch_hits_lines: normalized name → {name, line, home_team,
# away_team, over, under} for the batter_walks (Over/Under 0.5) market.
# Read by run_walks_picks. Cleared each call. Distinct from PITCHER walks.
WALKS_ODDS: dict = {}
# Populated by _fetch_hits_lines: normalized name → {name, line, home_team,
# away_team, over, under} for the batter_home_runs (Over/Under 0.5) market.
# Read by run_hr_picks. Cleared each call.
HR_ODDS: dict = {}

_BATTER_SAV_CACHE_UP: dict = {}  # {year: {player_id: {xba, hard_hit_pct}}}
LEAGUE_HARD_HIT_UP   = 35.0      # MLB avg hard-hit rate %, 2024-2025

def _fetch_batter_savant_up(year: str) -> dict:
    """Bulk-fetch hitter xBA + hard-hit% from Savant for under ranking signal. Cached."""
    if _BATTER_SAV_CACHE_UP.get(year):
        return _BATTER_SAV_CACHE_UP[year]
    import csv, io
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
    out = {}
    for _attempt in range(2):
        try:
            r = requests.get("https://baseballsavant.mlb.com/leaderboard/custom",
                params={"year": str(year), "type": "batter", "filter": "", "min": "30",
                        "selections": "xba,hard_hit_percent", "csv": "true"},
                headers=hdrs, timeout=15)
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
        _BATTER_SAV_CACHE_UP[year] = out
    return out

def _batter_sav_lookup_up(player_id) -> dict:
    """Return {xba, hard_hit_pct} for this batter — current season, prior-year fallback."""
    if not player_id: return {}
    from datetime import date as _d
    yr = str(_d.today().year)
    pid = int(player_id)
    cur = _BATTER_SAV_CACHE_UP.get(yr, {})
    if pid in cur: return cur[pid]
    prev = _BATTER_SAV_CACHE_UP.get(str(int(yr) - 1), {})
    return prev.get(pid, {})


# ── HR model data — Statcast power (batter), HR-allowed (pitcher), platoon ──
_BATTER_POWER_CACHE: dict = {}   # {year: {pid: {barrel_pct, xiso, xslg, hard_hit, ev}}}
_PITCHER_POWER_CACHE: dict = {}  # {year: {pid: {barrel_pct, hard_hit, xslg}}}
_PIT_HR9_CACHE: dict = {}        # {(pid, season): {season_hr9, recent_hr9, blended_hr9, disp}}
_BAT_SIDE_CACHE: dict = {}       # {pid: 'L'/'R'/'S'}
_PITCH_HAND_CACHE: dict = {}     # {pid: 'L'/'R'}
LEAGUE_HR9    = 1.15             # MLB starter HR/9 baseline
LEAGUE_BARREL = 7.5             # MLB barrel% baseline (batted-ball)


def _fetch_batter_power(year: str) -> dict:
    """Bulk Statcast power profile for hitters (barrel%, xISO, xSLG, hard-hit,
    exit velo). Drives the HR model's batter power component. Cached per year;
    Savant CSV carries a UTF-8 BOM so the first column is BOM-stripped."""
    if _BATTER_POWER_CACHE.get(year):
        return _BATTER_POWER_CACHE[year]
    import csv, io
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
    out = {}
    for _attempt in range(2):
        try:
            r = requests.get("https://baseballsavant.mlb.com/leaderboard/custom",
                params={"year": str(year), "type": "batter", "filter": "", "min": "30",
                        "selections": "barrel_batted_rate,xiso,xslg,hard_hit_percent,exit_velocity_avg",
                        "csv": "true"},
                headers=hdrs, timeout=15)
            for row in csv.DictReader(io.StringIO(r.text.lstrip("\ufeff"))):
                try:
                    pid = int(row.get("player_id") or 0)
                    if not pid:
                        continue
                    entry: dict = {}
                    for src, dst in (("barrel_batted_rate", "barrel_pct"),
                                     ("xiso", "xiso"), ("xslg", "xslg"),
                                     ("hard_hit_percent", "hard_hit"),
                                     ("exit_velocity_avg", "ev")):
                        v = row.get(src)
                        if v not in (None, ""):
                            entry[dst] = float(v)
                    if entry:
                        out[pid] = entry
                except Exception:
                    continue
        except Exception:
            pass
        if out:
            break
        time.sleep(1.0)
    if out:
        _BATTER_POWER_CACHE[year] = out
    return out


def _batter_power_lookup(player_id) -> dict:
    """{barrel_pct, xiso, xslg, hard_hit, ev} for a hitter — current season,
    prior-year fallback. {} when no Statcast row (model degrades gracefully)."""
    if not player_id:
        return {}
    from datetime import date as _d
    yr = str(_d.today().year)
    pid = int(player_id)
    cur = _BATTER_POWER_CACHE.get(yr, {})
    if pid in cur:
        return cur[pid]
    return _BATTER_POWER_CACHE.get(str(int(yr) - 1), {}).get(pid, {})


def _fetch_pitcher_power(year: str) -> dict:
    """Bulk Statcast contact-ALLOWED profile for pitchers (barrel% / hard-hit /
    xSLG allowed). Drives the HR model's pitcher component. Cached per year."""
    if _PITCHER_POWER_CACHE.get(year):
        return _PITCHER_POWER_CACHE[year]
    import csv, io
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
    out = {}
    for _attempt in range(2):
        try:
            r = requests.get("https://baseballsavant.mlb.com/leaderboard/custom",
                params={"year": str(year), "type": "pitcher", "filter": "", "min": "30",
                        "selections": "barrel_batted_rate,hard_hit_percent,xslg",
                        "csv": "true"},
                headers=hdrs, timeout=15)
            for row in csv.DictReader(io.StringIO(r.text.lstrip("\ufeff"))):
                try:
                    pid = int(row.get("player_id") or 0)
                    if not pid:
                        continue
                    entry: dict = {}
                    for src, dst in (("barrel_batted_rate", "barrel_pct"),
                                     ("hard_hit_percent", "hard_hit"),
                                     ("xslg", "xslg")):
                        v = row.get(src)
                        if v not in (None, ""):
                            entry[dst] = float(v)
                    if entry:
                        out[pid] = entry
                except Exception:
                    continue
        except Exception:
            pass
        if out:
            break
        time.sleep(1.0)
    if out:
        _PITCHER_POWER_CACHE[year] = out
    return out


def _pitcher_power_lookup(player_id) -> dict:
    if not player_id:
        return {}
    from datetime import date as _d
    yr = str(_d.today().year)
    pid = int(player_id)
    cur = _PITCHER_POWER_CACHE.get(yr, {})
    if pid in cur:
        return cur[pid]
    return _PITCHER_POWER_CACHE.get(str(int(yr) - 1), {}).get(pid, {})


def _ip_outs(ip) -> int:
    """MLB innings-pitched string ('5.2' = 5 and 2/3) -> total outs."""
    try:
        s = str(ip)
        if "." in s:
            w, f = s.split(".", 1)
            return int(w) * 3 + int(f[0])
        return int(s) * 3
    except Exception:
        return 0


def _pitcher_hr9(pitcher_id, season) -> dict:
    """Starter HR/9 = a season baseline (current + prior year) blended 60/40 with
    last-5-start recent form. Returns {season_hr9, recent_hr9, blended_hr9, disp}
    or None. Game logs come from the pitcher-K module (lazy import = no cycle)."""
    if not pitcher_id:
        return None
    key = (int(pitcher_id), int(season))
    if key in _PIT_HR9_CACHE:
        return _PIT_HR9_CACHE[key]
    out = None
    try:
        from pitcher_k import _get_pitching_logs
        base_hr = 0
        base_outs = 0
        cur_starts = []
        for _s in (int(season), int(season) - 1):
            for sp in (_get_pitching_logs(int(pitcher_id), _s) or []):
                st = sp.get("stat", {}) or {}
                o  = _ip_outs(st.get("inningsPitched", "0"))
                hr = int(st.get("homeRuns", 0) or 0)
                base_hr  += hr
                base_outs += o
                if _s == int(season) and o >= 9:   # a real start (>=3 IP)
                    cur_starts.append((hr, o))
        season_hr9 = (base_hr * 27.0 / base_outs) if base_outs > 0 else None
        rec = cur_starts[-5:]
        r_hr   = sum(h for h, _ in rec)
        r_outs = sum(o for _, o in rec)
        recent_hr9 = (r_hr * 27.0 / r_outs) if r_outs > 0 else None
        if season_hr9 is not None and recent_hr9 is not None:
            blended = 0.45 * season_hr9 + 0.55 * recent_hr9
        else:
            blended = season_hr9 if season_hr9 is not None else recent_hr9
        if blended is not None:
            out = {"season_hr9": season_hr9, "recent_hr9": recent_hr9,
                   "blended_hr9": blended, "disp": f"{blended:.2f} HR/9"}
    except Exception:
        out = None
    _PIT_HR9_CACHE[key] = out
    return out


def _get_bat_sides_batch(player_ids: list) -> dict:
    """Batched batSide ('L'/'R'/'S') for hitters via one /people call per 100 ids."""
    ids = [int(x) for x in player_ids if x and int(x) not in _BAT_SIDE_CACHE]
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        try:
            r = requests.get("https://statsapi.mlb.com/api/v1/people",
                params={"personIds": ",".join(str(x) for x in chunk)}, timeout=12)
            for p in r.json().get("people", []):
                pid = p.get("id")
                code = ((p.get("batSide") or {}).get("code") or "").upper()
                if pid and code in ("L", "R", "S"):
                    _BAT_SIDE_CACHE[pid] = code
        except Exception:
            continue
    return {int(pid): _BAT_SIDE_CACHE.get(int(pid)) for pid in player_ids if pid}


def _pitch_hand(pitcher_id) -> str:
    """Pitcher throwing hand 'L'/'R', cached. Lazy import of the K-module helper."""
    if not pitcher_id:
        return ""
    pid = int(pitcher_id)
    if pid in _PITCH_HAND_CACHE:
        return _PITCH_HAND_CACHE[pid]
    hand = ""
    try:
        from pitcher_k import _get_pitch_hand
        hand = (_get_pitch_hand(pid) or "").upper()[:1]
    except Exception:
        hand = ""
    _PITCH_HAND_CACHE[pid] = hand
    return hand


def _log(emit, msg, type_="log"):
    if emit:
        emit({"type": type_, "msg": msg})


def _team_nick(s: str) -> str:
    """Canonical team nickname — the unique mascot word that identifies the
    franchise. Two clubs that share a CITY (New York Mets/Yankees, Chicago
    Cubs/White Sox, Los Angeles Dodgers/Angels, San Francisco Giants/San Diego
    Padres) must NEVER match on the shared city word; only the nickname is
    decisive. The lone last-word collision is "sox" (Boston Red Sox vs Chicago
    White Sox), disambiguated by the colour word right before it."""
    w = (s or "").lower().replace(".", "").split()
    if not w:
        return ""
    if w[-1] == "sox" and len(w) >= 2:
        return w[-2] + " sox"
    return w[-1]


def _team_match(a: str, b: str) -> bool:
    a, b = (a or "").lower().strip(), (b or "").lower().strip()
    if not a or not b: return False
    if a == b: return True
    # Match on the canonical NICKNAME only. A bare city/substring overlap
    # ("New York" sits inside BOTH NY clubs) must NOT count as a match — that
    # was the bug that handed a Mets batter a Yankees starter (Will Warren).
    na, nb = _team_nick(a), _team_nick(b)
    return bool(na) and na == nb


def _build_player_map(season: int):
    global _PLAYER_MAP
    if _PLAYER_MAP: return
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/sports/1/players",
            params={"season": season, "gameType": "R"}, timeout=15)
        for p in r.json().get("people", []):
            name = p.get("fullName", "").lower().strip()
            pid  = p.get("id")
            if name and pid:
                _PLAYER_MAP[name] = pid
    except Exception:
        pass


def _resolve_id(name: str):
    key = name.lower().strip()
    if key in _PLAYER_MAP: return _PLAYER_MAP[key]
    parts = key.split()
    if not parts: return None
    last, first = parts[-1], parts[0]
    # Last-name fallback (handles accents/nickname spellings) but ALSO require the
    # first TWO characters to match — one initial isn't enough when two teammates
    # share a surname AND the same starting letter (e.g. Esmerlyn vs Enmanuel Valdez).
    # Two chars: "es" ≠ "en", but "mike"/"michael" still match on "mi", etc.
    for k, v in _PLAYER_MAP.items():
        kp = k.split()
        if not kp: continue
        if kp[-1] == last and kp[0][:2] == first[:2] and abs(len(k) - len(key)) <= 6:
            return v
    return None


def _get_teams_batch(player_ids: list) -> dict:
    if not player_ids: return {}
    result = {}
    # Chunk to keep the personIds URL short — the candidate pool is now the full
    # hit-odds set (~300 players), not just the ~57 on the 1.5 line.
    for i in range(0, len(player_ids), 100):
        chunk = player_ids[i:i + 100]
        try:
            r = requests.get(
                "https://statsapi.mlb.com/api/v1/people",
                params={"personIds": ",".join(str(x) for x in chunk), "hydrate": "currentTeam"},
                timeout=12)
            for p in r.json().get("people", []):
                pid  = p.get("id")
                team = p.get("currentTeam", {}).get("name", "")
                if pid: result[pid] = team
        except Exception:
            continue
    return result


def _get_probable_pitchers(run_date: str) -> dict:
    if run_date in _PITCHER_CACHE: return _PITCHER_CACHE[run_date]
    result = {}
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": run_date,
                    "hydrate": "probablePitcher,team", "gameType": "R"},
            timeout=12)
        for d in r.json().get("dates", []):
            for game in d.get("games", []):
                for side in ("home", "away"):
                    t         = game.get("teams", {}).get(side, {})
                    team_name = t.get("team", {}).get("name", "")
                    pitcher   = t.get("probablePitcher", {})
                    if team_name and pitcher:
                        result[team_name] = {"name": pitcher.get("fullName", "TBD"),
                                             "id":   pitcher.get("id")}
    except Exception:
        pass
    _PITCHER_CACHE[run_date] = result
    return result


_S1_VSP_CACHE: dict = {}

def _get_s1_vs_pitcher(batter_id, pitcher_id) -> dict:
    """Cached wrapper — the same (batter, pitcher) head-to-head is scored across
    many categories AND re-read for the popup display, so memoize the fetch."""
    _ck = (batter_id, pitcher_id)
    if _ck in _S1_VSP_CACHE:
        return _S1_VSP_CACHE[_ck]
    _res = _get_s1_vs_pitcher_uncached(batter_id, pitcher_id)
    _S1_VSP_CACHE[_ck] = _res
    return _res

def _get_s1_vs_pitcher_uncached(batter_id, pitcher_id) -> dict:
    if not batter_id or not pitcher_id:
        return {"ba": None, "display": "N/A", "ab": 0}
    try:
        r = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{batter_id}/stats",
            params={"stats": "vsPlayerTotal", "opposingPlayerId": pitcher_id,
                    "group": "hitting"}, timeout=10)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits: return {"ba": None, "display": "N/A", "ab": 0, "hr": 0}
        stat = splits[0].get("stat", {})
        ab = int(stat.get("atBats", 0) or 0)
        h  = int(stat.get("hits",   0) or 0)
        hr = int(stat.get("homeRuns", 0) or 0)
        if ab == 0: return {"ba": None, "display": "N/A", "ab": 0, "hr": 0}
        ba = h / ab
        return {"ba": ba, "display": f".{int(ba*1000):03d} ({ab}AB)", "ab": ab, "hr": hr}
    except Exception:
        return {"ba": None, "display": "N/A", "ab": 0}


_S1_HA_CACHE: dict = {}

def _get_s1_vs_pitcher_ha(batter_id, pitcher_id) -> dict:
    """Statcast home/away split of a batter's BA vs a SPECIFIC pitcher.
    DISPLAY ONLY — never feeds any gate or score. Groups every PA in the
    matchup by venue via `inning_topbot` (Bot = batter's team batting at home,
    Top = batter on the road). Returns {"home": {ba,ab,h}, "away": {...}};
    a side is omitted when it has no at-bats. An empty/throttled CSV leaves
    both sides empty so callers fall back to the combined-career line."""
    if not batter_id or not pitcher_id:
        return {"home": {}, "away": {}}
    _ck = (batter_id, pitcher_id)
    if _ck in _S1_HA_CACHE:
        return _S1_HA_CACHE[_ck]
    res = {"home": {}, "away": {}}
    try:
        import csv, io
        r = requests.get(
            "https://baseballsavant.mlb.com/statcast_search/csv",
            params={"type": "details", "player_type": "batter",
                    "batters_lookup[]": batter_id, "pitchers_lookup[]": pitcher_id,
                    "hfSea": "2021|2022|2023|2024|2025|2026|", "hfGT": "R|",
                    "all": "true", "min_pitches": "0", "min_results": "0",
                    "group_by": "name", "sort_col": "pitches",
                    "player_event_sort": "api_p_release_speed", "sort_order": "desc"},
            headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
        HIT   = {"single", "double", "triple", "home_run"}
        NONAB = {"walk", "intent_walk", "hit_by_pitch", "sac_fly", "sac_bunt",
                 "catcher_interf", "sac_fly_double_play", "sac_bunt_double_play"}
        agg = {"home": [0, 0], "away": [0, 0]}  # side -> [ab, hits]
        for row in csv.DictReader(io.StringIO(r.text.lstrip("\ufeff"))):
            ev = (row.get("events") or "").strip()
            if not ev:
                continue  # events populated only on the last pitch of a PA
            sd = "home" if (row.get("inning_topbot", "").strip().lower().startswith("bot")) else "away"
            if ev not in NONAB:
                agg[sd][0] += 1
            if ev in HIT:
                agg[sd][1] += 1
        for sd, (ab, h) in agg.items():
            if ab > 0:
                res[sd] = {"ba": h / ab, "ab": ab, "h": h}
    except Exception:
        res = {"home": {}, "away": {}}
    _S1_HA_CACHE[_ck] = res
    return res


def _s1_ha_fields(batter_id, pitcher_id, side, fallback) -> dict:
    """Card fields for the "vs Today's Pitcher" box, venue-matched to today's
    game (DISPLAY ONLY). Returns {"s1_disp","s1_ab","s1_tag"}; falls back to the
    combined-career `fallback` dict (tag "") when Statcast has no rows for that
    venue, so the card is never blank and gates/scoring stay on career."""
    is_home = str(side).upper() == "HOME"
    sd = "home" if is_home else "away"
    d  = _get_s1_vs_pitcher_ha(batter_id, pitcher_id).get(sd) or {}
    if d.get("ab"):
        return {"s1_disp": f".{int(d['ba'] * 1000):03d} ({d['ab']}AB)",
                "s1_ab": d["ab"], "s1_tag": ("Home" if is_home else "Away")}
    fb = fallback or {}
    return {"s1_disp": fb.get("display", "N/A"), "s1_ab": fb.get("ab", 0), "s1_tag": ""}


def _prewarm_s1_ha_cache(pairs: list) -> None:
    """Pre-fetch Statcast venue-split data for a list of (batter_id, pitcher_id)
    pairs using a small controlled pool (3 workers) so the main scoring executor
    never makes a live Statcast network call — every _s1_ha_fields call hits cache.
    Silent on any failure; the per-player fallback in _s1_ha_fields is unchanged."""
    unique = list({(b, p) for b, p in pairs if b})
    if not unique:
        return
    def _fetch(bp):
        try:
            _get_s1_vs_pitcher_ha(bp[0], bp[1])
        except Exception:
            pass
    try:
        with ThreadPoolExecutor(max_workers=3) as _pw:
            list(_pw.map(_fetch, unique))
    except Exception:
        pass


def _get_last7_ba(batter_id) -> dict:
    if not batter_id:
        return {"ba": None, "display": "N/A", "ab": 0}
    try:
        r = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{batter_id}/stats",
            params={"stats": "lastXGames", "group": "hitting",
                    "gameType": "R", "limit": 7}, timeout=10)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits: return {"ba": None, "display": "N/A", "ab": 0}
        stat = splits[0].get("stat", {})
        ab = int(stat.get("atBats", 0) or 0)
        h  = int(stat.get("hits",   0) or 0)
        if ab == 0: return {"ba": None, "display": "0H/0AB", "ab": 0}
        ba = h / ab
        return {"ba": ba, "display": f".{int(ba*1000):03d} ({h}H/{ab}AB)", "ab": ab}
    except Exception:
        return {"ba": None, "display": "N/A", "ab": 0}


def _last10_ba(player_id, side: str, opp_name: str = "", max_games: int = 10) -> dict:
    """BA over the most recent max_games H/A games (up to 5 seasons back), counting
       only games with >=1 AB and matching the side the player is on today. When
       opp_name is set, restrict to games vs THAT team. Returns {ba, display, games}."""
    if not player_id:
        return {"ba": None, "display": "N/A", "games": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        hits = abs_ = g = 0
        done = False
        for season in range(cy, cy - 5, -1):
            for sp in reversed(_get_game_logs(player_id, season)):
                is_home = sp.get("isHome", False)
                if (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                hits += int(stat.get("hits", 0) or 0)
                abs_ += ab
                g += 1
                if g >= max_games:
                    done = True
                    break
            if done:
                break
        if abs_ == 0:
            return {"ba": None, "display": "N/A", "games": 0}
        ba = hits / abs_
        return {"ba": ba, "display": f".{int(ba*1000):03d} ({g}G)", "games": g}
    except Exception:
        return {"ba": None, "display": "N/A", "games": 0}


def _fetch_hits_lines(run_date: str, emit=None) -> list:
    if not ODDS_API_KEY:
        _log(emit, "⚠️  ODDS_API_KEY not set — Under Picks skipped")
        return []

    # Fresh runs odds each call so the in-process scheduler (11/14/17:40 ET) and
    # any next-day run can't serve a first-seen matchup/price. (HIT_ODDS predates
    # this and is left as-is.)
    RUNS_ODDS.clear()
    TB_ODDS.clear()
    TB_OVER_ODDS.clear()
    RBI_ODDS.clear()
    HRR_ODDS.clear()
    WALKS_ODDS.clear()
    HR_ODDS.clear()
    PREFERRED = ["draftkings", "fanduel", "betmgm", "williamhill_us", "caesars", "betrivers", "ballybet", "bet365", "espnbet", "bet99", "thescore", "fliff", "mybookieag", "betonlineag", "bovada"]
    tomorrow  = (time.strftime("%Y-%m-%d",
                  time.gmtime(time.mktime(time.strptime(run_date, "%Y-%m-%d")) + 86400)))
    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY, "dateFormat": "iso"}, timeout=15)
        if r.status_code != 200:
            _log(emit, f"⚠️  Odds API events returned {r.status_code}")
            return []

        def _is_run_date_game(ct: str) -> bool:
            """True if this game belongs to run_date (handles UTC rollover for late PT games)."""
            if not ct: return False
            day = ct[:10]
            if day == run_date: return True
            # Late-night PT games (10pm PT = 1am UTC next day) — cap at 09:00 UTC
            if day == tomorrow:
                try:
                    hour = int(ct[11:13])
                    return hour < 9
                except Exception:
                    return False
            return False
        events = [e for e in r.json() if _is_run_date_game(e.get("commence_time", ""))]
        _log(emit, f"  Odds API: {len(events)} games for {run_date}")
        seen:  dict = {}
        hit05: dict = {}  # nk -> {name, home_team, away_team} for every 0.5-line player

        now_utc = datetime.now(timezone.utc)
        for ev in events:
            ct = ev.get("commence_time", "")
            if ct:
                try:
                    game_start = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                    if game_start < now_utc:
                        continue  # game already started — skip live odds
                except Exception:
                    pass
            home_team = ev.get("home_team", "")
            away_team = ev.get("away_team", "")
            r2 = requests.get(
                f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{ev['id']}/odds",
                params={"apiKey": ODDS_API_KEY, "regions": "us,us2,ca",
                        "markets": "batter_hits,batter_hits_alternate,batter_total_bases,batter_total_bases_alternate,batter_runs_scored,batter_rbis,batter_hits_runs_rbis,batter_walks,batter_home_runs",
                        "oddsFormat": "american"}, timeout=15)
            if r2.status_code != 200: continue
            all_bms = r2.json().get("bookmakers", [])
            # Scan ALL books for both the 0.5 hit odds and the 1.5-line candidates.
            _bm_map = {b.get("key"): b for b in all_bms}
            # Collect 0.5-line Over odds from every bookmaker (first seen per player)
            for bm_any in all_bms:
                _bk = bm_any.get("key")
                for mkt in bm_any.get("markets", []):
                    if mkt.get("key") not in ("batter_hits", "batter_hits_alternate"): continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt is None or side != "Over": continue
                        nk = _norm_name(player)
                        if pt == 0.5 and price is not None:
                            # Displayed 0.5 "to record a hit" Over price: best price
                            # across all books; big books win on ties.
                            _cur = HIT_ODDS.get(nk)
                            _cur_book = HIT_ODDS_BOOK.get(nk)
                            if _cur is None or price > _cur or (price == _cur and _BOOK_PRIORITY.get(_bk, 999) < _BOOK_PRIORITY.get(_cur_book, 999)):
                                HIT_ODDS[nk] = price
                                HIT_ODDS_BOOK[nk] = _bk
                            hit05.setdefault(nk, {"name": player,
                                                  "home_team": home_team,
                                                  "away_team": away_team})
            # Build 1.5-line candidates from ALL books — a player qualifies if ANY
            # book posts his 1.5 line (stops part-time players from blinking in/out
            # based on a single book's coverage). Honor PREFERRED order for the
            # displayed price, and backfill the Under side from a lower-priority
            # book when the preferred book only posts an Over.
            #
            # Aggregation is scoped to THIS event and keyed by normalized name so:
            #   • name-variant spellings across books merge into one candidate, and
            #   • a name appearing in another game can't backfill odds/teams here.
            # Cross-event dedup keeps the first game seen (matches prior behavior).
            ordered_books = ([_bm_map[k] for k in PREFERRED if k in _bm_map]
                             + [b for b in all_bms if b.get("key") not in PREFERRED])
            event_entries: dict = {}
            for book in ordered_books:
                bk = book.get("key")
                for mkt in book.get("markets", []):
                    if mkt.get("key") not in ("batter_hits", "batter_hits_alternate"): continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt != 1.5 or price is None: continue
                        nk = _norm_name(player)
                        if nk in seen: continue  # already locked to an earlier game
                        entry = event_entries.get(nk)
                        if entry is None:
                            entry = {"name": player, "line": 1.5,
                                     "home_team": home_team, "away_team": away_team,
                                     "over_odds": None, "under_odds": None}
                            event_entries[nk] = entry
                        if side == "Over":
                            _take_odds_any(entry, "over_odds", "over_odds_book", price, bk)
                        elif side == "Under":
                            _take_odds_any(entry, "under_odds", "under_odds_book", price, bk)
            # Under 1.5 TOTAL BASES odds for the same players, shown alongside the
            # hits line (pays more because a double/HR busts it even on one hit).
            # Same all-books union + PREFERRED order; first Under price seen wins.
            for book in ordered_books:
                bk = book.get("key")
                for mkt in book.get("markets", []):
                    if mkt.get("key") not in ("batter_total_bases", "batter_total_bases_alternate"): continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt != 1.5 or price is None: continue
                        nk = _norm_name(player)
                        entry = event_entries.get(nk)
                        if entry is None: continue  # TB only attaches to 1.5-hit-line players
                        if side == "Under":
                            _take_odds(entry, "tb_under_odds", "tb_under_odds_book", price, bk)
                        elif side == "Over":
                            _take_odds(entry, "tb_over_odds", "tb_over_odds_book", price, bk)
            for nk, entry in event_entries.items():
                entry.setdefault("tb_under_odds", None)
                entry.setdefault("tb_over_odds", None)
                seen.setdefault(nk, entry)
                if entry.get("tb_under_odds") is not None:
                    TB_ODDS.setdefault(nk, entry)
                if entry.get("tb_over_odds") is not None:
                    TB_OVER_ODDS.setdefault(nk, entry)
            # Batter runs scored (Over/Under, line ~0.5) for the Runs Picks category.
            # Same all-books union + PREFERRED order; first price per side wins, stored
            # in the module-global RUNS_ODDS (parallel to HIT_ODDS), first game seen wins.
            # ZERO extra Odds API calls beyond the one market added to the request above.
            for book in ordered_books:
                bk = book.get("key")
                for mkt in book.get("markets", []):
                    if mkt.get("key") != "batter_runs_scored": continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        # This is the Over/Under 0.5 ("to score a run") market only.
                        if not player or pt != 0.5 or price is None: continue
                        nk = _norm_name(player)
                        entry = RUNS_ODDS.get(nk)
                        if entry is None:
                            entry = {"name": player, "line": pt,
                                     "home_team": home_team, "away_team": away_team,
                                     "over": None, "under": None}
                            RUNS_ODDS[nk] = entry
                        if side == "Over":
                            _take_odds(entry, "over", "over_book", price, bk); entry["line"] = pt
                        elif side == "Under":
                            _take_odds(entry, "under", "under_book", price, bk)

            # Batter RBIs (Over/Under 0.5) for the RBI Picks category.
            # ZERO extra Odds API calls — market added to the same per-game request.
            for book in ordered_books:
                bk = book.get("key")
                for mkt in book.get("markets", []):
                    if mkt.get("key") != "batter_rbis": continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt != 0.5 or price is None: continue
                        nk = _norm_name(player)
                        entry = RBI_ODDS.get(nk)
                        if entry is None:
                            entry = {"name": player, "line": pt,
                                     "home_team": home_team, "away_team": away_team,
                                     "over": None, "under": None}
                            RBI_ODDS[nk] = entry
                        if side == "Over":
                            _take_odds(entry, "over", "over_book", price, bk); entry["line"] = pt
                        elif side == "Under":
                            _take_odds(entry, "under", "under_book", price, bk)

            # Batter HRR (Hits+Runs+RBI, Over/Under 1.5) — ZERO extra Odds API calls.
            for book in ordered_books:
                bk = book.get("key")
                for mkt in book.get("markets", []):
                    if mkt.get("key") != "batter_hits_runs_rbis": continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt != 1.5 or price is None: continue
                        nk = _norm_name(player)
                        entry = HRR_ODDS.get(nk)
                        if entry is None:
                            entry = {"name": player, "line": 1.5,
                                     "home_team": home_team, "away_team": away_team,
                                     "hrr_over_odds": None, "hrr_under_odds": None}
                            HRR_ODDS[nk] = entry
                        if side == "Over":
                            _take_odds(entry, "hrr_over_odds", "hrr_over_odds_book", price, bk)
                        elif side == "Under":
                            _take_odds(entry, "hrr_under_odds", "hrr_under_odds_book", price, bk)

            # Batter Walks (Over/Under 0.5) for the Batter Walks category.
            # ZERO extra Odds API calls — market added to the same per-game request.
            # Distinct from the PITCHER walks (Walks Allowed) market.
            # Best price preferring MY_BOOKS is handled by _take_odds, so iterate
            # plain ordered_books (no special bet99-first reordering needed).
            for book in ordered_books:
                bk = book.get("key")
                for mkt in book.get("markets", []):
                    if mkt.get("key") != "batter_walks": continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt != 0.5 or price is None: continue
                        nk = _norm_name(player)
                        entry = WALKS_ODDS.get(nk)
                        if entry is None:
                            entry = {"name": player, "line": pt,
                                     "home_team": home_team, "away_team": away_team,
                                     "over": None, "under": None}
                            WALKS_ODDS[nk] = entry
                        if side == "Over":
                            _take_odds(entry, "over", "over_book", price, bk); entry["line"] = pt
                        elif side == "Under":
                            _take_odds(entry, "under", "under_book", price, bk)

            # Batter Home Runs (Over/Under 0.5) for the HR Picks category.
            # ZERO extra Odds API calls — market added to the same per-game request.
            for book in ordered_books:
                bk = book.get("key")
                for mkt in book.get("markets", []):
                    if mkt.get("key") != "batter_home_runs": continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt != 0.5 or price is None: continue
                        nk = _norm_name(player)
                        entry = HR_ODDS.get(nk)
                        if entry is None:
                            entry = {"name": player, "line": pt,
                                     "home_team": home_team, "away_team": away_team,
                                     "over": None, "under": None}
                            HR_ODDS[nk] = entry
                        if side == "Over":
                            _take_odds(entry, "over", "over_book", price, bk); entry["line"] = pt
                        elif side == "Under":
                            _take_odds(entry, "under", "under_book", price, bk)

        _log(emit, f"  ✅ {len(seen)} players on 1.5 hits line | {len(HIT_ODDS)} players with 0.5 hit odds | {len(RUNS_ODDS)} with runs odds | {len(TB_ODDS)} with TB under odds | {len(RBI_ODDS)} with RBI odds | {len(HRR_ODDS)} with HRR odds | {len(WALKS_ODDS)} with walks odds | {len(HR_ODDS)} with HR odds")
        # Scan ALL players who have any posted hit odds (the 0.5 set), not just
        # the ~57 with a 1.5 line. Players who DO have a 1.5 line keep their
        # Under 1.5 / total-bases odds; 0.5-only players are still evaluated as
        # potential unders but carry no 1.5/TB price (no book posted one). This
        # adds ZERO Odds API calls — both lines come from the per-game odds
        # already fetched above; only the (free) MLB Stats scan grows.
        candidates = list(seen.values())
        # Export the full 0.5-hit-line set (with team info) so run_hit_picks can
        # build pool B. Refreshed each call to match this run's slate.
        HIT_TEAMS.clear()
        HIT_TEAMS.update(hit05)
        for nk, info in hit05.items():
            if nk in seen: continue
            candidates.append({"name": info["name"], "line": 1.5,
                               "home_team": info["home_team"], "away_team": info["away_team"],
                               "over_odds": None, "under_odds": None, "tb_under_odds": None})
        _log(emit, f"  ▸ Scanning {len(candidates)} players for unders (was {len(seen)} on the 1.5 line)")
        return candidates
    except Exception as exc:
        _log(emit, f"⚠️  Odds API error: {exc}")
        return []


def run_under_picks(run_date: str, team_schedule: dict, emit=None,
                    top_era=None, top_era_list=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ Under Picks — Fetching 1.5 hits lines from The Odds API", "section")
    season = int(run_date[:4])

    candidates = _fetch_hits_lines(run_date, emit)
    if not candidates: return []

    _log(emit, "  Loading probable pitchers…")
    pitchers = _get_probable_pitchers(run_date)
    _log(emit, f"  ✅ {len(pitchers)} probable pitchers found")

    _log(emit, "  Building MLB player ID map…")
    _build_player_map(season)
    _log(emit, f"  ✅ {len(_PLAYER_MAP)} active players indexed")

    # Pre-load batter Savant xBA + hard-hit% for quality-of-contact ranking signal.
    _yr_u = run_date[:4]
    _fetch_batter_savant_up(_yr_u)
    _fetch_batter_savant_up(str(int(_yr_u) - 1))

    id_map: dict = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid: id_map[c["name"]] = pid

    _log(emit, f"  Looking up teams for {len(id_map)} players…")
    team_map = _get_teams_batch(list(id_map.values()))
    _log(emit, "  ✅ Teams resolved")
    _log(emit, f"  Evaluating {len(candidates)} candidates…")

    # Pre-warm Statcast venue-split cache (3 workers) before the main executor
    # fires so worker threads never make a live network call.
    _pw_pairs = []
    for _c in candidates:
        _bid = id_map.get(_c["name"])
        _pt  = team_map.get(_bid, "") if _bid else ""
        if not _bid or not _pt: continue
        if _team_match(_pt, _c["home_team"]):   _opp = _c["away_team"]
        elif _team_match(_pt, _c["away_team"]): _opp = _c["home_team"]
        else: continue
        _pid = next((pi.get("id") for pt, pi in pitchers.items() if _team_match(pt, _opp)), None)
        _pw_pairs.append((_bid, _pid))
    _prewarm_s1_ha_cache(_pw_pairs)

    # Evaluate candidates in parallel (≤8 threads). Each worker is independent —
    # it does up to 4 MLB Stats API calls with short-circuit filters — and the
    # shared splits _CACHE / id maps are GIL-safe (worst case = harmless dup
    # fetch). Logs are emitted from the main thread as futures complete so the
    # live progress feed stays intact.
    def _eval_candidate(c):
        name      = c["name"]
        home_team = c["home_team"]
        away_team = c["away_team"]
        batter_id   = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team: return None
        if _team_match(player_team, home_team):
            side, opp_name = "HOME", away_team
        elif _team_match(player_team, away_team):
            side, opp_name = "AWAY", home_team
        else:
            return None
        pitcher_name, pitcher_id = "TBD", None
        for pteam, pinfo in pitchers.items():
            if _team_match(pteam, opp_name):
                pitcher_name = pinfo["name"]
                pitcher_id   = pinfo.get("id")
                break
        # Facing a top-30 ERA ace is DISPLAY ONLY — it shows a chip on the card
        # but does NOT bypass or affect any qualification gate.
        def _plast(nm):
            nm = (nm or "").strip()
            if not nm or nm.upper() == "TBD": return ""
            return (nm.split(".")[-1] if "." in nm else nm.split()[-1]).strip().lower()
        p_last = _plast(pitcher_name)
        ace = bool(top_era) and p_last != "" and p_last in top_era
        ace_era = None
        if ace and top_era_list:
            ace_era = next((q["era"] for q in top_era_list
                            if q.get("name", "").lower().endswith(p_last)), None)
        s1 = _get_s1_vs_pitcher(batter_id, pitcher_id)
        # S1: career BA vs today's pitcher. N/A / 0 AB passes. DQ if >= .250.
        if s1["ba"] is not None and s1["ab"] > 0 and s1["ba"] >= 0.250: return None
        # S2: H/A games vs TODAY'S opponent. Data required AND < .250.
        s2 = _last10_ba(batter_id, side, opp_name, 10)
        if s2["ba"] is None or s2["ba"] >= 0.250: return None
        # S3: H/A games vs ANY opponent. Data required AND < .250.
        s3 = _last10_ba(batter_id, side, "", 10)
        if s3["ba"] is None or s3["ba"] >= 0.250: return None
        # L7: last 7 games (general). N/A passes; DQ if >= .250.
        l7 = _get_last7_ba(batter_id)
        if l7["ba"] is not None and l7["ba"] >= 0.250: return None
        # Coldest first: lower under_score ranks higher.
        def _ba(x, fb=0.250): return x["ba"] if x and x["ba"] is not None else fb
        l7_ba = l7["ba"] if l7["ba"] is not None else _ba(s3, _ba(s1))
        under_score = round((_ba(s2) + _ba(s3) + l7_ba) * 1000)
        # Quality-of-contact penalty: hard-hitters are harder to fade — push them down the board.
        _sav_u = _batter_sav_lookup_up(batter_id)
        if _sav_u.get("hard_hit_pct") is not None:
            under_score += int(max(-20, min(30, round((_sav_u["hard_hit_pct"] - LEAGUE_HARD_HIT_UP) * 1.5))))
        return {"name": name, "team": player_team, "pos": "—", "side": side, "opp": opp_name,
                "pitcher": pitcher_name, **_s1_ha_fields(batter_id, pitcher_id, side, s1),
                "s2": s2, "s3": s3, "l7": l7,
                "lineup_status": "TBD", "under_score": under_score,
                "batter_id": batter_id, "under_basis": "vs-ace" if ace else "recent",
                "ace_era": ace_era,
                "xba": _sav_u.get("xba"), "hard_hit_pct": _sav_u.get("hard_hit_pct"),
                "under_odds": c.get("under_odds"), "over_odds": c.get("over_odds"),
                "tb_under_odds": c.get("tb_under_odds"),
                "book": _book_label(c.get("under_odds_book"))}

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval_candidate, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pick = _fut.result()
            except Exception as _exc:
                pick = None
                _log(emit, f"  ⚠️ {_futs[_fut].get('name', '?')} — eval failed: {_exc}")
            if pick:
                picks.append(pick)
                _log(emit, f"  ✅ UNDER: {pick['name']:<22}  S1:{pick['s1_disp']:<14}  S2:{pick['s2']['display']}  S3:{pick['s3']['display']}")

    # Sort coldest first; name as a deterministic tie-breaker since workers now
    # finish out of order.
    picks.sort(key=lambda x: (x["under_score"], x["name"]))
    _log(emit, f"✅ Under Picks: {len(picks)} picks found")
    return picks


# ── Runs Picks (Batter Runs Scored, Over/Under 0.5) ────────────────────────
# A full over/under category mirroring the hit list, driven by how often a batter
# scores a run (H/A vs the opponent, falling back to L10 H/A any opp when there's
# no head-to-head sample). Ranked by the Wilson lower bound of that rate so a proven
# sample outranks a thin lucky one. Odds come from RUNS_ODDS, populated by
# _fetch_hits_lines (no extra Odds API calls).
import math as _math

def _wilson_lb(hits: int, games: int, z: float = 1.96) -> float:
    """Lower bound of a 95% Wilson interval — rewards sample size."""
    if not games:
        return 0.0
    p = hits / games
    den = 1.0 + z * z / games
    centre = p + z * z / (2 * games)
    margin = z * _math.sqrt(p * (1 - p) / games + z * z / (4 * games * games))
    return (centre - margin) / den


def _runs_consistency(player_id, side: str, opp_name: str = "",
                      max_games: int = 10, ignore_ha: bool = False) -> dict:
    """Last max_games career H/A games (5 seasons back) counting games with 1+ run.
       When opp_name is set, restrict to games vs THAT opponent.
       ignore_ha=True drops the H/A filter (true recent form, any side)."""
    if not player_id:
        return {"runs_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        seasons = list(range(cy, cy - 5, -1))
        matching = []
        for season in seasons:
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                is_home = sp.get("isHome", False)
                if not ignore_ha and (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                runs = int(stat.get("runs", 0) or 0)
                matching.append(1 if runs >= 1 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        runs_games = sum(matching)
        if games == 0:
            return {"runs_games": 0, "games": 0, "display": "N/A", "score": 0}
        return {"runs_games": runs_games, "games": games,
                "display": f"{runs_games}/{games}",
                "score": round(runs_games / games * 100)}
    except Exception:
        return {"runs_games": 0, "games": 0, "display": "ERR", "score": 0}


def _runs_rate(player_id, side: str, opp_name: str) -> dict:
    """Runs-scored rate vs THIS opponent (H/A); fall back to L10 H/A any opp when
       there's no head-to-head sample. Returns the consistency dict + a `basis`."""
    vs = _runs_consistency(player_id, side, opp_name, 10)
    if vs["games"] > 0:
        vs["basis"] = "vs opp"
        return vs
    la = _runs_consistency(player_id, side, "", 10)
    la["basis"] = "L10 H/A"
    return la


def _recent_runs_log(player_id, n: int = 5) -> list:
    """Last n games (any opp), newest-first: date, runs, hits, opp, H/A."""
    if not player_id:
        return []
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        games = []
        for season in range(cy, cy - 2, -1):
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                games.append({
                    "d":   (sp.get("date") or "")[5:],
                    "r":   int(stat.get("runs", 0) or 0),
                    "h":   int(stat.get("hits", 0) or 0),
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


# Pick qualifies as OVER when the runs-scored rate is high, UNDER when low.
RUNS_OVER_CUT  = 60   # >= this % → likely to score a run (vs opp)
RUNS_UNDER_CUT = 30   # <= this % → likely NOT to score (vs opp)
RUNS_MIN_VS    = 2    # vs-opp games to use the head-to-head anchor
RUNS_MIN_ANY   = 5    # else fall back to L10 H/A any-opp with >= this many games
RUNS_TOP_N     = 20   # cap per side (top N overs / top N unders)


def run_runs_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ Runs Picks — Batter Runs Scored (Over/Under 0.5)", "section")
    season = int(run_date[:4])

    if not RUNS_ODDS:
        _fetch_hits_lines(run_date, emit)   # populates RUNS_ODDS as a side effect
    candidates = list(RUNS_ODDS.values())
    if not candidates:
        _log(emit, "  No batter runs-scored lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with a runs line")

    _build_player_map(season)
    id_map = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))
    pitchers = _get_probable_pitchers(run_date)

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        pitcher_name, pitcher_id = "TBD", None
        for pteam, pinfo in pitchers.items():
            if _team_match(pteam, opp_name):
                pitcher_name = pinfo["name"]
                pitcher_id   = pinfo.get("id")
                break
        s1_pit = _get_s1_vs_pitcher(batter_id, pitcher_id)
        # vs-opp preferred (>=RUNS_MIN_VS games); else L10 H/A any-opp (>=RUNS_MIN_ANY).
        _vsop = _runs_consistency(batter_id, side, opp_name, 10)
        if _vsop["games"] >= RUNS_MIN_VS:
            rate = _vsop; rate["basis"] = "vs opp"
        else:
            rate = _runs_consistency(batter_id, side, "", 10)
            if rate["games"] < RUNS_MIN_ANY:
                return None
            rate["basis"] = "L10 H/A"
        # Card shows BOTH: head-to-head (vs this opp, H/A) AND last-10 H/A any-opp.
        _HH = _vsop
        _L10D = rate["display"] if rate.get("basis") == "L10 H/A" else _runs_consistency(batter_id, side, "", 10)["display"]
        # 3-window convergence blend: vs-opp 35%, L10 any-opp 40%, L5 any-opp 25%
        r10 = _runs_consistency(batter_id, side, "", 10, ignore_ha=True)
        r5  = _runs_consistency(batter_id, side, "", 5, ignore_ha=True)
        comps = [(0.35, rate["score"] / 100.0)]
        if r10["games"] > 0: comps.append((0.40, r10["score"] / 100.0))
        if r5["games"]  > 0: comps.append((0.25, r5["score"]  / 100.0))
        wsum = sum(w for w, _ in comps)
        blend_score = round(sum(w * v for w, v in comps) / wsum * 100)
        # OVERS only: hot hand (recent power + active hit streak) nudges a hitter
        # over the over-cut. Unders use the plain blend (bonus is always >= 0).
        hot = _hitter_hot_hand(batter_id)
        over_score = min(blend_score + hot["bonus"], 100)
        if over_score >= RUNS_OVER_CUT:
            pick = "OVER"
            final_score = over_score
        elif blend_score <= RUNS_UNDER_CUT:
            pick = "UNDER"
            final_score = blend_score
        else:
            return None
        l5_s = r5["score"] if r5["games"] > 0 else None
        conv_flag = all((pick == "OVER" and v >= RUNS_OVER_CUT) or
                        (pick == "UNDER" and v <= RUNS_UNDER_CUT)
                        for v in [rate["score"], r10["score"] if r10["games"] > 0 else None, l5_s]
                        if v is not None)
        cold_flag = ((pick == "OVER"  and l5_s is not None and l5_s <= RUNS_UNDER_CUT) or
                     (pick == "UNDER" and l5_s is not None and l5_s >= RUNS_OVER_CUT))
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": pick, "line": c.get("line", 0.5),
                "rate_disp": rate["display"], "score": final_score, "base_score": blend_score,
                "opp_score": rate["score"], "recent_l10": r10["display"], "recent_l5": r5["display"],
                "h2h_disp": _HH["display"], "h2h_games": _HH["games"], "l10_disp": _L10D,
                "games": rate["games"], "basis": rate.get("basis", ""),
                "conv_flag": conv_flag, "cold_flag": cold_flag,
                "wilson": round(_wilson_lb(rate["runs_games"], rate["games"]), 4),
                "hot_bonus": hot["bonus"] if pick == "OVER" else 0,
                "hot_disp": hot["disp"] if pick == "OVER" else "",
                "over_odds": c.get("over"), "under_odds": c.get("under"),
                "book": _book_label(c.get("over_book") if pick == "OVER" else c.get("under_book")),
                "batter_id": batter_id,
                "pitcher": pitcher_name, **_s1_ha_fields(batter_id, pitcher_id, side, s1_pit),
                "recent_runs_log": _recent_runs_log(batter_id)}

    _pw_pairs = []
    for _c in candidates:
        _bid = id_map.get(_c["name"])
        _pt  = team_map.get(_bid, "") if _bid else ""
        if not _bid or not _pt: continue
        if _team_match(_pt, _c["home_team"]):   _opp = _c["away_team"]
        elif _team_match(_pt, _c["away_team"]): _opp = _c["home_team"]
        else: continue
        _pid = next((pi.get("id") for pt, pi in pitchers.items() if _team_match(pt, _opp)), None)
        _pw_pairs.append((_bid, _pid))
    _prewarm_s1_ha_cache(_pw_pairs)

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    # OVERs first (highest confidence-adjusted rate), then UNDERs (coldest first).
    picks.sort(key=lambda p: (
        0 if p["pick"] == "OVER" else 1,
        -p["score"] if p["pick"] == "OVER" else p["score"],
        -p["games"],
    ))
    # Cap to the top RUNS_TOP_N on each side (overs / unders).
    overs  = [p for p in picks if p["pick"] == "OVER"][:RUNS_TOP_N]
    unders = [p for p in picks if p["pick"] == "UNDER"][:RUNS_TOP_N]
    picks = overs + unders
    _log(emit, f"✅ Runs Picks: {len(picks)} "
               f"({sum(1 for p in picks if p['pick']=='OVER')} over / "
               f"{sum(1 for p in picks if p['pick']=='UNDER')} under)")
    return picks


# ── RBI Picks (Batter RBIs, Over/Under 0.5) ────────────────────────────────
# Full over/under category: OVER when batter drives in runs at ≥60% H/A vs opp,
# UNDER when ≤30%. Vs-opp only (min 3 games). Ranked by Wilson lower bound.
# Odds from RBI_ODDS (batter_rbis market), zero extra Odds API calls.

RBI_OVER_CUT  = 60   # >= this % → likely to drive in a run
RBI_UNDER_CUT = 30   # <= this % → likely NOT to drive in a run
RBI_MIN_VS  = 2    # vs-opp games to use the head-to-head anchor
RBI_MIN_ANY = 5    # else fall back to L10 H/A any-opp with >= this many games
RBI_TOP_N       = 20   # cap per side (OVER)
RBI_UNDER_TOP_N = 30   # UNDER cap: top 10 + 20 overflow (Unders only)


def _rbi_consistency(player_id, side: str, opp_name: str = "",
                     max_games: int = 10, ignore_ha: bool = False) -> dict:
    """Last max_games career H/A games counting games with 1+ RBI.
       ignore_ha=True drops the H/A filter (true recent form, any side)."""
    if not player_id:
        return {"rbi_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        seasons = list(range(cy, cy - 5, -1))
        matching = []
        for season in seasons:
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                is_home = sp.get("isHome", False)
                if not ignore_ha and (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                rbi = int(stat.get("rbi", 0) or 0)
                matching.append(1 if rbi >= 1 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        rbi_games = sum(matching)
        if games == 0:
            return {"rbi_games": 0, "games": 0, "display": "N/A", "score": 0}
        return {"rbi_games": rbi_games, "games": games,
                "display": f"{rbi_games}/{games}",
                "score": round(rbi_games / games * 100)}
    except Exception:
        return {"rbi_games": 0, "games": 0, "display": "ERR", "score": 0}


def _rbi_rate(player_id, side: str, opp_name: str) -> dict:
    """RBI rate vs THIS opponent (H/A); vs-opp only (no fallback)."""
    vs = _rbi_consistency(player_id, side, opp_name, 10)
    vs["basis"] = "vs opp"
    return vs


def _recent_rbi_log(player_id, n: int = 5) -> list:
    """Last n games (any opp), newest-first: date, rbi, hits, opp, H/A."""
    if not player_id:
        return []
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        games = []
        for season in range(cy, cy - 2, -1):
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                games.append({
                    "d":   (sp.get("date") or "")[5:],
                    "rbi": int(stat.get("rbi", 0) or 0),
                    "h":   int(stat.get("hits", 0) or 0),
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


# Hitter hot-hand bonus (OVER side only): recent power (HRs) + an active hit streak
# nudge a hot hitter over the over-cut they'd otherwise just miss. RBI / Runs / HRR /
# TB-Over all travel with HRs and hitting streaks. From game logs already pulled —
# zero extra API calls. Unders ignore this entirely (bonus is always >= 0).
RBI_HOT_WINDOW     = 10   # recent games to scan (any opp)
RBI_HOT_HR_PTS     = 4    # bonus points per HR in the window (power)
RBI_HOT_HR_MAX     = 8    # cap on the power bonus
RBI_HOT_STREAK_HI  = 5    # active hit streak >= this -> full streak bonus
RBI_HOT_STREAK_LO  = 3    # active hit streak >= this -> partial streak bonus
RBI_HOT_STREAK_PTS = 5    # full streak bonus (partial = 3)


def _hitter_hot_hand(player_id, n: int = RBI_HOT_WINDOW) -> dict:
    """Recent power/contact signal for hitter OVER picks only. Last n games (any opp).
       Returns recent HR total, active hit-streak length, the point bonus, and a
       short reason string. Bonus is always >= 0 (only ever helps an over)."""
    blank = {"hr": 0, "streak": 0, "bonus": 0, "disp": ""}
    if not player_id:
        return blank
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        rows = []
        for season in range(cy, cy - 2, -1):
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                stat = sp.get("stat", {})
                if int(stat.get("atBats", 0) or 0) < 1:
                    continue
                rows.append((int(stat.get("homeRuns", 0) or 0),
                             int(stat.get("hits", 0) or 0)))
                if len(rows) >= n:
                    break
            if len(rows) >= n:
                break
        if not rows:
            return blank
        recent_hr = sum(hr for hr, _ in rows)
        streak = 0
        for _hr, h in rows:            # rows are newest-first
            if h >= 1:
                streak += 1
            else:
                break
        power_bonus = min(recent_hr * RBI_HOT_HR_PTS, RBI_HOT_HR_MAX)
        if streak >= RBI_HOT_STREAK_HI:
            streak_bonus = RBI_HOT_STREAK_PTS
        elif streak >= RBI_HOT_STREAK_LO:
            streak_bonus = 3
        else:
            streak_bonus = 0
        parts = []
        if recent_hr:
            parts.append(f"{recent_hr} HR L{len(rows)}")
        if streak >= RBI_HOT_STREAK_LO:
            parts.append(f"{streak}-game hit streak")
        return {"hr": recent_hr, "streak": streak,
                "bonus": power_bonus + streak_bonus,
                "disp": ", ".join(parts)}
    except Exception:
        return blank


def run_rbi_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ RBI Picks — Batter RBIs (Over/Under 0.5)", "section")
    season = int(run_date[:4])

    if not RBI_ODDS:
        _fetch_hits_lines(run_date, emit)   # populates RBI_ODDS as a side effect
    candidates = list(RBI_ODDS.values())
    if not candidates:
        _log(emit, "  No batter RBI lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with an RBI line")

    _build_player_map(season)
    id_map = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))
    pitchers = _get_probable_pitchers(run_date)

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        pitcher_name, pitcher_id = "TBD", None
        for pteam, pinfo in pitchers.items():
            if _team_match(pteam, opp_name):
                pitcher_name = pinfo["name"]
                pitcher_id   = pinfo.get("id")
                break
        s1_pit = _get_s1_vs_pitcher(batter_id, pitcher_id)
        # vs-opp preferred (>=RBI_MIN_VS games); else L10 H/A any-opp (>=RBI_MIN_ANY).
        _vsop = _rbi_consistency(batter_id, side, opp_name, 10)
        if _vsop["games"] >= RBI_MIN_VS:
            rate = _vsop; rate["basis"] = "vs opp"
        else:
            rate = _rbi_consistency(batter_id, side, "", 10)
            if rate["games"] < RBI_MIN_ANY:
                return None
            rate["basis"] = "L10 H/A"
        # Card shows BOTH: head-to-head (vs this opp, H/A) AND last-10 H/A any-opp.
        _HH = _vsop
        _L10D = rate["display"] if rate.get("basis") == "L10 H/A" else _rbi_consistency(batter_id, side, "", 10)["display"]
        # 3-window convergence blend: vs-opp 35%, L10 any-opp 40%, L5 any-opp 25%
        r10 = _rbi_consistency(batter_id, side, "", 10, ignore_ha=True)
        r5  = _rbi_consistency(batter_id, side, "", 5, ignore_ha=True)
        comps = [(0.35, rate["score"] / 100.0)]
        if r10["games"] > 0: comps.append((0.40, r10["score"] / 100.0))
        if r5["games"]  > 0: comps.append((0.25, r5["score"]  / 100.0))
        wsum = sum(w for w, _ in comps)
        blend_score = round(sum(w * v for w, v in comps) / wsum * 100)
        # RBI OVERS only: a hot hand (recent power + active hit streak) nudges a
        # hitter over the over-cut they'd otherwise just miss. Unders use the
        # plain blend (bonus is >= 0, so it never pulls anyone toward an under).
        hot = _hitter_hot_hand(batter_id)
        over_score = min(blend_score + hot["bonus"], 100)
        if over_score >= RBI_OVER_CUT:
            pick = "OVER"
            final_score = over_score
        elif blend_score <= RBI_UNDER_CUT:
            pick = "UNDER"
            final_score = blend_score
        else:
            return None
        l5_s = r5["score"] if r5["games"] > 0 else None
        conv_flag = all((pick == "OVER" and v >= RBI_OVER_CUT) or
                        (pick == "UNDER" and v <= RBI_UNDER_CUT)
                        for v in [rate["score"], r10["score"] if r10["games"] > 0 else None, l5_s]
                        if v is not None)
        cold_flag = ((pick == "OVER"  and l5_s is not None and l5_s <= RBI_UNDER_CUT) or
                     (pick == "UNDER" and l5_s is not None and l5_s >= RBI_OVER_CUT))
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": pick, "line": c.get("line", 0.5),
                "rate_disp": rate["display"], "score": final_score, "base_score": blend_score,
                "opp_score": rate["score"], "recent_l10": r10["display"], "recent_l5": r5["display"],
                "h2h_disp": _HH["display"], "h2h_games": _HH["games"], "l10_disp": _L10D,
                "games": rate["games"], "basis": rate.get("basis", ""),
                "conv_flag": conv_flag, "cold_flag": cold_flag,
                "wilson": round(_wilson_lb(rate["rbi_games"], rate["games"]), 4),
                "over_odds": c.get("over"), "under_odds": c.get("under"),
                "book": _book_label(c.get("over_book") if pick == "OVER" else c.get("under_book")),
                "batter_id": batter_id,
                "pitcher": pitcher_name, **_s1_ha_fields(batter_id, pitcher_id, side, s1_pit),
                "hot_bonus": hot["bonus"] if pick == "OVER" else 0,
                "hot_disp": hot["disp"] if pick == "OVER" else "",
                "recent_rbi_log": _recent_rbi_log(batter_id)}

    _pw_pairs = []
    for _c in candidates:
        _bid = id_map.get(_c["name"])
        _pt  = team_map.get(_bid, "") if _bid else ""
        if not _bid or not _pt: continue
        if _team_match(_pt, _c["home_team"]):   _opp = _c["away_team"]
        elif _team_match(_pt, _c["away_team"]): _opp = _c["home_team"]
        else: continue
        _pid = next((pi.get("id") for pt, pi in pitchers.items() if _team_match(pt, _opp)), None)
        _pw_pairs.append((_bid, _pid))
    _prewarm_s1_ha_cache(_pw_pairs)

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    picks.sort(key=lambda p: (
        0 if p["pick"] == "OVER" else 1,
        -p["score"] if p["pick"] == "OVER" else p["score"],
        -p["games"],
    ))
    overs  = [p for p in picks if p["pick"] == "OVER"][:RBI_TOP_N]
    unders = [p for p in picks if p["pick"] == "UNDER"][:RBI_UNDER_TOP_N]
    picks = overs + unders
    _log(emit, f"✅ RBI Picks: {len(picks)} "
               f"({sum(1 for p in picks if p['pick']=='OVER')} over / "
               f"{sum(1 for p in picks if p['pick']=='UNDER')} under)")
    return picks


# ── HR Picks (Batter Home Runs, Over/Under 0.5) ────────────────────────────
# Two-sided category. Blended HR likelihood = recent form 50% + vs-pitcher 30%
# + vs-team 20% (weights renormalized over whatever components have data), each
# component shrunk toward the league base rate so tiny samples can't run away.
# Recent form gets a "bunch" bump when the hitter homered in his last 3 games
# (homers come in bunches). OVER when blended >= HR_OVER_CUT %, UNDER when
# <= HR_UNDER_CUT %. Odds from HR_ODDS (batter_home_runs), zero extra API calls.

HR_OVER_CUT  = 20     # blended HR% >= this → OVER (likely to homer)
HR_UNDER_CUT = 8      # blended HR% <= this → UNDER (very unlikely to homer) [legacy, unused]
HR_UNDER_MAX_JUICE = -500  # HR UNDER juice cap: keep unders priced -500..-100 (real
                           # power hitters the market won't homer); drop deeper juice
                           # (-600/-2000 scrubs nobody plays). Tunable. American odds.
HR_TOP_N     = 20     # cap per side (top 10 on cards + 11-20 overflow)
HR_LG_PA     = 0.035  # league HR per plate appearance (vs-pitcher prior)
HR_LG_PG     = 0.11   # league HR per game for a rostered hitter (recent/team prior)


def _hr_recent(player_id, n: int = 15) -> dict:
    """Last n games (any opp, any side, >=1 AB): games with 1+ HR + a 'bunch'
       flag (homered in the last 3 games). Newest-first over up to 2 seasons."""
    if not player_id:
        return {"hr_games": 0, "games": 0, "rate": None, "bunch": False, "display": "N/A"}
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        flags = []
        for season in range(cy, cy - 2, -1):
            for sp in reversed(_get_game_logs(player_id, season)):
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                hr = int(stat.get("homeRuns", 0) or 0)
                flags.append(1 if hr >= 1 else 0)
                if len(flags) >= n:
                    break
            if len(flags) >= n:
                break
        games = len(flags)
        hr_games = sum(flags)
        if games == 0:
            return {"hr_games": 0, "games": 0, "rate": None, "bunch": False, "display": "N/A"}
        return {"hr_games": hr_games, "games": games,
                "rate": hr_games / games, "bunch": sum(flags[:3]) >= 1,
                "display": f"{hr_games}/{games}"}
    except Exception:
        return {"hr_games": 0, "games": 0, "rate": None, "bunch": False, "display": "ERR"}


def _hr_vs_team(player_id, side: str, opp_name: str, max_games: int = 10) -> dict:
    """Last max_games career H/A games vs opp_name: games with 1+ HR."""
    if not player_id:
        return {"hr_games": 0, "games": 0, "rate": None, "display": "N/A"}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        flags = []
        for season in range(cy, cy - 5, -1):
            for sp in reversed(_get_game_logs(player_id, season)):
                is_home = sp.get("isHome", False)
                if (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                hr = int(stat.get("homeRuns", 0) or 0)
                flags.append(1 if hr >= 1 else 0)
                if len(flags) >= max_games:
                    break
            if len(flags) >= max_games:
                break
        games = len(flags)
        hr_games = sum(flags)
        if games == 0:
            return {"hr_games": 0, "games": 0, "rate": None, "display": "N/A"}
        return {"hr_games": hr_games, "games": games,
                "rate": hr_games / games, "display": f"{hr_games}/{games}"}
    except Exception:
        return {"hr_games": 0, "games": 0, "rate": None, "display": "ERR"}


def _recent_hr_log(player_id, n: int = 8) -> list:
    """Last n games (any opp), newest-first: date, hr, hits, opp, H/A."""
    if not player_id:
        return []
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        games = []
        for season in range(cy, cy - 2, -1):
            for sp in reversed(_get_game_logs(player_id, season)):
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                games.append({
                    "d":   (sp.get("date") or "")[5:],
                    "hr":  int(stat.get("homeRuns", 0) or 0),
                    "h":   int(stat.get("hits", 0) or 0),
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


def run_hr_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ HR Picks — Batter Home Runs (Over/Under 0.5)", "section")
    season = int(run_date[:4])

    if not HR_ODDS:
        _fetch_hits_lines(run_date, emit)   # populates HR_ODDS as a side effect
    candidates = list(HR_ODDS.values())
    if not candidates:
        _log(emit, "  No batter HR lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with an HR line")

    _build_player_map(season)
    id_map = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))
    pitchers = _get_probable_pitchers(run_date)

    # HR model inputs — fetched/cached ONCE per run: Statcast power (batter +
    # pitcher contact-allowed) for this season + prior fallback, batter
    # handedness (one batched call), and each probable starter's throwing hand.
    _fetch_batter_power(str(season));  _fetch_batter_power(str(season - 1))
    _fetch_pitcher_power(str(season)); _fetch_pitcher_power(str(season - 1))
    bat_sides = _get_bat_sides_batch(list(id_map.values()))
    pit_hand_map: dict = {}
    for _pinfo in pitchers.values():
        _ppid = _pinfo.get("id")
        if _ppid and _ppid not in pit_hand_map:
            pit_hand_map[_ppid] = _pitch_hand(_ppid)

    def _shrunk_pg(hr_games, games, k=4):
        if not games:
            return None
        return (hr_games + k * HR_LG_PG) / (games + k)

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        pitcher_name, pitcher_id = "TBD", None
        for pteam, pinfo in pitchers.items():
            if _team_match(pteam, opp_name):
                pitcher_name = pinfo["name"]
                pitcher_id   = pinfo.get("id")
                break

        recent = _hr_recent(batter_id, 15)
        team   = _hr_vs_team(batter_id, side, opp_name, 10)
        pit    = _get_s1_vs_pitcher(batter_id, pitcher_id)

        r_recent = _shrunk_pg(recent["hr_games"], recent["games"], 4)
        if r_recent is not None and recent.get("bunch"):
            r_recent = min(0.60, r_recent * 1.15)
        r_team = _shrunk_pg(team["hr_games"], team["games"], 4)
        r_pit  = None
        pit_ab = int(pit.get("ab", 0) or 0)
        pit_hr = int(pit.get("hr", 0) or 0)
        if pit_ab > 0:
            p_pa  = (pit_hr + 15 * HR_LG_PA) / (pit_ab + 15)
            r_pit = 1.0 - (1.0 - p_pa) ** 4

        # Statcast power component — a stable HR floor for true sluggers that is
        # independent of recent-form variance (a cold slugger still rates high).
        bp = _batter_power_lookup(batter_id)
        p_pow = None
        _xiso = bp.get("xiso")
        if _xiso is not None:
            p_pow = max(0.02, min(0.35, 0.05 + (_xiso - 0.060) * 0.9))
        elif bp.get("barrel_pct") is not None:
            p_pow = max(0.02, min(0.35, 0.02 + bp["barrel_pct"] / 100.0 * 1.6))

        comps = []
        # History/true-power dominant: a hitter's Statcast power (xISO/barrel) is
        # the real "is this an HR hitter" signal. Recent form is mostly fluke
        # variance (1-2 HRs in a 15-game window), so it is the smallest weight.
        if r_recent is not None: comps.append((0.20, r_recent))
        if r_pit    is not None: comps.append((0.15, r_pit))
        if r_team   is not None: comps.append((0.15, r_team))
        if p_pow    is not None: comps.append((0.50, p_pow))
        if not comps:
            return None
        wsum = sum(w for w, _ in comps)
        blended = sum(w * v for w, v in comps) / wsum

        # Pitcher HR-allowed multiplier — blended HR/9 vs league, nudged by
        # Statcast barrel-allowed. Homer-prone arms lift the over; stingy arms fade.
        pit_mult = 1.0
        pit_hr9_disp = ""
        pit_barrel_disp = ""
        _hr9 = _pitcher_hr9(pitcher_id, season) if pitcher_id else None
        if _hr9 and _hr9.get("blended_hr9") is not None:
            pit_mult = max(0.55, min(1.85, _hr9["blended_hr9"] / LEAGUE_HR9))
            pit_hr9_disp = _hr9["disp"]
        _pp = _pitcher_power_lookup(pitcher_id) if pitcher_id else {}
        _pbrl = _pp.get("barrel_pct")
        if _pbrl is not None:
            _bm = max(0.85, min(1.20, 1.0 + (_pbrl - LEAGUE_BARREL) * 0.02))
            pit_mult = max(0.55, min(1.90, pit_mult * _bm))
            pit_barrel_disp = f"{_pbrl:.0f}% brl allowed"

        # Platoon — generic handedness edge (batter side vs pitcher hand).
        platoon_mult = 1.0
        platoon_disp = ""
        _bs = bat_sides.get(batter_id)
        _ph = pit_hand_map.get(pitcher_id)
        if _bs and _ph:
            if _bs == "S":
                platoon_mult = 1.0
            elif _bs == _ph:
                platoon_mult = 0.93
                platoon_disp = f"vs {_ph}HP \u00b7 same side"
            else:
                platoon_mult = 1.07
                platoon_disp = f"vs {_ph}HP \u00b7 platoon edge"

        # Final pre-park P(>=1 HR). Park/weather folds in later (pipeline EV pass,
        # where the env factor is known). `wilson` carries this prob for ranking.
        blended = max(0.01, min(0.85, blended * pit_mult * platoon_mult))
        score = round(blended * 100)
        barrel_disp = (f"{bp['barrel_pct']:.0f}% barrel"
                       if bp.get("barrel_pct") is not None else "")

        over_odds  = c.get("over")
        under_odds = c.get("under")
        # OVER qualifies on blended HR likelihood (likely to homer).
        over_ok  = score >= HR_OVER_CUT
        # UNDER is now ODDS-driven, not score-driven: only big hitters the market
        # prices as a real HR threat (under is a minus-odds favorite no deeper than
        # HR_UNDER_MAX_JUICE, e.g. -500..-100). Drops scrub -600/-2000 unders that
        # nobody plays, and surfaces sluggers (Alvarez-type) whose under is the play.
        under_ok = (under_odds is not None
                    and HR_UNDER_MAX_JUICE <= under_odds < 0)
        if not (over_ok or under_ok):
            return []

        pit_disp = f"{pit_hr}HR/{pit_ab}AB" if pit_ab > 0 else "N/A"
        base = {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "line": c.get("line", 0.5),
                "score": score, "blended": score, "games": recent["games"],
                "recent_disp": recent["display"], "team_disp": team["display"],
                "pit_disp": pit_disp, "basis": "blend",
                "wilson": round(blended, 4),
                "pit_hr9_disp": pit_hr9_disp, "pit_barrel_disp": pit_barrel_disp,
                "barrel_disp": barrel_disp, "platoon_disp": platoon_disp,
                "over_odds": over_odds, "under_odds": under_odds,
                "batter_id": batter_id,
                "pitcher": pitcher_name,
                "recent_hr_log": _recent_hr_log(batter_id)}
        out = []
        if over_ok:
            d = dict(base); d["pick"] = "OVER"
            d["book"] = _book_label(c.get("over_book"))
            out.append(d)
        if under_ok:
            d = dict(base); d["pick"] = "UNDER"
            d["book"] = _book_label(c.get("under_book"))
            out.append(d)
        return out

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pks = _fut.result() or []
            except Exception:
                pks = []
            picks.extend(pks)

    overs  = [p for p in picks if p["pick"] == "OVER"]
    unders = [p for p in picks if p["pick"] == "UNDER"]
    overs.sort(key=lambda p: (-p["score"], -p["games"]))
    # Unders ranked biggest-hitter-first: least-juiced under (closest to even, e.g.
    # -150 before -500) on top = the strongest power threats the market fades.
    unders.sort(key=lambda p: (p.get("under_odds") if p.get("under_odds") is not None else -100000), reverse=True)
    overs  = overs[:HR_TOP_N]
    unders = unders[:HR_TOP_N]
    picks = overs + unders
    _log(emit, f"✅ HR Picks: {len(picks)} "
               f"({len(overs)} over / {len(unders)} under)")
    return picks


# ── Batter Walks Picks (Batter Walks, Over/Under 0.5) ──────────────────────
# Mirrors RBI exactly but counts games with 1+ walk (baseOnBalls) instead of
# RBI. OVER when batter walks at ≥55% of games, UNDER when ≤40% of games.
# Uses vs-opp
# H/A rate (min 3 games) when available, else falls back to overall last-15
# recent form so thin vs-opp samples still qualify. Odds from WALKS_ODDS
# (batter_walks market), zero extra Odds API calls. Distinct from PITCHER walks.

WALKS_OVER_CUT  = 55   # >= this % → likely to draw a walk (lowered from 60 to widen the OVER pool)
WALKS_UNDER_CUT = 40   # <= this % → likely NOT to draw a walk (UNDER side unchanged)
WALKS_MIN_GAMES = 3    # minimum head-to-head games vs THIS opponent to qualify
WALKS_TOP_N     = 20   # cap per side


def _walks_consistency(player_id, side: str, opp_name: str = "",
                       max_games: int = 10) -> dict:
    """Last max_games career H/A games counting games with 1+ walk."""
    if not player_id:
        return {"bb_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        seasons = list(range(cy, cy - 5, -1))
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
                pa = int(stat.get("plateAppearances", 0) or 0)
                if pa < 1:
                    continue
                bb = int(stat.get("baseOnBalls", 0) or 0)
                matching.append(1 if bb >= 1 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        bb_games = sum(matching)
        if games == 0:
            return {"bb_games": 0, "games": 0, "display": "N/A", "score": 0}
        return {"bb_games": bb_games, "games": games,
                "display": f"{bb_games}/{games}",
                "score": round(bb_games / games * 100)}
    except Exception:
        return {"bb_games": 0, "games": 0, "display": "ERR", "score": 0}


def _walks_overall(player_id, n: int = 15) -> dict:
    """Overall last-n games (any opp, any side): % of games with 1+ walk.
    Fallback pool so players without a vs-opp sample still qualify."""
    if not player_id:
        return {"bb_games": 0, "games": 0, "display": "N/A", "score": 0, "basis": ""}
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        flags = []
        for season in range(cy, cy - 3, -1):
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                stat = sp.get("stat", {})
                pa = int(stat.get("plateAppearances", 0) or 0)
                if pa < 1:
                    continue
                bb = int(stat.get("baseOnBalls", 0) or 0)
                flags.append(1 if bb >= 1 else 0)
                if len(flags) >= n:
                    break
            if len(flags) >= n:
                break
        games = len(flags)
        if games == 0:
            return {"bb_games": 0, "games": 0, "display": "N/A", "score": 0, "basis": ""}
        bb_games = sum(flags)
        return {"bb_games": bb_games, "games": games,
                "display": f"{bb_games}/{games}",
                "score": round(bb_games / games * 100),
                "basis": f"L{games}"}
    except Exception:
        return {"bb_games": 0, "games": 0, "display": "ERR", "score": 0, "basis": ""}


def _walks_rate(player_id, side: str, opp_name: str) -> dict:
    """Walk rate vs THIS opponent (H/A); fall back to overall recent form
    (last 15 games, any opp) when the vs-opp sample is too thin to qualify."""
    vs = _walks_consistency(player_id, side, opp_name, 10)
    if vs["games"] >= WALKS_MIN_GAMES:
        vs["basis"] = "vs opp"
        return vs
    return _walks_overall(player_id, 15)


def _recent_walks_log(player_id, n: int = 5) -> list:
    """Last n games (any opp), newest-first: date, bb, hits, opp, H/A."""
    if not player_id:
        return []
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        games = []
        for season in range(cy, cy - 2, -1):
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                stat = sp.get("stat", {})
                pa = int(stat.get("plateAppearances", 0) or 0)
                if pa < 1:
                    continue
                games.append({
                    "d":   (sp.get("date") or "")[5:],
                    "bb":  int(stat.get("baseOnBalls", 0) or 0),
                    "h":   int(stat.get("hits", 0) or 0),
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


def run_walks_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ Batter Walks Picks — Batter Walks (Over/Under 0.5)", "section")
    season = int(run_date[:4])

    if not WALKS_ODDS:
        _fetch_hits_lines(run_date, emit)   # populates WALKS_ODDS as a side effect
    candidates = list(WALKS_ODDS.values())
    if not candidates:
        _log(emit, "  No batter walks lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with a walks line")

    _build_player_map(season)
    id_map = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        rate = _walks_rate(batter_id, side, opp_name)
        if rate["games"] < WALKS_MIN_GAMES:
            return None
        score = rate["score"]
        if score >= WALKS_OVER_CUT:
            pick = "OVER"
        elif score <= WALKS_UNDER_CUT:
            pick = "UNDER"
        else:
            return None
        _HH = _walks_consistency(batter_id, side, opp_name, 10)
        _L10D = _walks_consistency(batter_id, side, "", 10)["display"]
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": pick, "line": c.get("line", 0.5),
                "rate_disp": rate["display"], "score": score,
                "games": rate["games"], "basis": rate.get("basis", ""),
                "h2h_disp": _HH["display"], "h2h_games": _HH["games"], "l10_disp": _L10D,
                "wilson": round(_wilson_lb(rate["bb_games"], rate["games"]), 4),
                "over_odds": c.get("over"), "under_odds": c.get("under"),
                "book": _book_label(c.get("over_book") if pick == "OVER" else c.get("under_book")),
                "batter_id": batter_id,
                "recent_walks_log": _recent_walks_log(batter_id)}

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    picks.sort(key=lambda p: (
        0 if p["pick"] == "OVER" else 1,
        -p["wilson"] if p["pick"] == "OVER" else p["score"],
        -p["games"],
    ))
    overs  = [p for p in picks if p["pick"] == "OVER"][:WALKS_TOP_N]
    unders = [p for p in picks if p["pick"] == "UNDER"][:WALKS_TOP_N]
    picks = overs + unders
    _log(emit, f"✅ Batter Walks Picks: {len(picks)} "
               f"({sum(1 for p in picks if p['pick']=='OVER')} over / "
               f"{sum(1 for p in picks if p['pick']=='UNDER')} under)")
    return picks


# ─── Total Bases Under ─────────────────────────────────────────────────────
# Players who frequently go Under 1.5 Total Bases (TB < 2 = 0 hits or exactly
# 1 single). TB = hits + doubles + 2*triples + 3*HR.  Picks use the same Odds
# API data already fetched (TB_ODDS populated by _fetch_hits_lines, zero extra
# calls). Only UNDER picks — qualify at ≥TB_UNDER_CUT% of H/A career games.
TB_UNDER_CUT = 70   # % of games with TB < 2 to qualify
TB_MIN_VS    = 2    # minimum games vs THIS opponent (preferred path)
TB_MIN_ANY   = 5    # minimum games any-opp (fallback path)
TB_TOP_N     = 20   # cap (unders only)


def _tb_consistency(player_id, side: str, opp_name: str = "",
                    max_games: int = 10, ignore_ha: bool = False) -> dict:
    """Last max_games career H/A games; count games where total bases < 2.
       ignore_ha=True drops the H/A filter (true recent form, any side)."""
    if not player_id:
        return {"tb_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        seasons = list(range(cy, cy - 5, -1))
        matching = []
        for season in seasons:
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                is_home = sp.get("isHome", False)
                if not ignore_ha and (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                h  = int(stat.get("hits",     0) or 0)
                d  = int(stat.get("doubles",  0) or 0)
                t  = int(stat.get("triples",  0) or 0)
                hr = int(stat.get("homeRuns", 0) or 0)
                tb = h + d + 2 * t + 3 * hr   # singles×1 + D×2 + T×3 + HR×4
                matching.append(1 if tb < 2 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        tb_games = sum(matching)
        if games == 0:
            return {"tb_games": 0, "games": 0, "display": "N/A", "score": 0}
        return {"tb_games": tb_games, "games": games,
                "display": f"{tb_games}/{games}",
                "score": round(tb_games / games * 100)}
    except Exception:
        return {"tb_games": 0, "games": 0, "display": "ERR", "score": 0}


def _recent_tb_log(player_id, n: int = 5) -> list:
    """Last n games (any opp), newest-first: date, hits, total_bases, opp, H/A."""
    if not player_id:
        return []
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        games = []
        for season in range(cy, cy - 2, -1):
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                h  = int(stat.get("hits",     0) or 0)
                d  = int(stat.get("doubles",  0) or 0)
                t  = int(stat.get("triples",  0) or 0)
                hr = int(stat.get("homeRuns", 0) or 0)
                tb = h + d + 2 * t + 3 * hr
                games.append({
                    "d":   (sp.get("date") or "")[5:],
                    "h":   h,
                    "tb":  tb,
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


def _tb_consistency_over(player_id, side: str, opp_name: str = "",
                         max_games: int = 10, ignore_ha: bool = False) -> dict:
    """Last max_games career H/A games vs opp; count games where total bases >= 2 (OVER).
       ignore_ha=True drops the H/A filter (true recent form, any side)."""
    if not player_id:
        return {"tb_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        seasons = list(range(cy, cy - 5, -1))
        matching = []
        for season in seasons:
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                is_home = sp.get("isHome", False)
                if not ignore_ha and (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                h  = int(stat.get("hits",     0) or 0)
                d  = int(stat.get("doubles",  0) or 0)
                t  = int(stat.get("triples",  0) or 0)
                hr = int(stat.get("homeRuns", 0) or 0)
                tb = h + d + 2 * t + 3 * hr
                matching.append(1 if tb >= 2 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        tb_games = sum(matching)
        if games == 0:
            return {"tb_games": 0, "games": 0, "display": "N/A", "score": 0}
        return {"tb_games": tb_games, "games": games,
                "display": f"{tb_games}/{games}",
                "score": round(tb_games / games * 100)}
    except Exception:
        return {"tb_games": 0, "games": 0, "display": "ERR", "score": 0}


def _hit_consistency(player_id, side: str, opp_name: str = "",
                     max_games: int = 10, ignore_ha: bool = False) -> dict:
    """Last max_games career H/A games vs opp; count games with 1+ hit (record-a-hit
       OVER). ignore_ha=True drops the H/A filter (true recent form, any side).
       Mirrors _tb_consistency_over so the hit list reuses the same Over engine."""
    if not player_id:
        return {"hit_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        seasons = list(range(cy, cy - 5, -1))
        matching = []
        for season in seasons:
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                is_home = sp.get("isHome", False)
                if not ignore_ha and (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                h = int(stat.get("hits", 0) or 0)
                matching.append(1 if h >= 1 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        hit_games = sum(matching)
        if games == 0:
            return {"hit_games": 0, "games": 0, "display": "N/A", "score": 0}
        return {"hit_games": hit_games, "games": games,
                "display": f"{hit_games}/{games}",
                "score": round(hit_games / games * 100)}
    except Exception:
        return {"hit_games": 0, "games": 0, "display": "ERR", "score": 0}


def _hrr_consistency_over(player_id, side: str, opp_name: str = "",
                          max_games: int = 10, ignore_ha: bool = False) -> dict:
    """Last max_games career H/A games vs opp; count games where H+R+RBI >= 2 (OVER 1.5).
       ignore_ha=True drops the H/A filter (true recent form, any side)."""
    if not player_id:
        return {"hrr_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        seasons = list(range(cy, cy - 5, -1))
        matching = []
        for season in seasons:
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                is_home = sp.get("isHome", False)
                if not ignore_ha and (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                h   = int(stat.get("hits", 0) or 0)
                r   = int(stat.get("runs", 0) or 0)
                rbi = int(stat.get("rbi",  0) or 0)
                hrr = h + r + rbi
                matching.append(1 if hrr >= 2 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        hrr_games = sum(matching)
        if games == 0:
            return {"hrr_games": 0, "games": 0, "display": "N/A", "score": 0}
        return {"hrr_games": hrr_games, "games": games,
                "display": f"{hrr_games}/{games}",
                "score": round(hrr_games / games * 100)}
    except Exception:
        return {"hrr_games": 0, "games": 0, "display": "ERR", "score": 0}


def _recent_hrr_log(player_id, n: int = 5) -> list:
    """Last n games (any opp), newest-first: date, h, r, rbi, total hrr, opp, H/A."""
    if not player_id:
        return []
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        games = []
        for season in range(cy, cy - 2, -1):
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                h   = int(stat.get("hits", 0) or 0)
                r   = int(stat.get("runs", 0) or 0)
                rbi = int(stat.get("rbi",  0) or 0)
                games.append({
                    "d":   (sp.get("date") or "")[5:],
                    "h":   h,
                    "r":   r,
                    "rbi": rbi,
                    "hrr": h + r + rbi,
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


HRR_OVER_CUT    = 60   # >= this % → likely to get H+R+RBI >= 2 (vs opp H/A)
HRR_UNDER_CUT   = 30   # <= this % → likely to stay under 1.5 H+R+RBI (vs opp H/A)
HRR_MIN_VS  = 2    # vs-opp H/A games to use the head-to-head anchor
HRR_MIN_ANY = 5    # else fall back to L10 H/A any-opp with >= this many games
HRR_OVER_TOP_N  = 30   # OVER side cap (top 10 on the card + ranks 11-30 in "more")
HRR_UNDER_TOP_N = 20   # UNDER side cap


def run_hrr_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ HRR Picks — Batter Hits+Runs+RBI Over 1.5", "section")
    season = int(run_date[:4])

    if not HRR_ODDS:
        _fetch_hits_lines(run_date, emit)
    candidates = list(HRR_ODDS.values())
    if not candidates:
        _log(emit, "  No batter HRR over lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with an HRR over line")

    _build_player_map(season)
    id_map: dict = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))
    pitchers = _get_probable_pitchers(run_date)

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        pitcher_name, pitcher_id = "TBD", None
        for pteam, pinfo in pitchers.items():
            if _team_match(pteam, opp_name):
                pitcher_name = pinfo["name"]
                pitcher_id   = pinfo.get("id")
                break
        s1_pit = _get_s1_vs_pitcher(batter_id, pitcher_id)
        # vs-opp preferred (>=HRR_MIN_VS games); else L10 H/A any-opp (>=HRR_MIN_ANY).
        _vsop = _hrr_consistency_over(batter_id, side, opp_name, 10)
        if _vsop["games"] >= HRR_MIN_VS:
            vs = _vsop; vs["basis"] = "vs opp"
        else:
            vs = _hrr_consistency_over(batter_id, side, "", 10)
            if vs["games"] < HRR_MIN_ANY:
                return None
            vs["basis"] = "L10 H/A"
        # Card shows BOTH: head-to-head (vs this opp, H/A) AND last-10 H/A any-opp.
        _HH = _vsop
        _L10D = vs["display"] if vs.get("basis") == "L10 H/A" else _hrr_consistency_over(batter_id, side, "", 10)["display"]
        # 3-window convergence blend: vs-opp 35%, L10 any-opp 40%, L5 any-opp 25%
        r10 = _hrr_consistency_over(batter_id, side, "", 10, ignore_ha=True)
        r5  = _hrr_consistency_over(batter_id, side, "", 5, ignore_ha=True)
        comps = [(0.35, vs["score"] / 100.0)]
        if r10["games"] > 0: comps.append((0.40, r10["score"] / 100.0))
        if r5["games"]  > 0: comps.append((0.25, r5["score"]  / 100.0))
        wsum = sum(w for w, _ in comps)
        blend_score = round(sum(w * v for w, v in comps) / wsum * 100)
        # OVERS only: hot hand (recent power + active hit streak) nudges a hitter
        # over the over-cut. Unders use the plain blend (bonus is always >= 0).
        hot = _hitter_hot_hand(batter_id)
        over_score = min(blend_score + hot["bonus"], 100)
        if over_score >= HRR_OVER_CUT:
            pick = "OVER"
            final_score = over_score
        elif blend_score <= HRR_UNDER_CUT:
            pick = "UNDER"
            final_score = blend_score
        else:
            return None
        l5_s = r5["score"] if r5["games"] > 0 else None
        conv_flag = all((pick == "OVER" and v >= HRR_OVER_CUT) or
                        (pick == "UNDER" and v <= HRR_UNDER_CUT)
                        for v in [vs["score"], r10["score"] if r10["games"] > 0 else None, l5_s]
                        if v is not None)
        cold_flag = ((pick == "OVER"  and l5_s is not None and l5_s <= HRR_UNDER_CUT) or
                     (pick == "UNDER" and l5_s is not None and l5_s >= HRR_OVER_CUT))
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": pick, "line": 1.5,
                "rate_disp": vs["display"], "score": final_score, "base_score": blend_score,
                "opp_score": vs["score"], "recent_l10": r10["display"], "recent_l5": r5["display"],
                "h2h_disp": _HH["display"], "h2h_games": _HH["games"], "l10_disp": _L10D,
                "games": vs["games"], "basis": vs.get("basis", ""),
                "conv_flag": conv_flag, "cold_flag": cold_flag,
                "wilson": round(_wilson_lb(vs["hrr_games"], vs["games"]), 4),
                "hot_bonus": hot["bonus"] if pick == "OVER" else 0,
                "hot_disp": hot["disp"] if pick == "OVER" else "",
                "hrr_over_odds": c.get("hrr_over_odds"),
                "hrr_under_odds": c.get("hrr_under_odds"),
                "book": _book_label(c.get("hrr_over_odds_book") if pick == "OVER" else c.get("hrr_under_odds_book")),
                "batter_id": batter_id,
                "pitcher": pitcher_name, **_s1_ha_fields(batter_id, pitcher_id, side, s1_pit),
                "recent_hrr_log": _recent_hrr_log(batter_id)}

    _pw_pairs = []
    for _c in candidates:
        _bid = id_map.get(_c["name"])
        _pt  = team_map.get(_bid, "") if _bid else ""
        if not _bid or not _pt: continue
        if _team_match(_pt, _c["home_team"]):   _opp = _c["away_team"]
        elif _team_match(_pt, _c["away_team"]): _opp = _c["home_team"]
        else: continue
        _pid = next((pi.get("id") for pt, pi in pitchers.items() if _team_match(pt, _opp)), None)
        _pw_pairs.append((_bid, _pid))
    _prewarm_s1_ha_cache(_pw_pairs)

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    picks.sort(key=lambda p: (
        0 if p["pick"] == "OVER" else 1,
        -p["score"] if p["pick"] == "OVER" else p["score"],
        -p["games"],
    ))
    overs  = [p for p in picks if p["pick"] == "OVER"][:HRR_OVER_TOP_N]
    unders = [p for p in picks if p["pick"] == "UNDER"][:HRR_UNDER_TOP_N]
    picks = overs + unders
    _log(emit, f"✅ HRR Picks: {len(picks)} "
               f"({len(overs)} over / {len(unders)} under)")
    return picks


# ── HRR SPECIAL (parlay confluence board) ─────────────────────────────────
# A separate, stricter OVER-only board built for parlays. A pick qualifies ONLY
# when ALL gates clear together (AND confluence). Gates 1-3 live here; the 4th
# (day/night BA >= .270 for today's game type) is applied in the pipeline where
# the day/night BA is already fetched. The regular run_hrr_picks board is left
# completely untouched.
HRR_SPECIAL_BA    = 0.270  # min career BA vs today's pitcher
HRR_SPECIAL_RATE  = 60     # min % (vs-team H/A AND last-10 H/A) for 2+ HRR
HRR_SPECIAL_TOP_N = 30     # cap


def run_hrr_special_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ HRR Special (Parlay) — BA>=.275 vs P · 65% vs team · 65% L10 H/A", "section")
    season = int(run_date[:4])

    if not HRR_ODDS:
        _fetch_hits_lines(run_date, emit)
    candidates = list(HRR_ODDS.values())
    if not candidates:
        _log(emit, "  No batter HRR over lines posted today.")
        return []

    _build_player_map(season)
    id_map: dict = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))
    pitchers = _get_probable_pitchers(run_date)

    def _eval(c):
        name = c["name"]
        over_odds = c.get("hrr_over_odds")          # must be priced — feeds parlays
        if over_odds is None:
            return None
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        pitcher_name, pitcher_id = "TBD", None
        for pteam, pinfo in pitchers.items():
            if _team_match(pteam, opp_name):
                pitcher_name = pinfo["name"]
                pitcher_id   = pinfo.get("id")
                break
        # GATE 1 — career BA vs today's pitcher >= .275
        s1 = _get_s1_vs_pitcher(batter_id, pitcher_id)
        if s1.get("ba") is None or s1["ba"] < HRR_SPECIAL_BA:
            return None
        # GATE 2 — vs-team H/A 2+ HRR rate (last 10 such games) >= 65%
        vs_team = _hrr_consistency_over(batter_id, side, opp_name, 10)
        if vs_team["games"] < 1 or vs_team["score"] < HRR_SPECIAL_RATE:
            return None
        # GATE 3 — last-10 H/A any-opp 2+ HRR rate >= 65%
        l10 = _hrr_consistency_over(batter_id, side, "", 10)
        if l10["games"] < 1 or l10["score"] < HRR_SPECIAL_RATE:
            return None
        conf = round((vs_team["score"] + l10["score"]) / 2)
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": "OVER", "line": 1.5, "special": True,
                "score": conf, "games": vs_team["games"],
                "vsp_ba": round(s1["ba"], 3), "vsp_ba_disp": s1.get("display", "N/A"),
                "vsteam_disp": vs_team["display"], "vsteam_score": vs_team["score"],
                "l10_disp": l10["display"], "l10_score": l10["score"],
                "rate_disp": vs_team["display"],
                "wilson": round(_wilson_lb(vs_team["hrr_games"], vs_team["games"]), 4),
                "hrr_over_odds": over_odds, "hrr_under_odds": None,
                "book": _book_label(c.get("hrr_over_odds_book")),
                "batter_id": batter_id, "pitcher": pitcher_name,
                "recent_hrr_log": _recent_hrr_log(batter_id)}

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    picks.sort(key=lambda p: (-p["score"], -p["vsp_ba"], -p["games"]))
    picks = picks[:HRR_SPECIAL_TOP_N]
    _log(emit, f"✅ HRR Special (Parlay): {len(picks)} confluence plays "
               f"(pre day/night gate)")
    return picks


TB_OVER_CUT    = 60   # >= this % → likely to get 1.5+ total bases (vs opp H/A)
TB_OVER_MIN_VS  = 2    # vs-opp H/A games to use the head-to-head anchor
TB_OVER_MIN_ANY = 5    # else fall back to L10 H/A any-opp with >= this many games
TB_OVER_TOP_N  = 20   # cap (overs only)


def run_tb_over_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ TB Over Picks — Batter Total Bases Over 1.5", "section")
    season = int(run_date[:4])

    if not TB_OVER_ODDS:
        _fetch_hits_lines(run_date, emit)
    candidates = list(TB_OVER_ODDS.values())
    if not candidates:
        _log(emit, "  No batter total-bases over lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with a TB over line")

    _build_player_map(season)
    id_map: dict = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))
    pitchers = _get_probable_pitchers(run_date)

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        pitcher_name, pitcher_id = "TBD", None
        for pteam, pinfo in pitchers.items():
            if _team_match(pteam, opp_name):
                pitcher_name = pinfo["name"]
                pitcher_id   = pinfo.get("id")
                break
        s1_pit = _get_s1_vs_pitcher(batter_id, pitcher_id)
        # vs-opp preferred (>=TB_OVER_MIN_VS games); else L10 H/A any-opp (>=TB_OVER_MIN_ANY).
        _vsop = _tb_consistency_over(batter_id, side, opp_name, 10)
        if _vsop["games"] >= TB_OVER_MIN_VS:
            vs = _vsop; vs["basis"] = "vs opp"
        else:
            vs = _tb_consistency_over(batter_id, side, "", 10)
            if vs["games"] < TB_OVER_MIN_ANY:
                return None
            vs["basis"] = "L10 H/A"
        # Card shows BOTH: head-to-head (vs this opp, H/A) AND last-10 H/A any-opp.
        _HH = _vsop
        _L10D = vs["display"] if vs.get("basis") == "L10 H/A" else _tb_consistency_over(batter_id, side, "", 10)["display"]
        # 3-window convergence blend: vs-opp 35%, L10 any-opp 40%, L5 any-opp 25%
        r10 = _tb_consistency_over(batter_id, side, "", 10, ignore_ha=True)
        r5  = _tb_consistency_over(batter_id, side, "", 5, ignore_ha=True)
        comps = [(0.35, vs["score"] / 100.0)]
        if r10["games"] > 0: comps.append((0.40, r10["score"] / 100.0))
        if r5["games"]  > 0: comps.append((0.25, r5["score"]  / 100.0))
        wsum = sum(w for w, _ in comps)
        blend_score = round(sum(w * v for w, v in comps) / wsum * 100)
        # OVERS only: hot hand (recent power + active hit streak) nudges a hitter
        # over the over-cut they'd otherwise just miss.
        hot = _hitter_hot_hand(batter_id)
        over_score = min(blend_score + hot["bonus"], 100)
        if over_score < TB_OVER_CUT:
            return None
        final_score = over_score
        l5_s = r5["score"] if r5["games"] > 0 else None
        conv_flag = all(v >= TB_OVER_CUT
                        for v in [vs["score"], r10["score"] if r10["games"] > 0 else None, l5_s]
                        if v is not None)
        cold_flag = (l5_s is not None and l5_s < TB_OVER_CUT - 15)
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": "OVER", "line": 1.5,
                "rate_disp": vs["display"], "score": final_score, "base_score": blend_score,
                "opp_score": vs["score"], "recent_l10": r10["display"], "recent_l5": r5["display"],
                "h2h_disp": _HH["display"], "h2h_games": _HH["games"], "l10_disp": _L10D,
                "games": vs["games"], "basis": vs.get("basis", ""),
                "conv_flag": conv_flag, "cold_flag": cold_flag,
                "wilson": round(_wilson_lb(vs["tb_games"], vs["games"]), 4),
                "hot_bonus": hot["bonus"],
                "hot_disp": hot["disp"],
                "tb_over_odds": c.get("tb_over_odds"),
                "book": _book_label(c.get("tb_over_odds_book")),
                "batter_id": batter_id,
                "pitcher": pitcher_name, **_s1_ha_fields(batter_id, pitcher_id, side, s1_pit),
                "recent_tb_log": _recent_tb_log(batter_id)}

    _pw_pairs = []
    for _c in candidates:
        _bid = id_map.get(_c["name"])
        _pt  = team_map.get(_bid, "") if _bid else ""
        if not _bid or not _pt: continue
        if _team_match(_pt, _c["home_team"]):   _opp = _c["away_team"]
        elif _team_match(_pt, _c["away_team"]): _opp = _c["home_team"]
        else: continue
        _pid = next((pi.get("id") for pt, pi in pitchers.items() if _team_match(pt, _opp)), None)
        _pw_pairs.append((_bid, _pid))
    _prewarm_s1_ha_cache(_pw_pairs)

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    picks.sort(key=lambda p: (-p["score"], -p["games"]))
    picks = picks[:TB_OVER_TOP_N]
    _log(emit, f"✅ TB Over Picks: {len(picks)} qualifying")
    return picks


# ── "Top Plays to Record a Hit" as an OVER category (pool B) ───────────
# The hit list KEEPS its career-vs-pitcher backbone (pool A, built in
# pipeline.py). These add pool B: hot hitters with a posted 0.5 hit line but
# NO career history vs today's pitcher, qualified by the SAME Over engine the
# TB-Over / Runs / RBI / HRR overs use (vs-opp anchor OR L10 H/A fallback,
# 3-window convergence blend + hot-hand, HIT_OVER_CUT gate).
HIT_OVER_CUT     = 60   # >= this % recent hit-rate to qualify a non-career hot hitter
HIT_OVER_MIN_VS  = 2    # vs-opp H/A games to use the head-to-head anchor
HIT_OVER_MIN_ANY = 5    # else fall back to L10 H/A any-opp with >= this many games
HIT_OVER_TOP_N   = 30   # cap on pool-B additions


def hit_over_signals(batter_id, side: str, opp_name: str = "") -> dict:
    """Shared 3-window record-a-hit Over signal used by BOTH pools:
       - pool A (career picks in pipeline.py) layer these on as display + a hot
         nudge to the career total (never dropped),
       - pool B (run_hit_picks) qualify off over_score vs HIT_OVER_CUT.
       Returns blend, over_score, convergence flags, Wilson LB and hot-hand."""
    _vsop = _hit_consistency(batter_id, side, opp_name, 10)
    if _vsop["games"] >= HIT_OVER_MIN_VS:
        vs = _vsop; vs["basis"] = "vs opp"
    else:
        vs = _hit_consistency(batter_id, side, "", 10)
        vs["basis"] = "L10 H/A"
    # 3-window convergence blend: vs-opp 35%, L10 any-opp 40%, L5 any-opp 25%
    r10 = _hit_consistency(batter_id, side, "", 10, ignore_ha=True)
    r5  = _hit_consistency(batter_id, side, "", 5,  ignore_ha=True)
    comps = [(0.35, vs["score"] / 100.0)]
    if r10["games"] > 0: comps.append((0.40, r10["score"] / 100.0))
    if r5["games"]  > 0: comps.append((0.25, r5["score"]  / 100.0))
    wsum = sum(w for w, _ in comps) or 1.0
    blend_score = round(sum(w * v for w, v in comps) / wsum * 100)
    # OVERS only: hot hand (recent power + active hit streak) nudges over the cut.
    hot = _hitter_hot_hand(batter_id)
    over_score = min(blend_score + hot["bonus"], 100)
    l5_s = r5["score"] if r5["games"] > 0 else None
    conv_flag = all(v >= HIT_OVER_CUT
                    for v in [vs["score"], r10["score"] if r10["games"] > 0 else None, l5_s]
                    if v is not None)
    cold_flag = (l5_s is not None and l5_s < HIT_OVER_CUT - 15)
    _HH = _vsop
    _L10D = vs["display"] if vs.get("basis") == "L10 H/A" else _hit_consistency(batter_id, side, "", 10)["display"]
    return {"blend": blend_score, "over_score": over_score,
            "vs_games": vs["games"], "basis": vs.get("basis", ""),
            "rate_disp": vs["display"], "opp_score": vs["score"],
            "h2h_disp": _HH["display"], "h2h_games": _HH["games"], "l10_disp": _L10D,
            "recent_l10": r10["display"], "recent_l5": r5["display"],
            "conv_flag": conv_flag, "cold_flag": cold_flag,
            "wilson_hit": round(_wilson_lb(vs["hit_games"], vs["games"]), 4) if vs["games"] > 0 else 0,
            "hot_bonus": hot["bonus"], "hot_disp": hot["disp"]}


def run_hit_picks(run_date: str, team_schedule: dict,
                  exclude_ids=None, emit=None) -> list:
    """Pool B for the Record-a-Hit list: every hitter with a posted 0.5 hit line
       who is NOT already on the career-model list (exclude_ids) and clears
       HIT_OVER_CUT via the shared Over engine. Returns hit-card-shaped dicts so
       pipeline.py can fold them straight into top9/also_ran."""
    _log(emit, "", "log")
    _log(emit, "▸ Hit Over Picks — broadened pool (hot hitters, no career vs pitcher)", "section")
    season = int(run_date[:4])
    exclude_ids = set(exclude_ids or [])

    if not HIT_TEAMS:
        _fetch_hits_lines(run_date, emit)
    candidates = list(HIT_TEAMS.values())
    if not candidates:
        _log(emit, "  No 0.5 hit lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with a 0.5 hit line")

    _build_player_map(season)
    id_map: dict = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid and pid not in exclude_ids:   # skip career-model players (pool A)
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))
    pitchers = _get_probable_pitchers(run_date)

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        pitcher_name, pitcher_id = "TBD", None
        for pteam, pinfo in pitchers.items():
            if _team_match(pteam, opp_name):
                pitcher_name = pinfo["name"]
                pitcher_id   = pinfo.get("id")
                break
        s1_pit = _get_s1_vs_pitcher(batter_id, pitcher_id)
        sig = hit_over_signals(batter_id, side, opp_name)
        if sig["over_score"] < HIT_OVER_CUT:
            return None
        nk = _norm_name(name)
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": "OVER", "line": 0.5,
                "rate_disp": sig["rate_disp"], "score": sig["over_score"],
                "base_score": sig["blend"], "opp_score": sig["opp_score"],
                "recent_l10": sig["recent_l10"], "recent_l5": sig["recent_l5"],
                "games": sig["vs_games"], "basis": sig["basis"],
                "h2h_disp": sig["h2h_disp"], "h2h_games": sig["h2h_games"], "l10_disp": sig["l10_disp"],
                "conv_flag": sig["conv_flag"], "cold_flag": sig["cold_flag"],
                "wilson": sig["wilson_hit"],
                "hot_bonus": sig["hot_bonus"], "hot_disp": sig["hot_disp"],
                "hit_odds": HIT_ODDS.get(nk),
                "book": _book_label(HIT_ODDS_BOOK.get(nk)),
                "batter_id": batter_id,
                "pitcher": pitcher_name, **_s1_ha_fields(batter_id, pitcher_id, side, s1_pit)}

    _pw_pairs = []
    for _c in candidates:
        _bid = id_map.get(_c["name"])
        _pt  = team_map.get(_bid, "") if _bid else ""
        if not _bid or not _pt: continue
        if _team_match(_pt, _c["home_team"]):   _opp = _c["away_team"]
        elif _team_match(_pt, _c["away_team"]): _opp = _c["home_team"]
        else: continue
        _pid = next((pi.get("id") for pt, pi in pitchers.items() if _team_match(pt, _opp)), None)
        _pw_pairs.append((_bid, _pid))
    _prewarm_s1_ha_cache(_pw_pairs)

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    picks.sort(key=lambda p: (-p["score"], -p["games"]))
    picks = picks[:HIT_OVER_TOP_N]
    _log(emit, f"✅ Hit Over Picks (pool B): {len(picks)} qualifying")
    return picks


def run_tb_under_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ TB Under Picks — Batter Total Bases Under 1.5", "section")
    season = int(run_date[:4])

    if not TB_ODDS:
        _fetch_hits_lines(run_date, emit)
    candidates = list(TB_ODDS.values())
    if not candidates:
        _log(emit, "  No batter total-bases under lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with a TB under line")

    _build_player_map(season)
    id_map: dict = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))
    pitchers = _get_probable_pitchers(run_date)

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        pitcher_name, pitcher_id = "TBD", None
        for pteam, pinfo in pitchers.items():
            if _team_match(pteam, opp_name):
                pitcher_name = pinfo["name"]
                pitcher_id   = pinfo.get("id")
                break
        s1_pit = _get_s1_vs_pitcher(batter_id, pitcher_id)
        vs = _tb_consistency(batter_id, side, opp_name, 10)
        if vs["games"] >= TB_MIN_VS:
            rate = vs; rate["basis"] = "vs opp"
        else:
            any_opp = _tb_consistency(batter_id, side, "", 10)
            if any_opp["games"] < TB_MIN_ANY:
                return None
            rate = any_opp; rate["basis"] = "L10 H/A"
        # Card shows BOTH: head-to-head (vs this opp, H/A) AND last-10 H/A any-opp.
        _HH = vs
        _L10D = rate["display"] if rate.get("basis") == "L10 H/A" else _tb_consistency(batter_id, side, "", 10)["display"]
        # 3-window convergence blend: primary anchor 35%, L10 any-opp 40%, L5 any-opp 25%
        r10 = _tb_consistency(batter_id, side, "", 10, ignore_ha=True)
        r5  = _tb_consistency(batter_id, side, "", 5, ignore_ha=True)
        comps = [(0.35, rate["score"] / 100.0)]
        if r10["games"] > 0: comps.append((0.40, r10["score"] / 100.0))
        if r5["games"]  > 0: comps.append((0.25, r5["score"]  / 100.0))
        wsum = sum(w for w, _ in comps)
        blend_score = round(sum(w * v for w, v in comps) / wsum * 100)
        if blend_score < TB_UNDER_CUT:
            return None
        l5_s = r5["score"] if r5["games"] > 0 else None
        conv_flag = all(v >= TB_UNDER_CUT
                        for v in [rate["score"], r10["score"] if r10["games"] > 0 else None, l5_s]
                        if v is not None)
        cold_flag = (l5_s is not None and l5_s < TB_UNDER_CUT - 15)
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": "UNDER", "line": 1.5,
                "rate_disp": rate["display"], "score": blend_score,
                "opp_score": rate["score"], "recent_l10": r10["display"], "recent_l5": r5["display"],
                "h2h_disp": _HH["display"], "h2h_games": _HH["games"], "l10_disp": _L10D,
                "games": rate["games"], "basis": rate.get("basis", ""),
                "conv_flag": conv_flag, "cold_flag": cold_flag,
                "wilson": round(_wilson_lb(rate["tb_games"], rate["games"]), 4),
                "tb_under_odds": c.get("tb_under_odds"),
                "book": _book_label(c.get("tb_under_odds_book")),
                "batter_id": batter_id,
                "pitcher": pitcher_name, **_s1_ha_fields(batter_id, pitcher_id, side, s1_pit),
                "recent_tb_log": _recent_tb_log(batter_id)}

    _pw_pairs = []
    for _c in candidates:
        _bid = id_map.get(_c["name"])
        _pt  = team_map.get(_bid, "") if _bid else ""
        if not _bid or not _pt: continue
        if _team_match(_pt, _c["home_team"]):   _opp = _c["away_team"]
        elif _team_match(_pt, _c["away_team"]): _opp = _c["home_team"]
        else: continue
        _pid = next((pi.get("id") for pt, pi in pitchers.items() if _team_match(pt, _opp)), None)
        _pw_pairs.append((_bid, _pid))
    _prewarm_s1_ha_cache(_pw_pairs)

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    picks.sort(key=lambda p: (-p["score"], -p["games"]))
    picks = picks[:TB_TOP_N]
    _log(emit, f"✅ TB Under Picks: {len(picks)} qualifying")
    return picks
