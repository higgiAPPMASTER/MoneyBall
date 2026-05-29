
"""
under_picks.py — Under Picks via The Odds API (batter_hits 1.5 line).
Replaces the DraftKings scraper. Requires ODDS_API_KEY env var.

Algorithm per candidate:
  S1  Career BA vs today's probable pitcher  — N/A passes; DQ only if >= .250 with AB > 0
  S2  H/A BA vs today's opponent             — must have data AND < .225
  S3  Current-season H/A BA                  — must have data AND < .250
  All three pass -> ranked coldest first (lowest S2 + S3 combined BA).
"""
import os
import requests
import time
from datetime import date, datetime, timezone

from mlb_stats_splits import fetch_step2_ba, fetch_step3_ba
from concurrent.futures import ThreadPoolExecutor, as_completed

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

_PLAYER_MAP:    dict = {}
_PITCHER_CACHE: dict = {}

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


def _log(emit, msg, type_="log"):
    if emit:
        emit({"type": type_, "msg": msg})


def _team_match(a: str, b: str) -> bool:
    a, b = a.lower().strip(), b.lower().strip()
    if a == b: return True
    al = a.split()[-1] if a else ""
    bl = b.split()[-1] if b else ""
    if al and al == bl: return True
    return (a in b) or (b in a)


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
    last = key.split()[-1] if key else ""
    for k, v in _PLAYER_MAP.items():
        if k.endswith(last) and abs(len(k) - len(key)) <= 6:
            return v
    return None


def _get_teams_batch(player_ids: list) -> dict:
    if not player_ids: return {}
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/people",
            params={"personIds": ",".join(str(i) for i in player_ids), "hydrate": "currentTeam"},
            timeout=12)
        result = {}
        for p in r.json().get("people", []):
            pid  = p.get("id")
            team = p.get("currentTeam", {}).get("name", "")
            if pid: result[pid] = team
        return result
    except Exception:
        return {}


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


def _get_s1_vs_pitcher(batter_id, pitcher_id) -> dict:
    if not batter_id or not pitcher_id:
        return {"ba": None, "display": "N/A", "ab": 0}
    try:
        r = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{batter_id}/stats",
            params={"stats": "vsPlayer", "opposingPlayerId": pitcher_id,
                    "group": "hitting", "gameType": "R"}, timeout=10)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits: return {"ba": None, "display": "N/A", "ab": 0}
        stat = splits[0].get("stat", {})
        ab = int(stat.get("atBats", 0) or 0)
        h  = int(stat.get("hits",   0) or 0)
        if ab == 0: return {"ba": None, "display": "N/A", "ab": 0}
        ba = h / ab
        return {"ba": ba, "display": f".{int(ba*1000):03d} ({ab}AB)", "ab": ab}
    except Exception:
        return {"ba": None, "display": "N/A", "ab": 0}


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


def _fetch_hits_lines(run_date: str, emit=None) -> list:
    if not ODDS_API_KEY:
        _log(emit, "⚠️  ODDS_API_KEY not set — Under Picks skipped")
        return []

    PREFERRED = ["draftkings", "betmgm", "espnbet", "hardrockbet", "fanduel", "williamhill_us", "pointsbetus"]
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
        seen: dict = {}

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
                params={"apiKey": ODDS_API_KEY, "regions": "us",
                        "markets": "batter_hits,batter_hits_alternate,batter_total_bases,batter_total_bases_alternate",
                        "oddsFormat": "american"}, timeout=15)
            if r2.status_code != 200: continue
            all_bms = r2.json().get("bookmakers", [])
            # Scan ALL books for both the 0.5 hit odds and the 1.5-line candidates.
            _bm_map = {b.get("key"): b for b in all_bms}
            # Collect 0.5-line Over odds from every bookmaker (first seen per player)
            for bm_any in all_bms:
                for mkt in bm_any.get("markets", []):
                    if mkt.get("key") not in ("batter_hits", "batter_hits_alternate"): continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt is None or side != "Over": continue
                        nk = _norm_name(player)
                        if pt == 0.5 and nk not in HIT_ODDS and price is not None:
                            HIT_ODDS[nk] = price
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
                        if side == "Over" and entry["over_odds"] is None:
                            entry["over_odds"] = price
                        elif side == "Under" and entry["under_odds"] is None:
                            entry["under_odds"] = price
            # Under 1.5 TOTAL BASES odds for the same players, shown alongside the
            # hits line (pays more because a double/HR busts it even on one hit).
            # Same all-books union + PREFERRED order; first Under price seen wins.
            tb_under: dict = {}
            for book in ordered_books:
                for mkt in book.get("markets", []):
                    if mkt.get("key") not in ("batter_total_bases", "batter_total_bases_alternate"): continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt != 1.5 or side != "Under" or price is None: continue
                        nk = _norm_name(player)
                        if nk not in tb_under:
                            tb_under[nk] = price
            for nk, entry in event_entries.items():
                entry["tb_under_odds"] = tb_under.get(nk)
                seen.setdefault(nk, entry)

        _log(emit, f"  ✅ {len(seen)} players on 1.5 hits line | {len(HIT_ODDS)} players with 0.5 hit odds")
        return list(seen.values())
    except Exception as exc:
        _log(emit, f"⚠️  Odds API error: {exc}")
        return []


def run_under_picks(run_date: str, team_schedule: dict, emit=None) -> list:
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

    id_map: dict = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid: id_map[c["name"]] = pid

    _log(emit, f"  Looking up teams for {len(id_map)} players…")
    team_map = _get_teams_batch(list(id_map.values()))
    _log(emit, "  ✅ Teams resolved")
    _log(emit, f"  Evaluating {len(candidates)} candidates…")

    # Evaluate candidates in parallel (≤4 threads). Each worker is independent —
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
        s1 = _get_s1_vs_pitcher(batter_id, pitcher_id)
        if s1["ba"] is not None and s1["ab"] > 0 and s1["ba"] >= 0.250: return None
        s2 = fetch_step2_ba(batter_id, side, opp_name)
        if s2["ba"] is None or s2["ba"] >= 0.225: return None
        s3 = fetch_step3_ba(batter_id, side, season)
        if s3["ba"] is None or s3["ba"] >= 0.250: return None
        l7 = _get_last7_ba(batter_id)
        if l7["ba"] is not None and l7["ba"] > 0.250: return None
        l7_ba = l7["ba"] if l7["ba"] is not None else s3["ba"]
        under_score = round((s2["ba"] + s3["ba"] + l7_ba) * 1000)
        return {"name": name, "pos": "—", "side": side, "opp": opp_name,
                "pitcher": pitcher_name, "s1_disp": s1["display"],
                "s1_ab": s1["ab"], "s2": s2, "s3": s3, "l7": l7,
                "lineup_status": "TBD", "under_score": under_score,
                "under_odds": c.get("under_odds"), "over_odds": c.get("over_odds"),
                "tb_under_odds": c.get("tb_under_odds")}

    picks = []
    with ThreadPoolExecutor(max_workers=4) as _ex:
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
