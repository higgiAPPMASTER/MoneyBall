
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
from datetime import date

from mlb_stats_splits import fetch_step2_ba, fetch_step3_ba

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

_PLAYER_MAP:    dict = {}
_PITCHER_CACHE: dict = {}


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


def _fetch_hits_lines(run_date: str, emit=None) -> list:
    if not ODDS_API_KEY:
        _log(emit, "⚠️  ODDS_API_KEY not set — Under Picks skipped")
        return []

    PREFERRED = ["draftkings", "fanduel", "betmgm", "williamhill_us", "pointsbetus"]
    tomorrow  = (time.strftime("%Y-%m-%d",
                  time.gmtime(time.mktime(time.strptime(run_date, "%Y-%m-%d")) + 86400)))
    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY, "dateFormat": "iso"}, timeout=15)
        if r.status_code != 200:
            _log(emit, f"⚠️  Odds API events returned {r.status_code}")
            return []

        events = [e for e in r.json()
                  if e.get("commence_time", "")[:10] in (run_date, tomorrow)]
        _log(emit, f"  Odds API: {len(events)} games for {run_date}")
        seen: dict = {}

        for ev in events:
            home_team = ev.get("home_team", "")
            away_team = ev.get("away_team", "")
            r2 = requests.get(
                f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{ev['id']}/odds",
                params={"apiKey": ODDS_API_KEY, "regions": "us,us2",
                        "markets": "batter_hits", "oddsFormat": "american"}, timeout=15)
            if r2.status_code != 200: continue
            bms = r2.json().get("bookmakers", [])
            bm  = next((b for b in bms if b.get("key") in PREFERRED), bms[0] if bms else None)
            if not bm: continue
            for mkt in bm.get("markets", []):
                if mkt.get("key") != "batter_hits": continue
                pairs: dict = {}
                for oc in mkt.get("outcomes", []):
                    player = oc.get("description", "").strip()
                    pt     = oc.get("point")
                    side   = oc.get("name", "")
                    price  = oc.get("price")
                    if not player or pt is None: continue
                    key = (player, pt)
                    pairs.setdefault(key, {})
                    pairs[key][side] = price
                for (player, pt), sides in pairs.items():
                    if pt != 1.5 or player in seen: continue
                    seen[player] = {"name": player, "line": pt,
                                    "home_team": home_team, "away_team": away_team,
                                    "over_odds": sides.get("Over"),
                                    "under_odds": sides.get("Under")}

        _log(emit, f"  ✅ {len(seen)} players on 1.5 hits line")
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

    picks = []
    for c in candidates:
        name      = c["name"]
        home_team = c["home_team"]
        away_team = c["away_team"]
        batter_id   = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team: continue
        if _team_match(player_team, home_team):
            side, opp_name = "HOME", away_team
        elif _team_match(player_team, away_team):
            side, opp_name = "AWAY", home_team
        else:
            continue
        pitcher_name, pitcher_id = "TBD", None
        for pteam, pinfo in pitchers.items():
            if _team_match(pteam, opp_name):
                pitcher_name = pinfo["name"]
                pitcher_id   = pinfo.get("id")
                break
        s1 = _get_s1_vs_pitcher(batter_id, pitcher_id)
        if s1["ba"] is not None and s1["ab"] > 0 and s1["ba"] >= 0.250: continue
        s2 = fetch_step2_ba(batter_id, side, opp_name)
        if s2["ba"] is None or s2["ba"] >= 0.225: continue
        s3 = fetch_step3_ba(batter_id, side, season)
        if s3["ba"] is None or s3["ba"] >= 0.250: continue
        under_score = round((s2["ba"] + s3["ba"]) * 1000)
        picks.append({"name": name, "pos": "—", "side": side, "opp": opp_name,
                      "pitcher": pitcher_name, "s1_disp": s1["display"],
                      "s1_ab": s1["ab"], "s2": s2, "s3": s3,
                      "lineup_status": "TBD", "under_score": under_score})
        _log(emit, f"  ✅ UNDER: {name:<22}  S1:{s1['display']:<14}  S2:{s2['display']}  S3:{s3['display']}")

    picks.sort(key=lambda x: x["under_score"])
    _log(emit, f"✅ Under Picks: {len(picks)} picks found")
    return picks
