
"""
pitcher_k.py — Pitcher Strikeout Picks for MoneyBall.

Algorithm:
  Step 1 : Get pitcher K lines from The Odds API (pitcher_strikeouts market).
  Step 2 : Pull career H/A game logs vs today's specific opponent.
           Calculate avg Ks in those H/A starts.
  Pick   : avg > line -> OVER  |  avg < line -> UNDER
           Min 2 qualifying starts required.
"""
import os, time, requests
from datetime import date

ODDS_API_KEY  = os.environ.get("ODDS_API_KEY", "")
ODDS_BASE     = "https://api.the-odds-api.com/v4"
MLB_API       = "https://statsapi.mlb.com/api/v1"

MIN_STARTS       = 2
MIN_IP_START     = 3.0
K_SEASONS        = [2021, 2022, 2023, 2024, 2025, 2026]
SEASON           = "2026"
BOTTOM_K_TEAMS_N = 5

_pitcher_id_cache = {}
_team_id_cache    = {}


def _normalize(text: str) -> str:
    subs = {'á':'a','à':'a','ä':'a','é':'e','è':'e','ë':'e',
            'í':'i','ì':'i','ó':'o','ò':'o','ö':'o','ú':'u',
            'ù':'u','ü':'u','ñ':'n','ç':'c'}
    t = text.lower()
    for a, p in subs.items():
        t = t.replace(a, p)
    return t


def _teams_match(t1: str, t2: str) -> bool:
    n1, n2 = _normalize(t1), _normalize(t2)
    if n1 == n2 or n1 in n2 or n2 in n1: return True
    stop = {"of", "the", "los", "las", "san", "new", "de"}
    w1 = set(n1.split()) - stop
    w2 = set(n2.split()) - stop
    return len(w1 & w2) >= 2


def _ip_to_float(ip_str) -> float:
    try:
        parts = str(ip_str).split(".")
        return int(parts[0]) + (int(parts[1]) if len(parts) > 1 else 0) / 3.0
    except Exception:
        return 0.0


def _get_bottom_k_teams(season: str, n: int = BOTTOM_K_TEAMS_N):
    try:
        r = requests.get(f"{MLB_API}/teams/stats",
            params={"season": season, "sportId": 1, "group": "hitting", "stats": "season"},
            timeout=12)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        teams_data = []
        for sp in splits:
            stat = sp.get("stat", {})
            ks = stat.get("strikeOuts", 0)
            gp = stat.get("gamesPlayed", 1)
            if gp < 5: continue
            teams_data.append({"name": sp.get("team", {}).get("name", ""),
                                "k_per_g": round(ks / gp, 2)})
        teams_data.sort(key=lambda x: x["k_per_g"])
        bottom_n = teams_data[:n]
        return {t["name"] for t in bottom_n}, bottom_n
    except Exception:
        return set(), []


def _fetch_k_lines(run_date: str, emit=None) -> list:
    """Per-event Odds API call — works on all paid plans."""
    def log(m):
        if emit: emit({"type": "log", "msg": m})

    if not ODDS_API_KEY:
        log("⚠️  ODDS_API_KEY not set — Pitcher K Picks skipped")
        return []

    PREFERRED = ["draftkings", "fanduel", "betmgm", "caesars", "pointsbetus"]
    MARKETS   = ["pitcher_strikeouts", "pitcher_strikeouts_alternate"]
    tomorrow  = (date.today() + __import__('datetime').timedelta(days=1)).isoformat()

    try:
        r = requests.get(f"{ODDS_BASE}/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY, "dateFormat": "iso"}, timeout=15)
        if not r.ok:
            log(f"  ⚠️  Odds API events returned {r.status_code}")
            return []

        events = [e for e in r.json()
                  if e.get("commence_time", "")[:10] in (run_date, tomorrow)]
        log(f"  Odds API: {len(events)} games for {run_date}")
        seen: dict = {}

        for ev in events:
            home_team = ev.get("home_team", "")
            away_team = ev.get("away_team", "")
            for market in MARKETS:
                r2 = requests.get(
                    f"{ODDS_BASE}/sports/baseball_mlb/events/{ev['id']}/odds",
                    params={"apiKey": ODDS_API_KEY, "regions": "us",
                            "markets": market, "bookmakers": ",".join(PREFERRED),
                            "oddsFormat": "american"}, timeout=15)
                if not r2.ok: continue
                for bm in r2.json().get("bookmakers", []):
                    for mkt in bm.get("markets", []):
                        pairs: dict = {}
                        for oc in mkt.get("outcomes", []):
                            name  = (oc.get("description") or oc.get("name", "")).strip()
                            pt    = oc.get("point")
                            side  = oc.get("name", "")
                            price = oc.get("price")
                            if not name or pt is None: continue
                            key = _normalize(name)
                            pairs.setdefault(key, {"name": name, "point": float(pt)})
                            if side == "Over":    pairs[key]["over_odds"]  = price
                            elif side == "Under": pairs[key]["under_odds"] = price
                        for key, p in pairs.items():
                            if key not in seen:
                                seen[key] = {"name": p["name"], "line": p["point"],
                                             "home_team": home_team, "away_team": away_team,
                                             "over_odds": p.get("over_odds"),
                                             "under_odds": p.get("under_odds")}
                    break
        return list(seen.values())
    except Exception as exc:
        log(f"  ⚠️  Odds API error: {exc}")
        return []


def _get_pitcher_id(full_name: str):
    key = _normalize(full_name)
    if key in _pitcher_id_cache: return _pitcher_id_cache[key]
    try:
        last = full_name.strip().split()[-1]
        r = requests.get(f"{MLB_API}/people/search",
            params={"names": last, "sportId": 1}, timeout=8)
        norm = _normalize(full_name)
        candidates = r.json().get("people", [])
        for p in candidates:
            if (_normalize(p.get("fullName", "")) == norm and p.get("active") and
                    p.get("primaryPosition", {}).get("code") == "1"):
                _pitcher_id_cache[key] = p["id"]
                return p["id"]
        for p in candidates:
            if (_normalize(p.get("lastName", "")) == _normalize(last) and
                    p.get("active") and p.get("primaryPosition", {}).get("code") == "1"):
                _pitcher_id_cache[key] = p["id"]
                return p["id"]
    except Exception:
        pass
    return None


def _get_pitcher_team(pitcher_id: int) -> str:
    try:
        r = requests.get(f"{MLB_API}/people/{pitcher_id}",
            params={"hydrate": "currentTeam"}, timeout=8)
        return r.json()["people"][0].get("currentTeam", {}).get("name", "")
    except Exception:
        return ""


def _get_team_id(team_name: str):
    key = _normalize(team_name)
    if key in _team_id_cache: return _team_id_cache[key]
    try:
        r = requests.get(f"{MLB_API}/teams",
            params={"sportId": 1, "season": SEASON}, timeout=8)
        for t in r.json().get("teams", []):
            if _teams_match(t.get("name", ""), team_name):
                _team_id_cache[key] = t["id"]
                return t["id"]
    except Exception:
        pass
    return None


def _get_pitching_logs(pitcher_id: int, season: int) -> list:
    try:
        r = requests.get(f"{MLB_API}/people/{pitcher_id}/stats",
            params={"stats": "gameLog", "group": "pitching",
                    "season": season, "gameType": "R"}, timeout=12)
        data = r.json().get("stats", [])
        return data[0].get("splits", []) if data else []
    except Exception:
        return []


def career_ha_ks_vs_opp(pitcher_id: int, side: str, opp_name: str) -> dict:
    opp_id = _get_team_id(opp_name)
    time.sleep(0.1)
    if not opp_id: return None
    is_home = (side == "HOME")
    k_list   = []
    ip_list  = []
    era_list = []
    for season in reversed(K_SEASONS):
        splits = _get_pitching_logs(pitcher_id, season)
        time.sleep(0.08)
        for sp in reversed(splits):
            if sp.get("opponent", {}).get("id") != opp_id: continue
            if sp.get("isHome") != is_home: continue
            stat = sp.get("stat", {})
            ip = _ip_to_float(stat.get("inningsPitched", "0"))
            if ip < MIN_IP_START: continue
            k_list.append(stat.get("strikeOuts", 0))
            ip_list.append(ip)
            er = int(stat.get("earnedRuns", 0) or 0)
            if ip > 0:
                era_list.append(round(er / ip * 9, 2))
    if len(k_list) < MIN_STARTS:
        return {"avg_k": None, "starts": len(k_list), "k_list": k_list,
                "min_k": None, "max_k": None, "avg_ip": None, "era": None}
    avg_k  = round(sum(k_list) / len(k_list), 1)
    avg_ip = round(sum(ip_list) / len(ip_list), 1) if ip_list else None
    era    = round(sum(era_list) / len(era_list), 2) if era_list else None
    return {"avg_k": avg_k, "starts": len(k_list), "k_list": k_list,
            "min_k": min(k_list), "max_k": max(k_list),
            "avg_ip": avg_ip, "era": era}


def run_pitcher_k_picks(run_date: str, team_schedule: dict, emit=None) -> dict:
    if emit is None: emit = lambda _: None

    emit({"type": "section", "msg": "⚾ Pitcher K Picks — Fetching lines from The Odds API"})
    all_lines = _fetch_k_lines(run_date, emit)
    if not all_lines:
        emit({"type": "log", "msg": "⚠️ No pitcher K lines found"})
        return {"picks": [], "all": []}

    emit({"type": "log", "msg": f"✅ {len(all_lines)} pitcher K lines found"})
    bottom_k_set, bottom_k_list = _get_bottom_k_teams(SEASON)
    if bottom_k_set:
        emit({"type": "log", "msg": f"✅ Bottom {BOTTOM_K_TEAMS_N} K teams (DQ): " +
              ", ".join(f"{t['name']} ({t['k_per_g']} K/G)" for t in bottom_k_list)})

    emit({"type": "section", "msg": "⚾ Pitcher K Picks — Pulling career H/A K history"})
    all_results = []

    for pl in all_lines:
        name = pl["name"]
        line = pl["line"]
        pid  = _get_pitcher_id(name)
        time.sleep(0.15)
        if not pid:
            emit({"type": "log", "msg": f"  ⚠️ {name} — not found, skipping"})
            continue

        pitcher_team = _get_pitcher_team(pid)
        time.sleep(0.1)
        side = "HOME" if _teams_match(pitcher_team, pl["home_team"]) else "AWAY"
        opp  = pl["away_team"] if side == "HOME" else pl["home_team"]

        opp_k_info = next((t for t in bottom_k_list if _teams_match(t["name"], opp)), None)
        if bottom_k_set and opp_k_info:
            dq_note = f"Opp {opp} is bottom {BOTTOM_K_TEAMS_N} K team ({opp_k_info['k_per_g']} K/G)"
            emit({"type": "log", "msg": f"  ❌ {name} — {dq_note}"})
            all_results.append({"name": name, "team": pitcher_team, "opp": opp, "side": side,
                                 "line": line, "over_odds": pl.get("over_odds"),
                                 "under_odds": pl.get("under_odds"), "avg_k": None, "starts": 0,
                                 "min_k": None, "max_k": None, "k_history": "—",
                                 "pick": None, "pick_note": dq_note})
            continue

        emit({"type": "log", "msg": f"  {name}  K line:{line}  {side} vs {opp}..."})
        hist   = career_ha_ks_vs_opp(pid, side, opp)
        avg_k  = hist["avg_k"]  if hist else None
        starts = hist["starts"] if hist else 0
        k_list = hist["k_list"] if hist else []

        if avg_k is None:
            pick, pick_note = None, f"N/A — {starts} starts vs {opp}"
        elif avg_k > line:
            pick, pick_note = "OVER",  f"avg {avg_k} K > line {line}"
            emit({"type": "log", "msg": f"    ✅ OVER — avg {avg_k} > {line} ({starts} starts)"})
        elif avg_k < line:
            pick, pick_note = "UNDER", f"avg {avg_k} K < line {line}"
            emit({"type": "log", "msg": f"    ✅ UNDER — avg {avg_k} < {line} ({starts} starts)"})
        else:
            pick, pick_note = None, f"avg {avg_k} exactly on line"

        hits_over = sum(1 for k in k_list if k > line) if k_list else 0
        k_hit_rate = f"{hits_over}/{starts}" if starts else "—"
        all_results.append({"name": name, "team": pitcher_team, "opp": opp, "side": side,
                             "line": line, "over_odds": pl.get("over_odds"),
                             "under_odds": pl.get("under_odds"), "avg_k": avg_k,
                             "starts": starts, "min_k": hist["min_k"] if hist else None,
                             "max_k": hist["max_k"] if hist else None,
                             "avg_ip": hist["avg_ip"] if hist else None,
                             "era":    hist["era"]    if hist else None,
                             "k_hit_rate": k_hit_rate,
                             "k_history": ", ".join(str(k) for k in k_list) if k_list else "—",
                             "pick": pick, "pick_note": pick_note})

    confirmed = sorted([r for r in all_results if r["pick"]],
                       key=lambda r: abs((r["avg_k"] or 0) - r["line"]), reverse=True)
    emit({"type": "log", "msg": f"✅ Pitcher K done — {len(confirmed)} picks"})
    return {"picks": confirmed, "all": all_results}
