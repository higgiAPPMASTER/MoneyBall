
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
from concurrent.futures import ThreadPoolExecutor, as_completed

ODDS_API_KEY  = os.environ.get("ODDS_API_KEY", "")
ODDS_BASE     = "https://api.the-odds-api.com/v4"
MLB_API       = "https://statsapi.mlb.com/api/v1"

MIN_STARTS       = 1
MIN_IP_START     = 3.0
K_SEASONS        = [2021, 2022, 2023, 2024, 2025, 2026]
SEASON           = "2026"
BOTTOM_K_TEAMS_N = 0  # disabled — show all teams

# ── Pitcher prop O/U categories (real Odds API markets, parallel to K) ─────
# Each is a true Over/Under betting line that feeds the parlay builder.
# Data comes from the SAME pitching gameLog already pulled for K's:
#   hits allowed = stat["hits"], outs = inningsPitched×3, earned runs = stat["earnedRuns"].
PROP_MARKETS = ["pitcher_hits_allowed", "pitcher_outs", "pitcher_earned_runs", "pitcher_walks"]
# market -> (display label, per-start value field on the stat dicts, unit suffix)
PROP_META = {
    "pitcher_hits_allowed": ("Hits Allowed", "h",    "H"),
    "pitcher_outs":         ("Outs",         "outs", " outs"),
    "pitcher_earned_runs":  ("Earned Runs",  "er",   "ER"),
    "pitcher_walks":        ("Walks Allowed", "bb",  "BB"),
}
# Populated by _fetch_pitcher_props each run (cleared at the start so a warm
# process / 3×-day scheduler never serves a stale matchup):
#   {market: {norm_name: {name,line,over_odds,under_odds,home_team,away_team}}}
PROP_ODDS = {}

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
    # Tomorrow's UTC date derived from run_date (matches under_picks). Used only
    # to keep tonight's late games that roll past midnight UTC — NOT to pull
    # tomorrow's actual slate.
    tomorrow  = (time.strftime("%Y-%m-%d",
                  time.gmtime(time.mktime(time.strptime(run_date, "%Y-%m-%d")) + 86400)))

    try:
        r = requests.get(f"{ODDS_BASE}/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY, "dateFormat": "iso"}, timeout=15)
        if not r.ok:
            log(f"  ⚠️  Odds API events returned {r.status_code}")
            return []

        def _is_run_date_game(ct: str) -> bool:
            """True only for run_date games, plus tonight's late games that roll
            into tomorrow's UTC date before 09:00 UTC. Tomorrow's real slate is
            excluded."""
            if not ct: return False
            day = ct[:10]
            if day == run_date: return True
            if day == tomorrow:
                try:
                    return int(ct[11:13]) < 9
                except Exception:
                    return False
            return False
        events = [e for e in r.json()
                  if _is_run_date_game(e.get("commence_time", ""))]
        log(f"  Odds API: {len(events)} games for {run_date}")
        seen: dict = {}
        ladder: dict = {}

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
                            if side == "Over" and price is not None:
                                ladder.setdefault(key, {}).setdefault(float(pt), price)
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
        for key, entry in seen.items():
            entry["over_ladder"] = ladder.get(key, {})
        return list(seen.values())
    except Exception as exc:
        log(f"  ⚠️  Odds API error: {exc}")
        return []


def _fetch_pitcher_props(run_date: str, emit=None) -> None:
    """Pull the 3 pitcher prop O/U lines (hits allowed / outs / earned runs) into
    the module-global PROP_ODDS. ONE Odds API call per event (all 3 markets in a
    single request) — separate from the K fetch so the proven K path is untouched.
    First posted line per pitcher per market wins; captures Over AND Under odds."""
    def log(m):
        if emit: emit({"type": "log", "msg": m})

    PROP_ODDS.clear()
    for m in PROP_MARKETS:
        PROP_ODDS[m] = {}
    if not ODDS_API_KEY:
        log("⚠️  ODDS_API_KEY not set — Pitcher prop picks skipped")
        return

    PREFERRED = ["draftkings", "fanduel", "betmgm", "caesars", "pointsbetus"]
    tomorrow  = (time.strftime("%Y-%m-%d",
                  time.gmtime(time.mktime(time.strptime(run_date, "%Y-%m-%d")) + 86400)))

    def _is_run_date_game(ct: str) -> bool:
        if not ct: return False
        day = ct[:10]
        if day == run_date: return True
        if day == tomorrow:
            try:
                return int(ct[11:13]) < 9
            except Exception:
                return False
        return False

    try:
        r = requests.get(f"{ODDS_BASE}/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY, "dateFormat": "iso"}, timeout=15)
        if not r.ok:
            log(f"  ⚠️  Odds API events returned {r.status_code} (props)")
            return
        events = [e for e in r.json()
                  if _is_run_date_game(e.get("commence_time", ""))]
        for ev in events:
            home_team = ev.get("home_team", "")
            away_team = ev.get("away_team", "")
            r2 = requests.get(
                f"{ODDS_BASE}/sports/baseball_mlb/events/{ev['id']}/odds",
                params={"apiKey": ODDS_API_KEY, "regions": "us",
                        "markets": ",".join(PROP_MARKETS),
                        "bookmakers": ",".join(PREFERRED),
                        "oddsFormat": "american"}, timeout=15)
            if not r2.ok: continue
            for bm in r2.json().get("bookmakers", []):
                for mkt in bm.get("markets", []):
                    mk = mkt.get("key")
                    if mk not in PROP_ODDS: continue
                    for oc in mkt.get("outcomes", []):
                        name  = (oc.get("description") or oc.get("name", "")).strip()
                        pt    = oc.get("point")
                        side  = oc.get("name", "")
                        price = oc.get("price")
                        if not name or pt is None or price is None: continue
                        key = _normalize(name)
                        d = PROP_ODDS[mk].get(key)
                        if d is None:
                            d = {"name": name, "line": float(pt),
                                 "home_team": home_team, "away_team": away_team,
                                 "over_odds": None, "under_odds": None}
                            PROP_ODDS[mk][key] = d
                        if abs(float(pt) - d["line"]) > 1e-9:
                            continue  # only the first-seen line for this pitcher
                        if side == "Over" and d["over_odds"] is None:
                            d["over_odds"] = price
                        elif side == "Under" and d["under_odds"] is None:
                            d["under_odds"] = price
        log(f"  Pitcher props: " +
            ", ".join(f"{PROP_META[m][0]} {len(PROP_ODDS[m])}" for m in PROP_MARKETS))
    except Exception as exc:
        log(f"  ⚠️  Odds API error (props): {exc}")


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


def _get_recent_k_form(pitcher_id: int, n: int = 5) -> dict:
    """Last n actual starts (any opponent) from the current season."""
    splits = _get_pitching_logs(pitcher_id, int(SEASON))
    starts = [sp for sp in splits
              if _ip_to_float(sp.get("stat", {}).get("inningsPitched", "0")) >= MIN_IP_START]
    recent = starts[-n:]
    if not recent:
        return {"recent_avg_k": None, "recent_k_list": [], "recent_starts": 0, "recent_k_log": [],
                "recent_avg_hits": None, "recent_hits_list": [],
                "recent_avg_er": None, "recent_er_list": [],
                "recent_avg_outs": None, "recent_outs_list": [],
                "recent_avg_bb": None, "recent_bb_list": []}
    k_list    = [sp.get("stat", {}).get("strikeOuts", 0) for sp in recent]
    h_list    = [int(sp.get("stat", {}).get("hits", 0) or 0) for sp in recent]
    er_list   = [int(sp.get("stat", {}).get("earnedRuns", 0) or 0) for sp in recent]
    outs_list = [round(_ip_to_float(sp.get("stat", {}).get("inningsPitched", "0")) * 3) for sp in recent]
    bb_list   = [int(sp.get("stat", {}).get("baseOnBalls", 0) or 0) for sp in recent]
    k_log = [{
        "d": (sp.get("date") or "")[5:],
        "v": sp.get("stat", {}).get("strikeOuts", 0),
        "h": int(sp.get("stat", {}).get("hits", 0) or 0),
        "er": int(sp.get("stat", {}).get("earnedRuns", 0) or 0),
        "outs": round(_ip_to_float(sp.get("stat", {}).get("inningsPitched", "0")) * 3),
        "bb": int(sp.get("stat", {}).get("baseOnBalls", 0) or 0),
        "ip": sp.get("stat", {}).get("inningsPitched", ""),
        "opp": (sp.get("opponent", {}) or {}).get("name", ""),
    } for sp in reversed(recent)]
    return {
        "recent_avg_k": round(sum(k_list) / len(k_list), 1),
        "recent_k_list": k_list,
        "recent_starts": len(k_list),
        "recent_k_log": k_log,
        "recent_avg_hits": round(sum(h_list) / len(h_list), 1) if h_list else None,
        "recent_hits_list": h_list,
        "recent_avg_er": round(sum(er_list) / len(er_list), 1) if er_list else None,
        "recent_er_list": er_list,
        "recent_avg_outs": round(sum(outs_list) / len(outs_list), 1) if outs_list else None,
        "recent_outs_list": outs_list,
        "recent_avg_bb": round(sum(bb_list) / len(bb_list), 1) if bb_list else None,
        "recent_bb_list": bb_list,
    }


def career_ha_ks_vs_opp(pitcher_id: int, side: str, opp_name: str) -> dict:
    opp_id = _get_team_id(opp_name)
    time.sleep(0.1)
    if not opp_id: return None
    is_home = (side == "HOME")
    k_list    = []
    ip_list   = []
    era_list  = []
    h_list    = []          # hits allowed per start vs opp
    er_list   = []          # earned runs per start vs opp
    outs_list = []          # outs recorded per start vs opp (IP×3)
    bb_list   = []          # walks allowed per start vs opp
    vs_log    = []          # dated per-start log vs opp (K + hits + ER + outs + BB)
    for season in reversed(K_SEASONS):
        splits = _get_pitching_logs(pitcher_id, season)
        time.sleep(0.08)
        for sp in reversed(splits):
            if sp.get("opponent", {}).get("id") != opp_id: continue
            if sp.get("isHome") != is_home: continue
            stat = sp.get("stat", {})
            ip = _ip_to_float(stat.get("inningsPitched", "0"))
            if ip < MIN_IP_START: continue
            k = stat.get("strikeOuts", 0)
            h = int(stat.get("hits", 0) or 0)   # "hits" in pitching gameLog = hits ALLOWED
            er = int(stat.get("earnedRuns", 0) or 0)
            bb = int(stat.get("baseOnBalls", 0) or 0)   # walks ALLOWED
            outs = round(ip * 3)
            k_list.append(k)
            h_list.append(h)
            er_list.append(er)
            bb_list.append(bb)
            outs_list.append(outs)
            ip_list.append(ip)
            if ip > 0:
                era_list.append(round(er / ip * 9, 2))
            vs_log.append({"d": (sp.get("date") or ""), "k": k, "h": h, "er": er,
                           "bb": bb, "outs": outs, "ip": stat.get("inningsPitched", "")})
    # newest-first; compact the date to YY-MM-DD (vs-opp log spans seasons)
    vs_log.sort(key=lambda e: e["d"], reverse=True)
    for e in vs_log:
        e["d"] = (e["d"] or "")[2:]
    if len(k_list) < MIN_STARTS:
        return {"avg_k": None, "starts": len(k_list), "k_list": k_list,
                "min_k": None, "max_k": None, "avg_ip": None, "era": None,
                "avg_hits": None, "h_list": h_list, "avg_er": None, "er_list": er_list,
                "avg_outs": None, "outs_list": outs_list,
                "avg_bb": None, "bb_list": bb_list, "vs_opp_log": vs_log}
    avg_k    = round(sum(k_list) / len(k_list), 1)
    avg_ip   = round(sum(ip_list) / len(ip_list), 1) if ip_list else None
    era      = round(sum(era_list) / len(era_list), 2) if era_list else None
    avg_hits = round(sum(h_list) / len(h_list), 1) if h_list else None
    avg_er   = round(sum(er_list) / len(er_list), 1) if er_list else None
    avg_outs = round(sum(outs_list) / len(outs_list), 1) if outs_list else None
    avg_bb   = round(sum(bb_list) / len(bb_list), 1) if bb_list else None
    return {"avg_k": avg_k, "starts": len(k_list), "k_list": k_list,
            "min_k": min(k_list), "max_k": max(k_list),
            "avg_ip": avg_ip, "era": era,
            "avg_hits": avg_hits, "h_list": h_list,
            "avg_er": avg_er, "er_list": er_list,
            "avg_outs": avg_outs, "outs_list": outs_list,
            "avg_bb": avg_bb, "bb_list": bb_list, "vs_opp_log": vs_log}


def _fetch_probable_starters(run_date: str) -> list:
    """Fetch today's probable starting pitchers from MLB schedule API."""
    try:
        r = requests.get(
            f"{MLB_API}/schedule",
            params={"sportId": 1, "date": run_date,
                    "hydrate": "probablePitcher,team", "gameType": "R"},
            timeout=12)
        starters = []
        for d in r.json().get("dates", []):
            for game in d.get("games", []):
                home_team = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
                away_team = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
                for side_key, side_val in [("home", "HOME"), ("away", "AWAY")]:
                    t = game.get("teams", {}).get(side_key, {})
                    pitcher = t.get("probablePitcher", {})
                    if pitcher and pitcher.get("fullName"):
                        opp = away_team if side_val == "HOME" else home_team
                        starters.append({
                            "name": pitcher.get("fullName", "TBD"),
                            "mlb_id": pitcher.get("id"),
                            "side": side_val,
                            "team": home_team if side_val == "HOME" else away_team,
                            "opp": opp,
                        })
        return starters
    except Exception:
        return []


def _blend(career_avg, recent_avg, recent_n):
    """50/50 blend of career-vs-opp avg + recent-form avg (mirrors the K logic)."""
    if career_avg is not None and recent_avg is not None:
        return round((career_avg + recent_avg) / 2, 1), f"career {career_avg} · L{recent_n} {recent_avg}"
    if career_avg is not None:
        return career_avg, f"career {career_avg} only"
    if recent_avg is not None:
        return recent_avg, f"L{recent_n} {recent_avg} only (no career vs opp)"
    return None, "no data"


# market -> (career avg field, career list field, recent avg field, recent list field)
_PROP_SRC = {
    "pitcher_hits_allowed": ("avg_hits", "h_list",    "recent_avg_hits", "recent_hits_list"),
    "pitcher_outs":         ("avg_outs", "outs_list", "recent_avg_outs", "recent_outs_list"),
    "pitcher_earned_runs":  ("avg_er",   "er_list",   "recent_avg_er",   "recent_er_list"),
    "pitcher_walks":        ("avg_bb",   "bb_list",   "recent_avg_bb",   "recent_bb_list"),
}


def _build_prop_picks(name, team, opp, side, hist, rf) -> list:
    """One Over/Under pick per prop market that has a posted line in PROP_ODDS.
    Uniform dict so the frontend can render all 3 categories generically."""
    props = []
    nkey = _normalize(name)
    for market in PROP_MARKETS:
        odds = PROP_ODDS.get(market, {}).get(nkey)
        if not odds:
            continue
        label, vfield, unit = PROP_META[market]
        c_avg_f, c_list_f, r_avg_f, r_list_f = _PROP_SRC[market]
        career_avg  = hist.get(c_avg_f) if hist else None
        career_list = (hist.get(c_list_f) if hist else None) or []
        recent_avg  = rf.get(r_avg_f) if rf else None
        recent_n    = rf.get("recent_starts", 0) if rf else 0
        line = odds["line"]
        blended, blend_src = _blend(career_avg, recent_avg, recent_n)
        if blended is None:
            pick, pick_note = None, f"no data vs {opp}"
        elif blended > line:
            pick, pick_note = "OVER",  f"blend {blended} > line {line} ({blend_src})"
        elif blended < line:
            pick, pick_note = "UNDER", f"blend {blended} < line {line} ({blend_src})"
        else:
            pick, pick_note = None, f"blend {blended} exactly on line {line}"
        starts = hist.get("starts", 0) if hist else 0
        over_hits = sum(1 for v in career_list if v is not None and v > line)
        hit_rate = f"{over_hits}/{len(career_list)}" if career_list else "—"
        vs_opp_log = [{"d": e.get("d", ""), "v": e.get(vfield), "ip": e.get("ip", "")}
                      for e in ((hist.get("vs_opp_log") if hist else None) or [])]
        recent_log = [{"d": e.get("d", ""), "v": e.get(vfield), "ip": e.get("ip", ""),
                       "opp": e.get("opp", "")}
                      for e in ((rf.get("recent_k_log") if rf else None) or [])]
        props.append({
            "market": market, "label": label, "unit": unit, "_prop": True,
            "name": name, "team": team, "opp": opp, "side": side,
            "line": line, "over_odds": odds.get("over_odds"), "under_odds": odds.get("under_odds"),
            "career_avg": career_avg, "recent_avg": recent_avg, "recent_starts": recent_n,
            "blended": blended, "avg": blended, "blend_src": blend_src, "starts": starts,
            "vs_opp_log": vs_opp_log, "recent_log": recent_log,
            "hit_rate": hit_rate, "pick": pick, "pick_note": pick_note,
        })
    return props


def run_pitcher_k_picks(run_date: str, team_schedule: dict, emit=None) -> dict:
    if emit is None: emit = lambda _: None

    emit({"type": "section", "msg": "⚾ Pitcher K Picks — Fetching lines from The Odds API"})
    all_lines = _fetch_k_lines(run_date, emit)
    if not all_lines:
        # No K lines today — but the 3 prop markets (hits/outs/ER) may still have
        # lines posted. Do NOT bail; continue so probable starters get prop picks
        # built (maximizes parlay depth even on no-K days).
        emit({"type": "log", "msg": "⚠️ No pitcher K lines found — continuing for prop markets"})
        all_lines = []
    else:
        emit({"type": "log", "msg": f"✅ {len(all_lines)} pitcher K lines found"})

    bottom_k_set, bottom_k_list = _get_bottom_k_teams(SEASON)
    if bottom_k_set:
        emit({"type": "log", "msg": f"✅ Bottom {BOTTOM_K_TEAMS_N} K teams (DQ): " +
              ", ".join(f"{t['name']} ({t['k_per_g']} K/G)" for t in bottom_k_list)})

    # Fetch the 3 prop O/U lines (hits allowed / outs / earned runs) into PROP_ODDS
    # BEFORE the eval loop so each worker can attach its prop picks. Separate fetch
    # from K — never blocks the K path if props fail.
    emit({"type": "section", "msg": "⚾ Pitcher Props — Fetching hits/outs/earned-runs lines"})
    try:
        _fetch_pitcher_props(run_date, emit)
    except Exception as _pexc:
        emit({"type": "log", "msg": f"  ⚠️ Pitcher props fetch failed: {_pexc}"})
        PROP_ODDS.clear()

    emit({"type": "section", "msg": "⚾ Pitcher K Picks — Pulling career H/A K history"})
    all_results = []

    # Pull each pitcher's H/A K history in parallel (≤8 threads). Each worker is
    # independent (3 MLB Stats API calls); the id/team caches are GIL-safe. Each
    # worker returns (result, logs) and the main thread emits logs as futures
    # complete so the live progress feed stays intact. Sequential time.sleep
    # pacing is no longer needed — the bounded pool throttles concurrency.
    def _eval_pitcher(pl):
        logs = []
        name = pl["name"]
        line = pl["line"]
        pid  = _get_pitcher_id(name)
        if not pid:
            logs.append(f"  ⚠️ {name} — not found, skipping")
            return None, logs

        # Prefer the big-league club from today's probable-starters schedule;
        # _get_pitcher_team (currentTeam) can be a minor-league affiliate for
        # optioned pitchers (e.g. "Toledo Mud Hens" instead of "Detroit Tigers").
        pitcher_team = prob_team_map.get(_normalize(name)) or _get_pitcher_team(pid)
        side = "HOME" if _teams_match(pitcher_team, pl["home_team"]) else "AWAY"
        opp  = pl["away_team"] if side == "HOME" else pl["home_team"]

        opp_k_info = next((t for t in bottom_k_list if _teams_match(t["name"], opp)), None)
        if bottom_k_set and opp_k_info:
            dq_note = f"Opp {opp} is bottom {BOTTOM_K_TEAMS_N} K team ({opp_k_info['k_per_g']} K/G)"
            logs.append(f"  ❌ {name} — {dq_note}")
            return ({"name": name, "team": pitcher_team, "opp": opp, "side": side,
                     "line": line, "over_odds": pl.get("over_odds"),
                     "under_odds": pl.get("under_odds"), "avg_k": None, "starts": 0,
                     "min_k": None, "max_k": None, "k_history": "—",
                     "pick": None, "pick_note": dq_note}), logs

        logs.append(f"  {name}  K line:{line}  {side} vs {opp}...")
        hist   = career_ha_ks_vs_opp(pid, side, opp)
        avg_k  = hist["avg_k"]  if hist else None
        starts = hist["starts"] if hist else 0
        k_list = hist["k_list"] if hist else []

        # Recent form: last 5 starts any opponent (current season)
        rf = _get_recent_k_form(pid)
        recent_avg_k  = rf["recent_avg_k"]
        recent_k_list = rf["recent_k_list"]
        recent_starts = rf["recent_starts"]

        # Blended average: 50/50 career H/A vs opp + recent form
        if avg_k is not None and recent_avg_k is not None:
            blended_avg = round((avg_k + recent_avg_k) / 2, 1)
            blend_src   = f"career {avg_k} · L{recent_starts} {recent_avg_k}"
        elif avg_k is not None:
            blended_avg = avg_k
            blend_src   = f"career {avg_k} only"
        elif recent_avg_k is not None:
            blended_avg = recent_avg_k
            blend_src   = f"L{recent_starts} {recent_avg_k} only (no career vs opp)"
        else:
            blended_avg = None
            blend_src   = "no data"

        sugg_line, sugg_odds = None, None
        if blended_avg is None:
            pick, pick_note = None, f"N/A — {starts} starts vs {opp}, no recent data"
        elif blended_avg > line:
            pick, pick_note = "OVER",  f"blend {blended_avg} > line {line} ({blend_src})"
            logs.append(f"    ✅ OVER blend {blended_avg} > {line} ({blend_src})")
        elif blended_avg < line:
            pick, pick_note = "UNDER", f"blend {blended_avg} < line {line} ({blend_src})"
            logs.append(f"    ✅ UNDER blend {blended_avg} < {line} ({blend_src})")
        else:
            # blended exactly on line → try alt line from career k_list floor
            sugg_line = (min(k_list) - 0.5) if k_list else None
            k_ladder  = pl.get("over_ladder") or {}
            sugg_odds = k_ladder.get(sugg_line) if sugg_line is not None else None
            if sugg_line is not None and sugg_line < line:
                pick = "OVER"
                pick_note = (f"blend {blended_avg} on line {line} → floor OVER {sugg_line} ({blend_src})")
                logs.append(f"    ✅ OVER {sugg_line} (alt) blend on line")
            else:
                pick, pick_note = None, f"blend {blended_avg} exactly on line"
                sugg_line, sugg_odds = None, None

        hits_over = sum(1 for k in k_list if k > line) if k_list else 0
        k_hit_rate = f"{hits_over}/{starts}" if starts else "—"
        return ({"name": name, "team": pitcher_team, "opp": opp, "side": side,
                 "line": line, "over_odds": pl.get("over_odds"),
                 "under_odds": pl.get("under_odds"), "avg_k": avg_k,
                 "starts": starts, "min_k": hist["min_k"] if hist else None,
                 "max_k": hist["max_k"] if hist else None,
                 "avg_ip": hist["avg_ip"] if hist else None,
                 "era":    hist["era"]    if hist else None,
                 "avg_hits":   hist["avg_hits"]   if hist else None,
                 "avg_bb":     hist["avg_bb"]     if hist else None,
                 "vs_opp_log": hist["vs_opp_log"] if hist else [],
                 "k_hit_rate": k_hit_rate,
                 "k_history": ", ".join(str(k) for k in k_list) if k_list else "—",
                 "sugg_line": sugg_line, "sugg_odds": sugg_odds,
                 "recent_avg_k": recent_avg_k, "recent_k_list": recent_k_list,
                 "recent_starts": recent_starts, "recent_k_log": rf["recent_k_log"],
                 "blended_avg_k": blended_avg, "blend_src": blend_src,
                 "props": _build_prop_picks(name, pitcher_team, opp, side, hist, rf),
                 "pick": pick, "pick_note": pick_note}), logs

    # Today's probable starters — used to map each pitcher to his big-league club
    # (so optioned pitchers don't show their minor-league affiliate) and reused
    # below for no-K-line starters (avoids a second schedule API call).
    prob_starters = _fetch_probable_starters(run_date)
    prob_team_map = {_normalize(s["name"]): s["team"] for s in prob_starters if s.get("team")}

    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = [_ex.submit(_eval_pitcher, pl) for pl in all_lines]
        for _fut in as_completed(_futs):
            try:
                _res, _logs = _fut.result()
            except Exception as _exc:
                _res, _logs = None, [f"  ⚠️ pitcher eval failed: {_exc}"]
            for _m in _logs:
                emit({"type": "log", "msg": _m})
            if _res:
                all_results.append(_res)

    # Add today's probable starters who have no K line posted
    try:
        starters = prob_starters
        seen_names = {_normalize(r["name"]) for r in all_results}
        new_starters = [st for st in starters if _normalize(st["name"]) not in seen_names]

        def _eval_starter(st):
            pid2 = _get_pitcher_id(st["name"])
            hist2 = career_ha_ks_vs_opp(pid2, st["side"], st["opp"]) if pid2 else None
            avg_k2 = hist2["avg_k"] if hist2 else None
            starts2 = hist2["starts"] if hist2 else 0
            k_list2 = hist2["k_list"] if hist2 else []
            k_history2 = ", ".join(str(k) for k in k_list2) if k_list2 else "—"
            rf2 = _get_recent_k_form(pid2) if pid2 else {"recent_avg_k": None, "recent_k_list": [], "recent_starts": 0, "recent_k_log": []}
            return {
                "name": st["name"], "team": st["team"], "opp": st["opp"],
                "side": st["side"], "line": None,
                "over_odds": None, "under_odds": None,
                "avg_k": avg_k2, "starts": starts2,
                "min_k": min(k_list2) if k_list2 else None,
                "max_k": max(k_list2) if k_list2 else None,
                "avg_ip": hist2["avg_ip"] if hist2 else None,
                "era": hist2["era"] if hist2 else None,
                "avg_hits": hist2["avg_hits"] if hist2 else None,
                "avg_bb": hist2["avg_bb"] if hist2 else None,
                "vs_opp_log": hist2["vs_opp_log"] if hist2 else [],
                "k_hit_rate": "—", "k_history": k_history2,
                "recent_avg_k": rf2["recent_avg_k"], "recent_k_list": rf2["recent_k_list"],
                "recent_starts": rf2["recent_starts"], "recent_k_log": rf2["recent_k_log"],
                "blended_avg_k": None, "blend_src": None,
                # Pitchers with NO K line may still have hits/outs/ER lines posted —
                # build their prop picks too so the parlay pool is as deep as possible.
                "props": _build_prop_picks(st["name"], st["team"], st["opp"], st["side"], hist2, rf2),
                "pick": None, "pick_note": "No K line posted today",
            }

        with ThreadPoolExecutor(max_workers=8) as _ex:
            for _r in _ex.map(_eval_starter, new_starters):
                all_results.append(_r)
        emit({"type": "log", "msg": f"  ✅ {len(starters)} probable starters fetched — "
              f"{len([r for r in all_results if r.get('pick_note')=='No K line posted today'])} added (no line)"})
    except Exception as exc:
        emit({"type": "log", "msg": f"  ⚠️ Probable starters fetch failed: {exc}"})

    # name as a deterministic tie-breaker (workers finish out of order); stable
    # sort keeps name order within equal edge sizes.
    confirmed = sorted([r for r in all_results if r["pick"]], key=lambda r: r["name"])
    confirmed = sorted(confirmed,
                       key=lambda r: abs((r["blended_avg_k"] if r["blended_avg_k"] is not None else (r["avg_k"] or 0)) - (r["line"] or 0)), reverse=True)
    emit({"type": "log", "msg": f"✅ Pitcher K done — {len(confirmed)} picks, {len(all_results)} total pitchers"})

    # Collect the 3 prop categories from every pitcher's embedded `props` list.
    # picks = qualifying (has a pick), ranked by edge (|blend − line|) desc;
    # all   = every pitcher with a posted line in that market (for the no-pick table).
    prop_picks = {m: {"picks": [], "all": []} for m in PROP_MARKETS}
    for r in all_results:
        for pr in r.get("props", []):
            m = pr.get("market")
            if m not in prop_picks:
                continue
            prop_picks[m]["all"].append(pr)
            if pr.get("pick"):
                prop_picks[m]["picks"].append(pr)
    for m in prop_picks:
        prop_picks[m]["picks"].sort(
            key=lambda x: abs((x["blended"] if x["blended"] is not None else 0) - (x["line"] or 0)),
            reverse=True)
        prop_picks[m]["all"].sort(key=lambda x: x.get("name", ""))
    emit({"type": "log", "msg": "✅ Pitcher props — " +
          ", ".join(f"{PROP_META[m][0]}: {len(prop_picks[m]['picks'])}" for m in PROP_MARKETS)})

    return {"picks": confirmed, "all": all_results, "props": prop_picks}
