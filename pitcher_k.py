"""
pitcher_k.py — MLB Pitcher Strikeout Picks for MoneyBall
=========================================================
Columns returned: name, team, side, opp, line,
                  avg_k, avg_ip, era, k_hit_rate,
                  starts, k_history, pick, over_odds, under_odds
Data sources:
  - MLB Stats API  (schedule, game logs, season ERA)  free, no key
  - The Odds API   (pitcher_strikeouts O/U lines)      ODDS_API_KEY env var
"""
import os, requests
from datetime import datetime

MLB          = "https://statsapi.mlb.com/api/v1"
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
CUR_YEAR     = datetime.now().year
MIN_STARTS   = 2     # min H/A starts vs opponent to issue a pick
GAP_THRESH   = 0.4   # avg_k must differ from line by this much to pick


# ─────────────────  HELPERS  ────────────────────────────────────────────────

def _req(url, **kw):
    try:
        return requests.get(url, timeout=13, **kw).json()
    except Exception:
        return {}


def _ip_to_float(ip_str) -> float:
    """'6.1' -> 6.333  |  '5.2' -> 5.667"""
    try:
        parts = str(ip_str).split(".")
        full   = int(parts[0])
        thirds = int(parts[1]) if len(parts) > 1 else 0
        return full + thirds / 3.0
    except Exception:
        return 0.0


def _hit_rate_label(over: int, total: int) -> str:
    pct   = round(over / total * 100)
    emoji = "🟢" if pct >= 65 else ("🟡" if pct >= 40 else "🔴")
    return f"{over}/{total} ({pct}%) {emoji}"


# ─────────────────  DATA  ───────────────────────────────────────────────────

def _schedule(date_str: str) -> list:
    d = _req(f"{MLB}/schedule", params={
        "date": date_str, "sportId": 1, "hydrate": "probablePitcher,team",
    })
    out = []
    for dd in d.get("dates", []):
        for g in dd.get("games", []):
            ht = g["teams"]["home"]
            at = g["teams"]["away"]
            hp = ht.get("probablePitcher")
            ap = at.get("probablePitcher")
            hn = ht.get("team", {}).get("name", "")
            an = at.get("team", {}).get("name", "")
            hi = ht.get("team", {}).get("id")
            ai = at.get("team", {}).get("id")
            if hp:
                out.append({"pitcher_id": hp["id"], "pitcher_name": hp["fullName"],
                            "team": hn, "side": "HOME", "opp": an, "opp_id": ai})
            if ap:
                out.append({"pitcher_id": ap["id"], "pitcher_name": ap["fullName"],
                            "team": an, "side": "AWAY", "opp": hn, "opp_id": hi})
    return out


def _k_lines() -> dict:
    """
    Pitcher strikeout O/U lines from The Odds API.
    Returns {last_name_lower: {line, over_odds, under_odds}}
    """
    if not ODDS_API_KEY:
        print("[pitcher_k] ODDS_API_KEY not set — add it to Render environment variables")
        return {}

    lines: dict = {}
    try:
        r   = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY}, timeout=10,
        )
        raw = r.json()
        if isinstance(raw, dict) and raw.get("message"):
            print(f"[pitcher_k] Odds API error: {raw['message']}")
            return {}
        event_ids = [e["id"] for e in raw] if isinstance(raw, list) else []
        print(f"[pitcher_k] Odds API: {len(event_ids)} events found")
    except Exception as exc:
        print(f"[pitcher_k] Odds API events error: {exc}")
        return {}

    for eid in event_ids[:20]:
        try:
            r2 = requests.get(
                f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{eid}/odds",
                params={"apiKey": ODDS_API_KEY, "regions": "us,us2",
                        "markets": "pitcher_strikeouts", "oddsFormat": "american"},
                timeout=10,
            )
            for bookie in r2.json().get("bookmakers", []):
                for mkt in bookie.get("markets", []):
                    if mkt.get("key") != "pitcher_strikeouts":
                        continue
                    over_p = under_p = line_v = name_k = None
                    for oc in mkt.get("outcomes", []):
                        desc = oc.get("description", "").lower().strip()
                        if oc.get("name") == "Over":
                            line_v = oc.get("point")
                            over_p = oc.get("price")
                            name_k = desc
                        elif oc.get("name") == "Under":
                            under_p = oc.get("price")
                    if name_k and line_v is not None and name_k not in lines:
                        lines[name_k] = {"line": line_v, "over_odds": over_p, "under_odds": under_p}
        except Exception:
            continue
    return lines


def _pitcher_logs(pitcher_id: int) -> list:
    logs = []
    for season in [CUR_YEAR, CUR_YEAR - 1, CUR_YEAR - 2]:
        try:
            d = _req(f"{MLB}/people/{pitcher_id}/stats", params={
                "stats": "gameLog", "group": "pitching",
                "season": season, "hydrate": "team,opponent",
            })
            logs.extend(d.get("stats", [{}])[0].get("splits", []))
        except Exception:
            pass
    return logs


def _pitcher_era(pitcher_id: int) -> str:
    try:
        d = _req(f"{MLB}/people/{pitcher_id}/stats", params={
            "stats": "season", "group": "pitching", "season": CUR_YEAR,
        })
        splits = d.get("stats", [{}])[0].get("splits", [])
        if splits:
            return str(splits[0].get("stat", {}).get("era", "-.--"))
    except Exception:
        pass
    return "-.--"


def _filter_logs(logs: list, side: str, opp_id) -> list:
    want_home = (side == "HOME")
    return [
        g for g in logs
        if g.get("isHome", False) == want_home
        and (opp_id is None or g.get("opponent", {}).get("id") == opp_id)
    ]


def _match_line(name: str, k_lines: dict):
    nl   = name.lower()
    last = nl.split()[-1] if nl else ""
    if nl in k_lines:
        return k_lines[nl]
    for key, val in k_lines.items():
        if last and (last in key or key.endswith(last)):
            return val
    return None


# ─────────────────  MAIN  ───────────────────────────────────────────────────

def get_pitcher_k_picks(date_str: str, emit=None) -> dict:
    def log(msg):
        if emit:
            emit({"type": "log", "msg": msg})

    log("⚾ Pitcher K: fetching schedule…")
    matchups = _schedule(date_str)
    if not matchups:
        log("⚾ Pitcher K: no probable pitchers found")
        return {"picks": [], "all": []}

    log(f"⚾ Pitcher K: {len(matchups)} pitchers — fetching K lines from Odds API…")
    lines = _k_lines()
    log(f"⚾ Pitcher K: {len(lines)} K lines returned")

    picks:   list = []
    all_pit: list = []

    for m in matchups:
        pid   = m["pitcher_id"]
        name  = m["pitcher_name"]
        team  = m["team"]
        side  = m["side"]
        opp   = m["opp"]
        opp_id = m["opp_id"]

        ld = _match_line(name, lines)
        if not ld:
            all_pit.append({"name": name, "team": team, "side": side, "opp": opp,
                            "line": None, "avg_k": None, "avg_ip": None, "era": None,
                            "k_hit_rate": None, "k_hit_rate_pct": None,
                            "starts": 0, "k_history": "", "pick": None,
                            "over_odds": None, "under_odds": None,
                            "pick_note": "No K line available"})
            continue

        k_line     = ld["line"]
        over_odds  = ld.get("over_odds")
        under_odds = ld.get("under_odds")

        logs     = _pitcher_logs(pid)
        opp_logs = _filter_logs(logs, side, opp_id)
        era      = _pitcher_era(pid)

        base = {"name": name, "team": team, "side": side, "opp": opp,
                "line": k_line, "over_odds": over_odds, "under_odds": under_odds, "era": era}

        if len(opp_logs) < MIN_STARTS:
            base.update({"avg_k": None, "avg_ip": None,
                         "k_hit_rate": None, "k_hit_rate_pct": None,
                         "starts": len(opp_logs), "k_history": "", "pick": None,
                         "pick_note": f"Only {len(opp_logs)} H/A start(s) vs {opp}"})
            all_pit.append(base)
            continue

        k_vals  = [int(g.get("stat", {}).get("strikeOuts", 0)) for g in opp_logs]
        ip_vals = [_ip_to_float(g.get("stat", {}).get("inningsPitched", 0)) for g in opp_logs]

        avg_k  = round(sum(k_vals)  / len(k_vals),  1)
        avg_ip = round(sum(ip_vals) / len(ip_vals), 1)

        k_history     = ",".join(str(k) for k in list(reversed(k_vals))[:8])
        over_count    = sum(1 for k in k_vals if k >= k_line)
        total         = len(k_vals)
        k_hit_rate    = _hit_rate_label(over_count, total)
        k_hit_rate_pct = round(over_count / total * 100)

        gap  = avg_k - k_line
        pick = "OVER" if gap > GAP_THRESH else ("UNDER" if gap < -GAP_THRESH else None)

        base.update({"avg_k": avg_k, "avg_ip": avg_ip,
                     "k_hit_rate": k_hit_rate, "k_hit_rate_pct": k_hit_rate_pct,
                     "starts": total, "k_history": k_history,
                     "pick": pick, "pick_note": None})

        if pick:
            picks.append(base)
        all_pit.append(base)

    picks.sort(key=lambda p: abs((p.get("avg_k") or 0) - (p.get("line") or 0)), reverse=True)
    log(f"⚾ Pitcher K complete: {len(picks)} picks from {len(all_pit)} pitchers processed")
    return {"picks": picks, "all": all_pit}
