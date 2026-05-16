"""
pitcher_k.py  —  MLB Pitcher Strikeout Picks for MoneyBall
Enhanced: avg_ip, era, K/Starts H/A hit-rate, team name
Data sources:
  • MLB Stats API  (schedule, game logs, season ERA)  — free, no key
  • The Odds API   (pitcher K lines)                  — ODDS_API_KEY env var
"""

import os
import requests
from datetime import datetime

MLB_BASE     = "https://statsapi.mlb.com/api/v1"
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
CUR_YEAR     = datetime.now().year
MIN_STARTS   = 2   # minimum H/A starts vs opponent to issue a pick
GAP_THRESH   = 0.4 # avg_k must differ from line by at least this to pick


# ─────────────────────────────  HELPERS  ─────────────────────────────

def _ip_to_float(ip_str) -> float:
    """Convert MLB innings string to decimal: '6.1' → 6.333, '5.2' → 5.667"""
    try:
        parts = str(ip_str).split(".")
        full   = int(parts[0])
        thirds = int(parts[1]) if len(parts) > 1 else 0
        return full + thirds / 3.0
    except Exception:
        return 0.0


def _hit_rate_label(over_count: int, total: int) -> str:
    """Format: '6/8 (75%) 🟢'  — green ≥65%, yellow ≥40%, red <40%"""
    pct = round(over_count / total * 100)
    emoji = "🟢" if pct >= 65 else ("🟡" if pct >= 40 else "🔴")
    return f"{over_count}/{total} ({pct}%) {emoji}"


# ────────────────────────────  DATA FETCHES  ─────────────────────────

def _get_schedule(date_str: str) -> list:
    """Return [{pitcher_id, pitcher_name, team, side, opp, opp_id}, …]"""
    try:
        r = requests.get(
            f"{MLB_BASE}/schedule",
            params={"date": date_str, "sportId": 1, "hydrate": "probablePitcher,team"},
            timeout=15,
        )
        matchups = []
        for d in r.json().get("dates", []):
            for g in d.get("games", []):
                ht = g["teams"]["home"]
                at = g["teams"]["away"]
                hp = ht.get("probablePitcher")
                ap = at.get("probablePitcher")
                home_name = ht.get("team", {}).get("name", "")
                away_name = at.get("team", {}).get("name", "")
                home_id   = ht.get("team", {}).get("id")
                away_id   = at.get("team", {}).get("id")
                if hp:
                    matchups.append({
                        "pitcher_id":   hp["id"],
                        "pitcher_name": hp["fullName"],
                        "team":         home_name,
                        "side":         "HOME",
                        "opp":          away_name,
                        "opp_id":       away_id,
                    })
                if ap:
                    matchups.append({
                        "pitcher_id":   ap["id"],
                        "pitcher_name": ap["fullName"],
                        "team":         away_name,
                        "side":         "AWAY",
                        "opp":          home_name,
                        "opp_id":       home_id,
                    })
        return matchups
    except Exception as exc:
        print(f"[pitcher_k] schedule error: {exc}")
        return []


def _get_k_lines() -> dict:
    """
    Fetch pitcher strikeout O/U lines from The Odds API.
    Returns {last_name_lower: {line, over_odds, under_odds}}
    Falls back to empty dict if no key or API error.
    """
    if not ODDS_API_KEY:
        return {}
    lines: dict = {}
    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY},
            timeout=10,
        )
        event_ids = [e["id"] for e in r.json()]
    except Exception:
        return {}

    for eid in event_ids[:20]:           # cap requests to protect quota
        try:
            r2 = requests.get(
                f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{eid}/odds",
                params={
                    "apiKey":     ODDS_API_KEY,
                    "regions":    "us,us2",
                    "markets":    "pitcher_strikeouts",
                    "oddsFormat": "american",
                },
                timeout=10,
            )
            for bookie in r2.json().get("bookmakers", []):
                for mkt in bookie.get("markets", []):
                    if mkt.get("key") != "pitcher_strikeouts":
                        continue
                    over_p = under_p = line_v = name_k = None
                    for outcome in mkt.get("outcomes", []):
                        desc = outcome.get("description", "").lower().strip()
                        if outcome.get("name") == "Over":
                            line_v = outcome.get("point")
                            over_p = outcome.get("price")
                            name_k = desc
                        elif outcome.get("name") == "Under":
                            under_p = outcome.get("price")
                    if name_k and line_v is not None and name_k not in lines:
                        lines[name_k] = {
                            "line":       line_v,
                            "over_odds":  over_p,
                            "under_odds": under_p,
                        }
        except Exception:
            continue
    return lines


def _get_pitcher_logs(pitcher_id: int) -> list:
    """Pitching game-log splits for current + 2 prior seasons."""
    logs = []
    for season in [CUR_YEAR, CUR_YEAR - 1, CUR_YEAR - 2]:
        try:
            r = requests.get(
                f"{MLB_BASE}/people/{pitcher_id}/stats",
                params={
                    "stats":   "gameLog",
                    "group":   "pitching",
                    "season":  season,
                    "hydrate": "team,opponent",
                },
                timeout=10,
            )
            splits = r.json().get("stats", [{}])[0].get("splits", [])
            logs.extend(splits)
        except Exception:
            pass
    return logs


def _get_pitcher_era(pitcher_id: int) -> str:
    """Current-season ERA as a string, e.g. '2.85'."""
    try:
        r = requests.get(
            f"{MLB_BASE}/people/{pitcher_id}/stats",
            params={"stats": "season", "group": "pitching", "season": CUR_YEAR},
            timeout=10,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if splits:
            return str(splits[0].get("stat", {}).get("era", "-.--"))
    except Exception:
        pass
    return "-.--"


def _filter_logs(logs: list, side: str, opp_id) -> list:
    """Keep starts that match home/away AND today's opponent."""
    want_home = (side == "HOME")
    return [
        g for g in logs
        if g.get("isHome", False) == want_home
        and (opp_id is None or g.get("opponent", {}).get("id") == opp_id)
    ]


def _match_line(name: str, k_lines: dict):
    """Fuzzy-match pitcher name to a K-line dict entry (last-name match)."""
    name_lower = name.lower()
    last = name_lower.split()[-1] if name_lower else ""
    # Exact full-name match first
    if name_lower in k_lines:
        return k_lines[name_lower]
    # Last-name match
    for key, val in k_lines.items():
        if last and (last in key or key.endswith(last)):
            return val
    return None


# ──────────────────────────────  MAIN  ───────────────────────────────

def get_pitcher_k_picks(date_str: str, emit=None) -> dict:
    """
    Returns:
      {
        "picks": [ ... ],   # pitchers with a clear OVER / UNDER call
        "all":   [ ... ],   # every pitcher processed (for the no-pick accordion)
      }

    Each entry contains:
      name, team, side, opp, line,
      avg_k, avg_ip, era,
      k_hit_rate  (e.g. "6/8 (75%) 🟢"),
      k_hit_rate_pct  (integer 0-100),
      starts, k_history,
      pick  (OVER / UNDER / None),
      over_odds, under_odds, pick_note
    """
    def log(msg: str):
        if emit:
            emit({"type": "log", "msg": msg})

    log("⚾ Pitcher K: fetching schedule…")
    matchups = _get_schedule(date_str)
    if not matchups:
        log("⚾ Pitcher K: no probable pitchers found for this date")
        return {"picks": [], "all": []}

    log(f"⚾ Pitcher K: {len(matchups)} pitchers — fetching K lines from Odds API…")
    k_lines = _get_k_lines()
    log(f"⚾ Pitcher K: {len(k_lines)} K lines returned")

    picks        = []
    all_pitchers = []

    for m in matchups:
        pid    = m["pitcher_id"]
        name   = m["pitcher_name"]
        team   = m["team"]
        side   = m["side"]
        opp    = m["opp"]
        opp_id = m["opp_id"]

        # ── find K line ───────────────────────────────────────────────
        line_data = _match_line(name, k_lines)
        if not line_data:
            all_pitchers.append({
                "name": name, "team": team, "side": side, "opp": opp,
                "line": None, "avg_k": None, "avg_ip": None, "era": None,
                "k_hit_rate": None, "k_hit_rate_pct": None,
                "starts": 0, "k_history": "",
                "pick": None, "over_odds": None, "under_odds": None,
                "pick_note": "No K line available",
            })
            continue

        k_line     = line_data["line"]
        over_odds  = line_data.get("over_odds")
        under_odds = line_data.get("under_odds")

        # ── game logs & ERA ───────────────────────────────────────────
        logs     = _get_pitcher_logs(pid)
        opp_logs = _filter_logs(logs, side, opp_id)
        era      = _get_pitcher_era(pid)

        base = {
            "name": name, "team": team, "side": side, "opp": opp,
            "line": k_line, "over_odds": over_odds, "under_odds": under_odds,
            "era": era,
        }

        if len(opp_logs) < MIN_STARTS:
            base.update({
                "avg_k": None, "avg_ip": None,
                "k_hit_rate": None, "k_hit_rate_pct": None,
                "starts": len(opp_logs), "k_history": "",
                "pick": None,
                "pick_note": f"Only {len(opp_logs)} H/A start(s) vs {opp}",
            })
            all_pitchers.append(base)
            continue

        # ── calculate stats ───────────────────────────────────────────
        k_vals  = [int(g.get("stat", {}).get("strikeOuts", 0))         for g in opp_logs]
        ip_vals = [_ip_to_float(g.get("stat", {}).get("inningsPitched", 0)) for g in opp_logs]

        avg_k  = round(sum(k_vals)  / len(k_vals),  1)
        avg_ip = round(sum(ip_vals) / len(ip_vals), 1)

        # K history — most recent 8 starts, comma-separated
        k_history = ",".join(str(k) for k in list(reversed(k_vals))[:8])

        # K hit rate — how often did pitcher meet/beat the posted K line
        over_count     = sum(1 for k in k_vals if k >= k_line)
        total          = len(k_vals)
        k_hit_rate     = _hit_rate_label(over_count, total)
        k_hit_rate_pct = round(over_count / total * 100)

        # Pick logic: gap must exceed GAP_THRESH to call it
        gap = avg_k - k_line
        if   gap >  GAP_THRESH:
            pick = "OVER"
        elif gap < -GAP_THRESH:
            pick = "UNDER"
        else:
            pick = None   # too close to call

        base.update({
            "avg_k":          avg_k,
            "avg_ip":         avg_ip,
            "k_hit_rate":     k_hit_rate,
            "k_hit_rate_pct": k_hit_rate_pct,
            "starts":         total,
            "k_history":      k_history,
            "pick":           pick,
            "pick_note":      None,
        })

        if pick:
            picks.append(base)
        all_pitchers.append(base)

    # Sort picks by strength of gap (largest first)
    picks.sort(key=lambda p: abs((p.get("avg_k") or 0) - (p.get("line") or 0)), reverse=True)

    log(f"⚾ Pitcher K complete: {len(picks)} picks from {len(all_pitchers)} pitchers processed")
    return {"picks": picks, "all": all_pitchers}
