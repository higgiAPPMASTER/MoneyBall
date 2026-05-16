"""
pitcher_history.py — Pitcher vs Opponent History for MoneyBall.

For each probable starter today:
  1. Fetch career H/A game logs vs today's opponent (MLB Stats API)
  2. Calculate avg Ks, avg IP, ERA, WHIP vs that team
  3. Compare avg Ks to the K line (from Underdog/Odds API)
  4. Display in NBA-style collapsible table
"""

import os, time, requests, re

MLB_API      = "https://statsapi.mlb.com/api/v1"
UNDERDOG_URL = "https://api.underdogfantasy.com/beta/v5/over_under_lines"
UD_HEADERS   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Accept": "application/json"}

PH_SEASONS   = [2021, 2022, 2023, 2024, 2025, 2026]
MIN_STARTS   = 1   # minimum starts vs opponent to show


# ── Helpers ───────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    subs = {'á':'a','à':'a','é':'e','è':'e','í':'i','ì':'i',
            'ó':'o','ò':'o','ú':'u','ù':'u','ñ':'n','ç':'c'}
    t = text.lower()
    for a, p in subs.items():
        t = t.replace(a, p)
    return re.sub(r"[^a-z0-9 ]", "", t).strip()


def _ip_to_float(ip_str) -> float:
    try:
        parts = str(ip_str).split(".")
        return int(parts[0]) + (int(parts[1]) if len(parts) > 1 else 0) / 3.0
    except Exception:
        return 0.0


# ── Step 1: Today's probable starters ────────────────────────────────

def _get_probable_starters(run_date: str) -> list:
    """Returns list of {pitcher_id, name, team, team_id, opp, opp_id, side}"""
    try:
        r = requests.get(f"{MLB_API}/schedule",
            params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"},
            timeout=15)
        starters = []
        for date_data in r.json().get("dates", []):
            for game in date_data.get("games", []):
                home = game["teams"]["home"]
                away = game["teams"]["away"]
                home_name = home["team"]["name"]
                away_name = away["team"]["name"]
                home_id   = home["team"]["id"]
                away_id   = away["team"]["id"]
                for side, team, opp, opp_id in [
                    ("HOME", home, away_name, away_id),
                    ("AWAY", away, home_name, home_id),
                ]:
                    pp = team.get("probablePitcher", {})
                    if pp and pp.get("id"):
                        starters.append({
                            "pitcher_id": pp["id"],
                            "name":       pp["fullName"],
                            "team":       team["team"]["name"],
                            "opp":        opp,
                            "opp_id":     opp_id,
                            "side":       side,
                        })
        return starters
    except Exception:
        return []


# ── Step 2: K lines from Underdog ────────────────────────────────────

def _get_underdog_k_lines() -> dict:
    """{normalized_name: line_value}"""
    try:
        r = requests.get(UNDERDOG_URL, headers=UD_HEADERS, timeout=15)
        if not r.ok:
            return {}
        lines = {}
        for l in r.json().get("over_under_lines", []):
            title = l.get("over_under", {}).get("title", "")
            stat  = l.get("stat_value")
            if "Strikeouts O/U" in title and "Batter" not in title and "1st" not in title:
                try:
                    name = _normalize(title.replace(" Strikeouts O/U", "").strip())
                    lines[name] = float(stat)
                except Exception:
                    pass
        return lines
    except Exception:
        return {}


# ── Step 3: Career H/A game logs vs opponent ─────────────────────────

def _get_pitcher_logs_vs_opp(pitcher_id: int, opp_id: int, is_home: bool) -> list:
    """Returns list of game stat dicts for H/A starts vs this opponent."""
    results = []
    for season in PH_SEASONS:
        try:
            r = requests.get(f"{MLB_API}/people/{pitcher_id}/stats",
                params={"stats": "gameLog", "group": "pitching", "season": season},
                timeout=10)
            for sg in r.json().get("stats", []):
                for sp in sg.get("splits", []):
                    if sp.get("opponent", {}).get("id") != opp_id:
                        continue
                    if sp.get("isHome", False) != is_home:
                        continue
                    s = sp.get("stat", {})
                    ip = _ip_to_float(s.get("inningsPitched", "0"))
                    if ip < 1.0:
                        continue  # skip relief appearances
                    er  = int(s.get("earnedRuns", 0) or 0)
                    ks  = int(s.get("strikeOuts", 0) or 0)
                    h   = int(s.get("hits", 0) or 0)
                    bb  = int(s.get("baseOnBalls", 0) or 0)
                    era_game = round(er / ip * 9, 2) if ip > 0 else None
                    results.append({
                        "date":  sp.get("date", ""),
                        "ks":    ks,
                        "ip":    round(ip, 1),
                        "era":   era_game,
                        "hits":  h,
                        "bb":    bb,
                        "whip":  round((h + bb) / ip, 2) if ip > 0 else None,
                    })
            time.sleep(0.08)
        except Exception:
            pass
    # Sort by date descending (most recent first)
    results.sort(key=lambda x: x["date"], reverse=True)
    return results[:10]  # last 10 starts vs this opponent


# ── Main Pipeline ─────────────────────────────────────────────────────

def run_pitcher_history(run_date: str, emit=None) -> dict:
    if emit is None:
        emit = lambda _: None

    emit({"type": "section", "msg": "⚾ Pitcher vs Opponent History"})

    starters = _get_probable_starters(run_date)
    if not starters:
        emit({"type": "log", "msg": "⚠️ No probable pitchers found for today"})
        return {"pitchers": []}

    emit({"type": "log", "msg": f"📅 {len(starters)} probable starters found"})

    # Get K lines from Underdog
    k_lines = _get_underdog_k_lines()
    emit({"type": "log", "msg": f"📊 {len(k_lines)} pitcher K lines from Underdog"})

    results = []
    for starter in starters:
        pid    = starter["pitcher_id"]
        name   = starter["name"]
        team   = starter["team"]
        opp    = starter["opp"]
        opp_id = starter["opp_id"]
        side   = starter["side"]
        is_home = (side == "HOME")

        emit({"type": "log", "msg": f"  {name} ({side} vs {opp})..."})

        logs = _get_pitcher_logs_vs_opp(pid, opp_id, is_home)
        starts = len(logs)

        if starts == 0:
            results.append({
                "name": name, "team": team, "opp": opp, "side": side,
                "k_line": k_lines.get(_normalize(name)),
                "starts": 0, "avg_ks": None, "avg_ip": None,
                "avg_era": None, "avg_whip": None,
                "hits_over": 0, "hit_pct": 0,
                "k_history": "—", "ip_history": "—",
                "pick": None, "pick_note": f"No {side} starts vs {opp} in database",
            })
            continue

        # Calculate averages
        avg_ks   = round(sum(l["ks"] for l in logs) / starts, 1)
        avg_ip   = round(sum(l["ip"] for l in logs) / starts, 1)
        era_vals = [l["era"] for l in logs if l["era"] is not None]
        whip_vals= [l["whip"] for l in logs if l["whip"] is not None]
        avg_era  = round(sum(era_vals) / len(era_vals), 2) if era_vals else None
        avg_whip = round(sum(whip_vals) / len(whip_vals), 2) if whip_vals else None

        k_line = k_lines.get(_normalize(name))

        # Hit rate vs K line
        if k_line:
            hits_over = sum(1 for l in logs if l["ks"] > k_line)
            hit_pct   = round(hits_over / starts * 100)
            gap       = round(avg_ks - k_line, 1)
            pick      = "OVER" if avg_ks > k_line else ("UNDER" if avg_ks < k_line else None)
            pick_note = f"avg {avg_ks} K {'>' if avg_ks > k_line else '<'} line {k_line}"
        else:
            hits_over = 0
            hit_pct   = 0
            gap       = None
            pick      = None
            pick_note = "No K line available"

        k_history  = ", ".join(str(l["ks"]) for l in logs)
        ip_history = ", ".join(str(l["ip"]) for l in logs)

        results.append({
            "name":       name,
            "team":       team,
            "opp":        opp,
            "side":       side,
            "k_line":     k_line,
            "starts":     starts,
            "avg_ks":     avg_ks,
            "avg_ip":     avg_ip,
            "avg_era":    avg_era,
            "avg_whip":   avg_whip,
            "hits_over":  hits_over,
            "hit_pct":    hit_pct,
            "gap":        gap,
            "k_history":  k_history,
            "ip_history": ip_history,
            "pick":       pick,
            "pick_note":  pick_note,
        })
        time.sleep(0.1)

    picks   = [r for r in results if r["pick"]]
    no_pick = [r for r in results if not r["pick"]]
    emit({"type": "log", "msg": f"✅ Pitcher history done: {len(picks)} picks, {len(no_pick)} no pick"})

    return {"pitchers": results, "picks": picks}
