"""
fic_cache.py — Step 1: Player pool builder.

SOURCE 1 (PRIMARY): Fantasy Info Central daily matchups
        https://www.fantasyinfocentral.com/mlb/daily-matchups
        Shows every batter vs today's probable pitcher with career stats.
        Filter: min 4 AB, min .250 career BA vs that pitcher.
        Uses curl_cffi (Chrome impersonation) to bypass Cloudflare on Render.

SOURCE 1b (FALLBACK): MLB Stats API career BA vs pitcher
        Used automatically if FIC is unreachable.
        Same filter: min 4 AB, min .250 BA.

SOURCE 2: Baseball Musings Hot Streaks
        Adds players on active hit streaks (streak BA >= .250).

SOURCE 3: MLB Stats API last-7-day hot hitters
        Adds players hitting .300+ with 5+ AB in the past 7 days.
        Always works — no scraping needed.

All sources merged + deduplicated before passing to Steps 2-5.
"""
import json, os, time, re, requests
from datetime import date as _date, timedelta
from bs4 import BeautifulSoup

CACHE_DIR  = os.environ.get("CACHE_DIR", "/tmp")
MLB_API    = "https://statsapi.mlb.com/api/v1"
FIC_URL    = "https://www.fantasyinfocentral.com/mlb/daily-matchups"
BM_URL     = "https://www.baseballmusings.com/cgi-bin/CurStreak.py"

_BROWSER_HDRS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.google.com/",
}


def _cache_path(run_date: str) -> str:
    return os.path.join(CACHE_DIR, f"fic_step1_{run_date.replace('-','')}.json")


def _short_name(full_name: str) -> str:
    parts = full_name.strip().split()
    if not parts:
        return full_name
    return f"{parts[0][0]}. {' '.join(parts[1:])}"


def _parse_avg(avg_str) -> float:
    try:
        s = str(avg_str or "0").strip().replace(",", "")
        if s in ("", "-.--", "-.-", "---", "N/A"):
            return 0.0
        return float(f"0{s}") if s.startswith(".") else float(s)
    except (ValueError, TypeError):
        return 0.0


# ── SOURCE 1 (PRIMARY): Fantasy Info Central ──────────────────────────

def _get_fic_players(run_date: str, min_ab: int, min_ba: float, emit=None) -> list:
    """
    Scrape Fantasy Info Central daily matchups.
    Returns list of player dicts with batter, pos, pitcher, ab, h, hr, ba, source.
    Uses curl_cffi to bypass Cloudflare — falls back to requests.
    """
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 1: Fantasy Info Central daily matchups (FIC)...")

    html = ""
    try:
        try:
            from curl_cffi import requests as cffi
            r = cffi.get(FIC_URL, impersonate="chrome120", timeout=20)
            html = r.text
            log(f"   FIC fetched via curl_cffi — {len(html):,} bytes")
        except ImportError:
            r = requests.get(FIC_URL, headers=_BROWSER_HDRS, timeout=20)
            html = r.text
            log(f"   FIC fetched via requests — {len(html):,} bytes")

        if len(html) < 500:
            log("   ⚠️ FIC returned empty page — falling back to MLB Stats API")
            return []

    except Exception as exc:
        log(f"   ⚠️ FIC unreachable: {exc} — falling back to MLB Stats API")
        return []

    # ── Parse the HTML table ─────────────────────────────────────────
    try:
        soup = BeautifulSoup(html, "lxml")
        # FIC daily matchups table — find by looking for the AB / BA columns
        tables = soup.find_all("table")
        target = None
        for t in tables:
            headers = [th.get_text(strip=True).upper() for th in t.find_all("th")]
            if any(h in ("AB", "BA", "AVG") for h in headers):
                target = t
                break
        if not target:
            log("   ⚠️ FIC table not found in page — falling back to MLB Stats API")
            return []

        # Identify column indices from header row
        headers = [th.get_text(strip=True).upper() for th in target.find_all("th")]
        def col(names):
            for n in names:
                if n in headers:
                    return headers.index(n)
            return None

        idx_player  = col(["PLAYER", "BATTER", "NAME"])
        idx_pos     = col(["POS", "POSITION"])
        idx_pitcher = col(["PITCHER", "OPP PITCHER", "VS PITCHER"])
        idx_ab      = col(["AB"])
        idx_h       = col(["H", "HITS"])
        idx_hr      = col(["HR"])
        idx_ba      = col(["BA", "AVG", "AVERAGE"])

        if idx_player is None or idx_ab is None or idx_ba is None:
            log(f"   ⚠️ FIC column layout unexpected (headers={headers[:8]}) — falling back")
            return []

        results = []
        for row in target.find_all("tr")[1:]:   # skip header row
            cells = row.find_all(["td", "th"])
            if len(cells) < max(filter(None, [idx_player, idx_ab, idx_ba])) + 1:
                continue

            def cell(i):
                if i is None or i >= len(cells):
                    return ""
                return cells[i].get_text(strip=True)

            player_text  = cell(idx_player)
            pitcher_text = cell(idx_pitcher) if idx_pitcher is not None else ""
            ab_text      = cell(idx_ab)
            h_text       = cell(idx_h)   if idx_h  is not None else "0"
            hr_text      = cell(idx_hr)  if idx_hr is not None else "0"
            ba_text      = cell(idx_ba)
            pos_text     = cell(idx_pos) if idx_pos is not None else ""

            if not player_text or player_text.upper() in ("PLAYER", "BATTER", "NAME"):
                continue

            try:
                ab = int(ab_text.replace(",", "")) if ab_text else 0
            except ValueError:
                ab = 0

            ba = _parse_avg(ba_text)

            # Apply filters
            if ab < min_ab or ba < min_ba:
                continue

            try:
                h  = int(h_text.replace(",", ""))  if h_text  else 0
                hr = int(hr_text.replace(",", "")) if hr_text else 0
            except ValueError:
                h = hr = 0

            results.append({
                "batter":  player_text,
                "pos":     pos_text,
                "pitcher": pitcher_text,
                "ab":      ab,
                "h":       h,
                "hr":      hr,
                "ba":      ba,
                "source":  "fic",
            })

        log(f"✅ Source 1 (FIC): {len(results)} players (min {min_ab} AB, min {min_ba:.3f} BA)")
        return results

    except Exception as exc:
        log(f"   ⚠️ FIC parse error: {exc} — falling back to MLB Stats API")
        return []


# ── SOURCE 1b (FALLBACK): MLB Stats API career BA ─────────────────────

def _get_schedule_with_pitchers(run_date: str) -> list:
    r = requests.get(f"{MLB_API}/schedule",
        params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"},
        timeout=15)
    matchups = []
    for date_data in r.json().get("dates", []):
        for game in date_data.get("games", []):
            home   = game["teams"]["home"]
            away   = game["teams"]["away"]
            home_p = home.get("probablePitcher", {})
            away_p = away.get("probablePitcher", {})
            if home_p and away_p:
                matchups.append({"team_id": away["team"]["id"], "team_name": away["team"]["name"],
                                 "pitcher_id": home_p["id"], "pitcher_name": home_p["fullName"],
                                 "pitcher_short": _short_name(home_p["fullName"])})
                matchups.append({"team_id": home["team"]["id"], "team_name": home["team"]["name"],
                                 "pitcher_id": away_p["id"], "pitcher_name": away_p["fullName"],
                                 "pitcher_short": _short_name(away_p["fullName"])})
    return matchups


def _get_position_players(team_id: int) -> list:
    r = requests.get(f"{MLB_API}/teams/{team_id}/roster",
        params={"rosterType": "active"}, timeout=10)
    return [p for p in r.json().get("roster", []) if p.get("position", {}).get("code") != "1"]


def _get_career_vs_pitcher(batter_id: int, pitcher_id: int) -> dict:
    r = requests.get(f"{MLB_API}/people/{batter_id}/stats",
        params={"stats": "vsPlayerTotal", "group": "hitting", "opposingPlayerId": pitcher_id},
        timeout=8)
    for sg in r.json().get("stats", []):
        if "vsPlayer" in sg.get("type", {}).get("displayName", ""):
            for sp in sg.get("splits", []):
                s = sp.get("stat", {})
                return {"ab": s.get("atBats", 0), "hits": s.get("hits", 0),
                        "hr": s.get("homeRuns", 0), "ba": _parse_avg(s.get("avg"))}
    return {"ab": 0, "hits": 0, "hr": 0, "ba": 0.0}


def _get_mlb_api_players(run_date: str, min_ab: int, min_ba: float, emit=None) -> list:
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 1b (fallback): MLB Stats API — career BA vs today's pitchers...")
    matchups = _get_schedule_with_pitchers(run_date)
    log(f"   {len(matchups)//2} games with both pitchers confirmed")

    results = []
    seen = set()
    for m in matchups:
        try:
            roster = _get_position_players(m["team_id"])
        except Exception:
            continue
        time.sleep(0.1)
        for player in roster:
            batter_id   = player["person"]["id"]
            batter_name = player["person"]["fullName"]
            pos         = player.get("position", {}).get("abbreviation", "")
            key         = (batter_id, m["pitcher_id"])
            if key in seen:
                continue
            seen.add(key)
            try:
                stats = _get_career_vs_pitcher(batter_id, m["pitcher_id"])
            except Exception:
                time.sleep(0.2)
                continue
            if stats["ab"] >= min_ab and stats["ba"] >= min_ba:
                results.append({
                    "batter":  _short_name(batter_name),
                    "pos":     pos,
                    "pitcher": m["pitcher_short"],
                    "ab":      stats["ab"],
                    "h":       stats["hits"],
                    "hr":      stats["hr"],
                    "ba":      stats["ba"],
                    "source":  "mlb_api",
                })
            time.sleep(0.08)

    log(f"✅ Source 1b (MLB API): {len(results)} players")
    return results


# ── SOURCE 2: Baseball Musings Hot Streaks ────────────────────────────

def _get_bm_players(run_date: str, emit=None) -> list:
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 2: Baseball Musings hot streaks...")
    matchups_by_team = {}
    try:
        r = requests.get(f"{MLB_API}/schedule",
            params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"},
            timeout=15)
        for date_data in r.json().get("dates", []):
            for game in date_data.get("games", []):
                home = game["teams"]["home"]; away = game["teams"]["away"]
                hp   = home.get("probablePitcher", {}); ap = away.get("probablePitcher", {})
                if hp: matchups_by_team[away["team"]["id"]] = _short_name(hp.get("fullName", ""))
                if ap: matchups_by_team[home["team"]["id"]] = _short_name(ap.get("fullName", ""))
    except Exception:
        pass

    try:
        r = requests.get(BM_URL, headers=_BROWSER_HDRS, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        tables = soup.find_all("table")
        table  = tables[1] if len(tables) > 1 else (tables[0] if tables else None)
        if not table:
            raise ValueError("no table found")
    except Exception as exc:
        log(f"   ⚠️ Baseball Musings unavailable ({exc})")
        return []

    bm_players = []
    for row in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) < 10:
            continue
        full_name = cols[0]
        ba_str    = cols[9]
        try:
            ba = float(f"0{ba_str}") if ba_str.startswith(".") else float(ba_str)
            ab = int(cols[2])
        except (ValueError, IndexError):
            continue
        if ba >= 0.250:
            bm_players.append({"full_name": full_name, "ab_streak": ab, "ba_streak": ba})

    results = []
    for bm in bm_players:
        try:
            r = requests.get(f"{MLB_API}/people/search",
                params={"names": bm["full_name"], "sportId": 1}, timeout=8)
            people  = r.json().get("people", [])
            matched = [p for p in people if p.get("active") and
                       p.get("fullName","").lower() == bm["full_name"].lower()]
            if not matched:
                matched = [p for p in people if p.get("active")]
            if not matched:
                continue
            pid     = matched[0]["id"]
            r2      = requests.get(f"{MLB_API}/people/{pid}",
                params={"hydrate": "currentTeam"}, timeout=8)
            info    = r2.json()["people"][0]
            team_id = info.get("currentTeam", {}).get("id")
            pos     = info.get("primaryPosition", {}).get("abbreviation", "")
            if not team_id or team_id not in matchups_by_team:
                continue
            results.append({
                "batter":  _short_name(bm["full_name"]),
                "pos":     pos,
                "pitcher": matchups_by_team[team_id],
                "ab":      bm["ab_streak"],
                "h":       int(bm["ab_streak"] * bm["ba_streak"]),
                "hr":      0,
                "ba":      bm["ba_streak"],
                "source":  "baseball_musings",
            })
        except Exception:
            pass
        time.sleep(0.1)

    log(f"✅ Source 2 (Baseball Musings): {len(results)} hot streak players playing today")
    return results


# ── SOURCE 3: MLB Stats API last-7-day hot hitters ────────────────────

def _get_recent_hot_hitters(run_date: str, emit=None) -> list:
    """
    MLB Stats API byDateRange — players hitting .300+ with 5+ AB in last 7 days.
    Always works on Render. Used to fill gaps when Sources 1 & 2 are thin.
    """
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 3: MLB Stats API last-7-day hot hitters (.300+ BA, 5+ AB)...")
    try:
        end_dt   = _date.fromisoformat(run_date)
        start_dt = end_dt - timedelta(days=7)
        r = requests.get(f"{MLB_API}/stats", params={
            "stats":      "byDateRange",
            "group":      "hitting",
            "startDate":  start_dt.strftime("%Y-%m-%d"),
            "endDate":    run_date,
            "playerPool": "All",
            "sportId":    1,
            "season":     run_date[:4],
            "limit":      500,
        }, timeout=15)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
    except Exception as exc:
        log(f"   ⚠️ Source 3 fetch failed: {exc}")
        return []

    # Get today's teams so we only include players playing today
    playing_team_ids = set()
    pitcher_by_team  = {}
    try:
        r2 = requests.get(f"{MLB_API}/schedule",
            params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"},
            timeout=10)
        for dd in r2.json().get("dates", []):
            for g in dd.get("games", []):
                ht = g["teams"]["home"]; at = g["teams"]["away"]
                hp = ht.get("probablePitcher", {}); ap = at.get("probablePitcher", {})
                playing_team_ids.update([ht["team"]["id"], at["team"]["id"]])
                if hp: pitcher_by_team[at["team"]["id"]] = _short_name(hp.get("fullName", ""))
                if ap: pitcher_by_team[ht["team"]["id"]] = _short_name(ap.get("fullName", ""))
    except Exception:
        pass

    results = []
    for sp in splits:
        stat = sp.get("stat", {})
        ab   = int(stat.get("atBats", 0))
        h    = int(stat.get("hits",   0))
        if ab < 5:
            continue
        ba = round(h / ab, 3) if ab > 0 else 0.0
        if ba < 0.300:
            continue
        fname   = sp.get("player", {}).get("fullName", "")
        team_id = sp.get("team", {}).get("id")
        if not fname or not team_id:
            continue
        if playing_team_ids and team_id not in playing_team_ids:
            continue
        pos = sp.get("player", {}).get("primaryPosition", {}).get("abbreviation", "")
        results.append({
            "batter":  _short_name(fname),
            "pos":     pos,
            "pitcher": pitcher_by_team.get(team_id, ""),
            "ab":      ab,
            "h":       h,
            "hr":      int(stat.get("homeRuns", 0)),
            "ba":      ba,
            "source":  "mlb_recent_7d",
        })

    log(f"✅ Source 3 (MLB last 7d): {len(results)} hot hitters playing today")
    return results


# ── MERGE ─────────────────────────────────────────────────────────────

def _merge_players(*sources):
    """Merge any number of player lists. Deduplicate by batter name, keep highest BA."""
    merged: dict = {}
    for source in sources:
        for p in source:
            name = p["batter"]
            if name not in merged or p["ba"] > merged[name]["ba"]:
                merged[name] = p
    return sorted(merged.values(), key=lambda x: x["ba"], reverse=True)


# ── PUBLIC API ────────────────────────────────────────────────────────

def get_step1_players_or_scrape(run_date=None, min_ab=4, min_ba=0.250, emit=None):
    if run_date is None:
        run_date = _date.today().strftime("%Y-%m-%d")

    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    # Return from cache if already run today
    path = _cache_path(run_date)
    if os.path.exists(path):
        with open(path) as f:
            players = json.load(f)
        log(f"✅ Loaded {len(players)} players from cache")
        return players

    log("🔍 Building Step 1 player pool (FIC + hot streaks + recent hits)...")

    # Source 1: Fantasy Info Central (PRIMARY)
    fic_players = _get_fic_players(run_date, min_ab, min_ba, emit)

    # Source 1b: MLB Stats API fallback — only if FIC returned nothing
    if not fic_players:
        log("   FIC empty — using MLB Stats API career stats as fallback...")
        fic_players = _get_mlb_api_players(run_date, min_ab, min_ba, emit)

    # Source 2: Baseball Musings hot streaks
    bm_players = _get_bm_players(run_date, emit)

    # Source 3: MLB Stats API last-7-day hot hitters (always runs)
    recent_hot = _get_recent_hot_hitters(run_date, emit)

    # Merge all sources
    combined = _merge_players(fic_players, bm_players, recent_hot)

    s1_count = len(fic_players)
    s2_count = len(bm_players)
    s3_count = len(recent_hot)
    log(f"✅ Step 1 pool: {len(combined)} unique players "
        f"(FIC/API:{s1_count} + BM:{s2_count} + recent:{s3_count}, deduped)")

    with open(path, "w") as f:
        json.dump(combined, f)

    return combined
