"""
fic_cache.py — Step 1: Player pool builder.

SOURCE 1 (PRIMARY): Fantasy Info Central  https://www.fantasyinfocentral.com/mlb/daily-matchups
        FIC is a JavaScript-rendered page — uses Playwright (already on Render) to load it.
        Filter: min 4 AB career vs today's pitcher, min .250 BA.

SOURCE 1b (FALLBACK): MLB Stats API  — parallel career BA vs pitcher lookups.
        Used automatically if FIC/Playwright fails. Parallelised (8 threads, ~30s max).

SOURCE 2: Baseball Musings hot streaks — adds players on active hitting streaks.

SOURCE 3: MLB Stats API last-7-day hot hitters — .300+ BA, 5+ AB, always works on Render.

All sources merged + deduplicated before passing to Steps 2-5.
"""
import json, os, time, re, glob, requests
from datetime import date as _date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

CACHE_DIR  = os.environ.get("CACHE_DIR", "/tmp")
MLB_API    = "https://statsapi.mlb.com/api/v1"
FIC_URL    = "https://www.fantasyinfocentral.com/mlb/daily-matchups"
BM_URL     = "https://www.baseballmusings.com/cgi-bin/CurStreak.py"

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _cache_path(run_date: str) -> str:
    return os.path.join(CACHE_DIR, f"fic_step1_{run_date.replace('-','')}.json")


def _short_name(full_name: str) -> str:
    parts = full_name.strip().split()
    return f"{parts[0][0]}. {' '.join(parts[1:])}" if parts else full_name


def _parse_avg(s) -> float:
    try:
        s = str(s or "0").strip().replace(",", "")
        if s in ("", "-.--", "-.-", "---", "N/A"):
            return 0.0
        return float(f"0{s}") if s.startswith(".") else float(s)
    except (ValueError, TypeError):
        return 0.0


def _find_chromium() -> str | None:
    browsers = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/project/.browsers")
    patterns = [
        f"{browsers}/chromium-*/chrome-linux/chrome",
        f"{browsers}/chromium-*/chrome-linux/chromium",
    ]
    for pat in patterns:
        hits = glob.glob(pat)
        if hits:
            return hits[0]
    return None


# ── SOURCE 1 (PRIMARY): FIC via Playwright ────────────────────────────

def _fic_html_via_playwright(log_fn) -> str:
    """
    Use Playwright (sync API, already installed on Render) to render FIC.
    FIC is a React/JS app — plain HTTP gets only the 5KB shell.
    Returns full rendered HTML, or '' on failure.
    """
    try:
        from playwright.sync_api import sync_playwright
        exec_path = _find_chromium()
        log_fn(f"   Launching Playwright chromium{' at '+exec_path if exec_path else ''}...")
        with sync_playwright() as p:
            browser = p.chromium.launch(
                executable_path=exec_path,
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            ctx  = browser.new_context(user_agent=_BROWSER_UA)
            page = ctx.new_page()
            page.goto(FIC_URL, wait_until="domcontentloaded", timeout=25_000)
            # Wait for the data table to appear (up to 12s after initial load)
            try:
                page.wait_for_selector("table", timeout=12_000)
            except Exception:
                pass  # proceed anyway — might still have content
            html = page.content()
            browser.close()
            log_fn(f"   Playwright rendered {len(html):,} bytes")
            return html
    except Exception as exc:
        log_fn(f"   ⚠️ Playwright error: {exc}")
        return ""


def _parse_fic_table(html: str, min_ab: int, min_ba: float, log_fn) -> list:
    """Parse BeautifulSoup table from FIC page. Returns player list."""
    soup   = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    target = None
    for t in tables:
        hdrs = [th.get_text(strip=True).upper() for th in t.find_all("th")]
        if any(h in ("AB", "BA", "AVG") for h in hdrs):
            target = t
            break
    if not target:
        log_fn(f"   ⚠️ No FIC table found (page size {len(html):,} bytes)")
        return []

    hdrs = [th.get_text(strip=True).upper() for th in target.find_all("th")]

    def col(*names):
        for n in names:
            if n in hdrs:
                return hdrs.index(n)
        return None

    ip = col("PLAYER","BATTER","NAME")
    ipos = col("POS","POSITION")
    ipit = col("PITCHER","OPP PITCHER","VS PITCHER","OPP")
    iab  = col("AB")
    ih   = col("H","HITS")
    ihr  = col("HR")
    iba  = col("BA","AVG","AVERAGE")

    if ip is None or iab is None or iba is None:
        log_fn(f"   ⚠️ FIC column layout unexpected: {hdrs[:10]}")
        return []

    results = []
    for row in target.find_all("tr")[1:]:
        cells = row.find_all(["td","th"])
        def cell(i):
            return cells[i].get_text(strip=True) if i is not None and i < len(cells) else ""

        name = cell(ip)
        if not name or name.upper() in ("PLAYER","BATTER","NAME"):
            continue
        try:
            ab = int(cell(iab).replace(",",""))
        except ValueError:
            continue
        ba = _parse_avg(cell(iba))
        if ab < min_ab or ba < min_ba:
            continue
        try:
            h  = int(cell(ih).replace(",",""))  if ih  is not None else 0
            hr = int(cell(ihr).replace(",","")) if ihr is not None else 0
        except ValueError:
            h = hr = 0
        results.append({
            "batter":  name,
            "pos":     cell(ipos) if ipos is not None else "",
            "pitcher": cell(ipit) if ipit is not None else "",
            "ab": ab, "h": h, "hr": hr, "ba": ba,
            "source": "fic",
        })
    return results


def _get_fic_players(run_date: str, min_ab: int, min_ba: float, emit=None) -> list:
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 1: Fantasy Info Central (FIC) via Playwright...")
    html = _fic_html_via_playwright(log)

    if len(html) < 10_000:
        log("   ⚠️ FIC page too small — JS may not have rendered yet")
        return []

    results = _parse_fic_table(html, min_ab, min_ba, log)
    log(f"✅ Source 1 (FIC): {len(results)} players (min {min_ab} AB, min {min_ba:.3f} BA)")
    return results


# ── SOURCE 1b (FALLBACK): MLB Stats API  parallel  ───────────────────

def _get_schedule_with_pitchers(run_date: str) -> list:
    r = requests.get(f"{MLB_API}/schedule",
        params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"}, timeout=15)
    out = []
    for dd in r.json().get("dates", []):
        for g in dd.get("games", []):
            ht = g["teams"]["home"]; at = g["teams"]["away"]
            hp = ht.get("probablePitcher",{}); ap = at.get("probablePitcher",{})
            away_t = at.get("team", {})
            home_t = ht.get("team", {})
            if hp and away_t.get("id"):
                out.append({"team_id": away_t["id"], "team_name": away_t.get("name",""),
                            "pitcher_id": hp["id"], "pitcher_short": _short_name(hp["fullName"])})
            if ap and home_t.get("id"):
                out.append({"team_id": home_t["id"], "team_name": home_t.get("name",""),
                            "pitcher_id": ap["id"], "pitcher_short": _short_name(ap["fullName"])})
    return out


def _check_one_player(batter_id, batter_name, pos, pitcher_id, pitcher_short,
                      min_ab, min_ba):
    """Check one batter vs one pitcher. Returns player dict or None."""
    try:
        r = requests.get(f"{MLB_API}/people/{batter_id}/stats",
            params={"stats": "vsPlayerTotal", "group": "hitting",
                    "opposingPlayerId": pitcher_id}, timeout=8)
        for sg in r.json().get("stats", []):
            if "vsPlayer" in sg.get("type",{}).get("displayName",""):
                for sp in sg.get("splits", []):
                    s  = sp.get("stat", {})
                    ab = s.get("atBats", 0)
                    h  = s.get("hits", 0)
                    hr = s.get("homeRuns", 0)
                    ba = _parse_avg(s.get("avg"))
                    if ab >= min_ab and ba >= min_ba:
                        return {"batter": _short_name(batter_name), "pos": pos,
                                "pitcher": pitcher_short, "ab": ab, "h": h,
                                "hr": hr, "ba": ba, "source": "mlb_api"}
    except Exception:
        pass
    return None


def _get_mlb_api_players(run_date: str, min_ab: int, min_ba: float, emit=None) -> list:
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 1b (fallback): MLB Stats API — parallel career BA vs pitchers...")
    matchups = _get_schedule_with_pitchers(run_date)
    log(f"   {len(matchups)//2} games | building player tasks in parallel (8 threads)...")

    # Build list of (batter_id, batter_name, pos, pitcher_id, pitcher_short) tasks
    tasks   = []
    seen    = set()
    for m in matchups:
        try:
            r = requests.get(f"{MLB_API}/teams/{m['team_id']}/roster",
                params={"rosterType": "active"}, timeout=10)
            for pl in r.json().get("roster", []):
                if pl.get("position",{}).get("code") == "1":
                    continue  # skip pitchers
                bid  = pl["person"]["id"]
                key  = (bid, m["pitcher_id"])
                if key in seen:
                    continue
                seen.add(key)
                tasks.append((
                    bid,
                    pl["person"]["fullName"],
                    pl.get("position",{}).get("abbreviation",""),
                    m["pitcher_id"],
                    m["pitcher_short"],
                ))
        except Exception:
            pass

    log(f"   {len(tasks)} batter-pitcher combos to check...")

    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {
            ex.submit(_check_one_player, bid, bname, pos, pid, pshort, min_ab, min_ba): None
            for bid, bname, pos, pid, pshort in tasks
        }
        for fut in as_completed(futs, timeout=60):
            try:
                r = fut.result()
                if r:
                    results.append(r)
            except Exception:
                pass

    log(f"✅ Source 1b (MLB API): {len(results)} players")
    return results


# ── SOURCE 2: Baseball Musings ────────────────────────────────────────

def _get_bm_players(run_date: str, emit=None) -> list:
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 2: Baseball Musings hot streaks...")
    matchups_by_team = {}
    try:
        r = requests.get(f"{MLB_API}/schedule",
            params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"}, timeout=15)
        for dd in r.json().get("dates", []):
            for g in dd.get("games", []):
                ht = g["teams"]["home"]; at = g["teams"]["away"]
                hp = ht.get("probablePitcher",{}); ap = at.get("probablePitcher",{})
                if hp: matchups_by_team[at["team"]["id"]] = _short_name(hp.get("fullName",""))
                if ap: matchups_by_team[ht["team"]["id"]] = _short_name(ap.get("fullName",""))
    except Exception:
        pass

    try:
        r = requests.get(BM_URL, headers={"User-Agent": _BROWSER_UA}, timeout=12)
        soup   = BeautifulSoup(r.text, "lxml")
        tables = soup.find_all("table")
        table  = tables[1] if len(tables) > 1 else (tables[0] if tables else None)
        if not table:
            raise ValueError("no table")
    except Exception as exc:
        log(f"   ⚠️ Baseball Musings unavailable ({exc})")
        return []

    bm = []
    for row in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) < 10:
            continue
        try:
            ba = _parse_avg(cols[9])
            ab = int(cols[2])
        except (ValueError, IndexError):
            continue
        if ba >= 0.250:
            bm.append({"full_name": cols[0], "ab": ab, "ba": ba})

    results = []
    for p in bm:
        try:
            r = requests.get(f"{MLB_API}/people/search",
                params={"names": p["full_name"], "sportId": 1}, timeout=8)
            people = r.json().get("people", [])
            active = [x for x in people if x.get("active")]
            if not active:
                continue
            pid = active[0]["id"]
            r2  = requests.get(f"{MLB_API}/people/{pid}",
                params={"hydrate": "currentTeam"}, timeout=8)
            info    = r2.json()["people"][0]
            team_id = info.get("currentTeam",{}).get("id")
            pos     = info.get("primaryPosition",{}).get("abbreviation","")
            if not team_id or team_id not in matchups_by_team:
                continue
            results.append({
                "batter":  _short_name(p["full_name"]),
                "pos":     pos,
                "pitcher": matchups_by_team[team_id],
                "ab":      p["ab"],
                "h":       int(p["ab"] * p["ba"]),
                "hr":      0,
                "ba":      p["ba"],
                "source":  "baseball_musings",
            })
        except Exception:
            pass
        time.sleep(0.1)

    log(f"✅ Source 2 (Baseball Musings): {len(results)} players")
    return results


# ── SOURCE 3: MLB Stats API last-7-day hot hitters ────────────────────

def _get_recent_hot_hitters(run_date: str, emit=None) -> list:
    def log(msg):
        if emit: emit({"type": "log", "msg": msg})

    log("⬇️  Source 3: MLB last-7-day hot hitters (.300+ BA, 5+ AB)...")
    try:
        end   = _date.fromisoformat(run_date)
        start = (end - timedelta(days=7)).strftime("%Y-%m-%d")
        r = requests.get(f"{MLB_API}/stats", params={
            "stats": "byDateRange", "group": "hitting",
            "startDate": start, "endDate": run_date,
            "playerPool": "All", "sportId": 1,
            "season": run_date[:4], "limit": 500,
        }, timeout=15)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
    except Exception as exc:
        log(f"   ⚠️ Source 3 failed: {exc}")
        return []

    playing_teams = set()
    pitcher_by_team = {}
    try:
        r2 = requests.get(f"{MLB_API}/schedule",
            params={"date": run_date, "sportId": 1, "hydrate": "probablePitcher"}, timeout=10)
        for dd in r2.json().get("dates", []):
            for g in dd.get("games", []):
                ht = g["teams"]["home"]; at = g["teams"]["away"]
                hp = ht.get("probablePitcher",{}); ap = at.get("probablePitcher",{})
                playing_teams.update([ht["team"]["id"], at["team"]["id"]])
                if hp: pitcher_by_team[at["team"]["id"]] = _short_name(hp.get("fullName",""))
                if ap: pitcher_by_team[ht["team"]["id"]] = _short_name(ap.get("fullName",""))
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
        fname   = sp.get("player",{}).get("fullName","")
        team_id = sp.get("team",{}).get("id")
        if not fname or not team_id:
            continue
        if playing_teams and team_id not in playing_teams:
            continue
        pos = sp.get("player",{}).get("primaryPosition",{}).get("abbreviation","")
        results.append({
            "batter":  _short_name(fname),
            "pos":     pos,
            "pitcher": pitcher_by_team.get(team_id, ""),
            "ab":      ab, "h": h,
            "hr":      int(stat.get("homeRuns", 0)),
            "ba":      ba,
            "source":  "mlb_recent_7d",
        })

    log(f"✅ Source 3 (MLB last 7d): {len(results)} hot hitters playing today")
    return results


# ── MERGE ─────────────────────────────────────────────────────────────

def _merge(*sources) -> list:
    merged: dict = {}
    for src in sources:
        for p in src:
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

    path = _cache_path(run_date)
    if os.path.exists(path):
        with open(path) as f:
            players = json.load(f)
        log(f"✅ Loaded {len(players)} players from cache")
        return players

    log("🔍 Building Step 1 player pool (FIC + streaks + recent hot hitters)...")

    # Source 1: FIC via Playwright
    s1 = _get_fic_players(run_date, min_ab, min_ba, emit)

    # Source 1b: MLB Stats API (parallel) — only if FIC returned nothing
    if not s1:
        log("   FIC returned 0 — running MLB Stats API fallback (parallel, ~30s)...")
        s1 = _get_mlb_api_players(run_date, min_ab, min_ba, emit)

    # Source 2: Baseball Musings streaks
    s2 = _get_bm_players(run_date, emit)

    # Source 3: MLB last-7-day hot hitters (always runs)
    s3 = _get_recent_hot_hitters(run_date, emit)

    combined = _merge(s1, s2, s3)
    log(f"✅ Step 1 pool: {len(combined)} players "
        f"(FIC/API:{len(s1)} + BM:{len(s2)} + last7d:{len(s3)}, deduped)")

    with open(path, "w") as f:
        json.dump(combined, f)

    return combined
