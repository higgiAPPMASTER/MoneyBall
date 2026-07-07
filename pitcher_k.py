
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
MIN_K_EDGE       = 0.5  # projection must beat the line by ≥0.5 K or pick is dropped
LEAGUE_AVG_K_PER_GAME = 16.5   # 2024-2026 MLB avg Ks per game (both teams combined)
_UMP_K_CACHE: dict = {}         # {ump_name: ump_dict | None} — season-level cache
# ── Projection-model edges (handedness K% + whiff% + rest) ──────────────────
LEAGUE_K_PCT   = 0.222   # MLB avg strikeout rate (SO / PA), 2024-2025
LEAGUE_WHIFF   = 24.5    # MLB avg whiff% (whiffs / swings), Baseball Savant
LEAGUE_AVG     = 0.243   # MLB avg batting average, 2024-2025 (hits projection)
LEAGUE_BB_PCT  = 0.083   # MLB avg walk rate (BB / PA), 2024-2025 (walks projection)
LEAGUE_OPS     = 0.711   # MLB avg OPS, 2024-2025 (earned-runs projection)
_WHIFF_CACHE: dict = {}        # {year: {player_id: whiff_pct}}
_PITCH_HAND_CACHE: dict = {}   # {pitcher_id: "R"/"L"}
_OPP_KPCT_CACHE: dict = {}     # {(opp_id, hand, season): k_pct}
_OPP_SPLIT_CACHE: dict = {}    # {(opp_id, hand, season): {k_pct,avg,bb_pct,ops}}
LEAGUE_GB_PCT  = 43.0    # MLB avg groundball rate %, 2024-2025
LEAGUE_XWOBA   = 0.315   # MLB avg xwOBA-against (pitcher), 2024-2025
LEAGUE_TOTAL   = 8.5     # MLB avg game O/U total, 2024-2025
LEAGUE_VELO    = 93.3    # MLB avg fastball velocity (mph), 2024-2025
LEAGUE_KRATE   = 22.0    # pitcher strikeout rate (K%) league average
LEAGUE_AVG_PITCH_WOBA = 0.310  # MLB avg batter wOBA vs any pitch type, ~2024-2025
_GB_XWOBA_CACHE: dict = {}     # {year: {player_id: {gb_pct, xwoba}}}
_GAME_TOTALS:   dict = {}      # {(norm_home, norm_away): total_line}
_GAME_MONEYLINES: dict = {}    # {(norm_home, norm_away): (home_american, away_american)}
_EVENTS_CACHE:  dict = {}      # {run_date: [event, ...]} — shared across K + props fetch
_VELO_CACHE:    dict = {}      # {year: {player_id: avg_release_speed_mph}}
_KRATE_CACHE:   dict = {}      # {year: {player_id: k_percent}}
_PK_PITCH_TYPES    = ["FF", "SL", "SI", "CH", "CU", "FC"]
_PK_ARSENAL_CACHE: dict = {}   # {pitcher_id: {pitch_type: usage_pct}}
_PK_BATTER_WOBA_CACHE: dict = {}  # {batter_id: {pitch_type: woba}}
_PK_PITCH_LOADED:  set  = set()   # years fetched for pitch-type data
_PK_LINEUP_MAP:    dict = {}      # {norm_team: [batter_id_int]} — tonight's lineups
_CAREER_HA_ERA_CACHE: dict = {}  # {(pitcher_id, side): era | None}

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
# Min projection-vs-line edge to post an O/U pick (thin coin-flips dropped).
# Membership ALSO flags which markets get the opponent-adjusted projection.
# Outs use a larger edge (~half an inning) since outs are bigger, noisier numbers
# than hits/walks/ER, so a fraction-of-an-out "edge" would be meaningless.
PROP_EDGE = {
    "pitcher_hits_allowed": 0.6,
    "pitcher_earned_runs":  0.5,
    "pitcher_walks":        0.4,
    "pitcher_outs":         1.5,
}
# Populated by _fetch_pitcher_props each run (cleared at the start so a warm
# process / 3×-day scheduler never serves a stale matchup):
#   {market: {norm_name: {name,line,over_odds,under_odds,home_team,away_team}}}
PROP_ODDS = {}

# Populated by _get_bottom_k_teams each run.
# Full MLB team K/game ranking: key = team name, value = {rank, k_per_g, total}
# rank 1 = most Ks (easiest matchup for pitcher), rank 30 = fewest Ks (toughest).
TEAM_K_RANKS: dict = {}
# Home/away split K rankings (rank 1 = most Ks = easiest matchup for pitcher)
TEAM_K_RANKS_HOME: dict = {}   # opponent's rank when playing at HOME
TEAM_K_RANKS_AWAY: dict = {}   # opponent's rank when playing AWAY (road)
# Full MLB team BB/game ranking (walks DRAWN by offense).
# rank 1 = most BBs drawn (hardest matchup for pitcher control), rank 30 = fewest.
TEAM_BB_RANKS: dict = {}

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
    global TEAM_K_RANKS, TEAM_BB_RANKS, TEAM_K_RANKS_HOME, TEAM_K_RANKS_AWAY
    try:
        r = requests.get(f"{MLB_API}/teams/stats",
            params={"season": season, "sportId": 1, "group": "hitting", "stats": "season"},
            timeout=12)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        teams_data = []
        bb_data = []
        for sp in splits:
            stat = sp.get("stat", {})
            tname = sp.get("team", {}).get("name", "")
            ks = stat.get("strikeOuts", 0)
            bb = stat.get("baseOnBalls", 0)
            gp = stat.get("gamesPlayed", 1)
            if gp < 5: continue
            teams_data.append({"name": tname, "k_per_g": round(ks / gp, 2)})
            bb_data.append({"name": tname, "bb_per_g": round(bb / gp, 2)})
        teams_data.sort(key=lambda x: x["k_per_g"])
        bottom_n = teams_data[:n]
        # Build full K ranking: rank 1 = most Ks per game (easiest for pitchers)
        teams_desc = sorted(teams_data, key=lambda x: x["k_per_g"], reverse=True)
        total = len(teams_desc)
        TEAM_K_RANKS = {t["name"]: {"rank": i + 1, "k_per_g": t["k_per_g"], "total": total}
                        for i, t in enumerate(teams_desc)}
        # Build BB ranking: rank 1 = most BBs drawn (hardest for pitcher command)
        bb_desc = sorted(bb_data, key=lambda x: x["bb_per_g"], reverse=True)
        bb_total = len(bb_desc)
        TEAM_BB_RANKS = {t["name"]: {"rank": i + 1, "bb_per_g": t["bb_per_g"], "total": bb_total}
                         for i, t in enumerate(bb_desc)}
        # ── Home / Away K split rankings ─────────────────────────────────────
        try:
            rha = requests.get(f"{MLB_API}/teams/stats",
                params={"season": season, "sportId": 1, "group": "hitting", "stats": "homeAndAway"},
                timeout=12)
            ha_splits = rha.json().get("stats", [{}])[0].get("splits", [])
            home_data, away_data = [], []
            for sp in ha_splits:
                stat = sp.get("stat", {})
                tname = sp.get("team", {}).get("name", "")
                ks = stat.get("strikeOuts", 0)
                gp = stat.get("gamesPlayed", 1)
                code = (sp.get("split") or {}).get("code", "")
                if gp < 3: continue
                entry = {"name": tname, "k_per_g": round(ks / gp, 2)}
                if code == "H":
                    home_data.append(entry)
                elif code in ("A", "R"):
                    away_data.append(entry)
            for data, is_home in ((home_data, True), (away_data, False)):
                desc = sorted(data, key=lambda x: x["k_per_g"], reverse=True)
                tot = len(desc)
                ranks = {t["name"]: {"rank": i + 1, "k_per_g": t["k_per_g"], "total": tot}
                         for i, t in enumerate(desc)}
                if is_home:
                    TEAM_K_RANKS_HOME = ranks
                else:
                    TEAM_K_RANKS_AWAY = ranks
        except Exception as e:
            print(f"[pitcher_k] home/away K ranks failed: {e}")
        return {t["name"] for t in bottom_n}, bottom_n
    except Exception:
        return set(), []


def _get_events_for_date(run_date: str) -> list:
    """Fetch today's event list once and cache it — shared by K and props fetches."""
    global _EVENTS_CACHE
    if run_date in _EVENTS_CACHE:
        return _EVENTS_CACHE[run_date]
    tomorrow = (time.strftime("%Y-%m-%d",
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
    r = requests.get(f"{ODDS_BASE}/sports/baseball_mlb/events",
        params={"apiKey": ODDS_API_KEY, "dateFormat": "iso"}, timeout=15)
    r.raise_for_status()
    events = [e for e in r.json() if _is_run_date_game(e.get("commence_time", ""))]
    _EVENTS_CACHE[run_date] = events
    return events


def _fetch_k_lines(run_date: str, emit=None) -> list:
    """Per-event Odds API call — works on all paid plans."""
    def log(m):
        if emit: emit({"type": "log", "msg": m})

    if not ODDS_API_KEY:
        log("⚠️  ODDS_API_KEY not set — Pitcher K Picks skipped")
        return []

    PREFERRED = ["draftkings", "fanduel", "betmgm", "williamhill_us", "caesars", "betrivers", "ballybet", "bet365", "espnbet", "bet99", "thescore", "fliff", "mybookieag", "betonlineag", "bovada"]
    K_MARKETS = "pitcher_strikeouts,pitcher_strikeouts_alternate"

    try:
        events = _get_events_for_date(run_date)
        log(f"  Odds API: {len(events)} games for {run_date}")
        seen: dict = {}
        ladder: dict = {}

        for ev in events:
            home_team = ev.get("home_team", "")
            away_team = ev.get("away_team", "")
            r2 = requests.get(
                f"{ODDS_BASE}/sports/baseball_mlb/events/{ev['id']}/odds",
                params={"apiKey": ODDS_API_KEY, "regions": "us,us2,ca",
                        "markets": K_MARKETS,
                        "oddsFormat": "american"}, timeout=15)
            if not r2.ok: continue
            for bm in r2.json().get("bookmakers", []):
                bk = bm.get("key")
                for mkt in bm.get("markets", []):
                    for oc in mkt.get("outcomes", []):
                        name  = (oc.get("description") or oc.get("name", "")).strip()
                        pt    = oc.get("point")
                        side  = oc.get("name", "")
                        price = oc.get("price")
                        if not name or pt is None: continue
                        key = _normalize(name)
                        if side == "Over" and price is not None:
                            ladder.setdefault(key, {}).setdefault(float(pt), price)
                        entry = seen.get(key)
                        if entry is None:
                            entry = {"name": name, "line": float(pt),
                                     "home_team": home_team, "away_team": away_team,
                                     "over_odds": None, "under_odds": None}
                            seen[key] = entry
                        # Only take odds posted at the displayed line so the
                        # best-book price always matches the shown line.
                        if abs(float(pt) - entry["line"]) > 1e-9:
                            continue
                        if side == "Over":
                            _take_odds(entry, "over_odds", "over_odds_book", price, bk)
                        elif side == "Under":
                            _take_odds(entry, "under_odds", "under_odds_book", price, bk)
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

    try:
        events = _get_events_for_date(run_date)
        for ev in events:
            home_team = ev.get("home_team", "")
            away_team = ev.get("away_team", "")
            r2 = requests.get(
                f"{ODDS_BASE}/sports/baseball_mlb/events/{ev['id']}/odds",
                params={"apiKey": ODDS_API_KEY, "regions": "us,us2,ca",
                        "markets": ",".join(PROP_MARKETS),
                        "oddsFormat": "american"}, timeout=15)
            if not r2.ok: continue
            for bm in r2.json().get("bookmakers", []):
                bk = bm.get("key")
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
                            continue  # only the displayed line for this pitcher
                        if side == "Over":
                            _take_odds(d, "over_odds", "over_odds_book", price, bk)
                        elif side == "Under":
                            _take_odds(d, "under_odds", "under_odds_book", price, bk)
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
        # Pass 1: exact full name + pitcher/TWP position
        for p in candidates:
            if (_normalize(p.get("fullName", "")) == norm and p.get("active") and
                    p.get("primaryPosition", {}).get("code") in ("1", "TWP")):
                _pitcher_id_cache[key] = p["id"]
                return p["id"]
        # Pass 2: last name + pitcher/TWP position
        for p in candidates:
            if (_normalize(p.get("lastName", "")) == _normalize(last) and
                    p.get("active") and p.get("primaryPosition", {}).get("code") in ("1", "TWP")):
                _pitcher_id_cache[key] = p["id"]
                return p["id"]
        # Pass 3: exact full name, any position — catches two-way players (e.g. Ohtani)
        # whose primaryPosition code is registered as DH/hitter in the API.
        # Safe because we still require an exact full-name match.
        for p in candidates:
            if _normalize(p.get("fullName", "")) == norm and p.get("active"):
                _pitcher_id_cache[key] = p["id"]
                return p["id"]
        # Pass 4: exact full name + pitcher/TWP, ignore active flag — catches IL/rehab
        # pitchers who have an Odds API K line today but are still marked inactive in
        # the MLB Stats API (e.g. McClanahan post-TJ, E. Rodriguez on IL start).
        for p in candidates:
            if (_normalize(p.get("fullName", "")) == norm and
                    p.get("primaryPosition", {}).get("code") in ("1", "TWP")):
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
              if int(sp.get("stat", {}).get("gamesStarted", 0) or 0) >= 1]
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
        "last_start_date": (recent[-1].get("date") or "") if recent else "",
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
    h_list    = []          # hits allowed per start vs opp (H/A-matched)
    er_list   = []          # earned runs per start vs opp (H/A-matched)
    outs_list = []          # outs recorded per start vs opp (H/A-matched)
    bb_list   = []          # walks allowed per start vs opp (H/A-matched)
    vs_log    = []          # dated per-start log vs opp (H/A-matched)
    # all-venue fallback — ERA only; used when H/A-matched starts are sparse
    # (e.g. all career starts vs this opp happened at the other venue).
    # K projection stays H/A-filtered; only ERA/display uses this fallback.
    era_all_list = []
    vs_log_all   = []
    for season in reversed(K_SEASONS):
        splits = _get_pitching_logs(pitcher_id, season)
        time.sleep(0.08)
        for sp in reversed(splits):
            if sp.get("opponent", {}).get("id") != opp_id: continue
            stat_a = sp.get("stat", {})
            if int(stat_a.get("gamesStarted", 0) or 0) < 1: continue
            # collect all-venue ERA before the home/away filter
            ip_a  = _ip_to_float(stat_a.get("inningsPitched", "0"))
            er_a  = int(stat_a.get("earnedRuns", 0) or 0)
            if ip_a > 0:
                era_all_list.append(round(er_a / ip_a * 9, 2))
            vs_log_all.append({"d": (sp.get("date") or ""),
                               "k": stat_a.get("strikeOuts", 0),
                               "h": int(stat_a.get("hits", 0) or 0),
                               "er": er_a,
                               "bb": int(stat_a.get("baseOnBalls", 0) or 0),
                               "outs": round(ip_a * 3),
                               "ip": stat_a.get("inningsPitched", "")})
            if sp.get("isHome") != is_home: continue
            stat = stat_a
            ip = ip_a
            k = stat.get("strikeOuts", 0)
            h = int(stat.get("hits", 0) or 0)   # "hits" in pitching gameLog = hits ALLOWED
            er = er_a
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
    vs_log_all.sort(key=lambda e: e["d"], reverse=True)
    for e in vs_log_all:
        e["d"] = (e["d"] or "")[2:]
    if len(k_list) < MIN_STARTS:
        # K projection unavailable (H/A-matched starts too sparse). Fall back to
        # all-venue ERA so Game Predictor doesn't show blank for the starter row.
        era_fb = round(sum(era_all_list) / len(era_all_list), 2) if era_all_list else None
        return {"avg_k": None, "starts": len(k_list), "k_list": k_list,
                "min_k": None, "max_k": None, "avg_ip": None, "era": era_fb,
                "avg_hits": None, "h_list": h_list, "avg_er": None, "er_list": er_list,
                "avg_outs": None, "outs_list": outs_list,
                "avg_bb": None, "bb_list": bb_list,
                "vs_opp_log": vs_log_all if vs_log_all else vs_log}
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


def career_ha_era(pitcher_id: int, side: str) -> float | None:
    """Lifetime ERA in home starts (side='HOME') or away starts (side='AWAY').
    Sums ER + IP across K_SEASONS game logs filtered to that venue, returns
    9*ER/IP. Requires at least 3 qualifying starts (MIN_IP_START each). Cached.
    Game logs are already cached by _get_pitching_logs so these calls are cheap."""
    key = (pitcher_id, side)
    if key in _CAREER_HA_ERA_CACHE:
        return _CAREER_HA_ERA_CACHE[key]
    is_home = (side == "HOME")
    tot_er = 0.0; tot_ip = 0.0; n_starts = 0
    for season in reversed(K_SEASONS):
        splits = _get_pitching_logs(pitcher_id, season)
        for sp in splits:
            stat = sp.get("stat", {})
            if int(stat.get("gamesStarted", 0) or 0) < 1:
                continue
            if sp.get("isHome") != is_home:
                continue
            ip_f = _ip_to_float(stat.get("inningsPitched", "0"))
            if ip_f < MIN_IP_START:
                continue
            tot_er += int(stat.get("earnedRuns", 0) or 0)
            tot_ip += ip_f
            n_starts += 1
    result = round(tot_er / tot_ip * 9, 2) if n_starts >= 3 and tot_ip > 0 else None
    _CAREER_HA_ERA_CACHE[key] = result
    return result


def fetch_recent_sp_form(pitcher_id: int) -> dict:
    """Return {r_er, r_outs} from the last 5 starts for the GP recent-form row.
    Reuses _get_recent_k_form's cached game logs — no extra API call when the
    pitcher was already evaluated in the K pipeline."""
    try:
        d = _get_recent_k_form(int(pitcher_id))
        return {"r_er": d.get("recent_avg_er"), "r_outs": d.get("recent_avg_outs")}
    except Exception:
        return {"r_er": None, "r_outs": None}


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


def _fetch_game_umps(run_date: str) -> dict:
    """Return {(norm_home, norm_away): ump_name} for today's games via MLB officials hydration."""
    try:
        r = requests.get(
            f"{MLB_API}/schedule",
            params={"sportId": 1, "date": run_date,
                    "hydrate": "team,officials", "gameType": "R"},
            timeout=12)
        result = {}
        for d in r.json().get("dates", []):
            for game in d.get("games", []):
                officials = game.get("officials", [])
                hp = next((o.get("official", {}).get("fullName", "")
                           for o in officials if o.get("officialType") == "Home Plate"), None)
                if hp:
                    home = _normalize(game.get("teams", {}).get("home", {}).get("team", {}).get("name", ""))
                    away = _normalize(game.get("teams", {}).get("away", {}).get("team", {}).get("name", ""))
                    result[(home, away)] = hp
        return result
    except Exception:
        return {}


def _fetch_ump_stats(ump_name: str) -> dict | None:
    """Fetch ump K-rate from umpscorecards.com; return chip dict or None on failure."""
    try:
        import datetime
        year = str(datetime.date.today().year)
        r = requests.get("https://umpscorecards.com/api/umpires/",
                         params={"year": year}, timeout=8)
        data = r.json()
        items = data if isinstance(data, list) else (data.get("umpires") or data.get("data") or [])
        norm_target = _normalize(ump_name)
        for u in items:
            uname = u.get("name") or u.get("umpire_name") or u.get("fullName") or ""
            if _normalize(uname) != norm_target:
                continue
            games   = int(u.get("games") or u.get("game_count") or 0)
            k_total = float(u.get("total_strikeouts") or u.get("strikeouts")
                            or u.get("k_total") or u.get("totalStrikeouts") or 0)
            k_pg    = float(u.get("k_per_game") or u.get("kPerGame") or 0) or (
                      round(k_total / games, 2) if games else 0)
            if not k_pg:
                continue
            factor = round(k_pg / LEAGUE_AVG_K_PER_GAME, 3)
            zone   = "WIDE" if factor >= 1.03 else ("TIGHT" if factor <= 0.97 else "NORMAL")
            zone_lbl = {"WIDE": "Wide Zone", "TIGHT": "Tight Zone", "NORMAL": "Normal Zone"}[zone]
            return {"name": ump_name, "summary": f"{ump_name} \u00b7 {zone_lbl}",
                    "zone": zone, "games": games, "k_per_game": k_pg, "kFactor": factor}
        return None
    except Exception:
        return None


def _get_pitch_hand(pitcher_id: int) -> str:
    """Pitcher's throwing hand ('R'/'L'), cached. Defaults 'R' on failure."""
    if pitcher_id in _PITCH_HAND_CACHE:
        return _PITCH_HAND_CACHE[pitcher_id]
    hand = "R"
    try:
        r = requests.get(f"{MLB_API}/people", params={"personIds": pitcher_id}, timeout=8)
        hand = ((r.json().get("people", [{}]) or [{}])[0].get("pitchHand", {}) or {}).get("code", "R") or "R"
    except Exception:
        pass
    _PITCH_HAND_CACHE[pitcher_id] = hand
    return hand


def _get_opp_k_pct_vs_hand(opp_name: str, hand: str, season) -> float | None:
    """Opponent team's strikeout rate (SO/PA) vs the pitcher's hand. Cached."""
    opp_id = _get_team_id(opp_name)
    if not opp_id:
        return None
    key = (opp_id, hand, str(season))
    if key in _OPP_KPCT_CACHE:
        return _OPP_KPCT_CACHE[key]
    code = "vl" if hand == "L" else "vr"
    val = None
    try:
        r = requests.get(f"{MLB_API}/teams/{opp_id}/stats",
            params={"stats": "statSplits", "sitCodes": code, "group": "hitting",
                    "season": str(season), "gameType": "R"}, timeout=10)
        for s in r.json().get("stats", []):
            for sp in s.get("splits", []):
                st = sp.get("stat", {})
                pa = int(st.get("plateAppearances", 0) or 0)
                so = int(st.get("strikeOuts", 0) or 0)
                if pa:
                    val = round(so / pa, 3)
    except Exception:
        pass
    _OPP_KPCT_CACHE[key] = val
    return val


def _get_opp_split_rates(opp_name: str, hand: str, season):
    """Opponent team's hitting rates vs the pitcher's hand, in ONE statSplits call.
    Returns {k_pct, avg, bb_pct, ops} (powers hits/walks/earned-runs projections),
    or None on failure. Cached by (opp_id, hand, season)."""
    opp_id = _get_team_id(opp_name)
    if not opp_id:
        return None
    key = (opp_id, hand, str(season))
    if key in _OPP_SPLIT_CACHE:
        return _OPP_SPLIT_CACHE[key]
    code = "vl" if hand == "L" else "vr"
    rates = None
    try:
        r = requests.get(f"{MLB_API}/teams/{opp_id}/stats",
            params={"stats": "statSplits", "sitCodes": code, "group": "hitting",
                    "season": str(season), "gameType": "R"}, timeout=10)
        for s in r.json().get("stats", []):
            for sp in s.get("splits", []):
                st = sp.get("stat", {})
                pa = int(st.get("plateAppearances", 0) or 0)
                ab = int(st.get("atBats", 0) or 0)
                so = int(st.get("strikeOuts", 0) or 0)
                bb = int(st.get("baseOnBalls", 0) or 0)
                h  = int(st.get("hits", 0) or 0)
                if not pa:
                    continue
                def _f(x):
                    try:
                        return float(x)
                    except Exception:
                        return None
                avg = _f(st.get("avg"))
                if avg is None and ab:
                    avg = round(h / ab, 3)
                rates = {"k_pct":  round(so / pa, 3),
                         "bb_pct": round(bb / pa, 3),
                         "avg":    avg,
                         "ops":    _f(st.get("ops"))}
    except Exception:
        pass
    _OPP_SPLIT_CACHE[key] = rates
    return rates


def _fetch_whiff_map(year: str) -> dict:
    """Bulk-fetch every pitcher's whiff% from Baseball Savant once per year. Cached.
    Savant throttles rapid requests with an EMPTY 200 body, so retry once on empty.
    Only caches a NON-empty result — an empty fetch stays uncached so a later
    pitcher eval can re-try rather than serving a permanently blank map."""
    if _WHIFF_CACHE.get(year):
        return _WHIFF_CACHE[year]
    import csv, io
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
    out = {}
    for _attempt in range(2):
        try:
            r = requests.get("https://baseballsavant.mlb.com/leaderboard/custom",
                params={"year": str(year), "type": "pitcher", "filter": "", "min": "10",
                        "selections": "whiff_percent", "csv": "true"},
                headers=hdrs, timeout=15)
            for row in csv.DictReader(io.StringIO(r.text.lstrip("\ufeff"))):
                try:
                    pid = int(row.get("player_id") or 0)
                    wp = row.get("whiff_percent")
                    if pid and wp not in (None, ""):
                        out[pid] = float(wp)
                except Exception:
                    continue
        except Exception:
            pass
        if out:
            break
        time.sleep(1.0)
    if out:
        _WHIFF_CACHE[year] = out
    return out


def _whiff_lookup(pitcher_id: int) -> float | None:
    """This pitcher's whiff% — current season, falling back to prior year."""
    cur = _WHIFF_CACHE.get(SEASON, {})
    if pitcher_id in cur:
        return cur[pitcher_id]
    prev = _WHIFF_CACHE.get(str(int(SEASON) - 1), {})
    return prev.get(pitcher_id)


def _fetch_gb_xwoba_map(year: str) -> dict:
    """Bulk-fetch pitcher GB% and xwOBA-against from Baseball Savant. Cached per year.
    Returns {player_id: {gb_pct: float, xwoba: float}}. Falls back to {} on error."""
    if _GB_XWOBA_CACHE.get(year):
        return _GB_XWOBA_CACHE[year]
    import csv, io
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
    out = {}
    for _attempt in range(2):
        try:
            r = requests.get("https://baseballsavant.mlb.com/leaderboard/custom",
                params={"year": str(year), "type": "pitcher", "filter": "", "min": "10",
                        "selections": "gb_percent,xwoba", "csv": "true"},
                headers=hdrs, timeout=15)
            for row in csv.DictReader(io.StringIO(r.text.lstrip("\ufeff"))):
                try:
                    pid = int(row.get("player_id") or 0)
                    if not pid:
                        continue
                    gb_raw = (row.get("gb_percent") or row.get("groundballs_percent") or
                              row.get("gb%") or "")
                    xw_raw = (row.get("xwoba") or row.get("est_woba") or "")
                    entry: dict = {}
                    if gb_raw not in (None, ""):
                        entry["gb_pct"] = float(gb_raw)
                    if xw_raw not in (None, ""):
                        entry["xwoba"] = float(xw_raw)
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
        _GB_XWOBA_CACHE[year] = out
    return out


def _gb_xwoba_lookup(pitcher_id: int) -> dict:
    """Return {gb_pct, xwoba} for this pitcher — current season with prior-year fallback."""
    cur = _GB_XWOBA_CACHE.get(SEASON, {})
    if pitcher_id in cur:
        return cur[pitcher_id]
    prev = _GB_XWOBA_CACHE.get(str(int(SEASON) - 1), {})
    return prev.get(pitcher_id, {})


def _fetch_velo_map(year: str) -> dict:
    """Bulk-fetch pitcher avg fastball velocity from Baseball Savant. Cached per year.
    Returns {player_id: avg_velo_mph}. Falls back to {} on error."""
    if _VELO_CACHE.get(year):
        return _VELO_CACHE[year]
    import csv, io
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
    out = {}
    for _attempt in range(2):
        try:
            r = requests.get("https://baseballsavant.mlb.com/leaderboard/custom",
                params={"year": str(year), "type": "pitcher", "filter": "", "min": "10",
                        "selections": "fastball_avg_speed", "csv": "true"},
                headers=hdrs, timeout=15)
            for row in csv.DictReader(io.StringIO(r.text.lstrip("\ufeff"))):
                try:
                    pid = int(row.get("player_id") or 0)
                    rv  = (row.get("fastball_avg_speed") or row.get("release_speed_avg") or row.get("avg_speed") or "")
                    if pid and rv not in (None, ""):
                        out[pid] = float(rv)
                except Exception:
                    continue
        except Exception:
            pass
        if out: break
        time.sleep(1.0)
    if out:
        _VELO_CACHE[year] = out
    return out


def _velo_lookup(pitcher_id: int) -> float | None:
    """Return pitcher avg fastball velocity — current season, prior-year fallback."""
    cur = _VELO_CACHE.get(SEASON, {})
    if pitcher_id in cur:
        return cur[pitcher_id]
    prev = _VELO_CACHE.get(str(int(SEASON) - 1), {})
    return prev.get(pitcher_id)


def _fetch_krate_map(year: str) -> dict:
    """Bulk-fetch pitcher strikeout rate (K%) from Baseball Savant. Cached per year.
    K% league avg ~22; higher = more swing-and-miss / strikeout ability → more Ks.
    (Replaces Stuff+, which Savant dropped from the custom leaderboard.)"""
    if _KRATE_CACHE.get(year): return _KRATE_CACHE[year]
    import csv, io
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
    out = {}
    for _attempt in range(2):
        try:
            r = requests.get("https://baseballsavant.mlb.com/leaderboard/custom",
                params={"year": str(year), "type": "pitcher", "filter": "", "min": "10",
                        "selections": "k_percent", "csv": "true"},
                headers=hdrs, timeout=15)
            for row in csv.DictReader(io.StringIO(r.text.lstrip("\ufeff"))):
                try:
                    pid = int(row.get("player_id") or 0)
                    sp  = row.get("k_percent") or ""
                    if pid and sp not in (None, ""):
                        out[pid] = float(sp)
                except Exception:
                    continue
        except Exception:
            pass
        if out: break
        time.sleep(1.0)
    if out: _KRATE_CACHE[year] = out
    return out


def _krate_lookup(pitcher_id: int) -> float | None:
    """Return pitcher strikeout rate (K%) — current season, prior-year fallback."""
    cur = _KRATE_CACHE.get(SEASON, {})
    if pitcher_id in cur: return cur[pitcher_id]
    prev = _KRATE_CACHE.get(str(int(SEASON) - 1), {})
    return prev.get(pitcher_id)


def _pk_fetch_one_pt(args) -> None:
    """Fetch pitcher pitch-usage% or batter wOBA vs pitch type from Savant arsenal endpoint."""
    import csv, io
    pt, ptype, year = args
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
    try:
        r = requests.get(
            "https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats",
            params={"type": ptype, "pitchType": pt, "year": str(year),
                    "position": "", "team": "", "min": "1",
                    "stat": "p_run_exp", "sort": "1", "sortDir": "desc", "csv": "true"},
            headers=hdrs, timeout=15)
        txt = r.text.lstrip("\ufeff").strip()
        if not txt or txt.startswith("<"): return
        for row in csv.DictReader(io.StringIO(txt)):
            try:
                pid = int(row.get("player_id") or 0)
                if not pid: continue
                if ptype == "pitcher":
                    pct = float(row.get("pitch_usage") or row.get("pitch_percent") or 0)
                    _PK_ARSENAL_CACHE.setdefault(pid, {})[pt] = pct
                else:
                    w = row.get("woba") or row.get("est_woba") or ""
                    if w:
                        _PK_BATTER_WOBA_CACHE.setdefault(pid, {})[pt] = float(w)
            except Exception:
                continue
    except Exception:
        pass


def _pk_load_pitch_data(year: str) -> None:
    """Parallel-fetch pitcher arsenal% + batter wOBA vs pitch type from Savant.
    No-op if already loaded for this year."""
    if year in _PK_PITCH_LOADED: return
    combos = [(pt, ptype, year)
              for pt in _PK_PITCH_TYPES
              for ptype in ("pitcher", "batter")]
    with ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(_pk_fetch_one_pt, combos))
    _PK_PITCH_LOADED.add(year)


def _pk_fetch_lineup_map(run_date: str) -> dict:
    """Fetch tonight's confirmed lineups. Returns {team_name_lower: [batter_id_int]}.
    Falls back to {} when lineups aren't posted yet."""
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": run_date, "hydrate": "lineups"},
            timeout=12)
        out: dict = {}
        for date_entry in r.json().get("dates", []):
            for game in date_entry.get("games", []):
                lu    = game.get("lineups", {})
                teams = game.get("teams", {})
                for side_key, lu_key in (("home", "homePlayers"), ("away", "awayPlayers")):
                    tname = (teams.get(side_key, {}).get("team", {}).get("name") or "").lower()
                    ids   = [int(p["id"]) for p in lu.get(lu_key, []) if p.get("id")]
                    if tname and ids:
                        out[tname] = ids
        return out
    except Exception:
        return {}


def _arsenal_opp_adj(pitcher_id: int, opp_team: str) -> float:
    """Compute arsenal-vs-lineup matchup factor for K projection.
    Finds pitcher's primary pitch type (by usage%), then measures tonight's opp lineup
    avg wOBA vs that pitch vs league average.  Weak lineup vs pitch → K boost; strong → drag.
    Returns factor in [0.94, 1.06]. Returns 1.0 when data is sparse."""
    arsenal = _PK_ARSENAL_CACHE.get(pitcher_id, {})
    if not arsenal: return 1.0
    primary_pt = max(arsenal, key=lambda k: arsenal[k])
    if arsenal[primary_pt] < 20: return 1.0   # < 20% usage = not truly dominant
    opp_norm = opp_team.lower()
    lineup_ids = next(
        (ids for team, ids in _PK_LINEUP_MAP.items()
         if opp_norm in team or team in opp_norm or
         (opp_norm.split()[-1:] or [''])[0] in team),
        None)
    if not lineup_ids: return 1.0
    wobas = [_PK_BATTER_WOBA_CACHE[bid][primary_pt]
             for bid in lineup_ids
             if bid in _PK_BATTER_WOBA_CACHE and primary_pt in _PK_BATTER_WOBA_CACHE[bid]]
    if len(wobas) < 3: return 1.0
    avg_woba = sum(wobas) / len(wobas)
    gap = avg_woba - LEAGUE_AVG_PITCH_WOBA  # positive = lineup hits this pitch well = K drag
    return max(0.94, min(1.06, 1.0 - gap * 3.0))


def _fetch_game_totals(run_date: str) -> dict:
    """Fetch today's MLB game O/U totals from the Odds API.
    Returns {(norm_home, norm_away): total_line}. Cached for the run."""
    global _GAME_TOTALS
    if _GAME_TOTALS:
        return _GAME_TOTALS
    try:
        r = requests.get(f"{ODDS_BASE}/sports/baseball_mlb/odds",
            params={"apiKey": ODDS_API_KEY, "regions": "us,us2,ca", "markets": "totals",
                    "dateFormat": "iso", "oddsFormat": "american"},
            timeout=15)
        out = {}
        for ev in r.json():
            ht = _normalize(ev.get("home_team", ""))
            at = _normalize(ev.get("away_team", ""))
            for bk in ev.get("bookmakers", []):
                for mkt in bk.get("markets", []):
                    if mkt.get("key") != "totals":
                        continue
                    for oc in mkt.get("outcomes", []):
                        if oc.get("name") == "Over":
                            try:
                                pt = float(oc.get("point", 0))
                                if pt > 0:
                                    out[(ht, at)] = pt
                            except Exception:
                                pass
                    if (ht, at) in out:
                        break
                if (ht, at) in out:
                    break
        _GAME_TOTALS = out
    except Exception:
        pass
    return _GAME_TOTALS


def _lookup_game_total(pitcher_team: str, opp: str) -> float | None:
    """Return the game O/U total line for this pitcher's matchup, or None."""
    pt = _normalize(pitcher_team)
    op = _normalize(opp)
    for (ht, at), total in _GAME_TOTALS.items():
        if (pt in ht or ht in pt) and (op in at or at in op):
            return total
        if (pt in at or at in pt) and (op in ht or ht in op):
            return total
    return None


def _fetch_game_moneylines(run_date: str) -> dict:
    """Fetch today's MLB moneyline (h2h) prices from the Odds API.
    Returns {(norm_home, norm_away): (home_american, away_american)}. Cached
    for the run. Silent on failure (-> {}); the Game Predictor treats a missing
    line as 'no market' and shows model-only."""
    global _GAME_MONEYLINES
    if _GAME_MONEYLINES:
        return _GAME_MONEYLINES
    try:
        r = requests.get(f"{ODDS_BASE}/sports/baseball_mlb/odds",
            params={"apiKey": ODDS_API_KEY, "regions": "us,us2,ca", "markets": "h2h",
                    "dateFormat": "iso", "oddsFormat": "american"},
            timeout=15)
        out = {}
        for ev in r.json():
            ht = _normalize(ev.get("home_team", ""))
            at = _normalize(ev.get("away_team", ""))
            home_full = ev.get("home_team", "")
            away_full = ev.get("away_team", "")
            for bk in ev.get("bookmakers", []):
                hp = ap = None
                for mkt in bk.get("markets", []):
                    if mkt.get("key") != "h2h":
                        continue
                    for oc in mkt.get("outcomes", []):
                        try:
                            price = float(oc.get("price"))
                        except Exception:
                            continue
                        nm = oc.get("name", "")
                        if nm == home_full:
                            hp = price
                        elif nm == away_full:
                            ap = price
                if hp is not None and ap is not None:
                    out[(ht, at)] = (hp, ap)
                    break
        _GAME_MONEYLINES = out
    except Exception:
        pass
    return _GAME_MONEYLINES


def _lookup_game_ml(home: str, away: str):
    """Return (home_american, away_american) moneyline for this matchup, or None."""
    hn = _normalize(home)
    an = _normalize(away)
    for (ht, at), prices in _GAME_MONEYLINES.items():
        if (hn in ht or ht in hn) and (an in at or at in an):
            return prices
        if (hn in at or at in hn) and (an in ht or ht in an):
            return (prices[1], prices[0])
    return None


def _days_rest(last_date: str, run_date: str) -> int | None:
    """Days between the pitcher's last start and tonight's game."""
    try:
        import datetime
        d1 = datetime.date.fromisoformat(last_date[:10])
        d2 = datetime.date.fromisoformat(run_date[:10])
        diff = (d2 - d1).days
        return diff if diff >= 0 else None
    except Exception:
        return None


def _project_k(blended_avg, opp_k_pct, whiff_pct, days_rest,
               gb_pct=None, xwoba_pct=None, implied_total=None, velo_avg=None,
               k_rate=None, arsenal_f=1.0):
    """Opponent-adjusted K projection: blend × hand × whiff × rest × GB × xwOBA × total × velo × krate × arsenal.
    Each factor is clamped so a single thin signal can't swing the pick wildly.
    gb_pct: pitcher's groundball rate — groundballers miss fewer bats (slight K drag).
    xwoba_pct: pitcher's xwOBA-against — low xwOBA = limits hard contact = quality pitcher → more Ks.
    implied_total: today's game O/U line — low total = pitcher dominance expected → boost Ks.
    velo_avg: pitcher avg fastball velocity — higher velo = more swing-and-miss → K boost.
    k_rate: pitcher season strikeout rate (K%, ~22 avg) — high-K arms project for more Ks.
    arsenal_f: pre-computed lineup matchup factor — how well opp hits pitcher's primary pitch."""
    if blended_avg is None:
        return None, {}
    hand_f = 1.0
    if opp_k_pct:
        hand_f = max(0.85, min(1.15, opp_k_pct / LEAGUE_K_PCT))
    whiff_f = 1.0
    if whiff_pct:
        whiff_f = max(0.90, min(1.12, 1 + (whiff_pct - LEAGUE_WHIFF) * 0.008))
    rest_f = 1.0
    if days_rest is not None:
        if days_rest >= 6:
            rest_f = 1.03
        elif days_rest <= 3:
            rest_f = 0.97
    gb_f = 1.0
    if gb_pct is not None:
        gb_f = max(0.97, min(1.03, 1.0 - (gb_pct - LEAGUE_GB_PCT) * 0.002))
    xwoba_f = 1.0
    if xwoba_pct is not None:
        xwoba_f = max(0.94, min(1.06, 1.0 + (LEAGUE_XWOBA - xwoba_pct) * 1.5))
    implied_f = 1.0
    if implied_total is not None:
        implied_f = max(0.94, min(1.06, 1.0 + (LEAGUE_TOTAL - implied_total) * 0.02))
    velo_f = 1.0
    if velo_avg is not None:
        # Higher velo = more swing-and-miss = more Ks. >95 mph = +3%; <90 mph = -5%.
        velo_f = max(0.95, min(1.05, 1.0 + (velo_avg - LEAGUE_VELO) * 0.006))
    krate_f = 1.0
    if k_rate is not None:
        # K% ~22 = avg. Each point above/below scales ±0.5% (capped ±3.5%).
        krate_f = max(0.965, min(1.035, 1.0 + (k_rate - LEAGUE_KRATE) * 0.005))
    proj = round(blended_avg * hand_f * whiff_f * rest_f * gb_f * xwoba_f * implied_f * velo_f * krate_f * arsenal_f, 1)
    factors = {"hand": round(hand_f, 3), "whiff": round(whiff_f, 3), "rest": round(rest_f, 3),
               "gb": round(gb_f, 3), "xwoba": round(xwoba_f, 3), "implied": round(implied_f, 3),
               "velo": round(velo_f, 3), "krate": round(krate_f, 3), "arsenal": round(arsenal_f, 3),
               "opp_k_pct": opp_k_pct, "whiff_pct": whiff_pct, "days_rest": days_rest,
               "gb_pct": gb_pct, "xwoba_pct": xwoba_pct, "implied_total": implied_total,
               "velo_avg": velo_avg, "k_rate": k_rate}
    return proj, factors


def _project_prop(market, blended, rates, whiff_pct,
                  gb_pct=None, xwoba_pct=None, implied_total=None):
    """Opponent-adjusted projection for hits/walks/earned-runs (parallel to
    _project_k). Dominant signal = opp matchup rate vs pitcher hand; whiff% is a
    smaller inverse nudge; GB%, xwOBA-against, and implied total add further
    context. Each factor clamped so one thin signal can't swing the pick wildly.
    Returns (proj, factors) or (None, {})."""
    if blended is None or not rates:
        return None, {}
    hand_f, whiff_f, gb_f, xwoba_f, implied_f = 1.0, 1.0, 1.0, 1.0, 1.0
    if market == "pitcher_hits_allowed":
        if rates.get("avg"):
            hand_f = max(0.85, min(1.15, rates["avg"] / LEAGUE_AVG))
        if whiff_pct:
            whiff_f = max(0.92, min(1.08, 1 - (whiff_pct - LEAGUE_WHIFF) * 0.006))
        if gb_pct is not None:
            gb_f = max(0.90, min(1.10, 1.0 - (gb_pct - LEAGUE_GB_PCT) * 0.004))
        if xwoba_pct is not None:
            xwoba_f = max(0.90, min(1.10, 1.0 + (xwoba_pct - LEAGUE_XWOBA) * 2.0))
        if implied_total is not None:
            implied_f = max(0.90, min(1.10, 1.0 + (implied_total - LEAGUE_TOTAL) * 0.025))
    elif market == "pitcher_walks":
        if rates.get("bb_pct"):
            hand_f = max(0.85, min(1.15, rates["bb_pct"] / LEAGUE_BB_PCT))
        if implied_total is not None:
            implied_f = max(0.97, min(1.03, 1.0 + (implied_total - LEAGUE_TOTAL) * 0.01))
    elif market == "pitcher_earned_runs":
        if rates.get("ops"):
            hand_f = max(0.85, min(1.15, rates["ops"] / LEAGUE_OPS))
        if whiff_pct:
            whiff_f = max(0.94, min(1.06, 1 - (whiff_pct - LEAGUE_WHIFF) * 0.004))
        if gb_pct is not None:
            gb_f = max(0.88, min(1.12, 1.0 - (gb_pct - LEAGUE_GB_PCT) * 0.005))
        if xwoba_pct is not None:
            xwoba_f = max(0.88, min(1.12, 1.0 + (xwoba_pct - LEAGUE_XWOBA) * 2.5))
        if implied_total is not None:
            implied_f = max(0.88, min(1.12, 1.0 + (implied_total - LEAGUE_TOTAL) * 0.03))
    elif market == "pitcher_outs":
        # Outs = how deep the start goes. Tougher offense (higher OPS / xwOBA) and a
        # higher game total => pulled earlier => FEWER outs (inverse signals).
        # Whiffs and grounders => more efficient innings => slightly MORE outs.
        if rates.get("ops"):
            hand_f = max(0.85, min(1.15, 1.0 - (rates["ops"] - LEAGUE_OPS) * 1.5))
        if whiff_pct:
            whiff_f = max(0.95, min(1.05, 1.0 + (whiff_pct - LEAGUE_WHIFF) * 0.004))
        if gb_pct is not None:
            gb_f = max(0.92, min(1.08, 1.0 + (gb_pct - LEAGUE_GB_PCT) * 0.004))
        if xwoba_pct is not None:
            xwoba_f = max(0.90, min(1.10, 1.0 - (xwoba_pct - LEAGUE_XWOBA) * 2.0))
        if implied_total is not None:
            implied_f = max(0.90, min(1.10, 1.0 - (implied_total - LEAGUE_TOTAL) * 0.025))
    else:
        return None, {}
    proj = round(blended * hand_f * whiff_f * gb_f * xwoba_f * implied_f, 1)
    factors = {"hand": round(hand_f, 3), "whiff": round(whiff_f, 3),
               "gb": round(gb_f, 3), "xwoba": round(xwoba_f, 3),
               "implied": round(implied_f, 3),
               "rates": rates, "whiff_pct": whiff_pct,
               "gb_pct": gb_pct, "xwoba_pct": xwoba_pct, "implied_total": implied_total}
    return proj, factors


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


def _build_prop_picks(name, team, opp, side, hist, rf, pid=None,
                      gb_pct=None, xwoba_pct=None, implied_total=None) -> list:
    """One Over/Under pick per prop market that has a posted line in PROP_ODDS.
    Uniform dict so the frontend can render all 3 categories generically."""
    props = []
    nkey = _normalize(name)
    # Computed lazily on the first projected market that has a posted line, so a
    # pitcher with no prop lines costs zero extra API calls.
    pit_hand   = _get_pitch_hand(pid) if pid else "R"
    _whiff     = _whiff_lookup(pid) if pid else None
    _opp_rates = None
    _rates_done = False
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

        # ── Opponent-adjusted projection (hits/walks/ER only; outs excluded) ──
        proj, proj_factors = None, {}
        decision_val = blended
        if blended is not None and market in PROP_EDGE:
            if not _rates_done:
                _opp_rates = _get_opp_split_rates(opp, pit_hand, SEASON)
                _rates_done = True
            proj, proj_factors = _project_prop(
                market, blended, _opp_rates, _whiff,
                gb_pct=gb_pct, xwoba_pct=xwoba_pct, implied_total=implied_total)
            if proj is not None:
                decision_val = proj
                blend_src += (f" → proj {proj} [hand×{proj_factors['hand']}"
                              f" whiff×{proj_factors['whiff']}"
                              f" gb×{proj_factors['gb']}"
                              f" xwoba×{proj_factors['xwoba']}"
                              f" total×{proj_factors['implied']}]")
        edge = PROP_EDGE.get(market, 0.0)

        if decision_val is None:
            pick, pick_note = None, f"no data vs {opp}"
        elif edge and abs(decision_val - line) < edge:
            pick, pick_note = None, f"edge too thin (proj {decision_val} vs line {line}, need ≥{edge})"
        elif decision_val > line:
            pick, pick_note = "OVER",  f"proj {decision_val} > line {line} ({blend_src})"
        elif decision_val < line:
            pick, pick_note = "UNDER", f"proj {decision_val} < line {line} ({blend_src})"
        else:
            pick, pick_note = None, f"proj {decision_val} exactly on line {line}"
        starts = hist.get("starts", 0) if hist else 0
        over_hits = sum(1 for v in career_list if v is not None and v > line)
        hit_rate = f"{over_hits}/{len(career_list)}" if career_list else "—"
        vs_opp_log = [{"d": e.get("d", ""), "v": e.get(vfield), "ip": e.get("ip", "")}
                      for e in ((hist.get("vs_opp_log") if hist else None) or [])]
        recent_log = [{"d": e.get("d", ""), "v": e.get(vfield), "ip": e.get("ip", ""),
                       "opp": e.get("opp", "")}
                      for e in ((rf.get("recent_k_log") if rf else None) or [])]
        pick_dict = {
            "market": market, "label": label, "unit": unit, "_prop": True,
            "name": name, "team": team, "opp": opp, "side": side,
            "pid": pid,
            "line": line, "over_odds": odds.get("over_odds"), "under_odds": odds.get("under_odds"),
            "book": _book_label(odds.get("over_odds_book") if pick == "OVER" else odds.get("under_odds_book")),
            "career_avg": career_avg, "recent_avg": recent_avg, "recent_starts": recent_n,
            "blended": blended, "avg": blended, "blend_src": blend_src, "starts": starts,
            "proj": proj, "proj_factors": proj_factors, "pit_hand": pit_hand,
            "vs_opp_log": vs_opp_log, "recent_log": recent_log,
            "hit_rate": hit_rate, "pick": pick, "pick_note": pick_note,
            "gb_pct": gb_pct, "xwoba_against": xwoba_pct, "implied_total": implied_total,
        }
        if market == "pitcher_walks":
            _opp_bbr = next((v for k, v in TEAM_BB_RANKS.items() if _teams_match(k, opp)), None)
            pick_dict["opp_bb_rank"]  = _opp_bbr["rank"]    if _opp_bbr else None
            pick_dict["opp_bb_pg"]    = _opp_bbr["bb_per_g"] if _opp_bbr else None
            pick_dict["opp_bb_total"] = _opp_bbr["total"]    if _opp_bbr else None
        props.append(pick_dict)
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

        # Full K-rank lookup for the chip (rank 1 = most Ks = easiest matchup)
        _opp_kr = next((v for k, v in TEAM_K_RANKS.items() if _teams_match(k, opp)), None)
        opp_k_rank = _opp_kr["rank"]   if _opp_kr else None
        opp_k_pg   = _opp_kr["k_per_g"] if _opp_kr else None
        opp_k_total = _opp_kr["total"] if _opp_kr else None
        # H/A split: pitcher HOME → opp is traveling (road); pitcher AWAY → opp at home
        _ha_tbl = TEAM_K_RANKS_AWAY if side == "HOME" else TEAM_K_RANKS_HOME
        _opp_kr_ha = next((v for k, v in _ha_tbl.items() if _teams_match(k, opp)), None)
        opp_k_rank_ha = _opp_kr_ha["rank"]    if _opp_kr_ha else None
        opp_k_pg_ha   = _opp_kr_ha["k_per_g"] if _opp_kr_ha else None
        opp_k_context = "road" if side == "HOME" else "home"

        opp_k_info = next((t for t in bottom_k_list if _teams_match(t["name"], opp)), None)
        if bottom_k_set and opp_k_info:
            dq_note = f"Opp {opp} is bottom {BOTTOM_K_TEAMS_N} K team ({opp_k_info['k_per_g']} K/G)"
            logs.append(f"  ❌ {name} — {dq_note}")
            return ({"name": name, "team": pitcher_team, "opp": opp, "side": side,
                     "line": line, "over_odds": pl.get("over_odds"),
                     "under_odds": pl.get("under_odds"), "avg_k": None, "starts": 0,
                     "min_k": None, "max_k": None, "k_history": "—",
                     "book": _book_label(pl.get("over_odds_book") or pl.get("under_odds_book")),
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

        # Blended average: 60% recent form + 40% career H/A vs opp.
        # Recent stuff/form is more predictive of Ks than a thin career-vs-team sample.
        if avg_k is not None and recent_avg_k is not None:
            blended_avg = round(avg_k * 0.4 + recent_avg_k * 0.6, 1)
            blend_src   = f"career {avg_k} · L{recent_starts} {recent_avg_k} (60/40 recent)"
        elif avg_k is not None:
            blended_avg = avg_k
            blend_src   = f"career {avg_k} only"
        elif recent_avg_k is not None:
            blended_avg = recent_avg_k
            blend_src   = f"L{recent_starts} {recent_avg_k} only (no career vs opp)"
        else:
            blended_avg = None
            blend_src   = "no data"

        # ── Opponent-adjusted projection: handedness K% + whiff% + days rest ──
        pit_hand  = _get_pitch_hand(pid) if blended_avg is not None else "R"
        opp_k_pct = _get_opp_k_pct_vs_hand(opp, pit_hand, SEASON) if blended_avg is not None else None
        whiff_pct = _whiff_lookup(pid)
        d_rest    = _days_rest(rf.get("last_start_date", ""), run_date)
        _gbx      = _gb_xwoba_lookup(pid) if pid else {}
        _gb_pct   = _gbx.get("gb_pct")
        _xwoba    = _gbx.get("xwoba")
        _implied  = _lookup_game_total(pitcher_team, opp)
        _velo     = _velo_lookup(pid) if pid else None
        _krate    = _krate_lookup(pid) if pid else None
        _ars_f    = _arsenal_opp_adj(pid, opp) if pid else 1.0
        proj_k, proj_factors = _project_k(blended_avg, opp_k_pct, whiff_pct, d_rest,
                                          gb_pct=_gb_pct, xwoba_pct=_xwoba,
                                          implied_total=_implied, velo_avg=_velo,
                                          k_rate=_krate, arsenal_f=_ars_f)
        # The pick decides off the PROJECTION (falls back to blend if no factors).
        decision_val = proj_k if proj_k is not None else blended_avg
        if proj_factors:
            blend_src += (f" → proj {proj_k} [hand×{proj_factors['hand']}"
                          f" whiff×{proj_factors['whiff']} rest×{proj_factors['rest']}]")

        sugg_line, sugg_odds = None, None
        if decision_val is None:
            pick, pick_note = None, f"N/A — {starts} starts vs {opp}, no recent data"
        elif abs(decision_val - line) < MIN_K_EDGE:
            pick, pick_note = None, f"edge too thin (proj {decision_val} vs line {line}, need ≥{MIN_K_EDGE}K edge — {blend_src})"
            logs.append(f"    ⚠️ skip thin edge {decision_val} vs {line} ({blend_src})")
        elif decision_val > line:
            pick, pick_note = "OVER",  f"proj {decision_val} > line {line} ({blend_src})"
            logs.append(f"    ✅ OVER proj {decision_val} > {line} ({blend_src})")
        elif decision_val < line:
            pick, pick_note = "UNDER", f"proj {decision_val} < line {line} ({blend_src})"
            logs.append(f"    ✅ UNDER proj {decision_val} < {line} ({blend_src})")
        else:
            # projection exactly on line → try alt line from career k_list floor
            sugg_line = (min(k_list) - 0.5) if k_list else None
            k_ladder  = pl.get("over_ladder") or {}
            sugg_odds = k_ladder.get(sugg_line) if sugg_line is not None else None
            if sugg_line is not None and sugg_line < line:
                pick = "OVER"
                pick_note = (f"proj {decision_val} on line {line} → floor OVER {sugg_line} ({blend_src})")
                logs.append(f"    ✅ OVER {sugg_line} (alt) proj on line")
            else:
                pick, pick_note = None, f"proj {decision_val} exactly on line"
                sugg_line, sugg_odds = None, None

        hits_over = sum(1 for k in k_list if k > line) if k_list else 0
        k_hit_rate = f"{hits_over}/{starts}" if starts else "—"
        _ump_data = None
        for (_nh, _na), _uname in game_ump_map.items():
            if _teams_match(_nh, pitcher_team) or _teams_match(_na, pitcher_team):
                _ump_data = _UMP_K_CACHE.get(_uname)
                break
        # K consistency: variance of recent K output across last N starts
        _k_mean = sum(recent_k_list) / len(recent_k_list) if recent_k_list else 0
        _k_std  = (sum((k - _k_mean) ** 2 for k in recent_k_list) / len(recent_k_list)) ** 0.5 \
                  if recent_k_list and len(recent_k_list) >= 2 else None
        _k_consistency = ("consistent" if _k_std is not None and _k_std < 1.5 else
                          "volatile"   if _k_std is not None and _k_std < 3.0 else
                          "boom_bust"  if _k_std is not None else None)
        return ({"name": name, "team": pitcher_team, "opp": opp, "side": side,
                 "line": line, "over_odds": pl.get("over_odds"),
                 "under_odds": pl.get("under_odds"),
                 "book": _book_label(pl.get("over_odds_book") if pick == "OVER" else pl.get("under_odds_book")),
                 "avg_k": avg_k,
                 "starts": starts, "min_k": hist["min_k"] if hist else None,
                 "max_k": hist["max_k"] if hist else None,
                 "avg_ip": hist["avg_ip"] if hist else None,
                 "era":    hist["era"]    if hist else None,
                 "era_home": career_ha_era(pid, "HOME"),
                 "era_away": career_ha_era(pid, "AWAY"),
                 "avg_hits":   hist["avg_hits"]   if hist else None,
                 "avg_er":     hist["avg_er"]     if hist else None,
                 "avg_outs":   hist["avg_outs"]   if hist else None,
                 "avg_bb":     hist["avg_bb"]     if hist else None,
                 "vs_opp_log": hist["vs_opp_log"] if hist else [],
                 "k_hit_rate": k_hit_rate,
                 "k_history": ", ".join(str(k) for k in k_list) if k_list else "—",
                 "sugg_line": sugg_line, "sugg_odds": sugg_odds,
                 "recent_avg_k": recent_avg_k, "recent_k_list": recent_k_list,
                 "recent_starts": recent_starts, "recent_k_log": rf["recent_k_log"],
                 "k_consistency": _k_consistency, "k_std": round(_k_std, 2) if _k_std is not None else None,
                 "blended_avg_k": blended_avg, "blend_src": blend_src,
                 "proj_k": proj_k, "proj_factors": proj_factors,
                 "pit_hand": pit_hand, "days_rest": d_rest,
                 "whiff_pct": whiff_pct, "opp_k_pct_hand": opp_k_pct,
                 "opp_k_rank": opp_k_rank, "opp_k_pg": opp_k_pg, "opp_k_total": opp_k_total,
                 "opp_k_rank_ha": opp_k_rank_ha, "opp_k_pg_ha": opp_k_pg_ha, "opp_k_context": opp_k_context,
                 "gb_pct": _gb_pct, "xwoba_against": _xwoba, "implied_total": _implied,
                 "velo_avg": _velo, "k_rate": _krate, "arsenal_f": round(_ars_f, 3),
                 "pid": pid,
                 "props": _build_prop_picks(name, pitcher_team, opp, side, hist, rf, pid,
                                            gb_pct=_gb_pct, xwoba_pct=_xwoba,
                                            implied_total=_implied),
                 "ump": _ump_data,
                 "pick": pick, "pick_note": pick_note}), logs

    # Today's probable starters — used to map each pitcher to his big-league club
    # (so optioned pitchers don't show their minor-league affiliate) and reused
    # below for no-K-line starters (avoids a second schedule API call).
    prob_starters = _fetch_probable_starters(run_date)
    prob_team_map = {_normalize(s["name"]): s["team"] for s in prob_starters if s.get("team")}

    # Pre-load whiff%, GB%, xwOBA, velocity, K% leaderboards (current + prior-year fallback).
    _fetch_whiff_map(SEASON)
    _fetch_whiff_map(str(int(SEASON) - 1))
    _fetch_gb_xwoba_map(SEASON)
    _fetch_gb_xwoba_map(str(int(SEASON) - 1))
    _fetch_velo_map(SEASON)
    _fetch_velo_map(str(int(SEASON) - 1))
    _fetch_krate_map(SEASON)
    _fetch_krate_map(str(int(SEASON) - 1))
    # Pre-load pitch arsenal + batter wOBA vs pitch type for arsenal matchup.
    _pk_load_pitch_data(SEASON)
    _pk_load_pitch_data(str(int(SEASON) - 1))
    # Pre-fetch tonight's confirmed lineups for arsenal matchup computation.
    _PK_LINEUP_MAP.clear()
    _PK_LINEUP_MAP.update(_pk_fetch_lineup_map(run_date))
    # Pre-fetch today's game O/U totals (one Odds API call, cached for the run).
    _fetch_game_totals(run_date)
    emit({"type": "log", "msg": f"  ✅ Savant loaded: {len(_GB_XWOBA_CACHE.get(SEASON, {}))} pitchers · "
          f"{len(_VELO_CACHE.get(SEASON, {}))} velo · {len(_KRATE_CACHE.get(SEASON, {}))} K% · "
          f"{len(_PK_ARSENAL_CACHE)} arsenals · {len(_PK_LINEUP_MAP)} lineups · {len(_GAME_TOTALS)} totals"})

    # Fetch HP umpires for tonight's games; pre-load K-rate stats per ump.
    game_ump_map = _fetch_game_umps(run_date)
    for _uname in set(game_ump_map.values()):
        if _uname not in _UMP_K_CACHE:
            _UMP_K_CACHE[_uname] = _fetch_ump_stats(_uname)
    emit({"type": "log", "msg": f"  ⚖️ Umpires: {len(game_ump_map)} games · "
          + ", ".join(f"{v} ({'W' if (_UMP_K_CACHE.get(v) or {}).get('zone')=='WIDE' else 'T' if (_UMP_K_CACHE.get(v) or {}).get('zone')=='TIGHT' else 'N'})" for v in set(game_ump_map.values()))})

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
            _opp_kr2 = next((v for k, v in TEAM_K_RANKS.items() if _teams_match(k, st["opp"])), None)
            _ha_tbl2 = TEAM_K_RANKS_AWAY if st["side"] == "HOME" else TEAM_K_RANKS_HOME
            _opp_kr2_ha = next((v for k, v in _ha_tbl2.items() if _teams_match(k, st["opp"])), None)
            _hand2  = _get_pitch_hand(pid2) if pid2 else "R"
            _whiff2 = _whiff_lookup(pid2) if pid2 else None
            _rest2  = _days_rest(rf2.get("last_start_date", ""), run_date)
            _oppkp2 = _get_opp_k_pct_vs_hand(st["opp"], _hand2, SEASON) if pid2 else None
            return {
                "name": st["name"], "team": st["team"], "opp": st["opp"],
                "side": st["side"], "line": None,
                "over_odds": None, "under_odds": None,
                "avg_k": avg_k2, "starts": starts2,
                "min_k": min(k_list2) if k_list2 else None,
                "max_k": max(k_list2) if k_list2 else None,
                "avg_ip": hist2["avg_ip"] if hist2 else None,
                "era": hist2["era"] if hist2 else None,
                "era_home": career_ha_era(pid2, "HOME") if pid2 else None,
                "era_away": career_ha_era(pid2, "AWAY") if pid2 else None,
                "avg_hits": hist2["avg_hits"] if hist2 else None,
                "avg_er":   hist2["avg_er"]   if hist2 else None,
                "avg_outs": hist2["avg_outs"] if hist2 else None,
                "avg_bb":   hist2["avg_bb"]   if hist2 else None,
                "vs_opp_log": hist2["vs_opp_log"] if hist2 else [],
                "k_hit_rate": "—", "k_history": k_history2,
                "recent_avg_k": rf2["recent_avg_k"], "recent_k_list": rf2["recent_k_list"],
                "recent_starts": rf2["recent_starts"], "recent_k_log": rf2["recent_k_log"],
                "blended_avg_k": None, "blend_src": None,
                "proj_k": None, "proj_factors": {},
                "pit_hand": _hand2, "days_rest": _rest2,
                "whiff_pct": _whiff2, "opp_k_pct_hand": _oppkp2,
                "gb_pct": _gb_xwoba_lookup(pid2).get("gb_pct") if pid2 else None,
                "xwoba_against": _gb_xwoba_lookup(pid2).get("xwoba") if pid2 else None,
                "implied_total": _lookup_game_total(st["team"], st["opp"]),
                "velo_avg": _velo_lookup(pid2) if pid2 else None,
                "k_rate": _krate_lookup(pid2) if pid2 else None,
                "opp_k_rank": _opp_kr2["rank"]    if _opp_kr2 else None,
                "opp_k_pg":   _opp_kr2["k_per_g"] if _opp_kr2 else None,
                "opp_k_total": _opp_kr2["total"]  if _opp_kr2 else None,
                "opp_k_rank_ha": _opp_kr2_ha["rank"]    if _opp_kr2_ha else None,
                "opp_k_pg_ha":   _opp_kr2_ha["k_per_g"] if _opp_kr2_ha else None,
                "opp_k_context": "road" if st["side"] == "HOME" else "home",
                "pid": pid2,
                # Pitchers with NO K line may still have hits/outs/ER lines posted —
                # build their prop picks too so the parlay pool is as deep as possible.
                "props": _build_prop_picks(st["name"], st["team"], st["opp"], st["side"], hist2, rf2, pid2),
                "pick": None, "pick_note": "No K line posted today",
                "ump": next((_UMP_K_CACHE.get(un) for (nh, na), un in game_ump_map.items()
                             if _teams_match(nh, st["team"]) or _teams_match(na, st["team"])), None),
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

    return {"picks": confirmed, "all": all_results, "props": prop_picks,
            "team_k_ranks": [{"name": k, "rank": v["rank"], "k_per_g": v["k_per_g"]}
                             for k, v in sorted(TEAM_K_RANKS.items(), key=lambda x: x[1]["rank"])]}
