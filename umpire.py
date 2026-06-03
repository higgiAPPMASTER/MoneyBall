"""
umpire.py — Home-plate umpire effect for MLB props.

All data from the MLB Stats API (statsapi.mlb.com) — NO website scraping.

Umpires move strikeout, walk and run totals via how wide/tight they call the
strike zone. This module figures out (a) WHO is behind the plate for each of
today's games and (b) how that umpire's games have played out this season vs a
league baseline, then exposes per-game multipliers the pipeline uses both for a
display chip AND to nudge pick ordering (reorder only — gates untouched).

Public
------
build_today(run_date, emit=None) -> ump_map
    ump_map: {full_team_name: effect_dict}. Both teams in a game map to the
    SAME home-plate umpire. Returns {} when officials are not posted yet (MLB
    assigns them only a few hours before first pitch).
lookup(ump_map, team_name) -> effect_dict | None   (fuzzy team-name match)

effect_dict
-----------
    name, id, games,
    k_pg, bb_pg, r_pg,                 per-game totals across BOTH teams
    kFactor, bbFactor, rFactor,        ump per-game / league per-game, clamped
    zone  ("WIDE" | "TIGHT" | "EVEN"),
    summary                            chip text

Method
------
1. Today's HP umps:  schedule?date=run_date&hydrate=officials.
2. One ranged officials call (season start..run_date) -> the recent Final games
   each of TODAY's umps worked behind the plate.
3. For those games only, pull boxscores in parallel; per game sum both teams'
   batting K / BB / runs.
4. League baseline = pooled mean across every boxscore fetched (self-
   calibrating); hardcoded fallback if the pool is empty.
5. factor = ump_per_game / league_per_game, clamped to UMP_CLAMP.

Disk-cached per run_date (.umpire_cache/) — only the first run of the day pays
the cost, and it survives Render spin-down. A partial slate (officials not yet
posted for every game) is returned but NOT cached, so a later run refills it.
"""
import os, json, datetime
import concurrent.futures as cf
import requests

BASE = "https://statsapi.mlb.com/api/v1"
CACHE_DIR = ".umpire_cache"

UMP_MAX_GAMES = 20          # most-recent HP games per ump used for the averages
UMP_MIN_GAMES = 5           # below this -> no factor (name-only chip, neutral 1.0)
UMP_CLAMP     = (0.90, 1.10)  # umpire pull is smaller than park/weather
WIDE_CUT      = 1.02        # zoneScore (kFactor/rFactor) >= -> wide zone
TIGHT_CUT     = 0.98        # zoneScore <= -> tight zone

# Hardcoded league per-GAME baselines (both teams combined). Only used if the
# pooled sample is empty; refresh seasonally. ~2024-2025 MLB.
LEAGUE_K_PG  = 16.8
LEAGUE_BB_PG = 6.4
LEAGUE_R_PG  = 8.9

TIMEOUT = 12
# Regular season + postseason only (drop spring 'S', exhibition 'E', all-star 'A').
GAME_TYPES = {"R", "F", "D", "L", "W", "P", "C"}
# Games that will not be played today -> ignored when deciding if the slate is
# "complete" (they never get an HP umpire, so one of them must not block caching).
SKIP_STATES = {"postponed", "cancelled", "canceled", "suspended"}


def _get(url):
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def _norm(s):
    return "".join(c for c in (s or "").lower() if c.isalnum())


def _season_start(run_date):
    try:
        y = int(str(run_date)[:4])
    except Exception:
        y = datetime.date.today().year
    return "%d-03-01" % y


def _cache_path(run_date):
    return os.path.join(CACHE_DIR, "%s.json" % run_date)


def _load_cache(run_date):
    try:
        with open(_cache_path(run_date)) as f:
            return json.load(f)
    except Exception:
        return None


def _save_cache(run_date, data):
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(_cache_path(run_date), "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def _hp(game):
    """(ump_id, ump_name) for the Home Plate official, or (None, None)."""
    for o in game.get("officials", []):
        if o.get("officialType") == "Home Plate":
            off = o.get("official", {}) or {}
            return off.get("id"), off.get("fullName")
    return None, None


def _today_umps(run_date):
    """({full_team_name: (ump_id, ump_name)}, complete_bool) for today's slate.
    complete is True only when every game already has a posted HP umpire."""
    d = _get("%s/schedule?sportId=1&date=%s&hydrate=officials" % (BASE, run_date))
    out = {}
    total = 0
    with_hp = 0
    if not d:
        return out, False
    for dt in d.get("dates", []):
        for g in dt.get("games", []):
            state = (g.get("status", {}) or {}).get("detailedState", "").lower()
            if any(s in state for s in SKIP_STATES):
                continue  # postponed/cancelled -> no ump, no picks, ignore
            total += 1
            uid, uname = _hp(g)
            if not uid:
                continue
            with_hp += 1
            for side in ("home", "away"):
                nm = (((g.get("teams", {}).get(side, {}) or {}).get("team", {}) or {}).get("name"))
                if nm:
                    out[nm] = (uid, uname)
    complete = (total > 0 and with_hp == total)
    return out, complete


def _ump_games(run_date, want_ids):
    """{ump_id: [gamePk,...]} newest-first (<= UMP_MAX_GAMES), restricted to
    want_ids, from one ranged officials call over the season to date."""
    start = _season_start(run_date)
    d = _get("%s/schedule?sportId=1&startDate=%s&endDate=%s&hydrate=officials"
             % (BASE, start, run_date))
    games = {}
    if not d:
        return games
    rows = []
    for dt in d.get("dates", []):
        day = dt.get("date")
        for g in dt.get("games", []):
            if g.get("status", {}).get("abstractGameState") != "Final":
                continue
            if g.get("gameType") and g.get("gameType") not in GAME_TYPES:
                continue
            uid, _ = _hp(g)
            if uid in want_ids and g.get("gamePk"):
                rows.append((day or "", g["gamePk"], uid))
    rows.sort(key=lambda x: x[0], reverse=True)  # newest day first
    for day, pk, uid in rows:
        bucket = games.setdefault(uid, [])
        if len(bucket) < UMP_MAX_GAMES:
            bucket.append(pk)
    return games


def _box_totals(game_pk):
    """(K, BB, R) summed across BOTH teams' batting for one game, or None."""
    d = _get("%s/game/%s/boxscore" % (BASE, game_pk))
    if not d:
        return None
    try:
        k = bb = r = 0
        for side in ("home", "away"):
            bat = d["teams"][side]["teamStats"]["batting"]
            k += bat.get("strikeOuts", 0) or 0
            bb += bat.get("baseOnBalls", 0) or 0
            r += bat.get("runs", 0) or 0
        return (k, bb, r)
    except Exception:
        return None


def build_today(run_date, emit=None):
    cached = _load_cache(run_date)
    if cached is not None:
        return cached

    def log(m):
        if emit:
            try:
                emit({"type": "log", "msg": m})
            except Exception:
                pass

    today, complete = _today_umps(run_date)
    if not today:
        log("  \u2696\ufe0f Umpires not posted yet (assigned a few hours before first pitch)")
        return {}

    want = set(uid for uid, _ in today.values())
    games = _ump_games(run_date, want)

    all_pks = sorted({pk for pks in games.values() for pk in pks})
    box = {}
    if all_pks:
        try:
            with cf.ThreadPoolExecutor(max_workers=10) as ex:
                for pk, res in zip(all_pks, ex.map(_box_totals, all_pks)):
                    if res:
                        box[pk] = res
        except Exception:
            for pk in all_pks:
                res = _box_totals(pk)
                if res:
                    box[pk] = res

    pool = list(box.values())
    if pool:
        lg_k = sum(b[0] for b in pool) / len(pool)
        lg_bb = sum(b[1] for b in pool) / len(pool)
        lg_r = sum(b[2] for b in pool) / len(pool)
    else:
        lg_k, lg_bb, lg_r = LEAGUE_K_PG, LEAGUE_BB_PG, LEAGUE_R_PG
    lg_k = lg_k if lg_k > 0 else LEAGUE_K_PG
    lg_bb = lg_bb if lg_bb > 0 else LEAGUE_BB_PG
    lg_r = lg_r if lg_r > 0 else LEAGUE_R_PG

    def _clamp(x):
        return max(UMP_CLAMP[0], min(UMP_CLAMP[1], x))

    id_name = {}
    for uid, uname in today.values():
        id_name[uid] = uname

    effects = {}
    ready = 0
    for uid in want:
        name = id_name.get(uid, "") or "Umpire"
        pks = [pk for pk in games.get(uid, []) if pk in box]
        n = len(pks)
        if n < UMP_MIN_GAMES:
            effects[uid] = {
                "name": name, "id": uid, "games": n,
                "k_pg": None, "bb_pg": None, "r_pg": None,
                "kFactor": 1.0, "bbFactor": 1.0, "rFactor": 1.0,
                "zone": "EVEN",
                "summary": "\u2696\ufe0f %s \u00b7 n/a" % name,
            }
            continue
        k_pg = sum(box[pk][0] for pk in pks) / n
        bb_pg = sum(box[pk][1] for pk in pks) / n
        r_pg = sum(box[pk][2] for pk in pks) / n
        kF = _clamp(k_pg / lg_k)
        bbF = _clamp(bb_pg / lg_bb)
        rF = _clamp(r_pg / lg_r)
        zscore = (kF / rF) if rF else 1.0
        zone = "WIDE" if zscore >= WIDE_CUT else "TIGHT" if zscore <= TIGHT_CUT else "EVEN"
        kp = round((kF - 1) * 100)
        rp = round((rF - 1) * 100)
        zlabel = {"WIDE": "Wide zone", "TIGHT": "Tight zone", "EVEN": "Even zone"}[zone]
        summary = "\u2696\ufe0f %s \u00b7 %s \u00b7 K %s%d%% \u00b7 R %s%d%%" % (
            name, zlabel, "+" if kp > 0 else "", kp, "+" if rp > 0 else "", rp)
        effects[uid] = {
            "name": name, "id": uid, "games": n,
            "k_pg": round(k_pg, 1), "bb_pg": round(bb_pg, 1), "r_pg": round(r_pg, 1),
            "kFactor": round(kF, 3), "bbFactor": round(bbF, 3), "rFactor": round(rF, 3),
            "zone": zone, "summary": summary,
        }
        ready += 1

    ump_map = {}
    for team_name, (uid, _) in today.items():
        ump_map[team_name] = effects.get(uid)

    log("  \u2696\ufe0f Umpire effect: %d ump(s) graded, %d boxscore(s)%s"
        % (ready, len(box), "" if complete else " (partial slate \u2014 not cached)"))
    if complete:
        _save_cache(run_date, ump_map)
    return ump_map


def lookup(ump_map, team_name):
    if not ump_map or not team_name:
        return None
    if team_name in ump_map:
        return ump_map[team_name]
    tn = _norm(team_name)
    if not tn:
        return None
    for k, v in ump_map.items():
        nk = _norm(k)
        if tn in nk or nk in tn:
            return v
    return None
