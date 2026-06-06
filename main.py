"""
main.py — FastAPI app for MoneyBall
  • POST /api/login          — get JWT token
  • POST /api/run            — kick off pipeline (returns task_id)
  • GET  /api/stream/{id}   — SSE progress stream (token as query param)
  • GET  /api/results/{date} — fetch cached results
  • GET  /                   — serves the frontend SPA
"""
import asyncio, json, os, uuid, glob as _glob
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Optional

_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".pick_cache")
os.makedirs(_CACHE_DIR, exist_ok=True)

def _disk_cache_path(date_str: str) -> str:
    return os.path.join(_CACHE_DIR, f"{date_str}.json")

def _save_disk_cache(date_str: str, result: dict):
    try:
        p = _disk_cache_path(date_str)
        tmp = f"{p}.{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump(result, f)
        os.replace(tmp, p)  # atomic swap — readers never see a half-written file
        # Remove cache files older than 3 days
        for old in _glob.glob(os.path.join(_CACHE_DIR, "*.json")):
            bn = os.path.basename(old).replace(".json", "")
            if bn < str(date.today().isoformat()[:8] + "00")[:10] and bn != date_str:
                try: os.remove(old)
                except: pass
    except Exception as e:
        print(f"[disk_cache] save failed: {e}")

def _load_disk_cache(date_str: str):
    p = _disk_cache_path(date_str)
    if os.path.exists(p):
        try:
            with open(p) as f:
                return json.load(f)
        except Exception as e:
            print(f"[disk_cache] load failed: {e}")
    return None

from fastapi import FastAPI, HTTPException, Form, Request
from fastapi.responses import StreamingResponse, FileResponse, HTMLResponse
import os as _os
from jose import jwt as _jose_jwt
_JWT_SECRET = _os.environ.get("JWT_SECRET", "")

def _verify_hub_token(token: str) -> bool:
    if not token or len(token.split(".")) != 3:
        return False
    if not _JWT_SECRET:
        return False
    try:
        _jose_jwt.decode(token, _JWT_SECRET, algorithms=["HS256"])
        return True
    except Exception:
        return False


# Admin auto-detect: the hub stamps the logged-in user's email into the token
# as "sub". If that email matches the admin email, the picks page turns on the
# admin view automatically — on any device, no key needed. Defaults to the
# owner's email; can be overridden with an ADMIN_EMAIL env var.
_ADMIN_EMAILS = {e.strip().lower() for e in _os.environ.get("ADMIN_EMAIL", "higgi117711@gmail.com").split(",") if e.strip()}


def _token_email(token: str) -> str:
    """Return the email (sub) from a valid hub token, else ''."""
    if not token or len(token.split(".")) != 3 or not _JWT_SECRET:
        return ""
    try:
        payload = _jose_jwt.decode(token, _JWT_SECRET, algorithms=["HS256"])
        return str(payload.get("sub", "")).strip().lower()
    except Exception:
        return ""


def _is_admin_token(token: str) -> bool:
    return bool(_ADMIN_EMAILS) and _token_email(token) in _ADMIN_EMAILS


from replit_push import push_picks_to_replit  # pushes daily picks to Replit DB


app = FastAPI(title="MoneyBall", docs_url=None, redoc_url=None)
executor = ThreadPoolExecutor(max_workers=4)
_tasks: dict = {}
_cache: dict = {}

@app.post("/api/login")
async def login(username: str = Form(...), password: str = Form(...)):
    if username == "higgi" and password == "Elbowlake77":
        return {"access_token": "mpa-token", "username": username}
    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.get("/api/health")
async def health():
    return {"status": "ok", "today": str(date.today())}

@app.get("/api/test-statmuse")
async def test_statmuse():
    return {"ok": True, "message": "✅ MLB Stats API active"}

_CRON_BUSY = False

@app.api_route("/api/cron-run", methods=["GET", "POST"])
async def cron_run(request: Request, date_str: str = ""):
    # Cron-friendly trigger: authed by the static INTERNAL_API_TOKEN secret sent
    # as a header (kept out of the URL so it isn't logged). No expiring hub login
    # needed. Runs the pipeline + caches it so members can pull the picks, and
    # wakes the free-tier app on Render. An in-flight guard blocks overlapping runs.
    global _CRON_BUSY
    import hmac
    secret = os.environ.get("INTERNAL_API_TOKEN", "")
    tok = request.headers.get("X-Internal-Token", "") or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not secret or not hmac.compare_digest(tok or "", secret):
        raise HTTPException(status_code=401, detail="Invalid cron token")
    ds = date_str or date.today().isoformat()
    if _CRON_BUSY:
        return {"ran": False, "cached": ds in _cache, "date": ds, "reason": "already running"}
    _CRON_BUSY = True
    try:
        await asyncio.to_thread(_auto_run_pipeline, ds, "cron")
    finally:
        _CRON_BUSY = False
    cached = (ds in _cache) or bool(_load_disk_cache(ds))
    return {"ran": True, "cached": cached, "date": ds}


@app.post("/api/run")
async def start_run(request: Request, date_str: str, force: bool = False, token: str = ""):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _verify_hub_token(tok):
        raise HTTPException(status_code=401, detail="Subscription required — please log in via moneypicksarena.com")
    if not force:
        if date_str not in _cache:
            disk = _load_disk_cache(date_str)
            if disk:
                _cache[date_str] = disk
    if not force and date_str in _cache and not _cache[date_str].get("stats", {}).get("has_tbd"):
        task_id = str(uuid.uuid4())
        notify  = asyncio.Event()
        _tasks[task_id] = {
            "events": [
                {"type": "cached", "msg": "⚡ Results loaded from cache — no re-run needed"},
                {"type": "done",   "result": _cache[date_str]},
            ],
            "status": "done", "result": _cache[date_str], "notify": notify,
        }
        notify.set()
        return {"task_id": task_id, "cached": True}

    task_id = str(uuid.uuid4())
    loop    = asyncio.get_event_loop()
    notify  = asyncio.Event()
    task    = {"events": [], "status": "running", "result": None, "notify": notify}
    _tasks[task_id] = task

    def emit(event: dict):
        task["events"].append(event)
        loop.call_soon_threadsafe(notify.set)

    def run_in_thread():
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from pipeline import run_pipeline
        try:
            result = run_pipeline(date_str, emit=emit)
            task["status"] = "done"
            task["result"] = result
            # Always persist so the read-only /api/results endpoint (parlay hub)
            # can serve the slate even when a starter is still TBD. The MLB app's
            # own load re-runs when has_tbd to pick up late-named starters.
            _cache[date_str] = result
            try: _update_track_ledger()
            except Exception as _le: print(f"[track_ledger] {_le}")
            _save_disk_cache(date_str, result)
            try:
                # Bake the picks into the page HTML so the Replit hub can serve
                # an instant, no-cold-start snapshot at moneypicksarena.com.
                baked = {**result, "date": date_str}
                inject = (
                    '<script>window.__INITIAL_PICKS__ = '
                    + json.dumps(baked).replace('</', '<\\/')
                    + ';</script></head>'
                )
                snapshot_html = _HTML.replace('</head>', inject, 1)
                push_picks_to_replit("mlb", baked, html=snapshot_html)
            except Exception as _e:
                print(f"[replit_push] mlb push failed: {_e}")
        except Exception as exc:
            import traceback
            emit({"type": "error", "msg": f"{exc}\n{traceback.format_exc()}"})
            task["status"] = "error"

    executor.submit(run_in_thread)
    return {"task_id": task_id, "cached": False}

@app.get("/api/stream/{task_id}")
async def stream_task(task_id: str):
    if task_id not in _tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    task = _tasks[task_id]

    async def event_generator():
        idx = 0
        while True:
            while idx < len(task["events"]):
                yield f"data: {json.dumps(task['events'][idx])}\n\n"
                idx += 1
            if task["status"] in ("done", "error"):
                while idx < len(task["events"]):
                    yield f"data: {json.dumps(task['events'][idx])}\n\n"
                    idx += 1
                return
            task["notify"].clear()
            try:
                await asyncio.wait_for(task["notify"].wait(), timeout=20.0)
            except asyncio.TimeoutError:
                yield ": heartbeat\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"})

@app.get("/api/results/{date_str}")
async def get_results(date_str: str, request: Request, token: str = ""):
    # Read-only: serve saved picks from memory or disk. Never triggers a pipeline
    # run, so any member can load the picks we already have on file. Loading from
    # disk first keeps it working after a Render cold start wipes the in-memory cache.
    # Subscriber-only: enforce the hub token like /api/run so picks aren't scrapeable.
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _verify_hub_token(tok):
        raise HTTPException(status_code=401, detail="Subscription required — please log in via moneypicksarena.com")
    if date_str not in _cache:
        disk = _load_disk_cache(date_str)
        if disk is not None:
            _cache[date_str] = disk
    if date_str in _cache:
        return _cache[date_str]
    raise HTTPException(status_code=404, detail="No results for this date.")


# ── Grading core (shared by /api/grade and the Track Record ledger) ──────
def _mlb_box_lookup(date_str: str):
    """Fetch box scores for a date. Returns (player_stats, name_stats, any_game, all_final).
    MLB's /schedule hydrate=boxscore stopped embedding player stats (returns 0 players),
    so we pull the schedule for game IDs + status, then fetch each started game's boxscore
    from the dedicated /game/{gamePk}/boxscore endpoint (in parallel)."""
    import requests as _rq
    from concurrent.futures import ThreadPoolExecutor as _TPE
    MLB_BASE = "https://statsapi.mlb.com/api/v1"
    try:
        sched = _rq.get(f"{MLB_BASE}/schedule", params={
            "sportId": 1, "date": date_str, "gameType": "R",
        }, timeout=30).json()
    except Exception as e:
        print(f"[box_lookup] schedule fetch failed {date_str}: {e}")
        return {}, {}, False, True
    player_stats: dict = {}
    name_stats: dict   = {}
    any_game = False
    all_final = True
    _NOT_STARTED = ("Scheduled", "Pre-Game", "Warmup", "Postponed",
                    "Cancelled", "Delayed Start")
    games = []
    for d in sched.get("dates", []):
        for game in d.get("games", []):
            any_game = True
            status = game.get("status", {}).get("detailedState", "Scheduled")
            final  = status in ("Final", "Game Over")
            if not final:
                all_final = False
            pk = game.get("gamePk")
            if pk and status not in _NOT_STARTED:
                games.append((pk, status, final))

    def _one(args):
        pk, status, final = args
        try:
            bx = _rq.get(f"{MLB_BASE}/game/{pk}/boxscore", timeout=30).json()
        except Exception as e:
            print(f"[box_lookup] boxscore {pk} fetch failed: {e}")
            return []
        rows = []
        for sd in ("home", "away"):
            td = bx.get("teams", {}).get(sd, {})
            for _key, pdata in td.get("players", {}).items():
                pid       = (pdata.get("person") or {}).get("id")
                full_name = (pdata.get("person") or {}).get("fullName", "")
                if not pid:
                    continue
                bat = pdata.get("stats", {}).get("batting")  or {}
                pit = pdata.get("stats", {}).get("pitching") or {}
                rows.append((int(pid), full_name, {
                    "hits":         bat.get("hits"),
                    "runs":         bat.get("runs"),
                    "total_bases":  bat.get("totalBases"),
                    "rbi":          bat.get("rbi"),
                    "strikeOuts":   pit.get("strikeOuts"),
                    "earnedRuns":   pit.get("earnedRuns"),
                    "outs":         pit.get("outs"),
                    "hits_allowed": pit.get("hits"),
                    "walks":        pit.get("baseOnBalls"),
                    "status": status,
                    "final":  final,
                    "name":   full_name,
                }))
        return (final, rows)

    if games:
        try:
            with _TPE(max_workers=min(8, len(games))) as ex:
                results = list(ex.map(_one, games))
        except Exception as e:
            print(f"[box_lookup] parallel boxscore fetch failed {date_str}: {e}")
            results = [_one(g) for g in games]
        fetch_complete = True
        for gfinal, rows in results:
            if gfinal and not rows:
                fetch_complete = False   # final game but boxscore fetch returned nothing
            for pid, full_name, entry in rows:
                player_stats[pid] = entry
                if full_name:
                    name_stats[full_name.lower()] = entry
        if not fetch_complete:
            all_final = False            # defer locking until a clean pass grades it
    return player_stats, name_stats, any_game, all_final


def _grade_date(date_str: str, picks: dict) -> dict:
    """Grade every pick category for a date against actual box scores.
    Each row carries category + side so the Track Record ledger can tally O/U splits."""
    player_stats, name_stats, any_game, all_final = _mlb_box_lookup(date_str)

    def _lookup(player_id, fallback_name=None):
        if player_id:
            e = player_stats.get(int(player_id))
            if e:
                return e
        if fallback_name:
            return name_stats.get((fallback_name or "").lower())
        return None

    def _grade(pick_dir, line, actual, final):
        if actual is None or not final:
            return "pending"
        if pick_dir == "OVER":
            return "WIN" if actual > float(line) else "LOSS"
        return "WIN" if actual < float(line) else "LOSS"

    # Hitter OVERs — top 10 only (top9 list); also_ran "solid plays" excluded from Track Record
    hitter_overs = []
    for p in (picks.get("top9") or [])[:10]:
        st = _lookup(p.get("player_id"), p.get("full_name") or p.get("name"))
        actual = st["hits"] if st else None
        hitter_overs.append({
            "name": p.get("full_name") or p.get("name", ""),
            "team": p.get("team", ""),
            "category": "Hitter Hits", "side": "OVER",
            "pick": "OVER 0.5 Hits",
            "odds": p.get("hit_odds"),
            "line": 0.5,
            "actual": actual,
            "stat": "Hits",
            "result": _grade("OVER", 0.5, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
        })

    # Under 1.5 Hits — top 10 for Track Record
    hitter_unders = []
    for p in (picks.get("under_picks") or [])[:10]:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["hits"] if st else None
        pick_dir = p.get("pick", "UNDER")
        hitter_unders.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "Hitter Hits", "side": pick_dir,
            "pick": f"{pick_dir} 1.5 Hits",
            "odds": p.get("under_odds") if pick_dir == "UNDER" else p.get("over_odds"),
            "line": 1.5,
            "actual": actual,
            "stat": "Hits",
            "result": _grade(pick_dir, 1.5, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
        })

    # Runs OVER/UNDER 0.5 — top 10 per side for Track Record
    _runs_all = picks.get("runs_picks") or []
    _runs_capped = [p for p in _runs_all if p.get("pick") == "OVER"][:10] + \
                   [p for p in _runs_all if p.get("pick") == "UNDER"][:10]
    runs = []
    for p in _runs_capped:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["runs"] if st else None
        pick_dir = p.get("pick", "OVER")
        runs.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "Runs", "side": pick_dir,
            "pick": f"{pick_dir} 0.5 Runs",
            "odds": p.get("over_odds") if pick_dir == "OVER" else p.get("under_odds"),
            "line": 0.5,
            "actual": actual,
            "stat": "Runs",
            "result": _grade(pick_dir, 0.5, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
        })

    # TB Under 1.5 — top 10 for Track Record
    tb_under = []
    for p in (picks.get("tb_picks") or [])[:10]:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["total_bases"] if st else None
        tb_under.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "TB Under", "side": "UNDER",
            "pick": "UNDER 1.5 Total Bases",
            "odds": p.get("tb_under_odds"),
            "line": 1.5,
            "actual": actual,
            "stat": "Total Bases",
            "result": _grade("UNDER", 1.5, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
        })

    # RBI OVER/UNDER — top 10 per side for Track Record
    _rbi_all = picks.get("rbi_picks") or []
    _rbi_capped = [p for p in _rbi_all if p.get("pick") == "OVER"][:10] + \
                  [p for p in _rbi_all if p.get("pick") == "UNDER"][:10]
    rbi_picks = []
    for p in _rbi_capped:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["rbi"] if st else None
        pick_dir = p.get("pick", "OVER")
        line = p.get("line") if p.get("line") is not None else 0.5
        rbi_picks.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "RBI", "side": pick_dir,
            "pick": f"{pick_dir} {line} RBI",
            "odds": p.get("over_odds") if pick_dir == "OVER" else p.get("under_odds"),
            "line": line,
            "actual": actual,
            "stat": "RBI",
            "result": _grade(pick_dir, line, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
        })

    # Pitcher Ks — top 10 for Track Record
    pitcher_ks = []
    for p in ((picks.get("pitcher_k") or {}).get("picks") or [])[:10]:
        if not p.get("pick"):
            continue
        st  = _lookup(None, p.get("name"))
        actual = st["strikeOuts"] if st else None
        line   = p.get("sugg_line") if p.get("sugg_line") is not None else p.get("line")
        if line is None:
            continue
        pick_dir = p.get("pick")
        pitcher_ks.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "Pitcher Ks", "side": pick_dir,
            "pick": f"{pick_dir} {line} Ks",
            "odds": p.get("over_odds") if pick_dir == "OVER" else p.get("under_odds"),
            "line": line,
            "actual": actual,
            "stat": "Ks",
            "result": _grade(pick_dir, line, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
        })

    # Pitcher Props (Hits Allowed / Outs / Earned Runs)
    PROP_STAT_MAP = {
        "pitcher_hits_allowed": ("hits_allowed", "Hits Allowed"),
        "pitcher_outs":         ("outs",         "Outs"),
        "pitcher_earned_runs":  ("earnedRuns",   "Earned Runs"),
        "pitcher_walks":        ("walks",        "Walks"),
    }
    pitcher_props = []
    for mkt, mdata in (picks.get("pitcher_props") or {}).items():
        stat_key, stat_label = PROP_STAT_MAP.get(mkt, (None, None))
        if not stat_key:
            continue   # skip unknown markets
        for p in (mdata.get("picks") or [])[:10]:
            if not p.get("pick") or p.get("line") is None:
                continue
            st     = _lookup(None, p.get("name"))
            actual = st[stat_key] if (st and stat_key) else None
            pick_dir = p.get("pick")
            line     = p.get("line")
            pitcher_props.append({
                "name": p.get("name", ""),
                "team": p.get("team", ""),
                "category": f"Pitcher {stat_label}", "side": pick_dir,
                "pick": f"{pick_dir} {line} {stat_label}",
                "odds": p.get("over_odds") if pick_dir == "OVER" else p.get("under_odds"),
                "line": line,
                "actual": actual,
                "stat": stat_label,
                "result": _grade(pick_dir, line, actual, (st or {}).get("final", False)),
                "game_status": (st or {}).get("status", "—"),
            })

    return {
        "date": date_str,
        "hitter_overs":  hitter_overs,
        "hitter_unders": hitter_unders,
        "runs":          runs,
        "tb_under":      tb_under,
        "rbi":           rbi_picks,
        "pitcher_ks":    pitcher_ks,
        "pitcher_props": pitcher_props,
        "any_game":      any_game,
        "all_final":     all_final,
    }


# ── Track Record: permanent W/L ledger across all graded days ────────────
_TRACK_LEDGER_PATH = os.path.join(_CACHE_DIR, "_track_record.json")
_TRACK_CAT_ORDER = [
    "Hitter Hits", "Runs", "TB Under", "RBI", "Pitcher Ks",
    "Pitcher Hits Allowed", "Pitcher Outs", "Pitcher Earned Runs",
]

def _load_ledger() -> dict:
    try:
        with open(_TRACK_LEDGER_PATH) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_ledger(led: dict):
    try:
        tmp = f"{_TRACK_LEDGER_PATH}.{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump(led, f)
        os.replace(tmp, _TRACK_LEDGER_PATH)
    except Exception as e:
        print(f"[track_ledger] save failed: {e}")

# Per-pick detail ledger (parallel to the W/L ledger above). Stores one row
# per graded top-10 pick — player, category, side, odds, line, result — so the
# Track Record panel can build a per-player earnings sheet. The W/L ledger is
# left untouched for backward compatibility; detail only accrues from deploy
# forward + any day still in the disk cache (backfilled on demand).
_TRACK_DETAIL_PATH = os.path.join(_CACHE_DIR, "_track_detail.json")

def _load_detail() -> dict:
    try:
        with open(_TRACK_DETAIL_PATH) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_detail(det: dict):
    try:
        tmp = f"{_TRACK_DETAIL_PATH}.{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump(det, f)
        os.replace(tmp, _TRACK_DETAIL_PATH)
    except Exception as e:
        print(f"[track_detail] save failed: {e}")

def _aggregate_graded(graded: dict) -> dict:
    """Collapse a graded day into {category: {side: [W, L]}} counting only decided picks."""
    agg: dict = {}
    for key in ("hitter_overs", "hitter_unders", "runs", "tb_under", "rbi", "pitcher_ks", "pitcher_props"):
        for r in graded.get(key, []):
            res = r.get("result")
            if res not in ("WIN", "LOSS"):
                continue
            cat  = r.get("category") or key
            side = r.get("side") or "OVER"
            rec  = agg.setdefault(cat, {}).setdefault(side, [0, 0])
            if res == "WIN":
                rec[0] += 1
            else:
                rec[1] += 1
    return agg

def _detail_graded(graded: dict) -> list:
    """Flatten a graded day into per-pick rows (decided picks only) carrying the
    fields an earnings sheet needs: player, team, category, side, pick, odds,
    line, result."""
    out = []
    for key in ("hitter_overs", "hitter_unders", "runs", "tb_under", "rbi", "pitcher_ks", "pitcher_props"):
        for r in graded.get(key, []):
            res = r.get("result")
            if res not in ("WIN", "LOSS"):
                continue
            out.append({
                "name": r.get("name", ""),
                "team": r.get("team", ""),
                "category": r.get("category") or key,
                "side": r.get("side") or "OVER",
                "pick": r.get("pick", ""),
                "odds": r.get("odds"),
                "line": r.get("line"),
                "result": res,
            })
    return out

import threading as _trk_threading
_LEDGER_LOCK = _trk_threading.Lock()

def _update_track_ledger() -> dict:
    """Grade every cached PAST date not yet locked and append it to the permanent ledger.
    A date is locked only once ALL its games are Final (or it is >=2 days old, so a
    permanently-postponed game can't block it forever) — this avoids freezing a
    partial-day record. The lock survives cache rollover/cleanup. Serialized by
    _LEDGER_LOCK so the run thread, scheduler thread and the endpoint never clobber
    each other's writes."""
    with _LEDGER_LOCK:
        led = _load_ledger()
        det = _load_detail()
        today = date.today().isoformat()
        try:
            _today_d = date.fromisoformat(today)
        except Exception:
            _today_d = None
        changed = False
        det_changed = False
        try:
            files = sorted(_glob.glob(os.path.join(_CACHE_DIR, "*.json")))
        except Exception:
            files = []
        for fp in files:
            bn = os.path.basename(fp).replace(".json", "")
            if bn.startswith("_") or len(bn) != 10 or bn[4] != "-":
                continue          # skip ledger file / non-date files
            if bn >= today:
                continue          # today/future — games not final yet
            need_led = bn not in led or not led.get(bn)
            need_det = bn not in det or not det.get(bn)
            if not need_led and not need_det:
                continue          # already locked — W/L and detail both present
            picks = _load_disk_cache(bn)
            if not picks:
                continue
            try:
                graded = _grade_date(bn, picks)
            except Exception as e:
                print(f"[track_ledger] grade failed for {bn}: {e}")
                continue
            if not graded.get("any_game"):
                continue          # no box scores yet — don't lock an empty day
            old_enough = False
            if _today_d is not None:
                try:
                    old_enough = (_today_d - date.fromisoformat(bn)).days >= 2
                except Exception:
                    old_enough = False
            if not graded.get("all_final") and not old_enough:
                continue          # slate not all Final yet — wait, don't lock partial
            if need_led:
                led[bn] = _aggregate_graded(graded)
                changed = True
            if need_det:
                det[bn] = _detail_graded(graded)
                det_changed = True
        if changed:
            _save_ledger(led)
        if det_changed:
            _save_detail(det)
        return led


# ── My Bets: personal bet log + ROI (admin-only, account-keyed) ─────────
# Stored server-side so it follows the user across devices and survives the
# monthly cache cleanup (the "_" prefix is skipped by the date-file sweeper,
# same as the Track Record ledger). Keyed by hub-account email so it is
# multi-user-ready; for now only the admin can read/write. Each bet self-
# settles from box scores by player name, so it grades even after that date's
# pick-cache file is gone.
_BET_LOG_PATH = os.path.join(_CACHE_DIR, "_bet_log.json")
_BET_LOCK = _trk_threading.Lock()
_BET_STAT_KEYS = ("hits", "runs", "total_bases", "rbi", "strikeOuts", "hits_allowed", "outs", "earnedRuns", "walks")
_BET_PITCH_STATS = ("strikeOuts", "hits_allowed", "outs", "earnedRuns", "walks")

def _load_bets() -> dict:
    try:
        with open(_BET_LOG_PATH) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_bets(data: dict):
    try:
        tmp = f"{_BET_LOG_PATH}.{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, _BET_LOG_PATH)
    except Exception as e:
        print(f"[bet_log] save failed: {e}")

def _bet_admin_ok(tok: str, admin: str) -> bool:
    return _is_admin_token(tok) or (
        bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__"))

def _bet_user_key(tok: str, admin: str) -> str:
    """Storage key for the current user — hub email when available, else a
    fixed admin bucket for the legacy ?admin=KEY link."""
    em = _token_email(tok) if tok else ""
    return em.lower() if em else "__admin__"

def _american_profit(odds, stake, result) -> float:
    """Net profit (stake already risked). WIN pays per American odds; LOSS
    forfeits the stake; PUSH refunds (0 net)."""
    try:
        stake = float(stake)
    except Exception:
        return 0.0
    if result == "WIN":
        try:
            o = float(odds)
        except Exception:
            return 0.0
        return stake * (o / 100.0) if o > 0 else stake * (100.0 / abs(o))
    if result == "LOSS":
        return -stake
    return 0.0  # PUSH / pending

def _am_to_dec(odds) -> float:
    try:
        o = float(odds)
    except Exception:
        return 0.0
    return round(1 + o / 100, 6) if o > 0 else round(1 + 100 / abs(o), 6)

def _settle_bet_cached(bet: dict, name_stats: dict) -> bool:
    """Grade a pending bet using pre-fetched name_stats (no extra API call)."""
    if bet.get("result") in ("WIN", "LOSS", "PUSH"):
        return False
    st = name_stats.get((bet.get("name") or "").lower())
    if not st or not st.get("final"):
        return False
    stat_key = bet.get("stat_key")
    actual = st.get(stat_key)
    if actual is None:
        if stat_key in _BET_PITCH_STATS:
            return False  # pitcher didn't pitch → leave pending
        actual = 0        # batter appeared but no hits/runs
    try:
        line = float(bet.get("line"))
    except Exception:
        return False
    side = bet.get("side", "OVER")
    if actual == line:
        res = "PUSH"
    elif side == "OVER":
        res = "WIN" if actual > line else "LOSS"
    else:
        res = "WIN" if actual < line else "LOSS"
    bet["result"] = res
    bet["actual"] = actual
    bet["profit"] = round(_american_profit(bet.get("odds"), bet.get("stake"), res), 2)
    bet["settled_at"] = date.today().isoformat()
    return True

def _settle_bet(bet: dict) -> bool:
    """Grade a still-pending bet against final box scores. Returns True if it
    changed. Only settles past dates; a player whose game isn't Final (or who
    didn't pitch, for pitching props) stays pending."""
    if bet.get("result") in ("WIN", "LOSS", "PUSH"):
        return False
    bdate = bet.get("date")
    if not bdate or bdate >= date.today().isoformat():
        return False
    try:
        _ps, ns, _any, _af = _mlb_box_lookup(bdate)
    except Exception as e:
        print(f"[bet_log] settle lookup failed {bdate}: {e}")
        return False
    return _settle_bet_cached(bet, ns)

def _settle_parlay_cached(parlay: dict, ns_cache: dict) -> bool:
    """Grade a parlay using pre-fetched ns_cache. WIN=all legs win, LOSS=any leg loses."""
    if parlay.get("result") in ("WIN", "LOSS", "PUSH"):
        return False
    legs = parlay.get("legs") or []
    for lg in legs:
        if lg.get("result") in ("WIN", "LOSS", "PUSH"):
            continue
        bdate = lg.get("date")
        if bdate and bdate in ns_cache:
            _settle_bet_cached(lg, ns_cache[bdate])
    results = [lg.get("result") for lg in legs]
    if not results or any(r not in ("WIN", "LOSS", "PUSH") for r in results):
        if any(r == "LOSS" for r in results):
            pass  # fall through to LOSS
        else:
            return False  # still pending legs
    if any(r == "LOSS" for r in results):
        parlay["result"] = "LOSS"
        parlay["profit"] = round(-float(parlay.get("stake") or 0), 2)
        parlay["settled_at"] = date.today().isoformat()
        return True
    if all(r == "WIN" for r in results):
        dec = _am_to_dec(parlay.get("odds"))
        stake = float(parlay.get("stake") or 0)
        parlay["result"] = "WIN"
        parlay["profit"] = round(stake * (dec - 1), 2) if dec else None
        parlay["settled_at"] = date.today().isoformat()
        return True
    if all(r in ("WIN", "PUSH") for r in results):
        parlay["result"] = "PUSH"
        parlay["profit"] = 0.0
        parlay["settled_at"] = date.today().isoformat()
        return True
    return False

def _settle_parlay(parlay: dict) -> bool:
    """One-shot settle attempt for a just-logged parlay."""
    legs = parlay.get("legs") or []
    today = date.today().isoformat()
    dates_needed = {lg["date"] for lg in legs if lg.get("date") and lg["date"] < today}
    if not dates_needed:
        return False
    ns_cache: dict = {}
    for d in dates_needed:
        try:
            _, ns, _, _ = _mlb_box_lookup(d)
            ns_cache[d] = ns
        except Exception:
            pass
    return _settle_parlay_cached(parlay, ns_cache)

def _settle_bets_batch(bets: list) -> bool:
    """Settle all pending bets (single + parlay) with ONE box-score API call per
    unique date. Returns True if any bet changed."""
    today = date.today().isoformat()
    dates_needed: set = set()
    for b in bets:
        if b.get("result") in ("WIN", "LOSS", "PUSH"):
            continue
        if b.get("bet_type") == "parlay":
            for lg in b.get("legs") or []:
                if lg.get("result") not in ("WIN", "LOSS", "PUSH") and lg.get("date") and lg["date"] < today:
                    dates_needed.add(lg["date"])
        elif b.get("date") and b["date"] < today:
            dates_needed.add(b["date"])
    if not dates_needed:
        return False
    ns_cache: dict = {}
    for d in sorted(dates_needed):
        try:
            _ps, ns, _any, _af = _mlb_box_lookup(d)
            ns_cache[d] = ns
        except Exception as e:
            print(f"[bet_log] batch settle lookup failed {d}: {e}")
    changed = False
    for b in bets:
        if b.get("bet_type") == "parlay":
            if _settle_parlay_cached(b, ns_cache):
                changed = True
        else:
            bdate = b.get("date")
            if bdate and bdate in ns_cache:
                if _settle_bet_cached(b, ns_cache[bdate]):
                    changed = True
    return changed

def _summarize_bets(bets: list) -> dict:
    cats: dict = {}
    tot_staked = tot_profit = 0.0
    w = l = pu = pend = 0
    for b in bets:
        res = b.get("result", "pending")
        try:
            stake = float(b.get("stake") or 0)
        except Exception:
            stake = 0.0
        c = cats.setdefault(b.get("category", "?"),
                            {"wins": 0, "losses": 0, "push": 0, "pending": 0,
                             "staked": 0.0, "profit": 0.0})
        if res == "WIN":
            w += 1; c["wins"] += 1
        elif res == "LOSS":
            l += 1; c["losses"] += 1
        elif res == "PUSH":
            pu += 1; c["push"] += 1
        else:
            pend += 1; c["pending"] += 1
        if res in ("WIN", "LOSS", "PUSH"):
            prof = float(b.get("profit") or 0)
            tot_staked += stake; c["staked"] += stake
            tot_profit += prof;  c["profit"] += prof
    roi = (tot_profit / tot_staked * 100.0) if tot_staked > 0 else None
    by_cat = []
    ordered = _TRACK_CAT_ORDER + [k for k in cats if k not in _TRACK_CAT_ORDER]
    for cat in ordered:
        c = cats.get(cat)
        if not c:
            continue
        st = c["staked"]; pr = c["profit"]
        by_cat.append({
            "category": cat, "wins": c["wins"], "losses": c["losses"],
            "push": c["push"], "pending": c["pending"],
            "staked": round(st, 2), "profit": round(pr, 2),
            "roi": round(pr / st * 100, 1) if st > 0 else None,
        })
    return {
        "wins": w, "losses": l, "push": pu, "pending": pend,
        "staked": round(tot_staked, 2), "profit": round(tot_profit, 2),
        "returned": round(tot_staked + tot_profit, 2),
        "roi": round(roi, 1) if roi is not None else None,
        "by_category": by_cat,
    }

@app.get("/api/bets")
async def get_bets(request: Request, token: str = "", admin: str = "",
                   settle: bool = True):
    """Load logged bets. settle=false skips box-score lookups (fast read).
    settle=true (default) grades any unresolved past-game bets on the spot."""
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _bet_admin_ok(tok, admin):
        raise HTTPException(status_code=403, detail="Admin only")
    with _BET_LOCK:
        data = _load_bets()
        key = _bet_user_key(tok, admin)
        bets = data.get(key, [])
        changed = False
        if settle:
            changed = _settle_bets_batch(bets)
        if changed:
            data[key] = bets
            _save_bets(data)
        snapshot = list(bets)
    snapshot.sort(key=lambda b: (b.get("date", ""), b.get("placed_at", "")), reverse=True)
    return {"bets": snapshot, "summary": _summarize_bets(snapshot)}

@app.post("/api/bets")
async def add_bet(request: Request, token: str = "", admin: str = ""):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _bet_admin_ok(tok, admin):
        raise HTTPException(status_code=403, detail="Admin only")
    body = await request.json()
    import uuid as _uuid
    # ── PARLAY PATH ──────────────────────────────────────────────────────────
    if body.get("bet_type") == "parlay":
        legs_raw = body.get("legs") or []
        if len(legs_raw) < 2:
            raise HTTPException(status_code=400, detail="Parlay needs at least 2 legs")
        try:
            stake = round(float(body.get("stake")), 2)
            odds  = int(round(float(body.get("odds"))))
        except Exception:
            raise HTTPException(status_code=400, detail="stake and odds must be numbers")
        if stake <= 0:
            raise HTTPException(status_code=400, detail="Bet size must be > 0")
        legs = []
        for lg in legs_raw:
            try: lline = float(lg.get("line"))
            except Exception: lline = None
            legs.append({
                "name":       (lg.get("name") or "").strip(),
                "team":       (lg.get("team") or "").strip(),
                "opp":        (lg.get("opp") or "").strip(),
                "category":   (lg.get("category") or "").strip(),
                "side":       (lg.get("side") or "OVER").strip().upper(),
                "stat_key":   (lg.get("stat_key") or "").strip(),
                "stat_label": (lg.get("stat_label") or "").strip(),
                "line":       lline,
                "odds":       lg.get("odds"),
                "date":       (lg.get("date") or date.today().isoformat()).strip(),
                "result":     "pending",
                "actual":     None,
            })
        parlay = {
            "id":         _uuid.uuid4().hex[:12],
            "bet_type":   "parlay",
            "date":       (body.get("date") or date.today().isoformat()).strip(),
            "category":   "Parlay",
            "legs":       legs,
            "odds":       odds,
            "stake":      stake,
            "placed_at":  (body.get("placed_at") or date.today().isoformat()),
            "result":     "pending",
            "profit":     None,
            "settled_at": None,
        }
        _settle_parlay(parlay)
        with _BET_LOCK:
            data = _load_bets()
            key  = _bet_user_key(tok, admin)
            data.setdefault(key, []).append(parlay)
            _save_bets(data)
        return {"ok": True, "bet": parlay}
    # ── SINGLE BET PATH ───────────────────────────────────────────────────────
    try:
        stake = round(float(body.get("stake")), 2)
        odds = int(round(float(body.get("odds"))))
        line = float(body.get("line"))
    except Exception:
        raise HTTPException(status_code=400, detail="stake, odds and line must be numbers")
    if stake <= 0:
        raise HTTPException(status_code=400, detail="Bet size must be greater than 0")
    name = (body.get("name") or "").strip()
    stat_key = (body.get("stat_key") or "").strip()
    side = (body.get("side") or "OVER").strip().upper()
    if not name or stat_key not in _BET_STAT_KEYS or side not in ("OVER", "UNDER"):
        raise HTTPException(status_code=400, detail="Invalid bet")
    bdate = (body.get("date") or date.today().isoformat()).strip()
    bet = {
        "id": _uuid.uuid4().hex[:12],
        "date": bdate,
        "name": name,
        "team": (body.get("team") or "").strip(),
        "opp": (body.get("opp") or "").strip(),
        "category": (body.get("category") or "?").strip(),
        "side": side,
        "stat_key": stat_key,
        "stat_label": (body.get("stat_label") or "").strip(),
        "line": line,
        "odds": odds,
        "stake": stake,
        "placed_at": (body.get("placed_at") or date.today().isoformat()),
        "result": "pending",
        "actual": None,
        "profit": None,
        "settled_at": None,
    }
    _settle_bet(bet)
    with _BET_LOCK:
        data = _load_bets()
        key = _bet_user_key(tok, admin)
        data.setdefault(key, []).append(bet)
        _save_bets(data)
    return {"ok": True, "bet": bet}

@app.delete("/api/bets/{bet_id}")
async def delete_bet(bet_id: str, request: Request, token: str = "", admin: str = ""):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _bet_admin_ok(tok, admin):
        raise HTTPException(status_code=403, detail="Admin only")
    with _BET_LOCK:
        data = _load_bets()
        key = _bet_user_key(tok, admin)
        bets = data.get(key, [])
        new = [b for b in bets if b.get("id") != bet_id]
        if len(new) != len(bets):
            data[key] = new
            _save_bets(data)
    return {"ok": True}


@app.get("/api/grade/{date_str}")
async def grade_picks(date_str: str, request: Request, token: str = "", admin: str = ""):
    """Fetch actual MLB box scores and grade all picks for the given date."""
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    is_ok = _verify_hub_token(tok) or _is_admin_token(tok) or (
        bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__"))
    if not is_ok:
        raise HTTPException(status_code=401, detail="Subscription required")
    if date_str not in _cache:
        disk = _load_disk_cache(date_str)
        if disk is not None:
            _cache[date_str] = disk
    picks = _cache.get(date_str)
    if not picks:
        raise HTTPException(status_code=404, detail="No picks for this date")
    try:
        return _grade_date(date_str, picks)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLB API error: {e}")


@app.get("/api/track-record")
async def track_record(request: Request, token: str = "", admin: str = ""):
    """Admin-only. All-time + daily W/L record per category (Over vs Under) from the
    permanent ledger. Grades any past cached day not yet locked, then aggregates."""
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    is_admin = _is_admin_token(tok) or (
        bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__")
    )
    if not is_admin:
        raise HTTPException(status_code=403, detail="Admin only")

    led = _update_track_ledger()
    det = _load_detail()

    alltime: dict = {}   # {category: {side: [W, L]}}
    daily = []
    for ds in sorted(led.keys()):
        day_w = day_l = 0
        for cat, sides in (led[ds] or {}).items():
            for side, wl in sides.items():
                rec = alltime.setdefault(cat, {}).setdefault(side, [0, 0])
                rec[0] += wl[0]; rec[1] += wl[1]
                day_w += wl[0]; day_l += wl[1]
        daily.append({"date": ds, "wins": day_w, "losses": day_l, "cats": led[ds]})

    cats = [c for c in _TRACK_CAT_ORDER if c in alltime] + \
           [c for c in alltime if c not in _TRACK_CAT_ORDER]
    rows = []
    for cat in cats:
        for side in ("OVER", "UNDER"):
            if side in alltime.get(cat, {}):
                w, l = alltime[cat][side]
                rows.append({"category": cat, "side": side, "wins": w, "losses": l})

    detail = []
    for ds in sorted(det.keys()):
        for r in (det[ds] or []):
            row = dict(r)
            row["date"] = ds
            detail.append(row)

    return {"alltime": rows, "daily": daily, "days": len(led), "detail": detail}


# ── Any-player lookup ────────────────────────────────────────────────
# Lets the search bar grade ANY hitter in today's games (not just the
# analyzed pool). Quick verdict from career BA vs today's pitcher (S1)
# + recent H/A hit rate vs opponent (S4). On-demand: no run-time cost.
_LOOKUP_PLAYERS: dict = {}   # season -> {name_lower: {"id","team_id","full"}}
_LOOKUP_TEAMS: dict = {}     # season -> {team_id: name}


def _load_lookup_index(season: str):
    import requests as _rq
    MLB = "https://statsapi.mlb.com/api/v1"
    if season not in _LOOKUP_TEAMS:
        try:
            tr = _rq.get(f"{MLB}/teams", params={"sportId": 1, "season": season},
                         timeout=15).json()
            _LOOKUP_TEAMS[season] = {t["id"]: t.get("name", "") for t in tr.get("teams", [])}
        except Exception:
            _LOOKUP_TEAMS[season] = {}
    if season not in _LOOKUP_PLAYERS:
        idx = {}
        try:
            pr = _rq.get(f"{MLB}/sports/1/players", params={"season": season},
                         timeout=20).json()
            for p in pr.get("people", []):
                nm  = (p.get("fullName") or "").strip()
                tid = (p.get("currentTeam") or {}).get("id")
                if nm and tid:
                    idx[nm.lower()] = {"id": p["id"], "team_id": tid, "full": nm}
        except Exception:
            pass
        _LOOKUP_PLAYERS[season] = idx
    return _LOOKUP_PLAYERS[season], _LOOKUP_TEAMS[season]


@app.get("/api/lookup")
def api_lookup(name: str, date_str: str):
    # Sync def → Starlette runs this in a threadpool, so the blocking MLB
    # API calls below never stall the event loop / SSE progress stream.
    import requests as _rq
    MLB = "https://statsapi.mlb.com/api/v1"
    q = (name or "").strip().lower()
    if len(q) < 3:
        return {"found": False, "msg": "Type at least 3 letters of a name."}

    season = (date_str or "")[:4] or "2026"
    players, teams = _load_lookup_index(season)

    # Resolve: exact full name → unambiguous substring → unambiguous last name
    match = players.get(q)
    if not match:
        cands = [v for k, v in players.items() if q in k]
        if len(cands) == 1:
            match = cands[0]
        elif len(cands) > 1:
            ln = [v for k, v in players.items() if k.split() and k.split()[-1] == q]
            if len(ln) == 1:
                match = ln[0]
            else:
                names = sorted({v["full"] for v in cands})[:6]
                return {"found": False,
                        "msg": "Multiple players match — try a full name: " + ", ".join(names)}
    if not match:
        return {"found": False, "msg": f'No MLB player found for "{name}".'}

    pid = match["id"]; team_id = match["team_id"]; full = match["full"]

    from fic_cache import _get_all_games
    side = opp_id = opp_pid = opp_pname = None
    for g in _get_all_games(date_str):
        if g["home_id"] == team_id:
            side, opp_id, opp_pid, opp_pname = "HOME", g["away_id"], g["away_pitcher_id"], g.get("away_pitcher_short"); break
        if g["away_id"] == team_id:
            side, opp_id, opp_pid, opp_pname = "AWAY", g["home_id"], g["home_pitcher_id"], g.get("home_pitcher_short"); break
    if not side:
        return {"found": True, "verdict": "NOT_PLAYING", "full_name": full,
                "team": teams.get(team_id, ""),
                "msg": f"{full} isn't in a game on {date_str}."}

    opp_name = teams.get(opp_id, "the opponent")

    # S1 — career BA vs today's opposing pitcher
    s1_ba = s1_ab = None
    if opp_pid:
        try:
            r = _rq.get(f"{MLB}/people/{pid}/stats",
                params={"stats": "vsPlayerTotal", "group": "hitting",
                        "opposingPlayerId": opp_pid}, timeout=8).json()
            for sg in r.get("stats", []):
                if "vsPlayer" in sg.get("type", {}).get("displayName", ""):
                    for sp in sg.get("splits", []):
                        st = sp.get("stat", {})
                        ab = int(st.get("atBats", 0) or 0)
                        if ab:
                            s1_ab = ab
                            av = str(st.get("avg", ""))
                            if av.startswith("."):
                                s1_ba = float(f"0{av}")
                            elif av not in ("", "-", ".---"):
                                try: s1_ba = float(av)
                                except ValueError: s1_ba = None
        except Exception:
            pass

    # S4 — recent H/A hit rate vs opponent
    s4 = {"games": 0, "score": 0, "display": "N/A"}
    try:
        from pipeline import fetch_step4_consistency
        s4 = fetch_step4_consistency(pid, side, opp_name) or s4
    except Exception:
        pass

    games_n = int(s4.get("games", 0) or 0)
    rate    = int(s4.get("score", 0) or 0)
    pname   = opp_pname or "today's starter"
    parts, signal = [], 0
    if s1_ba is not None and s1_ab:
        parts.append(f"career {s1_ba:.3f} ({s1_ab} AB) vs {pname}")
        if   s1_ba >= 0.300: signal += 2
        elif s1_ba >= 0.250: signal += 1
        elif s1_ba <  0.200: signal -= 1
    if games_n > 0:
        side_word = "home" if side == "HOME" else "away"
        parts.append(f"{s4.get('display')} {side_word} games with a hit vs {opp_name} ({rate}%)")
        if   rate >= 70: signal += 2
        elif rate >= 60: signal += 1
        elif rate <  50: signal -= 1

    # Data-sufficiency gate: a verdict needs a real sample behind it.
    # "Enough" = 10+ career AB vs this pitcher, OR 3+ recent H/A games vs opp.
    # Thin samples (e.g. 3 AB / 1 game) get a no-call instead of false confidence.
    enough = (s1_ab or 0) >= 10 or games_n >= 3

    if not parts or not enough:
        verdict  = "INSUFFICIENT"
        headline = "Not enough data to recommend this player today"
        if parts:
            blurb = " · ".join(parts) + " — too small a sample to call"
        else:
            blurb = "No matchup history available yet — can't call it today."
    else:
        blurb = " · ".join(parts)
        # UNDER LEAN: a genuinely bad career mark vs THIS pitcher (real sample,
        # 10+ AB, BA <= .150) can't be redeemed by a hot small-sample H/A streak —
        # those streak games are single-hit games, not multi-hit. So the better
        # angle is Under 1.5 hits / total bases, not "stack him for a hit".
        if s1_ba is not None and (s1_ab or 0) >= 10 and s1_ba <= 0.150:
            verdict, headline = "UNDER", "Better angle: lean Under 1.5 hits / TB"
            if games_n > 0:
                blurb = (" · ".join(parts)
                         + f" — only {s1_ba:.3f} lifetime vs {pname}; the "
                         + f"{s4.get('display')} streak is single-hit games, "
                         + "so lean Under not over")
            else:
                blurb = (" · ".join(parts)
                         + f" — only {s1_ba:.3f} lifetime vs {pname}; lean Under")
        elif signal >= 2:
            verdict, headline = "GOOD", "Good choice for a hit today"
        elif signal >= 1:
            verdict, headline = "DECENT", "Decent shot at a hit today"
        else:
            verdict, headline = "WEAK", "Not a strong choice for a hit today"

    return {
        "found": True, "verdict": verdict, "headline": headline,
        "full_name": full, "team": teams.get(team_id, ""),
        "side": side, "opp": opp_name, "pitcher": pname,
        "s1": (round(s1_ba, 3) if s1_ba is not None else None), "s1_ab": s1_ab,
        "s4_display": s4.get("display"), "s4_pct": rate, "s4_games": games_n,
        "blurb": blurb,
    }

@app.get("/api/whoami")
async def whoami(request: Request, token: str = ""):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    return {"is_admin": _is_admin_token(tok)}

_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MLB MoneyBall &mdash; Money Picks Arena</title>
  <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700;900&family=Source+Sans+Pro:wght@300;400;600;700&display=swap" rel="stylesheet">
  <style>
    /* responsive: phones & tablets (mobile fit) */
    html,body{max-width:100%;overflow-x:hidden}
    img{max-width:100%;height:auto}
    @media (max-width:1200px){table{display:block;width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;white-space:nowrap}}
    @media (max-width:560px){table{font-size:12px}table th,table td{padding:6px 8px}}
    :root {
      --navy:#0f0f0f;--navy2:#161616;--navy3:#1a1a1a;--green:#f59e0b;--red:#ff1744;
      --yellow:#f59e0b;--orange:#f59e0b;--gold:#f59e0b;--silver:#c0c0c0;--bronze:#cd7f32;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: #0f0f0f; color: #fff; font-family: 'Source Sans Pro', sans-serif; min-height: 100vh; }
    .app-nav { position: fixed; top: 0; width: 100%; background: rgba(10,10,10,.95); backdrop-filter: blur(12px); border-bottom: 1px solid #1c1c1c; z-index: 100; padding: 0 32px; height: 80px; display: flex; align-items: center; justify-content: space-between; }
    .app-nav-logo { font-family: 'Playfair Display', serif; font-size: 36px; font-weight: 900; color: #f59e0b; letter-spacing:.02em; line-height:1; }
    .app-nav-logo span { color: #fff; }
    .card { background: #161616; border: 1px solid #262626; border-radius: 16px; transition: border-color .2s; }
    .card:hover { border-color: rgba(245,158,11,.2); }
    .p-6 { padding: 24px; }
    .btn-primary { background: #f59e0b; border: none; cursor: pointer; border-radius: 8px; padding: 12px 28px; font-size: 1rem; font-weight: 700; color: #000; transition: all .2s; font-family: 'Source Sans Pro', sans-serif; }
    .btn-primary:hover:not(:disabled) { background: #fbbf24; transform: translateY(-1px); box-shadow: 0 4px 20px rgba(245,158,11,.4); }
    .btn-primary:disabled { opacity: .5; cursor: not-allowed; transform: none; }
    .flex { display: flex; } .flex-col { flex-direction: column; } .flex-1 { flex: 1; }
    .items-center { align-items: center; } .justify-between { justify-content: space-between; }
    .gap-4 { gap: 16px; } .hidden { display: none !important; } .w-full { width: 100%; }
    .space-y-6 > * + * { margin-top: 24px; } .min-h-screen { min-height: 100vh; }
    .px-4 { padding-left: 16px; padding-right: 16px; } .py-6 { padding-top: 24px; padding-bottom: 24px; }
    .max-w-7xl { max-width: 1280px; } .mx-auto { margin-left: auto; margin-right: auto; }
    .mt-2 { margin-top: 8px; } .mt-4 { margin-top: 16px; }
    .text-xs { font-size: 12px; } .text-sm { font-size: 14px; }
    .text-slate-400 { color: #94a3b8; } .text-slate-500 { color: #64748b; }
    .sm\\:flex-row { flex-direction: row; } .sm\\:items-end { align-items: flex-end; }
    #log-box { background: #0a0a0a; border: 1px solid #262626; border-radius: 8px; height: 260px; overflow-y: auto; padding: 12px 16px; font-family: 'Courier New', monospace; font-size: .82rem; line-height: 1.6; }
    .log-section { color: var(--yellow); font-weight: 700; margin-top: 6px; }
    .log-ok { color: var(--green); } .log-dq { color: var(--red); } .log-skip { color: #64748b; }
    .log-info { color: #93c5fd; } .log-cached { color: #a78bfa; } .log-default { color: #cbd5e1; }
    .log-under { color: #ff8a65; }
    #prog-bar-inner { height: 6px; border-radius: 3px; background: linear-gradient(90deg, #f59e0b, #fbbf24); transition: width .4s ease; }
    .results-table { width: 100%; border-collapse: collapse; }
    .results-table th { background: #0f0f0f; color: #f59e0b; font-size: .75rem; text-transform: uppercase; letter-spacing: 1px; padding: 10px 14px; text-align: left; white-space: nowrap; }
    .results-table td { padding: 12px 14px; border-bottom: 1px solid rgba(255,255,255,.05); vertical-align: middle; }
    .results-table tr:hover td { background: rgba(255,255,255,.03); }
    .results-table tr:last-child td { border-bottom: none; }
    .grade-table { width:100%; border-collapse:collapse; font-size:.82rem; }
    .grade-table th { color:#94a3b8; font-weight:600; padding:6px 12px; border-bottom:1px solid #1f2937; text-align:left; white-space:nowrap; }
    .grade-table td { padding:7px 12px; border-bottom:1px solid #111827; vertical-align:middle; }
    .grade-table tr:hover td { background:rgba(255,255,255,.02); }
    .bet-input { width:68px; background:#1e1e1e; border:1px solid #374151; border-radius:6px; color:#fff; padding:4px 6px; font-size:.85rem; text-align:center; }
    .bet-input:focus { outline:none; border-color:#f59e0b; }
    /* Admin gate: hidden by default, shown only when body has is-admin */
    .admin-only { display: none !important; }
    body.is-admin .admin-only { display: revert !important; }
    body.is-admin .results-table th.admin-only,
    body.is-admin .results-table td.admin-only { display: table-cell !important; }
    .rank-badge { width: 32px; height: 32px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-weight: 800; font-size: .85rem; }
    .rank-1 { background: var(--gold); color: #000; } .rank-2 { background: var(--silver); color: #000; }
    .rank-3 { background: var(--bronze); color: #fff; } .rank-n { background: var(--navy3); color: #94a3b8; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: .72rem; font-weight: 700; letter-spacing:.3px; }
    .badge-home { background: rgba(21,101,192,.35); color: #90caf9; }
    .badge-away { background: rgba(103,58,183,.35); color: #ce93d8; }
    .badge-pos  { background: rgba(255,255,255,.08); color: #94a3b8; }
    .badge-day  { background: rgba(255,214,0,.2); color: var(--yellow); }
    .badge-night{ background: rgba(100,100,255,.2); color: #a5b4fc; }
    .badge-dq   { background: rgba(255,23,68,.15); color: #ff6b6b; font-size:.7rem; padding: 2px 6px; }
    .badge-in   { background: rgba(0,200,83,.2); color: #00e676; }
    .badge-tbd  { background: rgba(255,214,0,.2); color: #f59e0b; }
    .badge-out  { background: rgba(255,23,68,.2); color: #ff6b6b; }
    .stat-cell { font-family: 'Courier New', monospace; font-size: .88rem; font-weight: 600; }
    .stat-good { color: var(--green); } .stat-warn { color: var(--yellow); } .stat-na { color: #475569; }
    .stat-under { color: #ff8a65; font-weight: 700; } .score-big { font-size: 1.1rem; font-weight: 800; color: #f59e0b; }
    .section-hdr { display: flex; align-items: center; gap: 8px; font-size: .9rem; font-weight: 700; color: #f59e0b; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 12px; }
    .section-hdr::after { content:''; flex:1; height:1px; background:#262626; }
    details > summary { cursor: pointer; list-style: none; user-select: none; }
    details > summary::-webkit-details-marker { display: none; }
    .dq-row { font-size: .82rem; padding: 7px 14px; border-bottom: 1px solid rgba(255,255,255,.04); display: flex; gap: 16px; align-items: center; }
    .dq-row:last-child { border-bottom: none; }
    @keyframes spin { to { transform: rotate(360deg); } }
    .spinner { width: 18px; height: 18px; border: 3px solid rgba(255,255,255,.15); border-top-color: #3b82f6; border-radius: 50%; animation: spin .7s linear infinite; display: inline-block; }
    .login-input { background: var(--navy3); border: 1px solid rgba(255,255,255,.15); color: #e2e8f0; border-radius: 8px; padding: 11px 16px; width: 100%; font-size: 1rem; outline: none; transition: border-color .2s; }
    .login-input:focus { border-color: #3b82f6; }
    ::-webkit-scrollbar { width: 6px; } ::-webkit-scrollbar-track { background: transparent; } ::-webkit-scrollbar-thumb { background: rgba(255,255,255,.12); border-radius: 3px; }
    .run-box{background:#161616;border:1px solid #262626;border-radius:16px;padding:28px;text-align:center;margin-bottom:20px;transition:border-color .2s}
    .run-box:hover{border-color:rgba(245,158,11,.3)}
    .date-row{display:flex;align-items:center;justify-content:center;gap:12px;margin-bottom:18px}
    .date-row label{font-size:11px;font-weight:700;color:#6b7280;letter-spacing:1.5px;text-transform:uppercase}
    .date-row input{background:#0f0f0f;color:#f59e0b;border:1px solid #262626;border-radius:8px;padding:9px 14px;font-size:.9rem;font-weight:600;outline:none;cursor:pointer;font-family:'Source Sans Pro',sans-serif}
    .date-row input:focus{border-color:#f59e0b}
    .btn-run{background:#f59e0b;color:#000;border:none;border-radius:8px;padding:14px 48px;font-size:1rem;font-weight:900;cursor:pointer;letter-spacing:.5px;transition:all .2s;font-family:'Source Sans Pro',sans-serif}
    .btn-run:hover{background:#fbbf24;transform:translateY(-1px);box-shadow:0 4px 20px rgba(245,158,11,.4)}
    .btn-run:disabled{background:#333;color:#666;cursor:not-allowed;transform:none}
    input[type=date]::-webkit-calendar-picker-indicator{filter:invert(1);opacity:.7;cursor:pointer}
    .chips{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:12px;margin-bottom:24px}
    .chip{background:#111;border-top:3px solid #FDB827;border-radius:8px;padding:16px 10px;text-align:center}
    .chip .val{font-size:1.9rem;font-weight:900;color:#FDB827}
    .chip .lbl{font-size:.65rem;color:#555;text-transform:uppercase;letter-spacing:1px;margin-top:4px}
    .chip.chip-link{cursor:pointer;transition:transform .12s ease,border-top-color .12s ease,box-shadow .12s ease}
    .chip.chip-link:hover{transform:translateY(-2px);border-top-color:#fff;box-shadow:0 8px 22px rgba(253,184,39,.18)}
    .chip.chip-link:active{transform:translateY(0)}
    .flash{animation:cardflash 1.1s ease-out}
    @keyframes cardflash{0%{box-shadow:0 0 0 3px rgba(253,184,39,.9)}100%{box-shadow:0 0 0 3px rgba(253,184,39,0)}}
    .mlb-picks-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:16px}
    .parlay-cat-row{display:flex;align-items:center;gap:8px;padding:4px 2px;cursor:pointer;font-size:.78rem;color:#ddd;user-select:none}
    .parlay-cat-row input{cursor:pointer;width:15px;height:15px;accent-color:#f59e0b}
    .env-chip{display:inline-block;margin:6px 0 2px;padding:3px 7px;border:1px solid #333;border-radius:6px;font-size:.64rem;font-weight:700;letter-spacing:.01em;line-height:1.35;background:#0d0d0d}
    .more-btn{width:100%;margin-top:14px;padding:11px 16px;background:#0f172a;border:1px solid #334155;border-radius:12px;font-size:.82rem;font-weight:700;cursor:pointer;letter-spacing:.06em;text-align:center;transition:background .15s,border-color .15s}
    .more-btn:hover{background:#1e293b;border-color:#475569}
    .mlb-pick-card{border-radius:14px;overflow:hidden;background:linear-gradient(180deg,#161616 0%,#0f0f0f 100%);border:1px solid #262626;display:flex;flex-direction:column}
    .mlb-pick-card:hover{border-color:rgba(245,158,11,.35)}
    .mlb-card-header{padding:10px 14px;display:flex;align-items:center;justify-content:space-between;border-bottom:2px solid #f59e0b}
    .mlb-card-photo{position:relative;height:140px;overflow:hidden;background:radial-gradient(ellipse at center top,rgba(245,158,11,.15),transparent 70%),linear-gradient(180deg,#1a2a1a 0%,#0a1a0a 100%)}
    .mlb-card-name{background:#f59e0b;color:#000;text-align:center;padding:8px 10px;font-weight:900;font-size:1rem;letter-spacing:.01em}
    .mlb-card-body{padding:10px 12px 12px;flex:1;display:flex;flex-direction:column;gap:6px}
  </style>
</head>
<body class="min-h-screen">

<div id="login-screen" class="min-h-screen flex items-center justify-center px-4" style="display:none">
  <div class="card p-10 w-full max-w-md shadow-2xl">
    <div class="text-center mb-8">
      <div style="font-size:7rem;line-height:1;margin-bottom:12px">⚾</div>
      <h1 style="font-size:3rem;font-weight:900;letter-spacing:-1px">MoneyBall</h1>
      <p class="text-slate-400 text-sm mt-1">Your daily MLB edge ⚫🟡</p>
    </div>
    <div id="login-error" class="hidden" style="background:rgba(185,28,28,.4);border:1px solid #b91c1c;color:#fca5a5;border-radius:8px;padding:12px 16px;font-size:.875rem;margin-bottom:16px"></div>
    <form id="login-form" onsubmit="doLogin(event)" class="space-y-6">
      <div>
        <label class="block text-xs font-semibold text-slate-400 uppercase" style="letter-spacing:.1em;margin-bottom:8px">Username</label>
        <input id="inp-user" type="text" autocomplete="username" placeholder="your username" class="login-input" required />
      </div>
      <div>
        <label class="block text-xs font-semibold text-slate-400 uppercase" style="letter-spacing:.1em;margin-bottom:8px">Password</label>
        <input id="inp-pass" type="password" autocomplete="current-password" placeholder="••••••••" class="login-input" required />
      </div>
      <button type="submit" class="btn-primary w-full mt-2" id="login-btn">Sign In</button>
    </form>
  </div>
</div>

<div id="dashboard" class="hidden min-h-screen flex flex-col" style="padding-top:80px">
  <nav class="app-nav">
    <span class="app-nav-logo">Money<span> Picks</span> Arena</span>
    <div style="display:flex;gap:10px;align-items:center">
      <button id="results-btn" onclick="checkResults()" title="Grade every pick for the selected date against final box scores" style="background:#1d4ed8;color:#fff;border:none;border-radius:10px;padding:9px 18px;min-width:140px;text-align:center;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">📊 Results</button>
      <button class="admin-only" id="track-btn" onclick="openTrackRecord()" title="All-time + daily Win/Loss record across every graded day, by category" style="background:#7c3aed;color:#fff;border:none;border-radius:10px;padding:9px 18px;min-width:140px;text-align:center;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">🏆 Track Record</button>
      <button class="admin-only" id="mybets-btn" onclick="openMyBets()" title="Your personal logged bets — click Get Results to grade against box scores" style="background:#4338ca;color:#fff;border:none;border-radius:10px;padding:9px 18px;min-width:140px;text-align:center;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">💰 My Bets</button>
      <button class="admin-only" onclick="_manualParlayForm()" title="Manually log a parlay — add legs one by one then save" style="background:#7e22ce;color:#fff;border:none;border-radius:10px;padding:9px 18px;min-width:140px;text-align:center;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">📋 Log Parlay</button>
    </div>
  </nav>
  <main class="flex-1 px-4 py-6 max-w-7xl mx-auto w-full space-y-6">
    <div style="text-align:center;margin-bottom:32px">
      <h1 style="font-family:'Playfair Display',serif;font-size:2.6rem;font-weight:900;color:#fff;margin-bottom:6px">MLB <span style="color:#f59e0b">MoneyBall</span></h1>
      <p style="font-size:.85rem;color:#6b7280;letter-spacing:.15em;text-transform:uppercase">MLB Daily Picks</p>
    </div>
    <div id="betting-context-card" class="card" style="max-width:960px;margin:0 auto 18px;padding:0;overflow:hidden">
      <div onclick="_bcToggle()" style="display:flex;justify-content:space-between;align-items:center;padding:12px 16px;cursor:pointer;user-select:none;background:linear-gradient(135deg,rgba(245,158,11,.06),transparent)">
        <div>
          <span style="font-weight:800;color:#fbbf24;font-size:.85rem">&#9918; MLB Prop Strategy &#8212; Day &amp; Trend Master Sheet</span>
          <span style="font-size:.68rem;color:#64748b;margin-left:8px">Batters &amp; Pitchers</span>
        </div>
        <span id="bc-arrow" style="color:#64748b;font-size:.74rem">&#9658; expand</span>
      </div>
      <div id="betting-context-body" class="hidden" style="padding:0 14px 16px">
        <div style="display:flex;gap:8px;margin:10px 0 12px">
          <button id="bc-tab-bat" onclick="_bcTab('bat')" style="padding:5px 16px;border-radius:8px;border:1px solid #4ade80;background:rgba(74,222,128,.1);color:#4ade80;font-weight:800;font-size:.74rem;cursor:pointer;letter-spacing:.04em">&#9918; BATTERS</button>
          <button id="bc-tab-pit" onclick="_bcTab('pit')" style="padding:5px 16px;border-radius:8px;border:1px solid #334155;background:transparent;color:#64748b;font-weight:800;font-size:.74rem;cursor:pointer;letter-spacing:.04em">&#128142; PITCHERS</button>
        </div>
        <div id="bc-bat" style="overflow-x:auto">
          <div style="font-size:.65rem;color:#64748b;margin-bottom:8px"><b style="color:#4ade80">O</b> = Over signal &nbsp;&#183;&nbsp; <b style="color:#ff8a65">U</b> = Under signal</div>
          <table style="width:100%;border-collapse:collapse;font-size:.71rem;min-width:680px">
            <thead>
              <tr style="border-bottom:2px solid #1e293b">
                <th style="text-align:left;padding:7px 8px;color:#94a3b8;font-size:.62rem;letter-spacing:.06em;white-space:nowrap;font-weight:700;width:9%">DAY &amp; TREND</th>
                <th style="text-align:left;padding:7px 8px;color:#4ade80;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#128994; TOP PICKS + MONEY BALL<br><span style="color:#64748b;font-weight:400">Hits O 0.5</span></th>
                <th style="text-align:left;padding:7px 8px;color:#ff8a65;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#128308; UNDER PICKS<br><span style="color:#64748b;font-weight:400">Hits U 1.5</span></th>
                <th style="text-align:left;padding:7px 8px;color:#a78bfa;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#128995; TB UNDER<br><span style="color:#64748b;font-weight:400">TB U 1.5</span></th>
                <th style="text-align:left;padding:7px 8px;color:#60a5fa;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#128309; RUNS PICKS<br><span style="color:#64748b;font-weight:400">Runs O/U 0.5</span></th>
                <th style="text-align:left;padding:7px 8px;color:#fbbf24;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:19%">&#128993; RBI PICKS<br><span style="color:#64748b;font-weight:400">RBI O/U 0.5</span></th>
              </tr>
            </thead>
            <tbody>
              <tr style="border-bottom:1px solid #1e1e1e">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Monday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Series Openers</span></td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> High-contact leadoff hitters vs. rusty #4/#5 back-rotation starters.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Cold hitters facing an elite Ace opener with a fresh pitch mix.</td>
                <td style="padding:8px 8px;color:#c4b5fd;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Extreme pull-hitters facing a starter with an elite heavy sinker.</td>
                <td style="padding:8px 8px;color:#93c5fd;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Target under on teams traveling long distances across time zones.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Offenses start slow adjusting to a new series.</td>
              </tr>
              <tr style="border-bottom:1px solid #1e1e1e;background:rgba(255,255,255,.015)">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Tuesday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Mid-Series G2</span></td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Elite hitters who tracked this starter well in past head-to-heads.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Low-walk hitters facing a high-spin pitcher with elite deception.</td>
                <td style="padding:8px 8px;color:#c4b5fd;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Flyball hitters facing a high-strikeout starter in cold weather.</td>
                <td style="padding:8px 8px;color:#93c5fd;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Target high-scoring home teams against low-strikeout pitchers.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Clean-up hitters facing a starting pitcher with a high WHIP metric.</td>
              </tr>
              <tr style="border-bottom:1px solid #1e1e1e">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Wednesday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Mid-Series G3</span></td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Contact-first batters facing a starter with low chase-rates (&lt;25%).</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Hitters facing a starter who dominates with opposite-handed splits.</td>
                <td style="padding:8px 8px;color:#c4b5fd;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Groundball hitters facing a starter with a high-ride fastball.</td>
                <td style="padding:8px 8px;color:#93c5fd;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Lineups with high OPS metrics facing an injury-return starter.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Power hitters facing a starter prone to giving up home runs.</td>
              </tr>
              <tr style="border-bottom:1px solid #1e1e1e;background:rgba(255,255,255,.015)">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Thursday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Travel Days</span></td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Hungry utility players or backup hitters getting spot starts.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Star players who risk an early pull to rest for travel.</td>
                <td style="padding:8px 8px;color:#c4b5fd;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> High-contact hitters facing an elite groundball specialist.</td>
                <td style="padding:8px 8px;color:#93c5fd;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Mixed lineups filled with bench players resting key stars.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Clean-up hitters if the primary on-base runners are sitting.</td>
              </tr>
              <tr style="border-bottom:1px solid #1e1e1e">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Friday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Weekend Openers</span></td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Dynamic baserunners in high-altitude/hot home stadiums.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Hitters facing a rested, top-tier Friday night Ace.</td>
                <td style="padding:8px 8px;color:#c4b5fd;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Pull-heavy hitters facing an elite starter with a sharp slider.</td>
                <td style="padding:8px 8px;color:#93c5fd;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Elite offenses playing against a starter with a high away-ERA.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> RBI leaders facing a starter who panics with runners on base.</td>
              </tr>
              <tr style="border-bottom:1px solid #1e1e1e;background:rgba(255,255,255,.015)">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Saturday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Weekend G2</span></td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Hitters with a documented history of crushing this starter.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Cold hitters facing a starter with an elite sweeping breaking ball.</td>
                <td style="padding:8px 8px;color:#c4b5fd;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Low-power hitters facing a starter with high physical extension.</td>
                <td style="padding:8px 8px;color:#93c5fd;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Target high-walk offenses facing a wild starting pitcher.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Middle-of-the-order hitters vs. low-velocity starters.</td>
              </tr>
              <tr>
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Sunday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Series Finales</span></td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Hitters facing a starter they have completely solved all weekend.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Hitters facing an elite high-strikeout stopper pitcher.</td>
                <td style="padding:8px 8px;color:#c4b5fd;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Soft-contact hitters facing a high-heat fastball starter.</td>
                <td style="padding:8px 8px;color:#93c5fd;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> High-scoring teams capitalizing on a worn-down pitching staff.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Power hitters facing a starter with poor late-inning stamina.</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div id="bc-pit" style="overflow-x:auto;display:none">
          <div style="font-size:.65rem;color:#64748b;margin-bottom:8px"><b style="color:#4ade80">O</b> = Over signal &nbsp;&#183;&nbsp; <b style="color:#ff8a65">U</b> = Under signal</div>
          <table style="width:100%;border-collapse:collapse;font-size:.71rem;min-width:680px">
            <thead>
              <tr style="border-bottom:2px solid #1e293b">
                <th style="text-align:left;padding:7px 8px;color:#94a3b8;font-size:.62rem;letter-spacing:.06em;white-space:nowrap;font-weight:700;width:9%">DAY &amp; TREND</th>
                <th style="text-align:left;padding:7px 8px;color:#63cab7;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#129518; PITCHER K<br><span style="color:#64748b;font-weight:400">Strikeouts O/U</span></th>
                <th style="text-align:left;padding:7px 8px;color:#93c5fd;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#127919; HITS ALLOWED<br><span style="color:#64748b;font-weight:400">Hits Allowed O/U</span></th>
                <th style="text-align:left;padding:7px 8px;color:#94a3b8;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#9200; OUTS RECORDED<br><span style="color:#64748b;font-weight:400">Outs Recorded O/U</span></th>
                <th style="text-align:left;padding:7px 8px;color:#ff8a65;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#128165; EARNED RUNS<br><span style="color:#64748b;font-weight:400">Earned Runs O/U</span></th>
                <th style="text-align:left;padding:7px 8px;color:#fbbf24;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:19%">&#128694; WALKS ALLOWED<br><span style="color:#64748b;font-weight:400">Walks Allowed O/U</span></th>
              </tr>
            </thead>
            <tbody>
              <tr style="border-bottom:1px solid #1e1e1e">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Monday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Series Openers</span></td>
                <td style="padding:8px 8px;color:#a7f3d0;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Elite Aces facing aggressive, high-strikeout road teams.</td>
                <td style="padding:8px 8px;color:#bfdbfe;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Unfamiliarity favors fresh starters; bet under on mid-tier arms.</td>
                <td style="padding:8px 8px;color:#cbd5e1;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Managers push Aces deeper to preserve bullpen early.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Target under on home starters with an ERA under 3.50.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> High-command starters with a career walk-rate under 7%.</td>
              </tr>
              <tr style="border-bottom:1px solid #1e1e1e;background:rgba(255,255,255,.015)">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Tuesday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Mid-Series G2</span></td>
                <td style="padding:8px 8px;color:#a7f3d0;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Batters adjust quickly to team pitch shapes; avoid high lines.</td>
                <td style="padding:8px 8px;color:#bfdbfe;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Target over on flyball pitchers playing in small stadiums.</td>
                <td style="padding:8px 8px;color:#cbd5e1;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Workhorse mid-rotation starters facing a slumping lineup.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Sub-par starters with high hard-hit rates allowed (&gt;40%).</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Erratic starters facing highly disciplined, high-BB% teams.</td>
              </tr>
              <tr style="border-bottom:1px solid #1e1e1e">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Wednesday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Mid-Series G3</span></td>
                <td style="padding:8px 8px;color:#a7f3d0;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> High-whiff pitchers facing bottom-tier, high-strikeout teams.</td>
                <td style="padding:8px 8px;color:#bfdbfe;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Pitchers who rely heavily on a single pitch type (predictable).</td>
                <td style="padding:8px 8px;color:#cbd5e1;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Target under on starters facing a lineup for the 3rd time in a year.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Pitchers with a high FIP (Fielding Independent Pitching).</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Young starters who rely heavily on pitching to the edges.</td>
              </tr>
              <tr style="border-bottom:1px solid #1e1e1e;background:rgba(255,255,255,.015)">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Thursday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Travel Days</span></td>
                <td style="padding:8px 8px;color:#a7f3d0;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Managers use quick hooks to avoid blowout fatigue.</td>
                <td style="padding:8px 8px;color:#bfdbfe;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Weak spot-starters or emergency call-ups filling in.</td>
                <td style="padding:8px 8px;color:#cbd5e1;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Shortest leash of the week; top target for UNDER bets.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Emergency call-up pitchers or long-relief spot-starters.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Quick hooks pull starters before stacking up walks.</td>
              </tr>
              <tr style="border-bottom:1px solid #1e1e1e">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Friday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Weekend Openers</span></td>
                <td style="padding:8px 8px;color:#a7f3d0;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Home Aces backed by loud, energized weekend crowds.</td>
                <td style="padding:8px 8px;color:#bfdbfe;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Friday night elite Aces playing in large, deep ballparks.</td>
                <td style="padding:8px 8px;color:#cbd5e1;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Friday night workhorses expected to carry the bulk of the game.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Top-tier Aces with low home-stadium home-run rates.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Confident home starters who attack the strike zone early.</td>
              </tr>
              <tr style="border-bottom:1px solid #1e1e1e;background:rgba(255,255,255,.015)">
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Saturday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Weekend G2</span></td>
                <td style="padding:8px 8px;color:#a7f3d0;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Smart contact lineups facing a mid-tier starting pitcher.</td>
                <td style="padding:8px 8px;color:#bfdbfe;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Starters facing a lineup that is hitting well all weekend.</td>
                <td style="padding:8px 8px;color:#cbd5e1;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Fast managerial hooks to secure a critical weekend win.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> High-contact starters facing an offense on a hot streak.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Pitchers who struggle to find their release points under pressure.</td>
              </tr>
              <tr>
                <td style="padding:8px 8px;color:#e2e8f0;font-weight:700;white-space:nowrap;vertical-align:top">Sunday<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Series Finales</span></td>
                <td style="padding:8px 8px;color:#a7f3d0;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Batters have maximum familiarity with pitches; avoid high lines.</td>
                <td style="padding:8px 8px;color:#bfdbfe;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Exhausted starters who are tipping pitches by late afternoon.</td>
                <td style="padding:8px 8px;color:#cbd5e1;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Shortest leash of the weekend; managers pull starters early.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Starters facing an offense that scored heavily in games 1 and 2.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Tired starters losing their command and walking batters early.</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="run-box" id="runBox" style="text-align:center;max-width:600px;margin:0 auto 20px">
      <h2 style="font-family:'Playfair Display',serif;font-size:1.5rem;font-weight:700;color:#fff;margin-bottom:20px">Run Today's Picks</h2>
      <div class="date-row" style="justify-content:center;margin-bottom:20px">
        <label>Date</label>
        <input type="date" id="date-picker" max=""/>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:10px;justify-content:center;margin-bottom:12px">
        <button class="btn-primary" id="get-btn" onclick="getPicks()">🎯 Get Picks</button>
        <button class="btn-primary admin-only" id="run-btn" onclick="startRun()">Run Picks</button>
        <button class="btn-primary admin-only" id="force-btn" onclick="startRun(true)" style="background:#dc2626;color:#fff" title="Bypass cache and rebuild today's picks from scratch">Force Refresh</button>
      </div>
      <div id="run-spinner" class="hidden" style="margin-top:12px;color:#6b7280;font-size:13px">
        <span class="spinner"></span> Analyzing player histories…
      </div>
    </div>
    <div id="progress-card" class="card p-6 hidden admin-only">
      <div class="flex justify-between items-center mb-3">
        <div class="section-hdr mb-0">Live Progress</div>
        <span id="prog-label" class="text-xs text-slate-400"></span>
      </div>
      <div style="background:rgba(255,255,255,.05);border-radius:9999px;overflow:hidden;margin-bottom:16px">
        <div id="prog-bar-inner" style="width:0%"></div>
      </div>
      <div id="log-box"></div>
    </div>
    <div id="results-card" class="hidden space-y-6">
      <div id="stats-row" class="chips"></div>
      <div style="display:flex;justify-content:flex-end;margin-top:-4px">
        <button type="button" onclick="downloadPicksCSV()"
          style="background:#1e1e1e;border:1px solid #f59e0b;color:#f59e0b;font-weight:700;font-size:.82rem;padding:8px 16px;border-radius:8px;cursor:pointer">
          &#11015; Download CSV
        </button>
      </div>
      <div class="card p-6 admin-only" id="parlayCard">
        <div class="section-hdr" style="color:#f59e0b">🎰 Auto Parlay Builder <span style="font-size:.7rem;color:#777;font-weight:400">admin only</span></div>
        <p class="text-xs text-slate-400 mb-3" style="margin-top:-4px">Pulls from any play today — hits, Under 1.5's, Pitcher K's — best available odds priced in.</p>
        <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
          <label class="text-slate-300" style="font-weight:700">Legs</label>
          <select id="parlayLegs" style="background:#0f0f0f;border:1px solid #262626;border-radius:8px;color:#fff;padding:8px 10px">
            <option>2</option><option>3</option><option>4</option><option>5</option><option>6</option><option>7</option><option>8</option><option>9</option><option>10</option><option>11</option><option>12</option><option>13</option><option>14</option><option>15</option><option>16</option><option>17</option><option>18</option><option>19</option><option>20</option>
          </select>
          <button class="btn-primary" onclick="buildParlay()">Build Best Parlay</button>
          <button class="btn-primary" onclick="generateParlay()" style="background:#1f2937;color:#fff">🎲 Generate New</button>
          <button class="btn-primary" id="parlay-overs-btn" onclick="toggleParlayOvers()" style="background:#1f2937;color:#fff">&#11014; Overs Only</button>
          <button class="btn-primary" id="parlay-unders-btn" onclick="toggleParlayUnders()" style="background:#1f2937;color:#fff">&#11015; Unders Only</button>
          <button class="btn-primary" id="parlay-minus-btn" onclick="toggleParlayMinus()" style="background:#1f2937;color:#fff">&minus; Odds Only</button>
          <button class="btn-primary" id="parlay-plus-btn" onclick="toggleParlayPlus()" style="background:#1f2937;color:#fff">&plus; Odds Only</button>
          <div style="position:relative;display:inline-block">
            <button class="btn-primary" id="parlay-cats-btn" onclick="toggleCatMenu(event)" style="background:#1f2937;color:#fff">&#9776; Categories (9/9) &#9662;</button>
            <div id="parlay-cats-menu" style="display:none;position:absolute;z-index:60;top:calc(100% + 6px);left:0;background:#0e0e0e;border:1px solid #2a2a2a;border-radius:10px;padding:10px;min-width:215px;box-shadow:0 12px 34px rgba(0,0,0,.55)">
              <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px">
                <span style="font-size:.66rem;color:#888;font-weight:800;letter-spacing:.06em">PARLAY CATEGORIES</span>
                <span style="font-size:.66rem"><a onclick="_catSetAll(true)" style="color:#63cab7;cursor:pointer;font-weight:800">All</a> <span style="color:#444">·</span> <a onclick="_catSetAll(false)" style="color:#ff8a65;cursor:pointer;font-weight:800">None</a></span>
              </div>
              <div id="parlay-cats-list">
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="HIT" checked onchange="_catChanged()"> Hits</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="UNDER_HITS" checked onchange="_catChanged()"> Under 1.5 Hits</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="UNDER_TB" checked onchange="_catChanged()"> Under 1.5 Total Bases</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="K" checked onchange="_catChanged()"> Pitcher K</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="RUN" checked onchange="_catChanged()"> Runs</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="pitcher_hits_allowed" checked onchange="_catChanged()"> Hits Allowed</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="pitcher_outs" checked onchange="_catChanged()"> Outs</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="pitcher_earned_runs" checked onchange="_catChanged()"> Earned Runs</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="pitcher_walks" checked onchange="_catChanged()"> Walks Allowed</label>
              </div>
            </div>
          </div>
          <div style="position:relative;display:inline-block">
            <button class="btn-primary" id="parlay-games-btn" onclick="toggleGamesMenu(event)" style="background:#1f2937;color:#fff">&#9776; Games (0/0) &#9662;</button>
            <div id="parlay-games-menu" style="display:none;position:absolute;z-index:60;top:calc(100% + 6px);left:0;background:#0e0e0e;border:1px solid #2a2a2a;border-radius:10px;padding:10px;min-width:240px;max-height:340px;overflow:auto;box-shadow:0 12px 34px rgba(0,0,0,.55)">
              <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px">
                <span style="font-size:.66rem;color:#888;font-weight:800;letter-spacing:.06em">PARLAY GAMES</span>
                <span style="font-size:.66rem"><a onclick="_gameSetAll(true)" style="color:#63cab7;cursor:pointer;font-weight:800">All</a> <span style="color:#444">·</span> <a onclick="_gameSetAll(false)" style="color:#ff8a65;cursor:pointer;font-weight:800">None</a></span>
              </div>
              <div id="parlay-games-list"><div style="font-size:.72rem;color:#666;padding:4px 2px">Run picks first.</div></div>
            </div>
          </div>
        </div>
        <div id="parlayResult" style="margin-top:16px"></div>
      </div>
      <div class="card p-6" id="top-picks-card">
        <div class="section-hdr">🏆 Top Picks — To Record a Hit</div>
        <div id="picks-body" class="mlb-picks-grid"></div>
        <div id="also-ran-wrap"></div>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>S1</strong> Lifetime BA vs today's pitcher &nbsp;|&nbsp;
          <strong>S2</strong> Lifetime H/A BA vs today's opponent &nbsp;|&nbsp;
          <strong>S3</strong> 2026 season H/A BA vs all teams &nbsp;|&nbsp;
          <strong>S4</strong> Last 10 H/A games vs THIS opponent — hit-rate reference &nbsp;|&nbsp;
          <strong>Hit Odds</strong> Sportsbook price "to record a hit" (0.5 line) &nbsp;|&nbsp;
          <strong>S5</strong> Day/night BA &nbsp;|&nbsp;
          <strong>Score</strong> = (S1+S2+S3+S5)×1000
        </p>
      </div>
      <div class="card p-6 hidden" id="under-picks-card" style="border-color:rgba(255,107,107,.25)">
        <div class="section-hdr" style="color:#ff8a65">⬇️ Under Picks — Bet Under 1.5 Hits</div>
        <div id="under-picks-body" class="mlb-picks-grid"></div>
        <div id="under-more-wrap"></div>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>Source</strong>: The Odds API — players with 1.5 hits O/U line &nbsp;|&nbsp;
          <strong>S1</strong> Career BA vs today's pitcher (under &lt; .250, N/A passes) &nbsp;|&nbsp;
          <strong>S2</strong> BA — last 10 (or fewer) H/A games vs today's opponent (under &lt; .250) &nbsp;|&nbsp;
          <strong>S3</strong> BA — last 10 (or fewer) H/A games vs any team (under &lt; .250) &nbsp;|&nbsp;
          <strong>L7</strong> Last 7 games BA, general (under &lt; .250) &nbsp;|&nbsp;
          <strong>Ranked #1 → coldest bat (S2 + S3 + L7)</strong>
        </p>
      </div>
      <div class="card p-6 hidden" id="tb-picks-card" style="border-color:rgba(167,139,250,.25)">
        <div class="section-hdr" style="color:#a78bfa">⬇️ Under 1.5 Total Bases</div>
        <div id="tb-picks-body" class="mlb-picks-grid"></div>
        <div id="tb-more-wrap"></div>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>Rate</strong> = % of H/A games vs opp (min 2 games) or any opp (min 5) with TB &lt; 2 &nbsp;|&nbsp;
          <strong>Threshold</strong> ≥70% under 1.5 TB &nbsp;|&nbsp; ranked by Wilson lower-bound
        </p>
      </div>
      <div class="card p-6 hidden" id="pitcher-k-card" style="border-color:rgba(99,202,183,.25)">
        <div class="section-hdr" style="color:#63cab7">⚾ Pitcher Picks — Strikeouts, Hits, Outs, Earned Runs &amp; Walks</div>
        <p class="text-xs text-slate-400 mb-3" style="margin-top:-4px">Click any pitcher for all 5 markets (K · Hits · Outs · ER · Walks). Each stat has its own pulldown below.</p>
        <div style="font-size:.72rem;font-weight:800;letter-spacing:.1em;color:#63cab7;margin-bottom:10px;text-transform:uppercase">📋 All Today's Pitchers</div>
        <div id="pitcher-all-body" class="mlb-picks-grid"></div>
        <div id="pitcher-all-more"></div>
        <div style="margin-top:24px;padding-top:20px;border-top:1px solid #1e293b">
          <div style="font-size:.72rem;font-weight:800;letter-spacing:.1em;color:#63cab7;margin-bottom:10px;text-transform:uppercase">⚡ Top K Overs</div>
          <div id="pitcher-k-over-body" class="mlb-picks-grid"></div>
          <div id="pitcher-k-over-more"></div>
        </div>
        <div style="margin-top:20px;padding-top:16px;border-top:1px solid #1e293b">
          <div style="font-size:.72rem;font-weight:800;letter-spacing:.1em;color:#ff8a65;margin-bottom:10px;text-transform:uppercase">⬇ Top K Unders</div>
          <div id="pitcher-k-under-body" class="mlb-picks-grid"></div>
          <div id="pitcher-k-under-more"></div>
        </div>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>K History</strong> = H/A starts vs today's opponent only &nbsp;|&nbsp;
          <strong>Pick</strong> = OVER/UNDER based on blended avg (50% career H/A vs opp + 50% last 5 starts). ⚠️ = signals conflict. Min 1 career start vs opp.
        </p>
      </div>
      <div id="pitcher-props-wrap"></div>
      <div class="card p-6 hidden" id="rbi-picks-card" style="border-color:rgba(245,158,11,.25)">
        <div class="section-hdr" style="color:#f59e0b">💥 RBI Picks — Drive In a Run (Over / Under 0.5)</div>
        <p class="text-xs text-slate-400 mb-3" style="margin-top:-4px">Who drives in runs vs this opponent. Vs-opp only (min 3 games), ranked by Wilson confidence.</p>
        <div id="rbi-over-section">
          <div style="font-size:.72rem;font-weight:800;letter-spacing:.1em;color:#f59e0b;margin-bottom:10px;text-transform:uppercase">⬆ RBI Over</div>
          <div id="rbi-over-body" class="mlb-picks-grid"></div>
          <div id="rbi-over-more"></div>
        </div>
        <div id="rbi-under-section" style="margin-top:24px;padding-top:20px;border-top:1px solid #1e293b">
          <div style="font-size:.72rem;font-weight:800;letter-spacing:.1em;color:#ff8a65;margin-bottom:10px;text-transform:uppercase">⬇ RBI Under</div>
          <div id="rbi-under-body" class="mlb-picks-grid"></div>
          <div id="rbi-under-more"></div>
        </div>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>RBI Rate vr Opp</strong> = last 10 H/A games vs THIS opponent with 1+ RBI &nbsp;|&nbsp;
          <strong>Pick</strong> = OVER ≥70%, UNDER ≤30%, vs-opp only (min 3 games) &nbsp;|&nbsp; ranked by Wilson lower-bound.
        </p>
      </div>
      <div class="card p-6 hidden" id="runs-picks-card" style="border-color:rgba(96,165,250,.25)">
        <div class="section-hdr" style="color:#60a5fa">🏃 Runs Picks — Score a Run (Over / Under 0.5)</div>
        <p class="text-xs text-slate-400 mb-3" style="margin-top:-4px">Who's likely to cross the plate. Runs are lower-frequency than hits — higher-variance plays.</p>
        <div id="runs-over-section">
          <div style="font-size:.72rem;font-weight:800;letter-spacing:.1em;color:#60a5fa;margin-bottom:10px;text-transform:uppercase">⬆ Runs Over</div>
          <div id="runs-over-body" class="mlb-picks-grid"></div>
          <div id="runs-over-more"></div>
        </div>
        <div id="runs-under-section" style="margin-top:24px;padding-top:20px;border-top:1px solid #1e293b">
          <div style="font-size:.72rem;font-weight:800;letter-spacing:.1em;color:#ff8a65;margin-bottom:10px;text-transform:uppercase">⬇ Runs Under</div>
          <div id="runs-under-body" class="mlb-picks-grid"></div>
          <div id="runs-under-more"></div>
        </div>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>Runs Rate vr Opp</strong> = last 10 H/A games vs THIS opponent with 1+ run &nbsp;|&nbsp;
          <strong>Pick</strong> = OVER ≥70%, UNDER ≤30%, vs-opp only (min 3 games) &nbsp;|&nbsp; ranked by Wilson lower-bound.
        </p>
      </div>
      <div class="card p-6" id="player-search-card">
        <div class="section-hdr">🔍 Player Lookup</div>
        <p class="text-xs text-slate-400 mb-3">Type a hitter or pitcher's name — see where they rank and why.</p>
        <input id="player-search-input" type="text" placeholder="e.g. Aaron Judge, Gerrit Cole..."
               style="width:100%;padding:12px 16px;background:#0f0f0f;border:1px solid #262626;border-radius:10px;color:#fff;font-size:.95rem;outline:none"
               oninput="runPlayerSearch(this.value)">
        <div id="player-search-result" class="mt-3"></div>
      </div>
      <div class="card p-6" id="by-game-card">
        <div class="section-hdr" style="color:#f59e0b">🏟️ Picks by Game</div>
        <p class="text-xs text-slate-500 mb-3">Pick your team's game to see all picks for that matchup.</p>
        <div id="by-game-body"></div>
      </div>
    </div>
  </main>
</div>

<script>
let token = localStorage.getItem('mlb_token') || '';
let username = localStorage.getItem('mlb_user') || '';
let es = null;

window.onload = () => {
  // Parse the hub login token first (URL or stored) so admin detection runs in
  // BOTH snapshot mode and the normal flow — never skipped by an early return.
  const KEY = '__mpa_token';
  const params = new URLSearchParams(window.location.search);
  const urlTok = params.get('token');
  if (urlTok) { localStorage.setItem(KEY, urlTok); window.history.replaceState({}, '', window.location.pathname); }
  token = localStorage.getItem(KEY) || '';
  // Auto-enable the admin view if this logged-in user is the admin — works on
  // any device with no key. (Cosmetic toggle; the underlying data is the same.)
  // Silently no-ops if there is no token or the endpoint isn't reachable.
  if (!window.IS_ADMIN && token) {
    fetch('/api/whoami?token=' + encodeURIComponent(token))
      .then(r => r.json())
      .then(d => { if (d && d.is_admin) { window.IS_ADMIN = true; document.body.classList.add('is-admin'); } })
      .catch(() => {});
  }
  // Snapshot mode: the Replit hub serves this page with picks already baked in
  // as window.__INITIAL_PICKS__ (set just before </head>). When present, skip
  // the login + /api/run flow entirely and render straight from the snapshot.
  if (window.__INITIAL_PICKS__) {
    hide('login-screen'); show('dashboard');
    hide('progress-card');
    const r = window.__INITIAL_PICKS__;
    const dp = document.getElementById('date-picker');
    if (dp && r.date) dp.value = r.date;
    showResults(r);
    return;
  }
  if (!token) { window.location.href = 'https://moneypicksarena.com'; return; }
  showDashboard();
};

async function doLogin(e) {
  e.preventDefault();
  const btn = document.getElementById('login-btn');
  btn.disabled = true; btn.textContent = 'Signing in…';
  const fd = new FormData();
  fd.append('username', document.getElementById('inp-user').value.trim());
  fd.append('password', document.getElementById('inp-pass').value);
  try {
    const r = await fetch('/api/login', { method: 'POST', body: fd });
    if (r.ok) {
      const d = await r.json(); token = d.access_token; username = d.username;
      localStorage.setItem('mlb_token', token); localStorage.setItem('mlb_user', username);
      showDashboard();
    } else { showLoginError('Invalid username or password.'); }
  } catch { showLoginError('Server error. Please try again.'); }
  finally { btn.disabled = false; btn.textContent = 'Sign In'; }
}

function showLoginError(msg) {
  const el = document.getElementById('login-error');
  el.textContent = msg; el.classList.remove('hidden');
}

function showDashboard() {
  hide('login-screen'); show('dashboard');
  const d = new Date();
  const today = d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
  const tom = new Date(d); tom.setDate(tom.getDate()+1);
  const tomorrow = tom.getFullYear()+'-'+String(tom.getMonth()+1).padStart(2,'0')+'-'+String(tom.getDate()).padStart(2,'0');
  const dp = document.getElementById('date-picker');
  if (dp) { dp.value = today; dp.min = today; dp.max = tomorrow; }
  hide('progress-card'); hide('results-card');
}

// Get Picks: load the picks already saved on file for the chosen date and show
// them. Read-only — never starts a pipeline run, so any member can use it.
async function getPicks() {
  const dateStr = document.getElementById('date-picker').value;
  if (!dateStr) { alert('Please select a date.'); return; }
  const btn = document.getElementById('get-btn');
  const orig = btn.textContent;
  btn.disabled = true; btn.textContent = 'Loading…';
  hide('results-card');
  try {
    const _mlbTok = localStorage.getItem('__mpa_token') || '';
    const r = await fetch(`/api/results/${dateStr}?token=${encodeURIComponent(_mlbTok)}`);
    if (r.status === 404) {
      alert("Today's picks aren't ready yet — check back a little later.");
      return;
    }
    if (!r.ok) throw new Error('Could not load picks. Please try again.');
    const data = await r.json();
    showResults(data);
  } catch (err) {
    alert(err.message || 'Could not load picks. Please try again.');
  } finally {
    btn.disabled = false; btn.textContent = orig;
  }
}

async function startRun(force=false) {
  const dateStr = document.getElementById('date-picker').value;
  if (!dateStr) { alert('Please select a date.'); return; }
  if (force && !window.IS_ADMIN) { return; }
  clearLog(); hide('results-card');
  show('progress-card'); setProgress(0, '');
  show('run-spinner'); disableRunBtn(true);
  try {
    const urlForce = new URLSearchParams(window.location.search).get('force') === 'true';
    const forceParam = (force || urlForce) ? '&force=true' : '';
    const r = await fetch(`/api/run?date_str=${dateStr}${forceParam}&token=${encodeURIComponent(token)}`, { method: 'POST' });
    if (!r.ok) { const e = await r.json(); throw new Error(e.detail || 'Run failed'); }
    const { task_id } = await r.json();
    openSSE(task_id);
  } catch (err) {
    appendLog(`❌ ${err.message}`, 'dq');
    hide('run-spinner'); disableRunBtn(false);
  }
}

function openSSE(taskId) {
  if (es) es.close();
  es = new EventSource(`/api/stream/${taskId}?token=${encodeURIComponent(token)}`);
  es.onmessage = evt => handleEvent(JSON.parse(evt.data));
  es.onerror = () => {
    if (document.getElementById('results-card').classList.contains('hidden'))
      appendLog('⚠️  Connection lost — refresh and try again.', 'dq');
    hide('run-spinner'); disableRunBtn(false); es.close();
  };
}

function handleEvent(ev) {
  switch (ev.type) {
    case 'section': appendLog('', 'default'); appendLog(`▸ ${ev.msg}`, 'section'); break;
    case 'log': case 'step1_done': appendLog(ev.msg, ev.msg.startsWith('✅') ? 'ok' : 'info'); break;
    case 'cached': appendLog(ev.msg, 'cached'); break;
    case 'progress': setProgress(Math.round((ev.current/ev.total)*80), `${ev.current}/${ev.total}: ${ev.name}`); break;
    case 'player_ok': appendLog(`  ✅ ${pad(ev.name,22)} S1:${ev.s1}  S2:${ev.s2}  S3:${ev.s3}  ${ev.side} vs ${ev.opp}  → ${ev.total}pts`, 'ok'); break;
    case 'player_dq': appendLog(`  ❌ ${pad(ev.name,22)} S1:${ev.s1}  S2:${ev.s2}  S3:${ev.s3}  DQ: ${ev.reason}`, 'dq'); break;
    case 'player_skip': appendLog(`  — ${pad(ev.name,22)} No game today`, 'skip'); break;
    case 'dn_ok': appendLog(`  ✅ ${pad(ev.name,22)} ${ev.label} ${ev.display}`, 'ok'); break;
    case 'dn_dq': appendLog(`  ❌ ${pad(ev.name,22)} ${ev.label} ${ev.display} < .200 — DQ`, 'dq'); break;
    case 'done':
      setProgress(100, 'Complete!');
      appendLog('', 'default');
      appendLog(`🏆 Done! ${ev.result.stats.picks} picks found in ${ev.result.stats.elapsed}s`, 'ok');
      hide('run-spinner'); disableRunBtn(false); showResults(ev.result); es.close(); break;
    case 'error': appendLog(`❌ ERROR: ${ev.msg}`, 'dq'); hide('run-spinner'); disableRunBtn(false); es.close(); break;
  }
}

// ── Live "game already started" filter ──────────────────────────────────────
// A player is dropped once their game's first pitch (game_start, ISO UTC) is in the
// past. Picks with no game_start (older cache from before this was deployed) are kept.
function _started(p){
  if(!p||!p.game_start) return false;
  var t=Date.parse(p.game_start);
  return !isNaN(t) && t<=Date.now();
}
// Returns a shallow copy of the result with started-game players removed from every
// list the board renders from (cards, under, pitcher K). The original payload is left
// untouched so re-filtering later (e.g. in the parlay pool) stays accurate.
function _filterStarted(result){
  if(!result) return result;
  var r=Object.assign({},result);
  function f(a){return (a||[]).filter(function(p){return !_started(p);});}
  r.top9=f(r.top9); r.also_ran=f(r.also_ran); r.under_picks=f(r.under_picks); r.runs_picks=f(r.runs_picks); r.tb_picks=f(r.tb_picks);
  if(r.pitcher_k){
    r.pitcher_k=Object.assign({},r.pitcher_k);
    r.pitcher_k.picks=f(r.pitcher_k.picks);
    r.pitcher_k.all=f(r.pitcher_k.all);
  }
  if(r.pitcher_props){
    var pp={};
    Object.keys(r.pitcher_props).forEach(function(m){
      var b=r.pitcher_props[m]||{};
      pp[m]={picks:f(b.picks),all:f(b.all)};
    });
    r.pitcher_props=pp;
  }
  return r;
}
function showResults(result) {
  // Hide players whose game has already started so the cards, parlay, all-by-game view
  // and CSV only show bettable plays. Re-run picks after deploy to populate game_start.
  result = _filterStarted(result);
  window._lastResult = result;
  // Admin-only "Unders Only" view: hide every OVER-based pick (hitter Top Picks,
  // Money Ball, pitcher K OVERs) and keep only UNDER plays. window._lastResult
  // stays the FULL result so parlay/CSV/search are unaffected — we only filter a
  // local render copy.
  const view = (window.UNDERS_ONLY && window.IS_ADMIN)
    ? Object.assign({}, result, {
        top9: [],
        also_ran: [],
        pitcher_k: result.pitcher_k ? Object.assign({}, result.pitcher_k, {
          all: (result.pitcher_k.all || []).filter(p => p.pick === 'UNDER'),
          picks: (result.pitcher_k.picks || []).filter(p => p.pick === 'UNDER'),
        }) : result.pitcher_k,
        runs_picks: (result.runs_picks || []).filter(p => p.pick === 'UNDER'),
        pitcher_props: (function(){
          var src=result.pitcher_props||{}, out={};
          Object.keys(src).forEach(function(m){
            var b=src[m]||{};
            out[m]={picks:(b.picks||[]).filter(p=>p.pick==='UNDER'),
                    all:(b.all||[]).filter(p=>p.pick==='UNDER')};
          });
          return out;
        })(),
      })
    : result;
  const { top9, stats, pitcher_k } = view;
  hide('under-picks-card'); hide('tb-picks-card'); hide('pitcher-k-card'); hide('runs-picks-card'); hide('rbi-picks-card');

  document.getElementById('stats-row').innerHTML = [
    statCard('🎯','Top Picks',top9.length,'top-picks-card'),
    statCard('⬇️','Under Picks',(view.under_picks||[]).length,'under-picks-card'),
    statCard('⬇️','TB Under',(view.tb_picks||[]).length,'tb-picks-card'),
    statCard('💥','RBI Picks',(view.rbi_picks||[]).length,'rbi-picks-card'),
    statCard('🏃','Runs Picks',(view.runs_picks||[]).length,'runs-picks-card'),
    statCard('⚾','Pitcher K',((view.pitcher_k||{}).all||[]).filter(p=>p.pick&&(p.starts||0)>0).length,'pitcher-k-card'),
    statCard('🧮','Pitcher Props',PROP_ORDER.reduce((n,m)=>n+(((view.pitcher_props||{})[m]||{}).picks||[]).length,0),'pitcher-k-card'),
    statCard('⚾','Games Today',stats.games,'by-game-card'),
    statCard('🔍','Players Run',stats.step1_count),
    statCard('⏱️','Time (s)',stats.elapsed),
  ].join('');

  if (window.UNDERS_ONLY && window.IS_ADMIN) { hide('top-picks-card'); } else { show('top-picks-card'); }
  window.__HIT_REG__={};
  document.getElementById('picks-body').innerHTML = top9.map((p,i) => _mlbCard(p, i+1)).join('');
  const alsoRan = view.also_ran || [];
  document.getElementById('also-ran-wrap').innerHTML = alsoRan.length > 0
    ? _moreWrap(alsoRan, function(p,r){ return _mlbCard(p, r, true); }, 11, 'More Hit Picks', '#f59e0b')
    : '';

  const underPicks = view.under_picks || [];
  if (underPicks.length > 0) {
    show('under-picks-card');
    document.getElementById('under-picks-body').innerHTML = underPicks.slice(0, 10).map((p,i) => _underCard(p, i+1)).join('');
    document.getElementById('under-more-wrap').innerHTML = underPicks.length > 10
      ? _moreWrap(underPicks.slice(10), function(p,r){ return _underCard(p, r); }, 11, 'Under Picks', '#ff8a65')
      : '';
  }

  const pkData=view.pitcher_k||{}, pkAll=pkData.all||[];
  window.__TEAM_K_RANKS__=(pkData.team_k_ranks||[]);
  if (pkAll.length > 0) {
    show('pitcher-k-card');
    const pkSorted = pkAll.filter(p=>p.pick && (p.starts||0)>0).sort((a,b)=>{
      const ga=Math.abs((a.blended_avg_k!=null?a.blended_avg_k:(a.avg_k||0))-(a.line||0))*_umpKMul(a);
      const gb=Math.abs((b.blended_avg_k!=null?b.blended_avg_k:(b.avg_k||0))-(b.line||0))*_umpKMul(b);
      return gb-ga;
    });
    const pkOvers=pkSorted.filter(p=>p.pick==='OVER');
    const pkUnders=pkSorted.filter(p=>p.pick==='UNDER');
    window.__PK_REG__={};
    const _renderKSec=function(arr,bodyId,moreId,themeClr,label,kpfx){
      var el=document.getElementById(bodyId);
      if(el) el.innerHTML=arr.length>0?arr.slice(0,10).map((p,_i)=>_pitcherCard(p,_i+1,kpfx)).join('')
        :'<p class="text-slate-500 text-center" style="padding:16px">No '+label+' today</p>';
      var me=document.getElementById(moreId);
      if(me) me.innerHTML=arr.length>10?'<details style="margin-top:10px"><summary class="more-btn" style="color:'+themeClr+';border-color:'+themeClr+'33">&#9655; '+(arr.length-10)+' more</summary><div class="mlb-picks-grid mt-3">'+arr.slice(10).map((p,_i)=>_pitcherCard(p,10+_i+1,kpfx)).join('')+'</div></details>':'';
    };
    _renderKSec(pkOvers,'pitcher-k-over-body','pitcher-k-over-more','#63cab7','K Overs','pk');
    _renderKSec(pkUnders,'pitcher-k-under-body','pitcher-k-under-more','#ff8a65','K Unders','pu');
    // All Today's Pitchers cards — ranked by avg K desc
    const pkAllSorted=[...pkAll].sort((a,b)=>{
      const ka=a.blended_avg_k!=null?a.blended_avg_k:(a.avg_k||0);
      const kb=b.blended_avg_k!=null?b.blended_avg_k:(b.avg_k||0);
      return kb-ka;
    });
    document.getElementById('pitcher-all-body').innerHTML=pkAllSorted.length>0
      ?pkAllSorted.slice(0,10).map((p,_i)=>_pitcherCard(p,_i+1,'pa')).join('')
      :'<p class="text-slate-500 text-center" style="padding:16px">No pitchers today</p>';
    const paMoreEl=document.getElementById('pitcher-all-more');
    if(paMoreEl) paMoreEl.innerHTML=pkAllSorted.length>10
      ?'<details style="margin-top:10px"><summary class="more-btn" style="color:#63cab7;border-color:#63cab733">&#9655; '+(pkAllSorted.length-10)+' more pitchers</summary><div class="mlb-picks-grid mt-3">'+pkAllSorted.slice(10).map((p,_i)=>_pitcherCard(p,10+_i+1,'pa')).join('')+'</div></details>'
      :'';
  }

  const rbiPicks = view.rbi_picks || [];
  if (rbiPicks.length > 0) {
    show('rbi-picks-card');
    window.__RBI_REG__={};
    const rbiOver = rbiPicks.filter(function(p){ return p.pick==='OVER'; });
    const rbiUnder = rbiPicks.filter(function(p){ return p.pick==='UNDER'; });
    document.getElementById('rbi-over-body').innerHTML = rbiOver.slice(0,10).map(function(p,i){ return _rbiCard(p, i+1, 'rbo'); }).join('');
    document.getElementById('rbi-over-more').innerHTML = rbiOver.length > 10
      ? _moreWrap(rbiOver.slice(10), function(p,r){ return _rbiCard(p, r, 'rbo'); }, 11, 'RBI Over', '#f59e0b')
      : '';
    document.getElementById('rbi-under-section').style.display = rbiUnder.length ? '' : 'none';
    document.getElementById('rbi-under-body').innerHTML = rbiUnder.slice(0,10).map(function(p,i){ return _rbiCard(p, i+1, 'rbu'); }).join('');
    document.getElementById('rbi-under-more').innerHTML = rbiUnder.length > 10
      ? _moreWrap(rbiUnder.slice(10), function(p,r){ return _rbiCard(p, r, 'rbu'); }, 11, 'RBI Under', '#ff8a65')
      : '';
  }

  const runsPicks = view.runs_picks || [];
  if (runsPicks.length > 0) {
    show('runs-picks-card');
    window.__RUNS_REG__={};
    const runsOver = runsPicks.filter(function(p){ return p.pick==='OVER'; });
    const runsUnder = runsPicks.filter(function(p){ return p.pick==='UNDER'; });
    document.getElementById('runs-over-body').innerHTML = runsOver.slice(0,10).map(function(p,i){ return _runsCard(p, i+1, 'rno'); }).join('');
    document.getElementById('runs-over-more').innerHTML = runsOver.length > 10
      ? _moreWrap(runsOver.slice(10), function(p,r){ return _runsCard(p, r, 'rno'); }, 11, 'Runs Over', '#60a5fa')
      : '';
    document.getElementById('runs-under-section').style.display = runsUnder.length ? '' : 'none';
    document.getElementById('runs-under-body').innerHTML = runsUnder.slice(0,10).map(function(p,i){ return _runsCard(p, i+1, 'rnu'); }).join('');
    document.getElementById('runs-under-more').innerHTML = runsUnder.length > 10
      ? _moreWrap(runsUnder.slice(10), function(p,r){ return _runsCard(p, r, 'rnu'); }, 11, 'Runs Under', '#ff8a65')
      : '';
  }

  const tbPicks = view.tb_picks || [];
  if (tbPicks.length > 0) {
    show('tb-picks-card');
    window.__TB_REG__={};
    document.getElementById('tb-picks-body').innerHTML = tbPicks.slice(0,10).map(function(p,i){ return _tbCard(p, i+1); }).join('');
    document.getElementById('tb-more-wrap').innerHTML = tbPicks.length > 10
      ? _moreWrap(tbPicks.slice(10), function(p,r){ return _tbCard(p, r); }, 11, 'TB Under', '#a78bfa')
      : '';
  }

  renderPitcherProps(view);
  renderByGame(view);
  _buildGamesMenu();  // refresh the parlay "Games" filter list from today's full slate
  show('results-card');
}

// ── Pitcher recent-form popup (click a pitcher row) ────────────────────
function _pkForm(key){
  var p=(key&&typeof key==='object')?key:(window.__PK_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('pk-modal');
  if(!ov){
    ov=document.createElement('div');
    ov.id='pk-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var line=p.line;
  var vlog=p.vs_opp_log||[];
  var usingVs=vlog.length>0;
  var log=usingVs?vlog:(p.recent_k_log||[]);
  var rows=log.length?log.map(function(g){
    var kv=(g.k!=null?g.k:g.v);
    var over=line!=null&&kv>line;
    var clr=line!=null?(over?'#63cab7':'#ff8a65'):'#e2e8f0';
    var oppCell=usingVs?'':`<td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">${g.opp?('vs '+g.opp):''}</td>`;
    return `<tr>
      <td style="padding:6px 10px;color:#94a3b8;font-family:monospace">${g.d||'—'}</td>
      ${oppCell}
      <td style="padding:6px 10px;color:#93c5fd;font-family:monospace;font-size:.8rem">${g.ip?(g.ip+' IP'):''}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:${clr}">${kv!=null?kv+' K':'—'}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:#fca5a5">${g.h!=null?g.h+' H':'—'}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;color:#a78bfa">${g.outs!=null?g.outs+' outs':'—'}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;color:#fb923c">${g.er!=null?g.er+' ER':'—'}</td>
    </tr>`;
  }).join(''):`<tr><td colspan="${usingVs?6:7}" style="padding:14px;color:#64748b;text-align:center">No starts on record</td></tr>`;
  // Recent form (last N any-opp starts) — always shown as its own section when we
  // also have a vs-opp table above (so user sees dated K's like 5, 7, 12).
  var rlog=p.recent_k_log||[];
  var recentRows=rlog.length?rlog.map(function(g){
    var kv=(g.k!=null?g.k:g.v);
    var over=line!=null&&kv>line;
    var clr=line!=null?(over?'#63cab7':'#ff8a65'):'#e2e8f0';
    return `<tr>
      <td style="padding:6px 10px;color:#94a3b8;font-family:monospace">${g.d||'—'}</td>
      <td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">${g.opp?('vs '+g.opp):''}</td>
      <td style="padding:6px 10px;color:#93c5fd;font-family:monospace;font-size:.8rem">${g.ip?(g.ip+' IP'):''}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:${clr}">${kv!=null?kv+' K':'—'}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:#fca5a5">${g.h!=null?g.h+' H':'—'}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;color:#a78bfa">${g.outs!=null?g.outs+' outs':'—'}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;color:#fb923c">${g.er!=null?g.er+' ER':'—'}</td>
    </tr>`;
  }).join(''):'';
  var recentSection=(usingVs&&recentRows)?`
    <div style="margin-top:18px;font-size:.72rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:8px">Last ${rlog.length} Starts (any opp)</div>
    <table style="width:100%;border-collapse:collapse;font-size:.85rem"><thead><tr><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">Date</th><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">Opp</th><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">IP</th><th style="text-align:right;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">K</th><th style="text-align:right;padding:4px 10px;color:#fca5a5;font-size:.68rem;font-weight:600">H</th><th style="text-align:right;padding:4px 10px;color:#a78bfa;font-size:.68rem;font-weight:600">Outs</th><th style="text-align:right;padding:4px 10px;color:#fb923c;font-size:.68rem;font-weight:600">ER</th></tr></thead><tbody>${recentRows}</tbody></table>`:'';
  var histTitle=usingVs?('Starts vs '+(p.opp||'opp')+' — Ks, Hits, Outs & ER'):('Last '+(log.length||0)+' Starts (any opp)');
  var careerTxt=p.avg_k!=null?(p.avg_k+' K · '+(p.starts||0)+' starts vs '+(p.opp||'opp')):'no career vs opp';
  var recentTxt=p.recent_avg_k!=null?(p.recent_avg_k+' K · last '+(p.recent_starts||0)):'no recent data';
  var blendTxt=p.blended_avg_k!=null?(p.blended_avg_k+' K'):'—';
  var lineTxt=line!=null?(line+' Ks'):'no line';
  var pickClr=p.pick==='OVER'?'#63cab7':(p.pick==='UNDER'?'#ff8a65':'#94a3b8');
  var pickTxt=p.pick?(p.sugg_line!=null?('OVER '+p.sugg_line):p.pick):'No pick';
  // ── All-4-markets summary ─────────────────────────────────────────────
  // Strikeouts (this pick) + Hits Allowed / Outs / Earned Runs pulled from the
  // per-name prop index (window.__PP_BY_NAME__) built in renderPitcherProps.
  // Prop rows are clickable → _ppForm for that market's game-by-game log.
  var _nm=String(p.name||'').toLowerCase().trim();
  var _mk=(window.__PP_BY_NAME__||{})[_nm]||{};
  var _adm=!!window.IS_ADMIN;
  function _mkRow(lbl,ln,bl,unit,pk,od,key,clickable,betSrc,betCat,statKey){
    var pc=pk==='OVER'?'#63cab7':(pk==='UNDER'?'#ff8a65':'#64748b');
    var odStr=od!=null?((od>0?'+':'')+od):'';
    var pickStr=pk?(pk+(odStr?(' '+odStr):'')):'\u2014';
    var clk=(clickable&&key)?(' onclick="_ppForm(&#39;'+key+'&#39;)" style="cursor:pointer" title="Game-by-game log"'):'';
    var caret=(clickable&&key)?' <span style="color:#64748b;font-size:.62rem">\u25be</span>':'';
    var betCell='';
    if(_adm){
      var bb=(betSrc&&pk&&ln!=null&&statKey)?_betBtn(betSrc,betCat,pk,statKey,lbl,ln,od):'';
      betCell='<td style="padding:5px 8px;text-align:right;white-space:nowrap">'+bb+'</td>';
    }
    return '<tr'+clk+'><td style="padding:5px 8px;color:#e2e8f0;font-weight:600">'+lbl+caret+'</td>'
      +'<td style="padding:5px 8px;font-family:monospace;color:#fff">'+(ln!=null?ln:'\u2014')+'</td>'
      +'<td style="padding:5px 8px;font-family:monospace;color:#cbd5e1">'+(bl!=null?(bl+(unit?(' '+unit):'')):'\u2014')+'</td>'
      +'<td style="padding:5px 8px;font-weight:800;color:'+pc+'">'+pickStr+'</td>'+betCell+'</tr>';
  }
  var _kHasSugg=p.sugg_line!=null;
  var _kLine=_kHasSugg?p.sugg_line:p.line;
  var _kPick=_kHasSugg?'OVER':p.pick;
  var _kOd=_kHasSugg?p.sugg_odds:(p.pick==='OVER'?p.over_odds:(p.pick==='UNDER'?p.under_odds:null));
  var _kBl=(p.blended_avg_k!=null?p.blended_avg_k:p.avg_k);
  var _kSrc={name:p.name,team:p.team,opp:p.opp};
  var mkBody=_mkRow('Strikeouts',_kLine,_kBl,'K',_kPick,_kOd,'',false,_kSrc,'Pitcher Ks','strikeOuts');
  [['pitcher_hits_allowed','Hits Allowed','hits_allowed','Pitcher Hits Allowed'],['pitcher_outs','Outs','outs','Pitcher Outs'],['pitcher_earned_runs','Earned Runs','earnedRuns','Pitcher Earned Runs'],['pitcher_walks','Walks Allowed','walks','Pitcher Walks Allowed']].forEach(function(mm){
    var e=_mk[mm[0]];
    if(e&&e.obj){ var o=e.obj; var od=o.pick==='OVER'?o.over_odds:(o.pick==='UNDER'?o.under_odds:null);
      mkBody+=_mkRow(mm[1],o.line,o.blended,(o.unit?String(o.unit).trim():''),o.pick,od,e.key,true,o,mm[3],mm[2]);
    } else { mkBody+=_mkRow(mm[1],null,null,'',null,null,'',false,null,'',''); }
  });
  var mkTable='<div style="font-size:.72rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:6px">All 5 Markets</div>'
    +'<table style="width:100%;border-collapse:collapse;font-size:.82rem;margin-bottom:16px;border-bottom:1px solid #1e293b">'
    +'<thead><tr><th style="text-align:left;padding:4px 8px;color:#64748b;font-size:.66rem;font-weight:600">Market</th><th style="text-align:left;padding:4px 8px;color:#64748b;font-size:.66rem;font-weight:600">Line</th><th style="text-align:left;padding:4px 8px;color:#64748b;font-size:.66rem;font-weight:600">Blend</th><th style="text-align:left;padding:4px 8px;color:#64748b;font-size:.66rem;font-weight:600">Pick</th>'+(_adm?'<th style="text-align:right;padding:4px 8px;color:#64748b;font-size:.66rem;font-weight:600">Bet</th>':'')+'</tr></thead>'
    +'<tbody>'+mkBody+'</tbody></table>';
  ov.innerHTML=`<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:440px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">
      <div>
        <div style="font-weight:800;font-size:1.05rem;color:#fff">${p.name}</div>
        <div style="color:#94a3b8;font-size:.78rem">${p.side||''} vs ${p.opp||''} · K Line ${lineTxt}</div>
      </div>
      <button onclick="document.getElementById('pk-modal').style.display='none'" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">✕</button>
    </div>
    <div style="padding:14px 18px">
      ${mkTable}
      <div style="font-size:.72rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:8px">${histTitle}</div>
      <table style="width:100%;border-collapse:collapse;font-size:.85rem">${usingVs?'<thead><tr><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">Date</th><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">IP</th><th style="text-align:right;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">K</th><th style="text-align:right;padding:4px 10px;color:#fca5a5;font-size:.68rem;font-weight:600">H</th><th style="text-align:right;padding:4px 10px;color:#a78bfa;font-size:.68rem;font-weight:600">Outs</th><th style="text-align:right;padding:4px 10px;color:#fb923c;font-size:.68rem;font-weight:600">ER</th></tr></thead>':''}<tbody>${rows}</tbody></table>
      ${recentSection}
      <div style="margin-top:16px;border-top:1px solid #1e293b;padding-top:12px;display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:.82rem">
        <div><span style="color:#64748b">Career vs opp</span><br><span style="color:#e2e8f0;font-weight:600">${careerTxt}</span></div>
        <div><span style="color:#64748b">Hits allowed vs opp</span><br><span style="color:#e2e8f0;font-weight:600">${p.avg_hits!=null?(p.avg_hits+' H avg'):'—'}</span></div>
        <div><span style="color:#64748b">Recent form</span><br><span style="color:#e2e8f0;font-weight:600">${recentTxt}</span></div>
        <div><span style="color:#64748b">Blended (pick driver)</span><br><span style="color:#e2e8f0;font-weight:800">${blendTxt}</span></div>
        <div><span style="color:#64748b">Pick</span><br><span style="color:${pickClr};font-weight:800">${pickTxt}</span></div>
      </div>
      ${p.blend_src?('<div style="margin-top:10px;color:#64748b;font-size:.74rem">'+p.blend_src+'</div>'):''}
    </div>
  </div>`;
  ov.style.display='flex';
}

// ── Pitcher PROP categories (Hits Allowed / Outs / Earned Runs) ────────
// Generic, data-driven mirror of the Pitcher K table. Each market is an
// Over/Under pick built from the blended (career-vs-opp + recent) average vs
// the posted line. Renders into #pitcher-props-wrap; rows open _ppForm.
var PROP_CFG = {
  pitcher_hits_allowed: {label:'Pitcher Hits Allowed', icon:'🎯', color:'#f87171',
    cardId:'prop-hits-card', bodyId:'prop-hits-body', npId:'prop-hits-nopick'},
  pitcher_outs:         {label:'Pitcher Outs',         icon:'🔢', color:'#a78bfa',
    cardId:'prop-outs-card', bodyId:'prop-outs-body', npId:'prop-outs-nopick'},
  pitcher_earned_runs:  {label:'Pitcher Earned Runs',  icon:'🔥', color:'#fb923c',
    cardId:'prop-er-card', bodyId:'prop-er-body', npId:'prop-er-nopick'},
  pitcher_walks:        {label:'Pitcher Walks Allowed', icon:'🚶', color:'#34d399',
    cardId:'prop-bb-card', bodyId:'prop-bb-body', npId:'prop-bb-nopick'},
};
var PROP_ORDER = ['pitcher_hits_allowed','pitcher_outs','pitcher_earned_runs','pitcher_walks'];
function _ppU(p){ return p && p.unit ? (' '+String(p.unit).trim()) : ''; }
function _propBestCard(p, key, rank) {
  var mktColors={pitcher_hits_allowed:'#f87171',pitcher_outs:'#a78bfa',pitcher_earned_runs:'#fb923c'};
  var clr=mktColors[p.market]||'#63cab7';
  var abbr=_mlbTeamAbbr(p.team);
  var teamLogo=abbr?`https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png`:'';
  var isOver=(p.pick||'').toUpperCase()==='OVER';
  var odds=isOver?(p.over_odds!=null?(p.over_odds>0?'+':'')+p.over_odds:'')
                 :(p.under_odds!=null?(p.under_odds>0?'+':'')+p.under_odds:'');
  var gap=p.blended!=null&&p.line!=null?Math.abs(p.blended-p.line):null;
  var gapDisp=gap!=null?'edge +'+gap.toFixed(1)+(p.unit?' '+p.unit:''):'';
  var blendDisp=p.blended!=null?p.blended+(p.unit?' '+p.unit:''):'—';
  var lineDisp=p.line!=null?p.line+(p.unit?' '+p.unit:''):'—';
  var sideLabel=p.side?`<span style="font-size:.62rem;background:rgba(255,255,255,.07);border-radius:4px;padding:1px 5px;color:#94a3b8">${p.homeRoad||p.side}</span>`:'';
  var oppLabel=p.opp?`<span style="font-size:.62rem;color:#64748b">vs ${p.opp}</span>`:'';
  var gapHtml=gapDisp?`<div style="margin-top:4px;font-size:.66rem;color:#fbbf24">${gapDisp}</div>`:'';
  var oddsHtml=odds?`<div style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.85rem;margin-top:2px">${odds}</div>`:'';
  var _propStatKey={pitcher_hits_allowed:'hits_allowed',pitcher_outs:'outs',pitcher_earned_runs:'earnedRuns',pitcher_walks:'walks'}[p.market]||'prop';
  var _propOdds=isOver?(p.over_odds!=null?p.over_odds:null):(p.under_odds!=null?p.under_odds:null);
  var rgbClr=p.market==='pitcher_hits_allowed'?'248,113,113':p.market==='pitcher_outs'?'167,139,250':'251,146,60';
  var _dowMktMap={pitcher_hits_allowed:'hits_allowed',pitcher_outs:'outs',pitcher_earned_runs:'er',pitcher_walks:'walks'};
  var _propDowMkt=_dowMktMap[p.market]||'k';
  window.__PP_REG__=window.__PP_REG__||{}; window.__PP_REG__[key]=p;
  return `<div class="mlb-pick-card" onclick="_ppForm('${key}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,rgba(${rgbClr},.18) 0%,#08111d 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:26px;height:26px;border-radius:50%;background:${clr};color:#000;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.78rem">${rank}</div>
        ${_mlbHead(p.pid)}
        <span style="font-size:.62rem;letter-spacing:.1em;color:${clr};font-weight:800">${String(p.label||p.market||'PROP').toUpperCase()}</span>
      </div>
      ${teamLogo?`<img src="${teamLogo}" alt="${p.team||''}" style="height:30px;width:30px;object-fit:contain" onerror="this.style.display='none'"/>`:''}
    </div>
    <div class="mlb-card-name">${String(p.name||'')}</div>
    <div style="padding:10px 14px;flex:1;display:flex;flex-direction:column">
      <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px">${sideLabel}${oppLabel}</div>
      <div style="display:flex;align-items:center;justify-content:space-between;border-top:1px solid #1f2d3d;padding-top:6px;margin-top:2px">
        <span style="font-size:.7rem;color:#64748b">Line ${lineDisp} · Blend ${blendDisp}</span>
        <span style="color:${isOver?'#63cab7':'#ff8a65'};font-weight:900;font-size:.9rem">${p.pick||'—'}</span>
      </div>
      ${gapHtml}
      ${oddsHtml}
      <div style="margin-top:5px;display:flex;align-items:center;gap:5px"><span style="font-size:.6rem;color:#475569">day trend</span>${_dowChip(_propDowMkt,p.pick)}</div>
    </div>
  ${_betBtn(p,'Pitcher Props',p.pick,_propStatKey,String(p.label||'Prop'),p.line,_propOdds)}
  </div>`;
}
function renderPitcherProps(view){
  var wrap=document.getElementById('pitcher-props-wrap'); if(!wrap) return;
  var props=(view&&view.pitcher_props)||{};
  window.__PP_REG__={}; window.__PP_BY_NAME__={};
  var _ppN=0;
  // Index all entries by name for the _pkForm popup (K + all 4 prop markets).
  PROP_ORDER.forEach(function(_mkt){
    ((props[_mkt]||{}).all||[]).forEach(function(_p){
      var _nm=String(_p.name||'').toLowerCase().trim(); if(!_nm) return;
      var _key='pp'+(_ppN++); window.__PP_REG__[_key]=_p;
      if(!window.__PP_BY_NAME__[_nm]) window.__PP_BY_NAME__[_nm]={};
      var _ex=window.__PP_BY_NAME__[_nm][_mkt];
      if(!_ex||(!_ex.obj.pick&&_p.pick)) window.__PP_BY_NAME__[_nm][_mkt]={obj:_p,key:_key};
    });
  });
  // ── Combined "Best Pitching Props": top 10 by edge across all markets ──
  var bestPicks=[];
  PROP_ORDER.forEach(function(m){
    ((props[m]||{}).picks||[]).forEach(function(p2){
      var g=(p2.blended!=null&&p2.line!=null)?Math.abs(p2.blended-p2.line):0;
      var k='bp'+(++_ppN); window.__PP_REG__[k]=p2;
      bestPicks.push({p:p2,key:k,gap:g});
    });
  });
  bestPicks.sort(function(a,b){return b.gap-a.gap;});
  var combinedHtml='';
  if(bestPicks.length){
    var bp10=bestPicks.slice(0,10), bpRest=bestPicks.slice(10);
    var bpTop=bp10.map(function(x,i){return _propBestCard(x.p,x.key,i+1);}).join('');
    var bpMore='';
    if(bpRest.length){
      var bpRestCards=bpRest.map(function(x,i){ var k2='bpx'+(++_ppN); window.__PP_REG__[k2]=x.p; return _propBestCard(x.p,k2,11+i); }).join('');
      bpMore='<details style="margin-top:12px"><summary class="more-btn" style="color:#94a3b8;border-color:#94a3b833">&#9655; '+bpRest.length+' more Pitching Props</summary><div class="mlb-picks-grid mt-3">'+bpRestCards+'</div></details>';
    }
    combinedHtml='<div class="section-card" style="margin-top:20px;padding:20px;background:linear-gradient(180deg,#0f1924 0%,#0a1118 100%);border-radius:16px;border:1px solid #1e293b">'
      +'<div style="font-size:1.05rem;font-weight:800;color:#f8fafc;margin-bottom:14px">&#127919; Best Pitching Props <span style="font-size:.72rem;font-weight:400;color:#64748b">top picks by edge across all markets</span></div>'
      +'<div class="mlb-picks-grid">'+bpTop+'</div>'
      +bpMore
      +'</div>';
  }
  // ── Per-market pulldowns: top 10 + overflow button each ──
  var perMktHtml='';
  PROP_ORDER.forEach(function(m){
    var cfg=PROP_CFG[m];
    var allPicks=((props[m]||{}).picks)||[];
    if(!allPicks.length) return;
    var pm10=allPicks.slice(0,10), pmRest=allPicks.slice(10);
    var pmCards=pm10.map(function(p2,i){ var k='pm'+(++_ppN); window.__PP_REG__[k]=p2; return _propBestCard(p2,k,i+1); }).join('');
    var pmMore='';
    if(pmRest.length){
      var pmRestCards=pmRest.map(function(p2,i){ var k='pmx'+(++_ppN); window.__PP_REG__[k]=p2; return _propBestCard(p2,k,11+i); }).join('');
      pmMore='<details style="margin-top:10px"><summary class="more-btn" style="color:'+cfg.color+';border-color:'+cfg.color+'33">&#9655; '+pmRest.length+' more '+cfg.label.replace('Pitcher ','')+'</summary><div class="mlb-picks-grid mt-3">'+pmRestCards+'</div></details>';
    }
    perMktHtml+='<details style="margin-top:14px">'
      +'<summary style="cursor:pointer;list-style:none;-webkit-appearance:none;display:flex;align-items:center;gap:8px;padding:13px 18px;background:linear-gradient(180deg,#0f1924 0%,#0a1118 100%);border-radius:14px;border:1px solid #1e293b;outline:none">'
      +'<span style="font-size:1rem;font-weight:800;color:'+cfg.color+'">'+cfg.icon+' '+cfg.label.replace('Pitcher ','')+'</span>'
      +'<span style="font-size:.72rem;font-weight:600;color:#94a3b8;background:rgba(255,255,255,.07);border-radius:10px;padding:2px 8px">'+allPicks.length+'</span>'
      +'<span style="margin-left:auto;color:#64748b;font-size:.85rem">&#9660;</span>'
      +'</summary>'
      +'<div style="padding:0 4px"><div class="mlb-picks-grid" style="margin-top:12px">'+pmCards+'</div>'+pmMore+'</div>'
      +'</details>';
  });
  wrap.innerHTML=combinedHtml+perMktHtml;
}
// Generic prop recent-form popup (mirrors _pkForm, market-agnostic).
function _ppForm(key){
  var p=(key&&typeof key==='object')?key:(window.__PP_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('pp-modal');
  if(!ov){
    ov=document.createElement('div'); ov.id='pp-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var line=p.line, u=_ppU(p);
  var unitW=(p.unit?String(p.unit).trim():'');
  var vlog=p.vs_opp_log||[]; var usingVs=vlog.length>0;
  var log=usingVs?vlog:(p.recent_log||[]);
  function valColor(v){ return (line!=null&&v!=null)?(v>line?'#63cab7':'#ff8a65'):'#e2e8f0'; }
  var rows=log.length?log.map(function(g){
    var v=g.v; var clr=valColor(v);
    var oppCell=usingVs?'':'<td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">'+(g.opp?('vs '+g.opp):'')+'</td>';
    return '<tr>'
      +'<td style="padding:6px 10px;color:#94a3b8;font-family:monospace">'+(g.d||'\u2014')+'</td>'
      +oppCell
      +'<td style="padding:6px 10px;color:#93c5fd;font-family:monospace;font-size:.8rem">'+(g.ip?(g.ip+' IP'):'')+'</td>'
      +'<td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:'+clr+'">'+(v!=null?v:'\u2014')+(v!=null?' '+unitW:'')+'</td>'
    +'</tr>';
  }).join(''):'<tr><td colspan="4" style="padding:14px;color:#64748b;text-align:center">No starts on record</td></tr>';
  var rlog=p.recent_log||[];
  var recentRows=rlog.length?rlog.map(function(g){
    var v=g.v; var clr=valColor(v);
    return '<tr>'
      +'<td style="padding:6px 10px;color:#94a3b8;font-family:monospace">'+(g.d||'\u2014')+'</td>'
      +'<td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">'+(g.opp?('vs '+g.opp):'')+'</td>'
      +'<td style="padding:6px 10px;color:#93c5fd;font-family:monospace;font-size:.8rem">'+(g.ip?(g.ip+' IP'):'')+'</td>'
      +'<td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:'+clr+'">'+(v!=null?v:'\u2014')+(v!=null?' '+unitW:'')+'</td>'
    +'</tr>';
  }).join(''):'';
  var recentSection=(usingVs&&recentRows)?'<div style="margin-top:18px;font-size:.72rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:8px">Last '+rlog.length+' Starts (any opp)</div>'
    +'<table style="width:100%;border-collapse:collapse;font-size:.85rem"><thead><tr><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">Date</th><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">Opp</th><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">IP</th><th style="text-align:right;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">'+(unitW||'Val')+'</th></tr></thead><tbody>'+recentRows+'</tbody></table>':'';
  var histTitle=usingVs?('Starts vs '+(p.opp||'opp')):('Last '+(log.length||0)+' Starts (any opp)');
  var careerTxt=p.career_avg!=null?(p.career_avg+u+' \u00b7 '+(p.starts||0)+' starts vs '+(p.opp||'opp')):'no career vs opp';
  var recentTxt=p.recent_avg!=null?(p.recent_avg+u+' \u00b7 last '+(p.recent_starts||0)):'no recent data';
  var blendTxt=p.blended!=null?(p.blended+u):'\u2014';
  var lineTxt=line!=null?(line+(unitW?(' '+unitW):'')):'no line';
  var pickClr=p.pick==='OVER'?'#63cab7':(p.pick==='UNDER'?'#ff8a65':'#94a3b8');
  var pickTxt=p.pick||'No pick';
  var oddsTxt=(p.pick==='OVER'?p.over_odds:(p.pick==='UNDER'?p.under_odds:null));
  oddsTxt=oddsTxt!=null?((oddsTxt>0?'+':'')+oddsTxt):'';
  ov.innerHTML='<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:440px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">'
      +'<div><div style="font-weight:800;font-size:1.05rem;color:#fff">'+p.name+'</div>'
      +'<div style="color:#94a3b8;font-size:.78rem">'+(p.side||'')+' vs '+(p.opp||'')+' \u00b7 '+(p.label||'')+' Line '+lineTxt+'</div></div>'
      +'<button onclick="document.getElementById(&#39;pp-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">\u2715</button>'
    +'</div>'
    +'<div style="padding:14px 18px">'
      +'<div style="font-size:.72rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:8px">'+histTitle+'</div>'
      +'<table style="width:100%;border-collapse:collapse;font-size:.85rem">'+(usingVs?'<thead><tr><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">Date</th><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">IP</th><th style="text-align:right;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">'+(unitW||'Val')+'</th></tr></thead>':'')+'<tbody>'+rows+'</tbody></table>'
      +recentSection
      +'<div style="margin-top:16px;border-top:1px solid #1e293b;padding-top:12px;display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:.82rem">'
        +'<div><span style="color:#64748b">Career vs opp</span><br><span style="color:#e2e8f0;font-weight:600">'+careerTxt+'</span></div>'
        +'<div><span style="color:#64748b">Recent form</span><br><span style="color:#e2e8f0;font-weight:600">'+recentTxt+'</span></div>'
        +'<div><span style="color:#64748b">Blended (pick driver)</span><br><span style="color:#e2e8f0;font-weight:800">'+blendTxt+'</span></div>'
        +'<div><span style="color:#64748b">Pick</span><br><span style="color:'+pickClr+';font-weight:800">'+pickTxt+' '+oddsTxt+'</span></div>'
      +'</div>'
      +(p.blend_src?('<div style="margin-top:10px;color:#64748b;font-size:.74rem">'+p.blend_src+'</div>'):'')
    +'</div>'
  +'</div>';
  ov.style.display='flex';
}

// ── Hitter recent-form popup (last 5 games) — mirrors _pkForm ───────────
// Works for both "to record a hit" cards (over 0.5) and Under 1.5 picks.
// Detect under picks via under_score; color each game green/red vs the goal.
function _hitForm(key){
  var p=(key&&typeof key==='object')?key:(window.__HIT_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('hit-modal');
  if(!ov){
    ov=document.createElement('div');
    ov.id='hit-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var log=p.recent_hit_log||[];
  var isUnder=(p.under_score!=null);
  var goal=isUnder?'Under 1.5 hits':'To record a hit';
  var rows=log.length?log.map(function(g){
    var good=isUnder?(g.h<=1):(g.h>=1);
    var clr=good?'#63cab7':'#ff8a65';
    var oppTxt=g.opp?((g.ha==='H'?'vs ':'@ ')+g.opp):'';
    return `<tr>
      <td style="padding:6px 10px;color:#94a3b8;font-family:monospace">${g.d||'—'}</td>
      <td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">${oppTxt}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-size:.8rem;color:#93c5fd">${g.tb} TB</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:${clr}">${g.h} H</td>
    </tr>`;
  }).join(''):'<tr><td colspan="4" style="padding:14px;color:#64748b;text-align:center">No recent games on record</td></tr>';
  var name=p.full_name||p.name||'';
  var pickClr=isUnder?'#ff8a65':'#63cab7';
  ov.innerHTML=`<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:440px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">
      <div>
        <div style="font-weight:800;font-size:1.05rem;color:#fff">${name}</div>
        <div style="color:#94a3b8;font-size:.78rem">${p.side||''} vs ${p.opp||''} · ${goal}</div>
      </div>
      <button onclick="document.getElementById('hit-modal').style.display='none'" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">✕</button>
    </div>
    <div style="padding:14px 18px">
      <div style="font-size:.72rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:8px">Last ${log.length||0} Games</div>
      <table style="width:100%;border-collapse:collapse;font-size:.85rem"><tbody>${rows}</tbody></table>
      <div style="margin-top:12px;border-top:1px solid #1e293b;padding-top:10px;color:${pickClr};font-weight:800;font-size:.85rem">Pick: ${goal}</div>
    </div>
  </div>`;
  ov.style.display='flex';
}

// ── Runs recent-form popup (last 5 games) — mirrors _hitForm ───────────
// Works for both OVER (score a run) and UNDER (no run) picks. Color each game
// green/red vs the goal: scored→good for OVER, didn't score→good for UNDER.
function _runsForm(key){
  var p=(key&&typeof key==='object')?key:(window.__RUNS_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('runs-modal');
  if(!ov){
    ov=document.createElement('div');
    ov.id='runs-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var log=p.recent_runs_log||[];
  var isOver=(p.pick==='OVER');
  var goal=isOver?'Over 0.5 runs (score a run)':'Under 0.5 runs (no run)';
  var rows=log.length?log.map(function(g){
    var scored=g.r>=1;
    var good=isOver?scored:!scored;
    var clr=good?'#63cab7':'#ff8a65';
    var oppTxt=g.opp?((g.ha==='H'?'vs ':'@ ')+g.opp):'';
    return `<tr>
      <td style="padding:6px 10px;color:#94a3b8;font-family:monospace">${g.d||'—'}</td>
      <td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">${oppTxt}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-size:.8rem;color:#93c5fd">${g.h} H</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:${clr}">${g.r} R</td>
    </tr>`;
  }).join(''):'<tr><td colspan="4" style="padding:14px;color:#64748b;text-align:center">No recent games on record</td></tr>';
  var name=p.full_name||p.name||'';
  var pickClr=isOver?'#63cab7':'#ff8a65';
  ov.innerHTML=`<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:440px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">
      <div>
        <div style="font-weight:800;font-size:1.05rem;color:#fff">${name}</div>
        <div style="color:#94a3b8;font-size:.78rem">${p.side||''} vs ${p.opp||''} · ${goal}</div>
      </div>
      <button onclick="document.getElementById('runs-modal').style.display='none'" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">✕</button>
    </div>
    <div style="padding:14px 18px">
      <div style="font-size:.72rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:8px">Runs Rate ${p.rate_disp||''} · Last ${log.length||0} Games</div>
      <table style="width:100%;border-collapse:collapse;font-size:.85rem"><tbody>${rows}</tbody></table>
      <div style="margin-top:12px;border-top:1px solid #1e293b;padding-top:10px;color:${pickClr};font-weight:800;font-size:.85rem">Pick: ${goal}</div>
    </div>
  </div>`;
  ov.style.display='flex';
}

// ── Universal clickable-name dispatcher ────────────────────────────────
// Lets ANY player name on the page (parlay legs, player-search results,
// all-plays-by-game) open the right recent-form popup — pitchers → _pkForm,
// hitters/unders → _hitForm. Each name registers its source pick object
// under a unique key; _playerForm routes by object shape (pitchers carry
// recent_k_log / avg_k). Keys never collide so cross-surface clicks survive
// independent re-renders. The pick sections themselves already use their own
// __HIT_REG__/__PK_REG__ registries.
window.__NAME_REG__=window.__NAME_REG__||{}; window.__NK__=window.__NK__||0;
function _esc(s){ return String(s==null?'':s).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];}); }
function _nameReg(obj){ if(!obj) return ''; var k='nm'+(++window.__NK__); window.__NAME_REG__[k]=obj; return k; }
function _nameSpan(obj,label){
  var k=_nameReg(obj);
  if(!k) return _esc(label);
  return `<span onclick="event.stopPropagation();_playerForm('${k}')" style="cursor:pointer;text-decoration:underline;text-decoration-style:dotted;text-underline-offset:2px" title="Click for recent form">${_esc(label)}</span>`;
}
function _playerForm(key){
  var p=(window.__NAME_REG__||{})[key]; if(!p) return;
  if(p._prop){ _ppForm(p); }
  else if(p.recent_k_log!==undefined || p.avg_k!==undefined){ _pkForm(p); }
  else if(p.recent_tb_log!==undefined){ _tbForm(p); }
  else if(p.recent_rbi_log!==undefined && p.recent_hit_log===undefined && p.recent_runs_log===undefined){ _rbiForm(p); }
  else if(p.recent_runs_log!==undefined && p.recent_hit_log===undefined){ _runsForm(p); }
  else { _hitForm(p); }
}

// ── CSV export (all picks → spreadsheet/betting tools) ──────────────────
// Runs entirely in the browser off picks already loaded — no server call,
// no Render memory, no Odds API cost.
function _csvCell(v){
  if(v==null) return '';
  var s=String(v);
  if(/[",]/.test(s)||s.indexOf(String.fromCharCode(10))>=0||s.indexOf(String.fromCharCode(13))>=0) s='"'+s.replace(/"/g,'""')+'"';
  return s;
}
function _csvOdds(v){ return v==null?'':((v>0?'+':'')+v); }
function _csvLineup(s){
  if(s==='IN_LINEUP') return 'In Lineup';
  if(s==='NOT_IN_LINEUP') return 'Not in Lineup';
  return s||'TBD';
}
function downloadPicksCSV(){
  var r=window._lastResult;
  if(!r){ alert('Run picks first, then download.'); return; }
  var date=r.date||'';
  var rows=[['Category','Rank','Player','Team','Pos','Side','Opponent','Pitcher','Pick','Line','Odds','Lineup','Detail']];
  (r.top9||[]).forEach(function(p,i){
    rows.push(['Top Pick', i+1, p.full_name||p.name||'', p.team||'', p.pos||'', p.side||'', p.opp||'', p.pitcher||'',
      'To Record a Hit (Over 0.5)', '0.5', _csvOdds(p.hit_odds), _csvLineup(p.lineup_status), '']);
  });
  (r.also_ran||[]).forEach(function(p,i){
    rows.push(['Money Ball', i+1, p.full_name||p.name||'', p.team||'', p.pos||'', p.side||'', p.opp||'', p.pitcher||'',
      'To Record a Hit (Over 0.5)', '0.5', _csvOdds(p.hit_odds), _csvLineup(p.lineup_status), '']);
  });
  (r.under_picks||[]).forEach(function(p,i){
    var detail=(p.tb_under_odds!=null?('TB U1.5 '+_csvOdds(p.tb_under_odds)):'');
    rows.push(['Under Pick', i+1, p.name||'', p.team||'', '', p.side||'', p.opp||'', p.pitcher||'',
      'Under 1.5 Hits', '1.5', _csvOdds(p.under_odds), _csvLineup(p.lineup_status), detail]);
  });
  (r.rbi_picks||[]).forEach(function(p,i){
    var isOver=p.pick==='OVER';
    var od=isOver?p.over_odds:p.under_odds;
    rows.push(['RBI Pick', i+1, p.name||'', p.team||'', '', p.side||'', p.opp||'', '',
      (isOver?'Over':'Under')+' '+(p.line!=null?p.line:0.5)+' RBI', (p.line!=null?p.line:0.5), _csvOdds(od), '', (p.rate_disp||'')+(p.basis?(' '+p.basis):'')]);
  });
  (r.runs_picks||[]).forEach(function(p,i){
    var isOver=p.pick==='OVER';
    var od=isOver?p.over_odds:p.under_odds;
    rows.push(['Runs Pick', i+1, p.name||'', p.team||'', '', p.side||'', p.opp||'', '',
      (isOver?'Over':'Under')+' '+(p.line!=null?p.line:0.5)+' Runs', (p.line!=null?p.line:0.5), _csvOdds(od), '', (p.rate_disp||'')+(p.basis?(' '+p.basis):'')]);
  });
  var pk=(r.pitcher_k&&r.pitcher_k.all)||[];
  pk.filter(function(p){return p.pick && (p.starts||0)>0;}).sort(function(a,b){
    var ga=Math.abs((a.avg_k||0)-(a.line||0)), gb=Math.abs((b.avg_k||0)-(b.line||0));
    return gb-ga;
  }).forEach(function(p,i){
    var hasSugg=p.sugg_line!=null;
    var line=hasSugg?p.sugg_line:p.line;
    var pick=hasSugg?('OVER '+p.sugg_line+' Ks'):(p.pick+' '+(p.line!=null?p.line:'')+' Ks');
    var odds=hasSugg?p.sugg_odds:(p.pick==='OVER'?p.over_odds:p.under_odds);
    var detail='Avg '+(p.avg_k!=null?p.avg_k+'K':'—')+(p.era?(', ERA '+p.era):'');
    rows.push(['Pitcher K', i+1, p.name||'', '', 'P', p.side||'', p.opp||'', '',
      pick, (line!=null?line:''), _csvOdds(odds), '', detail]);
  });
  var _ppAll=(r.pitcher_props)||{};
  PROP_ORDER.forEach(function(mkt){
    var cfg=PROP_CFG[mkt]; var picks=((_ppAll[mkt]||{}).picks)||[];
    var statLbl=(cfg.label||'').replace('Pitcher ','');
    picks.forEach(function(p,i){
      var isOver=p.pick==='OVER';
      var od=isOver?p.over_odds:p.under_odds;
      var detail='Blend '+(p.blended!=null?(p.blended+_ppU(p)):'—')+(p.hit_rate?(', vr opp '+p.hit_rate):'');
      rows.push(['Pitcher '+statLbl, i+1, p.name||'', '', 'P', p.side||'', p.opp||'', '',
        p.pick+' '+(p.line!=null?p.line:'')+' '+statLbl, (p.line!=null?p.line:''), _csvOdds(od), '', detail]);
    });
  });
  if(rows.length<=1){ alert('No picks to download yet.'); return; }
  var csv=rows.map(function(row){return row.map(_csvCell).join(',');}).join(String.fromCharCode(13)+String.fromCharCode(10));
  var blob=new Blob([String.fromCharCode(65279)+csv],{type:'text/csv;charset=utf-8;'});
  var url=URL.createObjectURL(blob);
  var a=document.createElement('a');
  a.href=url; a.download='mlb-picks-'+(date||'today')+'.csv';
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ── Auto Parlay Builder (admin) — pulls from hits, Under 1.5's, Pitcher K's ──
function _amToDec(am){ if(am==null||am==='') return null; var a=parseFloat(am); if(isNaN(a)||a===0) return null; return a>0 ? 1+a/100 : 1+100/Math.abs(a); }
function _decToAm(d){ if(!d||d<=1) return null; return d>=2 ? '+'+Math.round((d-1)*100) : '-'+Math.round(100/(d-1)); }
function _fmtOdds(o){ if(o==null||o==='') return null; var a=parseFloat(o); if(isNaN(a)) return null; return (a>0?'+':'')+a; }
function _shuffleP(a){ for(var i=a.length-1;i>0;i--){var j=Math.floor(Math.random()*(i+1));var t=a[i];a[i]=a[j];a[j]=t;} return a; }
function _legScoreP(c){ return (c.hasOdds?1:0)*1e9 + (c.conf||0)*1e4 + (c.dec?Math.min(c.dec,11)*100:0); }
// Under 1.5 legs only qualify for the parlay at -500 or better (no -1000-type juice).
// Applies to Under 1.5 legs ONLY — hit-to-record and Pitcher K legs are unfiltered.
function _underOk(am){ if(am==null||am==='') return false; var a=parseFloat(am); if(isNaN(a)||a===0) return false; return a>=-500; }
function _mlbPool(){
  var r=window._lastResult; if(!r) return [];
  r=_filterStarted(r);  // re-check the clock on every build so games that started after
                        // the page loaded also fall out of the parlay pool.
  function clampConf(base,idx){ var c=base-idx*3; return c<40?40:c; }
  var cands=[];
  (r.top9||[]).forEach(function(p,i){
    cands.push({type:'HIT',dir:'OVER',player:(p.full_name||p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Hits',line:0.5,odds:(p.hit_odds!=null?p.hit_odds:''),conf:clampConf(95,i),reason:'🎯 To record a hit vs '+(p.opp||''),src:p});
  });
  (r.also_ran||[]).forEach(function(p,i){
    cands.push({type:'HIT',dir:'OVER',player:(p.full_name||p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Hits',line:0.5,odds:(p.hit_odds!=null?p.hit_odds:''),conf:clampConf(82,i),reason:'🎯 To record a hit vs '+(p.opp||''),src:p});
  });
  (r.under_picks||[]).forEach(function(p,i){
    // Under 1.5 HITS leg — only when priced at -500 or better (drops -1000-type juice).
    if(_underOk(p.under_odds)){
      cands.push({type:'UNDER',dir:'UNDER',player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Hits',line:1.5,odds:p.under_odds,conf:clampConf(90,i),reason:'⬇️ Under 1.5 hits'+(p.under_score!=null?(' · score '+p.under_score):'')+' vs '+(p.opp||''),src:p});
    }
    // Under 1.5 TOTAL BASES leg — its own candidate now that odds are posted, same -500 floor.
    if(_underOk(p.tb_under_odds)){
      cands.push({type:'UNDER',dir:'UNDER',player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Total Bases',line:1.5,odds:p.tb_under_odds,conf:clampConf(90,i),reason:'⬇️ Under 1.5 total bases'+(p.under_score!=null?(' · score '+p.under_score):'')+' vs '+(p.opp||''),src:p});
    }
  });
  var pk=(r.pitcher_k&&r.pitcher_k.all)||[];
  pk.filter(function(p){return p.pick && (p.starts||0)>0;}).sort(function(a,b){var ga=Math.abs((a.avg_k||0)-(a.line||0)),gb=Math.abs((b.avg_k||0)-(b.line||0));return gb-ga;}).forEach(function(p,i){
    var hasSugg=(p.sugg_line!=null);
    var dir=hasSugg?'OVER':p.pick;
    var line=hasSugg?p.sugg_line:p.line;
    var odds=hasSugg?p.sugg_odds:(p.pick==='OVER'?p.over_odds:p.under_odds);
    cands.push({type:'K',dir:dir,player:(p.name||''),team:'',opp:(p.opp||''),stat:'Ks',line:line,odds:(odds!=null?odds:''),conf:clampConf(90,i),reason:'⚾ '+dir+' '+(line!=null?line:'')+' Ks · avg '+(p.avg_k!=null?p.avg_k+'K':'—')+(p.era?(' · ERA '+p.era):''),src:p});
  });
  (r.rbi_picks||[]).forEach(function(p,i){
    var isOver=p.pick==='OVER';
    var od=isOver?p.over_odds:p.under_odds;
    cands.push({type:'RBI',dir:p.pick,player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'RBI',line:(p.line!=null?p.line:0.5),odds:(od!=null?od:''),conf:clampConf(80,i),reason:'💥 '+p.pick+' '+(p.line!=null?p.line:0.5)+' RBI · '+(p.rate_disp||'')+' vs '+(p.opp||''),src:p});
  });
  (r.runs_picks||[]).forEach(function(p,i){
    var isOver=p.pick==='OVER';
    var od=isOver?p.over_odds:p.under_odds;
    cands.push({type:'RUN',dir:p.pick,player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Runs',line:(p.line!=null?p.line:0.5),odds:(od!=null?od:''),conf:clampConf(80,i),reason:'🏃 '+p.pick+' '+(p.line!=null?p.line:0.5)+' runs · '+(p.rate_disp||'')+' vs '+(p.opp||''),src:p});
  });
  (r.tb_picks||[]).forEach(function(p,i){
    if(_underOk(p.tb_under_odds)){
      cands.push({type:'TB',dir:'UNDER',player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Total Bases',line:1.5,odds:p.tb_under_odds,conf:clampConf(88,i),reason:'⬇️ Under 1.5 TB · '+(p.rate_disp||'')+' rate vs '+(p.opp||''),src:p});
    }
  });
  // Pitcher prop legs (Hits Allowed / Outs / Earned Runs) — one type per market.
  var _pp=(r.pitcher_props)||{};
  PROP_ORDER.forEach(function(mkt){
    var cfg=PROP_CFG[mkt]; var picks=((_pp[mkt]||{}).picks)||[];
    picks.forEach(function(p,i){
      var isOver=p.pick==='OVER';
      var od=isOver?p.over_odds:p.under_odds;
      var statLbl=(cfg.label||'').replace('Pitcher ','');
      cands.push({type:mkt,dir:p.pick,player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:statLbl,line:(p.line!=null?p.line:0),odds:(od!=null?od:''),conf:clampConf(85,i),reason:cfg.icon+' '+p.pick+' '+(p.line!=null?p.line:'')+' '+statLbl+' · blend '+(p.blended!=null?(p.blended+_ppU(p)):'—')+' vs '+(p.opp||''),src:p});
    });
  });
  cands.forEach(function(c){ c.dec=_amToDec(c.odds); c.hasOdds=!!c.dec; });
  // NO N/A LEGS: every parlay leg must be priced. Drops any leg with missing odds
  // (HIT legs with no hit_odds, K legs with no odds). Under legs already required odds.
  // To convert back, remove this filter line.
  cands=cands.filter(function(c){ return c.hasOdds; });
  // Parlay-builder "Unders only" option — keep only UNDER legs (Under 1.5 hits/TB + pitcher K Unders)
  if(window.PARLAY_UNDERS){ cands=cands.filter(function(c){ return c.dir==='UNDER'; }); }
  if(window.PARLAY_OVERS){ cands=cands.filter(function(c){ return c.dir==='OVER'; }); }
  // Parlay-builder "− Odds Only" / "+ Odds Only" — restrict to favorites (American odds < 0)
  // or plus-money (> 0). Independent of Overs/Unders; mutually exclusive with each other.
  if(window.PARLAY_MINUS){ cands=cands.filter(function(c){ return Number(c.odds)<0; }); }
  if(window.PARLAY_PLUS){ cands=cands.filter(function(c){ return Number(c.odds)>0; }); }
  // Parlay-builder category checkboxes — keep only legs whose category is checked.
  if(window.PARLAY_CATS){ cands=cands.filter(function(c){ return window.PARLAY_CATS[_legCat(c)]!==false; }); }
  // Parlay-builder game checkboxes — keep only legs whose game is checked. Uses the same
  // gameKey() label as the "By Game" card so every leg type (hit/under/K/run/prop) maps
  // consistently. A game is dropped only when explicitly unchecked (===false).
  if(window.PARLAY_GAMES){ cands=cands.filter(function(c){ return window.PARLAY_GAMES[gameKey(c.src||c)]!==false; }); }
  // Dedupe per player+market (was per player only). One pitcher can now supply a
  // K leg AND separate Hits Allowed / Outs / Earned Runs legs — and a hitter can
  // supply a Hits leg + a Total Bases leg — so the new prop categories actually
  // deepen the parlay pool instead of being collapsed into a single leg.
  var byKey={};
  cands.forEach(function(c){ if(!c.player) return; var k=c.player+'|'+c.type+'|'+c.stat; var cur=byKey[k]; if(!cur||_legScoreP(c)>_legScoreP(cur)) byKey[k]=c; });
  return Object.keys(byKey).map(function(k){return byKey[k];}).sort(function(a,b){return _legScoreP(b)-_legScoreP(a);});
}
function closeParlay(){ var o=document.getElementById('parlayResult'); if(o) o.innerHTML=''; }
function buildParlay(){ _renderParlay(false); }
function generateParlay(){ _renderParlay(true); }
function _renderParlay(randomize){
  var sel=document.getElementById('parlayLegs');
  var n=parseInt(sel?sel.value:'3',10)||3;
  var out=document.getElementById('parlayResult'); if(!out) return;
  if(!window._lastResult){ out.innerHTML='<div style="color:#888;padding:10px">Run picks first, then build a parlay.</div>'; return; }
  var _anyCat=false; for(var _ck in window.PARLAY_CATS){ if(window.PARLAY_CATS[_ck]){ _anyCat=true; break; } }
  if(!_anyCat){ out.innerHTML='<div style="color:#f87171;padding:10px">Pick at least one category from the Categories menu.</div>'; return; }
  var cands=_mlbPool();
  if(cands.length<n){ out.innerHTML='<div style="color:#f87171;padding:10px">Only '+cands.length+' qualifying play'+(cands.length!==1?'s':'')+' on the board. Pick a smaller parlay.</div>'; return; }
  var legs;
  if(randomize){
    var pool=cands.slice();
    // FRESH LIST: for parlays of 5 legs or fewer, exclude the players from the parlay
    // currently shown so back-to-back "Generate New" draws don't repeat players. Parlays
    // of 6+ are exempt (pool too small). Falls back to the full pool if excluding would
    // leave too few to fill the parlay. To revert, delete this block.
    if(n<=5 && window._lastParlayPlayers && window._lastParlayPlayers.length){
      var avoid=window._lastParlayPlayers;
      var fresh=pool.filter(function(c){return avoid.indexOf(c.player)===-1;});
      if(fresh.length>=n) pool=fresh;
    }
    legs=_shuffleP(pool).slice(0,n).sort(function(a,b){return _legScoreP(b)-_legScoreP(a);});
  } else {
    legs=cands.slice(0,n);
  }
  window._lastParlayPlayers=legs.map(function(l){return l.player;});
  window._parlayLegs=legs;
  window._parlayMode=randomize?'RANDOM MIX':'TOP PLAYS';
  _paintParlay();
}
// Paints the parlay ticket from window._parlayLegs. Split out of _renderParlay so a
// single-leg Replace (_replaceParlayLeg) can repaint without regenerating the slate.
function _paintParlay(){
  var out=document.getElementById('parlayResult'); if(!out) return;
  var legs=window._parlayLegs||[]; var n=legs.length;
  var dec=1, priced=0, missing=0;
  legs.forEach(function(l){ if(l.dec){dec*=l.dec;priced++;}else{missing++;} });
  var am = priced? _decToAm(dec) : null;
  var payout = priced? (100*dec) : null;
  var dirColor=function(d){return d==='OVER'?'#63cab7':d==='UNDER'?'#ff8a65':'#9ca3af';};
  var tagBg={HIT:'rgba(245,158,11,.16)',UNDER:'rgba(255,138,101,.16)',K:'rgba(99,202,183,.16)',RUN:'rgba(96,165,250,.16)',pitcher_hits_allowed:'rgba(248,113,113,.16)',pitcher_outs:'rgba(167,139,250,.16)',pitcher_earned_runs:'rgba(251,146,60,.16)',pitcher_walks:'rgba(52,211,153,.16)'};
  var tagFg={HIT:'#f59e0b',UNDER:'#ff8a65',K:'#63cab7',RUN:'#60a5fa',pitcher_hits_allowed:'#f87171',pitcher_outs:'#a78bfa',pitcher_earned_runs:'#fb923c',pitcher_walks:'#34d399'};
  var tagLbl={HIT:'HIT',UNDER:'UNDER 1.5',K:'PITCHER K',RUN:'RUNS',pitcher_hits_allowed:'HITS ALLOWED',pitcher_outs:'OUTS',pitcher_earned_runs:'EARNED RUNS',pitcher_walks:'WALKS ALLOWED'};
  var rows=legs.map(function(l,idx){var fo=_fmtOdds(l.odds);return '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid #1a1a1a">'
    +'<div style="min-width:0">'
    +'<div style="font-weight:800;color:#fff;font-size:.85rem">'+(idx+1)+'. '+_nameSpan(l.src,l.player)+' <span style="color:#777;font-size:.7rem">'+(l.team?l.team+' ':'')+'vs '+l.opp+'</span> <span style="background:'+(tagBg[l.type]||'#222')+';color:'+(tagFg[l.type]||'#aaa')+';padding:1px 6px;border-radius:4px;font-size:.6rem;font-weight:800">'+(tagLbl[l.type]||l.type)+'</span></div>'
    +'<div style="color:#999;font-size:.72rem;margin-top:2px">'+l.reason+'</div>'
    +'</div>'
    +'<div style="display:flex;align-items:center;gap:8px;white-space:nowrap">'
    +'<div style="text-align:right">'
    +'<div style="color:'+dirColor(l.dir)+';font-weight:900;font-size:.8rem">'+l.dir+' '+l.stat+'</div>'
    +'<div style="color:#fbbf24;font-size:.72rem;font-weight:800">'+(fo||'odds N/A')+'</div>'
    +'</div>'
    +'<button id="mlbrep'+idx+'" onclick="event.stopPropagation();_replaceParlayLeg('+idx+')" title="Swap this leg for another play" style="background:#1e3a8a;color:#bfdbfe;border:1px solid #1d4ed8;border-radius:7px;padding:4px 9px;font-size:.85rem;cursor:pointer;font-weight:800;line-height:1;flex-shrink:0">&#8635;</button>'
    +'</div></div>';}).join('');
  var header='<div style="display:flex;justify-content:space-between;align-items:center;padding:8px 12px;border-bottom:1px solid #262626;background:#121212">'
    +'<span style="font-weight:800;color:#ccc;font-size:.74rem">'+(window._parlayMode||'TOP PLAYS')+'</span>'
    +'<span onclick="closeParlay()" title="Close" style="cursor:pointer;color:#888;font-weight:900;font-size:1.15rem;line-height:1;padding:0 6px">×</span></div>';
  var logBtn=window.IS_ADMIN?('<button class="admin-only" onclick="_parlayBetForm()" style="margin-top:7px;background:rgba(67,56,202,.18);border:1px solid rgba(129,140,248,.55);color:#c7d2fe;border-radius:7px;padding:5px 11px;font-size:.72rem;font-weight:800;cursor:pointer;display:block">&#128221; Log This Parlay</button>'):'';
  var summary='<div style="display:flex;justify-content:space-between;align-items:center;padding:12px;background:linear-gradient(135deg,rgba(245,158,11,.12),rgba(245,158,11,.02));border-top:1px solid #262626">'
    +'<div><div style="font-weight:900;color:#f59e0b">'+n+'-LEG PARLAY</div>'+logBtn+'</div>'
    +'<div style="text-align:right">'+(am?('<div style="font-weight:900;color:#63cab7;font-size:1.05rem">'+am+'</div><div style="color:#999;font-size:.7rem">$100 → $'+payout.toFixed(2)+(missing?(' · '+priced+'/'+n+' legs priced'):'')+'</div>'):('<div style="color:#888;font-size:.78rem">No book odds available for these legs</div>'))+'</div>'
    +'</div>';
  out.innerHTML='<div style="background:#0e0e0e;border:1px solid #262626;border-radius:12px;overflow:hidden">'+header+rows+summary+'</div>';
}
// Swap ONE leg for a different qualifying play, keeping every other leg in place.
// Candidate pool = _mlbPool() (respects all current parlay filters), minus the legs
// already on the ticket (by player|type|stat). Random pick; repeated presses cycle
// since the just-placed leg is then on the ticket. No regenerate, no other leg lost.
function _replaceParlayLeg(idx){
  var legs=window._parlayLegs; if(!legs||!legs[idx]) return;
  var cur=legs[idx];
  var curKey=cur.player+'|'+cur.type+'|'+cur.stat;
  var used={}; legs.forEach(function(l,i){ if(i!==idx) used[l.player+'|'+l.type+'|'+l.stat]=1; });
  var pool=_mlbPool().filter(function(c){ var k=c.player+'|'+c.type+'|'+c.stat; return k!==curKey && !used[k]; });
  if(!pool.length){ _flashNoSwapP(idx); return; }
  legs[idx]=pool[Math.floor(Math.random()*pool.length)];
  window._lastParlayPlayers=legs.map(function(l){return l.player;});
  _paintParlay();
}
function _flashNoSwapP(idx){
  var b=document.getElementById('mlbrep'+idx); if(!b) return;
  var o=b.innerHTML; b.innerHTML='none'; b.style.background='#374151'; b.style.color='#9ca3af';
  setTimeout(function(){ b.innerHTML=o; b.style.background='#1e3a8a'; b.style.color='#bfdbfe'; },1000);
}

function _fmtBA(v){return (v==null)?'—':(typeof v==='number'?v.toFixed(3):v);}
function _lineupTxt(s){
  if(s==='IN_LINEUP') return 'In Lineup ✅';
  if(s==='NOT_IN_LINEUP') return 'Not in Lineup ❌';
  return s||'TBD';
}
function _matchName(p, q){
  var n=((p.full_name||p.name||'')+'').toLowerCase();
  return n.indexOf(q)>=0;
}

window._lastResult = null;
window.UNDERS_ONLY = false;
window.PARLAY_UNDERS = false;
window.PARLAY_OVERS = false;
window.PARLAY_MINUS = false;
window.PARLAY_PLUS = false;
// Parlay category checkboxes — which pick categories feed the parlay pool (all on by default).
window.PARLAY_CATS = {HIT:true,UNDER_HITS:true,UNDER_TB:true,K:true,RUN:true,pitcher_hits_allowed:true,pitcher_outs:true,pitcher_earned_runs:true,pitcher_walks:true};
// Parlay game filter — which games feed the parlay pool. Empty = all games allowed; a
// game is excluded only when explicitly set false. Keyed by the same gameKey() label as
// the "By Game" card. Repopulated each run from the day's slate (_buildGamesMenu).
window.PARLAY_GAMES = {};

// Paints both Overs Only / Unders Only buttons to match their toggle state.
function _paintParlayDirBtns(){
  var u=document.getElementById('parlay-unders-btn');
  if(u){ u.style.background=window.PARLAY_UNDERS?'#ff8a65':'#1f2937'; u.style.color=window.PARLAY_UNDERS?'#0e0e0e':'#fff'; }
  var o=document.getElementById('parlay-overs-btn');
  if(o){ o.style.background=window.PARLAY_OVERS?'#63cab7':'#1f2937'; o.style.color=window.PARLAY_OVERS?'#0e0e0e':'#fff'; }
  var mn=document.getElementById('parlay-minus-btn');
  if(mn){ mn.style.background=window.PARLAY_MINUS?'#fbbf24':'#1f2937'; mn.style.color=window.PARLAY_MINUS?'#0e0e0e':'#fff'; }
  var pl=document.getElementById('parlay-plus-btn');
  if(pl){ pl.style.background=window.PARLAY_PLUS?'#34d399':'#1f2937'; pl.style.color=window.PARLAY_PLUS?'#0e0e0e':'#fff'; }
}

// Parlay-builder "Unders Only" toggle button — restricts the parlay candidate pool to
// UNDER legs only (Under 1.5 hits/TB + pitcher K/prop Unders + run Unders). Mutually
// exclusive with Overs Only. Re-builds the parlay if one is already on screen.
function toggleParlayUnders(){
  window.PARLAY_UNDERS = !window.PARLAY_UNDERS;
  if(window.PARLAY_UNDERS) window.PARLAY_OVERS = false;
  _paintParlayDirBtns();
  if((document.getElementById('parlayResult').innerHTML||'').trim()) buildParlay();
}

// Parlay-builder "Overs Only" toggle button — restricts the parlay candidate pool to
// OVER legs only (to-record-a-hit + pitcher K/prop Overs + run Overs). Mutually
// exclusive with Unders Only. Re-builds the parlay if one is already on screen.
function toggleParlayOvers(){
  window.PARLAY_OVERS = !window.PARLAY_OVERS;
  if(window.PARLAY_OVERS) window.PARLAY_UNDERS = false;
  _paintParlayDirBtns();
  if((document.getElementById('parlayResult').innerHTML||'').trim()) buildParlay();
}

// Parlay-builder "− Odds Only" toggle — keeps only favorites (American odds < 0).
// Independent of Overs/Unders; mutually exclusive with "+ Odds Only".
function toggleParlayMinus(){
  window.PARLAY_MINUS = !window.PARLAY_MINUS;
  if(window.PARLAY_MINUS) window.PARLAY_PLUS = false;
  _paintParlayDirBtns();
  if((document.getElementById('parlayResult').innerHTML||'').trim()) buildParlay();
}

// Parlay-builder "+ Odds Only" toggle — keeps only plus-money legs (American odds > 0).
// Independent of Overs/Unders; mutually exclusive with "− Odds Only".
function toggleParlayPlus(){
  window.PARLAY_PLUS = !window.PARLAY_PLUS;
  if(window.PARLAY_PLUS) window.PARLAY_MINUS = false;
  _paintParlayDirBtns();
  if((document.getElementById('parlayResult').innerHTML||'').trim()) buildParlay();
}

// Maps a parlay candidate leg to its category key (the unders split by stat so
// Under 1.5 Hits and Under 1.5 Total Bases are independently checkable).
function _legCat(c){
  if(c.type==='UNDER') return (c.stat==='Total Bases')?'UNDER_TB':'UNDER_HITS';
  return c.type;
}
function _catCount(){ var n=0,t=0; for(var k in window.PARLAY_CATS){ t++; if(window.PARLAY_CATS[k]) n++; } return n+'/'+t; }
function _paintCatBtn(){ var b=document.getElementById('parlay-cats-btn'); if(b) b.innerHTML='&#9776; Categories ('+_catCount()+') &#9662;'; }
function toggleCatMenu(e){ if(e){ e.stopPropagation(); } var m=document.getElementById('parlay-cats-menu'); if(m) m.style.display=(m.style.display==='block')?'none':'block'; }
function _catChanged(){
  var cbs=document.querySelectorAll('.parlay-cat-cb');
  for(var i=0;i<cbs.length;i++){ window.PARLAY_CATS[cbs[i].value]=cbs[i].checked; }
  _paintCatBtn();
  if((document.getElementById('parlayResult').innerHTML||'').trim()) buildParlay();
}
function _catSetAll(v){
  var cbs=document.querySelectorAll('.parlay-cat-cb');
  for(var i=0;i<cbs.length;i++){ cbs[i].checked=v; }
  _catChanged();
}
// ── Parlay game filter (mirrors the category menu; list built from the day's slate) ──
// Unique games on today's board, via the same gameKey() used by the "By Game" card.
function _allGameKeys(){
  var r=window._lastResult; if(!r) return [];
  r=_filterStarted(r);
  var all=[];
  (r.top9||[]).forEach(function(p){all.push(p);});
  (r.also_ran||[]).forEach(function(p){all.push(p);});
  (r.under_picks||[]).forEach(function(p){all.push(p);});
  ((((r.pitcher_k||{}).all)||[]).filter(function(p){return p.pick&&(p.starts||0)>0;})).forEach(function(p){all.push(p);});
  (r.runs_picks||[]).forEach(function(p){all.push(p);});
  var _pp=(r.pitcher_props)||{};
  PROP_ORDER.forEach(function(mkt){ ((((_pp[mkt]||{}).picks))||[]).forEach(function(p){all.push(p);}); });
  var seen={}, out=[];
  all.forEach(function(p){ var g=gameKey(p); if(g&&g!=='Unknown'&&!seen[g]){seen[g]=1;out.push(g);} });
  out.sort();
  return out;
}
function _gameCount(){ var keys=_allGameKeys(); var n=0; for(var i=0;i<keys.length;i++){ if(window.PARLAY_GAMES[keys[i]]!==false) n++; } return n+'/'+keys.length; }
function _paintGamesBtn(){ var b=document.getElementById('parlay-games-btn'); if(b) b.innerHTML='&#9776; Games ('+_gameCount()+') &#9662;'; }
function _buildGamesMenu(){
  var list=document.getElementById('parlay-games-list'); if(!list) return;
  var keys=_allGameKeys();
  if(!keys.length){ list.innerHTML='<div style="font-size:.72rem;color:#666;padding:4px 2px">Run picks first.</div>'; _paintGamesBtn(); return; }
  list.innerHTML=keys.map(function(g){
    var on=(window.PARLAY_GAMES[g]!==false);
    return '<label class="parlay-cat-row"><input type="checkbox" class="parlay-game-cb" value="'+_esc(g)+'"'+(on?' checked':'')+' onchange="_gameChanged()"> '+_esc(g)+'</label>';
  }).join('');
  _paintGamesBtn();
}
function toggleGamesMenu(e){ if(e){ e.stopPropagation(); } var m=document.getElementById('parlay-games-menu'); if(!m) return; if(m.style.display==='block'){ m.style.display='none'; } else { _buildGamesMenu(); m.style.display='block'; } }
function _gameChanged(){
  var cbs=document.querySelectorAll('.parlay-game-cb');
  for(var i=0;i<cbs.length;i++){ window.PARLAY_GAMES[cbs[i].value]=cbs[i].checked; }
  _paintGamesBtn();
  if((document.getElementById('parlayResult').innerHTML||'').trim()) buildParlay();
}
function _gameSetAll(v){
  var cbs=document.querySelectorAll('.parlay-game-cb');
  for(var i=0;i<cbs.length;i++){ cbs[i].checked=v; }
  _gameChanged();
}
// Close the categories / games dropdowns when clicking anywhere outside them.
document.addEventListener('click', function(e){
  [['parlay-cats-menu','parlay-cats-btn'],['parlay-games-menu','parlay-games-btn']].forEach(function(pair){
    var m=document.getElementById(pair[0]); if(!m||m.style.display!=='block') return;
    var btn=document.getElementById(pair[1]);
    if(m.contains(e.target) || (btn&&btn.contains(e.target))) return;
    m.style.display='none';
  });
});

// Admin-only client-side filter: re-renders the current picks showing only UNDER
// plays (hitter Under 1.5 + pitcher K Unders). No re-run, no server call.
function toggleUndersOnly(){
  if(!window.IS_ADMIN) return;
  window.UNDERS_ONLY = !window.UNDERS_ONLY;
  var b=document.getElementById('unders-btn');
  if(b){
    if(window.UNDERS_ONLY){ b.style.background='#ff8a65'; b.style.color='#1a1a1a'; b.innerHTML='&#11015; Unders Only: ON'; }
    else { b.style.background='#1f2937'; b.style.color='#fff'; b.innerHTML='&#11015; Unders Only'; }
  }
  if(window._lastResult) showResults(window._lastResult);
}

function runPlayerSearch(raw){
  var box = document.getElementById('player-search-result');
  var q = (raw||'').trim().toLowerCase();
  if(!q){ box.innerHTML=''; return; }
  if(q.length < 2){ box.innerHTML='<div class="text-slate-500 text-sm">Keep typing…</div>'; return; }
  var r = window._lastResult;
  if(!r){ box.innerHTML='<div class="text-slate-500 text-sm">Run a date first.</div>'; return; }

  var hits = [];
  // 1) Top 9
  (r.top9||[]).forEach(function(p,i){
    if(_matchName(p,q)) hits.push({bucket:'Top Picks', rank:'#'+(i+1), kind:'HITTER', p:p});
  });
  // 2) Money Ball (also_ran)
  (r.also_ran||[]).forEach(function(p,i){
    if(_matchName(p,q)) hits.push({bucket:'Money Ball Picks', rank:'#'+(i+10), kind:'HITTER', p:p});
  });
  // 3) Under Picks
  (r.under_picks||[]).forEach(function(p,i){
    if(_matchName(p,q)) hits.push({bucket:'Under Picks', rank:'#'+(i+1), kind:'UNDER', p:p});
  });
  // 4) Pitcher K Picks
  var pk = r.pitcher_k||{};
  (pk.picks||[]).forEach(function(p,i){
    if(_matchName(p,q)) hits.push({bucket:'Pitcher K Picks', rank:'#'+(i+1), kind:'PITCHER', p:p});
  });
  (pk.all||[]).forEach(function(p){
    if(_matchName(p,q) && !(pk.picks||[]).some(function(x){return (x.name||'')===(p.name||'');})){
      hits.push({bucket:'Pitchers (no pick)', rank:'—', kind:'PITCHER', p:p});
    }
  });
  // 5) DQ'd hitters
  var dqAll = [].concat(r.dq_s1_s3||[], r.dq_step4||[], r.dq_step5||[], r.dq_lineup||[], r.dq_s4||[]);
  dqAll.forEach(function(p){
    if(_matchName(p,q)) hits.push({bucket:'Did Not Qualify', rank:'—', kind:'HITTER-DQ', p:p});
  });

  if(!hits.length){
    box.innerHTML='<div class="text-slate-500 text-sm" style="margin-bottom:10px">"<strong>'+raw+'</strong>" is not in today\\'s analyzed picks. Check any hitter in today\\'s games for a quick hit verdict:</div>'
      +'<button onclick="lookupAnyPlayer()" style="background:#fbbf24;color:#111;border:none;border-radius:8px;padding:8px 16px;font-weight:700;cursor:pointer">Look up this player →</button>'
      +'<div class="text-slate-600 text-xs" style="margin-top:8px">Searching a pitcher? Expand "All today\\'s pitchers" below the K Picks table.</div>'
      +'<div id="lookup-any-result" style="margin-top:12px"></div>';
    return;
  }

  box.innerHTML = hits.map(function(h){
    var p=h.p, kind=h.kind;
    var color = h.bucket==='Top Picks'?'#fbbf24':
                h.bucket==='Money Ball Picks'?'#94a3b8':
                h.bucket==='Under Picks'?'#ef4444':
                h.bucket==='Pitcher K Picks'?'#63cab7':
                h.bucket==='Did Not Qualify'?'#6b7280':'#9ca3af';
    var html='<div style="background:#0f0f0f;border:1px solid #262626;border-left:4px solid '+color+';border-radius:10px;padding:14px 18px;margin-bottom:10px">';
    html+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">';
    html+='<div><span style="color:#fff;font-weight:700;font-size:1.05rem">'+_nameSpan(p,(p.full_name||p.name||''))+'</span>';
    html+=' <span style="color:'+color+';font-weight:700;margin-left:8px">'+h.bucket+' '+h.rank+'</span></div>';
    if(p.side) html+='<span class="badge '+(p.side==='HOME'?'badge-home':'badge-away')+'">'+p.side+' vs '+(p.opp||'')+'</span>';
    html+='</div>';

    if(kind==='HITTER' || kind==='HITTER-DQ'){
      html+='<div style="display:flex;flex-wrap:wrap;gap:14px;font-size:.82rem;color:#cbd5e1">';
      if(p.s1!=null) html+='<span><strong style="color:#94a3b8">S1</strong> '+_fmtBA(p.s1)+'</span>';
      if(p.s2) html+='<span><strong style="color:#94a3b8">S2</strong> '+(p.s2.display||'—')+'</span>';
      if(p.s3) html+='<span><strong style="color:#94a3b8">S3</strong> '+(p.s3.display||'—')+'</span>';
      if(p.s4) html+='<span><strong style="color:#94a3b8">S4 L10 H/A</strong> '+(p.s4.display||'—')+'</span>';
      if(p.s5) html+='<span><strong style="color:#94a3b8">S5 D/N</strong> '+(p.s5.display||'—')+'</span>';
      if(p.total!=null) html+='<span><strong style="color:#fbbf24">Total</strong> '+p.total+'</span>';
      if(p.pitcher) html+='<span><strong style="color:#94a3b8">vs Pitcher</strong> '+p.pitcher+'</span>';
      if(p.lineup_status) html+='<span><strong style="color:#94a3b8">Lineup</strong> '+_lineupTxt(p.lineup_status)+'</span>';
      html+='</div>';
      if(kind==='HITTER-DQ' && p.dq_reason){
        html+='<div style="margin-top:8px;color:#fca5a5;font-size:.82rem"><strong>Why DQ\\'d:</strong> '+p.dq_reason+'</div>';
      } else if(h.bucket==='Top Picks'){
        html+='<div style="margin-top:8px;color:#cbd5e1;font-size:.82rem">Cleared every filter and ranks in the top 9 by total score.</div>';
      } else if(h.bucket==='Money Ball Picks'){
        html+='<div style="margin-top:8px;color:#cbd5e1;font-size:.82rem">Passed all 5 filters — solid play just outside the Top 9.</div>';
      } else if(h.bucket==='Under Picks'){
        html+='<div style="margin-top:8px;color:#cbd5e1;font-size:.82rem">Cold bat vs today\\'s pitcher \u2014 model likes the UNDER.</div>';
      }
    } else if(kind==='PITCHER'){
      html+='<div style="display:flex;flex-wrap:wrap;gap:14px;font-size:.82rem;color:#cbd5e1">';
      if(p.line!=null) html+='<span><strong style="color:#94a3b8">K Line</strong> '+p.line+'</span>';
      if(p.avg_k_vs_opp!=null) html+='<span><strong style="color:#94a3b8">Avg K vs Opp</strong> '+p.avg_k_vs_opp+'</span>';
      if(p.avg_ip_vs_opp!=null) html+='<span><strong style="color:#94a3b8">Avg IP</strong> '+p.avg_ip_vs_opp+'</span>';
      if(p.era_vs_opp!=null) html+='<span><strong style="color:#94a3b8">ERA</strong> '+p.era_vs_opp+'</span>';
      if(p.gap!=null) html+='<span><strong style="color:#94a3b8">Gap</strong> '+p.gap+'</span>';
      if(p.starts!=null) html+='<span><strong style="color:#94a3b8">Starts</strong> '+p.starts+'</span>';
      if(p.pick) html+='<span><strong style="color:#63cab7">Pick</strong> '+p.pick+'</span>';
      html+='</div>';
      if(p.pick_note) html+='<div style="margin-top:8px;color:#cbd5e1;font-size:.82rem">'+p.pick_note+'</div>';
    }
    html+='</div>';
    return html;
  }).join('');
}

async function lookupAnyPlayer(){
  var inp=document.getElementById('player-search-input');
  var name=(inp?inp.value:'').trim();
  var out=document.getElementById('lookup-any-result');
  if(!out) return;
  if(name.length<3){ out.innerHTML='<div class="text-slate-500 text-sm">Type at least 3 letters.</div>'; return; }
  var date=(window._lastResult&&window._lastResult.date)||'';
  out.innerHTML='<div class="text-slate-500 text-sm">Checking '+name+' across today\\'s games\u2026</div>';
  try{
    var r=await fetch('/api/lookup?name='+encodeURIComponent(name)+'&date_str='+encodeURIComponent(date));
    var d=await r.json();
    if(!d.found){ out.innerHTML='<div class="text-slate-400 text-sm">'+(d.msg||'No match.')+'</div>'; return; }
    if(d.verdict==='NOT_PLAYING'){ out.innerHTML='<div class="text-slate-400 text-sm">'+(d.msg||'')+'</div>'; return; }
    var color=d.verdict==='GOOD'?'#22c55e':d.verdict==='DECENT'?'#fbbf24':d.verdict==='UNDER'?'#ff8a65':(d.verdict==='UNKNOWN'||d.verdict==='INSUFFICIENT')?'#9ca3af':'#ef4444';
    var html='<div style="background:#0f0f0f;border:1px solid #262626;border-left:4px solid '+color+';border-radius:10px;padding:14px 18px">';
    html+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">';
    html+='<span style="color:#fff;font-weight:700;font-size:1.05rem">'+(d.full_name||name)+'</span>';
    if(d.side) html+='<span class="badge '+(d.side==='HOME'?'badge-home':'badge-away')+'">'+d.side+' vs '+(d.opp||'')+'</span>';
    html+='</div>';
    html+='<div style="color:'+color+';font-weight:700;font-size:1rem;margin-bottom:6px">'+(d.headline||'')+'</div>';
    html+='<div style="color:#cbd5e1;font-size:.85rem">'+(d.blurb||'')+'</div>';
    if(d.pitcher) html+='<div style="color:#94a3b8;font-size:.8rem;margin-top:6px">Facing '+d.pitcher+'</div>';
    html+='</div>';
    out.innerHTML=html;
  }catch(e){ out.innerHTML='<div class="text-red-400 text-sm">Lookup failed. Try again.</div>'; }
}

function toggleGameMLB(n){
  var el=document.getElementById('mlb_game_'+n);
  var btn=document.getElementById('mlb_game_btn_'+n);
  if(!el) return;
  var hidden=el.style.display==='none';
  el.style.display=hidden?'block':'none';
  if(btn) btn.textContent=hidden?'Collapse':'Expand';
}

function gameKey(p){
  var t=(p.team||'').trim(), o=(p.opp||'').trim();
  if(!t||!o) return o||t||'Unknown';
  return p.side==='HOME' ? (o+' @ '+t) : (t+' @ '+o);
}

function renderByGame(result){
  var body=document.getElementById('by-game-body');
  if(!body) return;
  var hitters=(result.top9||[]).concat(result.also_ran||[]).map(function(p){return Object.assign({_kind:'HITTER'},p);});
  var unders=(result.under_picks||[]).map(function(p){return Object.assign({_kind:'UNDER'},p);});
  var tbUnders=(result.tb_picks||[]).map(function(p){return Object.assign({_kind:'TB UNDER'},p);});
  var ks=((result.pitcher_k||{}).picks||[]).filter(function(p){return (p.starts||0)>0;}).map(function(p){return Object.assign({_kind:'PITCHER K'},p);});
  var runs=(result.runs_picks||[]).map(function(p){return Object.assign({_kind:'RUNS'},p);});
  var propLegs=[];
  var _ppBG=(result.pitcher_props)||{};
  PROP_ORDER.forEach(function(mkt){
    var cfg=PROP_CFG[mkt]; var picks=((_ppBG[mkt]||{}).picks)||[];
    var statLbl=(cfg.label||'').replace('Pitcher ','').toUpperCase();
    picks.forEach(function(p){ propLegs.push(Object.assign({_kind:statLbl},p)); });
  });
  var all=hitters.concat(unders, tbUnders, ks, runs, propLegs);
  if(!all.length){body.innerHTML='<div class="text-slate-500 text-sm">No picks yet.</div>';return;}
  var games={}, order=[];
  all.forEach(function(p){
    var k=gameKey(p);
    if(!games[k]){games[k]=[];order.push(k);}
    games[k].push(p);
  });
  order.sort();
  var html='';
  order.forEach(function(g,gi){
    var rows=games[g];
    var cnt=rows.length;
    html+='<div style="margin-bottom:10px">';
    html+='<div onclick="toggleGameMLB('+gi+')" style="background:#161616;border:1px solid #262626;border-radius:12px;padding:12px 18px;cursor:pointer;display:flex;align-items:center;justify-content:space-between">';
    html+='<span style="font-weight:700;color:#fff;font-size:.92rem">'+g+'</span>';
    html+='<div style="display:flex;align-items:center;gap:10px">';
    html+='<span style="background:rgba(245,158,11,.1);color:#f59e0b;padding:3px 12px;border-radius:999px;font-size:.75rem;font-weight:700">'+cnt+' pick'+(cnt===1?'':'s')+'</span>';
    html+='<button id="mlb_game_btn_'+gi+'" onclick="event.stopPropagation();toggleGameMLB('+gi+')" style="background:none;border:1px solid #374151;color:#9ca3af;border-radius:6px;padding:3px 12px;font-size:.72rem;cursor:pointer">Expand</button>';
    html+='</div></div>';
    html+='<div id="mlb_game_'+gi+'" style="display:none;margin-top:6px;border-radius:12px;overflow:hidden;border:1px solid #262626;background:#0f0f0f">';
    html+='<table class="results-table" style="width:100%"><thead><tr><th>Type</th><th>Player</th><th>H/A</th><th>Pick / Note</th><th>Lineup</th></tr></thead><tbody>';
    rows.forEach(function(p){
      var kind=p._kind;
      var isProp=(p._prop===true);
      var kindCls=kind==='HITTER'?'badge-home':(kind==='UNDER'?'badge-out':'badge-tbd');
      var sideBadge='<span class="badge '+(p.side==='HOME'?'badge-home':'badge-away')+'">'+(p.side||'')+'</span>';
      var note='';
      if(kind==='HITTER') note='OVER '+(p.line!=null?p.line:'0.5')+' Hits'+(p.hit_odds!=null?' · '+p.hit_odds:'');
      else if(kind==='UNDER') note=(p.pick||'UNDER')+' '+(p.line!=null?p.line:'1.5')+' Hits vs '+(p.pitcher||'TBD');
      else if(kind==='PITCHER K') note=(p.sugg_line!=null?('OVER '+p.sugg_line+' Ks (line '+(p.line||'')+')'):((p.pick||'')+' '+(p.line||'')+' Ks'));
      else if(kind==='RUNS') note=(p.pick||'')+' '+(p.line!=null?p.line:0.5)+' runs ('+(p.rate_disp||'')+')';
      else if(isProp) note=(p.pick||'')+' '+(p.line!=null?p.line:'')+' '+(p.label||'').replace('Pitcher ','')+' · blend '+(p.blended!=null?(p.blended+_ppU(p)):'—');
      var lineup=p.lineup_status==='IN_LINEUP'?'<span class="badge badge-in">✅ IN</span>'
        :p.lineup_status==='NOT_IN_LINEUP'?'<span class="badge badge-out">❌ OUT</span>'
        :'<span class="badge badge-tbd">⏳ TBD</span>';
      html+='<tr>';
      html+='<td><span class="badge '+kindCls+' text-xs">'+kind+'</span></td>';
      html+='<td class="font-semibold">'+_nameSpan(p,(p.name||''))+'</td>';
      html+='<td>'+sideBadge+'</td>';
      html+='<td class="text-slate-300 text-sm">'+note+'</td>';
      html+='<td>'+((kind==='PITCHER K'||isProp)?'<span class="text-slate-500 text-xs">—</span>':lineup)+'</td>';
      html+='</tr>';
    });
    html+='</tbody></table></div></div>';
  });
  body.innerHTML=html;
}

const _MLB_ABBR = {
  'arizona diamondbacks':'ari','atlanta braves':'atl','baltimore orioles':'bal',
  'boston red sox':'bos','chicago cubs':'chc','chicago white sox':'chw',
  'cincinnati reds':'cin','cleveland guardians':'cle','colorado rockies':'col',
  'detroit tigers':'det','houston astros':'hou','kansas city royals':'kc',
  'los angeles angels':'laa','los angeles dodgers':'lad','miami marlins':'mia',
  'milwaukee brewers':'mil','minnesota twins':'min','new york mets':'nym',
  'new york yankees':'nyy','oakland athletics':'oak','philadelphia phillies':'phi',
  'pittsburgh pirates':'pit','san diego padres':'sd','san francisco giants':'sf',
  'seattle mariners':'sea','st. louis cardinals':'stl','st louis cardinals':'stl',
  'tampa bay rays':'tb','texas rangers':'tex','toronto blue jays':'tor',
  'washington nationals':'wsh','athletics':'oak'
};
function _mlbTeamAbbr(teamName) {
  return _MLB_ABBR[(teamName||'').toLowerCase()] || '';
}
function _envChip(p){
  var e=p&&p.env; if(!e||!e.summary) return '';
  var c=e.lean==='OVER'?'#34d399':e.lean==='UNDER'?'#f87171':'#9ca3af';
  var tip=e.lean==='NEUTRAL'?'Park + weather: neutral game environment':('Park + weather environment leans '+e.lean+' (display only — does not change picks)');
  return '<div class="env-chip" title="'+_esc(tip)+'" style="border-color:'+c+'55;color:'+c+'">'+_esc(e.summary)+'</div>';
}
function _umpChip(p){
  var u=p&&p.ump; if(!u||!u.summary) return '';
  var c=u.zone==='WIDE'?'#5eead4':u.zone==='TIGHT'?'#fca5a5':'#c4b5fd';
  var tip='Home-plate umpire'+(u.games?(' \u00b7 '+u.games+' games graded'):'')
    +(u.zone==='WIDE'?' \u00b7 wide zone (trends more strikeouts, fewer runs)'
      :u.zone==='TIGHT'?' \u00b7 tight zone (trends fewer strikeouts, more runs)':'')
    +' \u2014 nudges pick order, gates untouched';
  return '<div class="env-chip" title="'+_esc(tip)+'" style="border-color:'+c+'55;color:'+c+'">'+_esc(u.summary)+'</div>';
}
function _kRankChip(p) {
  if (p.opp_k_rank == null || p.opp_k_pg == null) return '';
  var rank = p.opp_k_rank, total = p.opp_k_total || 30, kg = p.opp_k_pg;
  if (rank <= 10) {
    return '<div class="env-chip" style="border-color:#22c55e44;color:#22c55e">&#9650; High-K Lineup &middot; ' + kg + ' K/g &middot; #' + rank + ' of ' + total + '</div>';
  }
  if (rank >= total - 9) {
    return '<div class="env-chip" style="border-color:#ef444444;color:#ef4444">&#9660; Low-K Lineup &middot; ' + kg + ' K/g &middot; #' + rank + ' of ' + total + '</div>';
  }
  return '';
}
function _bpChip(p){
  // hitter cards carry bp_opp (opponent bullpen); pitcher cards carry bp_own
  var bp=null, isPitcher=false;
  if(p&&p.bp_opp!=null){ bp=p.bp_opp; isPitcher=false; }
  else if(p&&p.bp_own!=null){ bp=p.bp_own; isPitcher=true; }
  if(!bp||bp.bp_ip==null) return '';
  var ip=bp.bp_ip, taxed=bp.taxed;
  if(taxed){
    var lbl=isPitcher?'🔥 Taxed Own BP':'🔥 Taxed Opp BP';
    var clr=isPitcher?'#fbbf24':'#63cab7';
    var tip=isPitcher
      ?'Own bullpen threw '+ip+' IP in last 3 days \u2014 starter may be asked to go deeper'
      :'Opponent bullpen threw '+ip+' IP in last 3 days \u2014 late-game pitching may be weaker';
    return '<div class="env-chip" title="'+_esc(tip)+'" style="border-color:'+clr+'44;color:'+clr+'">'+lbl+' \u00b7 '+ip+' IP/3d</div>';
  }
  if(!isPitcher&&(p.pick==='UNDER'||(p.under_basis!=null))){
    var tip2='Opponent bullpen fresh ('+ip+' IP in last 3 days) \u2014 supports under lean';
    return '<div class="env-chip" title="'+_esc(tip2)+'" style="border-color:#60a5fa44;color:#60a5fa">\u2744\uFE0F Fresh Opp BP \u00b7 '+ip+' IP/3d</div>';
  }
  return '';
}
function _platoonChip(p) {
  var pl = p && p.platoon;
  if (!pl || !pl.bat_hand || !pl.pit_hand) return '';
  var adv = pl.adv;
  var c   = adv ? '#34d399' : '#f87171';
  var baStr = '';
  var tip;
  if (pl.ba != null) {
    var ba = '.' + String(Math.round(pl.ba * 1000)).padStart(3, '0');
    var ab = pl.ab ? ' (' + pl.ab + 'AB)' : '';
    baStr = ' \u00b7 ' + ba;
    tip = (adv ? 'Platoon advantage' : 'Platoon disadvantage') + ' \u2014 career BA ' + ba + ab + ' in this matchup';
  } else {
    tip = (adv ? 'Platoon advantage' : 'Platoon disadvantage') + ' \u2014 no career split data';
  }
  return `<div class="env-chip" title="${_esc(tip)}" style="border-color:${c}55;color:${c}">${_esc(pl.label)}${baStr}</div>`;
}
// Umpire-adjusted multiplier for the client-side pitcher-K sort: a wide-zone
// ump (kFactor>1) lifts OVER picks and lowers UNDER picks; tight zone inverse.
function _mlbHead(id) {
  if (!id) return '';
  return `<img src="https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/${id}/headshot/67/current" alt="" style="width:38px;height:38px;border-radius:50%;object-fit:cover;object-position:center 18%;border:1px solid rgba(255,255,255,.12);flex-shrink:0" onerror="this.style.display='none'">`;
}
function _teamMatchJS(a, b) {
  if (!a||!b) return false;
  var n1=a.toLowerCase(), n2=b.toLowerCase();
  if(n1===n2||n1.includes(n2)||n2.includes(n1)) return true;
  var st=['of','the','los','las','san','new','de'];
  var w1=n1.split(' ').filter(function(w){return st.indexOf(w)<0;});
  var w2=n2.split(' ').filter(function(w){return st.indexOf(w)<0;});
  return w1.some(function(w){return w2.indexOf(w)>=0;});
}
function _umpKMul(p){
  var u=p&&p.ump; if(!u) return 1;
  var k=Number(u.kFactor); if(!k||k<=0) return 1;
  return p.pick==='UNDER'?(1/k):k;
}
function _mlbCard(p, rank, dim) {
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const headshot = p.player_id ? `https://a.espncdn.com/i/headshots/mlb/players/full/${p.player_id}.png` : '';
  const rnkColors = rank===1?['#f59e0b','#000']:rank===2?['#c0c0c0','#000']:rank===3?['#cd7f32','#fff']:['#1e1e1e','#f59e0b'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const odds = p.hit_odds!=null?(p.hit_odds>0?'+':'')+p.hit_odds:'—';
  const s1Disp = p.s1!=null?p.s1.toFixed(3):'—';
  const s4Disp = p.s4?.display||'—';
  const s5Lbl = p.dn_label||(p.s5?'D/N':'');
  const s5Val = p.s5?.display||'—';
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>S1 <strong style="color:#94a3b8">${s1Disp}</strong></span> &nbsp;
    <span>S2 <strong style="color:#94a3b8">${p.s2?.display||'—'}</strong></span> &nbsp;
    <span>S3 <strong style="color:#94a3b8">${p.s3?.display||'—'}</strong></span><br>
    <span>S4 <strong style="color:#94a3b8">${s4Disp}</strong></span> &nbsp;
    <span>Score <strong style="color:#f59e0b">${p.total||'—'}</strong></span> &nbsp;
    <span>${s5Lbl} BA <strong style="color:#7dd3fc">${s5Val}</strong></span>
  </div>`;
  window.__HIT_REG__=window.__HIT_REG__||{}; window.__HIT_REG__['h'+rank]=p;
  return `<div class="mlb-pick-card" onclick="_hitForm('h${rank}')" title="Click for recent form" style="cursor:pointer;${dim?'opacity:0.85':''}">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#1a2a1a 0%,#0a1a0a 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:30px;height:30px;border-radius:50%;background:${rnkColors[0]};color:${rnkColors[1]};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.9rem">${rank}</div>
        ${_mlbHead(p.player_id)}
        <span style="font-size:.72rem;letter-spacing:.12em;color:#f59e0b;font-weight:800">MLB · ${p.pos||''}</span>
      </div>
      ${teamLogo?`<img src="${teamLogo}" alt="${p.team}" style="height:34px;width:34px;object-fit:contain" onerror="this.style.display='none'"/>`:''}
    </div>
    <div class="mlb-card-name">${p.full_name||p.name}</div>
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      ${_platoonChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:2px">
        <span style="font-size:.78rem;color:#64748b">${p.pitcher?'vs '+p.pitcher:''}</span>
        ${lineupBadge(p.lineup_status)}
      </div>
      ${p.blurb ? `<div style="margin-top:5px;font-size:.72rem;color:#94a3b8;line-height:1.5;font-style:italic">${p.blurb}</div>` : ''}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">Hit Odds</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odds}</span>
      </div>
      <div style="margin-top:5px;display:flex;align-items:center;gap:5px"><span style="font-size:.6rem;color:#475569">day trend</span>${_dowChip('hits_over','OVER')}</div>
      ${adminStats}
    </div>
  ${_betBtn(p,'Hitter Hits','OVER','hits','Hits',0.5,p.hit_odds)}
  </div>`;
}

function _underCard(p, rank) {
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const rnkColors = rank===1?['#ff8a65','#000']:rank===2?['#fb7185','#000']:rank===3?['#f87171','#000']:['#1e1e1e','#ff8a65'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const uOdds = p.under_odds!=null?(p.under_odds>0?'+':'')+p.under_odds:'—';
  const tbOdds = p.tb_under_odds!=null?(p.tb_under_odds>0?'+':'')+p.tb_under_odds:'—';
  const s5LblU = p.dn_label||(p.s5?'D/N':'');
  const s5ValU = p.s5?.display||'—';
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>S1 <strong style="color:#94a3b8">${p.s1_disp||'—'}</strong> <span style="color:#475569">(${p.s1_ab||0}AB)</span></span> &nbsp;
    <span>S2 <strong style="color:#94a3b8">${p.s2?.display||'—'}</strong></span><br>
    <span>S3 <strong style="color:#94a3b8">${p.s3?.display||'—'}</strong></span> &nbsp;
    <span>L7 <strong style="color:#94a3b8">${p.l7?.display||'—'}</strong></span> &nbsp;
    <span>Score <strong style="color:#ff8a65">${p.under_score||'—'}</strong></span> &nbsp;
    <span>${s5LblU} BA <strong style="color:#7dd3fc">${s5ValU}</strong></span>
  </div>`;
  window.__HIT_REG__=window.__HIT_REG__||{}; window.__HIT_REG__['u'+rank]=p;
  return `<div class="mlb-pick-card" onclick="_hitForm('u${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#2a1414 0%,#180808 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:30px;height:30px;border-radius:50%;background:${rnkColors[0]};color:${rnkColors[1]};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.9rem">${rank}</div>
        ${_mlbHead(p.batter_id)}
        <span style="font-size:.72rem;letter-spacing:.12em;color:#ff8a65;font-weight:800">MLB · UNDER</span>
      </div>
      ${teamLogo?`<img src="${teamLogo}" alt="${p.team}" style="height:34px;width:34px;object-fit:contain" onerror="this.style.display='none'"/>`:''}
    </div>
    <div class="mlb-card-name">${p.name}</div>
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      ${_platoonChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:2px">
        <span style="font-size:.78rem;color:#64748b">${p.pitcher?'vs '+p.pitcher:''}</span>
        ${lineupBadge(p.lineup_status)}
      </div>
      ${p.under_basis==='vs-ace'?`<div style="margin-top:6px;font-size:.7rem;color:#fca5a5;background:rgba(248,113,113,.12);border:1px solid rgba(248,113,113,.35);border-radius:6px;padding:3px 7px">🔥 Facing top-30 ERA ace${p.ace_era!=null?' · '+(+p.ace_era).toFixed(2)+' ERA':''}</div>`:''}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:#ff8a65;font-weight:800">U 1.5 Hits</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${uOdds}</span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">U 1.5 Total Bases</span>
        <span style="font-family:monospace;color:#63cab7;font-weight:700;font-size:.9rem">${tbOdds}</span>
      </div>
      <div style="margin-top:5px;display:flex;align-items:center;gap:5px"><span style="font-size:.6rem;color:#475569">day trend</span>${_dowChip('hits_under','UNDER')}</div>
      ${adminStats}
    </div>
  ${_betBtn(p,'Hitter Hits',(p.pick||'UNDER'),'hits','Hits',1.5,(p.pick==='OVER'?p.over_odds:p.under_odds))}
  </div>`;
}

// _moreWrap: renders a styled "Show N more" toggle button + hidden card grid.
// items: array of pick objects. renderFn(p, displayRank) -> card HTML string.
// startRank: display rank for the first item (e.g. 11). label: button text.
// color: accent hex for the button text/border.
function _moreWrap(items, renderFn, startRank, label, color) {
  if (!items || !items.length) return '';
  var clr = color || '#94a3b8';
  var cards = items.map(function(p,i){ return renderFn(p, startRank+i); }).join('');
  return '<details style="margin-top:14px">'
    + '<summary class="more-btn" style="color:'+clr+';border-color:'+clr+'33">'
    + '&#9655; '+items.length+' more '+label
    + '</summary>'
    + '<div class="mlb-picks-grid mt-3">'+cards+'</div>'
    + '</details>';
}

function _runsCard(p, rank, pfx) {
  pfx = pfx || 'rn';
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const isOver = p.pick==='OVER';
  const rnkColors = rank===1?['#60a5fa','#000']:rank===2?['#38bdf8','#000']:rank===3?['#818cf8','#000']:['#1e1e1e','#60a5fa'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const pickClr = isOver?'#63cab7':'#ff8a65';
  const od = isOver?p.over_odds:p.under_odds;
  const odDisp = od!=null?(od>0?'+':'')+od:'—';
  const scoreClr = p.score>=70?'#63cab7':p.score>=50?'#fbbf24':'#ff8a65';
  const log = p.recent_runs_log||[];
  const recCnt = log.filter(g=>g.r>=1).length;
  const s5LblR = p.dn_label||(p.s5?'D/N':'');
  const s5ValR = p.s5?.display||'—';
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>Score <strong style="color:#60a5fa">${p.score!=null?p.score+'%':'—'}</strong></span> &nbsp;
    <span>Games <strong style="color:#94a3b8">${p.games||0}</strong></span> &nbsp;
    <span>Wilson <strong style="color:#94a3b8">${p.wilson!=null?p.wilson:'—'}</strong></span> &nbsp;
    <span>${s5LblR} BA <strong style="color:#7dd3fc">${s5ValR}</strong></span>
  </div>`;
  window.__RUNS_REG__=window.__RUNS_REG__||{}; window.__RUNS_REG__[pfx+rank]=p;
  return `<div class="mlb-pick-card" onclick="_runsForm('${pfx}${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#0e1f33 0%,#08111d 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:30px;height:30px;border-radius:50%;background:${rnkColors[0]};color:${rnkColors[1]};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.9rem">${rank}</div>
        ${_mlbHead(p.batter_id)}
        <span style="font-size:.72rem;letter-spacing:.12em;color:#60a5fa;font-weight:800">MLB · RUN</span>
      </div>
      ${teamLogo?`<img src="${teamLogo}" alt="${p.team}" style="height:34px;width:34px;object-fit:contain" onerror="this.style.display='none'"/>`:''}
    </div>
    <div class="mlb-card-name">${p.name}</div>
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px">
        <span style="font-size:.78rem;color:#94a3b8">Runs Rate vr Opp</span>
        <span style="font-family:monospace;font-weight:700;color:${scoreClr}">${p.rate_disp||'—'} <span style="color:#64748b;font-size:.68rem">${p.basis||''}</span></span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b">Recent</span>
        <span style="font-size:.78rem;color:#cbd5e1">${log.length?recCnt+'/'+log.length:'—'}</span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:${pickClr};font-weight:900">${p.pick} ${p.line!=null?p.line:0.5} Runs</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odDisp}</span>
      </div>
      <div style="margin-top:5px;display:flex;align-items:center;gap:5px"><span style="font-size:.6rem;color:#475569">day trend</span>${_dowChip('runs',p.pick)}</div>
      ${adminStats}
    </div>
  ${_betBtn(p,'Runs',p.pick,'runs','Runs',(p.line!=null?p.line:0.5),(p.pick==='OVER'?p.over_odds:p.under_odds))}
  </div>`;
}

function _rbiCard(p, rank, pfx) {
  pfx = pfx || 'rb';
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const isOver = p.pick==='OVER';
  const rnkColors = rank===1?['#f59e0b','#000']:rank===2?['#fbbf24','#000']:rank===3?['#d97706','#fff']:['#1e1e1e','#f59e0b'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const pickClr = isOver?'#63cab7':'#ff8a65';
  const od = isOver?p.over_odds:p.under_odds;
  const odDisp = od!=null?(od>0?'+':'')+od:'—';
  const scoreClr = p.score>=70?'#63cab7':p.score>=50?'#fbbf24':'#ff8a65';
  const log = p.recent_rbi_log||[];
  const recCnt = log.filter(g=>g.rbi>=1).length;
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>Score <strong style="color:#f59e0b">${p.score!=null?p.score+'%':'—'}</strong></span> &nbsp;
    <span>Games <strong style="color:#94a3b8">${p.games||0}</strong></span> &nbsp;
    <span>Wilson <strong style="color:#94a3b8">${p.wilson!=null?p.wilson:'—'}</strong></span>
  </div>`;
  window.__RBI_REG__=window.__RBI_REG__||{}; window.__RBI_REG__[pfx+rank]=p;
  return `<div class="mlb-pick-card" onclick="_rbiForm('${pfx}${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#1a1200 0%,#0d0900 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:30px;height:30px;border-radius:50%;background:${rnkColors[0]};color:${rnkColors[1]};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.9rem">${rank}</div>
        ${_mlbHead(p.batter_id)}
        <span style="font-size:.72rem;letter-spacing:.12em;color:#f59e0b;font-weight:800">MLB · RBI</span>
      </div>
      ${teamLogo?`<img src="${teamLogo}" alt="${p.team}" style="height:34px;width:34px;object-fit:contain" onerror="this.style.display='none'"/>`:''}
    </div>
    <div class="mlb-card-name">${p.name}</div>
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px">
        <span style="font-size:.78rem;color:#94a3b8">RBI Rate vr Opp</span>
        <span style="font-family:monospace;font-weight:700;color:${scoreClr}">${p.rate_disp||'—'} <span style="color:#64748b;font-size:.68rem">${p.basis||''}</span></span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b">Recent</span>
        <span style="font-size:.78rem;color:#cbd5e1">${log.length?recCnt+'/'+log.length:'—'}</span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:${pickClr};font-weight:900">${p.pick} ${p.line!=null?p.line:0.5} RBI</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odDisp}</span>
      </div>
      <div style="margin-top:5px;display:flex;align-items:center;gap:5px"><span style="font-size:.6rem;color:#475569">day trend</span>${_dowChip('rbi',p.pick)}</div>
      ${adminStats}
    </div>
  ${_betBtn(p,'RBI',p.pick,'rbi','RBI',(p.line!=null?p.line:0.5),(p.pick==='OVER'?p.over_odds:p.under_odds))}
  </div>`;
}

function _rbiForm(key){
  var p=(key&&typeof key==='object')?key:(window.__RBI_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('rbi-modal');
  if(!ov){
    ov=document.createElement('div');
    ov.id='rbi-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var log=p.recent_rbi_log||[];
  var isOver=(p.pick==='OVER');
  var goal=isOver?'Over 0.5 RBI (drive in a run)':'Under 0.5 RBI (no RBI)';
  var rows=log.length?log.map(function(g){
    var hit=g.rbi>=1;
    var good=isOver?hit:!hit;
    var clr=good?'#63cab7':'#ff8a65';
    var oppTxt=g.opp?((g.ha==='H'?'vs ':'@ ')+g.opp):'';
    return `<tr>
      <td style="padding:6px 10px;color:#94a3b8;font-family:monospace">${g.d||'—'}</td>
      <td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">${oppTxt}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-size:.8rem;color:#93c5fd">${g.h} H</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:${clr}">${g.rbi} RBI</td>
    </tr>`;
  }).join(''):'<tr><td colspan="4" style="padding:14px;color:#64748b;text-align:center">No recent games on record</td></tr>';
  var name=p.full_name||p.name||'';
  var pickClr=isOver?'#63cab7':'#ff8a65';
  ov.innerHTML=`<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:440px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">
      <div>
        <div style="font-weight:800;font-size:1.05rem;color:#fff">${name}</div>
        <div style="color:#94a3b8;font-size:.78rem">${p.side||''} vs ${p.opp||''} · ${goal}</div>
      </div>
      <button onclick="document.getElementById('rbi-modal').style.display='none'" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">✕</button>
    </div>
    <div style="padding:14px 18px">
      <div style="font-size:.72rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:8px">RBI Rate ${p.rate_disp||''} · Last ${log.length||0} Games</div>
      <table style="width:100%;border-collapse:collapse;font-size:.85rem"><tbody>${rows}</tbody></table>
      <div style="margin-top:12px;border-top:1px solid #1e293b;padding-top:10px;color:${pickClr};font-weight:800;font-size:.85rem">Pick: ${goal}</div>
    </div>
  </div>`;
  ov.style.display='flex';
}

function _tbCard(p, rank) {
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const rnkColors = rank===1?['#a78bfa','#000']:rank===2?['#818cf8','#000']:rank===3?['#6366f1','#fff']:['#1e1e1e','#a78bfa'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const odDisp = p.tb_under_odds!=null?(p.tb_under_odds>0?'+':'')+p.tb_under_odds:'—';
  const scoreClr = p.score>=80?'#63cab7':p.score>=70?'#fbbf24':'#ff8a65';
  const log = p.recent_tb_log||[];
  const underCnt = log.filter(g=>g.tb<2).length;
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>Rate <strong style="color:#a78bfa">${p.score!=null?p.score+'%':'—'}</strong></span> &nbsp;
    <span>Games <strong style="color:#94a3b8">${p.games||0}</strong></span> &nbsp;
    <span>Wilson <strong style="color:#94a3b8">${p.wilson!=null?p.wilson:'—'}</strong></span>
  </div>`;
  window.__TB_REG__=window.__TB_REG__||{}; window.__TB_REG__['tb'+rank]=p;
  return `<div class="mlb-pick-card" onclick="_tbForm('tb${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#1a1030 0%,#0e0820 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:30px;height:30px;border-radius:50%;background:${rnkColors[0]};color:${rnkColors[1]};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.9rem">${rank}</div>
        ${_mlbHead(p.batter_id)}
        <span style="font-size:.72rem;letter-spacing:.12em;color:#a78bfa;font-weight:800">MLB · TB</span>
      </div>
      ${teamLogo?`<img src="${teamLogo}" alt="${p.team}" style="height:34px;width:34px;object-fit:contain" onerror="this.style.display='none'"/>`:''}
    </div>
    <div class="mlb-card-name">${p.name}</div>
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px">
        <span style="font-size:.78rem;color:#94a3b8">TB Under Rate <span style="color:#64748b;font-size:.68rem">${p.basis||''}</span></span>
        <span style="font-family:monospace;font-weight:700;color:${scoreClr}">${p.rate_disp||'—'}</span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b">Recent</span>
        <span style="font-size:.78rem;color:#cbd5e1">${log.length?underCnt+'/'+log.length+' under':'—'}</span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:#a78bfa;font-weight:900">UNDER 1.5 Total Bases</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odDisp}</span>
      </div>
      <div style="margin-top:5px;display:flex;align-items:center;gap:5px"><span style="font-size:.6rem;color:#475569">day trend</span>${_dowChip('tb_under','UNDER')}</div>
      ${adminStats}
    </div>
  ${_betBtn(p,'TB Under','UNDER','total_bases','Total Bases',1.5,p.tb_under_odds)}
  </div>`;
}

function _tbForm(key){
  var p=(key&&typeof key==='object')?key:(window.__TB_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('tb-modal');
  if(!ov){
    ov=document.createElement('div');
    ov.id='tb-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var log=p.recent_tb_log||[];
  var rows=log.length?log.map(function(g){
    var good=(g.tb<2);
    var clr=good?'#63cab7':'#ff8a65';
    var oppTxt=g.opp?((g.ha==='H'?'vs ':'@ ')+g.opp):'';
    return '<tr>'
      +'<td style="padding:6px 10px;color:#94a3b8;font-family:monospace">'+(g.d||'\u2014')+'</td>'
      +'<td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">'+oppTxt+'</td>'
      +'<td style="padding:6px 10px;text-align:right;font-family:monospace;font-size:.8rem;color:#93c5fd">'+g.h+' H</td>'
      +'<td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:'+clr+'">'+g.tb+' TB</td>'
    +'</tr>';
  }).join(''):'<tr><td colspan="4" style="padding:14px;color:#64748b;text-align:center">No recent games on record</td></tr>';
  var name=p.name||'';
  ov.innerHTML='<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:440px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">'
      +'<div><div style="font-weight:800;font-size:1.05rem;color:#fff">'+name+'</div>'
      +'<div style="color:#94a3b8;font-size:.78rem">'+(p.side||'')+' vs '+(p.opp||'')+' \u00b7 Under 1.5 Total Bases</div></div>'
      +'<button onclick="document.getElementById(&#39;tb-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">\u2715</button>'
    +'</div>'
    +'<div style="padding:14px 18px">'
      +'<div style="font-size:.72rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:8px">TB Rate '+(p.rate_disp||'')+' \u00b7 Last '+log.length+' Games</div>'
      +'<table style="width:100%;border-collapse:collapse;font-size:.85rem"><tbody>'+rows+'</tbody></table>'
      +'<div style="margin-top:12px;border-top:1px solid #1e293b;padding-top:10px;color:#a78bfa;font-weight:800;font-size:.85rem">Pick: Under 1.5 Total Bases</div>'
    +'</div>'
  +'</div>';
  ov.style.display='flex';
}

function _pitcherCard(p, rank, keyPfx) {
  keyPfx = keyPfx || 'pk';
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const rnkColors = rank===1?['#63cab7','#022']:rank===2?['#5eead4','#022']:rank===3?['#2dd4bf','#022']:['#1e1e1e','#63cab7'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const isOver = p.pick==='OVER';
  const pickClr = isOver?'#63cab7':'#ff8a65';
  const hasSugg = p.sugg_line!=null;
  const pickLabel = hasSugg?('OVER '+p.sugg_line+' K'):(p.pick?p.pick+' '+(p.line!=null?p.line:'')+' K':'—');
  const odds = hasSugg
    ?(p.sugg_odds!=null?(p.sugg_odds>0?'+':'')+p.sugg_odds:'')
    :(isOver?(p.over_odds!=null?(p.over_odds>0?'+':'')+p.over_odds:''):(p.under_odds!=null?(p.under_odds>0?'+':'')+p.under_odds:''));
  const conflict = p.avg_k!=null&&p.recent_avg_k!=null&&p.line!=null&&((p.avg_k>p.line)!==(p.recent_avg_k>p.line));
  const blDisp = p.blended_avg_k!=null?p.blended_avg_k+'K'+(conflict?' ⚠️':''):'—';
  var tkr=window.__TEAM_K_RANKS__||[];
  var tkRows=tkr.length?tkr.map(function(t){
    var hi=_teamMatchJS(t.name,p.opp||'');
    var s=hi?'color:#63cab7;font-weight:800':'color:#64748b';
    var bg=hi?'background:rgba(99,202,183,.1)':'';
    return '<tr style="'+bg+'"><td style="padding:1px 5px;'+s+'">#'+t.rank+'</td>'
      +'<td style="padding:1px 5px;font-size:.62rem;'+s+'">'+t.name+'</td>'
      +'<td style="padding:1px 5px;text-align:right;font-family:monospace;font-size:.6rem;'+s+'">'+t.k_per_g+' K/g</td></tr>';
  }).join(''):'';
  var tkSection=tkRows?'<details style="margin-top:5px"><summary style="cursor:pointer;font-size:.65rem;color:#64748b;list-style:none;user-select:none">&#9654; All Team K Rankings</summary><div style="max-height:120px;overflow-y:auto;margin-top:2px"><table style="width:100%;font-size:.62rem;border-collapse:collapse">'+tkRows+'</table></div></details>':'';
  window.__PK_REG__=window.__PK_REG__||{}; window.__PK_REG__[keyPfx+rank]=p;
  return `<div class="mlb-pick-card" onclick="_pkForm('${keyPfx}${rank}')" title="Click for all 5 markets" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#0f2420 0%,#08160f 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:30px;height:30px;border-radius:50%;background:${rnkColors[0]};color:${rnkColors[1]};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.9rem">${rank}</div>
        ${_mlbHead(p.pid)}
        <span style="font-size:.72rem;letter-spacing:.12em;color:#63cab7;font-weight:800">MLB · P</span>
      </div>
      ${teamLogo?`<img src="${teamLogo}" alt="${p.team}" style="height:34px;width:34px;object-fit:contain" onerror="this.style.display='none'"/>`:''}
    </div>
    <div class="mlb-card-name">${p.name}</div>
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      ${_kRankChip(p)}
      ${tkSection}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">K Line ${p.line!=null?p.line:'—'}</span>
        <span style="color:${pickClr};font-weight:900;font-size:1rem">${pickLabel}</span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:3px">
        <span style="font-size:.72rem;color:#64748b">Blend ${blDisp}</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.9rem">${odds||'—'}</span>
      </div>
      <div style="margin-top:5px;font-size:.68rem;color:#94a3b8;line-height:1.6">K <strong style="color:#cbd5e1">${p.avg_k!=null?p.avg_k:'—'}</strong> · H <strong style="color:#cbd5e1">${p.avg_hits!=null?p.avg_hits:'—'}</strong> · ER <strong style="color:#cbd5e1">${p.avg_er!=null?p.avg_er:'—'}</strong> · Outs <strong style="color:#cbd5e1">${p.avg_outs!=null?p.avg_outs:'—'}</strong> · BB <strong style="color:#cbd5e1">${p.avg_bb!=null?p.avg_bb:'—'}</strong> · IP <strong style="color:#cbd5e1">${p.avg_ip!=null?p.avg_ip:'—'}</strong> · ERA <strong style="color:#cbd5e1">${p.era||'—'}</strong> <span style="color:#64748b">vr opp</span></div>
      <div style="margin-top:5px;display:flex;align-items:center;justify-content:space-between"><span style="display:flex;align-items:center;gap:5px"><span style="font-size:.6rem;color:#475569">day trend</span>${_dowChip('k',p.pick)}</span><span style="font-size:.66rem;color:#63cab7">all 5 markets →</span></div>
    </div>
  ${_betBtn(p,'Pitcher Ks',(hasSugg?'OVER':p.pick),'strikeOuts','Ks',(hasSugg?p.sugg_line:p.line),(hasSugg?p.sugg_odds:(isOver?p.over_odds:p.under_odds)))}
  </div>`;
}

function statCard(icon,label,value,target) {
  const linkable = target && Number(value)>0;
  return `<div class="chip${linkable?' chip-link':''}"${linkable?` onclick="_jumpTo('${target}')" title="Jump to ${label}"`:''}><div class="val">${value}</div><div class="lbl">${label}</div></div>`;
}
function _jumpTo(id){
  const el=document.getElementById(id);
  if(!el) return;
  el.classList.remove('hidden');
  el.scrollIntoView({behavior:'smooth',block:'start'});
  el.classList.remove('flash');
  void el.offsetWidth;
  el.classList.add('flash');
  setTimeout(function(){el.classList.remove('flash');},1200);
}
function lineupBadge(s) {
  if(s==='IN_LINEUP') return '<span class="badge badge-in">✅ IN</span>';
  if(s==='NOT_IN_LINEUP') return '<span class="badge badge-out">❌ OUT</span>';
  return '<span class="badge badge-tbd">⏳ TBD</span>';
}
function statColor(ba) {
  if(!ba&&ba!==0) return 'stat-na';
  return ba>=0.300?'stat-good':ba>=0.250?'stat-warn':'stat-na';
}
function statColorStr(s) {
  if(!s||s==='N/A'||s==='—') return 'stat-na';
  const n=parseFloat(s); return isNaN(n)?'stat-na':statColor(n);
}
function appendLog(msg,type) {
  const box=document.getElementById('log-box');
  const div=document.createElement('div');
  div.className={section:'log-section',ok:'log-ok',dq:'log-dq',skip:'log-skip',
    info:'log-info',cached:'log-cached',under:'log-under',default:'log-default'}[type]||'log-default';
  div.textContent=msg; box.appendChild(div); box.scrollTop=box.scrollHeight;
}
function clearLog(){document.getElementById('log-box').innerHTML='';}
function setProgress(pct,label){
  document.getElementById('prog-bar-inner').style.width=pct+'%';
  document.getElementById('prog-label').textContent=label;
}
function authFetch(url,opts={}){return fetch(url,{...opts,headers:{...(opts.headers||{})}});}
function pad(s,n){return(s+' '.repeat(n)).slice(0,n);}
function show(id){document.getElementById(id)?.classList.remove('hidden');}
function hide(id){document.getElementById(id)?.classList.add('hidden');}
function disableRunBtn(d){
  const b=document.getElementById('run-btn');
  b.disabled=d; b.textContent=d?"Running...":"Run Picks";
  const fb=document.getElementById('force-btn');
  if(fb) fb.disabled=d;
}

// ── Results / Grader ──────────────────────────────────────────────────────
window.__GRADE_ROWS__ = [];

async function checkResults() {
  var dateStr = document.getElementById('date-picker').value;
  if (!dateStr) { alert('Pick a date first'); return; }
  var tok = localStorage.getItem('hub_token') || localStorage.getItem('__mpa_token') || '';
  var adm = new URLSearchParams(location.search).get('admin') || '';
  var btn = document.getElementById('results-btn');
  btn.disabled = true; btn.textContent = 'Loading...';
  show('grade-card');
  document.getElementById('grade-card').scrollIntoView({behavior:'smooth',block:'start'});
  document.getElementById('grade-spinner').classList.remove('hidden');
  document.getElementById('grade-body').innerHTML = '';
  document.getElementById('grade-summary').innerHTML = '';
  try {
    var res = await fetch('/api/grade/' + dateStr + '?token=' + encodeURIComponent(tok) + (adm ? ('&admin=' + encodeURIComponent(adm)) : ''));
    if (!res.ok) { var t = await res.text(); throw new Error(t); }
    renderGradeResults(await res.json());
  } catch(e) {
    document.getElementById('grade-body').innerHTML = '<p style="color:#f87171;padding:16px">' + (e.message || 'Error fetching results') + '</p>';
  } finally {
    btn.disabled = false; btn.textContent = '📊 Results';
    document.getElementById('grade-spinner').classList.add('hidden');
  }
}

function _gradeOddsDisp(odds) {
  if (odds == null || odds === '') return '—';
  var o = parseFloat(odds);
  return isNaN(o) ? '—' : (o > 0 ? '+' + o : '' + o);
}

function _gradeResultBadge(result) {
  if (result === 'WIN')  return '<span style="color:#4ade80;font-weight:700">WIN</span>';
  if (result === 'LOSS') return '<span style="color:#f87171;font-weight:700">LOSS</span>';
  return '<span style="color:#94a3b8">Pending</span>';
}

function renderGradeSection(title, rows, color) {
  if (!rows || !rows.length) return '';
  var offset = window.__GRADE_ROWS__.length;
  rows.forEach(function(r) { window.__GRADE_ROWS__.push(r); });
  var trs = rows.map(function(r, i) {
    var idx = offset + i;
    var res = r.result || 'pending';
    var bg  = res === 'WIN' ? 'rgba(74,222,128,.06)' : res === 'LOSS' ? 'rgba(248,113,113,.06)' : '';
    var actual = r.actual != null ? r.actual : '—';
    var statusNote = (r.game_status && r.game_status !== 'Final' && r.game_status !== 'Game Over' && r.game_status !== '—')
      ? ' <span style="font-size:.68rem;color:#64748b">(' + r.game_status + ')</span>' : '';
    return '<tr style="background:' + bg + '">' +
      '<td style="color:#64748b;font-size:.75rem">' + (i + 1) + '</td>' +
      '<td style="font-weight:700;white-space:nowrap">' + (r.name || '—') + '</td>' +
      '<td style="font-size:.8rem;color:#cbd5e1">' + (r.pick || '—') + '</td>' +
      '<td style="font-family:monospace;color:#94a3b8">' + _gradeOddsDisp(r.odds) + '</td>' +
      '<td><input type="number" min="0" step="1" placeholder="$" oninput="recalcPL()" id="gbet' + idx + '" class="bet-input"></td>' +
      '<td style="font-family:monospace;font-weight:700;color:#fff">' + actual + statusNote + '</td>' +
      '<td>' + _gradeResultBadge(res) + '</td>' +
      '<td id="gpl' + idx + '" style="font-family:monospace;font-weight:700;color:#94a3b8">—</td>' +
      '</tr>';
  }).join('');
  return '<details open style="margin-bottom:20px">' +
    '<summary style="cursor:pointer;list-style:none;display:flex;align-items:center;gap:8px;padding:10px 0;border-bottom:1px solid #1f2937;margin-bottom:8px">' +
    '<span style="font-weight:700;color:' + color + ';font-size:.9rem">' + title + '</span>' +
    '<span style="font-size:.72rem;color:#64748b;background:#111;border-radius:999px;padding:2px 8px">' + rows.length + '</span>' +
    '<span style="font-size:.7rem;color:#475569;margin-left:auto">▸ toggle</span></summary>' +
    '<div style="overflow-x:auto"><table class="grade-table">' +
    '<thead><tr><th>#</th><th>Player</th><th>Pick</th><th>Odds</th><th>Bet ($)</th><th>Actual</th><th>Result</th><th>P&L</th></tr></thead>' +
    '<tbody>' + trs + '</tbody></table></div></details>';
}

function renderGradeResults(data) {
  window.__GRADE_ROWS__ = [];
  var cats = [
    { key: 'hitter_overs',  label: 'Hitter OVER 0.5 Hits',  color: '#4ade80' },
    { key: 'hitter_unders', label: 'Hitter Under 1.5 Hits', color: '#ff8a65' },
    { key: 'runs',          label: 'Runs OVER / UNDER 0.5', color: '#60a5fa' },
    { key: 'pitcher_ks',    label: 'Pitcher Strikeouts',     color: '#63cab7' },
    { key: 'pitcher_props', label: 'Pitcher Props',          color: '#a78bfa' },
  ];
  var allRows = [];
  cats.forEach(function(c) { allRows = allRows.concat(data[c.key] || []); });
  var wins    = allRows.filter(function(r) { return r.result === 'WIN'; }).length;
  var losses  = allRows.filter(function(r) { return r.result === 'LOSS'; }).length;
  var pending = allRows.filter(function(r) { return r.result !== 'WIN' && r.result !== 'LOSS'; }).length;
  document.getElementById('grade-summary').innerHTML =
    '<div style="background:#111;border-radius:10px;padding:14px 18px;margin-bottom:20px;display:flex;flex-wrap:wrap;gap:12px;align-items:center">' +
    '<div><span style="color:#4ade80;font-weight:700;font-size:1.1rem">' + wins + 'W</span> ' +
    '<span style="color:#f87171;font-weight:700;font-size:1.1rem">' + losses + 'L</span>' +
    (pending > 0 ? ' <span style="color:#94a3b8;font-size:.85rem;margin-left:4px">' + pending + ' pending</span>' : '') + '</div>' +
    '<div style="margin-left:auto;font-size:.82rem;color:#94a3b8" id="grade-summary-stats">Enter bet amounts below to track P&L</div>' +
    '</div>';
  var bodyHtml = cats.map(function(c) {
    return renderGradeSection(c.label, data[c.key] || [], c.color);
  }).join('');
  document.getElementById('grade-body').innerHTML = bodyHtml ||
    '<p style="color:#94a3b8;padding:16px">No graded picks for this date.</p>';
}

function recalcPL() {
  var rows = window.__GRADE_ROWS__ || [];
  var totalWagered = 0, totalNet = 0, wins = 0, losses = 0;
  rows.forEach(function(r, idx) {
    var input = document.getElementById('gbet' + idx);
    var cell  = document.getElementById('gpl' + idx);
    if (!input || !cell) return;
    var stake = parseFloat(input.value) || 0;
    if (!stake) { cell.textContent = '—'; cell.style.color = '#94a3b8'; return; }
    var odds = parseFloat(r.odds);
    if (isNaN(odds) || r.odds == null) { cell.textContent = '—'; cell.style.color = '#94a3b8'; return; }
    if (r.result === 'WIN') {
      var profit = odds > 0 ? (odds / 100) * stake : (100 / Math.abs(odds)) * stake;
      cell.textContent = '+' + profit.toFixed(2); cell.style.color = '#4ade80';
      totalNet += profit; totalWagered += stake; wins++;
    } else if (r.result === 'LOSS') {
      cell.textContent = '-' + stake.toFixed(2); cell.style.color = '#f87171';
      totalNet -= stake; totalWagered += stake; losses++;
    } else {
      cell.textContent = 'TBD'; cell.style.color = '#94a3b8';
      totalWagered += stake;
    }
  });
  var stats = document.getElementById('grade-summary-stats');
  if (!stats || totalWagered === 0) return;
  var roi = totalNet / totalWagered * 100;
  stats.innerHTML =
    'Wagered <strong style="color:#fff">$' + totalWagered.toFixed(2) + '</strong>' +
    ' &nbsp;|&nbsp; Net <strong style="color:' + (totalNet >= 0 ? '#4ade80' : '#f87171') + '">' +
    (totalNet >= 0 ? '+' : '') + totalNet.toFixed(2) + '</strong>' +
    ' &nbsp;|&nbsp; ROI <strong style="color:' + (roi >= 0 ? '#4ade80' : '#f87171') + '">' +
    (roi >= 0 ? '+' : '') + roi.toFixed(1) + '%</strong>';
}

// ── Track Record (admin) — all-time + daily W/L by category ──────────────
async function openTrackRecord(){
  var btn=document.getElementById('track-btn');
  var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||'';
  var adm=new URLSearchParams(location.search).get('admin')||'';
  var lbl=btn.textContent; btn.disabled=true; btn.textContent='Loading...';
  show('track-card');
  document.getElementById('track-card').scrollIntoView({behavior:'smooth',block:'start'});
  document.getElementById('track-spinner').classList.remove('hidden');
  document.getElementById('track-alltime').innerHTML='';
  document.getElementById('track-daily').innerHTML='';
  try{
    var url='/api/track-record?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'');
    var res=await fetch(url);
    if(!res.ok){ var t=await res.text(); throw new Error(t); }
    window.__TRACK__=await res.json();
    renderTrackRecord(window.__TRACK__);
  }catch(e){
    document.getElementById('track-alltime').innerHTML='<p style="color:#f87171;padding:16px">'+(e.message||'Error loading track record')+'</p>';
  }finally{
    btn.disabled=false; btn.textContent=lbl;
    document.getElementById('track-spinner').classList.add('hidden');
  }
}

function _twPct(w,l){ var n=w+l; return n? (w/n*100).toFixed(1)+'%' : '—'; }
function _twColor(w,l){ var n=w+l; if(!n) return '#94a3b8'; var p=w/n*100; return p>=60?'#4ade80':(p>=50?'#facc15':'#f87171'); }
function _twSide(s){ return s==='OVER' ? '<span style="color:#4ade80">OVER</span>' : '<span style="color:#ff8a65">UNDER</span>'; }

function renderTrackRecord(d){
  var rows=d.alltime||[]; var daily=d.daily||[];
  var tw=0,tl=0;
  rows.forEach(function(r){ tw+=r.wins; tl+=r.losses; });
  var CAT_CFG={
    'Hitter Hits|OVER':          {lbl:'Top Picks (Over 0.5 Hits)', icon:'🎯', abbr:'Hits'},
    'Hitter Hits|UNDER':         {lbl:'Under 1.5 Hits',            icon:'📉', abbr:'Unders'},
    'Runs|OVER':                 {lbl:'Runs (Over 0.5)',            icon:'🏃', abbr:'Runs+'},
    'Runs|UNDER':                {lbl:'Runs (Under 0.5)',           icon:'🏃', abbr:'Runs-'},
    'Pitcher Ks|OVER':           {lbl:'Pitcher Ks (Over)',          icon:'⚾', abbr:'Ks+'},
    'Pitcher Ks|UNDER':          {lbl:'Pitcher Ks (Under)',         icon:'⚾', abbr:'Ks-'},
    'Pitcher Hits Allowed|OVER': {lbl:'Hits Allowed (Over)',        icon:'🎯', abbr:'H-All+'},
    'Pitcher Hits Allowed|UNDER':{lbl:'Hits Allowed (Under)',       icon:'🎯', abbr:'H-All-'},
    'Pitcher Outs|OVER':         {lbl:'Pitcher Outs (Over)',        icon:'🔢', abbr:'Outs+'},
    'Pitcher Outs|UNDER':        {lbl:'Pitcher Outs (Under)',       icon:'🔢', abbr:'Outs-'},
    'Pitcher Earned Runs|OVER':  {lbl:'Earned Runs (Over)',         icon:'🔥', abbr:'ER+'},
    'Pitcher Earned Runs|UNDER': {lbl:'Earned Runs (Under)',        icon:'🔥', abbr:'ER-'},
    'TB Under|UNDER':            {lbl:'TB Under 1.5',               icon:'📊', abbr:'TB-'},
    'RBI|OVER':                  {lbl:'RBI (Over 0.5)',             icon:'💥', abbr:'RBI+'},
    'RBI|UNDER':                 {lbl:'RBI (Under 0.5)',            icon:'💥', abbr:'RBI-'},
  };
  var CAT_ORDER=['Hitter Hits|OVER','Hitter Hits|UNDER','Runs|OVER','Runs|UNDER',
    'TB Under|UNDER','RBI|OVER','RBI|UNDER',
    'Pitcher Ks|OVER','Pitcher Ks|UNDER','Pitcher Hits Allowed|OVER','Pitcher Hits Allowed|UNDER',
    'Pitcher Outs|OVER','Pitcher Outs|UNDER','Pitcher Earned Runs|OVER','Pitcher Earned Runs|UNDER'];
  function _rc(w,n){ if(!n) return '#64748b'; var p=w/n; return p>=0.70?'#4ade80':(p>=0.55?'#facc15':'#f87171'); }
  function _bar(w,n,clr){
    var pct=n?Math.round(w/n*100):0;
    return '<div style="height:9px;border-radius:5px;background:#1e293b;overflow:hidden;flex:1;min-width:80px">'
      +'<div style="height:100%;width:'+pct+'%;background:'+clr+';border-radius:5px"></div></div>';
  }
  var sumHtml='<div style="background:#111;border-radius:10px;padding:14px 18px;margin-bottom:16px;display:flex;flex-wrap:wrap;gap:12px;align-items:center">'
    +'<div style="font-weight:700;font-size:.95rem">All-Time: '
    +'<span style="color:#4ade80;font-size:1.2rem;font-weight:900">'+tw+'W</span> '
    +'<span style="color:#f87171;font-size:1.2rem;font-weight:900">'+tl+'L</span> '
    +'<span style="color:'+_twColor(tw,tl)+'"> ('+_twPct(tw,tl)+')</span></div>'
    +'<div style="font-size:.79rem;color:#64748b">'+(d.days||0)+' day'+((d.days===1)?'':'s')+' graded \u00b7 top-10 picks per category \u00b7 Final games only</div>'
    +'<div style="margin-left:auto;display:flex;gap:8px">'
    +'<button onclick="downloadTrackAllTimeCSV()" style="background:#7c3aed;color:#fff;border:none;border-radius:8px;padding:7px 12px;font-size:.78rem;font-weight:600;cursor:pointer">\u2b07 All-Time CSV</button>'
    +'<button onclick="downloadTrackDailyCSV()" style="background:#1d4ed8;color:#fff;border:none;border-radius:8px;padding:7px 12px;font-size:.78rem;font-weight:600;cursor:pointer">\u2b07 Daily CSV</button>'
    +'</div>'
    +'</div>';
  var catRows='';
  rows.forEach(function(r){
    var key=r.category+'|'+r.side;
    var cfg=CAT_CFG[key]||{lbl:r.category+' ('+r.side+')',icon:'📊',abbr:r.side};
    var n=r.wins+r.losses;
    var clr=_rc(r.wins,n);
    var pctStr=n?Math.round(r.wins/n*100)+'%':'—';
    catRows+='<div style="display:flex;align-items:center;gap:10px;padding:11px 14px;border-bottom:1px solid #1e293b">'
      +'<span style="font-size:1.1rem;width:22px;text-align:center;flex-shrink:0">'+cfg.icon+'</span>'
      +'<span style="color:#e2e8f0;font-weight:600;min-width:195px;font-size:.87rem;flex-shrink:0">'+cfg.lbl+'</span>'
      +'<span style="font-family:monospace;font-weight:900;font-size:1.1rem;color:'+clr+';min-width:54px;text-align:right;flex-shrink:0">'+r.wins+'/'+n+'</span>'
      +_bar(r.wins,n,clr)
      +'<span style="font-family:monospace;font-size:.87rem;font-weight:700;color:'+clr+';min-width:40px;text-align:right;flex-shrink:0">'+pctStr+'</span>'
      +'</div>';
  });
  var catSection=rows.length
    ?'<div style="border:1px solid #1e293b;border-radius:12px;overflow:hidden;margin-bottom:16px">'
      +'<div style="display:flex;align-items:center;padding:7px 14px;background:#0c1829;border-bottom:1px solid #1e293b">'
      +'<span style="font-size:.68rem;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.07em;min-width:217px">Category</span>'
      +'<span style="font-size:.68rem;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.07em;min-width:54px;text-align:right">W/N</span>'
      +'<span style="font-size:.68rem;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.07em;flex:1;margin:0 10px">Hit Rate</span>'
      +'<span style="font-size:.68rem;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.07em;min-width:40px;text-align:right">%</span>'
      +'</div>'+catRows+'</div>'
    :'<p style="color:#94a3b8;padding:16px">No graded days yet \u2014 fills in automatically as slates go Final.</p>';
  var det=d.detail||[];
  var earnHtml='<div style="background:#0a1f14;border:1px solid #16432c;border-radius:10px;padding:14px 18px;margin-bottom:16px;display:flex;flex-wrap:wrap;gap:14px;align-items:center">'
    +'<div style="font-weight:800;font-size:.92rem;color:#6ee7b7">💰 Potential Earnings</div>'
    +'<label style="font-size:.82rem;color:#94a3b8">Flat bet $ <input id="trkBet" type="number" min="1" step="1" value="100" oninput="_recalcEarnings()" style="width:84px;margin-left:4px;background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:5px 8px;font-size:.82rem"></label>'
    +'<div id="trkNet" style="font-size:.88rem;font-weight:700;color:#e2e8f0"></div>'
    +'<button onclick="downloadTrackEarningsCSV()" style="margin-left:auto;background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 Earnings CSV (Excel)</button>'
    +'</div>';
  document.getElementById('track-alltime').innerHTML=sumHtml+earnHtml+catSection;
  _recalcEarnings();
  var dRows=daily.slice().reverse().map(function(x){
    var cats=x.cats||{};
    var pills='';
    CAT_ORDER.forEach(function(key){
      var parts=key.split('|'); var cat=parts[0],side=parts[1];
      var wl=(cats[cat]||{})[side];
      if(!wl) return;
      var w=wl[0],l=wl[1],n=w+l;
      if(!n) return;
      var cfg2=CAT_CFG[key]||{abbr:cat,icon:''};
      var clr2=_rc(w,n);
      pills+='<span style="display:inline-block;background:#1e293b;border-radius:6px;padding:3px 8px;font-size:.76rem;color:'+clr2+';font-weight:700;margin:2px 3px 2px 0">'+cfg2.icon+' '+cfg2.abbr+' '+w+'/'+n+'</span>';
    });
    return '<tr>'
      +'<td style="white-space:nowrap;color:#94a3b8;font-weight:600">'+x.date+'</td>'
      +'<td style="font-family:monospace;color:#4ade80;font-weight:700">'+x.wins+'</td>'
      +'<td style="font-family:monospace;color:#f87171;font-weight:700">'+x.losses+'</td>'
      +'<td style="font-family:monospace;font-weight:700;color:'+_twColor(x.wins,x.losses)+'">'+_twPct(x.wins,x.losses)+'</td>'
      +'<td style="padding-left:8px;padding-top:3px;padding-bottom:3px">'+pills+'</td></tr>';
  }).join('');
  document.getElementById('track-daily').innerHTML=daily.length
    ?'<details style="margin-top:0"><summary style="cursor:pointer;font-weight:700;color:#a78bfa;padding:10px 0;border-bottom:1px solid #1f2937">📅 Daily Breakdown ('+daily.length+' days)</summary>'
      +'<div style="overflow-x:auto;margin-top:8px"><table class="grade-table">'
      +'<thead><tr><th>Date</th><th>W</th><th>L</th><th>%</th><th style="text-align:left">By Category</th></tr></thead>'
      +'<tbody>'+dRows+'</tbody></table></div></details>'
    :'';
}

// American-odds profit on a winning bet; a loss always costs the full stake.
// Returns null for a WIN whose odds we never captured (can't value the payout).
function _amProfit(odds, stake, win){
  if(!win) return -stake;
  if(odds==null||odds==='') return null;
  odds=Number(odds);
  if(!isFinite(odds)||odds===0) return null;   // unpriceable / malformed -> exclude
  return odds>0 ? stake*(odds/100) : stake*(100/Math.abs(odds));
}
// Single source of truth for the flat bet size — blank/zero/negative/NaN all
// fall back to the 100 default so the live total and the CSV always agree.
function _trkStake(){
  var inp=document.getElementById('trkBet');
  var s=inp?Number(inp.value):100;
  if(!isFinite(s)||s<=0) s=100;
  return s;
}
function _recalcEarnings(){
  var d=window.__TRACK__; if(!d) return;
  var el=document.getElementById('trkNet'); if(!el) return;
  var det=d.detail||[];
  var stake=_trkStake();
  if(!det.length){ el.innerHTML='<span style="color:#64748b">No per-pick detail yet \u2014 builds up from today forward as slates go Final.</span>'; return; }
  var net=0, counted=0, skipped=0;
  det.forEach(function(r){
    var pl=_amProfit(r.odds, stake, (r.result==='WIN'));
    if(pl===null){ skipped++; return; }
    net+=pl; counted++;
  });
  var risk=counted*stake;
  var roi=risk?(net/risk*100):0;
  var clr=net>=0?'#4ade80':'#f87171';
  el.innerHTML='Net P/L across '+counted+' plays: <span style="color:'+clr+';font-weight:900;font-size:1.05rem">'+(net>=0?'+':'\u2212')+'$'+Math.abs(net).toFixed(0)+'</span> '
    +'<span style="color:#64748b">(ROI '+(roi>=0?'+':'\u2212')+Math.abs(roi).toFixed(1)+'% on $'+risk.toFixed(0)+' risked)</span>'
    +(skipped?(' <span style="color:#facc15">\u00b7 '+skipped+' win'+(skipped===1?'':'s')+' had no odds (excluded)</span>'):'');
}
function downloadTrackEarningsCSV(){
  var d=window.__TRACK__; if(!d){ alert('Open Track Record first.'); return; }
  var det=d.detail||[];
  if(!det.length){ alert('No per-pick detail yet \u2014 it accrues from today forward as slates go Final.'); return; }
  var stake=_trkStake();
  var rows=[['Date','Player','Team','Category','Side','Pick','Odds','Result','Bet Size','Profit/Loss']];
  var net=0, counted=0;
  det.forEach(function(r){
    var pl=_amProfit(r.odds, stake, (r.result==='WIN'));
    var plStr='';
    if(pl!==null){ plStr=pl.toFixed(2); net+=pl; counted++; }
    rows.push([r.date, r.name, r.team, r.category, r.side, r.pick,
      (r.odds!=null?((r.odds>0?'+':'')+r.odds):''), r.result, stake, plStr]);
  });
  rows.push([]);
  rows.push(['','','','','','','','TOTALS ('+counted+' graded)', (counted*stake), net.toFixed(2)]);
  var csv=rows.map(function(row){return row.map(_csvCell).join(',');}).join(String.fromCharCode(13)+String.fromCharCode(10));
  var blob=new Blob([String.fromCharCode(65279)+csv],{type:'text/csv;charset=utf-8;'});
  var url=URL.createObjectURL(blob);
  var a=document.createElement('a');
  a.href=url; a.download='mlb-earnings-flat'+stake+'.csv';
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
function downloadTrackAllTimeCSV(){ _trackCSV('alltime'); }
function downloadTrackDailyCSV(){ _trackCSV('daily'); }
function _trackCSV(which){
  var d=window.__TRACK__; if(!d){ alert('Open Track Record first.'); return; }
  var rows;
  var DAYS=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  var CAT_COLS=[
    ['Hitter Hits','OVER','Hits W','Hits L'],
    ['Hitter Hits','UNDER','Unders W','Unders L'],
    ['Runs','OVER','Runs+ W','Runs+ L'],
    ['Runs','UNDER','Runs- W','Runs- L'],
    ['Pitcher Ks','OVER','Ks+ W','Ks+ L'],
    ['Pitcher Ks','UNDER','Ks- W','Ks- L'],
    ['Pitcher Hits Allowed','OVER','H-All+ W','H-All+ L'],
    ['Pitcher Hits Allowed','UNDER','H-All- W','H-All- L'],
    ['Pitcher Outs','OVER','Outs+ W','Outs+ L'],
    ['Pitcher Outs','UNDER','Outs- W','Outs- L'],
    ['Pitcher Earned Runs','OVER','ER+ W','ER+ L'],
    ['Pitcher Earned Runs','UNDER','ER- W','ER- L'],
    ['Pitcher Walks','OVER','BB+ W','BB+ L'],
    ['Pitcher Walks','UNDER','BB- W','BB- L'],
  ];
  if(which==='daily'){
    var hdr=['Date','Day'];
    CAT_COLS.forEach(function(c){ hdr.push(c[2],c[3]); });
    hdr.push('Total W','Total L','Win %');
    rows=[hdr];
    (d.daily||[]).forEach(function(x){
      var dow='';
      try{ var dt=new Date(x.date+'T12:00:00'); dow=DAYS[dt.getDay()]||''; }catch(e){}
      var row=[x.date,dow];
      var cats=x.cats||{};
      CAT_COLS.forEach(function(c){
        var wl=(cats[c[0]]||{})[c[1]];
        row.push(wl?wl[0]:'', wl?wl[1]:'');
      });
      var n=x.wins+x.losses;
      row.push(x.wins, x.losses, n?(x.wins/n*100).toFixed(1):'');
      rows.push(row);
    });
  } else {
    rows=[['Category','Side','Wins','Losses','Win %']];
    (d.alltime||[]).forEach(function(r){ var n=r.wins+r.losses; rows.push([r.category,r.side,r.wins,r.losses, n?(r.wins/n*100).toFixed(1):'']); });
  }
  var csv=rows.map(function(row){return row.map(_csvCell).join(',');}).join(String.fromCharCode(13)+String.fromCharCode(10));
  var blob=new Blob([String.fromCharCode(65279)+csv],{type:'text/csv;charset=utf-8;'});
  var url=URL.createObjectURL(blob);
  var a=document.createElement('a');
  a.href=url; a.download='mlb-track-record-'+which+'.csv';
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ── My Bets: personal bet log + ROI (admin-only) ───────────────────────
function _betAuthQS(){
  var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||'';
  var adm=new URLSearchParams(location.search).get('admin')||'';
  return '?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'');
}
// Builds the "＋ Track Bet" control (admin-only). Registers the pick in
// __BET_SRC__ and opens the stake form. No line ⇒ no button (can't grade).
function _betBtn(p,cat,side,statKey,statLabel,line,odds){
  if(!window.IS_ADMIN) return '';
  if(line==null||!side||!statKey) return '';
  window.__BET_SRC__=window.__BET_SRC__||{}; window.__BET_N__=(window.__BET_N__||0)+1;
  var k='bs'+window.__BET_N__;
  window.__BET_SRC__[k]={name:(p.full_name||p.name||''),team:(p.team||''),opp:(p.opp||''),
    category:cat,side:side,stat_key:statKey,stat_label:statLabel,line:line,
    odds:(odds!=null?odds:null),date:((window._lastResult&&window._lastResult.date)||'')};
  return `<div style="display:flex;flex-direction:row;align-items:stretch;border-top:1px solid #1e293b;flex-shrink:0;width:100%;box-sizing:border-box">
    <button onclick="event.stopPropagation();_betForm('${k}')" style="width:50%;box-sizing:border-box;background:#1a1740;color:#a5b4fc;border:none;border-right:1px solid #1e293b;padding:6px 0;font-size:.75rem;font-weight:800;cursor:pointer;white-space:nowrap;text-align:center">Track Bet</button>
    <button onclick="event.stopPropagation();_addToCart('${k}')" style="width:50%;box-sizing:border-box;background:#0d2318;color:#6ee7b7;border:none;padding:6px 0;font-size:.75rem;font-weight:800;cursor:pointer;white-space:nowrap;text-align:center">+ Parlay</button>
  </div>`;
}
window._cartLegs=window._cartLegs||[];
function _addToCart(key){
  var src=(window.__BET_SRC__||{})[key]; if(!src) return;
  var already=window._cartLegs.some(function(l){ return l._key===key; });
  if(already){ _betToast('Already in parlay'); return; }
  var dec=src.odds!=null?_amToDec(src.odds):null;
  window._cartLegs.push({_key:key,player:src.name,team:src.team,opp:src.opp,
    dir:src.side,line:src.line,stat:src.stat_label,
    odds:src.odds,dec:dec,type:src.category,src:src});
  _updateCartBar();
  _betToast('\u2795 Added to parlay \u2014 '+window._cartLegs.length+' leg'+(window._cartLegs.length===1?'':'s'));
}
function _removeFromCart(key){
  window._cartLegs=window._cartLegs.filter(function(l){ return l._key!==key; });
  _updateCartBar();
}
function _updateCartBar(){
  var bar=document.getElementById('cart-bar');
  if(!bar){
    bar=document.createElement('div'); bar.id='cart-bar';
    bar.style.cssText='position:fixed;bottom:0;left:0;right:0;z-index:9998;background:linear-gradient(135deg,#1e1b4b,#2e1065);border-top:2px solid #4f46e5;padding:10px 16px;display:flex;align-items:center;justify-content:space-between;box-shadow:0 -4px 30px rgba(79,70,229,.35);transition:transform .25s;gap:10px';
    document.body.appendChild(bar);
  }
  var n=window._cartLegs.length;
  if(n===0){ bar.style.transform='translateY(100%)'; return; }
  bar.style.transform='translateY(0)';
  var dec=1; var allOdds=true;
  window._cartLegs.forEach(function(l){ if(l.dec) dec*=l.dec; else allOdds=false; });
  var amRaw=allOdds?_decToAm(dec):null;
  var amTxt=amRaw!=null?(amRaw>0?'+'+amRaw:''+amRaw):'—';
  var chips=window._cartLegs.map(function(l){
    return '<span style="display:inline-flex;align-items:center;gap:4px;background:rgba(255,255,255,.07);border:1px solid rgba(255,255,255,.1);border-radius:6px;padding:3px 8px;font-size:.7rem;white-space:nowrap">'
      +'<span style="color:#e2e8f0;font-weight:700">'+_esc(l.player)+'</span>'
      +'<span style="color:#a5b4fc">'+_esc(l.dir+' '+l.line)+'</span>'
      +'<button onclick="event.stopPropagation();_removeFromCart(\\''+l._key+'\\')" style="background:none;border:none;color:#f87171;cursor:pointer;font-size:.75rem;padding:0 1px;line-height:1">\u2715</button>'
      +'</span>';
  }).join('');
  bar.innerHTML='<div style="display:flex;flex-wrap:wrap;gap:5px;align-items:center;flex:1;min-width:0">'
    +'<span style="font-weight:800;color:#fff;font-size:.82rem;white-space:nowrap">\U0001F3AF '+n+'-Leg</span>'
    +chips
    +'</div>'
    +'<div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px;flex-shrink:0">'
    +'<span style="color:#fbbf24;font-weight:800;font-size:.9rem;font-family:monospace">'+amTxt+'</span>'
    +'<div style="display:flex;gap:5px">'
    +'<button onclick="window._cartLegs=[];_updateCartBar()" style="background:rgba(239,68,68,.15);border:1px solid rgba(239,68,68,.3);color:#fca5a5;border-radius:7px;padding:5px 9px;font-size:.7rem;font-weight:700;cursor:pointer">Clear</button>'
    +'<button onclick="_cartParlayForm()" style="background:#4f46e5;border:none;color:#fff;border-radius:7px;padding:6px 13px;font-weight:800;font-size:.8rem;cursor:pointer">\U0001F4DD Log Parlay</button>'
    +'</div></div>';
}
function _cartParlayForm(){
  if(!window._cartLegs||!window._cartLegs.length) return;
  window._parlayLegs=window._cartLegs.slice();
  _parlayBetForm();
}
function _betForm(key){
  var src=(window.__BET_SRC__||{})[key]; if(!src) return;
  window.__BET_CUR__=src;
  var ov=document.getElementById('bet-modal');
  if(!ov){ ov=document.createElement('div'); ov.id='bet-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.82);z-index:10000;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var pickTxt=src.side+' '+src.line+' '+(src.stat_label||'');
  ov.innerHTML='<div style="background:#0f172a;border:1px solid #312e81;border-radius:16px;max-width:360px;width:100%;box-shadow:0 20px 60px rgba(0,0,0,.6)">'
    +'<div style="display:flex;justify-content:space-between;align-items:flex-start;padding:16px 18px;border-bottom:1px solid #1e293b">'
      +'<div><div style="font-weight:800;color:#fff;font-size:1.02rem">'+_esc(src.name)+'</div>'
      +'<div style="color:#a5b4fc;font-size:.82rem;font-weight:800;margin-top:2px">'+_esc(pickTxt)+'</div>'
      +'<div style="color:#94a3b8;font-size:.72rem;margin-top:2px">'+_esc(src.category||'')+(src.opp?(' · vs '+_esc(src.opp)):'')+(src.date?(' · '+src.date):'')+'</div></div>'
      +'<button onclick="document.getElementById(&#39;bet-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">\u2715</button>'
    +'</div>'
    +'<div style="padding:16px 18px;display:grid;gap:12px">'
      +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Odds (American)<input id="bet-odds" type="number" value="'+(src.odds!=null?src.odds:'')+'" style="display:block;width:100%;margin-top:5px;background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 11px;color:#fbbf24;font-family:monospace;font-weight:700;font-size:.95rem"></label>'
      +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Bet size ($)<input id="bet-stake" type="number" min="0" step="0.01" placeholder="e.g. 50" style="display:block;width:100%;margin-top:5px;background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 11px;color:#fff;font-weight:700;font-size:.95rem"></label>'
      +'<div id="bet-payout" style="font-size:.78rem;color:#64748b;min-height:1em"></div>'
      +'<div id="bet-msg" style="font-size:.76rem;color:#f87171;min-height:1em"></div>'
      +'<button id="bet-save" onclick="_saveBet()" style="background:#4338ca;color:#fff;border:none;border-radius:9px;padding:11px;font-weight:800;cursor:pointer;font-size:.92rem">Log Bet</button>'
    +'</div></div>';
  ov.style.display='flex';
  var so=document.getElementById('bet-odds'), ss=document.getElementById('bet-stake');
  function _calc(){
    var o=parseFloat(so.value), s=parseFloat(ss.value);
    var pay=document.getElementById('bet-payout');
    if(!isFinite(o)||!isFinite(s)||s<=0){ pay.textContent=''; return; }
    var win=o>0?s*(o/100):s*(100/Math.abs(o));
    pay.innerHTML='To win <strong style="color:#4ade80">$'+win.toFixed(2)+'</strong> · total payout <strong style="color:#cbd5e1">$'+(s+win).toFixed(2)+'</strong>';
  }
  so.oninput=_calc; ss.oninput=_calc; _calc();
  setTimeout(function(){ ss.focus(); },50);
}
async function _saveBet(){
  var src=window.__BET_CUR__; if(!src) return;
  var o=parseFloat(document.getElementById('bet-odds').value);
  var s=parseFloat(document.getElementById('bet-stake').value);
  var msg=document.getElementById('bet-msg');
  if(!isFinite(o)){ msg.textContent='Enter the odds.'; return; }
  if(!isFinite(s)||s<=0){ msg.textContent='Enter a bet size greater than 0.'; return; }
  var btn=document.getElementById('bet-save'); btn.disabled=true; btn.textContent='Saving…';
  try{
    var body=Object.assign({},src,{odds:Math.round(o),stake:s,placed_at:new Date().toISOString()});
    var res=await fetch('/api/bets'+_betAuthQS(),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!res.ok){ throw new Error(await res.text()); }
    document.getElementById('bet-modal').style.display='none';
    _betToast('✅ Bet logged');
    var mb=document.getElementById('mybets-card');
    if(mb && !mb.classList.contains('hidden')) openMyBets(false);
  }catch(e){ msg.textContent=(e.message||'Save failed'); btn.disabled=false; btn.textContent='Log Bet'; }
}
function _legStatKey(l){
  if(l.type==='UNDER') return l.stat==='Total Bases'?'':'hits';
  var m={HIT:'hits',K:'strikeOuts',RUN:'runs',
    pitcher_hits_allowed:'hits_allowed',pitcher_outs:'outs',
    pitcher_earned_runs:'earnedRuns',pitcher_walks:'walks'};
  return m[l.type]||'';
}
function _parlayBetForm(){
  var legs=window._parlayLegs||[]; if(!legs.length) return;
  var dec=1;
  legs.forEach(function(l){ if(l.dec) dec*=l.dec; });
  var am=_decToAm(dec);
  var ov=document.getElementById('pbet-modal');
  if(!ov){ ov=document.createElement('div'); ov.id='pbet-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.85);z-index:10000;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var legRows=legs.map(function(l,i){
    var fo=l.odds!=null?((l.odds>0?'+':'')+l.odds):'—';
    return '<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid #1e293b;font-size:.78rem">'
      +'<span style="color:#e2e8f0;font-weight:700">'+(i+1)+'. '+_esc(l.player||'')+'</span>'
      +'<span style="color:#94a3b8">'+_esc(l.dir+' '+l.line+' '+(l.stat||''))+'</span>'
      +'<span style="font-family:monospace;color:#fbbf24;font-weight:700">'+fo+'</span>'
      +'</div>';
  }).join('');
  ov.innerHTML='<div style="background:#0f172a;border:1px solid #312e81;border-radius:16px;max-width:400px;width:100%;box-shadow:0 20px 60px rgba(0,0,0,.6);max-height:90vh;overflow-y:auto">'
    +'<div style="display:flex;justify-content:space-between;align-items:flex-start;padding:16px 18px;border-bottom:1px solid #1e293b">'
      +'<div><div style="font-weight:800;color:#fbbf24;font-size:1rem">'+legs.length+'-Leg Parlay</div>'
      +'<div style="color:#a5b4fc;font-size:.8rem;margin-top:2px">Combined: <strong>'+(am||'—')+'</strong></div></div>'
      +'<button onclick="document.getElementById(&#39;pbet-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">\u2715</button>'
    +'</div>'
    +'<div style="padding:12px 18px;border-bottom:1px solid #1e293b">'+legRows+'</div>'
    +'<div style="padding:16px 18px;display:grid;gap:12px">'
      +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Combined Odds (American)<input id="pbet-odds" type="number" value="'+(am!=null?am:'')+'" style="display:block;width:100%;margin-top:5px;background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 11px;color:#fbbf24;font-family:monospace;font-weight:700;font-size:.95rem"></label>'
      +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Bet size ($)<input id="pbet-stake" type="number" min="0" step="0.01" placeholder="e.g. 20" style="display:block;width:100%;margin-top:5px;background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 11px;color:#fff;font-weight:700;font-size:.95rem"></label>'
      +'<div id="pbet-payout" style="font-size:.78rem;color:#64748b;min-height:1em"></div>'
      +'<div id="pbet-msg" style="font-size:.76rem;color:#f87171;min-height:1em"></div>'
      +'<button id="pbet-save" onclick="_saveParlay()" style="background:#4338ca;color:#fff;border:none;border-radius:9px;padding:11px;font-weight:800;cursor:pointer;font-size:.92rem">Log Parlay</button>'
    +'</div></div>';
  ov.style.display='flex';
  var so=document.getElementById('pbet-odds'), ss=document.getElementById('pbet-stake');
  function _pc(){
    var o=parseFloat(so.value), s=parseFloat(ss.value);
    var pay=document.getElementById('pbet-payout');
    if(!isFinite(o)||!isFinite(s)||s<=0){ pay.textContent=''; return; }
    var win=o>0?s*(o/100):s*(100/Math.abs(o));
    pay.innerHTML='To win <strong style="color:#4ade80">$'+win.toFixed(2)+'</strong> · total payout <strong style="color:#cbd5e1">$'+(s+win).toFixed(2)+'</strong>';
  }
  so.oninput=_pc; ss.oninput=_pc; _pc();
  setTimeout(function(){ ss.focus(); },50);
}
async function _saveParlay(){
  var legs=window._parlayLegs||[]; if(!legs.length) return;
  var o=parseFloat(document.getElementById('pbet-odds').value);
  var s=parseFloat(document.getElementById('pbet-stake').value);
  var msg=document.getElementById('pbet-msg');
  if(!isFinite(o)){ msg.textContent='Enter combined odds.'; return; }
  if(!isFinite(s)||s<=0){ msg.textContent='Enter a stake > 0.'; return; }
  var btn=document.getElementById('pbet-save'); btn.disabled=true; btn.textContent='Saving…';
  var today=(window._lastResult&&window._lastResult.date)||new Date().toISOString().slice(0,10);
  var legsData=legs.map(function(l){
    return {name:(l.player||''),team:(l.team||''),opp:(l.opp||''),
      side:l.dir,stat_key:_legStatKey(l),stat_label:(l.stat||''),
      line:l.line,odds:l.odds,category:l.type,
      date:((l.src&&l.src.date)||today)};
  });
  var body={bet_type:'parlay',legs:legsData,odds:Math.round(o),stake:s,
    date:today,placed_at:new Date().toISOString()};
  try{
    var res=await fetch('/api/bets'+_betAuthQS(),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!res.ok) throw new Error(await res.text());
    document.getElementById('pbet-modal').style.display='none';
    _betToast('\u2705 Parlay logged');
    window._cartLegs=[]; _updateCartBar();
    var mb=document.getElementById('mybets-card');
    if(mb && !mb.classList.contains('hidden')) openMyBets(false);
  }catch(e){ msg.textContent=(e.message||'Save failed'); btn.disabled=false; btn.textContent='Log Parlay'; }
}
window._mpLegs=[];
window._mpPhase=1;
var _MP_TYPES=[['HIT','Hit'],['K','Strikeout'],['RUN','Run'],['UNDER_HITS','Under Hits'],
  ['PHITS','Pitcher Hits Allowed'],['POUTS','Pitcher Outs'],['PER','Pitcher Earned Runs'],['OTHER','Other']];
function _mpStatKey(t){return({HIT:'hits',K:'strikeOuts',RUN:'runs',UNDER_HITS:'hits',PHITS:'hits_allowed',POUTS:'outs',PER:'earnedRuns'})[t]||'';}
function _mpSide(t){return t==='UNDER_HITS'?'UNDER':'OVER';}
function _mpLegOddsDisplay(o){if(o==null) return '\u2014'; return (o>0?'+':'')+o;}
function _mpCalcCombined(){
  var dec=1;
  window._mpLegs.forEach(function(l){
    if(l.odds!=null){var d=l.odds>0?1+(l.odds/100):1-(100/l.odds); dec*=d;}
  });
  return _decToAm(dec);
}
function _mpGetModal(){
  var m=document.getElementById('mp-modal');
  if(!m){
    m=document.createElement('div'); m.id='mp-modal';
    m.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.88);z-index:10000;display:flex;align-items:center;justify-content:center;padding:16px';
    m.onclick=function(e){if(e.target===m){m.style.display='none';window._mpLegs=[];window._mpPhase=1;}};
    document.body.appendChild(m);
  }
  m.style.display='flex'; return m;
}
function _mpRender(){
  var m=_mpGetModal();
  var legs=window._mpLegs;
  var ph=window._mpPhase;
  var legListHtml=legs.length===0?('<div style="color:#475569;font-size:.76rem;padding:8px 0;font-style:italic">No legs yet — add your first leg below.</div>')
    :legs.map(function(l,i){
      var od=_mpLegOddsDisplay(l.odds);
      return '<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid #1e293b">'
        +'<span style="color:#f59e0b;font-weight:700;font-size:.78rem;min-width:18px">'+(i+1)+'.</span>'
        +'<span style="color:#e2e8f0;font-weight:600;flex:1;font-size:.82rem">'+_esc(l.name)+'</span>'
        +'<span style="color:#94a3b8;font-size:.76rem">'+_esc(l.side)+' '+l.line+' '+_esc(l.label)+'</span>'
        +'<span style="font-family:monospace;color:#fbbf24;font-size:.78rem;min-width:38px;text-align:right">'+od+'</span>'
        +(ph===1?('<button onclick="_mpRemoveLeg('+i+')" style="background:none;border:none;color:#475569;cursor:pointer;font-size:.9rem;padding:0 2px" title="Remove">\u2716</button>'):'')
        +'</div>';
    }).join('');
  var inner='';
  if(ph===1){
    var typeOpts=_MP_TYPES.map(function(t){return '<option value="'+t[0]+'">'+t[1]+'</option>';}).join('');
    inner='<div style="background:#0f172a;border:1px solid #4c1d95;border-radius:16px;max-width:420px;width:100%;max-height:92vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.65)">'
      +'<div style="display:flex;justify-content:space-between;align-items:center;padding:15px 18px;border-bottom:1px solid #1e293b">'
        +'<div style="font-weight:800;color:#c084fc;font-size:.95rem">&#128221; Log Parlay <span style="font-size:.72rem;color:#64748b;font-weight:400">Step 1 of 2 — Add Legs</span></div>'
        +'<button onclick="document.getElementById(&#39;mp-modal&#39;).style.display=&#39;none&#39;;window._mpLegs=[];window._mpPhase=1" style="background:#1e293b;border:none;color:#94a3b8;width:28px;height:28px;border-radius:7px;cursor:pointer">\u00d7</button>'
      +'</div>'
      +'<div style="padding:12px 18px;border-bottom:1px solid #1e293b">'
        +'<div style="font-size:.68rem;color:#64748b;font-weight:700;margin-bottom:6px;letter-spacing:.04em">LEGS ADDED ('+legs.length+')</div>'
        +legListHtml
      +'</div>'
      +'<div style="padding:14px 18px;border-bottom:1px solid #1e293b;display:grid;gap:10px">'
        +'<div style="font-size:.68rem;color:#64748b;font-weight:700;letter-spacing:.04em">ADD A LEG</div>'
        +'<input id="mp-name" type="text" placeholder="Player name (e.g. Freddie Freeman)" autocomplete="off" style="background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 11px;color:#f1f5f9;font-size:.88rem;width:100%">'
        +'<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">'
          +'<select id="mp-type" style="background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 10px;color:#f1f5f9;font-size:.82rem">'+typeOpts+'</select>'
          +'<select id="mp-side" style="background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 10px;color:#f1f5f9;font-size:.82rem"><option value="OVER">OVER</option><option value="UNDER">UNDER</option></select>'
        +'</div>'
        +'<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">'
          +'<label style="font-size:.7rem;color:#64748b">Line<input id="mp-line" type="number" step="0.5" value="0.5" style="display:block;margin-top:4px;width:100%;background:#0b1120;border:1px solid #334155;border-radius:8px;padding:8px 10px;color:#f1f5f9;font-size:.88rem"></label>'
          +'<label style="font-size:.7rem;color:#64748b">Leg Odds (American)<input id="mp-odds" type="number" placeholder="-115" style="display:block;margin-top:4px;width:100%;background:#0b1120;border:1px solid #334155;border-radius:8px;padding:8px 10px;color:#fbbf24;font-family:monospace;font-size:.88rem"></label>'
        +'</div>'
        +'<div id="mp-add-err" style="color:#f87171;font-size:.74rem;min-height:.9em"></div>'
        +'<button onclick="_mpAddLeg()" style="background:#7e22ce;color:#fff;border:none;border-radius:9px;padding:10px;font-weight:800;cursor:pointer;font-size:.88rem">+ Add Leg</button>'
      +'</div>'
      +'<div style="padding:14px 18px;display:flex;gap:10px;justify-content:flex-end">'
        +'<button onclick="_mpAddLeg(true)" '+(legs.length<2?'disabled style="opacity:.35;cursor:not-allowed"':'')+'style="background:#4f46e5;color:#fff;border:none;border-radius:9px;padding:10px 20px;font-weight:800;cursor:pointer;font-size:.9rem">Done \u2192 Review &amp; Save</button>'
      +'</div>'
      +'</div>';
  } else {
    var autoOdds=_mpCalcCombined();
    inner='<div style="background:#0f172a;border:1px solid #4c1d95;border-radius:16px;max-width:400px;width:100%;max-height:92vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.65)">'
      +'<div style="display:flex;justify-content:space-between;align-items:center;padding:15px 18px;border-bottom:1px solid #1e293b">'
        +'<div style="font-weight:800;color:#c084fc;font-size:.95rem">&#128221; Log Parlay <span style="font-size:.72rem;color:#64748b;font-weight:400">Step 2 of 2 — Confirm &amp; Save</span></div>'
        +'<button onclick="document.getElementById(&#39;mp-modal&#39;).style.display=&#39;none&#39;;window._mpLegs=[];window._mpPhase=1" style="background:#1e293b;border:none;color:#94a3b8;width:28px;height:28px;border-radius:7px;cursor:pointer">\u00d7</button>'
      +'</div>'
      +'<div style="padding:12px 18px;border-bottom:1px solid #1e293b">'
        +'<div style="font-size:.68rem;color:#64748b;font-weight:700;margin-bottom:6px;letter-spacing:.04em">'+legs.length+'-LEG PARLAY</div>'
        +legListHtml
      +'</div>'
      +'<div style="padding:16px 18px;display:grid;gap:12px">'
        +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Combined Odds (American)'
          +'<input id="mp-combined-odds" type="number" value="'+(autoOdds!=null?autoOdds:'')+'" placeholder="e.g. +650" style="display:block;width:100%;margin-top:5px;background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 11px;color:#fbbf24;font-family:monospace;font-weight:700;font-size:.95rem">'
        +'</label>'
        +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Bet size ($)'
          +'<input id="mp-stake" type="number" min="0" step="0.01" placeholder="e.g. 25" style="display:block;width:100%;margin-top:5px;background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 11px;color:#fff;font-weight:700;font-size:.95rem">'
        +'</label>'
        +'<div id="mp-pay-preview" style="font-size:.78rem;color:#64748b;min-height:1em"></div>'
        +'<div id="mp-save-err" style="color:#f87171;font-size:.74rem;min-height:.9em"></div>'
        +'<div style="display:flex;gap:8px">'
          +'<button onclick="window._mpPhase=1;_mpRender()" style="background:#1e293b;color:#94a3b8;border:none;border-radius:9px;padding:10px 16px;font-weight:700;cursor:pointer;font-size:.85rem">\u2190 Back</button>'
          +'<button id="mp-save-btn" onclick="_mpSave()" style="flex:1;background:#4f46e5;color:#fff;border:none;border-radius:9px;padding:11px;font-weight:800;cursor:pointer;font-size:.92rem">Log Parlay</button>'
        +'</div>'
      +'</div>'
      +'</div>';
  }
  m.innerHTML=inner;
  if(ph===2){
    var oc=document.getElementById('mp-combined-odds'), os=document.getElementById('mp-stake');
    function _pp(){ var o=parseFloat(oc.value),s=parseFloat(os.value);
      var pv=document.getElementById('mp-pay-preview'); if(!pv) return;
      if(!isFinite(o)||!isFinite(s)||s<=0){pv.textContent='';return;}
      var win=o>0?s*(o/100):s*(100/Math.abs(o));
      pv.innerHTML='To win <strong style="color:#4ade80">$'+win.toFixed(2)+'</strong> \u00b7 total $'+(s+win).toFixed(2);
    }
    oc.oninput=_pp; os.oninput=_pp; _pp();
    setTimeout(function(){os.focus();},50);
  }
  if(ph===1){ setTimeout(function(){var n=document.getElementById('mp-name'); if(n) n.focus();},50); }
}
function _mpAddLeg(done){
  var ne=document.getElementById('mp-add-err'); if(ne) ne.textContent='';
  var name=(document.getElementById('mp-name')||{}).value||''; name=name.trim();
  var type=(document.getElementById('mp-type')||{}).value||'HIT';
  var side=(document.getElementById('mp-side')||{}).value||'OVER';
  var lineEl=document.getElementById('mp-line'); var line=lineEl?parseFloat(lineEl.value):NaN;
  var oddsEl=document.getElementById('mp-odds'); var odds=oddsEl&&oddsEl.value.trim()?parseInt(oddsEl.value,10):null;
  if(!name){if(ne) ne.textContent='Enter a player name.'; return;}
  if(!isFinite(line)){if(ne) ne.textContent='Enter a valid line.'; return;}
  if(type!=='OTHER'&&odds!=null&&(odds>=-100&&odds<100)&&odds!==0){if(ne) ne.textContent='Odds look off \u2014 use American format (e.g. -115 or +250).'; return;}
  var typeLbl=(_MP_TYPES.find(function(x){return x[0]===type;})||[type,type])[1];
  window._mpLegs.push({name:name,type:type,label:typeLbl,side:side,line:line,odds:isFinite(odds)?odds:null,
    stat_key:_mpStatKey(type)});
  if(done&&window._mpLegs.length>=2){window._mpPhase=2;}
  _mpRender();
}
function _mpRemoveLeg(i){ window._mpLegs.splice(i,1); _mpRender(); }
async function _mpSave(){
  var legs=window._mpLegs; if(!legs.length) return;
  var o=parseFloat((document.getElementById('mp-combined-odds')||{}).value);
  var s=parseFloat((document.getElementById('mp-stake')||{}).value);
  var err=document.getElementById('mp-save-err');
  if(!isFinite(o)){if(err) err.textContent='Enter combined odds.'; return;}
  if(!isFinite(s)||s<=0){if(err) err.textContent='Enter a stake > 0.'; return;}
  var btn=document.getElementById('mp-save-btn'); if(btn){btn.disabled=true; btn.textContent='Saving\u2026';}
  var today=(window._lastResult&&window._lastResult.date)||new Date().toISOString().slice(0,10);
  var legsData=legs.map(function(l){
    return {name:l.name,team:'',opp:'',side:l.side,stat_key:l.stat_key,
      stat_label:l.label,line:l.line,odds:l.odds,category:l.type,date:today};
  });
  var body={bet_type:'parlay',legs:legsData,odds:Math.round(o),stake:s,date:today,placed_at:new Date().toISOString()};
  try{
    var res=await fetch('/api/bets'+_betAuthQS(),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!res.ok) throw new Error(await res.text());
    var m=document.getElementById('mp-modal'); if(m) m.style.display='none';
    window._mpLegs=[]; window._mpPhase=1;
    _betToast('\u2705 Parlay logged \u2014 '+legs.length+' legs');
    var mb=document.getElementById('mybets-card');
    if(mb&&!mb.classList.contains('hidden')) openMyBets(false);
  }catch(e){
    if(err) err.textContent=(e.message||'Save failed');
    if(btn){btn.disabled=false; btn.textContent='Log Parlay';}
  }
}
function _manualParlayForm(){ window._mpLegs=[]; window._mpPhase=1; _mpRender(); }
function _bcToggle(){
  var b=document.getElementById('betting-context-body');
  var a=document.getElementById('bc-arrow');
  var wasHidden=b.classList.contains('hidden');
  b.classList.toggle('hidden');
  if(a) a.textContent=wasHidden?'\u25bc collapse':'\u25b6 expand';
}
var _DOW_SIG={
  0:['O','U','U','O','O','U','O','U','O','O'],
  1:['O','U','U','U','U','O','U','O','U','U'],
  2:['O','U','U','O','O','U','O','O','O','O'],
  3:['O','U','U','O','O','O','O','U','O','O'],
  4:['O','U','U','U','U','U','O','U','O','U'],
  5:['O','U','U','O','O','O','U','O','U','U'],
  6:['O','U','U','O','O','U','O','U','O','O']
};
var _DOW_IDX={hits_over:0,hits_under:1,tb_under:2,runs:3,rbi:4,k:5,hits_allowed:6,outs:7,er:8,walks:9};
function _dowChip(mkt,pickDir){
  var day=new Date().getDay();
  var idx=_DOW_IDX[mkt]; if(idx===undefined) return '';
  var sig=(_DOW_SIG[day]||[])[idx]; if(!sig) return '';
  var match=sig===(pickDir||'').toUpperCase().charAt(0);
  var dn=['SUN','MON','TUE','WED','THU','FRI','SAT'][day];
  return match
    ?'<span style="font-size:.61rem;background:rgba(74,222,128,.1);border:1px solid rgba(74,222,128,.28);color:#4ade80;border-radius:5px;padding:1px 6px;letter-spacing:.04em;font-weight:700">'+dn+' \u2714<\/span>'
    :'<span style="font-size:.61rem;background:rgba(251,191,36,.07);border:1px solid rgba(251,191,36,.22);color:#fbbf24;border-radius:5px;padding:1px 6px;letter-spacing:.04em;font-weight:700">'+dn+' \u2195<\/span>';
}
function _bcTab(tab){
  document.getElementById('bc-bat').style.display=tab==='bat'?'block':'none';
  document.getElementById('bc-pit').style.display=tab==='pit'?'block':'none';
  var b=document.getElementById('bc-tab-bat');
  var p=document.getElementById('bc-tab-pit');
  b.style.borderColor=tab==='bat'?'#4ade80':'#334155';
  b.style.background=tab==='bat'?'rgba(74,222,128,.1)':'transparent';
  b.style.color=tab==='bat'?'#4ade80':'#64748b';
  p.style.borderColor=tab==='pit'?'#63cab7':'#334155';
  p.style.background=tab==='pit'?'rgba(99,202,183,.1)':'transparent';
  p.style.color=tab==='pit'?'#63cab7':'#64748b';
}
function _betToast(m){
  var t=document.getElementById('bet-toast');
  if(!t){ t=document.createElement('div'); t.id='bet-toast';
    t.style.cssText='position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:#4338ca;color:#fff;padding:10px 18px;border-radius:10px;font-weight:700;font-size:.85rem;z-index:10001;box-shadow:0 10px 30px rgba(0,0,0,.5);transition:opacity .3s;opacity:0';
    document.body.appendChild(t);
  }
  t.textContent=m; t.style.opacity='1';
  clearTimeout(window.__betToastT__);
  window.__betToastT__=setTimeout(function(){ t.style.opacity='0'; },1800);
}
async function openMyBets(scroll){
  show('mybets-card');
  if(scroll!==false) document.getElementById('mybets-card').scrollIntoView({behavior:'smooth',block:'start'});
  // Reset button state in case a previous getMyBetsResults call left it disabled
  var _rbtn=document.getElementById('mybets-results-btn');
  if(_rbtn){ _rbtn.disabled=false; _rbtn.textContent='🔄 Get Results'; }
  var _swrap=document.getElementById('mybets-spinner-wrap');
  if(_swrap) _swrap.innerHTML='';
  document.getElementById('mybets-body').innerHTML='<p style="color:#94a3b8;padding:8px 0;font-size:.85rem">Loading…</p>';
  try{
    var res=await fetch('/api/bets'+_betAuthQS()+'&settle=false');
    if(!res.ok){
      if(res.status===403) throw new Error('Session expired \u2014 reopen the MLB app from the hub to refresh your login.');
      throw new Error(await res.text());
    }
    window.__MYBETS__=await res.json();
    renderMyBets(window.__MYBETS__);
  }catch(e){
    document.getElementById('mybets-body').innerHTML='<p style="color:#f87171;padding:16px">'+(e.message||'Error loading bets')+'</p>';
  }
}
async function getMyBetsResults(){
  var btn=document.getElementById('mybets-results-btn');
  var wrap=document.getElementById('mybets-spinner-wrap');
  if(btn){ btn.disabled=true; btn.textContent='Checking…'; }
  if(wrap) wrap.innerHTML='<span style="color:#94a3b8;font-size:.85rem;display:inline-flex;align-items:center;gap:7px;margin-left:4px"><span class="spinner"></span> Checking results…</span>';
  var _mbc=new AbortController(); var _mbt=setTimeout(function(){ _mbc.abort(); },30000);
  try{
    var res=await fetch('/api/bets'+_betAuthQS(),{signal:_mbc.signal});
    if(!res.ok){ throw new Error(await res.text()); }
    window.__MYBETS__=await res.json();
    renderMyBets(window.__MYBETS__);
  }catch(e){
    var em=e.name==='AbortError'?'Timed out — try again':(e.message||'Error');
    document.getElementById('mybets-body').innerHTML+='<p style="color:#f87171;padding:8px">'+em+'</p>';
  }finally{
    clearTimeout(_mbt);
    if(btn){ btn.disabled=false; btn.textContent='🔄 Get Results'; }
    if(wrap) wrap.innerHTML='';
  }
}
function _money(n){ if(n==null) return '—'; var v=Number(n); return (v<0?'-$':'$')+Math.abs(v).toFixed(2); }
function _betOddsDisp(o){ return o!=null?((o>0?'+':'')+o):'—'; }
function _resColor(r){ return r==='WIN'?'#4ade80':(r==='LOSS'?'#f87171':(r==='PUSH'?'#facc15':'#94a3b8')); }
function _statBox(lbl,val,clr){ return '<div style="background:#111;border-radius:10px;padding:10px 14px;min-width:92px"><div style="font-size:.64rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">'+lbl+'</div><div style="font-size:1.12rem;font-weight:800;color:'+(clr||'#e2e8f0')+'">'+val+'</div></div>'; }
function renderMyBets(d){
  var s=d.summary||{}; var bets=d.bets||[];
  var roiTxt=s.roi!=null?((s.roi>0?'+':'')+s.roi+'%'):'—';
  var roiClr=s.roi==null?'#94a3b8':(s.roi>0?'#4ade80':(s.roi<0?'#f87171':'#facc15'));
  var netClr=(s.profit||0)>0?'#4ade80':((s.profit||0)<0?'#f87171':'#cbd5e1');
  var recTxt=(s.wins||0)+'-'+(s.losses||0)+(s.push?('-'+s.push+'P'):'');
  var head='<div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin-bottom:18px">'
    +_statBox('Record',recTxt,'#e2e8f0')
    +_statBox('Pending',(s.pending||0),'#94a3b8')
    +_statBox('Staked',_money(s.staked||0),'#cbd5e1')
    +_statBox('Net',_money(s.profit||0),netClr)
    +_statBox('Returned',_money(s.returned||0),'#cbd5e1')
    +_statBox('ROI',roiTxt,roiClr)
    +'<div style="margin-left:auto"><button onclick="downloadMyBetsCSV()" style="background:#4338ca;color:#fff;border:none;border-radius:8px;padding:8px 12px;font-size:.78rem;font-weight:700;cursor:pointer">⬇ CSV</button></div>'
    +'</div>';
  var bc=(s.by_category||[]).map(function(c){
    var croi=c.roi!=null?((c.roi>0?'+':'')+c.roi+'%'):'—';
    var cclr=c.roi==null?'#94a3b8':(c.roi>0?'#4ade80':(c.roi<0?'#f87171':'#facc15'));
    return '<tr><td style="font-weight:600">'+c.category+'</td>'
      +'<td style="font-family:monospace">'+c.wins+'-'+c.losses+(c.push?('-'+c.push+'P'):'')+'</td>'
      +'<td style="font-family:monospace;color:#94a3b8">'+(c.pending||0)+'</td>'
      +'<td style="font-family:monospace">'+_money(c.staked)+'</td>'
      +'<td style="font-family:monospace;color:'+((c.profit||0)>=0?'#4ade80':'#f87171')+'">'+_money(c.profit)+'</td>'
      +'<td style="font-family:monospace;font-weight:700;color:'+cclr+'">'+croi+'</td></tr>';
  }).join('');
  var bcHtml=bc?'<div style="overflow-x:auto;margin-bottom:18px"><table class="grade-table"><thead><tr><th>Category</th><th>W-L</th><th>Pend</th><th>Staked</th><th>Net</th><th>ROI</th></tr></thead><tbody>'+bc+'</tbody></table></div>':'';
  var rows=bets.map(function(b){
    var res=b.result||'pending';
    var delBtn='<button onclick="_deleteBet(&#39;'+b.id+'&#39;)" title="Remove" style="background:none;border:none;color:#64748b;cursor:pointer;font-size:1rem">\u2716</button>';
    if(b.bet_type==='parlay'){
      var n=(b.legs||[]).length;
      var lid='pleg_'+b.id;
      var legRows=(b.legs||[]).map(function(lg){
        var lr=lg.result||'pending';
        var at=lg.actual!=null?' ('+lg.actual+')':'';
        return '<div style="display:flex;gap:8px;align-items:center;padding:4px 0;border-bottom:1px solid #0f1422;font-size:.74rem">'
          +'<span style="color:#64748b;min-width:14px">↳</span>'
          +'<span style="color:#94a3b8;flex:1">'+_esc(lg.name||'')+'</span>'
          +'<span style="color:#cbd5e1">'+_esc((lg.side||'')+' '+lg.line+' '+(lg.stat_label||''))+'</span>'
          +'<span style="font-family:monospace;color:#64748b;min-width:40px;text-align:right">'+_betOddsDisp(lg.odds)+'</span>'
          +'<span style="font-weight:700;color:'+_resColor(lr)+';min-width:46px;text-align:right">'+(lr==='pending'?'pend':lr)+at+'</span>'
          +'</div>';
      }).join('');
      return '<tr onclick="var e=document.getElementById(\\''+lid+'\\');e.style.display=e.style.display===\\'none\\'?\\'table-row\\':\\'none\\'" style="cursor:pointer">'
        +'<td style="white-space:nowrap;color:#94a3b8;font-family:monospace;font-size:.76rem">'+(b.date||'')+'</td>'
        +'<td style="font-weight:700;color:#fbbf24">'+n+'-Leg Parlay <span style="font-size:.66rem;color:#475569;font-weight:400">&#9658; expand</span></td>'
        +'<td style="font-size:.78rem;color:#64748b">Combined</td>'
        +'<td style="font-family:monospace">'+_betOddsDisp(b.odds)+'</td>'
        +'<td style="font-family:monospace">'+_money(b.stake)+'</td>'
        +'<td style="font-weight:800;color:'+_resColor(res)+'">'+(res==='pending'?'pending':res)+'</td>'
        +'<td style="font-family:monospace;font-weight:700;color:'+((b.profit||0)>=0?'#4ade80':'#f87171')+'">'+(b.profit!=null?_money(b.profit):'—')+'</td>'
        +'<td>'+delBtn+'</td>'
        +'</tr>'
        +'<tr id="'+lid+'" style="display:none"><td colspan="8" style="padding:0 12px 8px 24px;background:#080c14">'
        +'<div style="padding:6px 0">'+legRows+'</div>'
        +'</td></tr>';
    }
    var pk=b.side+' '+b.line+' '+(b.stat_label||'');
    var actTxt=b.actual!=null?(' <span style="color:#64748b;font-weight:400;font-size:.72rem">('+b.actual+')</span>'):'';
    return '<tr>'
      +'<td style="white-space:nowrap;color:#94a3b8;font-family:monospace;font-size:.76rem">'+(b.date||'')+'</td>'
      +'<td style="font-weight:600">'+_esc(b.name)+'<div style="font-size:.68rem;color:#64748b">'+_esc(b.category||'')+'</div></td>'
      +'<td style="font-size:.82rem">'+_esc(pk)+'</td>'
      +'<td style="font-family:monospace">'+_betOddsDisp(b.odds)+'</td>'
      +'<td style="font-family:monospace">'+_money(b.stake)+'</td>'
      +'<td style="font-weight:800;color:'+_resColor(res)+'">'+(res==='pending'?'pending':res)+actTxt+'</td>'
      +'<td style="font-family:monospace;font-weight:700;color:'+((b.profit||0)>=0?'#4ade80':'#f87171')+'">'+(b.profit!=null?_money(b.profit):'—')+'</td>'
      +'<td>'+delBtn+'</td>'
      +'</tr>';
  }).join('');
  var rowsHtml=bets.length?'<div style="overflow-x:auto"><table class="grade-table"><thead><tr><th>Date</th><th>Player</th><th>Pick</th><th>Odds</th><th>Stake</th><th>Result</th><th>Profit</th><th></th></tr></thead><tbody>'+rows+'</tbody></table></div>':'<p style="color:#94a3b8;padding:16px">No bets logged yet. Click <strong style="color:#c7d2fe">＋ Track Bet</strong> on any pick card to start.</p>';
  document.getElementById('mybets-body').innerHTML=head+bcHtml+rowsHtml;
}
async function _deleteBet(id){
  if(!confirm('Remove this bet from your log?')) return;
  try{
    var res=await fetch('/api/bets/'+encodeURIComponent(id)+_betAuthQS(),{method:'DELETE'});
    if(!res.ok){ throw new Error(await res.text()); }
    openMyBets(false);
  }catch(e){ alert(e.message||'Delete failed'); }
}
function downloadMyBetsCSV(){
  var d=window.__MYBETS__; if(!d){ alert('Open My Bets first.'); return; }
  var rows=[['Date','Player','Category','Pick','Odds','Stake','Result','Actual','Profit']];
  (d.bets||[]).forEach(function(b){
    if(b.bet_type==='parlay'){
      rows.push([b.date||'',(b.legs||[]).length+'-Leg Parlay','Parlay','Combined',
        b.odds!=null?b.odds:'',b.stake!=null?b.stake:'',b.result||'','',b.profit!=null?b.profit:'']);
      (b.legs||[]).forEach(function(lg){
        rows.push([lg.date||'','  '+(lg.name||''),lg.category||'',(lg.side+' '+lg.line+' '+(lg.stat_label||'')),
          lg.odds!=null?lg.odds:'','',lg.result||'',lg.actual!=null?lg.actual:'','']);
      });
    } else {
      rows.push([b.date||'',b.name||'',b.category||'',(b.side+' '+b.line+' '+(b.stat_label||'')),
        b.odds!=null?b.odds:'',b.stake!=null?b.stake:'',b.result||'',b.actual!=null?b.actual:'',b.profit!=null?b.profit:'']);
    }
  });
  var csv=rows.map(function(row){return row.map(_csvCell).join(',');}).join(String.fromCharCode(13)+String.fromCharCode(10));
  var blob=new Blob([String.fromCharCode(65279)+csv],{type:'text/csv;charset=utf-8;'});
  var url=URL.createObjectURL(blob);
  var a=document.createElement('a'); a.href=url; a.download='mlb-my-bets.csv';
  document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
}
</script>
<div id="grade-card" class="hidden space-y-6" style="max-width:960px;margin:0 auto 24px;padding:0 16px">
  <div class="card p-6">
    <div class="section-hdr" style="color:#60a5fa;margin-bottom:16px">📊 Today's Results</div>
    <div id="grade-spinner" class="hidden" style="color:#94a3b8;font-size:.9rem;margin-bottom:12px;display:flex;align-items:center;gap:8px"><span class="spinner"></span> Fetching box scores…</div>
    <div id="grade-summary"></div>
    <div id="grade-body"></div>
  </div>
</div>
<div id="track-card" class="hidden space-y-6" style="max-width:960px;margin:0 auto 24px;padding:0 16px">
  <div class="card p-6">
    <div class="section-hdr" style="color:#a78bfa;margin-bottom:16px">🏆 Track Record — All-Time &amp; Daily</div>
    <div id="track-spinner" class="hidden" style="color:#94a3b8;font-size:.9rem;margin-bottom:12px;display:flex;align-items:center;gap:8px"><span class="spinner"></span> Grading history…</div>
    <div id="track-alltime"></div>
    <div id="track-daily"></div>
  </div>
</div>
<div id="mybets-card" class="hidden space-y-6" style="max-width:960px;margin:0 auto 24px;padding:0 16px">
  <div class="card p-6">
    <div class="section-hdr" style="color:#a5b4fc;margin-bottom:16px">💰 My Bets — Record &amp; ROI</div>
    <div id="mybets-body"></div>
    <div style="margin-top:18px;padding-top:14px;border-top:1px solid #1e293b;display:flex;align-items:center;gap:12px">
      <button id="mybets-results-btn" onclick="getMyBetsResults()" style="background:#22c55e;color:#0f172a;border:none;border-radius:10px;padding:10px 22px;font-size:.88rem;font-weight:800;cursor:pointer">🔄 Get Results</button>
      <span style="font-size:.78rem;color:#64748b">Fetches box scores and grades all pending bets</span>
      <span id="mybets-spinner-wrap"></span>
    </div>
  </div>
</div>
<footer style="text-align:center;padding:32px 24px;color:#4b5563;font-size:.78rem;border-top:1px solid #1c1c1c;margin-top:24px;font-family:'Source Sans Pro',sans-serif">
  <div style="font-family:'Playfair Display',serif;color:#f59e0b;font-weight:700;font-size:.95rem;margin-bottom:6px">Money Picks Arena</div>
  <div>MLB MoneyBall &middot; Daily Picks</div>
  <div style="margin-top:8px;font-size:.7rem">For entertainment only. Not a betting service. Must be 18+. Please gamble responsibly.</div>
</footer>
</body>
</html>
"""

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    from fastapi.responses import Response
    return Response(status_code=204)

@app.get("/")
async def serve_spa(admin: str = "", token: str = ""):
    import os as _os
    # Admin turns on via EITHER the legacy ?admin=KEY link OR a hub login token
    # whose email matches the admin (so it just works when the owner logs in).
    is_admin = (bool(admin) and admin == _os.environ.get("INTERNAL_API_TOKEN", "__none__")) or _is_admin_token(token)
    body_cls = "is-admin" if is_admin else ""
    js_flag = "true" if is_admin else "false"
    html = _HTML.replace('<body class="min-h-screen">', f'<body class="min-h-screen {body_cls}">').replace(
        "</head>",
        f'<script>window.IS_ADMIN = {js_flag};</script></head>', 1)
    return HTMLResponse(html)


# ── Daily auto-run scheduler ────────────────────────────────────────────────
# Runs the pipeline automatically three times a day so today's cache is warm
# before anyone opens the app — no cold pipeline wait for users (or the admin).
# Needs the always-on (paid) Render plan; on a sleeping free instance the thread
# is paused while the app is asleep. Runs at 11:00, 14:00 and 17:40 ET — the
# 14:00 pass catches starters still TBD at 11:00; the 17:40 pass refreshes odds
# (under-hit & total-bases lines) that books post closer to game time.
import threading as _threading
import time as _time
from datetime import datetime as _datetime

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
    _ET = _ZoneInfo("America/New_York")
except Exception:
    _ET = None  # fall back to server local time if zoneinfo unavailable

_AUTO_RUN_SLOTS = [(11, 0), (14, 0), (17, 40)]   # (hour, minute) in ET
_AUTO_RUN_WINDOW_MIN = 180             # catch-up window after a slot (minutes)
_AUTO_RUN_RETRY_SEC = 300              # wait between retries after a failure
_auto_run_done: set = set()            # slot keys that completed (e.g. "2026-05-29-11-0")
_auto_run_next: dict = {}              # slot key -> monotonic time of next allowed retry


def _auto_run_pipeline(date_str: str, label: str):
    """Run the pipeline for date_str and cache it — same path as a manual run,
    minus the SSE task plumbing. Safe to call from the scheduler thread."""
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from pipeline import run_pipeline
    try:
        print(f"[auto-run] {label} — running pipeline for {date_str}")
        result = run_pipeline(date_str, emit=lambda ev: None)
        # Always persist so the parlay hub's /api/results can serve the slate
        # even with a TBD starter. The MLB app's own load re-runs when has_tbd.
        _cache[date_str] = result
        try: _update_track_ledger()
        except Exception as _le: print(f"[track_ledger] {_le}")
        _save_disk_cache(date_str, result)
        if result.get("stats", {}).get("has_tbd"):
            print(f"[auto-run] {label} — cached {date_str} (has TBD starters; app will re-run on load)")
        else:
            print(f"[auto-run] {label} — cached {date_str}")
        try:
            baked = {**result, "date": date_str}
            inject = (
                '<script>window.__INITIAL_PICKS__ = '
                + json.dumps(baked).replace('</', '<\\/')
                + ';</script></head>'
            )
            snapshot_html = _HTML.replace('</head>', inject, 1)
            push_picks_to_replit("mlb", baked, html=snapshot_html)
        except Exception as _e:
            print(f"[auto-run] replit push failed: {_e}")
        return True  # pipeline completed (cache may be deferred if starters TBD)
    except Exception as exc:
        import traceback
        print(f"[auto-run] {label} failed: {exc}\n{traceback.format_exc()}")
        return False  # real failure — let the loop retry within the window


def _scheduler_loop():
    while True:
        try:
            now = _datetime.now(_ET) if _ET else _datetime.now()
            ds = now.strftime("%Y-%m-%d")
            now_min = now.hour * 60 + now.minute
            mono = _time.monotonic()
            for (h, m) in _AUTO_RUN_SLOTS:
                key = f"{ds}-{h}-{m}"
                if key in _auto_run_done:
                    continue
                # Catch-up window: fire any time from the slot until N minutes
                # later, so a deploy/restart/delay near the slot minute doesn't
                # skip the day's run entirely.
                elapsed = now_min - (h * 60 + m)
                if not (0 <= elapsed <= _AUTO_RUN_WINDOW_MIN):
                    continue
                # After a failure we back off before retrying within the window.
                if mono < _auto_run_next.get(key, 0):
                    continue
                if _auto_run_pipeline(ds, f"{h:02d}:{m:02d} ET"):
                    _auto_run_done.add(key)       # done for the day (success)
                else:
                    _auto_run_next[key] = mono + _AUTO_RUN_RETRY_SEC  # retry soon
            # keep the tracking sets from growing forever — drop anything not today
            # (mutate in place — never rebind these module-level names here)
            if len(_auto_run_done) > 50:
                _auto_run_done.intersection_update({k for k in _auto_run_done if k.startswith(ds)})
            if len(_auto_run_next) > 50:
                for _k in [k for k in _auto_run_next if not k.startswith(ds)]:
                    _auto_run_next.pop(_k, None)
        except Exception as _e:
            print(f"[scheduler] loop error: {_e}")
        _time.sleep(30)


@app.on_event("startup")
def _start_auto_run_scheduler():
    t = _threading.Thread(target=_scheduler_loop, name="mlb-autorun", daemon=True)
    t.start()
    print("[scheduler] auto-run thread started — slots 11:00, 14:00 & 17:40 ET")
