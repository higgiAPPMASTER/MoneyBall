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
_ADMIN_EMAIL = _os.environ.get("ADMIN_EMAIL", "higgi117711@gmail.com").strip().lower()


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
    return bool(_ADMIN_EMAIL) and _token_email(token) == _ADMIN_EMAIL


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
    if not force and date_str in _cache:
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
            # Don't freeze the cache while any starter is still TBD — let the
            # next load rebuild so a late-named starter gets picked up.
            if not result.get("stats", {}).get("has_tbd"):
                _cache[date_str] = result
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
        if signal >= 2:
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
    .gap-4 { gap: 16px; } .hidden { display: none; } .w-full { width: 100%; }
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
    .mlb-picks-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:16px}
    .parlay-cat-row{display:flex;align-items:center;gap:8px;padding:4px 2px;cursor:pointer;font-size:.78rem;color:#ddd;user-select:none}
    .parlay-cat-row input{cursor:pointer;width:15px;height:15px;accent-color:#f59e0b}
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
  </nav>
  <main class="flex-1 px-4 py-6 max-w-7xl mx-auto w-full space-y-6">
    <div style="text-align:center;margin-bottom:32px">
      <h1 style="font-family:'Playfair Display',serif;font-size:2.6rem;font-weight:900;color:#fff;margin-bottom:6px">MLB <span style="color:#f59e0b">MoneyBall</span></h1>
      <p style="font-size:.85rem;color:#6b7280;letter-spacing:.15em;text-transform:uppercase">MLB Daily Picks</p>
    </div>
    <div class="run-box" id="runBox" style="text-align:center;max-width:600px;margin:0 auto 20px">
      <h2 style="font-family:'Playfair Display',serif;font-size:1.5rem;font-weight:700;color:#fff;margin-bottom:20px">Run Today's Picks</h2>
      <div class="date-row" style="justify-content:center;margin-bottom:20px">
        <label>Date</label>
        <input type="date" id="date-picker" max=""/>
      </div>
      <div style="text-align:center;margin-bottom:12px">
        <button class="btn-primary" id="get-btn" onclick="getPicks()">🎯 Get Picks</button>
        <button class="btn-primary admin-only" id="run-btn" onclick="startRun()" style="margin-left:10px">Run Picks</button>
        <button class="btn-primary admin-only" id="force-btn" onclick="startRun(true)" style="margin-left:10px;background:#dc2626;color:#fff" title="Bypass cache and rebuild today's picks from scratch">Force Refresh</button>
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
            <option>2</option><option>3</option><option>4</option><option>5</option><option>6</option><option>7</option><option>8</option><option>9</option><option>10</option>
          </select>
          <button class="btn-primary" onclick="buildParlay()">Build Best Parlay</button>
          <button class="btn-primary" onclick="generateParlay()" style="background:#1f2937;color:#fff">🎲 Generate New</button>
          <button class="btn-primary" id="parlay-overs-btn" onclick="toggleParlayOvers()" style="background:#1f2937;color:#fff">&#11014; Overs Only</button>
          <button class="btn-primary" id="parlay-unders-btn" onclick="toggleParlayUnders()" style="background:#1f2937;color:#fff">&#11015; Unders Only</button>
          <div style="position:relative;display:inline-block">
            <button class="btn-primary" id="parlay-cats-btn" onclick="toggleCatMenu(event)" style="background:#1f2937;color:#fff">&#9776; Categories (8/8) &#9662;</button>
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
              </div>
            </div>
          </div>
        </div>
        <div id="parlayResult" style="margin-top:16px"></div>
      </div>
      <div class="card p-6" id="player-search-card">
        <div class="section-hdr">🔍 Player Lookup</div>
        <p class="text-xs text-slate-400 mb-3">Type a hitter or pitcher's name — see where they rank and why.</p>
        <input id="player-search-input" type="text" placeholder="e.g. Aaron Judge, Gerrit Cole..."
               style="width:100%;padding:12px 16px;background:#0f0f0f;border:1px solid #262626;border-radius:10px;color:#fff;font-size:.95rem;outline:none"
               oninput="runPlayerSearch(this.value)">
        <div id="player-search-result" class="mt-3"></div>
      </div>
      <div class="card p-6" id="top-picks-card">
        <div class="section-hdr">🏆 Top Picks — To Record a Hit</div>
        <div id="picks-body" class="mlb-picks-grid"></div>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>S1</strong> Lifetime BA vs today's pitcher (FIC) &nbsp;|&nbsp;
          <strong>S2</strong> Lifetime H/A BA vs today's opponent &nbsp;|&nbsp;
          <strong>S3</strong> 2026 season H/A BA vs all teams &nbsp;|&nbsp;
          <strong>S4</strong> Last 10 H/A games vs THIS opponent — games with 1+ hit (sets the rank order) &nbsp;|&nbsp;
          <strong>Hit Odds</strong> Sportsbook price "to record a hit" (0.5 line) &nbsp;|&nbsp;
          <strong>S5</strong> Day/night BA for tonight's game type &nbsp;|&nbsp;
          <strong>Score</strong> = (S1+S2+S3+S5)×1000 &nbsp;|&nbsp; <strong>Rank</strong> = S4 hit rate, ties → more games
        </p>
      </div>
      <div class="card p-6 hidden" id="also-ran-card">
        <div class="section-hdr">⚾ Money Ball Picks</div>
        <p class="text-xs text-slate-400 mb-3" style="margin-top:-8px">Solid plays the model still likes.</p>
        <div id="also-ran-body" class="mlb-picks-grid"></div>
        <p class="text-xs text-slate-500 mt-3 admin-only">These players passed all 5 steps — ranked by score.</p>
      </div>
      <div class="card p-6 hidden" id="under-picks-card" style="border-color:rgba(255,107,107,.25)">
        <div class="section-hdr" style="color:#ff8a65">⬇️ Under Picks — Bet Under 1.5 Hits</div>
        <div id="under-picks-body" class="mlb-picks-grid"></div>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>Source</strong>: The Odds API — players with 1.5 hits O/U line &nbsp;|&nbsp;
          <strong>S1</strong> Career BA vs today's pitcher (under &lt; .250, N/A passes) &nbsp;|&nbsp;
          <strong>S2</strong> Lifetime H/A BA vs today's opponent (under &lt; .225) &nbsp;|&nbsp;
          <strong>S3</strong> 2026 H/A BA (under &lt; .250) &nbsp;|&nbsp;
          <strong>L7</strong> Last 7 games BA — must be under .250 &nbsp;|&nbsp;
          <strong>Ranked #1 → coldest bat (S2 + S3 + L7)</strong>
        </p>
      </div>
      <div class="card p-6 hidden" id="pitcher-k-card" style="border-color:rgba(99,202,183,.25)">
        <div class="section-hdr" style="color:#63cab7">⚾ Pitcher Picks — Strikeouts, Hits, Outs &amp; Earned Runs</div>
        <p class="text-xs text-slate-400 mb-3" style="margin-top:-4px">Cards are ranked by the strikeout pick. Click any pitcher to see all 4 markets — Strikeouts, Hits Allowed, Outs, Earned Runs — each with its Over/Under, line and game-by-game form.</p>
        <div id="pitcher-k-body" class="mlb-picks-grid"></div>
        <details class="mt-4" id="pitcher-k-nopick-details">
          <summary class="cursor-pointer text-xs text-slate-500 select-none">▸ All today's pitchers (no qualifying pick)</summary>
          <table class="results-table mt-2" id="pitcher-k-nopick-table">
            <thead><tr><th>Pitcher</th><th>vs (H/A)</th><th>K Line</th><th>Avg K vr OPP</th><th>Note</th></tr></thead>
            <tbody id="pitcher-k-nopick-body"></tbody>
          </table>
        </details>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>K History</strong> = H/A starts vs today's opponent only &nbsp;|&nbsp;
          <strong>Pick</strong> = OVER/UNDER based on blended avg (50% career H/A vs opp + 50% last 5 starts). ⚠️ = signals conflict. Min 1 career start vs opp.
        </p>
      </div>
      <div class="card p-6 hidden" id="runs-picks-card" style="border-color:rgba(96,165,250,.25)">
        <div class="section-hdr" style="color:#60a5fa">🏃 Runs Picks — Score a Run (Over / Under 0.5)</div>
        <p class="text-xs text-slate-400 mb-3" style="margin-top:-4px">Who's likely to cross the plate. Runs are lower-frequency than hits, so treat these as higher-variance plays.</p>
        <div id="runs-picks-body" class="mlb-picks-grid"></div>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>Runs Rate vr Opp</strong> = last 10 H/A games vs THIS opponent with 1+ run (falls back to L10 H/A any opp when no head-to-head) &nbsp;|&nbsp;
          <strong>Pick</strong> = OVER when the rate is high, UNDER when low &nbsp;|&nbsp; ranked by Wilson lower-bound so proven samples beat thin lucky ones.
        </p>
      </div>
      <div id="pitcher-props-wrap"></div>
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
  r.top9=f(r.top9); r.also_ran=f(r.also_ran); r.under_picks=f(r.under_picks); r.runs_picks=f(r.runs_picks);
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
  hide('also-ran-card'); hide('under-picks-card'); hide('pitcher-k-card'); hide('runs-picks-card');

  document.getElementById('stats-row').innerHTML = [
    statCard('🎯','Top Picks',top9.length),
    statCard('⬇️','Under Picks',(view.under_picks||[]).length),
    statCard('🏃','Runs Picks',(view.runs_picks||[]).length),
    statCard('⚾','Pitcher K',((view.pitcher_k||{}).all||[]).filter(p=>p.pick&&(p.starts||0)>0).length),
    statCard('🧮','Pitcher Props',PROP_ORDER.reduce((n,m)=>n+(((view.pitcher_props||{})[m]||{}).picks||[]).length,0)),
    statCard('⚾','Games Today',stats.games),
    statCard('🔍','Players Run',stats.step1_count),
    statCard('⏱️','Time (s)',stats.elapsed),
  ].join('');

  if (window.UNDERS_ONLY && window.IS_ADMIN) { hide('top-picks-card'); } else { show('top-picks-card'); }
  window.__HIT_REG__={};
  document.getElementById('picks-body').innerHTML = top9.map((p,i) => _mlbCard(p, i+1)).join('');

  const alsoRan = view.also_ran || [];
  if (alsoRan.length > 0) {
    show('also-ran-card');
    document.getElementById('also-ran-body').innerHTML = alsoRan.map((p,i) => _mlbCard(p, i+11, true)).join('');
  }

  const underPicks = view.under_picks || [];
  if (underPicks.length > 0) {
    show('under-picks-card');
    document.getElementById('under-picks-body').innerHTML = underPicks.map((p,i) => _underCard(p, i+1)).join('');
  }

  const pkData=view.pitcher_k||{}, pkAll=pkData.all||[];
  if (pkAll.length > 0) {
    show('pitcher-k-card');
    const pkSorted = pkAll.filter(p=>p.pick && (p.starts||0)>0).sort((a,b)=>{
      const ga=Math.abs((a.blended_avg_k!=null?a.blended_avg_k:(a.avg_k||0))-(a.line||0));
      const gb=Math.abs((b.blended_avg_k!=null?b.blended_avg_k:(b.avg_k||0))-(b.line||0));
      return gb-ga;
    });
    window.__PK_REG__={};
    document.getElementById('pitcher-k-body').innerHTML = pkSorted.length > 0
      ? pkSorted.map((p,_i) => _pitcherCard(p, _i+1)).join('')
      : '<p class="text-slate-500 text-center" style="padding:16px">No qualifying picks today</p>';
    const pkNoPick=pkAll.filter(p=>!p.pick);
    const npDet=document.getElementById('pitcher-k-nopick-details');
    if (npDet) {
      if (pkNoPick.length>0) {
        npDet.style.display='';
        document.getElementById('pitcher-k-nopick-body').innerHTML=pkNoPick.map((p,_j)=>{
          const sideCls2=p.side==='HOME'?'badge-home':'badge-away';
          const _k2='pn'+_j; window.__PK_REG__[_k2]=p;
          return `<tr onclick="_pkForm('${_k2}')" style="cursor:pointer" title="Click for recent form">
            <td class="font-semibold">${p.name} <span style="color:#64748b;font-size:.7rem">▾</span></td>
            <td><span class="badge ${sideCls2}">${p.side}</span> <span class="text-slate-400 text-xs">${p.opp||''}</span></td>
            <td style="font-family:monospace;font-weight:700;color:#fff">${p.line!=null?p.line+' Ks':'—'}</td>
            <td style="font-family:monospace;color:#94a3b8">${p.avg_k!=null?p.avg_k+' K':'—'}</td>
            <td style="color:#94a3b8;font-size:.78rem">${p.pick_note||'—'}</td>
          </tr>`;
        }).join('');
      } else { npDet.style.display='none'; }
    }
  }

  const runsPicks = view.runs_picks || [];
  if (runsPicks.length > 0) {
    show('runs-picks-card');
    window.__RUNS_REG__={};
    document.getElementById('runs-picks-body').innerHTML = runsPicks.map((p,i) => _runsCard(p, i+1)).join('');
  }

  renderPitcherProps(view);
  renderByGame(view);
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
    var hCell=usingVs?`<td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:#fca5a5">${g.h!=null?g.h+' H':''}</td>`:'';
    return `<tr>
      <td style="padding:6px 10px;color:#94a3b8;font-family:monospace">${g.d||'—'}</td>
      ${oppCell}
      <td style="padding:6px 10px;color:#93c5fd;font-family:monospace;font-size:.8rem">${g.ip?(g.ip+' IP'):''}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:${clr}">${kv} K</td>
      ${hCell}
    </tr>`;
  }).join(''):'<tr><td colspan="4" style="padding:14px;color:#64748b;text-align:center">No starts on record</td></tr>';
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
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:${clr}">${kv} K</td>
    </tr>`;
  }).join(''):'';
  var recentSection=(usingVs&&recentRows)?`
    <div style="margin-top:18px;font-size:.72rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:8px">Last ${rlog.length} Starts (any opp)</div>
    <table style="width:100%;border-collapse:collapse;font-size:.85rem"><thead><tr><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">Date</th><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">Opp</th><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">IP</th><th style="text-align:right;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">K</th></tr></thead><tbody>${recentRows}</tbody></table>`:'';
  var histTitle=usingVs?('Starts vs '+(p.opp||'opp')+' — Ks & Hits allowed'):('Last '+(log.length||0)+' Starts (any opp)');
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
  function _mkRow(lbl,ln,bl,unit,pk,od,key,clickable){
    var pc=pk==='OVER'?'#63cab7':(pk==='UNDER'?'#ff8a65':'#64748b');
    var odStr=od!=null?((od>0?'+':'')+od):'';
    var pickStr=pk?(pk+(odStr?(' '+odStr):'')):'\u2014';
    var clk=(clickable&&key)?(' onclick="_ppForm(&#39;'+key+'&#39;)" style="cursor:pointer" title="Game-by-game log"'):'';
    var caret=(clickable&&key)?' <span style="color:#64748b;font-size:.62rem">\u25be</span>':'';
    return '<tr'+clk+'><td style="padding:5px 8px;color:#e2e8f0;font-weight:600">'+lbl+caret+'</td>'
      +'<td style="padding:5px 8px;font-family:monospace;color:#fff">'+(ln!=null?ln:'\u2014')+'</td>'
      +'<td style="padding:5px 8px;font-family:monospace;color:#cbd5e1">'+(bl!=null?(bl+(unit?(' '+unit):'')):'\u2014')+'</td>'
      +'<td style="padding:5px 8px;font-weight:800;color:'+pc+'">'+pickStr+'</td></tr>';
  }
  var _kHasSugg=p.sugg_line!=null;
  var _kLine=_kHasSugg?p.sugg_line:p.line;
  var _kPick=_kHasSugg?'OVER':p.pick;
  var _kOd=_kHasSugg?p.sugg_odds:(p.pick==='OVER'?p.over_odds:(p.pick==='UNDER'?p.under_odds:null));
  var _kBl=(p.blended_avg_k!=null?p.blended_avg_k:p.avg_k);
  var mkBody=_mkRow('Strikeouts',_kLine,_kBl,'K',_kPick,_kOd,'',false);
  [['pitcher_hits_allowed','Hits Allowed'],['pitcher_outs','Outs'],['pitcher_earned_runs','Earned Runs']].forEach(function(mm){
    var e=_mk[mm[0]];
    if(e&&e.obj){ var o=e.obj; var od=o.pick==='OVER'?o.over_odds:(o.pick==='UNDER'?o.under_odds:null);
      mkBody+=_mkRow(mm[1],o.line,o.blended,(o.unit?String(o.unit).trim():''),o.pick,od,e.key,true);
    } else { mkBody+=_mkRow(mm[1],null,null,'',null,null,'',false); }
  });
  var mkTable='<div style="font-size:.72rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:6px">All 4 Markets</div>'
    +'<table style="width:100%;border-collapse:collapse;font-size:.82rem;margin-bottom:16px;border-bottom:1px solid #1e293b">'
    +'<thead><tr><th style="text-align:left;padding:4px 8px;color:#64748b;font-size:.66rem;font-weight:600">Market</th><th style="text-align:left;padding:4px 8px;color:#64748b;font-size:.66rem;font-weight:600">Line</th><th style="text-align:left;padding:4px 8px;color:#64748b;font-size:.66rem;font-weight:600">Blend</th><th style="text-align:left;padding:4px 8px;color:#64748b;font-size:.66rem;font-weight:600">Pick</th></tr></thead>'
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
      <table style="width:100%;border-collapse:collapse;font-size:.85rem">${usingVs?'<thead><tr><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">Date</th><th style="text-align:left;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">IP</th><th style="text-align:right;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">K</th><th style="text-align:right;padding:4px 10px;color:#64748b;font-size:.68rem;font-weight:600">Hits</th></tr></thead>':''}<tbody>${rows}</tbody></table>
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
};
var PROP_ORDER = ['pitcher_hits_allowed','pitcher_outs','pitcher_earned_runs'];
function _ppU(p){ return p && p.unit ? (' '+String(p.unit).trim()) : ''; }
function renderPitcherProps(view){
  var wrap=document.getElementById('pitcher-props-wrap'); if(!wrap) return;
  var props=(view&&view.pitcher_props)||{};
  window.__PP_REG__={}; window.__PP_BY_NAME__={};
  // Index every market's entries by pitcher name so the single Pitcher box
  // popup (_pkForm) can show all 4 markets (K + Hits Allowed + Outs + Earned
  // Runs) at once. Prefer a qualifying pick over a no-pick entry on collisions.
  var _ppN=0;
  PROP_ORDER.forEach(function(_mkt){
    ((props[_mkt]||{}).all||[]).forEach(function(_p){
      var _nm=String(_p.name||'').toLowerCase().trim(); if(!_nm) return;
      var _key='pp'+(_ppN++); window.__PP_REG__[_key]=_p;
      if(!window.__PP_BY_NAME__[_nm]) window.__PP_BY_NAME__[_nm]={};
      var _ex=window.__PP_BY_NAME__[_nm][_mkt];
      if(!_ex || (!_ex.obj.pick && _p.pick)) window.__PP_BY_NAME__[_nm][_mkt]={obj:_p,key:_key};
    });
  });
  // Consolidated into the single Pitcher box popup (_pkForm shows all 4 markets);
  // the 3 separate prop sections are no longer drawn. The per-name index above
  // (window.__PP_BY_NAME__) and r.pitcher_props still feed the popup, parlay,
  // CSV and by-game views, so nothing downstream is affected.
  wrap.innerHTML='';
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
  (r.runs_picks||[]).forEach(function(p,i){
    var isOver=p.pick==='OVER';
    var od=isOver?p.over_odds:p.under_odds;
    cands.push({type:'RUN',dir:p.pick,player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Runs',line:(p.line!=null?p.line:0.5),odds:(od!=null?od:''),conf:clampConf(80,i),reason:'🏃 '+p.pick+' '+(p.line!=null?p.line:0.5)+' runs · '+(p.rate_disp||'')+' vs '+(p.opp||''),src:p});
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
  // Parlay-builder category checkboxes — keep only legs whose category is checked.
  if(window.PARLAY_CATS){ cands=cands.filter(function(c){ return window.PARLAY_CATS[_legCat(c)]!==false; }); }
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
  var dec=1, priced=0, missing=0;
  legs.forEach(function(l){ if(l.dec){dec*=l.dec;priced++;}else{missing++;} });
  var am = priced? _decToAm(dec) : null;
  var payout = priced? (100*dec) : null;
  var dirColor=function(d){return d==='OVER'?'#63cab7':d==='UNDER'?'#ff8a65':'#9ca3af';};
  var tagBg={HIT:'rgba(245,158,11,.16)',UNDER:'rgba(255,138,101,.16)',K:'rgba(99,202,183,.16)',RUN:'rgba(96,165,250,.16)',pitcher_hits_allowed:'rgba(248,113,113,.16)',pitcher_outs:'rgba(167,139,250,.16)',pitcher_earned_runs:'rgba(251,146,60,.16)'};
  var tagFg={HIT:'#f59e0b',UNDER:'#ff8a65',K:'#63cab7',RUN:'#60a5fa',pitcher_hits_allowed:'#f87171',pitcher_outs:'#a78bfa',pitcher_earned_runs:'#fb923c'};
  var tagLbl={HIT:'HIT',UNDER:'UNDER 1.5',K:'PITCHER K',RUN:'RUNS',pitcher_hits_allowed:'HITS ALLOWED',pitcher_outs:'OUTS',pitcher_earned_runs:'EARNED RUNS'};
  var rows=legs.map(function(l,idx){var fo=_fmtOdds(l.odds);return '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid #1a1a1a">'
    +'<div style="min-width:0">'
    +'<div style="font-weight:800;color:#fff;font-size:.85rem">'+(idx+1)+'. '+_nameSpan(l.src,l.player)+' <span style="color:#777;font-size:.7rem">'+(l.team?l.team+' ':'')+'vs '+l.opp+'</span> <span style="background:'+(tagBg[l.type]||'#222')+';color:'+(tagFg[l.type]||'#aaa')+';padding:1px 6px;border-radius:4px;font-size:.6rem;font-weight:800">'+(tagLbl[l.type]||l.type)+'</span></div>'
    +'<div style="color:#999;font-size:.72rem;margin-top:2px">'+l.reason+'</div>'
    +'</div>'
    +'<div style="text-align:right;white-space:nowrap">'
    +'<div style="color:'+dirColor(l.dir)+';font-weight:900;font-size:.8rem">'+l.dir+' '+l.stat+'</div>'
    +'<div style="color:#fbbf24;font-size:.72rem;font-weight:800">'+(fo||'odds N/A')+'</div>'
    +'</div></div>';}).join('');
  var header='<div style="display:flex;justify-content:space-between;align-items:center;padding:8px 12px;border-bottom:1px solid #262626;background:#121212">'
    +'<span style="font-weight:800;color:#ccc;font-size:.74rem">'+(randomize?'RANDOM MIX':'TOP PLAYS')+'</span>'
    +'<span onclick="closeParlay()" title="Close" style="cursor:pointer;color:#888;font-weight:900;font-size:1.15rem;line-height:1;padding:0 6px">×</span></div>';
  var summary='<div style="display:flex;justify-content:space-between;align-items:center;padding:12px;background:linear-gradient(135deg,rgba(245,158,11,.12),rgba(245,158,11,.02));border-top:1px solid #262626">'
    +'<div style="font-weight:900;color:#f59e0b">'+n+'-LEG PARLAY</div>'
    +'<div style="text-align:right">'+(am?('<div style="font-weight:900;color:#63cab7;font-size:1.05rem">'+am+'</div><div style="color:#999;font-size:.7rem">$100 → $'+payout.toFixed(2)+(missing?(' · '+priced+'/'+n+' legs priced'):'')+'</div>'):('<div style="color:#888;font-size:.78rem">No book odds available for these legs</div>'))+'</div>'
    +'</div>';
  out.innerHTML='<div style="background:#0e0e0e;border:1px solid #262626;border-radius:12px;overflow:hidden">'+header+rows+summary+'</div>';
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
// Parlay category checkboxes — which pick categories feed the parlay pool (all on by default).
window.PARLAY_CATS = {HIT:true,UNDER_HITS:true,UNDER_TB:true,K:true,RUN:true,pitcher_hits_allowed:true,pitcher_outs:true,pitcher_earned_runs:true};

// Paints both Overs Only / Unders Only buttons to match their toggle state.
function _paintParlayDirBtns(){
  var u=document.getElementById('parlay-unders-btn');
  if(u){ u.style.background=window.PARLAY_UNDERS?'#ff8a65':'#1f2937'; u.style.color=window.PARLAY_UNDERS?'#0e0e0e':'#fff'; }
  var o=document.getElementById('parlay-overs-btn');
  if(o){ o.style.background=window.PARLAY_OVERS?'#63cab7':'#1f2937'; o.style.color=window.PARLAY_OVERS?'#0e0e0e':'#fff'; }
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
// Close the categories dropdown when clicking anywhere outside it.
document.addEventListener('click', function(e){
  var m=document.getElementById('parlay-cats-menu'); if(!m||m.style.display!=='block') return;
  var btn=document.getElementById('parlay-cats-btn');
  if(m.contains(e.target) || (btn&&btn.contains(e.target))) return;
  m.style.display='none';
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
    box.innerHTML='<div class="text-slate-500 text-sm" style="margin-bottom:10px">"<strong>'+raw+'</strong>" isn\\'t in today\\'s analyzed picks. Check any hitter in today\\'s games for a quick hit verdict:</div>'
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
        html+='<div style="margin-top:8px;color:#cbd5e1;font-size:.82rem">Cold bat vs today\\'s pitcher — model likes the UNDER.</div>';
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
  out.innerHTML='<div class="text-slate-500 text-sm">Checking '+name+' across today\\'s games…</div>';
  try{
    var r=await fetch('/api/lookup?name='+encodeURIComponent(name)+'&date_str='+encodeURIComponent(date));
    var d=await r.json();
    if(!d.found){ out.innerHTML='<div class="text-slate-400 text-sm">'+(d.msg||'No match.')+'</div>'; return; }
    if(d.verdict==='NOT_PLAYING'){ out.innerHTML='<div class="text-slate-400 text-sm">'+(d.msg||'')+'</div>'; return; }
    var color=d.verdict==='GOOD'?'#22c55e':d.verdict==='DECENT'?'#fbbf24':(d.verdict==='UNKNOWN'||d.verdict==='INSUFFICIENT')?'#9ca3af':'#ef4444';
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
  var hitters=(result.top9||[]).map(function(p){return Object.assign({_kind:'HITTER'},p);});
  var unders=(result.under_picks||[]).map(function(p){return Object.assign({_kind:'UNDER'},p);});
  var ks=((result.pitcher_k||{}).picks||[]).filter(function(p){return (p.starts||0)>0;}).map(function(p){return Object.assign({_kind:'PITCHER K'},p);});
  var runs=(result.runs_picks||[]).map(function(p){return Object.assign({_kind:'RUNS'},p);});
  var propLegs=[];
  var _ppBG=(result.pitcher_props)||{};
  PROP_ORDER.forEach(function(mkt){
    var cfg=PROP_CFG[mkt]; var picks=((_ppBG[mkt]||{}).picks)||[];
    var statLbl=(cfg.label||'').replace('Pitcher ','').toUpperCase();
    picks.forEach(function(p){ propLegs.push(Object.assign({_kind:statLbl},p)); });
  });
  var all=hitters.concat(unders, ks, runs, propLegs);
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
      if(kind==='HITTER') note='Top hitter pick';
      else if(kind==='UNDER') note='UNDER — vs '+(p.pitcher||'TBD');
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
function _mlbCard(p, rank, dim) {
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const headshot = p.player_id ? `https://a.espncdn.com/i/headshots/mlb/players/full/${p.player_id}.png` : '';
  const rnkColors = rank===1?['#f59e0b','#000']:rank===2?['#c0c0c0','#000']:rank===3?['#cd7f32','#fff']:['#1e1e1e','#f59e0b'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const odds = p.hit_odds!=null?(p.hit_odds>0?'+':'')+p.hit_odds:'—';
  const s1Disp = p.s1!=null?p.s1.toFixed(3):'—';
  const s4Disp = p.s4?.display||'—';
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>S1 <strong style="color:#94a3b8">${s1Disp}</strong></span> &nbsp;
    <span>S2 <strong style="color:#94a3b8">${p.s2?.display||'—'}</strong></span> &nbsp;
    <span>S3 <strong style="color:#94a3b8">${p.s3?.display||'—'}</strong></span><br>
    <span>S4 <strong style="color:#94a3b8">${s4Disp}</strong></span> &nbsp;
    <span>Score <strong style="color:#f59e0b">${p.total||'—'}</strong></span>
  </div>`;
  window.__HIT_REG__=window.__HIT_REG__||{}; window.__HIT_REG__['h'+rank]=p;
  return `<div class="mlb-pick-card" onclick="_hitForm('h${rank}')" title="Click for recent form" style="cursor:pointer;${dim?'opacity:0.85':''}">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#1a2a1a 0%,#0a1a0a 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:30px;height:30px;border-radius:50%;background:${rnkColors[0]};color:${rnkColors[1]};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.9rem">${rank}</div>
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
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:2px">
        <span style="font-size:.78rem;color:#64748b">${p.pitcher?'vs '+p.pitcher:''}</span>
        ${lineupBadge(p.lineup_status)}
      </div>
      ${p.blurb ? `<div style="margin-top:5px;font-size:.72rem;color:#94a3b8;line-height:1.5;font-style:italic">${p.blurb}</div>` : ''}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">Hit Odds</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odds}</span>
      </div>
      ${adminStats}
    </div>
  </div>`;
}

function _underCard(p, rank) {
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const rnkColors = rank===1?['#ff8a65','#000']:rank===2?['#fb7185','#000']:rank===3?['#f87171','#000']:['#1e1e1e','#ff8a65'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const uOdds = p.under_odds!=null?(p.under_odds>0?'+':'')+p.under_odds:'—';
  const tbOdds = p.tb_under_odds!=null?(p.tb_under_odds>0?'+':'')+p.tb_under_odds:'—';
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>S1 <strong style="color:#94a3b8">${p.s1_disp||'—'}</strong> <span style="color:#475569">(${p.s1_ab||0}AB)</span></span> &nbsp;
    <span>S2 <strong style="color:#94a3b8">${p.s2?.display||'—'}</strong></span><br>
    <span>S3 <strong style="color:#94a3b8">${p.s3?.display||'—'}</strong></span> &nbsp;
    <span>L7 <strong style="color:#94a3b8">${p.l7?.display||'—'}</strong></span> &nbsp;
    <span>Score <strong style="color:#ff8a65">${p.under_score||'—'}</strong></span>
  </div>`;
  window.__HIT_REG__=window.__HIT_REG__||{}; window.__HIT_REG__['u'+rank]=p;
  return `<div class="mlb-pick-card" onclick="_hitForm('u${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#2a1414 0%,#180808 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:30px;height:30px;border-radius:50%;background:${rnkColors[0]};color:${rnkColors[1]};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.9rem">${rank}</div>
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
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:2px">
        <span style="font-size:.78rem;color:#64748b">${p.pitcher?'vs '+p.pitcher:''}</span>
        ${lineupBadge(p.lineup_status)}
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:#ff8a65;font-weight:800">U 1.5 Hits</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${uOdds}</span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">U 1.5 Total Bases</span>
        <span style="font-family:monospace;color:#63cab7;font-weight:700;font-size:.9rem">${tbOdds}</span>
      </div>
      ${adminStats}
    </div>
  </div>`;
}

function _runsCard(p, rank) {
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
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>Score <strong style="color:#60a5fa">${p.score!=null?p.score+'%':'—'}</strong></span> &nbsp;
    <span>Games <strong style="color:#94a3b8">${p.games||0}</strong></span> &nbsp;
    <span>Wilson <strong style="color:#94a3b8">${p.wilson!=null?p.wilson:'—'}</strong></span>
  </div>`;
  window.__RUNS_REG__=window.__RUNS_REG__||{}; window.__RUNS_REG__['rn'+rank]=p;
  return `<div class="mlb-pick-card" onclick="_runsForm('rn${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#0e1f33 0%,#08111d 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:30px;height:30px;border-radius:50%;background:${rnkColors[0]};color:${rnkColors[1]};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.9rem">${rank}</div>
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
      ${adminStats}
    </div>
  </div>`;
}

function _pitcherCard(p, rank) {
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
  window.__PK_REG__=window.__PK_REG__||{}; window.__PK_REG__['pk'+rank]=p;
  return `<div class="mlb-pick-card" onclick="_pkForm('pk${rank}')" title="Click for all 4 markets" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#0f2420 0%,#08160f 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:30px;height:30px;border-radius:50%;background:${rnkColors[0]};color:${rnkColors[1]};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.9rem">${rank}</div>
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
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">K Line ${p.line!=null?p.line:'—'}</span>
        <span style="color:${pickClr};font-weight:900;font-size:1rem">${pickLabel}</span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:3px">
        <span style="font-size:.72rem;color:#64748b">Blend ${blDisp}</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.9rem">${odds||'—'}</span>
      </div>
      <div style="margin-top:5px;font-size:.7rem;color:#94a3b8">Avg K <strong style="color:#cbd5e1">${p.avg_k!=null?p.avg_k:'—'}</strong> · IP <strong style="color:#cbd5e1">${p.avg_ip!=null?p.avg_ip:'—'}</strong> · ERA <strong style="color:#cbd5e1">${p.era||'—'}</strong> · H <strong style="color:#cbd5e1">${p.avg_hits!=null?p.avg_hits:'—'}</strong> <span style="color:#64748b">vr opp</span></div>
      <div style="margin-top:5px;font-size:.66rem;color:#63cab7;text-align:right">all 4 markets →</div>
    </div>
  </div>`;
}

function statCard(icon,label,value) {
  return `<div class="chip"><div class="val">${value}</div><div class="lbl">${label}</div></div>`;
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
</script>
<footer style="text-align:center;padding:32px 24px;color:#4b5563;font-size:.78rem;border-top:1px solid #1c1c1c;margin-top:24px;font-family:'Source Sans Pro',sans-serif">
  <div style="font-family:'Playfair Display',serif;color:#f59e0b;font-weight:700;font-size:.95rem;margin-bottom:6px">Money Picks Arena</div>
  <div>MLB MoneyBall &middot; Daily Picks</div>
  <div style="margin-top:8px;font-size:.7rem">For entertainment only. Not a betting service. Must be 18+. Please gamble responsibly.</div>
</footer>
</body>
</html>
"""

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
        # Don't freeze the cache while any starter is still TBD — let the next
        # load (or the next slot) rebuild so a late-named starter gets picked up.
        if not result.get("stats", {}).get("has_tbd"):
            _cache[date_str] = result
            _save_disk_cache(date_str, result)
            print(f"[auto-run] {label} — cached {date_str}")
        else:
            print(f"[auto-run] {label} — {date_str} still has TBD starters; not frozen")
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
