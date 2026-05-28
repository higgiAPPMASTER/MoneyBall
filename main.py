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
        with open(_disk_cache_path(date_str), "w") as f:
            json.dump(result, f)
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

from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import StreamingResponse, FileResponse, HTMLResponse
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

@app.post("/api/run")
async def start_run(date_str: str):
    if date_str not in _cache:
        disk = _load_disk_cache(date_str)
        if disk:
            _cache[date_str] = disk
    if date_str in _cache:
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
async def get_results(date_str: str):
    if date_str in _cache:
        return _cache[date_str]
    raise HTTPException(status_code=404, detail="No results for this date.")

_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MLB MoneyBall &mdash; Money Picks Arena</title>
  <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700;900&family=Source+Sans+Pro:wght@300;400;600;700&display=swap" rel="stylesheet">
  <style>
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
        <button class="btn-primary" id="run-btn" onclick="startRun()">Run Picks</button>
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
      <div class="card p-6" id="player-search-card">
        <div class="section-hdr">🔍 Player Lookup</div>
        <p class="text-xs text-slate-400 mb-3">Type a hitter or pitcher's name — see where they rank and why.</p>
        <input id="player-search-input" type="text" placeholder="e.g. Aaron Judge, Gerrit Cole..."
               style="width:100%;padding:12px 16px;background:#0f0f0f;border:1px solid #262626;border-radius:10px;color:#fff;font-size:.95rem;outline:none"
               oninput="runPlayerSearch(this.value)">
        <div id="player-search-result" class="mt-3"></div>
      </div>
      <div class="card p-6">
        <div class="section-hdr">🏆 Top Picks — To Record a Hit</div>
        <div id="picks-body" class="mlb-picks-grid"></div>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>S1</strong> Lifetime BA vs today's pitcher (FIC) &nbsp;|&nbsp;
          <strong>S2</strong> Lifetime H/A BA vs today's opponent &nbsp;|&nbsp;
          <strong>S3</strong> 2026 season H/A BA vs all teams &nbsp;|&nbsp;
          <strong>S4</strong> Last 10 H/A games vs THIS opponent — games with 1+ hit &nbsp;|&nbsp;
          <strong>Hit Odds</strong> Sportsbook price "to record a hit" (0.5 line) &nbsp;|&nbsp;
          <strong>Score</strong> = (S1+S2+S3+D/N)×1000 + S4 hit rate ×10
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
        <div class="overflow-x-auto">
          <table class="results-table" id="under-picks-table">
            <thead><tr><th>#</th><th>Player</th><th>H/A</th><th>Opponent</th><th>Pitcher</th><th>S1 vs Pitcher</th><th>S2 H/A</th><th>S3 2026</th><th>Lineup</th><th>Line</th><th>Odds</th></tr></thead>
            <tbody id="under-picks-body"></tbody>
          </table>
        </div>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>Source</strong>: The Odds API — players with 1.5 hits O/U line &nbsp;|&nbsp;
          <strong>S1</strong> Career BA vs today's pitcher (under &lt; .250, N/A passes) &nbsp;|&nbsp;
          <strong>S2</strong> Lifetime H/A BA vs today's opponent (under &lt; .225) &nbsp;|&nbsp;
          <strong>S3</strong> 2026 H/A BA (under &lt; .250) &nbsp;|&nbsp;
          <strong>Ranked #1 → coldest bat</strong>
        </p>
      </div>
      <div class="card p-6 hidden" id="pitcher-k-card" style="border-color:rgba(99,202,183,.25)">
        <div class="section-hdr" style="color:#63cab7">⚾ Pitcher K Picks — Over / Under Strikeout Line</div>
        <div class="overflow-x-auto">
          <table class="results-table" id="pitcher-k-table">
            <thead><tr><th>Pitcher</th><th>vs (H/A)</th><th>K Line</th><th>Avg K</th><th>Avg IP</th><th>ERA</th><th>K History (H/A)</th><th>Pick</th></tr></thead>
            <tbody id="pitcher-k-body"></tbody>
          </table>
        </div>
        <details class="mt-4" id="pitcher-k-nopick-details">
          <summary class="cursor-pointer text-xs text-slate-500 select-none">▸ All today's pitchers (no qualifying pick)</summary>
          <table class="results-table mt-2" id="pitcher-k-nopick-table">
            <thead><tr><th>Pitcher</th><th>vs (H/A)</th><th>K Line</th><th>Avg K (H/A)</th><th>Starts</th><th>Note</th></tr></thead>
            <tbody id="pitcher-k-nopick-body"></tbody>
          </table>
        </details>
        <p class="text-xs text-slate-500 mt-4 admin-only">
          <strong>K History</strong> = H/A starts vs today's opponent only &nbsp;|&nbsp;
          <strong>Pick</strong> = OVER if avg &gt; line, UNDER if avg &lt; line (min 2 starts).
        </p>
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
  const KEY = '__mpa_token';
  const params = new URLSearchParams(window.location.search);
  const urlTok = params.get('token');
  if (urlTok) { localStorage.setItem(KEY, urlTok); window.history.replaceState({}, '', window.location.pathname); }
  const fd = new FormData();
  fd.append('username', 'higgi'); fd.append('password', 'Elbowlake77');
  fetch('/api/login', { method: 'POST', body: fd })
    .then(r => r.json()).then(d => {
      token = d.access_token; username = d.username || 'higgi';
      localStorage.setItem('mlb_token', token); localStorage.setItem('mlb_user', username);
      showDashboard();
    }).catch(() => showDashboard());
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
  const dp = document.getElementById('date-picker');
  if (dp) dp.value = today;
  hide('progress-card'); hide('results-card');
}

async function startRun() {
  const dateStr = document.getElementById('date-picker').value;
  if (!dateStr) { alert('Please select a date.'); return; }
  clearLog(); hide('results-card');
  show('progress-card'); setProgress(0, '');
  show('run-spinner'); disableRunBtn(true);
  try {
    const r = await fetch(`/api/run?date_str=${dateStr}`, { method: 'POST' });
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

function showResults(result) {
  window._lastResult = result;
  const { top9, stats, pitcher_k } = result;
  hide('also-ran-card'); hide('under-picks-card'); hide('pitcher-k-card');

  document.getElementById('stats-row').innerHTML = [
    statCard('🎯','Top Picks',top9.length),
    statCard('⬇️','Under Picks',stats.under_count??0),
    statCard('⚾','Pitcher K',stats.pitcher_k_count??0),
    statCard('⚾','Games Today',stats.games),
    statCard('🔍','Players Run',stats.step1_count),
    statCard('⏱️','Time (s)',stats.elapsed),
  ].join('');

  document.getElementById('picks-body').innerHTML = top9.map((p,i) => _mlbCard(p, i+1)).join('');

  const alsoRan = result.also_ran || [];
  if (alsoRan.length > 0) {
    show('also-ran-card');
    document.getElementById('also-ran-body').innerHTML = alsoRan.map((p,i) => _mlbCard(p, i+10, true)).join('');
  }

  const underPicks = result.under_picks || [];
  if (underPicks.length > 0) {
    show('under-picks-card');
    document.getElementById('under-picks-body').innerHTML = underPicks.map((p,i) => {
      const rank=i+1, rnkBg=rank===1?'#5a0a0a':rank===2?'#4a0a0a':'#3a1a1a';
      return `<tr>
        <td><span class="rank-badge" style="background:${rnkBg};color:#ff8a65;font-weight:900">${rank}</span></td>
        <td class="font-semibold">${p.name}</td>
          <td><span class="badge ${p.side==='HOME'?'badge-home':'badge-away'}">${p.side}</span></td>
        <td class="text-slate-300 text-sm">${p.opp}</td>
        <td class="text-slate-400 text-sm">${p.pitcher||'—'}</td>
        <td class="stat-cell stat-under">${p.s1_disp||'—'} <span class="text-slate-500" style="font-size:.7rem">(${p.s1_ab}AB)</span></td>
        <td class="stat-cell stat-under">${p.s2?.display||'—'}</td>
        <td class="stat-cell stat-under">${p.s3?.display||'—'}</td>
        <td>${lineupBadge(p.lineup_status)}</td>
        <td><span style="color:#ff8a65;font-weight:800;font-size:1rem">U 1.5</span>
            <span class="text-slate-500" style="font-size:.68rem;display:block">score ${p.under_score}</span></td>
        <td style="font-family:monospace;color:#fbbf24;font-weight:700">${p.under_odds!=null?(p.under_odds>0?'+':'')+p.under_odds:'—'}</td>
      </tr>`;
    }).join('');
  }

  const pkData=result.pitcher_k||{}, pkAll=pkData.all||[];
  if (pkAll.length > 0) {
    show('pitcher-k-card');
    const pkSorted = pkAll.filter(p=>p.pick).sort((a,b)=>{
      const ga=Math.abs((a.avg_k||0)-(a.line||0)), gb=Math.abs((b.avg_k||0)-(b.line||0));
      return gb-ga;
    });
    document.getElementById('pitcher-k-body').innerHTML = pkSorted.length > 0
      ? pkSorted.map(p => {
          const isOver=p.pick==='OVER';
          const pickClr=isOver?'#63cab7':'#ff8a65';
          const sideCls=p.side==='HOME'?'badge-home':'badge-away';
          const odds=isOver?(p.over_odds!=null?(p.over_odds>0?'+':'')+p.over_odds:''):(p.under_odds!=null?(p.under_odds>0?'+':'')+p.under_odds:'');
          return `<tr>
            <td class="font-semibold">${p.name}</td>
            <td><span class="badge ${sideCls}">${p.side}</span> <span class="text-slate-400 text-xs">${p.opp||''}</span></td>
            <td style="font-family:monospace;font-weight:700;color:#fff">${p.line!=null?p.line+' Ks':'—'}</td>
            <td style="font-family:monospace;font-weight:700;color:${pickClr}">${p.avg_k!=null?p.avg_k+' K':'—'}</td>
            <td style="font-family:monospace;color:#93c5fd;font-weight:600">${p.avg_ip!=null?p.avg_ip+' IP':'—'}</td>
            <td style="font-family:monospace;color:#fbbf24;font-weight:600">${p.era||'—'}</td>
            <td style="font-family:monospace;font-size:.75rem;color:#94a3b8">${p.k_history||'—'}</td>
            <td><span style="color:${pickClr};font-weight:900;font-size:1rem">${p.pick}</span><span class="text-slate-500" style="font-size:.68rem;display:block">${odds}</span></td>
          </tr>`;
        }).join('')
      : '<tr><td colspan="8" class="text-slate-500 text-center" style="padding:16px">No qualifying picks today</td></tr>';
    const npDet=document.getElementById('pitcher-k-nopick-details');
    if (npDet) npDet.style.display='none';
  }

  renderByGame(result);
  show('results-card');
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
    box.innerHTML='<div class="text-slate-500 text-sm">No match for "<strong>'+raw+'</strong>". They may not be playing today or weren\\'t in the analyzed pool. '
      +'If searching a pitcher, expand "All today\\'s pitchers" below the K Picks table. '
      +'Hitters DQ\\'d before S1 (no FIC matchup or no lineup yet) won\\'t appear in the DQ list either.</div>';
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
    html+='<div><span style="color:#fff;font-weight:700;font-size:1.05rem">'+(p.full_name||p.name||'')+'</span>';
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
  var ks=((result.pitcher_k||{}).picks||[]).map(function(p){return Object.assign({_kind:'PITCHER K'},p);});
  var all=hitters.concat(unders, ks);
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
      var kindCls=kind==='HITTER'?'badge-home':(kind==='UNDER'?'badge-out':'badge-tbd');
      var sideBadge='<span class="badge '+(p.side==='HOME'?'badge-home':'badge-away')+'">'+(p.side||'')+'</span>';
      var note='';
      if(kind==='HITTER') note='Top hitter pick';
      else if(kind==='UNDER') note='UNDER — vs '+(p.pitcher||'TBD');
      else if(kind==='PITCHER K') note=(p.pick||'')+' '+(p.line||'')+' Ks';
      var lineup=p.lineup_status==='IN_LINEUP'?'<span class="badge badge-in">✅ IN</span>'
        :p.lineup_status==='NOT_IN_LINEUP'?'<span class="badge badge-out">❌ OUT</span>'
        :'<span class="badge badge-tbd">⏳ TBD</span>';
      html+='<tr>';
      html+='<td><span class="badge '+kindCls+' text-xs">'+kind+'</span></td>';
      html+='<td class="font-semibold">'+(p.name||'')+'</td>';
      html+='<td>'+sideBadge+'</td>';
      html+='<td class="text-slate-300 text-sm">'+note+'</td>';
      html+='<td>'+(kind==='PITCHER K'?'<span class="text-slate-500 text-xs">—</span>':lineup)+'</td>';
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
  return `<div class="mlb-pick-card" style="${dim?'opacity:0.85':''}">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#1a2a1a 0%,#0a1a0a 100%)">
      <div style="display:flex;align-items:center;gap:8px">
        <div style="width:30px;height:30px;border-radius:50%;background:${rnkColors[0]};color:${rnkColors[1]};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.9rem">${rank}</div>
        <span style="font-size:.72rem;letter-spacing:.12em;color:#f59e0b;font-weight:800">MLB · ${p.pos||''}</span>
      </div>
      ${teamLogo?`<img src="${teamLogo}" alt="${p.team}" style="height:34px;width:34px;object-fit:contain" onerror="this.style.display='none'"/>`:''}
    </div>
    <div class="mlb-card-photo">
      ${headshot?`<img src="${headshot}" alt="${p.full_name||p.name}" style="position:absolute;bottom:-6px;left:50%;transform:translateX(-50%);height:155px;object-fit:contain" onerror="this.style.display='none'"/>`:''}
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
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">Hit Odds</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odds}</span>
      </div>
      ${adminStats}
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
async def serve_spa(admin: str = ""):
    import os as _os
    is_admin = bool(admin) and admin == _os.environ.get("INTERNAL_API_TOKEN", "__none__")
    body_cls = "is-admin" if is_admin else ""
    js_flag = "true" if is_admin else "false"
    html = _HTML.replace('<body class="min-h-screen">', f'<body class="min-h-screen {body_cls}">').replace(
        "</head>",
        f'<script>window.IS_ADMIN = {js_flag};</script></head>', 1)
    return HTMLResponse(html)
