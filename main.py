"""
main.py — FastAPI app for MoneyBall
  • POST /api/login          — get JWT token
  • POST /api/run            — kick off pipeline (returns task_id)
  • GET  /api/stream/{id}   — SSE progress stream (token as query param)
  • GET  /api/results/{date} — fetch cached results
  • GET  /                   — serves the frontend SPA
"""
import asyncio, json, os, uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Optional

from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import StreamingResponse, FileResponse, HTMLResponse
# StaticFiles removed — HTML is now embedded directly in this file


# ── App setup ────────────────────────────────────────────────────────
app = FastAPI(title="MoneyBall", docs_url=None, redoc_url=None)
executor = ThreadPoolExecutor(max_workers=4)
_tasks: dict = {}   # task_id -> {events, status, result, notify}
_cache: dict = {}   # date -> result (in-memory, cleared on restart)
# ── File-based Picks Cache ────────────────────────────────────────────────────
import pathlib
_CACHE_DIR = pathlib.Path("/tmp/mpa_cache")
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_CACHE_TTL = 6 * 3600  # 6 hours

def _cache_path(app: str, date_key: str) -> pathlib.Path:
    return _CACHE_DIR / f"{app}_{date_key}.json"

def _cache_get(app: str, date_key: str):
    p = _cache_path(app, date_key)
    try:
        if p.exists() and (time.time() - p.stat().st_mtime) < _CACHE_TTL:
            data = json.loads(p.read_text(encoding="utf-8"))
            print(f"[Cache] FILE HIT {app}/{date_key}")
            return data
    except Exception as e:
        print(f"[Cache] Read error: {e}")
    return None

def _cache_set(app: str, date_key: str, result: dict):
    try:
        _cache_path(app, date_key).write_text(
            json.dumps(result, ensure_ascii=False), encoding="utf-8")
        print(f"[Cache] FILE SET {app}/{date_key}")
    except Exception as e:
        print(f"[Cache] Write error: {e}")

def _cache_clear(app: str = None):
    for p in _CACHE_DIR.glob("*.json"):
        if app is None or p.name.startswith(app + "_"):
            p.unlink(missing_ok=True)

# ── Auth ─────────────────────────────────────────────────────────────
@app.post("/api/login")
async def login(username: str = Form(...), password: str = Form(...)):
    if username == "higgi" and password == "Elbowlake77":
        return {"access_token": "mpa-token", "username": username}
    raise HTTPException(status_code=401, detail="Invalid credentials")

# ── Health ────────────────────────────────────────────────────────────

@app.get("/api/warm")
async def api_warm_mlb():
    """Pre-compute today's picks — called by cron-job.org at 10 AM."""
    from datetime import date as _date
    today = _date.today().isoformat()
    cached = _cache_get("mlb", today)
    if cached:
        return {"ok": True, "source": "cache", "date": today}
    # Kick off a run
    result = await start_run(today)
    return {"ok": True, "source": "computed", "date": today,
            "task_id": result.get("task_id"), "cached": result.get("cached", False)}

@app.get("/api/health")
async def health():
    return {"status": "ok", "today": str(date.today())}






@app.get("/api/test-statmuse")
async def test_statmuse():
    """Steps 2 & 3 now use MLB Stats API — always ready."""
    return {"ok": True, "message": "✅ MLB Stats API active"}


# ── Run pipeline ──────────────────────────────────────────────────────
@app.post("/api/run")
async def start_run(date_str: str):
    """
    Kick off the 4-step pipeline for date_str (YYYY-MM-DD).
    Returns task_id immediately; stream progress via /api/stream/{task_id}.
    """
    # Return cached result instantly
    # File cache check first
    _fc = _cache_get("mlb", date_str)
    if _fc:
        _cache[date_str] = _fc
    if date_str in _cache:
        task_id = str(uuid.uuid4())
        notify  = asyncio.Event()
        _tasks[task_id] = {
            "events": [
                {"type": "cached", "msg": "⚡ Results loaded from cache — no re-run needed"},
                {"type": "done",   "result": _cache[date_str]},
            ],
            "status": "done",
            "result": _cache[date_str],
            "notify": notify,
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
            _cache_set("mlb", date_str, result)
        except Exception as exc:
            import traceback
            msg = f"{exc}\n{traceback.format_exc()}"
            emit({"type": "error", "msg": msg})
            task["status"] = "error"

    executor.submit(run_in_thread)
    return {"task_id": task_id, "cached": False}


# ── SSE stream ────────────────────────────────────────────────────────
@app.get("/api/stream/{task_id}")
async def stream_task(task_id: str):
    """
    Server-Sent Events stream for a running task.
    Pass JWT as ?token=... (browsers can't set custom headers for SSE).
    """
    if task_id not in _tasks:
        raise HTTPException(status_code=404, detail="Task not found")

    task = _tasks[task_id]

    async def event_generator():
        idx = 0
        while True:
            # Flush any buffered events
            while idx < len(task["events"]):
                ev = task["events"][idx]
                yield f"data: {json.dumps(ev)}\n\n"
                idx += 1

            # Done?
            if task["status"] in ("done", "error"):
                # One final flush
                while idx < len(task["events"]):
                    ev = task["events"][idx]
                    yield f"data: {json.dumps(ev)}\n\n"
                    idx += 1
                return

            # Wait for next event (or heartbeat every 20 s)
            task["notify"].clear()
            try:
                await asyncio.wait_for(task["notify"].wait(), timeout=20.0)
            except asyncio.TimeoutError:
                yield ": heartbeat\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":       "keep-alive",
        },
    )


# ── Results cache ─────────────────────────────────────────────────────
@app.get("/api/results/{date_str}")
async def get_results(date_str: str):
    # File cache check first
    _fc = _cache_get("mlb", date_str)
    if _fc:
        _cache[date_str] = _fc
    if date_str in _cache:
        return _cache[date_str]
    raise HTTPException(status_code=404, detail="No results for this date. Run the pipeline first.")


# ── Frontend HTML (embedded — no static folder needed) ───────────────
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
      --navy:   #0f0f0f;
      --navy2:  #161616;
      --navy3:  #1a1a1a;
      --green:  #f59e0b;
      --red:    #ff1744;
      --yellow: #f59e0b;
      --orange: #f59e0b;
      --gold:   #f59e0b;
      --silver: #c0c0c0;
      --bronze: #cd7f32;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: #0f0f0f; color: #fff; font-family: 'Source Sans Pro', sans-serif; min-height: 100vh; }
    /* Hub-style fixed nav */
    .app-nav { position: fixed; top: 0; width: 100%; background: rgba(10,10,10,.95); backdrop-filter: blur(12px); border-bottom: 1px solid #1c1c1c; z-index: 100; padding: 0 32px; height: 80px; display: flex; align-items: center; justify-content: space-between; }
    .app-nav-logo { font-family: 'Playfair Display', serif; font-size: 36px; font-weight: 900; color: #f59e0b; letter-spacing:.02em; line-height:1; }
    .app-nav-logo span { color: #fff; }
    .app-nav-right { display: flex; align-items: center; gap: 16px; }
    .app-nav-user { font-size: 12px; color: #6b7280; }
    .app-nav-signout { font-size: 12px; color: #4b5563; cursor: pointer; background: none; border: none; transition: color .2s; }
    .app-nav-signout:hover { color: #f87171; }
    /* Cards */
    .card { background: #161616; border: 1px solid #262626; border-radius: 16px; transition: border-color .2s; }
    .card:hover { border-color: rgba(245,158,11,.2); }
    .p-6 { padding: 24px; }
    /* Buttons */
    .btn-primary { background: #f59e0b; border: none; cursor: pointer; border-radius: 8px; padding: 12px 28px; font-size: 1rem; font-weight: 700; color: #000; transition: all .2s; font-family: 'Source Sans Pro', sans-serif; }
    .btn-primary:hover:not(:disabled) { background: #fbbf24; transform: translateY(-1px); box-shadow: 0 4px 20px rgba(245,158,11,.4); }
    .btn-primary:disabled { opacity: .5; cursor: not-allowed; transform: none; }
    .btn-danger { background: linear-gradient(135deg, #c62828, #b71c1c); }
    /* Flex utilities (replacing Tailwind) */
    .flex { display: flex; } .flex-col { flex-direction: column; } .flex-1 { flex: 1; }
    .items-center { align-items: center; } .items-end { align-items: flex-end; }
    .justify-between { justify-content: space-between; }
    .gap-3 { gap: 12px; } .gap-4 { gap: 16px; } .gap-6 { gap: 24px; }
    .hidden { display: none; } .w-full { width: 100%; } .space-y-6 > * + * { margin-top: 24px; }
    .min-h-screen { min-height: 100vh; } .flex-col { flex-direction: column; }
    .px-4 { padding-left: 16px; padding-right: 16px; }
    .py-6 { padding-top: 24px; padding-bottom: 24px; }
    .max-w-7xl { max-width: 1280px; } .mx-auto { margin-left: auto; margin-right: auto; }
    .mt-2 { margin-top: 8px; } .mt-4 { margin-top: 16px; }
    .text-xs { font-size: 12px; } .text-sm { font-size: 14px; }
    .text-slate-400 { color: #94a3b8; } .text-slate-500 { color: #64748b; }
    .sm\:flex-row { flex-direction: row; } .sm\:items-end { align-items: flex-end; }

    /* Terminal log */
    #log-box { background: #0a0a0a; border: 1px solid #262626; border-radius: 8px; height: 260px; overflow-y: auto; padding: 12px 16px; font-family: 'Courier New', monospace; font-size: .82rem; line-height: 1.6; }
    .log-section { color: var(--yellow); font-weight: 700; margin-top: 6px; }
    .log-ok  { color: var(--green); }
    .log-dq  { color: var(--red); }
    .log-skip { color: #64748b; }
    .log-info { color: #93c5fd; }
    .log-cached { color: #a78bfa; }
    .log-default { color: #cbd5e1; }
    .log-under { color: #ff8a65; }

    /* Progress bar */
    #prog-bar-inner { height: 6px; border-radius: 3px; background: linear-gradient(90deg, #f59e0b, #fbbf24); transition: width .4s ease; }

    /* Results table */
    .results-table { width: 100%; border-collapse: collapse; }
    .results-table th { background: #0f0f0f; color: #f59e0b; font-size: .75rem; text-transform: uppercase; letter-spacing: 1px; padding: 10px 14px; text-align: left; white-space: nowrap; }
    .results-table td { padding: 12px 14px; border-bottom: 1px solid rgba(255,255,255,.05); vertical-align: middle; }
    .results-table tr:hover td { background: rgba(255,255,255,.03); }
    .results-table tr:last-child td { border-bottom: none; }

    .rank-badge { width: 32px; height: 32px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-weight: 800; font-size: .85rem; }
    .rank-1 { background: var(--gold);   color: #000; }
    .rank-2 { background: var(--silver); color: #000; }
    .rank-3 { background: var(--bronze); color: #fff; }
    .rank-n { background: var(--navy3);  color: #94a3b8; }

    .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: .72rem; font-weight: 700; letter-spacing:.3px; }
    .badge-home { background: rgba(21,101,192,.35); color: #90caf9; }
    .badge-away { background: rgba(103,58,183,.35); color: #ce93d8; }
    .badge-pos  { background: rgba(255,255,255,.08); color: #94a3b8; }
    .badge-day  { background: rgba(255,214,0,.2);  color: var(--yellow); }
    .badge-night{ background: rgba(100,100,255,.2);color: #a5b4fc; }
    .badge-dq   { background: rgba(255,23,68,.15); color: #ff6b6b; font-size:.7rem; padding: 2px 6px; }
    .badge-in   { background: rgba(0,200,83,.2);  color: #00e676; }
    .badge-tbd  { background: rgba(255,214,0,.2); color: #f59e0b; }
    .badge-out  { background: rgba(255,23,68,.2); color: #ff6b6b; }

    .stat-cell { font-family: 'Courier New', monospace; font-size: .88rem; font-weight: 600; }
    .stat-good { color: var(--green); }
    .stat-warn { color: var(--yellow); }
    .stat-na   { color: #475569; }
    .stat-under { color: #ff8a65; font-weight: 700; }  /* under-pick low BA */
    .score-big { font-size: 1.1rem; font-weight: 800; color: #f59e0b; }

    .section-hdr { display: flex; align-items: center; gap: 8px; font-size: .9rem; font-weight: 700; color: #f59e0b; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 12px; }
    .section-hdr::after { content:''; flex:1; height:1px; background:#262626; }

    /* DQ accordion */
    details > summary { cursor: pointer; list-style: none; user-select: none; }
    details > summary::-webkit-details-marker { display: none; }
    .dq-row { font-size: .82rem; padding: 7px 14px; border-bottom: 1px solid rgba(255,255,255,.04); display: flex; gap: 16px; align-items: center; }
    .dq-row:last-child { border-bottom: none; }

    /* Spinner */
    @keyframes spin { to { transform: rotate(360deg); } }
    .spinner { width: 18px; height: 18px; border: 3px solid rgba(255,255,255,.15); border-top-color: #3b82f6; border-radius: 50%; animation: spin .7s linear infinite; display: inline-block; }

    /* Login */
    .login-input { background: var(--navy3); border: 1px solid rgba(255,255,255,.15); color: #e2e8f0; border-radius: 8px; padding: 11px 16px; width: 100%; font-size: 1rem; outline: none; transition: border-color .2s; }
    .login-input:focus { border-color: #3b82f6; }

    ::-webkit-scrollbar { width: 6px; } ::-webkit-scrollbar-track { background: transparent; } ::-webkit-scrollbar-thumb { background: rgba(255,255,255,.12); border-radius: 3px; }

    .run-box{background:#161616;border:1px solid #262626;border-radius:16px;padding:28px;text-align:center;margin-bottom:20px;transition:border-color .2s}
    .run-box:hover{border-color:rgba(245,158,11,.3)}
    .run-box h2{font-family:'Playfair Display',serif;font-size:1.1rem;font-weight:700;color:#f59e0b;letter-spacing:1px;margin-bottom:6px}
    .run-box p{color:#6b7280;font-size:.85rem;margin-bottom:18px}
    .date-row{display:flex;align-items:center;justify-content:center;gap:12px;margin-bottom:18px}
    .date-row label{font-size:11px;font-weight:700;color:#6b7280;letter-spacing:1.5px;text-transform:uppercase}
    .date-row input{background:#0f0f0f;color:#f59e0b;border:1px solid #262626;border-radius:8px;padding:9px 14px;font-size:.9rem;font-weight:600;outline:none;cursor:pointer;font-family:'Source Sans Pro',sans-serif}
    .date-row input:focus{border-color:#f59e0b}
    .btn-run{background:#f59e0b;color:#000;border:none;border-radius:8px;padding:14px 48px;font-size:1rem;font-weight:900;cursor:pointer;letter-spacing:.5px;transition:all .2s;font-family:'Source Sans Pro',sans-serif}
    .btn-run:hover{background:#fbbf24;transform:translateY(-1px);box-shadow:0 4px 20px rgba(245,158,11,.4)}
    .btn-run:disabled{background:#333;color:#666;cursor:not-allowed;transform:none}
    input[type=date]::-webkit-calendar-picker-indicator{filter:invert(1);opacity:.7;cursor:pointer}

    /* Stat chips — NHL style */
    .chips{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:12px;margin-bottom:24px}
    .chip{background:#111;border-top:3px solid #FDB827;border-radius:8px;padding:16px 10px;text-align:center}
    .chip .val{font-size:1.9rem;font-weight:900;color:#FDB827}
    .chip .lbl{font-size:.65rem;color:#555;text-transform:uppercase;letter-spacing:1px;margin-top:4px}
  </style>
</head>
<body class="min-h-screen">

<!-- ═══════════════════════ LOGIN SCREEN ═══════════════════════════ -->
<div id="login-screen" class="min-h-screen flex items-center justify-center px-4" style="display:none">
  <div class="card p-10 w-full max-w-md shadow-2xl">
    <div class="text-center mb-8">
      <div style="font-size:7rem;line-height:1;margin-bottom:12px">⚾</div>
      <h1 style="font-size:3rem;font-weight:900;letter-spacing:-1px">MoneyBall</h1>
      <p class="text-slate-400 text-sm mt-1">Your daily MLB edge ⚫🟡</p>
    </div>

    <div id="login-error" class="hidden bg-red-900/40 border border-red-700 text-red-300 rounded-lg px-4 py-3 text-sm mb-4"></div>

    <form id="login-form" onsubmit="doLogin(event)" class="space-y-4">
      <div>
        <label class="block text-xs font-semibold text-slate-400 uppercase tracking-widest mb-2">Username</label>
        <input id="inp-user" type="text" autocomplete="username" placeholder="your username"
               class="login-input" required />
      </div>
      <div>
        <label class="block text-xs font-semibold text-slate-400 uppercase tracking-widest mb-2">Password</label>
        <input id="inp-pass" type="password" autocomplete="current-password" placeholder="••••••••"
               class="login-input" required />
      </div>
      <button type="submit" class="btn-primary w-full mt-2" id="login-btn">Sign In</button>
    </form>
  </div>
</div>

<!-- ═══════════════════════ DASHBOARD ══════════════════════════════ -->
<div id="dashboard" class="hidden min-h-screen flex flex-col" style="padding-top:80px">

  <!-- Header -->
  <nav class="app-nav">
    <span class="app-nav-logo">Money<span> Picks</span> Arena</span>
  </nav>

  <main class="flex-1 px-4 py-6 max-w-7xl mx-auto w-full space-y-6">

    <!-- App Header -->
    <div style="text-align:center;margin-bottom:32px">
      <h1 style="font-family:'Playfair Display',serif;font-size:2.6rem;font-weight:900;color:#fff;margin-bottom:6px">MLB <span style="color:#f59e0b">MoneyBall</span></h1>
      <p style="font-size:.85rem;color:#6b7280;letter-spacing:.15em;text-transform:uppercase">MLB Daily Picks</p>
    </div>

        <!-- Controls card -->
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
      <p class="text-xs text-slate-500 mt-3">

      </p>
    </div>

    <!-- Progress card (hidden until running) -->
    <div id="progress-card" class="card p-6 hidden">
      <div class="flex justify-between items-center mb-3">
        <div class="section-hdr mb-0">Live Progress</div>
        <span id="prog-label" class="text-xs text-slate-400"></span>
      </div>
      <div class="bg-white/5 rounded-full overflow-hidden mb-4">
        <div id="prog-bar-inner" style="width:0%"></div>
      </div>
      <div id="log-box"></div>
    </div>

    <!-- Results card (hidden until done) -->
    <div id="results-card" class="hidden space-y-6">

      <!-- Stats summary -->
      <div id="stats-row" class="chips" class="grid grid-cols-2 sm:grid-cols-4 gap-4"></div>

      <!-- Top 9 table -->
      <div class="card p-6">
        <div class="section-hdr">🏆 Top Picks</div>
        <div class="overflow-x-auto">
          <table class="results-table" id="picks-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Player</th>
                <th>Pos</th>
                <th>H/A</th>
                <th>Opponent</th>
                <th>S1 BA</th>
                <th>S2 BA</th>
                <th>S3 BA</th>
                <th>D/N</th>
                <th>Lineup</th>
                <th>Score</th>
              </tr>
            </thead>
            <tbody id="picks-body"></tbody>
          </table>
        </div>
        <p class="text-xs text-slate-500 mt-4">
          <strong>S1</strong> Lifetime BA vs today's pitcher (FIC) &nbsp;|&nbsp;
          <strong>S2</strong> Lifetime H/A BA vs today's opponent &nbsp;|&nbsp;
          <strong>S3</strong> 2026 season H/A BA vs all teams &nbsp;|&nbsp;
          <strong>Score</strong> = S1+S2+S3 × 1000
        </p>
      </div>

      <!-- Also Ran — picks #10+ who passed all 5 steps -->
      <div class="card p-6 hidden" id="also-ran-card">
        <div class="section-hdr">⏳ Also Ran — Passed All Steps, Just Missed Top 9</div>
        <div class="overflow-x-auto">
          <table class="results-table" id="also-ran-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Player</th>
                <th>Pos</th>
                <th>H/A</th>
                <th>Opponent</th>
                <th>S1 BA</th>
                <th>S2 BA</th>
                <th>S3 BA</th>
                <th>D/N</th>
                <th>Lineup</th>
                <th>Score</th>
              </tr>
            </thead>
            <tbody id="also-ran-body"></tbody>
          </table>
        </div>
        <p class="text-xs text-slate-500 mt-3">These players passed all 5 steps — ranked by score.</p>
      </div>

      <!-- Under Picks — 3rd subcategory -->
      <div class="card p-6 hidden" id="under-picks-card" style="border-color:rgba(255,107,107,.25)">
        <div class="section-hdr" style="color:#ff8a65">⬇️ Under Picks — Bet Under 1.5 Hits (DraftKings)</div>
        <div class="overflow-x-auto">
          <table class="results-table" id="under-picks-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Player</th>
                <th>Pos</th>
                <th>H/A</th>
                <th>Opponent</th>
                <th>Pitcher</th>
                <th>S1 vs Pitcher</th>
                <th>S2 H/A</th>
                <th>S3 2026</th>
                <th>Lineup</th>
                <th>DK Line</th>
              </tr>
            </thead>
            <tbody id="under-picks-body"></tbody>
          </table>
        </div>
        <p class="text-xs text-slate-500 mt-4">
          <strong>Source</strong>: DraftKings players with 1.5 hits O/U &nbsp;|&nbsp;
          <strong>S1</strong> Career BA vs today's pitcher (under &lt; .250, any AB or N/A passes) &nbsp;|&nbsp;
          <strong>S2</strong> Lifetime H/A BA vs today's opponent (under &lt; .225) &nbsp;|&nbsp;
          <strong>S3</strong> 2026 H/A BA (under &lt; .250) &nbsp;|&nbsp;
          All three must pass for a player to appear here. &nbsp;|&nbsp;
          <strong>Ranked #1 → coldest bat</strong> (lowest combined BA = strongest under pick).
        </p>
      </div>

      <!-- Pitcher K Picks -->
      <div class="card p-6 hidden" id="pitcher-k-card" style="border-color:rgba(99,202,183,.25)">
        <div class="section-hdr" style="color:#63cab7">⚾ Pitcher K Picks — Over / Under Strikeout Line</div>
        <div class="overflow-x-auto">
          <table class="results-table" id="pitcher-k-table">
            <thead>
              <tr>
                <th>#</th><th>Pitcher</th><th>H/A</th><th>Opponent</th>
                <th>K Line</th><th>Avg K</th><th>Avg IP</th><th>ERA</th>
                <th>K / Starts H/A</th><th>Gap</th><th>Starts</th><th>K History</th><th>Pick</th>
              </tr>
            </thead>
            <tbody id="pitcher-k-body"></tbody>
          </table>
        </div>
        <details class="mt-4" id="pitcher-k-nopick-details">
          <summary class="cursor-pointer text-xs text-slate-500 hover:text-slate-300 select-none">▸ Show pitchers with insufficient history vs today's opponent</summary>
          <table class="results-table mt-2" id="pitcher-k-nopick-table">
            <thead><tr><th>Pitcher</th><th>H/A</th><th>Opponent</th><th>K Line</th><th>Starts</th><th>Note</th></tr></thead>
            <tbody id="pitcher-k-nopick-body"></tbody>
          </table>
        </details>
        <p class="text-xs text-slate-500 mt-4">
          <strong>Avg K vs Opp</strong> = career H/A avg Ks vs today's opponent &nbsp;|&nbsp;
          <strong>Gap</strong> = how far avg is from the line &nbsp;|&nbsp;
          <strong>Pick</strong> = OVER if avg &gt; line, UNDER if avg &lt; line (min 2 starts).
          Pitchers facing bottom 5 K-rate teams are excluded.
        </p>
      </div>

      <!-- DQ'd players accordion -->
      <div class="card p-6" id="dq-card">
        <details>
          <summary>
            <div class="section-hdr cursor-pointer select-none">
              <span>❌ Disqualified Players</span>
              <span id="dq-count-badge" class="badge badge-dq ml-2"></span>
              <span class="ml-auto text-xs text-slate-500">click to expand ▾</span>
            </div>
          </summary>
          <div id="dq-body" class="mt-2 rounded-lg overflow-hidden border border-white/5"></div>
        </details>
      </div>

    </div><!-- /results-card -->

  </main>
</div><!-- /dashboard -->

<script>
// ─── State ────────────────────────────────────────────────────────────
let token    = localStorage.getItem('mlb_token') || '';
let username = localStorage.getItem('mlb_user')  || '';
let es       = null;   // EventSource

// ─── Boot ─────────────────────────────────────────────────────────────

window.onload = () => {
  // ── Hub Token Gate (must be first — return exits onload entirely) ──
  const HUB = 'https://www.moneypicksarena.com';
  const KEY = '__mpa_token';
  const params = new URLSearchParams(window.location.search);
  const urlTok = params.get('token');
  if (urlTok) {
    localStorage.setItem(KEY, urlTok);
    window.history.replaceState({}, '', window.location.pathname);
  }
  var _tok=localStorage.getItem(KEY);
  if (!_tok || _tok.split(".").length !== 3) {
    localStorage.removeItem(KEY);
    window.location.href = HUB;
    return;
  }

  // Token confirmed — proceed with internal login
  const fd = new FormData();
  fd.append('username', 'higgi');
  fd.append('password', 'Elbowlake77');
  fetch('/api/login', { method: 'POST', body: fd })
    .then(r => r.json())
    .then(d => {
      token = d.access_token;
      username = d.username || 'higgi';
      localStorage.setItem('mlb_token', token);
      localStorage.setItem('mlb_user', username);
      showDashboard();
    })
    .catch(() => showDashboard());
};

// ─── Auth ─────────────────────────────────────────────────────────────
async function doLogin(e) {
  e.preventDefault();
  const user = document.getElementById('inp-user').value.trim();
  const pass = document.getElementById('inp-pass').value;
  const btn  = document.getElementById('login-btn');
  btn.disabled = true; btn.textContent = 'Signing in…';

  const fd = new FormData();
  fd.append('username', user); fd.append('password', pass);

  try {
    const r = await fetch('/api/login', { method: 'POST', body: fd });
    if (r.ok) {
      const d = await r.json();
      token    = d.access_token;
      username = d.username;
      localStorage.setItem('mlb_token', token);
      localStorage.setItem('mlb_user',  username);
      showDashboard();
    } else {
      showLoginError('Invalid username or password. Try again.');
    }
  } catch {
    showLoginError('Server error. Please try again.');
  } finally {
    btn.disabled = false; btn.textContent = 'Sign In';
  }
}

function doLogout() {
  token = ''; username = '';
  localStorage.removeItem('mlb_token');
  localStorage.removeItem('mlb_user');
  localStorage.removeItem('__mpa_token');
  if (es) { es.close(); es = null; }
  window.location.href = 'https://www.moneypicksarena.com';
}

function showLoginError(msg) {
  const el = document.getElementById('login-error');
  el.textContent = msg; el.classList.remove('hidden');
}

// ─── Dashboard init ────────────────────────────────────────────────────
function showDashboard() {
  hide('login-screen'); show('dashboard');
  const _d=new Date(); const today=_d.getFullYear()+'-'+String(_d.getMonth()+1).padStart(2,'0')+'-'+String(_d.getDate()).padStart(2,'0');
  const dp = document.getElementById('date-picker');
  if(dp){ dp.value = today; dp.max = today; }
  hide('progress-card'); hide('results-card');
}
// ─── Run pipeline ──────────────────────────────────────────────────────
async function startRun() {
  const dateStr = document.getElementById('date-picker').value;
  if (!dateStr) { alert('Please select a date.'); return; }

  // Reset UI
  clearLog(); hide('results-card');
  show('progress-card'); setProgress(0, '');
  show('run-spinner'); disableRunBtn(true);

  try {
    const r = await authFetch(`/api/run?date_str=${dateStr}`, { method: 'POST' });
    if (!r.ok) { const e = await r.json(); throw new Error(e.detail || 'Run failed'); }
    const { task_id, cached } = await r.json();

    openSSE(task_id, cached);
  } catch (err) {
    appendLog(`❌ ${err.message}`, 'dq');
    hide('run-spinner'); disableRunBtn(false);
  }
}

function openSSE(taskId, cached) {
  if (es) es.close();
  es = new EventSource(`/api/stream/${taskId}?token=${encodeURIComponent(token)}`);

  es.onmessage = evt => handleEvent(JSON.parse(evt.data));
  es.onerror   = () => {
    if (document.getElementById('results-card').classList.contains('hidden')) {
      appendLog('⚠️  Connection lost — refresh and try again.', 'dq');
    }
    hide('run-spinner'); disableRunBtn(false);
    es.close();
  };
}

let _progTotal = 30;

function handleEvent(ev) {
  switch (ev.type) {

    case 'section':
      appendLog('', 'default');
      appendLog(`▸ ${ev.msg}`, 'section');
      break;

    case 'log':
    case 'step1_done':
      appendLog(ev.msg, ev.msg.startsWith('✅') ? 'ok' : 'info');
      break;

    case 'cached':
      appendLog(ev.msg, 'cached');
      break;

    case 'progress':
      _progTotal = ev.total;
      setProgress(Math.round((ev.current / ev.total) * 80), `${ev.current}/${ev.total}: ${ev.name}`);
      break;

    case 'player_ok':
      appendLog(
        `  ✅ ${pad(ev.name,22)} S1:${ev.s1}  S2:${ev.s2}  S3:${ev.s3}  ${ev.side} vs ${ev.opp}  → ${ev.total}pts`,
        'ok'
      );
      break;

    case 'player_dq':
      appendLog(
        `  ❌ ${pad(ev.name,22)} S1:${ev.s1}  S2:${ev.s2}  S3:${ev.s3}  DQ: ${ev.reason}`,
        'dq'
      );
      break;

    case 'player_skip':
      appendLog(`  — ${pad(ev.name,22)} No game today`, 'skip');
      break;

    case 'dn_ok':
      appendLog(`  ✅ ${pad(ev.name,22)} ${ev.label} ${ev.display}`, 'ok');
      break;

    case 'dn_dq':
      appendLog(`  ❌ ${pad(ev.name,22)} ${ev.label} ${ev.display} < .200 — DQ`, 'dq');
      break;

    case 'done':
      setProgress(100, 'Complete!');
      appendLog('', 'default');
      appendLog(`🏆 Done! ${ev.result.stats.picks} picks found in ${ev.result.stats.elapsed}s`, 'ok');
      hide('run-spinner'); disableRunBtn(false);
      showResults(ev.result);
      es.close();
      break;

    case 'lineup_ok':
      if (ev.status === 'NOT_IN_LINEUP') {
        appendLog(`  ❌ ${pad(ev.name,22)} NOT in lineup — removed`, 'dq');
      } else {
        const lbl = ev.status === 'IN_LINEUP' ? '✅ IN lineup' : '⏳ TBD';
        appendLog(`  ${lbl}: ${ev.name}`, 'ok');
      }
      break;

    case 'lineup_dq':
      appendLog(`  ❌ ${pad(ev.name,22)} NOT in today\'s lineup — DQ`, 'dq');
      break;

    case 'under_progress':
      // logged inline in the log box by the pipeline
      break;

    case 'under_pick_found':
      appendLog(
        `  ✅ UNDER: ${pad(ev.name,22)} S1:${ev.s1}  S2:${ev.s2}  S3:${ev.s3}  ${ev.side} vs ${ev.opp}`,
        'under'
      );
      break;

    case 'error':
      appendLog(`❌ ERROR: ${ev.msg}`, 'dq');
      hide('run-spinner'); disableRunBtn(false);
      es.close();
      break;
  }
}

// ─── Results rendering ────────────────────────────────────────────────
function showResults(result) {
  const { top9, dq_s1_s3, dq_step4, dq_step5, stats, pitcher_k } = result;
  hide('also-ran-card'); hide('under-picks-card'); hide('pitcher-k-card');  // reset on each run

  // Stats summary
  const sr = document.getElementById('stats-row');
  sr.innerHTML = [
    statCard('🎯', 'Top Picks',    top9.length,                         'text-yellow-400'),
    statCard('⬇️', 'Under Picks',  stats.under_count ?? 0,              'text-orange-400'),
    statCard('⚾', 'Pitcher K',    stats.pitcher_k_count ?? 0,          'text-teal-400'),
    statCard('⚾', 'Games Today',  stats.games,                         'text-blue-400'),
    statCard('🔍', 'Players Run',  stats.step1_count,                   'text-purple-400'),
    statCard('📋', 'Lineups Posted', stats.lineups_posted ?? 0,           'text-teal-400'),
  ].join('');

  // Top 9
  const tbody = document.getElementById('picks-body');
  tbody.innerHTML = top9.map((p, i) => {
    const rank = i + 1;
    const rnk  = rank <= 3 ? `rank-${rank}` : 'rank-n';
    const s1c  = statColor(p.s1);
    const s2c  = statColorStr(p.s2?.display);
    const s3c  = statColorStr(p.s3?.display);
    const dnLabel = p.dn_label || '—';
    const dnDisp  = p.dn?.display || 'N/A';
    const dnCls   = dnLabel === 'DAY' ? 'badge-day' : 'badge-night';
    return `
      <tr>
        <td><span class="rank-badge ${rnk}">${rank}</span></td>
        <td class="font-semibold">${p.name}</td>
        <td><span class="badge badge-pos">${p.pos || '—'}</span></td>
        <td><span class="badge ${p.side === 'HOME' ? 'badge-home' : 'badge-away'}">${p.side}</span></td>
        <td class="text-slate-300 text-sm">${p.opp}</td>
        <td class="stat-cell ${s1c}">${p.s1?.toFixed(3) || '—'}</td>
        <td class="stat-cell ${s2c}">${p.s2?.display || '—'}</td>
        <td class="stat-cell ${s3c}">${p.s3?.display || '—'}</td>
        <td><span class="badge ${dnCls} text-xs">${dnLabel}</span> <span class="stat-cell text-slate-300 text-xs">${dnDisp}</span></td>
        <td>${lineupBadge(p.lineup_status)}</td>
        <td><span class="score-big">${p.total}</span></td>
      </tr>`;
  }).join('');

  // Also Ran section (#10+)
  const alsoRan = result.also_ran || [];
  if (alsoRan.length > 0) {
    show('also-ran-card');
    document.getElementById('also-ran-body').innerHTML = alsoRan.map((p, i) => {
      const rank   = i + 10;
      const dnLabel = p.dn_label || '—';
      const dnDisp  = p.dn?.display || 'N/A';
      const dnCls   = dnLabel === 'DAY' ? 'badge-day' : 'badge-night';
      const s1c = statColor(p.s1);
      const s2c = statColorStr(p.s2?.display);
      const s3c = statColorStr(p.s3?.display);
      return `
      <tr style="opacity:0.8">
        <td><span class="rank-badge rank-n" style="background:#2a2a2a;color:#f59e0b">${rank}</span></td>
        <td class="font-semibold">${p.name}</td>
        <td><span class="badge badge-pos">${p.pos || '—'}</span></td>
        <td><span class="badge ${p.side === 'HOME' ? 'badge-home' : 'badge-away'}">${p.side}</span></td>
        <td class="text-slate-300 text-sm">${p.opp}</td>
        <td class="stat-cell ${s1c}">${p.s1?.toFixed(3) || '—'}</td>
        <td class="stat-cell ${s2c}">${p.s2?.display || '—'}</td>
        <td class="stat-cell ${s3c}">${p.s3?.display || '—'}</td>
        <td><span class="badge ${dnCls} text-xs">${dnLabel}</span> <span class="stat-cell text-slate-300 text-xs">${dnDisp}</span></td>
        <td>${lineupBadge(p.lineup_status)}</td>
        <td><span style="color:#94a3b8;font-weight:700">${p.total}</span></td>
      </tr>`;
    }).join('');
  }

  // Under Picks section
  hide('under-picks-card');
  const underPicks = result.under_picks || [];
  if (underPicks.length > 0) {
    show('under-picks-card');
    document.getElementById('under-picks-body').innerHTML = underPicks.map((p, i) => {
      const rank = i + 1;
      const rnkBg = rank === 1 ? '#5a0a0a' : rank === 2 ? '#4a0a0a' : '#3a1a1a';
      return `
      <tr>
        <td><span class="rank-badge" style="background:${rnkBg};color:#ff8a65;font-weight:900">${rank}</span></td>
        <td class="font-semibold">${p.name}</td>
        <td><span class="badge badge-pos">${p.pos || '—'}</span></td>
        <td><span class="badge ${p.side === 'HOME' ? 'badge-home' : 'badge-away'}">${p.side}</span></td>
        <td class="text-slate-300 text-sm">${p.opp}</td>
        <td class="text-slate-400 text-sm">${p.pitcher || '—'}</td>
        <td class="stat-cell stat-under">${p.s1_disp || '—'} <span class="text-slate-500" style="font-size:.7rem">(${p.s1_ab}AB)</span></td>
        <td class="stat-cell stat-under">${p.s2?.display || '—'}</td>
        <td class="stat-cell stat-under">${p.s3?.display || '—'}</td>
        <td>${lineupBadge(p.lineup_status)}</td>
        <td><span style="color:#ff8a65;font-weight:800;font-size:1rem">U 1.5</span>
            <span class="text-slate-500" style="font-size:.68rem;display:block">score ${p.under_score}</span></td>
      </tr>`;
    }).join('');
  }


  // ── Pitcher K Picks ──────────────────────────────────────────────
  hide('pitcher-k-card');
  const pkData  = result.pitcher_k || {};
  const pkPicks = pkData.picks || [];
  const pkAll   = pkData.all   || [];
  if (pkAll.length > 0) {
    show('pitcher-k-card');
    document.getElementById('pitcher-k-body').innerHTML = pkPicks.length > 0
      ? pkPicks.map((p, i) => {
          const isOver  = p.pick === 'OVER';
          const pickClr = isOver ? '#63cab7' : '#ff8a65';
          const gap     = p.avg_k != null ? (p.avg_k - p.line).toFixed(1) : '—';
          const gapDisp = p.avg_k != null ? (isOver ? '+' : '') + gap : '—';
          const odds    = isOver
            ? (p.over_odds  != null ? (p.over_odds  > 0 ? '+':'') + p.over_odds  : '')
            : (p.under_odds != null ? (p.under_odds > 0 ? '+':'') + p.under_odds : '');
          const sideCls = p.side === 'HOME' ? 'badge-home' : 'badge-away';
          return `<tr>
            <td><span class="rank-badge rank-n" style="background:#0d2e2e;color:#63cab7">${i+1}</span></td>
            <td class="font-semibold">${p.name}</td>
            <td><span class="badge ${sideCls}">${p.side}</span></td>
            <td class="text-slate-300 text-sm">${p.opp}</td>
            <td style="font-family:monospace;font-weight:700;color:#fff">${p.line} Ks</td>
            <td style="font-family:monospace;font-weight:700;color:${pickClr};font-size:1.05rem">${p.avg_k != null ? p.avg_k+' K' : '—'}</td>
            <td style="font-family:monospace;color:#93c5fd;font-weight:600">${p.avg_ip != null ? p.avg_ip+' IP' : '—'}</td>
            <td style="font-family:monospace;color:#fbbf24;font-weight:600">${p.era || '—'}</td>
            <td style="font-family:monospace;font-size:.82rem">${p.k_hit_rate || '—'}</td>
            <td style="font-family:monospace;color:${pickClr};font-weight:700">${gapDisp}</td>
            <td class="text-slate-400 text-sm">${p.starts}</td>
            <td style="font-family:monospace;font-size:.75rem;color:#94a3b8">${p.k_history || '—'}</td>
            <td><span style="color:${pickClr};font-weight:900;font-size:1rem">${p.pick}</span>
                <span class="text-slate-500" style="font-size:.68rem;display:block">${odds}</span></td>
          </tr>`;
        }).join('')
      : '<tr><td colspan="13" class="text-slate-500 text-center py-4">No picks today — pitchers on the line or vs tough K teams</td></tr>';
    const noPick = pkAll.filter(p => !p.pick);
    const npDet  = document.getElementById('pitcher-k-nopick-details');
    if (npDet) {
      if (noPick.length > 0) {
        npDet.style.display = '';
        document.getElementById('pitcher-k-nopick-body').innerHTML = noPick.map(p => {
          const sideCls = p.side === 'HOME' ? 'badge-home' : 'badge-away';
          return `<tr style="opacity:0.6">
            <td class="font-semibold">${p.name}</td>
            <td><span class="badge ${sideCls}">${p.side}</span></td>
            <td class="text-slate-400 text-sm">${p.opp}</td>
            <td style="font-family:monospace">${p.line} Ks</td>
            <td class="text-slate-400 text-sm">${p.starts}</td>
            <td class="text-slate-500 text-xs">${p.pick_note || '—'}</td>
          </tr>`;
        }).join('');
      } else { npDet.style.display = 'none'; }
    }
  }

  // ── DQ section
  const allDQ = [...(dq_s1_s3 || []), ...(dq_step4 || []), ...(result.dq_lineup || [])];
  document.getElementById('dq-count-badge').textContent = allDQ.length + ' players';
  const dqBody = document.getElementById('dq-body');
  if (allDQ.length === 0) {
    dqBody.innerHTML = `<div class="dq-row text-slate-500">None — all qualified!</div>`;
  } else {
    dqBody.innerHTML = allDQ.map(p => `
      <div class="dq-row">
        <span class="font-semibold w-36">${p.name}</span>
        <span class="badge badge-pos">${p.pos || '—'}</span>
        <span class="text-slate-400 text-xs">S1: ${p.s1?.toFixed(3)||'—'}</span>
        <span class="text-slate-400 text-xs">S2: ${p.s2?.display||'—'}</span>
        <span class="text-slate-400 text-xs">S3: ${p.s3?.display||'—'}</span>
        <span class="badge badge-dq ml-auto">${p.dq_reason || 'DQ'}</span>
      </div>`).join('');
  }

  show('results-card');
}

function statCard(icon, label, value, cls) {
  return `<div class="chip">
    <div class="val">${value}</div>
    <div class="lbl">${label}</div>
  </div>`;
}

function lineupBadge(status) {
  if (status === 'IN_LINEUP')     return '<span class="badge badge-in">✅ IN</span>';
  if (status === 'NOT_IN_LINEUP') return '<span class="badge badge-out">❌ OUT</span>';
  return '<span class="badge badge-tbd">⏳ TBD</span>';
}

function statColor(ba) {
  if (!ba && ba !== 0) return 'stat-na';
  return ba >= 0.300 ? 'stat-good' : ba >= 0.250 ? 'stat-warn' : 'stat-na';
}
function statColorStr(s) {
  if (!s || s === 'N/A' || s === '—') return 'stat-na';
  const n = parseFloat(s);
  return isNaN(n) ? 'stat-na' : statColor(n);
}

// ─── Log helpers ──────────────────────────────────────────────────────
function appendLog(msg, type) {
  const box = document.getElementById('log-box');
  const div = document.createElement('div');
  div.className = {
    section: 'log-section', ok: 'log-ok', dq: 'log-dq',
    skip: 'log-skip', info: 'log-info', cached: 'log-cached',
    under: 'log-under', default: 'log-default'
  }[type] || 'log-default';
  div.textContent = msg;
  box.appendChild(div);
  box.scrollTop = box.scrollHeight;
}
function clearLog() { document.getElementById('log-box').innerHTML = ''; }

function setProgress(pct, label) {
  document.getElementById('prog-bar-inner').style.width = pct + '%';
  document.getElementById('prog-label').textContent    = label;
}

// ─── Misc helpers ─────────────────────────────────────────────────────
function authFetch(url, opts = {}) {
  return fetch(url, {
    ...opts,
    headers: { ...(opts.headers || {}) }
  }).then(r => {
    return r;
  });
}

function pad(s, n) { return (s + ' '.repeat(n)).slice(0, n); }
function show(id)  { document.getElementById(id)?.classList.remove('hidden'); }
function hide(id)  { document.getElementById(id)?.classList.add('hidden'); }
function disableRunBtn(d) {
  const b = document.getElementById('run-btn');
  b.disabled = d;
  b.textContent = d ? "Running..." : "Run Picks";
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
async def serve_spa():
    return HTMLResponse(_HTML)
