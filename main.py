"""
main.py — FastAPI app for MLB Daily Picks
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

from fastapi import FastAPI, Depends, HTTPException, Form
from fastapi.responses import StreamingResponse, FileResponse, HTMLResponse
# StaticFiles removed — HTML is now embedded directly in this file

from auth import verify_user, create_token, get_current_user, get_user_from_query

# ── App setup ────────────────────────────────────────────────────────
app = FastAPI(title="MoneyBall", docs_url=None, redoc_url=None)
executor = ThreadPoolExecutor(max_workers=4)

# ── Background StatMuse login on startup ─────────────────────────────
@app.on_event("startup")
async def startup_login():
    """Trigger StatMuse login in background thread so it doesn't block startup."""
    import asyncio
    asyncio.create_task(_bg_statmuse_login())

async def _bg_statmuse_login():
    loop = asyncio.get_event_loop()
    def _do_login():
        from statmuse_auth import login, get_status
        from statmuse_fetch import refresh_session
        ok = login()
        if ok:
            refresh_session()
        return get_status()
    status = await loop.run_in_executor(executor, _do_login)
    print(f"[StatMuse] {status['message']}")

# Task store  {task_id: {"events": [], "status": "running"|"done"|"error",
#                        "result": dict|None, "notify": asyncio.Event}}
_tasks: dict = {}

# Date-keyed result cache  (lost on restart — that's fine)
_cache: dict = {}


# ── Auth ─────────────────────────────────────────────────────────────
@app.post("/api/login")
async def login(username: str = Form(...), password: str = Form(...)):
    if not verify_user(username, password):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    token = create_token(username)
    return {"access_token": token, "token_type": "bearer", "username": username}


# ── Health ────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok", "today": str(date.today())}


# ── StatMuse status & re-login ─────────────────────────────────────────
@app.get("/api/statmuse-status")
async def statmuse_status(user: str = Depends(get_current_user)):
    """Return current StatMuse login status + live connection test."""
    from statmuse_auth import get_status
    from statmuse_fetch import test_connection
    auth  = get_status()
    probe = test_connection()
    return {**auth, "probe_ok": probe["ok"], "probe_msg": probe["message"]}


@app.post("/api/statmuse-login")
async def statmuse_relogin(user: str = Depends(get_current_user)):
    """Force a fresh StatMuse login (e.g. after changing credentials)."""
    def _do():
        from statmuse_auth import login, get_status
        from statmuse_fetch import refresh_session
        ok = login(force=True)
        if ok:
            refresh_session()
        return get_status()
    status = await asyncio.get_event_loop().run_in_executor(executor, _do)
    return status


@app.get("/api/test-statmuse")
async def test_statmuse(user: str = Depends(get_current_user)):
    """Quick StatMuse connectivity probe (used by the UI status badge)."""
    from statmuse_auth import get_status
    from statmuse_fetch import test_connection
    auth  = get_status()
    probe = test_connection()
    return {"ok": auth["ok"],
            "message": auth["message"]}


# ── Run pipeline ──────────────────────────────────────────────────────
@app.post("/api/run")
async def start_run(date_str: str, user: str = Depends(get_current_user)):
    """
    Kick off the 4-step pipeline for date_str (YYYY-MM-DD).
    Returns task_id immediately; stream progress via /api/stream/{task_id}.
    """
    # Return cached result instantly
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
        except Exception as exc:
            import traceback
            msg = f"{exc}\n{traceback.format_exc()}"
            emit({"type": "error", "msg": msg})
            task["status"] = "error"

    executor.submit(run_in_thread)
    return {"task_id": task_id, "cached": False}


# ── SSE stream ────────────────────────────────────────────────────────
@app.get("/api/stream/{task_id}")
async def stream_task(task_id: str, token: Optional[str] = None):
    """
    Server-Sent Events stream for a running task.
    Pass JWT as ?token=... (browsers can't set custom headers for SSE).
    """
    get_user_from_query(token)  # auth check

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
@app.get("/api/lineups/{date_str}")
async def refresh_lineups(date_str: str, user: str = Depends(get_current_user)):
    from lineup_check import build_lineup_map
    if date_str not in _cache:
        raise HTTPException(status_code=404, detail="Run picks first.")
    id_map, name_map, teams = build_lineup_map(date_str.replace("-",""))
    for p in _cache[date_str].get("top9",[]):
        pid = p.get("player_id"); fn = p.get("full_name",""); tn = p.get("team","")
        if tn not in teams: p["lineup_status"] = "TBD"
        elif (pid and int(pid) in id_map) or fn.lower().strip() in name_map: p["lineup_status"] = "IN_LINEUP"
        else: p["lineup_status"] = "NOT_IN_LINEUP"
    return {"top9": _cache[date_str]["top9"], "teams_with_lineups": list(teams)}


@app.get("/api/results/{date_str}")
async def get_results(date_str: str, user: str = Depends(get_current_user)):
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
  <title>⚾ MoneyBall</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    :root {
      --black:  #0a0a0a;
      --black2: #141414;
      --black3: #1e1e1e;
      --gold:   #FDB827;
      --gold2:  #C4901A;
      --gold3:  #f5e27a;
      --green:  #00c853;
      --red:    #ff1744;
      --yellow: #FDB827;
      --orange: #ff6d00;
      --silver: #c0c0c0;
      --bronze: #cd7f32;
    }
    body { background: var(--black); color: #f0f0f0; font-family: 'Segoe UI', system-ui, sans-serif; }
    body::before { content:''; position:fixed; top:0; left:0; right:0; height:3px; background:linear-gradient(90deg,#FDB827,#fff5cc,#FDB827,#C4901A,#FDB827); background-size:300% 100%; animation:goldShimmer 4s linear infinite; z-index:999; }
    @keyframes goldShimmer { 0%{background-position:0% 50%} 100%{background-position:300% 50%} }
    .card { background: var(--black2); border: 1px solid rgba(253,184,39,.18); border-radius: 12px; box-shadow: 0 4px 24px rgba(0,0,0,.6); }
    .btn-primary { background: linear-gradient(135deg, #FDB827, #C4901A); border: none; cursor: pointer; border-radius: 8px; padding: 12px 28px; font-size: 1rem; font-weight: 800; color: #000; transition: all .2s; letter-spacing:.5px; }
    .btn-primary:hover:not(:disabled) { transform: translateY(-1px); filter: brightness(1.15); box-shadow: 0 4px 20px rgba(253,184,39,.5); }
    .btn-primary:disabled { opacity: .4; cursor: not-allowed; }
    .btn-danger { background: linear-gradient(135deg, #c62828, #b71c1c); color:#fff; }

    /* Terminal log */
    #log-box { background: #050505; border: 1px solid rgba(253,184,39,.25); border-radius: 8px; height: 260px; overflow-y: auto; padding: 12px 16px; font-family: 'Courier New', monospace; font-size: .82rem; line-height: 1.6; }
    .log-section { color: var(--gold); font-weight: 700; margin-top: 6px; }
    .log-ok      { color: var(--green); }
    .log-dq      { color: var(--red); }
    .log-skip    { color: #555; }
    .log-info    { color: var(--gold3); }
    .log-cached  { color: #a78bfa; }
    .log-default { color: #ccc; }

    /* Progress bar */
    #prog-bar-inner { height: 6px; border-radius: 3px; background: linear-gradient(90deg, #C4901A, #FDB827, #fff5cc); transition: width .4s ease; }

    /* Results table */
    .results-table { width: 100%; border-collapse: collapse; }
    .results-table th { background: #0f0f0f; color: var(--gold); font-size: .72rem; text-transform: uppercase; letter-spacing: 1.5px; padding: 12px 14px; text-align: left; white-space: nowrap; border-bottom: 2px solid rgba(253,184,39,.3); }
    .results-table td { padding: 13px 14px; border-bottom: 1px solid rgba(253,184,39,.06); vertical-align: middle; }
    .results-table tr:hover td { background: rgba(253,184,39,.04); }
    .results-table tr:last-child td { border-bottom: none; }

    .rank-badge { width: 34px; height: 34px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-weight: 900; font-size: .85rem; }
    .rank-1 { background: linear-gradient(135deg,#FDB827,#fff5a0); color: #000; box-shadow: 0 0 12px rgba(253,184,39,.6); }
    .rank-2 { background: var(--silver); color: #000; }
    .rank-3 { background: var(--bronze); color: #fff; }
    .rank-n { background: #222; color: #666; border: 1px solid #333; }

    .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: .72rem; font-weight: 700; letter-spacing:.3px; }
    .badge-home  { background: rgba(253,184,39,.2); color: var(--gold); }
    .badge-away  { background: rgba(255,255,255,.08); color: #ccc; }
    .badge-pos   { background: rgba(253,184,39,.1); color: #aaa; border: 1px solid rgba(253,184,39,.2); }
    .badge-day   { background: rgba(253,184,39,.2); color: var(--gold); }
    .badge-night { background: rgba(80,80,180,.3); color: #a5b4fc; }
    .badge-dq    { background: rgba(255,23,68,.15); color: #ff6b6b; font-size:.7rem; padding: 2px 6px; }

    .stat-cell { font-family: 'Courier New', monospace; font-size: .88rem; font-weight: 700; }
    .stat-good { color: var(--green); }
    .stat-warn { color: var(--gold); }
    .stat-na   { color: #444; }
    .score-big { font-size: 1.15rem; font-weight: 900; color: var(--gold); text-shadow: 0 0 8px rgba(253,184,39,.4); }

    .section-hdr { display: flex; align-items: center; gap: 8px; font-size: .85rem; font-weight: 800; color: var(--gold); text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 14px; }
    .section-hdr::after { content:''; flex:1; height:1px; background:rgba(253,184,39,.2); }

    /* DQ accordion */
    details > summary { cursor: pointer; list-style: none; user-select: none; }
    details > summary::-webkit-details-marker { display: none; }
    .dq-row { font-size: .82rem; padding: 7px 14px; border-bottom: 1px solid rgba(253,184,39,.06); display: flex; gap: 16px; align-items: center; }
    .dq-row:last-child { border-bottom: none; }

    /* Spinner */
    @keyframes spin { to { transform: rotate(360deg); } }
    .spinner { width: 18px; height: 18px; border: 3px solid rgba(253,184,39,.15); border-top-color: #FDB827; border-radius: 50%; animation: spin .7s linear infinite; display: inline-block; }

    /* Login */
    .login-input { background: #111; border: 1px solid rgba(253,184,39,.25); color: #f0f0f0; border-radius: 8px; padding: 11px 16px; width: 100%; font-size: 1rem; outline: none; transition: border-color .2s; }
    .login-input:focus { border-color: var(--gold); box-shadow: 0 0 0 3px rgba(253,184,39,.15); }

    ::-webkit-scrollbar { width: 6px; } ::-webkit-scrollbar-track { background: transparent; } ::-webkit-scrollbar-thumb { background: rgba(253,184,39,.2); border-radius: 3px; }
    .badge-lineup-in  { background:rgba(0,200,83,.2);  color:#00c853; border-radius:4px; padding:1px 6px; font-size:11px; font-weight:700; }
    .badge-lineup-out { background:rgba(255,23,68,.2);  color:#ff1744; border-radius:4px; padding:1px 6px; font-size:11px; font-weight:700; }
    .badge-lineup-tbd { background:rgba(253,184,39,.15); color:#FDB827; border-radius:4px; padding:1px 6px; font-size:11px; font-weight:700; }
    .row-scratched    { opacity:.4; text-decoration:line-through; }
  </style>
</head>
<body class="min-h-screen">

<!-- ═══════════════════════ LOGIN SCREEN ═══════════════════════════ -->
<div id="login-screen" class="min-h-screen flex items-center justify-center px-4">
  <div class="card p-10 w-full max-w-md shadow-2xl">
    <div class="text-center mb-8">
      <div style="font-size:4.5rem;filter:drop-shadow(0 0 16px rgba(253,184,39,.8));margin-bottom:12px">⚾</div>
      <h1 class="font-black" style="font-size:2.5rem;letter-spacing:4px;background:linear-gradient(135deg,#FDB827,#fff5a0,#C4901A);-webkit-background-clip:text;-webkit-text-fill-color:transparent">MoneyBall</h1>
      <p style="color:rgba(253,184,39,.6);font-size:.8rem;letter-spacing:3px;margin-top:4px">MLB DAILY PICKS</p>
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
<div id="dashboard" class="hidden min-h-screen flex flex-col">

  <!-- Header -->
  <header class="px-6 py-4 flex items-center justify-between" style="background:#0f0f0f;border-bottom:2px solid rgba(253,184,39,.3);box-shadow:0 2px 20px rgba(253,184,39,.1)">
    <div class="flex items-center gap-4">
      <div style="font-size:2.2rem;filter:drop-shadow(0 0 8px rgba(253,184,39,.7))">⚾</div>
      <div>
        <h1 class="font-black tracking-tight" style="font-size:1.6rem;letter-spacing:3px;background:linear-gradient(135deg,#FDB827,#fff5a0,#C4901A);-webkit-background-clip:text;-webkit-text-fill-color:transparent">MoneyBall</h1>
        <p class="text-xs" style="color:rgba(253,184,39,.6);letter-spacing:2px" id="hdr-date">MLB DAILY PICKS</p>
      </div>
    </div>
    <div class="flex items-center gap-4">
      <div id="sm-status"
           onclick="checkStatMuse()"
           title="Click to re-test StatMuse connection"
           style="display:flex;align-items:center;gap:6px;font-size:.75rem;padding:4px 12px;border-radius:999px;border:1px solid #475569;color:#94a3b8;cursor:pointer;transition:all .2s">
        <span id="sm-dot" style="width:8px;height:8px;border-radius:50%;background:#64748b;display:inline-block"></span>
        <span id="sm-label">StatMuse</span>
      </div>
      <span id="hdr-user" class="text-sm text-slate-400"></span>
      <button onclick="doLogout()" style="font-size:.75rem;color:rgba(253,184,39,.5);cursor:pointer;background:none;border:none;transition:color .2s" onmouseover="this.style.color='#FDB827'" onmouseout="this.style.color='rgba(253,184,39,.5)'">Sign out</button>
    </div>
  </header>

  <main class="flex-1 px-4 py-6 max-w-7xl mx-auto w-full space-y-6">

    <!-- StatMuse login status card -->
    <div id="sm-login-card" class="card p-5 hidden">
      <div class="flex items-center gap-4">
        <div id="sm-login-icon" class="text-3xl">🔄</div>
        <div class="flex-1">
          <div class="font-bold text-sm" id="sm-login-title">Connecting to StatMuse…</div>
          <div class="text-xs text-slate-400 mt-0.5" id="sm-login-msg">Launching browser login — this takes about 10 seconds</div>
        </div>
        <button onclick="forceRelogin()" id="sm-relogin-btn"
                class="text-xs text-slate-400 hover:text-blue-400 border border-slate-600 hover:border-blue-500 rounded px-3 py-1 transition-colors hidden">
          Retry login
        </button>
      </div>
    </div>

    <!-- Controls card -->
    <div class="card p-6">
      <div class="flex flex-col sm:flex-row gap-4 items-start sm:items-end">
        <div>
          <label class="block text-xs font-semibold text-slate-400 uppercase tracking-widest mb-2">Date</label>
          <input type="date" id="date-picker"
                 class="login-input" style="width:auto; min-width:160px;" />
        </div>
        <button id="run-btn" onclick="startRun()" class="btn-primary flex items-center gap-2">
          <span>▶</span> Run Today's Picks
        </button>
        <button id="lineup-btn" onclick="refreshLineups()" class="btn-primary flex items-center gap-2" style="background:linear-gradient(135deg,#1b5e20,#2e7d32);color:#fff">
          <span>&#128260;</span> Refresh Lineups
        </button>
        <div id="lineup-spinner" class="hidden flex items-center gap-2 text-sm" style="color:#FDB827">
          <span class="spinner"></span> Checking lineups…
        </div>
        <div id="run-spinner" class="hidden flex items-center gap-2 text-slate-400 text-sm">
          <span class="spinner"></span> Pipeline running…
        </div>
      </div>
      <p class="text-xs text-slate-500 mt-3">
        Runs all 4 steps: FIC lifetime BA → StatMuse H/A splits → ESPN Day/Night filter.
        Takes 3–5 minutes. Results are cached — re-selecting same date is instant.
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
      <div id="stats-row" class="grid grid-cols-2 sm:grid-cols-4 gap-4"></div>

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
  if (token) {
    showDashboard();
  } else {
    show('login-screen'); hide('dashboard');
  }
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
  if (es) { es.close(); es = null; }
  hide('dashboard'); show('login-screen');
}

function showLoginError(msg) {
  const el = document.getElementById('login-error');
  el.textContent = msg; el.classList.remove('hidden');
}

// ─── Dashboard init ────────────────────────────────────────────────────
function showDashboard() {
  hide('login-screen'); show('dashboard');
  document.getElementById('hdr-user').textContent = `👤 ${username}`;
  const today = new Date().toISOString().slice(0,10);
  document.getElementById('hdr-date').textContent = `Today: ${today}`;
  document.getElementById('date-picker').value = today;
  hide('progress-card'); hide('results-card');
  pollStatMuseLogin();
}

// Poll login status on startup until ready
let _smPollTimer = null;
async function pollStatMuseLogin() {
  show('sm-login-card');
  _smPoll();
}

async function _smPoll() {
  try {
    const r = await authFetch('/api/test-statmuse');
    const d = await r.json();
    updateSmBadge(d.ok, d.message);
    updateSmCard(d.ok, d.message);
    if (!d.ok) {
      // Still logging in or failed — re-poll in 3s
      _smPollTimer = setTimeout(_smPoll, 3000);
    } else {
      disableRunBtn(false);
    }
  } catch {
    _smPollTimer = setTimeout(_smPoll, 5000);
  }
}

function updateSmCard(ok, msg) {
  const icon  = document.getElementById('sm-login-icon');
  const title = document.getElementById('sm-login-title');
  const msgEl = document.getElementById('sm-login-msg');
  const retryBtn = document.getElementById('sm-relogin-btn');
  if (ok) {
    icon.textContent  = '✅';
    title.textContent = 'StatMuse connected';
    msgEl.textContent = msg;
    retryBtn.classList.add('hidden');
    // Auto-hide after 4 seconds
    setTimeout(() => hide('sm-login-card'), 4000); disableRunBtn(false);
  } else {
    icon.textContent  = msg.includes('Logging') || msg.includes('started') ? '🔄' : '❌';
    title.textContent = msg.includes('Logging') || msg.includes('started') ? 'Logging into StatMuse…' : 'StatMuse login failed';
    msgEl.textContent = msg;
    if (!msg.includes('Logging') && !msg.includes('started')) {
      retryBtn.classList.remove('hidden');
    }
  }
}

async function forceRelogin() {
  clearTimeout(_smPollTimer);
  document.getElementById('sm-login-icon').textContent = '🔄';
  document.getElementById('sm-login-title').textContent = 'Retrying login…';
  document.getElementById('sm-relogin-btn').classList.add('hidden');
  disableRunBtn(true);
  try { await authFetch('/api/statmuse-login', { method: 'POST' }); } catch {}
  _smPollTimer = setTimeout(_smPoll, 2000);
}

async function checkStatMuse() {
  const dot   = document.getElementById('sm-dot');
  const label = document.getElementById('sm-label');
  const pill  = document.getElementById('sm-status');
  dot.className   = 'w-2 h-2 rounded-full bg-yellow-400';
  label.textContent = 'Checking…';
  try {
    const r = await authFetch('/api/test-statmuse');
    const d = await r.json();
    if (d.ok) {
      dot.className   = 'w-2 h-2 rounded-full bg-green-400';
      label.textContent = 'StatMuse ✓';
      pill.style.borderColor = '#16a34a'; pill.style.color = '#4ade80';
      pill.title = d.message;
    } else {
      dot.className   = 'w-2 h-2 rounded-full bg-red-500';
      label.textContent = 'StatMuse ✗';
      pill.style.borderColor = '#dc2626'; pill.style.color = '#f87171';
      pill.title = d.message;
      showStatMuseWarning(d.message);
    }
  } catch(e) {
    label.textContent = 'StatMuse ?';
  }
}

function showStatMuseWarning(msg) {
  if (document.getElementById('sm-warning')) return;
  const w = document.createElement('div');
  w.id = 'sm-warning';
  w.style.cssText = 'margin-top:16px;padding:16px;border-radius:8px;border:1px solid #854d0e;background:rgba(120,53,15,.25);color:#fde68a;font-size:.85rem;line-height:1.7';
  w.innerHTML = `<strong>⚠️ StatMuse not connected</strong> — Steps 2 & 3 will fail on the server.<br>
<span style="color:#fcd34d">How to fix in 2 minutes:</span><br>
1. Log into <a href="https://www.statmuse.com" target="_blank" style="color:#93c5fd;text-decoration:underline">statmuse.com</a> in Chrome/Firefox<br>
2. Press <strong>F12</strong> → Network tab → reload the page<br>
3. Click any <em>statmuse.com</em> request → scroll to <strong>Request Headers</strong> → copy the full <code style="background:rgba(0,0,0,.4);padding:1px 5px;border-radius:3px">Cookie:</code> value<br>
4. On Render → Environment Variables → add <code style="background:rgba(0,0,0,.4);padding:1px 5px;border-radius:3px">STATMUSE_COOKIES</code> → paste → Save & Deploy`;
  document.getElementById('controls').appendChild(w);
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
    appendLog('⚠️  Connection lost — refresh and try again.', 'dq');
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

    case 'error':
      appendLog(`❌ ERROR: ${ev.msg}`, 'dq');
      hide('run-spinner'); disableRunBtn(false);
      es.close();
      break;
  }
}

// ─── Results rendering ────────────────────────────────────────────────
function showResults(result) {
  const { top9, dq_s1_s3, dq_step4, stats } = result;

  // Stats summary
  const sr = document.getElementById('stats-row');
  sr.innerHTML = [
    statCard('🎯', 'Top Picks',    top9.length,                       'text-yellow-400'),
    statCard('⚾', 'Games Today',  stats.games,                        'text-blue-400'),
    statCard('🔍', 'Players Run',  stats.step1_count,                  'text-purple-400'),
    statCard('⏱',  'Run Time',    `${stats.elapsed}s`,                'text-green-400'),
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
    const ls = p.lineup_status || 'TBD';
    const lb = ls==='IN_LINEUP'     ? '<span class=\"badge-lineup-in\">🟢 IN</span>'
             : ls==='NOT_IN_LINEUP' ? '<span class=\"badge-lineup-out\">🔴 OUT</span>'
             :                        '<span class=\"badge-lineup-tbd\">🟡 TBD</span>';
    return `
      <tr class="${ls==='NOT_IN_LINEUP'?'row-scratched':''}">
        <td><span class="rank-badge ${rnk}">${rank}</span></td>
        <td class="font-semibold">${p.name} ${lb}</td>
        <td><span class="badge badge-pos">${p.pos || '—'}</span></td>
        <td><span class="badge ${p.side === 'HOME' ? 'badge-home' : 'badge-away'}">${p.side}</span></td>
        <td class="text-slate-300 text-sm">${p.opp}</td>
        <td class="stat-cell ${s1c}">${p.s1?.toFixed(3) || '—'}</td>
        <td class="stat-cell ${s2c}">${p.s2?.display || '—'}</td>
        <td class="stat-cell ${s3c}">${p.s3?.display || '—'}</td>
        <td><span class="badge ${dnCls} text-xs">${dnLabel}</span> <span class="stat-cell text-slate-300 text-xs">${dnDisp}</span></td>
        <td><span class="score-big">${p.total}</span></td>
      </tr>`;
  }).join('');

  // DQ section
  const allDQ = [...(dq_s1_s3 || []), ...(dq_step4 || [])];
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
  return `
    <div class="card p-5 text-center">
      <div class="text-2xl mb-1">${icon}</div>
      <div class="text-2xl font-black ${cls}">${value}</div>
      <div class="text-xs text-slate-500 uppercase tracking-wider mt-1">${label}</div>
    </div>`;
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
    skip: 'log-skip', info: 'log-info', cached: 'log-cached', default: 'log-default'
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
    headers: { ...(opts.headers || {}), 'Authorization': `Bearer ${token}` }
  }).then(r => {
    if (r.status === 401) { doLogout(); throw new Error('Session expired — please sign in again.'); }
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
</body>
</html>

"""

@app.get("/")
async def serve_spa():
    return HTMLResponse(_HTML)
