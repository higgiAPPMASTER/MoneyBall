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

from fastapi import FastAPI, Depends, HTTPException, Form
from fastapi.responses import StreamingResponse, FileResponse, HTMLResponse
# StaticFiles removed — HTML is now embedded directly in this file

from auth import verify_user, create_token, get_current_user, get_user_from_query

# ── App setup ────────────────────────────────────────────────────────
app = FastAPI(title="MoneyBall", docs_url=None, redoc_url=None)
executor = ThreadPoolExecutor(max_workers=4)
_tasks: dict = {}   # task_id -> {events, status, result, notify}
_cache: dict = {}   # date -> result (in-memory, cleared on restart)

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






@app.get("/api/test-statmuse")
async def test_statmuse(user: str = Depends(get_current_user)):
    """Steps 2 & 3 now use MLB Stats API — always ready."""
    return {"ok": True, "message": "✅ MLB Stats API active"}


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
      --navy:   #0d0d0d;
      --navy2:  #1a1a1a;
      --navy3:  #2a2a2a;
      --green:  #FDB827;
      --red:    #ff1744;
      --yellow: #FDB827;
      --orange: #FDB827;
      --gold:   #FDB827;
      --silver: #c0c0c0;
      --bronze: #cd7f32;
    }
    body { background: #0d0d0d; color: #f0e6c8; font-family: 'Segoe UI', system-ui, sans-serif; }
    .card { background: var(--navy2); border: 1px solid rgba(255,255,255,.08); border-radius: 12px; }
    .btn-primary { background: linear-gradient(135deg, #FDB827, #e6a800); border: none; cursor: pointer; border-radius: 8px; padding: 12px 28px; font-size: 1rem; font-weight: 700; color: #000; transition: all .2s; letter-spacing:.5px; }
    .btn-primary:hover:not(:disabled) { transform: translateY(-1px); filter: brightness(1.15); box-shadow: 0 4px 20px rgba(21,101,192,.5); }
    .btn-primary:disabled { opacity: .5; cursor: not-allowed; }
    .btn-danger { background: linear-gradient(135deg, #c62828, #b71c1c); }

    /* Terminal log */
    #log-box { background: #050d1a; border: 1px solid rgba(0,200,83,.2); border-radius: 8px; height: 260px; overflow-y: auto; padding: 12px 16px; font-family: 'Courier New', monospace; font-size: .82rem; line-height: 1.6; }
    .log-section { color: var(--yellow); font-weight: 700; margin-top: 6px; }
    .log-ok  { color: var(--green); }
    .log-dq  { color: var(--red); }
    .log-skip { color: #64748b; }
    .log-info { color: #93c5fd; }
    .log-cached { color: #a78bfa; }
    .log-default { color: #cbd5e1; }
    .log-under { color: #ff8a65; }

    /* Progress bar */
    #prog-bar-inner { height: 6px; border-radius: 3px; background: linear-gradient(90deg, #FDB827, #fff176); transition: width .4s ease; }

    /* Results table */
    .results-table { width: 100%; border-collapse: collapse; }
    .results-table th { background: #1a1a1a; color: #FDB827; font-size: .75rem; text-transform: uppercase; letter-spacing: 1px; padding: 10px 14px; text-align: left; white-space: nowrap; }
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

    .stat-cell { font-family: 'Courier New', monospace; font-size: .88rem; font-weight: 600; }
    .stat-good { color: var(--green); }
    .stat-warn { color: var(--yellow); }
    .stat-na   { color: #475569; }
    .stat-under { color: #ff8a65; font-weight: 700; }  /* under-pick low BA */
    .score-big { font-size: 1.1rem; font-weight: 800; color: #FDB827; }

    .section-hdr { display: flex; align-items: center; gap: 8px; font-size: .9rem; font-weight: 700; color: #FDB827; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 12px; }
    .section-hdr::after { content:''; flex:1; height:1px; background:rgba(255,255,255,.07); }

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
  </style>
</head>
<body class="min-h-screen">

<!-- ═══════════════════════ LOGIN SCREEN ═══════════════════════════ -->
<div id="login-screen" class="min-h-screen flex items-center justify-center px-4">
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
<div id="dashboard" class="hidden min-h-screen flex flex-col">

  <!-- Header -->
  <header class="border-b border-white/5 px-6 py-4 flex items-center justify-between" style="background:var(--navy2)">
    <div class="flex items-center gap-3">
      <span style="font-size:2.8rem;line-height:1">⚾</span>
      <div>
        <h1 style="font-size:1.6rem;font-weight:900;letter-spacing:-0.5px">MoneyBall</h1>
        <p class="text-xs text-slate-500" id="hdr-date"></p>
      </div>
    </div>
    <div class="flex items-center gap-4">
      <span id="hdr-user" class="text-sm text-slate-400"></span>
      <button onclick="doLogout()" class="text-xs text-slate-500 hover:text-red-400 transition-colors">Sign out</button>
    </div>
  </header>

  <main class="flex-1 px-4 py-6 max-w-7xl mx-auto w-full space-y-6">

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
        <div id="run-spinner" class="hidden flex items-center gap-2 text-slate-400 text-sm">
          <span class="spinner"></span> Pipeline running…
        </div>
      </div>
      <p class="text-xs text-slate-500 mt-3">
        Runs all 5 steps: FIC + Baseball Musings → MLB H/A splits → ESPN Day/Night filter → ERA filter.
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
                <th>DK Line</th>
              </tr>
            </thead>
            <tbody id="under-picks-body"></tbody>
          </table>
        </div>
        <p class="text-xs text-slate-500 mt-4">
          <strong>Source</strong>: DraftKings players with 1.5 hits O/U &nbsp;|&nbsp;
          <strong>S1</strong> Career BA vs today's pitcher (under &lt; .250, min 4 AB) &nbsp;|&nbsp;
          <strong>S2</strong> Lifetime H/A BA vs today's opponent (under &lt; .225) &nbsp;|&nbsp;
          <strong>S3</strong> 2026 H/A BA (under &lt; .250) &nbsp;|&nbsp;
          All three must pass for a player to appear here. &nbsp;|&nbsp;
          <strong>Ranked #1 → coldest bat</strong> (lowest combined BA = strongest under pick).
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
  const { top9, dq_s1_s3, dq_step4, stats } = result;
  hide('also-ran-card'); hide('under-picks-card');  // reset on each run

  // Stats summary
  const sr = document.getElementById('stats-row');
  sr.innerHTML = [
    statCard('🎯', 'Top Picks',    top9.length,                         'text-yellow-400'),
    statCard('⬇️', 'Under Picks',  stats.under_count ?? 0,              'text-orange-400'),
    statCard('⚾', 'Games Today',  stats.games,                         'text-blue-400'),
    statCard('🔍', 'Players Run',  stats.step1_count,                   'text-purple-400'),
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
        <td><span class="rank-badge rank-n" style="background:#2a2a2a;color:#FDB827">${rank}</span></td>
        <td class="font-semibold">${p.name}</td>
        <td><span class="badge badge-pos">${p.pos || '—'}</span></td>
        <td><span class="badge ${p.side === 'HOME' ? 'badge-home' : 'badge-away'}">${p.side}</span></td>
        <td class="text-slate-300 text-sm">${p.opp}</td>
        <td class="stat-cell ${s1c}">${p.s1?.toFixed(3) || '—'}</td>
        <td class="stat-cell ${s2c}">${p.s2?.display || '—'}</td>
        <td class="stat-cell ${s3c}">${p.s3?.display || '—'}</td>
        <td><span class="badge ${dnCls} text-xs">${dnLabel}</span> <span class="stat-cell text-slate-300 text-xs">${dnDisp}</span></td>
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
        <td><span style="color:#ff8a65;font-weight:800;font-size:1rem">U 1.5</span>
            <span class="text-slate-500" style="font-size:.68rem;display:block">score ${p.under_score}</span></td>
      </tr>`;
    }).join('');
  }

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
