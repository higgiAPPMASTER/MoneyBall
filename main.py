"""
main.py — FastAPI app for MoneyBall
  • POST /api/login          — get JWT token
  • POST /api/run            — kick off pipeline (returns task_id)
  • GET  /api/stream/{id}   — SSE progress stream (token as query param)
  • GET  /api/results/{date} — fetch cached results
  • GET  /                   — serves the frontend SPA
"""
import asyncio, json, os, uuid, glob as _glob, unicodedata as _ud
import datetime as _dt, copy as _copy
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Optional

_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".pick_cache")
os.makedirs(_CACHE_DIR, exist_ok=True)

# ── Supabase (permanent bet log + track ledger) ─────────────────────────
_SB_URL_RAW = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
_SB_URL = (f"https://{_SB_URL_RAW}.supabase.co"
           if _SB_URL_RAW and not _SB_URL_RAW.startswith("http")
           else _SB_URL_RAW)
_SB_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
_SB_HDRS = {
    "apikey": _SB_KEY,
    "Authorization": f"Bearer {_SB_KEY}",
    "Content-Type": "application/json",
}

def _sb_get(table, params=None, timeout=10):
    import requests as _r
    try:
        rsp = _r.get(f"{_SB_URL}/rest/v1/{table}", headers=_SB_HDRS,
                     params=params, timeout=timeout)
        if rsp.status_code == 200:
            return rsp.json()
        print(f"[sb] GET {table} {rsp.status_code}: {rsp.text[:120]}")
    except Exception as e:
        print(f"[sb] GET {table} failed: {e}")
    return None

def _sb_upsert(table, rows, on_conflict=None, timeout=10):
    import requests as _r
    if not rows:
        return True
    h = {**_SB_HDRS, "Prefer": "resolution=merge-duplicates,return=minimal"}
    payload = rows if isinstance(rows, list) else [rows]
    params = {"on_conflict": on_conflict} if on_conflict else None
    try:
        rsp = _r.post(f"{_SB_URL}/rest/v1/{table}", headers=h, params=params,
                      json=payload, timeout=timeout)
        if rsp.status_code not in (200, 201, 204):
            print(f"[sb] UPSERT {table} {rsp.status_code}: {rsp.text[:120]}")
            return False
        return True
    except Exception as e:
        print(f"[sb] UPSERT {table} failed: {e}")
        return False

def _sb_delete(table, params, timeout=10):
    import requests as _r
    try:
        rsp = _r.delete(f"{_SB_URL}/rest/v1/{table}", headers=_SB_HDRS,
                        params=params, timeout=timeout)
        return rsp.status_code in (200, 204)
    except Exception as e:
        print(f"[sb] DELETE {table} failed: {e}")
        return False

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

# ── Daily pick snapshot in Supabase (survives Render redeploys) ──────────
# The disk pick cache lives on Render's ephemeral filesystem and is wiped on
# every deploy, so a day's picks can vanish before its games go Final and the
# day can be graded. We mirror each run's picks into Supabase keyed by date;
# the LAST run of the day overwrites the row, so the final pre-game run is what
# gets graded and locked. Stored in the existing mpa_track_ledger table as a
# category="__picks__" row (locked=False, so it never pollutes the W/L read).
_PICKS_CAT = "__picks__"

def _save_sb_picks(date_str: str, result: dict):
    if not (_SB_URL and _SB_KEY):
        return
    import datetime as _dt
    row = {"app": "mlb", "date": date_str, "category": _PICKS_CAT, "side": "ALL",
           "wins": 0, "losses": 0, "locked": False,
           "locked_at": _dt.datetime.utcnow().isoformat() + "Z", "detail": result}
    # merge on the (app,date,category,side) unique key → last run of the day wins
    # Use 30s timeout — the full picks payload can be large; 10s times out silently.
    import json as _json
    _sz = len(_json.dumps(result))
    ok = _sb_upsert("mpa_track_ledger", [row], on_conflict="app,date,category,side", timeout=30)
    if ok:
        print(f"[sb_picks] saved {date_str} ({_sz} bytes)")
    else:
        print(f"[sb_picks] SAVE FAILED for {date_str} ({_sz} bytes) — Track Record may show stale lines")
    # prune snapshots >7 days old (those dates are already locked in the ledger)
    try:
        cutoff = date.fromordinal(date.today().toordinal() - 7).isoformat()
        _sb_delete("mpa_track_ledger", {"app": "eq.mlb",
                   "category": f"eq.{_PICKS_CAT}", "date": f"lt.{cutoff}"})
    except Exception:
        pass

def _load_sb_picks(date_str: str):
    if not (_SB_URL and _SB_KEY):
        return None
    rows = _sb_get("mpa_track_ledger", {"app": "eq.mlb",
                   "category": f"eq.{_PICKS_CAT}", "side": "eq.ALL",
                   "date": f"eq.{date_str}", "select": "detail", "limit": "1"})
    if rows:
        return rows[0].get("detail") or None
    return None

def _list_sb_pick_dates():
    if not (_SB_URL and _SB_KEY):
        return []
    rows = _sb_get("mpa_track_ledger", {"app": "eq.mlb",
                   "category": f"eq.{_PICKS_CAT}", "side": "eq.ALL", "select": "date"})
    if not rows:
        return []
    return sorted({r["date"] for r in rows if r.get("date")})

def _load_pick_cache(date_str: str):
    """Picks for a date: local disk first (fast), else the Supabase snapshot
    (survives redeploys). Used by every read/grade path."""
    d = _load_disk_cache(date_str)
    if d is not None:
        return d
    return _load_sb_picks(date_str)

# ── Manual pick lock (Track Record integrity) ────────────────────────────
# Admin hits "Lock Picks" just before game time. That snapshot is what gets
# graded and banked — subsequent re-runs update the live display but never
# touch the locked picks. Stored as category="__locked__" in mpa_track_ledger.
_LOCKED_CAT = "__locked__"

def _save_locked_picks(date_str: str, result: dict) -> bool:
    """Save picks as the locked Track Record snapshot for date_str.
    Will NOT overwrite an existing lock — returns False if already locked."""
    if not (_SB_URL and _SB_KEY):
        return False
    # check existing
    existing = _sb_get("mpa_track_ledger", {"app": "eq.mlb",
                       "category": f"eq.{_LOCKED_CAT}", "side": "eq.ALL",
                       "date": f"eq.{date_str}", "select": "date", "limit": "1"})
    if existing:
        return False   # already locked — refuse to overwrite
    import datetime as _dt
    row = {"app": "mlb", "date": date_str, "category": _LOCKED_CAT, "side": "ALL",
           "wins": 0, "losses": 0, "locked": False,
           "locked_at": _dt.datetime.utcnow().isoformat() + "Z", "detail": result}
    ok = _sb_upsert("mpa_track_ledger", [row], on_conflict="app,date,category,side", timeout=30)
    if ok:
        print(f"[lock_picks] locked {date_str}")
    return ok

def _load_locked_picks(date_str: str):
    """Return the manually-locked snapshot for date_str, or None if not locked."""
    if not (_SB_URL and _SB_KEY):
        return None
    rows = _sb_get("mpa_track_ledger", {"app": "eq.mlb",
                   "category": f"eq.{_LOCKED_CAT}", "side": "eq.ALL",
                   "date": f"eq.{date_str}", "select": "detail", "limit": "1"})
    if rows:
        return rows[0].get("detail") or None
    return None

def _load_grading_picks(date_str: str):
    """For grading: prefer the manually-locked snapshot; fall back to __picks__."""
    locked = _load_locked_picks(date_str)
    if locked:
        return locked
    return _load_pick_cache(date_str)

# ── Pre-game snapshot freeze (Track Record integrity) ───────────────────
# The graded snapshot (disk + Supabase) must reflect the LAST run BEFORE each
# game's first pitch. A re-run while games are live would otherwise rewrite a
# pick's line/side with in-game numbers (e.g. a pitcher already past his K
# total flips UNDER 5.5 → OVER 2.5), and that corrupted line is what gets
# locked. So: for TODAY, any pick whose game has already started keeps the
# line/side from the prior snapshot; only not-yet-started games take fresh
# lines. Per-game (not whole-slate) so late games still update until they start.
def _pick_started(p, now_dt) -> bool:
    if not isinstance(p, dict):
        return False
    gs = p.get("game_start")
    if not gs:
        return False
    try:
        t = _dt.datetime.fromisoformat(str(gs).replace("Z", "+00:00"))
        if t.tzinfo is not None:
            t = t.astimezone(_dt.timezone.utc).replace(tzinfo=None)
        return now_dt >= t
    except Exception:
        return False

def _pick_ident(p):
    pid = p.get("player_id") or p.get("batter_id") or p.get("pid") or ""
    nm  = p.get("name") or p.get("full_name") or ""
    mkt = p.get("market") or ""
    return (str(pid), str(nm).lower(), str(mkt))

def _freeze_merge(old_node, new_node, now_dt):
    # Recurse through dicts on shared keys (e.g. pitcher_k -> picks/all, pitcher_props -> market -> picks/all).
    if isinstance(new_node, dict) and isinstance(old_node, dict):
        for k, v in new_node.items():
            if k in old_node:
                new_node[k] = _freeze_merge(old_node[k], v, now_dt)
        return new_node
    # Merge lists of pick dicts; leave every other list untouched.
    if isinstance(new_node, list) and isinstance(old_node, list):
        if not new_node or not all(isinstance(x, dict) for x in new_node):
            return new_node
        old_by_id = {}
        for i, op in enumerate(old_node):
            if isinstance(op, dict):
                old_by_id.setdefault(_pick_ident(op), (i, op))
        seen = set()
        merged = []
        for np in new_node:
            ident = _pick_ident(np)
            seen.add(ident)
            if _pick_started(np, now_dt) and ident in old_by_id:
                merged.append(old_by_id[ident][1])      # frozen pre-game pick
            else:
                merged.append(np)                        # not started → fresh line
        # Pre-game picks for started games that dropped out of the live run:
        # re-add near their original rank so the [:10] grading window is preserved.
        missing = [(i, op) for ident, (i, op) in old_by_id.items()
                   if ident not in seen and _pick_started(op, now_dt)]
        missing.sort()
        for i, op in missing:
            merged.insert(min(i, len(merged)), op)
        return merged
    return new_node

def _freeze_started_picks(date_str: str, new_result: dict):
    """Return a snapshot for date_str with started-game picks frozen at their
    prior-snapshot (pre-game) line/side. Only affects TODAY; past/future dates
    and the first run of the day pass through unchanged."""
    try:
        if date_str != date.today().isoformat():
            return new_result
        old = _load_pick_cache(date_str)
        if not isinstance(old, dict) or not old:
            return new_result            # no pre-game snapshot yet → take this run
        return _freeze_merge(old, _copy.deepcopy(new_result), _dt.datetime.utcnow())
    except Exception as e:
        print(f"[freeze] skipped for {date_str}: {e}")
        return new_result

# ── Opening-odds snapshot (Closing Line Value) ──────────────────────────
# The FIRST run of the day captures the price you'd have bet at; later runs
# (last-wins) become the closing line. CLV compares the two. Written ONCE per
# day and never overwritten, so the opening price is preserved even though the
# regular pick snapshot keeps getting replaced. Stored like the pick snapshot
# but under category="__open__" (locked=False, so it never hits the W/L read).
_OPEN_CAT = "__open__"

def _disk_open_path(date_str: str) -> str:
    return os.path.join(_CACHE_DIR, f"_open_{date_str}.json")

def _load_disk_open(date_str: str):
    p = _disk_open_path(date_str)
    if os.path.exists(p):
        try:
            with open(p) as f:
                return json.load(f)
        except Exception:
            pass
    return None

def _load_sb_open(date_str: str):
    if not (_SB_URL and _SB_KEY):
        return None
    rows = _sb_get("mpa_track_ledger", {"app": "eq.mlb",
                   "category": f"eq.{_OPEN_CAT}", "side": "eq.ALL",
                   "date": f"eq.{date_str}", "select": "detail", "limit": "1"})
    if rows:
        return rows[0].get("detail") or None
    return None

def _load_open_cache(date_str: str):
    """Opening snapshot for a date. Supabase is the source of truth for
    'first ever' (disk resets on redeploy), so prefer it when configured."""
    if _SB_URL and _SB_KEY:
        d = _load_sb_open(date_str)
        if d is not None:
            return d
        return _load_disk_open(date_str)
    return _load_disk_open(date_str)

def _save_open_snapshot(date_str: str, result: dict):
    """First run of the day only — capture opening odds for CLV. Write-once:
    if an opening snapshot already exists, keep it untouched."""
    def _write_disk():
        try:
            p = _disk_open_path(date_str)
            tmp = f"{p}.{os.getpid()}.tmp"
            with open(tmp, "w") as f:
                json.dump(result, f)
            os.replace(tmp, p)
        except Exception as e:
            print(f"[open_cache] save failed: {e}")
    if _SB_URL and _SB_KEY:
        if _load_sb_open(date_str) is not None:
            return                      # already captured today (survives redeploy)
        import datetime as _dt
        row = {"app": "mlb", "date": date_str, "category": _OPEN_CAT, "side": "ALL",
               "wins": 0, "losses": 0, "locked": False,
               "locked_at": _dt.datetime.utcnow().isoformat() + "Z", "detail": result}
        _sb_upsert("mpa_track_ledger", [row], on_conflict="app,date,category,side")
        try:
            cutoff = date.fromordinal(date.today().toordinal() - 7).isoformat()
            _sb_delete("mpa_track_ledger", {"app": "eq.mlb",
                       "category": f"eq.{_OPEN_CAT}", "date": f"lt.{cutoff}"})
        except Exception:
            pass
        _write_disk()                   # mirror for fast same-process reads
    else:
        if os.path.exists(_disk_open_path(date_str)):
            return                      # disk-only write-once
        _write_disk()

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

# Tester accounts — see everything admin sees except Run Picks / Force Refresh.
# Add emails here or override via TESTER_EMAILS env var (comma-separated).
_TESTER_EMAILS = {e.strip().lower() for e in _os.environ.get("TESTER_EMAILS", "curtsmith95@gmail.com").split(",") if e.strip()}


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

def _is_tester_token(token: str) -> bool:
    return bool(_TESTER_EMAILS) and _token_email(token) in _TESTER_EMAILS


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
            disk = _load_pick_cache(date_str)
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
            # Hard-lock the DISPLAY to the final pre-game run: any game already
            # past first pitch keeps its pre-game line/side (same freeze the
            # graded snapshot uses), so a has_tbd re-run after games start can
            # never rewrite a live in-game line (e.g. a 4.5 K line jumping to
            # 5.5/6.5 mid-game). Games not yet started still take fresh lines so
            # late-named starters appear. CLV opener below stays on the true run.
            _snap = _freeze_started_picks(date_str, result)
            task["status"] = "done"
            task["result"] = _snap
            # Always persist so the read-only /api/results endpoint (parlay hub)
            # can serve the slate even when a starter is still TBD. The MLB app's
            # own load re-runs when has_tbd to pick up late-named starters.
            _cache[date_str] = _snap
            try: _update_track_ledger()
            except Exception as _le: print(f"[track_ledger] {_le}")
            _save_disk_cache(date_str, _snap)
            _save_sb_picks(date_str, _snap)
            _save_open_snapshot(date_str, result)
            try:
                # Bake the picks into the page HTML so the Replit hub can serve
                # an instant, no-cold-start snapshot at moneypicksarena.com.
                baked = {**_snap, "date": date_str}
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

# ── Rotation order override (admin-editable, persisted in Supabase) ──────
# The card rotation-rank dot defaults to an automatic ranking; admins pin a
# team's true SP1..SPn order here. Stored as one special mpa_track_ledger row
# (app=mlb, date=__rotation__, category=__rotation__, side=ALL) with
# detail = { "<team_id>": [[pid, "Name"], ...] }. The save MERGES per team, so
# editing today's slate never wipes an override on a team that plays another day.
_ROT_OVR_CAT = "__rotation__"

def _load_rot_override_doc() -> dict:
    rows = _sb_get("mpa_track_ledger", {"app": "eq.mlb",
                   "category": f"eq.{_ROT_OVR_CAT}", "side": "eq.ALL",
                   "date": f"eq.{_ROT_OVR_CAT}", "select": "detail", "limit": "1"})
    if rows:
        return rows[0].get("detail") or {}
    return {}

@app.get("/api/rotation")
async def get_rotation(request: Request, date_str: str = "",
                       token: str = "", admin: str = ""):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _bet_admin_ok(tok, admin):
        raise HTTPException(status_code=403, detail="admin only")
    ds = date_str or date.today().isoformat()
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from pipeline import rotation_editor_data
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, rotation_editor_data, ds)
    return data

@app.get("/api/rotation/search")
async def rotation_search(request: Request, q: str = "",
                          token: str = "", admin: str = ""):
    # Admin pitcher lookup for the rotation editor — lets an admin hand-add a
    # starter the auto-detector cannot see yet (e.g. a just-promoted arm whose
    # next start has not been posted as an official probable). Official MLB Stats
    # API people search only; no scraping.
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _bet_admin_ok(tok, admin):
        raise HTTPException(status_code=403, detail="admin only")
    q = (q or "").strip()
    if len(q) < 3:
        return {"players": []}

    def _search():
        import requests as _rq
        import urllib.parse as _up
        try:
            j = _rq.get("https://statsapi.mlb.com/api/v1/people/search?names="
                        + _up.quote(q), timeout=12).json()
        except Exception:
            return []
        out = []
        for p in j.get("people", []):
            pos = ((p.get("primaryPosition") or {}).get("abbreviation") or "")
            if pos not in ("P", "SP", "RP"):
                continue
            out.append({"id": p.get("id"),
                        "name": p.get("fullName", ""),
                        "team": ((p.get("currentTeam") or {}).get("name") or "")})
            if len(out) >= 12:
                break
        return out
    loop = asyncio.get_event_loop()
    players = await loop.run_in_executor(executor, _search)
    return {"players": players}

@app.post("/api/rotation")
async def save_rotation(request: Request, token: str = "", admin: str = ""):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    is_admin = _is_admin_token(tok) or (
        bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__"))
    if not is_admin:
        raise HTTPException(status_code=403, detail="admin only")
    body = await request.json()
    doc = _load_rot_override_doc()

    def _pairs(lst):
        out = []
        for it in (lst or []):
            if isinstance(it, dict):
                pid = it.get("id"); nm = it.get("name", "")
            elif isinstance(it, (list, tuple)) and it:
                pid = it[0]; nm = it[1] if len(it) > 1 else ""
            else:
                pid = None; nm = ""
            if pid is None:
                continue
            out.append([int(pid), str(nm)])
        return out

    for tid, val in (body.get("set") or {}).items():
        tier = {}
        if isinstance(val, dict):
            order = _pairs(val.get("order"))
            inj = _pairs(val.get("inj"))
            for _pid, _n in (val.get("tier") or {}).items():
                try:
                    _ni = int(_n)
                except Exception:
                    continue
                if _ni in (1, 2, 3):
                    tier[str(int(_pid))] = _ni
        else:
            order = _pairs(val); inj = []
        if order or inj or tier:
            doc[str(tid)] = {"order": order, "inj": inj, "tier": tier}
        else:
            doc.pop(str(tid), None)
    for tid in (body.get("reset") or []):
        doc.pop(str(tid), None)
    import datetime as _dt
    row = {"app": "mlb", "date": _ROT_OVR_CAT, "category": _ROT_OVR_CAT,
           "side": "ALL", "wins": 0, "losses": 0, "locked": False,
           "locked_at": _dt.datetime.utcnow().isoformat() + "Z", "detail": doc}
    ok = _sb_upsert("mpa_track_ledger", [row], on_conflict="app,date,category,side")
    try:
        import sys as _sys
        _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import pipeline as _pl
        _pl._ROT_RANK_CACHE.clear()
        _pl._ROT_EDITOR_CACHE.clear()
    except Exception as _e:
        print(f"[rot] cache clear failed: {_e}")
    return {"ok": bool(ok), "teams": len(doc)}

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
        disk = _load_pick_cache(date_str)
        if disk is not None:
            _cache[date_str] = disk
    if date_str in _cache:
        return _cache[date_str]
    raise HTTPException(status_code=404, detail="No results for this date.")


def _norm_name(s) -> str:
    """Normalize a player name for matching: strip accents, lowercase, drop
    periods, collapse whitespace. Box scores spell names with accents while
    odds-sourced bets store them plain; without this the grader can't match
    and falsely VOIDs a bet whose player actually played."""
    if not s:
        return ""
    s = _ud.normalize("NFKD", str(s))
    s = "".join(c for c in s if not _ud.combining(c))
    return " ".join(s.lower().replace(".", " ").split())


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
            _sl    = status.lower()
            final  = status in ("Final", "Game Over")
            # A cancelled/postponed game produces no stats today, so it must NOT
            # keep the slate "not final" forever — that would strand every player
            # in it on "pending" and block the date from ever locking. Treat it
            # as resolved; its players then VOID (no action) below.
            dead   = ("cancel" in _sl) or ("postpone" in _sl)
            if not final and not dead:
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
                _hrr = ((bat.get("hits") or 0) + (bat.get("runs") or 0) + (bat.get("rbi") or 0)) if bat else None
                _tb = bat.get("totalBases") if bat else None
                if bat and _tb is None and bat.get("hits") is not None:
                    _h = bat.get("hits") or 0; _2b = bat.get("doubles") or 0
                    _3b = bat.get("triples") or 0; _hr = bat.get("homeRuns") or 0
                    _tb = (_h - _2b - _3b - _hr) + 2 * _2b + 3 * _3b + 4 * _hr
                rows.append((int(pid), full_name, {
                    "hits":         bat.get("hits"),
                    "runs":         bat.get("runs"),
                    "total_bases":  _tb,
                    "rbi":          bat.get("rbi"),
                    "walks_bat":    bat.get("baseOnBalls"),
                    "homeRuns":     bat.get("homeRuns"),
                    "hrr":          _hrr,
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
                    name_stats[_norm_name(full_name)] = entry
        if not fetch_complete:
            all_final = False            # defer locking until a clean pass grades it
    return player_stats, name_stats, any_game, all_final


# ── Top 10 Batter selection — MUST mirror the live page's _buildTop10All ────
# The "Top 10 Hitter Plays" cards on the page are built client-side by
# _buildTop10All: ranked by Wilson-EV (_t10Score), green/amber only (ace-faced
# batters dropped), each pick keyed to its SIDE's posted odds, and ONE pick per
# team (the best by Wilson-EV). We replicate ALL of that here so the Track
# Record's Top 10 == the cards the user actually saw.
def _t10_dec(o):
    if not o:
        return None
    try:
        o = float(o)
    except Exception:
        return None
    return 1 + o / 100.0 if o > 0 else 1 + 100.0 / abs(o)

def _t10_odds_for(p, kind):
    pk = p.get("pick")
    if kind == "TB OVER":
        return p.get("tb_over_odds")
    if kind == "HRR":
        return p.get("hrr_under_odds") if pk == "UNDER" else p.get("hrr_over_odds")
    if kind == "RBI":
        return p.get("over_odds") if pk == "OVER" else p.get("under_odds")
    if kind in ("RUNS", "BWALK"):
        return p.get("over_odds") if pk == "OVER" else p.get("under_odds")
    return None

def _t10_score(p, kind):
    w = p.get("wilson") or 0
    dec = _t10_dec(_t10_odds_for(p, kind))
    if not dec:
        return -999.0
    return w * (dec - 1) - (1 - w)

def _t10_batter_red(p):
    """Mirror the client _t10DotIsRed(p,'O',false,0): a batter is RED (and so
    excluded from the Top 10) iff the opposing starter is a tier-1 ace."""
    rank = p.get("opp_rot_rank")
    rookie = p.get("opp_rot_rookie")
    tovr = p.get("opp_rot_tier")
    if (rank is None or rank == 0) and not rookie and not (tovr and tovr > 0):
        return False
    if tovr and tovr > 0:
        tier = tovr
    elif rank is not None and rank > 0:
        tier = 1 if rank <= 2 else (2 if rank <= 4 else 3)
    elif rookie:
        tier = 3
    else:
        return False
    return tier == 1


def _t10_rank(cands, drop_red=True):
    """Rank hitter candidates the way the live _buildTop10All does: sort by
    Wilson-EV (highest edge first), dedup by name, drop ace-faced (RED) plays,
    then one pick per team. Top 10 = [:10], overflow = [10:20].
    drop_red=False keeps ace-faced plays — the NEW challenger list passes this so
    it mirrors the approved example (which surfaced Joe Mack / Goldschmidt etc.)."""
    cs = sorted(cands, key=lambda x: -(x.get("_t10sc") or -999.0))
    _seen = set(); _dd = []
    for c in cs:
        k = (c.get("name") or "").strip().lower()
        if k in _seen:
            continue
        _seen.add(k); _dd.append(c)
    if drop_red:
        _dd = [c for c in _dd if not c.get("_red")]
    _tseen = set(); _out = []
    for c in _dd:
        t = (c.get("team") or "").strip().upper()
        if t and t in _tseen:
            continue
        if t:
            _tseen.add(t)
        _out.append(c)
    return _out


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
            return name_stats.get(_norm_name(fallback_name))
        return None

    def _grade(pick_dir, line, actual, final):
        # VOID = no action: game is Final (or whole slate Final & cleanly fetched)
        # but the player never recorded a stat -> DNP / scratched / didn't bat.
        # Refunded: excluded from W/L and ROI, never stuck "pending" forever.
        if final:
            if actual is None:
                return "VOID"
            if pick_dir == "OVER":
                return "WIN" if actual > float(line) else "LOSS"
            return "WIN" if actual < float(line) else "LOSS"
        if actual is None and all_final:
            return "VOID"
        return "pending"

    # Hitter OVERs — top 10 (top9 list)
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
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
        })

    # Overflow / also_ran hits (positions 11-20) — tracked as "Hitter Hits (More)"
    hitter_more = []
    for p in (picks.get("also_ran") or [])[:10]:
        st = _lookup(p.get("player_id"), p.get("full_name") or p.get("name"))
        actual = st["hits"] if st else None
        hitter_more.append({
            "name": p.get("full_name") or p.get("name", ""),
            "team": p.get("team", ""),
            "category": "Hitter Hits (More)", "side": "OVER",
            "pick": "OVER 0.5 Hits",
            "odds": p.get("hit_odds"),
            "line": 0.5,
            "actual": actual,
            "stat": "Hits",
            "result": _grade("OVER", 0.5, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
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
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
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
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
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
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
        })

    # TB Over 1.5 — top 10 for Track Record
    tb_over = []
    for p in (picks.get("tb_over_picks") or [])[:10]:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["total_bases"] if st else None
        tb_over.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "TB Over", "side": "OVER",
            "pick": "OVER 1.5 Total Bases",
            "odds": p.get("tb_over_odds"),
            "line": 1.5,
            "actual": actual,
            "stat": "Total Bases",
            "result": _grade("OVER", 1.5, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
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
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
        })

    # HR OVER/UNDER — top 10 per side for Track Record
    _hr_all = picks.get("hr_picks") or []
    _hr_capped = [p for p in _hr_all if p.get("pick") == "OVER"][:10] + \
                 [p for p in _hr_all if p.get("pick") == "UNDER"][:10]
    hr_picks = []
    for p in _hr_capped:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st.get("homeRuns") if st else None
        pick_dir = p.get("pick", "OVER")
        line = p.get("line") if p.get("line") is not None else 0.5
        hr_picks.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "HR", "side": pick_dir,
            "pick": f"{pick_dir} {line} HR",
            "odds": p.get("over_odds") if pick_dir == "OVER" else p.get("under_odds"),
            "line": line,
            "actual": actual,
            "stat": "HR",
            "result": _grade(pick_dir, line, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
        })

    # Batter Walks OVER/UNDER 0.5 — top 10 per side for Track Record
    _walks_all = picks.get("walks_picks") or []
    _walks_capped = [p for p in _walks_all if p.get("pick") == "OVER"][:10] + \
                    [p for p in _walks_all if p.get("pick") == "UNDER"][:10]
    walks_rows = []
    for p in _walks_capped:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["walks_bat"] if st else None
        pick_dir = p.get("pick", "OVER")
        line = p.get("line") if p.get("line") is not None else 0.5
        walks_rows.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "Batter Walks", "side": pick_dir,
            "pick": f"{pick_dir} {line} Walks",
            "odds": p.get("over_odds") if pick_dir == "OVER" else p.get("under_odds"),
            "line": line,
            "actual": actual,
            "stat": "Walks",
            "result": _grade(pick_dir, line, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
        })

    # HRR (Hits+Runs+RBI) OVER/UNDER 1.5 — top 10 per side for Track Record
    _hrr_all = picks.get("hrr_picks") or []
    _hrr_capped = [p for p in _hrr_all if p.get("pick") == "OVER"][:10] + \
                  [p for p in _hrr_all if p.get("pick") == "UNDER"][:10]
    hrr_rows = []
    for p in _hrr_capped:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["hrr"] if st else None
        pick_dir = p.get("pick", "OVER")
        hrr_rows.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "HRR", "side": pick_dir,
            "pick": f"{pick_dir} 1.5 H+R+RBI",
            "odds": p.get("hrr_over_odds") if pick_dir == "OVER" else p.get("hrr_under_odds"),
            "line": 1.5,
            "actual": actual,
            "stat": "H+R+RBI",
            "result": _grade(pick_dir, 1.5, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
        })

    # HRR Special (Parlay Confluence) — OVER only, top 20 for its own record
    # Deliberately kept OUT of main Track Record (_TRK_KEYS) to avoid double-
    # counting with the regular HRR overs. Has its own button + modal.
    hrr_special_rows = []
    for p in (picks.get("hrr_special_picks") or [])[:20]:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["hrr"] if st else None
        hrr_special_rows.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "HRR Special", "side": "OVER",
            "pick": "OVER 1.5 H+R+RBI (Special)",
            "odds": p.get("hrr_over_odds"),
            "line": 1.5,
            "actual": actual,
            "stat": "H+R+RBI",
            "result": _grade("OVER", 1.5, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
        })

    # Triple Split Club — "to record a hit", top 20 for its own forward-only
    # record. Kept OUT of main Track Record (_TRK_KEYS) to avoid double-counting
    # with the regular Hits board (same market). Has its own button + modal.
    triple_split_rows = []
    for p in (picks.get("triple_split_picks") or [])[:20]:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["hits"] if st else None
        triple_split_rows.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "Triple Split Club", "side": "OVER",
            "pick": "OVER 0.5 Hits (Triple Split)",
            "odds": p.get("hit_odds"),
            "line": 0.5,
            "actual": actual,
            "stat": "Hits",
            "result": _grade("OVER", 0.5, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
        })

    # 5 Star Split — Triple Split qualifiers that ALSO clear vs-team >=60% and
    # last-10 >=60%, each carrying its single best production play (TB/Runs/RBI/
    # HRR OVER). Own forward-only record (own button + modal); kept OUT of the
    # main Track Record (_TRK_KEYS) so it never double-counts with the per-market
    # boards. Career-vs-pitcher line rides along as display-only reference.
    _FSS_BOX = {"tb": "total_bases", "runs": "runs", "rbi": "rbi", "hrr": "hrr"}
    five_star_split_rows = []
    for p in (picks.get("five_star_split_picks") or [])[:20]:
        st = _lookup(p.get("batter_id"), p.get("name"))
        _mk = p.get("pick_market", "tb")
        _bf = _FSS_BOX.get(_mk, "total_bases")
        actual = st[_bf] if (st and _bf in st) else None
        _line = p.get("line") if p.get("line") is not None else 1.5
        _slabel = p.get("stat_label", "Total Bases")
        five_star_split_rows.append({
            "name": p.get("name", ""),
            "team": p.get("team", ""),
            "category": "5 Star Split", "side": "OVER",
            "pick": f"OVER {_line} {_slabel}",
            "odds": p.get("odds"),
            "line": _line,
            "actual": actual,
            "stat": _slabel,
            "result": _grade("OVER", _line, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
        })

    # Pitcher Ks — top 10 PER SIDE for Track Record (Over and Under each get
    # their own top 10 so a side with <10 picks never spills into overflow)
    pitcher_ks = []
    _pk_all = (picks.get("pitcher_k") or {}).get("picks") or []
    _pk_capped = [q for q in _pk_all if q.get("pick") == "OVER"][:10] + \
                 [q for q in _pk_all if q.get("pick") == "UNDER"][:10]
    for p in _pk_capped:
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
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
            "proj": (p.get("proj_k") if p.get("proj_k") is not None else p.get("blended_avg_k")),
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
        # top 10 PER SIDE — Over and Under each get their own top 10
        _pp_all = mdata.get("picks") or []
        _pp_capped = [q for q in _pp_all if q.get("pick") == "OVER"][:10] + \
                     [q for q in _pp_all if q.get("pick") == "UNDER"][:10]
        for p in _pp_capped:
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
                "ev": p.get("ev"),
                "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
                "proj": (p.get("proj") if p.get("proj") is not None else p.get("blended")),
            })

    # ── Overflow (ranks 11-30 per side) — every pick BEYOND each category's top
    #    10, graded & banked permanently for the Overflow Tracker tab (kept OUT
    #    of the main daily Track Record). Hits overflow keeps the legacy
    #    "Hitter Hits (More)" label; every other category gets a " (OVF)" suffix
    #    so it never mixes with its own top-10 record.
    overflow = []
    def _ovf(p, name, team, cat, side, pick, odds, line, actual, stat, st):
        overflow.append({
            "name": name or "", "team": team or "",
            "category": cat, "side": side, "pick": pick,
            "odds": odds, "line": line, "actual": actual, "stat": stat,
            "result": _grade(side, line, actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
            "ev": p.get("ev"),
            "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
            "edge": p.get("edge"),
        })
    for p in (picks.get("under_picks") or [])[10:20]:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["hits"] if st else None
        pd = p.get("pick", "UNDER")
        _ovf(p, p.get("name", ""), p.get("team", ""), "Hitter Hits (More)", pd,
             f"{pd} 1.5 Hits", p.get("under_odds") if pd == "UNDER" else p.get("over_odds"),
             1.5, actual, "Hits", st)
    for p in ([q for q in _runs_all if q.get("pick") == "OVER"][10:20] +
              [q for q in _runs_all if q.get("pick") == "UNDER"][10:20]):
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["runs"] if st else None
        pd = p.get("pick", "OVER")
        _ovf(p, p.get("name", ""), p.get("team", ""), "Runs (OVF)", pd,
             f"{pd} 0.5 Runs", p.get("over_odds") if pd == "OVER" else p.get("under_odds"),
             0.5, actual, "Runs", st)
    for p in (picks.get("tb_picks") or [])[10:20]:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["total_bases"] if st else None
        _ovf(p, p.get("name", ""), p.get("team", ""), "TB Under (OVF)", "UNDER",
             "UNDER 1.5 Total Bases", p.get("tb_under_odds"), 1.5, actual, "Total Bases", st)
    for p in (picks.get("tb_over_picks") or [])[10:20]:
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["total_bases"] if st else None
        _ovf(p, p.get("name", ""), p.get("team", ""), "TB Over (OVF)", "OVER",
             "OVER 1.5 Total Bases", p.get("tb_over_odds"), 1.5, actual, "Total Bases", st)
    for p in ([q for q in _rbi_all if q.get("pick") == "OVER"][10:20] +
              [q for q in _rbi_all if q.get("pick") == "UNDER"][10:30]):
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["rbi"] if st else None
        pd = p.get("pick", "OVER"); ln = p.get("line") if p.get("line") is not None else 0.5
        _ovf(p, p.get("name", ""), p.get("team", ""), "RBI (OVF)", pd,
             f"{pd} {ln} RBI", p.get("over_odds") if pd == "OVER" else p.get("under_odds"),
             ln, actual, "RBI", st)
    for p in ([q for q in _hr_all if q.get("pick") == "OVER"][10:20] +
              [q for q in _hr_all if q.get("pick") == "UNDER"][10:20]):
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st.get("homeRuns") if st else None
        pd = p.get("pick", "OVER"); ln = p.get("line") if p.get("line") is not None else 0.5
        _ovf(p, p.get("name", ""), p.get("team", ""), "HR (OVF)", pd,
             f"{pd} {ln} HR", p.get("over_odds") if pd == "OVER" else p.get("under_odds"),
             ln, actual, "HR", st)
    for p in ([q for q in _walks_all if q.get("pick") == "OVER"][10:20] +
              [q for q in _walks_all if q.get("pick") == "UNDER"][10:20]):
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["walks_bat"] if st else None
        pd = p.get("pick", "OVER"); ln = p.get("line") if p.get("line") is not None else 0.5
        _ovf(p, p.get("name", ""), p.get("team", ""), "Batter Walks (OVF)", pd,
             f"{pd} {ln} Walks", p.get("over_odds") if pd == "OVER" else p.get("under_odds"),
             ln, actual, "Walks", st)
    for p in ([q for q in _hrr_all if q.get("pick") == "OVER"][10:20] +
              [q for q in _hrr_all if q.get("pick") == "UNDER"][10:20]):
        st = _lookup(p.get("batter_id"), p.get("name"))
        actual = st["hrr"] if st else None
        pd = p.get("pick", "OVER")
        _ovf(p, p.get("name", ""), p.get("team", ""), "HRR (OVF)", pd,
             f"{pd} 1.5 H+R+RBI", p.get("hrr_over_odds") if pd == "OVER" else p.get("hrr_under_odds"),
             1.5, actual, "H+R+RBI", st)
    for p in ([q for q in _pk_all if q.get("pick") == "OVER"][10:20] +
              [q for q in _pk_all if q.get("pick") == "UNDER"][10:20]):
        if not p.get("pick"):
            continue
        st = _lookup(None, p.get("name"))
        actual = st["strikeOuts"] if st else None
        ln = p.get("sugg_line") if p.get("sugg_line") is not None else p.get("line")
        if ln is None:
            continue
        pd = p.get("pick")
        _ovf(p, p.get("name", ""), p.get("team", ""), "Pitcher Ks (OVF)", pd,
             f"{pd} {ln} Ks", p.get("over_odds") if pd == "OVER" else p.get("under_odds"),
             ln, actual, "Ks", st)
    for mkt, mdata in (picks.get("pitcher_props") or {}).items():
        stat_key, stat_label = PROP_STAT_MAP.get(mkt, (None, None))
        if not stat_key:
            continue
        _ppo_all = mdata.get("picks") or []
        for p in ([q for q in _ppo_all if q.get("pick") == "OVER"][10:20] +
                  [q for q in _ppo_all if q.get("pick") == "UNDER"][10:20]):
            if not p.get("pick") or p.get("line") is None:
                continue
            st = _lookup(None, p.get("name"))
            actual = st[stat_key] if (st and stat_key) else None
            pd = p.get("pick"); ln = p.get("line")
            _ovf(p, p.get("name", ""), p.get("team", ""), f"Pitcher {stat_label} (OVF)", pd,
                 f"{pd} {ln} {stat_label}", p.get("over_odds") if pd == "OVER" else p.get("under_odds"),
                 ln, actual, stat_label, st)

    # Top 10 Batter — MIRROR the live page's _buildTop10All EXACTLY so this list
    # equals the "Top 10 Hitter Plays" cards the user actually sees:
    #   • same categories + insertion order: TB Over, HRR, RBI, Runs, Walks
    #     (NO single Hits, NO Under-1.5-Hits, NO HR — those have their own lists)
    #   • ranked by Wilson-EV (_t10_score), NOT raw pipeline ev
    #   • each pick keyed to its SIDE's posted odds; drop if no price
    #   • green/amber only: batters facing a tier-1 ace are filtered out
    #   • ONE pick per team — best by Wilson-EV (mirrors the page)
    _T10_SPECS = [
        ("tb_over_picks", "TB OVER", "total_bases", "Total Bases", 1.5),
        ("hrr_picks",     "HRR",     "hrr",         "H+R+RBI",     1.5),
        ("rbi_picks",     "RBI",     "rbi",         "RBI",         0.5),
        ("runs_picks",    "RUNS",    "runs",        "Runs",        0.5),
        ("walks_picks",   "BWALK",   "walks_bat",   "Walks",       0.5),
    ]
    _bat_cands = []
    for _src, _kind, _sk, _sl, _dln in _T10_SPECS:
        for p in (picks.get(_src) or []):
            _od = _t10_odds_for(p, _kind)
            if _od is None:
                continue
            _sc = _t10_score(p, _kind)
            if _sc <= -999:
                continue
            _side = "OVER" if _kind == "TB OVER" else (p.get("pick") or "OVER")
            _ln = p.get("line") if (_kind in ("RBI", "BWALK") and p.get("line") is not None) else _dln
            _bat_cands.append({"name":p.get("name",""),"team":p.get("team",""),
                "side":_side,"stat_key":_sk,"stat_label":_sl,"line":_ln,
                "odds":_od,"ev":p.get("ev"),"ev_prob":p.get("ev_prob"),
                "matchup_prob":p.get("matchup_prob"),"edge":p.get("edge"),
                "hot":p.get("hot_bonus"),
                "pid":p.get("batter_id"),"fname":p.get("name"),
                "_t10sc":_sc,"_red":_t10_batter_red(p)})
    # Snapshot the raw candidates BEFORE ranking so the NEW challenger rule can
    # re-rank its own filtered subset (A/B test — see below).
    _bat_raw = list(_bat_cands)
    # Current rule: rank by Wilson-EV, dedup by name, drop ace-faced, one per team.
    _bat_cands = _t10_rank(_bat_cands)
    top10_batter = []
    for c in _bat_cands[:10]:
        st = _lookup(c.get("pid"), c.get("fname") or c["name"])
        actual = st[c["stat_key"]] if (st and c["stat_key"] in st) else None
        top10_batter.append({
            "name": c["name"], "team": c["team"],
            "category": "Top 10 Batter", "side": c["side"],
            "pick": f"{c['side']} {c['line']} {c['stat_label']}",
            "odds": c["odds"], "line": c["line"], "actual": actual, "stat": c["stat_label"],
            "result": _grade(c["side"], c["line"], actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
            "ev": c.get("ev"),
            "ev_prob": (c.get("ev_prob") if c.get("ev_prob") is not None else c.get("matchup_prob")),
            "edge": c.get("edge"),
        })

    # Top 10 Batter overflow (ranks 11-20 of the same combined EV ranking) → banked
    # in the Overflow Tracker as "Top 10 Batter (OVF)". Mirrors the live "More Hitter
    # Plays" pulldown. Like the main Top 10 Batter, it intentionally overlaps the
    # per-category rows (curated best-of-the-rest), so it's excluded from cross-cat sums.
    for c in _bat_cands[10:20]:
        st = _lookup(c.get("pid"), c.get("fname") or c["name"])
        actual = st[c["stat_key"]] if (st and c["stat_key"] in st) else None
        _ovf(c, c.get("name", ""), c.get("team", ""), "Top 10 Batter (OVF)", c["side"],
             f"{c['side']} {c['line']} {c['stat_label']}", c.get("odds"),
             c.get("line"), actual, c.get("stat_label"), st)

    # Top 10 Pitcher — combine Ks + all pitcher props, rank by EV (mirrors the live
    # _buildPitchDay card builder), take top 10
    _pit_cands = []
    for p in ((picks.get("pitcher_k") or {}).get("picks") or []):
        if not p.get("pick"): continue
        ln = p.get("sugg_line") if p.get("sugg_line") is not None else p.get("line")
        if ln is None: continue
        pd = p.get("pick")
        bl = p.get("proj") or p.get("blended")
        gap = abs(bl - ln) if (bl is not None) else 0
        _pit_cands.append({"name":p.get("name",""),"team":p.get("team",""),
            "side":pd,"stat_key":"strikeOuts","stat_label":"Ks","line":ln,
            "odds":p.get("over_odds") if pd=="OVER" else p.get("under_odds"),"gap":gap,"ev":p.get("ev") or 0,"edge":p.get("edge")})
    for mkt, mdata in (picks.get("pitcher_props") or {}).items():
        sk, sl = PROP_STAT_MAP.get(mkt, (None, None))
        if not sk: continue
        for p in (mdata.get("picks") or []):
            if not p.get("pick") or p.get("line") is None: continue
            pd = p.get("pick"); ln = p.get("line"); bl = p.get("blended")
            gap = abs(bl - ln) if (bl is not None) else 0
            _pit_cands.append({"name":p.get("name",""),"team":p.get("team",""),
                "side":pd,"stat_key":sk,"stat_label":sl,"line":ln,
                "odds":p.get("over_odds") if pd=="OVER" else p.get("under_odds"),"gap":gap,"ev":p.get("ev") or 0,"edge":p.get("edge")})
    _pit_cands.sort(key=lambda x: -(x.get("ev") or 0))
    _pit_seen = set()
    _pit_dedup = []
    for _pc in _pit_cands:
        _pk = (_pc.get("name") or "").strip().lower()
        if _pk in _pit_seen: continue
        _pit_seen.add(_pk); _pit_dedup.append(_pc)
    _pit_cands = _pit_dedup
    top10_pitcher = []
    for c in _pit_cands[:10]:
        st = _lookup(None, c["name"])
        actual = st[c["stat_key"]] if (st and c["stat_key"] in st) else None
        top10_pitcher.append({
            "name": c["name"], "team": c["team"],
            "category": "Top 10 Pitcher", "side": c["side"],
            "pick": f"{c['side']} {c['line']} {c['stat_label']}",
            "odds": c["odds"], "line": c["line"], "actual": actual, "stat": c["stat_label"],
            "result": _grade(c["side"], c["line"], actual, (st or {}).get("final", False)),
            "game_status": (st or {}).get("status", "—"),
            "ev": c.get("ev"),
            "ev_prob": (c.get("ev_prob") if c.get("ev_prob") is not None else c.get("matchup_prob")),
            "edge": c.get("edge"),
        })

    # ── Value Plays — server mirror of the live "Top 10 Value Plays of the Day"
    #    board (_buildValuePlays). Per hitter, collect every plus-money (+odds)
    #    OVER value market (RBI / TB Over / Runs / Walks / HRR), rank players by
    #    the same 3-standard composite (geo-mean of HOT recent form, career vs
    #    PITCHER, rate vs opponent TEAM), take the top 10, and bank EACH surfaced
    #    +odds leg so the board earns its own forward record. Curated duplicate of
    #    the native categories (every leg also lives under RBI/TB/Runs/Walks/HRR),
    #    so it is kept OUT of the grand total (meta), exactly like Top 10 Batter.
    def _vp_num(s):
        s = str(s or ""); i = s.find("/")
        if i < 0:
            return (None, None)
        try:
            a = int(s[:i])
        except Exception:
            a = None
        try:
            b = int(s[i + 1:])
        except Exception:
            b = None
        return (a, b)

    def _vp_rate(s):
        a, b = _vp_num(s)
        return (a / b) if (b and b > 0 and a is not None) else None

    def _vp_ba(disp):
        disp = str(disp or ""); i = disp.find(".")
        if i < 0:
            return None
        dd = ""
        for ch in disp[i + 1:]:
            if ch.isdigit():
                dd += ch
            else:
                break
        return float("0." + dd[:3]) if dd else None

    _VP_MK = [
        ("1+ RBI",         "rbi_picks",     "over_odds",     "rbi",         "RBI",         0.5),
        ("2+ Total Bases", "tb_over_picks", "tb_over_odds",  "total_bases", "Total Bases", 1.5),
        ("1+ Run",         "runs_picks",    "over_odds",     "runs",        "Runs",        0.5),
        ("1+ Walk",        "walks_picks",   "over_odds",     "walks_bat",   "Walks",       0.5),
        ("2+ H+R+RBI",     "hrr_picks",     "hrr_over_odds", "hrr",         "H+R+RBI",     1.5),
    ]
    _vp_by_pid: dict = {}
    for _lbl, _src, _ofld, _sk, _slabel, _ln in _VP_MK:
        for p in (picks.get(_src) or []):
            o = p.get(_ofld)
            if o is None or float(o) <= 0:        # plus-money only
                continue
            pid = p.get("batter_id")
            if pid is None:
                continue
            e = _vp_by_pid.get(pid)
            if e is None:
                e = _vp_by_pid[pid] = {"plays": {}, "stat": None}
            vp = p.get("vs_pit") or {}
            if (not e["stat"]) or (vp.get("ab") and not ((e["stat"].get("vs_pit") or {}).get("ab"))):
                e["stat"] = p                       # keep richest record
            if (_lbl not in e["plays"]) or (float(o) < e["plays"][_lbl][0]):
                e["plays"][_lbl] = (float(o), _sk, _slabel, _ln, p)   # one per market, safest +odds
    _vp_out = []
    for pid, e in _vp_by_pid.items():
        s = e["stat"] or {}
        l10 = _vp_rate(s.get("recent_l10")); l5 = _vp_rate(s.get("recent_l5"))
        if l5 is None:
            l5 = l10
        streak = s.get("hot_bonus") or 0
        hot = (100 * (0.55 * (l10 or 0) + 0.30 * (l5 or 0) + 0.15 * min(streak / 13.0, 1))) if (l10 is not None) else None
        vp = s.get("vs_pit") or {}; vpab = vp.get("ab") or 0; ba = _vp_ba(vp.get("display")); vsP = None
        if vpab and ba is not None:
            shr = (ba * vpab + 0.25 * 5) / (vpab + 5)
            vsP = max(0.0, min(100.0, (shr - 0.15) / 0.30 * 100))
        hh = _vp_num(s.get("h2h_disp")); vsT = None
        if hh[1]:
            vsT = max(0.0, min(100.0, 100 * (hh[0] + 0.6 * 2) / (hh[1] + 2)))
        avail = [v for v in (hot, vsP, vsT) if v is not None]
        if not avail:
            continue
        prod = 1.0
        for v in avail:
            prod *= max(v, 1)
        comp = prod ** (1.0 / len(avail))
        _vp_out.append({"pid": pid, "stat": s, "plays": e["plays"], "comp": comp})
    _vp_out.sort(key=lambda x: x["comp"], reverse=True)
    value_plays = []
    for e in _vp_out[:10]:
        s = e["stat"] or {}
        nm = s.get("name") or s.get("full_name") or ""
        tm = s.get("team") or ""
        for _lbl, (o, _sk, _slabel, _ln, p) in e["plays"].items():
            st = _lookup(e.get("pid"), nm)
            actual = st.get(_sk) if (st and _sk in st) else None
            value_plays.append({
                "name": nm, "team": tm,
                "category": "Value Plays", "side": "OVER",
                "pick": f"OVER {_ln} {_slabel}",
                "odds": o, "line": _ln, "actual": actual, "stat": _slabel,
                "result": _grade("OVER", _ln, actual, (st or {}).get("final", False)),
                "game_status": (st or {}).get("status", "—"),
                "ev": p.get("ev"),
                "ev_prob": (p.get("ev_prob") if p.get("ev_prob") is not None else p.get("matchup_prob")),
                "edge": p.get("edge"),
            })

    result = {
        "date": date_str,
        "hitter_overs":  hitter_overs,
        "hitter_more":   hitter_more,
        "hitter_unders": hitter_unders,
        "runs":          runs,
        "tb_under":      tb_under,
        "tb_over":       tb_over,
        "rbi":           rbi_picks,
        "hr":            hr_picks,
        "batter_walks":  walks_rows,
        "hrr":           hrr_rows,
        "hrr_special":   hrr_special_rows,
        "triple_split":  triple_split_rows,
        "five_star_split": five_star_split_rows,
        "pitcher_ks":    pitcher_ks,
        "pitcher_props": pitcher_props,
        "top10_batter":  top10_batter,
        "top10_pitcher": top10_pitcher,
        "overflow":      overflow,
        "value_plays":   value_plays,
        "any_game":      any_game,
        "all_final":     all_final,
    }
    # Matrix Scorecard: stamp each graded row with its series position (G1/G2/G3)
    # so the SERIES half of the strategy chart can be scored going forward. Past
    # locked days carry no series_pos (the day-of-week half still scores there).
    _pos_by_name: dict = {}
    def _harvest_pos(o):
        if isinstance(o, dict):
            nm = (o.get("name") or o.get("full_name") or "").strip().lower()
            ss = o.get("series_splits")
            if nm and isinstance(ss, dict) and ss.get("today_pos"):
                _pos_by_name.setdefault(nm, ss.get("today_pos"))
            for v in o.values():
                _harvest_pos(v)
        elif isinstance(o, list):
            for v in o:
                _harvest_pos(v)
    _harvest_pos(picks)
    for _key in ("hitter_overs", "hitter_more", "hitter_unders", "runs", "tb_under", "tb_over",
                 "rbi", "hr", "batter_walks", "hrr", "pitcher_ks", "pitcher_props",
                 "top10_batter", "top10_pitcher", "overflow", "value_plays"):
        for _r in (result.get(_key) or []):
            if isinstance(_r, dict):
                _r["series_pos"] = _pos_by_name.get(
                    (_r.get("name") or _r.get("full_name") or "").strip().lower())
    return result


# ── Track Record: permanent W/L ledger across all graded days ────────────
_TRACK_LEDGER_PATH = os.path.join(_CACHE_DIR, "_track_record.json")
_TRACK_CAT_ORDER = [
    "Top 10 Batter", "Top 10 Pitcher", "Value Plays",
    "Hitter Hits", "Hitter Hits (More)", "Runs", "TB Under", "TB Over", "RBI", "HR", "Batter Walks", "HRR", "Pitcher Ks",
    "Pitcher Hits Allowed", "Pitcher Outs", "Pitcher Earned Runs", "Pitcher Walks",
]

def _is_ovf_cat(cat) -> bool:
    """Overflow categories (ranks 11-30) — banked permanently but kept OUT of the
    main Track Record; they surface only in the separate Overflow Tracker tab.
    Hits overflow keeps the legacy 'Hitter Hits (More)' label; every other
    category carries a ' (OVF)' suffix."""
    return cat == "Hitter Hits (More)" or (isinstance(cat, str) and cat.endswith(" (OVF)"))

def _is_hr_cat(cat) -> bool:
    """HR categories (home-run Over/Under, main + overflow) — banked permanently
    but kept OUT of BOTH the main Track Record and the Overflow Tracker; they
    surface only in the separate HR Tracker tab."""
    return cat == "HR" or cat == "HR (OVF)"

def _load_ledger() -> dict:
    if _SB_URL and _SB_KEY:
        rows = _sb_get("mpa_track_ledger", {
            "app": "eq.mlb", "category": "not.eq.__detail__",
            "locked": "eq.true",
            "select": "date,category,side,wins,losses",
        })
        if rows is not None:
            led: dict = {}
            for r in rows:
                led.setdefault(r["date"], {}).setdefault(r["category"], {})[r["side"]] = [r["wins"], r["losses"]]
            return led
    try:
        with open(_TRACK_LEDGER_PATH) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_ledger(led: dict):
    if _SB_URL and _SB_KEY:
        import datetime as _dt
        now = _dt.datetime.utcnow().isoformat() + "Z"
        rows = []
        for date_str, cats in led.items():
            for cat, sides in cats.items():
                for side, wl in sides.items():
                    rows.append({"app": "mlb", "date": date_str, "category": cat,
                                 "side": side, "wins": wl[0], "losses": wl[1],
                                 "locked": True, "locked_at": now})
        _sb_upsert("mpa_track_ledger", rows, on_conflict="app,date,category,side")
        return
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
    if _SB_URL and _SB_KEY:
        rows = _sb_get("mpa_track_ledger", {
            "app": "eq.mlb", "category": "eq.__detail__", "side": "eq.ALL",
            "select": "date,detail",
        })
        if rows is not None:
            return {r["date"]: (r.get("detail") or []) for r in rows}
    try:
        with open(_TRACK_DETAIL_PATH) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_detail(det: dict):
    if _SB_URL and _SB_KEY:
        import datetime as _dt
        now = _dt.datetime.utcnow().isoformat() + "Z"
        rows = [{"app": "mlb", "date": d, "category": "__detail__", "side": "ALL",
                 "wins": 0, "losses": 0, "locked": True, "locked_at": now,
                 "detail": picks}
                for d, picks in det.items()]
        _sb_upsert("mpa_track_ledger", rows, on_conflict="app,date,category,side")
        return
    try:
        tmp = f"{_TRACK_DETAIL_PATH}.{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump(det, f)
        os.replace(tmp, _TRACK_DETAIL_PATH)
    except Exception as e:
        print(f"[track_detail] save failed: {e}")

def _aggregate_graded(graded: dict) -> dict:
    """Collapse a graded day into {category: {side: [W, L]}} counting only decided picks
    that had odds posted — no-odds picks are excluded from the record."""
    agg: dict = {}
    for key in ("hitter_overs", "hitter_more", "hitter_unders", "runs", "tb_under", "tb_over", "rbi", "hr", "batter_walks", "hrr", "hrr_special", "triple_split", "five_star_split", "pitcher_ks", "pitcher_props", "top10_batter", "top10_pitcher", "overflow", "value_plays"):
        for r in graded.get(key, []):
            res = r.get("result")
            if res not in ("WIN", "LOSS"):
                continue
            if r.get("odds") is None or r.get("odds") == "":
                continue
            cat  = r.get("category") or key
            side = r.get("side") or "OVER"
            rec  = agg.setdefault(cat, {}).setdefault(side, [0, 0])
            if res == "WIN":
                rec[0] += 1
            else:
                rec[1] += 1
    # Version sentinel: marks a date as "Overflow Tracker graded" so the ledger
    # backfill re-grades each locked day exactly ONCE to bank its overflow rows,
    # then leaves it alone (no perpetual re-grading / box-score re-fetching).
    agg["__ovf_v1__"] = {"ALL": [0, 0]}
    return agg

def _detail_graded(graded: dict) -> list:
    """Flatten a graded day into per-pick rows (decided picks only) carrying the
    fields an earnings sheet needs: player, team, category, side, pick, odds,
    line, result. No-odds picks are excluded — they don't count in the record."""
    out = []
    for key in ("hitter_overs", "hitter_more", "hitter_unders", "runs", "tb_under", "tb_over", "rbi", "hr", "batter_walks", "hrr", "hrr_special", "triple_split", "five_star_split", "pitcher_ks", "pitcher_props", "top10_batter", "top10_pitcher", "overflow", "value_plays"):
        for r in graded.get(key, []):
            res = r.get("result")
            if res not in ("WIN", "LOSS"):
                continue
            if r.get("odds") is None or r.get("odds") == "":
                continue
            out.append({
                "name": r.get("name", ""),
                "team": r.get("team", ""),
                "category": r.get("category") or key,
                "side": r.get("side") or "OVER",
                "pick": r.get("pick", ""),
                "odds": r.get("odds"),
                "line": r.get("line"),
                "actual": r.get("actual"),
                "stat": r.get("stat", ""),
                "result": res,
                "ev": r.get("ev"),
                "ev_prob": r.get("ev_prob"),
                "edge": r.get("edge"),
                "proj": r.get("proj"),
                "series_pos": r.get("series_pos"),
            })
    return out

def _attach_clv(date_str: str, rows: list):
    """Stamp each graded row with open_odds (first run of the day) + close_odds
    (the locked last run). `odds` is left as the locked price for ROI; CLV is a
    separate lens that compares the price you'd have bet at vs the closing line."""
    open_picks = _load_open_cache(date_str)
    omap = {}
    if open_picks:
        try:
            og = _grade_date(date_str, open_picks)
            for r in _detail_graded(og):
                if r.get("odds") is not None:
                    omap[(r.get("name"), r.get("category"), r.get("side"), r.get("pick"))] = r.get("odds")
        except Exception as e:
            print(f"[clv] open grade failed {date_str}: {e}")
    for r in rows:
        # open_odds stays None when no opening line existed yet (e.g. props whose
        # line posts just before first pitch) — the frontend then skips it from
        # CLV rather than faking a 0% "even" move.
        r["close_odds"] = r.get("odds")
        r["open_odds"] = omap.get((r.get("name"), r.get("category"), r.get("side"), r.get("pick")))

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
        # Candidate past dates come from BOTH the local disk cache AND the
        # Supabase snapshots — so grading still works after a Render redeploy
        # wipes the disk. The last run of each day is what was stored, so that
        # final pre-game version is what gets graded and locked.
        cand = set()
        try:
            for fp in _glob.glob(os.path.join(_CACHE_DIR, "*.json")):
                bn = os.path.basename(fp).replace(".json", "")
                if not bn.startswith("_") and len(bn) == 10 and bn[4] == "-":
                    cand.add(bn)
        except Exception:
            pass
        for bn in _list_sb_pick_dates():
            if len(bn) == 10 and bn[4] == "-":
                cand.add(bn)
        for bn in sorted(cand):
            if bn >= today:
                continue          # today/future — games not final yet
            _bn_led = led.get(bn) or {}
            _need_ovf = "__ovf_v1__" not in _bn_led   # one-shot Overflow Tracker backfill
            need_led = (not _bn_led or
                        "Top 10 Batter" not in _bn_led or
                        "Top 10 Pitcher" not in _bn_led or
                        "Hitter Hits (More)" not in _bn_led or
                        _need_ovf)
            need_det = (bn not in det or not det.get(bn) or _need_ovf)
            if not need_led and not need_det:
                continue          # already locked — W/L and detail both present
            picks = _load_grading_picks(bn)
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
                _rows = _detail_graded(graded)
                try:
                    _attach_clv(bn, _rows)
                except Exception as _ce:
                    print(f"[clv] attach failed {bn}: {_ce}")
                det[bn] = _rows
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
# Bets persist in the working mpa_track_ledger jsonb `detail` column. The
# mpa_bet_log table's flat columns can't hold a full bet (name/team/stat_label
# /profit/settled_at) or a parlay's nested legs, so writes there 409/400 and
# silently drop. One row holds the entire {email: [bets]} dict under a sentinel
# date that never collides with real dated rows; category="__bets__" + locked
# False keep it out of the W/L read (_load_ledger requires locked=true) and the
# __detail__/__picks__ reads.
_BETS_CAT = "__bets__"
_BETS_DATE = "2000-01-01"
_BET_LOCK = _trk_threading.Lock()
_BET_STAT_KEYS = ("hits", "runs", "total_bases", "rbi", "homeRuns", "walks_bat", "hrr", "strikeOuts", "hits_allowed", "outs", "earnedRuns", "walks")
_BET_PITCH_STATS = ("strikeOuts", "hits_allowed", "outs", "earnedRuns", "walks")

def _load_bets() -> dict:
    if _SB_URL and _SB_KEY:
        rows = _sb_get("mpa_track_ledger", {
            "app": "eq.mlb", "category": f"eq.{_BETS_CAT}", "side": "eq.ALL",
            "date": f"eq.{_BETS_DATE}", "select": "detail", "limit": "1"})
        if rows is not None:
            return (rows[0].get("detail") or {}) if rows else {}
    try:
        with open(_BET_LOG_PATH) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_bets(data: dict):
    if _SB_URL and _SB_KEY:
        import datetime as _dt
        row = {"app": "mlb", "date": _BETS_DATE, "category": _BETS_CAT,
               "side": "ALL", "wins": 0, "losses": 0, "locked": False,
               "locked_at": _dt.datetime.utcnow().isoformat() + "Z", "detail": data}
        _sb_upsert("mpa_track_ledger", [row], on_conflict="app,date,category,side")
        return
    try:
        tmp = f"{_BET_LOG_PATH}.{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, _BET_LOG_PATH)
    except Exception as e:
        print(f"[bet_log] save failed: {e}")

def _bet_admin_ok(tok: str, admin: str) -> bool:
    return _is_admin_token(tok) or _is_tester_token(tok) or (
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

_REGRADE_DAYS = 3   # re-verify already-settled bets this many days, so a later
                    # MLB stat correction (e.g. an RBI added the next day) flips a
                    # wrong WIN/LOSS instead of staying locked forever.

def _recent_bet(d) -> bool:
    """True if a game date is recent enough that MLB could still post a stat
    correction — i.e. inside the re-grade window."""
    if not d:
        return False
    try:
        return 0 <= (date.today() - date.fromisoformat(str(d))).days <= _REGRADE_DAYS
    except Exception:
        return False

# Parlay legs are logged from the cart with a category code (BWALK/TBO/RBI/…) but,
# for several batter categories, the frontend historically stored an EMPTY stat_key.
# An empty key makes the box lookup read st.get("") -> None, so the leg can NEVER
# grade — under the old default-0 logic it silently auto-won every UNDER and auto-lost
# every batter OVER. Recover the real stat field from the leg's stat_label (preferred,
# unambiguous) or its category code so already-logged legs heal on the next re-grade.
_STAT_LABEL_KEYS = {
    "hits": "hits", "runs": "runs", "total bases": "total_bases", "rbi": "rbi",
    "hr": "homeRuns", "home runs": "homeRuns", "walks": "walks_bat",
    "batter walks": "walks_bat", "h+r+rbi": "hrr", "ks": "strikeOuts",
    "strikeouts": "strikeOuts", "outs": "outs", "hits allowed": "hits_allowed",
    "earned runs": "earnedRuns", "walks allowed": "walks",
}
_STAT_CAT_KEYS = {
    "hit": "hits", "hits": "hits", "hitter hits": "hits", "run": "runs",
    "runs": "runs", "rbi": "rbi", "hrr": "hrr", "hr": "homeRuns",
    "bwalk": "walks_bat", "batter walks": "walks_bat", "tb": "total_bases",
    "tbo": "total_bases", "tbu": "total_bases", "tb over": "total_bases",
    "tb under": "total_bases", "k": "strikeOuts", "pitcher ks": "strikeOuts",
    "pitcher outs": "outs", "pitcher hits allowed": "hits_allowed",
    "pitcher_hits_allowed": "hits_allowed", "pitcher_outs": "outs",
    "pitcher_earned_runs": "earnedRuns", "pitcher_walks": "walks",
    "pitcher walks": "walks",
}
def _resolve_stat_key(bet: dict) -> str:
    """The stat field this bet grades against. Falls back to stat_label / category
    when stat_key is blank (older parlay legs were logged without one)."""
    sk = (bet.get("stat_key") or "").strip()
    if sk:
        return sk
    lbl = (bet.get("stat_label") or "").strip().lower()
    if lbl in _STAT_LABEL_KEYS:
        return _STAT_LABEL_KEYS[lbl]
    cat = (bet.get("category") or "").strip().lower()
    return _STAT_CAT_KEYS.get(cat, "")

def _settle_bet_cached(bet: dict, name_stats: dict, all_final: bool = False) -> bool:
    """Grade a pending bet using pre-fetched name_stats (no extra API call).
    A player who never appears on a fully-Final, cleanly-fetched slate is VOID
    (no action: refunded, excluded from W/L and ROI) — never stuck pending, and a
    missing stat line is NEVER scored 0 (that would auto-win every UNDER).
    Already-settled bets are re-checked for a few days so a later MLB stat
    correction can flip a wrong result."""
    prev = bet.get("result")
    if prev in ("WIN", "LOSS", "PUSH", "VOID") and (bet.get("manual") or not _recent_bet(bet.get("date"))):
        return False

    def _void():
        if bet.get("result") == "VOID":
            return False
        bet["result"] = "VOID"; bet["actual"] = None
        bet["profit"] = 0.0; bet["settled_at"] = date.today().isoformat()
        return True

    def _apply(res, actual):
        prof = round(_american_profit(bet.get("odds"), bet.get("stake"), res), 2)
        if bet.get("result") == res and bet.get("actual") == actual and bet.get("profit") == prof:
            return False   # unchanged on a re-verify pass — no churn
        bet["result"] = res; bet["actual"] = actual
        bet["profit"] = prof; bet["settled_at"] = date.today().isoformat()
        return True

    st = name_stats.get(_norm_name(bet.get("name")))
    if not st or not st.get("final"):
        # game not final yet, or player absent on an incomplete slate: only a
        # fully-final slate with the player missing is a confirmed no-show -> VOID
        if (not st) and all_final:
            return _void()
        return False
    stat_key = _resolve_stat_key(bet)
    actual = st.get(stat_key) if stat_key else None
    if actual is None:
        # stat line missing (DNP / not yet posted). NEVER assume 0 — void on a
        # fully-final slate, otherwise leave pending until the box populates.
        if all_final:
            return _void()
        return False
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
    return _apply(res, actual)

def _settle_bet(bet: dict) -> bool:
    """Grade a still-pending bet against final box scores. Returns True if it
    changed. Only settles past dates; a player whose game isn't Final (or who
    didn't pitch, for pitching props) stays pending."""
    if bet.get("result") in ("WIN", "LOSS", "PUSH") or bet.get("manual"):
        return False
    bdate = bet.get("date")
    if not bdate or bdate >= date.today().isoformat():
        return False
    try:
        _ps, ns, _any, _af = _mlb_box_lookup(bdate)
    except Exception as e:
        print(f"[bet_log] settle lookup failed {bdate}: {e}")
        return False
    return _settle_bet_cached(bet, ns, _af)

def _settle_parlay_cached(parlay: dict, ns_cache: dict, af_cache: dict = None) -> bool:
    """Grade a parlay using pre-fetched ns_cache. WIN=all legs win, LOSS=any leg
    loses. A VOID leg (player DNP / cancelled game) is no-action: it can't keep
    the parlay pending, and a parlay with no loser but a voided leg refunds
    (PUSH). Recently-settled parlays are re-checked so a corrected leg flips the
    parlay too (e.g. a wrong WIN -> LOSS once a stat correction lands)."""
    legs = parlay.get("legs") or []
    if parlay.get("manual"):
        return False
    recent = any(_recent_bet(lg.get("date")) for lg in legs)
    if parlay.get("result") in ("WIN", "LOSS", "PUSH", "VOID") and not recent:
        return False
    for lg in legs:
        if lg.get("result") in ("WIN", "LOSS", "PUSH", "VOID") and not _recent_bet(lg.get("date")):
            continue
        bdate = lg.get("date")
        if bdate and bdate in ns_cache:
            _settle_bet_cached(lg, ns_cache[bdate], (af_cache or {}).get(bdate, False))
    results = [lg.get("result") for lg in legs]
    pending = (not results) or any(r not in ("WIN", "LOSS", "PUSH", "VOID") for r in results)
    if any(r == "LOSS" for r in results):
        newres, newprofit = "LOSS", round(-float(parlay.get("stake") or 0), 2)
    elif pending:
        return False  # no loss yet and legs still ungraded
    elif all(r == "WIN" for r in results):
        dec = _am_to_dec(parlay.get("odds"))
        stake = float(parlay.get("stake") or 0)
        newres, newprofit = "WIN", (round(stake * (dec - 1), 2) if dec else None)
    else:  # all resolved, no loser, some PUSH/VOID -> refund
        newres, newprofit = "PUSH", 0.0
    if parlay.get("result") == newres and parlay.get("profit") == newprofit:
        return False
    parlay["result"] = newres
    parlay["profit"] = newprofit
    parlay["settled_at"] = date.today().isoformat()
    return True

def _settle_parlay(parlay: dict) -> bool:
    """One-shot settle attempt for a just-logged parlay."""
    legs = parlay.get("legs") or []
    today = date.today().isoformat()
    dates_needed = {lg["date"] for lg in legs if lg.get("date") and lg["date"] < today}
    if not dates_needed:
        return False
    ns_cache: dict = {}
    af_cache: dict = {}
    for d in dates_needed:
        try:
            _, ns, _, _af = _mlb_box_lookup(d)
            ns_cache[d] = ns
            af_cache[d] = _af
        except Exception:
            pass
    return _settle_parlay_cached(parlay, ns_cache, af_cache)

def _settle_bets_batch(bets: list) -> bool:
    """Settle all pending bets (single + parlay) with ONE box-score API call per
    unique date. Returns True if any bet changed."""
    today = date.today().isoformat()
    dates_needed: set = set()
    _TERM = ("WIN", "LOSS", "PUSH", "VOID")
    def _want(d, res):
        # need a box lookup for any past-dated bet that's still open OR was
        # settled recently enough to re-verify against a possible stat correction
        return bool(d) and d < today and (res not in _TERM or _recent_bet(d))
    for b in bets:
        if b.get("bet_type") == "parlay":
            for lg in b.get("legs") or []:
                if _want(lg.get("date"), lg.get("result")):
                    dates_needed.add(lg["date"])
        else:
            if _want(b.get("date"), b.get("result")):
                dates_needed.add(b["date"])
    if not dates_needed:
        return False
    ns_cache: dict = {}
    af_cache: dict = {}
    for d in sorted(dates_needed):
        try:
            _ps, ns, _any, _af = _mlb_box_lookup(d)
            ns_cache[d] = ns
            af_cache[d] = _af
        except Exception as e:
            print(f"[bet_log] batch settle lookup failed {d}: {e}")
    changed = False
    for b in bets:
        if b.get("bet_type") == "parlay":
            if _settle_parlay_cached(b, ns_cache, af_cache):
                changed = True
        else:
            bdate = b.get("date")
            if bdate and bdate in ns_cache:
                if _settle_bet_cached(b, ns_cache[bdate], af_cache.get(bdate, False)):
                    changed = True
    return changed

def _summarize_bets(bets: list) -> dict:
    cats: dict = {}
    tot_staked = tot_profit = 0.0
    w = l = pu = pend = vo = 0
    for b in bets:
        res = b.get("result", "pending")
        try:
            stake = float(b.get("stake") or 0)
        except Exception:
            stake = 0.0
        c = cats.setdefault((b.get("category", "?"), (b.get("side") or "OVER").strip().upper()),
                            {"wins": 0, "losses": 0, "push": 0, "pending": 0, "void": 0,
                             "staked": 0.0, "profit": 0.0})
        if res == "WIN":
            w += 1; c["wins"] += 1
        elif res == "LOSS":
            l += 1; c["losses"] += 1
        elif res == "PUSH":
            pu += 1; c["push"] += 1
        elif res == "VOID":
            vo += 1; c["void"] += 1
        else:
            pend += 1; c["pending"] += 1
        if res in ("WIN", "LOSS", "PUSH"):
            prof = float(b.get("profit") or 0)
            tot_staked += stake; c["staked"] += stake
            tot_profit += prof;  c["profit"] += prof
    roi = (tot_profit / tot_staked * 100.0) if tot_staked > 0 else None
    by_cat = []
    _ord_idx = {c: i for i, c in enumerate(_TRACK_CAT_ORDER)}
    def _ck(k):
        cat, side = k
        return (_ord_idx.get(cat, 999), 0 if side == "OVER" else 1, cat, side)
    for key in sorted(cats.keys(), key=_ck):
        cat, side = key
        c = cats.get(key)
        if not c:
            continue
        st = c["staked"]; pr = c["profit"]
        by_cat.append({
            "category": cat, "side": side,
            "label": cat + (" Over" if side == "OVER" else " Under"),
            "wins": c["wins"], "losses": c["losses"],
            "push": c["push"], "pending": c["pending"], "void": c["void"],
            "staked": round(st, 2), "profit": round(pr, 2),
            "roi": round(pr / st * 100, 1) if st > 0 else None,
        })
    return {
        "wins": w, "losses": l, "push": pu, "pending": pend, "void": vo,
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
                "stat_key":   _resolve_stat_key(lg),
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
        "edge": round(float(body.get("edge") or 0), 4),
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


@app.post("/api/bets/{bet_id}/result")
async def set_bet_result(bet_id: str, request: Request, token: str = "", admin: str = ""):
    """Manually override a bet's result (WIN/LOSS/PUSH/VOID) or reset it to
    pending. A manual result is LOCKED so auto-grading never overwrites it again;
    resetting to pending re-enables auto-grading. Profit is recomputed from the
    stored odds + stake (0 for PUSH/VOID)."""
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _bet_admin_ok(tok, admin):
        raise HTTPException(status_code=403, detail="Admin only")
    try:
        body = await request.json()
    except Exception:
        body = {}
    res = str(body.get("result", "")).upper().strip()
    if res in ("", "PENDING", "CLEAR"):
        res = ""
    elif res not in ("WIN", "LOSS", "PUSH", "VOID"):
        raise HTTPException(status_code=400, detail="result must be WIN, LOSS, PUSH, VOID or PENDING")
    with _BET_LOCK:
        data = _load_bets()
        key = _bet_user_key(tok, admin)
        target = next((b for b in data.get(key, []) if b.get("id") == bet_id), None)
        if target is None:
            raise HTTPException(status_code=404, detail="Bet not found")
        if res == "":
            target["result"] = "pending"
            target["actual"] = None
            target["profit"] = None
            target["settled_at"] = None
            target.pop("manual", None)
            target.pop("manual_at", None)
        else:
            target["result"] = res
            if res in ("WIN", "LOSS"):
                target["profit"] = round(_american_profit(target.get("odds"), target.get("stake"), res), 2)
            else:
                target["profit"] = 0.0
            target["manual"] = True
            target["manual_at"] = date.today().isoformat()
            target["settled_at"] = date.today().isoformat()
        _save_bets(data)
    return {"ok": True, "result": target.get("result")}


@app.post("/api/bets/clear")
async def clear_bets(request: Request, token: str = "", admin: str = ""):
    """Wipe ALL logged bets for the current user and start fresh. Only the
    caller's own account bucket is cleared; other users' logs are untouched.
    Does NOT touch the global Track Record / Overflow / HR ledgers."""
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _bet_admin_ok(tok, admin):
        raise HTTPException(status_code=403, detail="Admin only")
    with _BET_LOCK:
        data = _load_bets()
        key = _bet_user_key(tok, admin)
        removed = len(data.get(key, []))
        if removed:
            data[key] = []
            _save_bets(data)
    return {"ok": True, "removed": removed}


@app.get("/api/grade/{date_str}")
async def grade_picks(date_str: str, request: Request, token: str = "", admin: str = ""):
    """Fetch actual MLB box scores and grade all picks for the given date."""
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    is_ok = _verify_hub_token(tok) or _is_admin_token(tok) or (
        bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__"))
    if not is_ok:
        raise HTTPException(status_code=401, detail="Subscription required")
    import datetime as _dt2
    _is_today = (date_str == _dt2.date.today().isoformat())
    if _is_today or date_str not in _cache:
        fresh = _load_grading_picks(date_str)
        if fresh is not None:
            _cache[date_str] = fresh
    picks = _cache.get(date_str)
    if not picks:
        raise HTTPException(status_code=404, detail="No picks for this date")
    try:
        return _grade_date(date_str, picks)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLB API error: {e}")


@app.post("/api/lock-picks")
async def lock_picks_endpoint(request: Request, token: str = "", admin: str = "", date: str = ""):
    """Admin only. Manually lock today's picks as the Track Record snapshot.
    Once locked, re-runs update the live display but grading always uses the lock."""
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    is_ok = _is_admin_token(tok) or (
        bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__"))
    if not is_ok:
        raise HTTPException(status_code=403, detail="Admin only")
    date_str = date or _dt.date.today().isoformat()
    # Check not already locked
    existing = _load_locked_picks(date_str)
    if existing:
        return {"ok": False, "msg": f"Already locked for {date_str}"}
    # Load current picks from the live snapshot
    picks = _load_pick_cache(date_str)
    if not picks:
        raise HTTPException(status_code=404, detail=f"No picks found for {date_str} — run picks first")
    ok = _save_locked_picks(date_str, picks)
    if ok:
        return {"ok": True, "msg": f"Picks locked for {date_str}"}
    return {"ok": False, "msg": "Save failed — check Supabase connection"}


@app.delete("/api/unlock-picks")
async def unlock_picks_endpoint(request: Request, token: str = "", admin: str = "", date: str = ""):
    """Admin only. Remove the manual lock for a date so re-runs update Track Record again."""
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    is_ok = _is_admin_token(tok) or (
        bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__"))
    if not is_ok:
        raise HTTPException(status_code=403, detail="Admin only")
    date_str = date or _dt.date.today().isoformat()
    existing = _load_locked_picks(date_str)
    if not existing:
        return {"ok": False, "msg": f"No lock found for {date_str}"}
    ok = _sb_delete("mpa_track_ledger", {
        "app": "eq.mlb", "date": f"eq.{date_str}",
        "category": f"eq.{_LOCKED_CAT}", "side": "eq.ALL"})
    if ok:
        print(f"[unlock_picks] unlocked {date_str}")
        return {"ok": True, "msg": f"Lock removed for {date_str}"}
    return {"ok": False, "msg": "Delete failed — check Supabase connection"}


@app.get("/api/lock-status")
async def lock_status_endpoint(request: Request, token: str = "", admin: str = "", date: str = ""):
    """Return whether picks are locked for a given date."""
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    is_ok = _is_admin_token(tok) or _is_tester_token(tok) or (
        bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__"))
    if not is_ok:
        raise HTTPException(status_code=403, detail="Admin only")
    date_str = date or _dt.date.today().isoformat()
    locked = _load_locked_picks(date_str) is not None
    return {"locked": locked, "date": date_str}


# Fresh-start cutoff for the RUNNING record. Picks dated before this stay in the
# ledger/Supabase (still openable via the daily date picker) but are excluded from
# the all-time record, the By-Day report, and the by-category breakdown. Bump this
# date whenever you want the scoreboard to start over.
_TRACK_START = "2026-06-15"


@app.get("/api/track-record")
async def track_record(request: Request, token: str = "", admin: str = ""):
    """Admin-only. All-time + daily W/L record per category (Over vs Under) from the
    permanent ledger. Grades any past cached day not yet locked, then aggregates."""
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    is_admin = _is_admin_token(tok) or _is_tester_token(tok) or (
        bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__")
    )
    if not is_admin:
        raise HTTPException(status_code=403, detail="Admin only")

    led = _update_track_ledger()
    det = _load_detail()

    alltime: dict = {}   # {category: {side: [W, L]}}
    daily = []
    for ds in sorted(led.keys()):
        if ds < _TRACK_START:
            continue   # pre-start dates kept in the ledger but off the running record
        day_w = day_l = 0
        for cat, sides in (led[ds] or {}).items():
            if cat == "__ovf_v1__" or _is_ovf_cat(cat) or _is_hr_cat(cat):
                continue   # overflow, HR, + version sentinel never count toward the main record
            _chal = (cat == "Value Plays")   # curated dup: own row, NOT in grand total
            for side, wl in sides.items():
                rec = alltime.setdefault(cat, {}).setdefault(side, [0, 0])
                rec[0] += wl[0]; rec[1] += wl[1]
                if not _chal:
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
        if ds < _TRACK_START:
            continue   # old detail rows stay saved, just hidden from the record
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
_LOOKUP_ABBR: dict = {}      # season -> {team_id: abbrev}


def _load_lookup_index(season: str):
    import requests as _rq
    MLB = "https://statsapi.mlb.com/api/v1"
    if season not in _LOOKUP_TEAMS:
        try:
            tr = _rq.get(f"{MLB}/teams", params={"sportId": 1, "season": season},
                         timeout=15).json()
            _LOOKUP_TEAMS[season] = {t["id"]: t.get("name", "") for t in tr.get("teams", [])}
            _LOOKUP_ABBR[season]  = {t["id"]: (t.get("abbreviation") or "").upper() for t in tr.get("teams", [])}
        except Exception:
            _LOOKUP_TEAMS[season] = {}; _LOOKUP_ABBR[season] = {}
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

@app.get("/api/lookup_matches")
def api_lookup_matches(name: str, date_str: str):
    # Lightweight: EVERY player whose name matches `name` AND is in a game today
    # (identity + game info only, no per-player stat calls). Lets the search show
    # both same-name players (e.g. the Contreras brothers), not just the one pick.
    q = (name or "").strip().lower()
    if len(q) < 3:
        return {"players": []}
    season = (date_str or "")[:4] or "2026"
    players, teams = _load_lookup_index(season)
    seen, cands = set(), []
    for k, v in players.items():
        if q in k and v["id"] not in seen:
            seen.add(v["id"]); cands.append(v)
    if not cands:
        return {"players": []}
    from fic_cache import _get_all_games
    games = _get_all_games(date_str)
    out = []
    for v in cands:
        tid = v["team_id"]; side = opp_id = opp_pname = None
        for g in games:
            if g["home_id"] == tid:
                side, opp_id, opp_pname = "HOME", g["away_id"], g.get("away_pitcher_short"); break
            if g["away_id"] == tid:
                side, opp_id, opp_pname = "AWAY", g["home_id"], g.get("home_pitcher_short"); break
        if not side:
            continue
        out.append({"full_name": v["full"], "team": teams.get(tid, ""),
                    "side": side, "opp": teams.get(opp_id, ""), "pitcher": opp_pname or ""})
    out.sort(key=lambda x: x["full_name"])
    return {"players": out[:8]}

@app.get("/api/player_deep")
def api_player_deep(name: str = "", date_str: str = ""):
    # Player Deep Dive (SEARCH BAR ONLY): resolve any hitter, return their last
    # 10 games (per-game H/R/RBI/BB/HR/TB) + career-vs-today's-pitcher, so the
    # search pop-up can show one consolidated card. One MLB gameLog call.
    import requests as _rq
    MLB = "https://statsapi.mlb.com/api/v1"
    q = (name or "").strip().lower()
    if len(q) < 3:
        return {"found": False, "msg": "Type at least 3 letters."}
    season = (date_str or "")[:4] or "2026"
    players, teams = _load_lookup_index(season)
    abbr = _LOOKUP_ABBR.get(season, {})
    match = players.get(q)
    if not match:
        cands = [v for k, v in players.items() if q in k]
        ids = {v["id"] for v in cands}
        if len(ids) == 1:
            match = cands[0]
        elif len(ids) > 1:
            ln = [v for k, v in players.items() if k.split() and k.split()[-1] == q]
            lnids = {v["id"] for v in ln}
            if len(lnids) == 1:
                match = ln[0]
    if not match:
        return {"found": False, "msg": f'No single MLB player found for "{name}".'}
    pid = match["id"]; team_id = match["team_id"]; full = match["full"]
    # today's game / opponent / opposing starter
    side = opp_id = opp_pid = opp_pname = None
    try:
        from fic_cache import _get_all_games
        for g in _get_all_games(date_str):
            if g["home_id"] == team_id:
                side, opp_id, opp_pid, opp_pname = "HOME", g["away_id"], g.get("away_pitcher_id"), g.get("away_pitcher_short"); break
            if g["away_id"] == team_id:
                side, opp_id, opp_pid, opp_pname = "AWAY", g["home_id"], g.get("home_pitcher_id"), g.get("home_pitcher_short"); break
    except Exception:
        pass
    # today's day/night + game-of-series (schedule) to highlight the live slot
    today_dn = today_series = None
    if side:
        try:
            r = _rq.get(f"{MLB}/schedule", params={"date": date_str, "sportId": 1},
                        timeout=8).json()
            done = False
            for dd in r.get("dates", []):
                for g in dd.get("games", []):
                    tt = g.get("teams", {}) or {}
                    hid = (((tt.get("home") or {}).get("team")) or {}).get("id")
                    aid = (((tt.get("away") or {}).get("team")) or {}).get("id")
                    if team_id in (hid, aid):
                        dn = (g.get("dayNight") or "").lower()
                        today_dn = dn if dn in ("day", "night") else None
                        sgn = g.get("seriesGameNumber")
                        if sgn:
                            today_series = "g1" if sgn == 1 else ("g2" if sgn == 2 else "g3")
                        done = True; break
                if done:
                    break
        except Exception:
            pass
    # last-10 hitting game log (per-game)
    games = []
    series_out = {}
    try:
        r = _rq.get(f"{MLB}/people/{pid}/stats",
                    params={"stats": "gameLog", "group": "hitting", "season": season},
                    timeout=10).json()
        splits = []
        for sg in r.get("stats", []):
            for sp in sg.get("splits", []):
                splits.append(sp)
        for sp in splits[-10:][::-1]:
            st = sp.get("stat", {}) or {}
            oid = (sp.get("opponent") or {}).get("id")
            oab = abbr.get(oid, "") or (((sp.get("opponent") or {}).get("name", "") or "")[:3].upper())
            def _i(k):
                try:
                    return int(st.get(k, 0) or 0)
                except Exception:
                    return 0
            games.append({"date": sp.get("date", ""), "opp": oab, "home": bool(sp.get("isHome")),
                          "ab": _i("atBats"), "h": _i("hits"), "r": _i("runs"), "rbi": _i("rbi"),
                          "bb": _i("baseOnBalls"), "hr": _i("homeRuns"), "tb": _i("totalBases")})
        # season series-game split (G1/G2/G3+) from full regular-season gameLog:
        # walk chronologically, numbering games within each run vs the same opp+venue
        buckets = {"g1": [0, 0], "g2": [0, 0], "g3": [0, 0]}  # [ab, hits]
        prev_key = None; cnt = 0
        for sp in splits:
            if (sp.get("gameType") or "R") != "R":
                continue
            key = ((sp.get("opponent") or {}).get("id"), bool(sp.get("isHome")))
            cnt = 1 if key != prev_key else cnt + 1
            prev_key = key
            bk = "g1" if cnt == 1 else ("g2" if cnt == 2 else "g3")
            st = sp.get("stat", {}) or {}
            try:
                buckets[bk][0] += int(st.get("atBats", 0) or 0)
                buckets[bk][1] += int(st.get("hits", 0) or 0)
            except Exception:
                pass
        for bk, (ab, h) in buckets.items():
            if ab:
                s = f"{(h / ab):.3f}"
                series_out[bk] = {"avg": (s[1:] if s.startswith("0.") else s), "ab": ab}
    except Exception:
        pass
    # career vs today's pitcher (one aggregate split)
    s1_ba = s1_ab = None
    if opp_pid:
        try:
            r = _rq.get(f"{MLB}/people/{pid}/stats",
                        params={"stats": "vsPlayerTotal", "group": "hitting", "opposingPlayerId": opp_pid},
                        timeout=8).json()
            for sg in r.get("stats", []):
                for sp in sg.get("splits", []):
                    st = sp.get("stat", {}) or {}
                    try:
                        ab = int(st.get("atBats", 0) or 0)
                    except Exception:
                        ab = 0
                    if ab:
                        s1_ab = ab
                        av = str(st.get("avg", "") or "")
                        try:
                            s1_ba = float(av) if av not in ("", ".---", "-.--") else None
                        except Exception:
                            s1_ba = None
        except Exception:
            pass
    # season Home/Away + Day/Night splits (one statSplits call)
    splits_out = {}
    try:
        r = _rq.get(f"{MLB}/people/{pid}/stats",
                    params={"stats": "statSplits", "group": "hitting",
                            "sitCodes": "h,a,d,n", "season": season, "gameType": "R"},
                    timeout=8).json()
        keymap = {"h": "home", "a": "away", "d": "day", "n": "night"}
        for sg in r.get("stats", []):
            for sp in sg.get("splits", []):
                k = keymap.get((sp.get("split") or {}).get("code"))
                if not k:
                    continue
                st = sp.get("stat", {}) or {}
                try:
                    ab = int(st.get("atBats", 0) or 0)
                except Exception:
                    ab = 0
                splits_out[k] = {"avg": str(st.get("avg", "") or ""), "ab": ab}
    except Exception:
        pass
    return {"found": True, "full_name": full, "team": teams.get(team_id, ""),
            "side": side, "opp": teams.get(opp_id, "") if opp_id else "",
            "opp_abbr": abbr.get(opp_id, "") if opp_id else "",
            "pitcher": opp_pname or "", "in_game": bool(side),
            "s1_ba": s1_ba, "s1_ab": s1_ab, "splits": splits_out,
            "series": series_out, "today_dn": today_dn,
            "today_series": today_series, "games": games}

@app.get("/api/whoami")
async def whoami(request: Request, token: str = ""):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    return {"is_admin": _is_admin_token(tok), "is_tester": _is_tester_token(tok)}

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
    /* Admin gate: hidden by default, shown only when body has is-admin or is-tester */
    .admin-only { display: none !important; }
    body.is-admin .admin-only { display: revert !important; }
    body.is-admin .results-table th.admin-only,
    body.is-admin .results-table td.admin-only { display: table-cell !important; }
    /* Tester: sees everything admin sees except run/force buttons */
    body.is-tester .admin-only { display: revert !important; }
    body.is-tester .results-table th.admin-only,
    body.is-tester .results-table td.admin-only { display: table-cell !important; }
    body.is-tester .admin-run-only { display: none !important; }
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
    .parlay-cat-row{display:flex;align-items:center;gap:6px;padding:3px 2px;cursor:pointer;font-size:.73rem;color:#ddd;user-select:none}
    .parlay-cat-row input{cursor:pointer;width:13px;height:13px;flex-shrink:0;accent-color:#f59e0b}
    .parlay-cat-section{font-size:.58rem;font-weight:800;letter-spacing:.09em;color:#555;padding:5px 2px 2px;text-transform:uppercase}
    .env-chip{display:inline-block;margin:3px 0 0;padding:2px 7px;border:1px solid #333;border-radius:6px;font-size:.6rem;font-weight:700;letter-spacing:.01em;line-height:1.25;background:#0d0d0d}
    .more-btn{width:100%;margin-top:14px;padding:11px 16px;background:#0f172a;border:1px solid #334155;border-radius:12px;font-size:.82rem;font-weight:700;cursor:pointer;letter-spacing:.06em;text-align:center;transition:background .15s,border-color .15s}
    .more-btn:hover{background:#1e293b;border-color:#475569}
    .mlb-pick-card{border-radius:14px;overflow:hidden;background:linear-gradient(180deg,#161616 0%,#0f0f0f 100%);border:1px solid #262626;display:flex;flex-direction:column}
    .mlb-pick-card:hover{border-color:rgba(245,158,11,.35)}
    .mlb-card-header{padding:6px 11px;display:flex;align-items:center;justify-content:space-between;gap:8px;border-bottom:2px solid #f59e0b}
    .mlb-card-header>div:first-child{min-width:0;flex:1 1 auto;flex-wrap:nowrap}
    .mlb-card-header>img{flex:0 0 auto}
    .mlb-card-photo{position:relative;height:140px;overflow:hidden;background:radial-gradient(ellipse at center top,rgba(245,158,11,.15),transparent 70%),linear-gradient(180deg,#1a2a1a 0%,#0a1a0a 100%)}
    .mlb-card-name{background:#f59e0b;color:#000;text-align:center;padding:4px 10px;font-weight:900;font-size:.9rem;letter-spacing:.01em}
    .mlb-card-body{padding:7px 11px 9px;flex:1;display:flex;flex-direction:column;gap:3px}
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
      <button class="admin-only" id="track-btn" onclick="openTrackRecord()" title="All-time + daily Win/Loss record across every graded day, by category" style="background:#7c3aed;color:#fff;border:none;border-radius:10px;padding:9px 18px;min-width:140px;text-align:center;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">🏆 Track Record</button>
      <button class="admin-only" id="mybets-btn" onclick="openMyBets()" title="Your personal logged bets — click Get Results to grade against box scores" style="background:#4338ca;color:#fff;border:none;border-radius:10px;padding:9px 18px;min-width:140px;text-align:center;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">💰 My Bets</button>
      <button class="admin-only" id="ovf-btn" onclick="openOverflow()" title="Every pick beyond each category's top 10 — graded and banked in its own permanent record" style="background:#b45309;color:#fff;border:none;border-radius:10px;padding:9px 18px;min-width:140px;text-align:center;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">⭐ Overflow</button>
      <button class="admin-only" id="hrtrk-btn" onclick="openHRTracker()" title="Home Run Over/Under picks — their own permanent record, kept out of the main Track Record and Overflow" style="background:#be123c;color:#fff;border:none;border-radius:10px;padding:9px 18px;min-width:140px;text-align:center;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">💣 HR Tracker</button>
      <button class="admin-only" id="dow-btn" onclick="openDowReport()" title="Which weekdays actually produce winners, and whether the matrix lean matches reality" style="background:#0e7490;color:#fff;border:none;border-radius:10px;padding:9px 18px;min-width:140px;text-align:center;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">📅 By Day</button>
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
        <div id="bc-today-lean" style="border-radius:8px;padding:8px 12px;background:rgba(245,158,11,.07);border:1px solid rgba(245,158,11,.2);margin:10px 0 10px;display:none">
          <span style="font-size:.68rem;color:#fbbf24;font-weight:800;letter-spacing:.06em;text-transform:uppercase">Today&#39;s Matrix Lean</span>
          <div id="bc-lean-text" style="font-size:.72rem;color:#e2e8f0;margin-top:4px;line-height:1.7"></div>
        </div>
        <script>
        var _DOW_SIG={
          0:['U','U','U','U','U','U','U','O','O','O'],
          1:['U','U','U','U','U','O','O','U','U','U'],
          2:['O','O','O','O','O','U','O','O','O','O'],
          3:['O','O','O','O','O','O','U','O','O','O'],
          4:['U','U','U','U','U','U','U','U','O','U'],
          5:['O','O','O','O','O','O','O','U','U','U'],
          6:['O','O','O','O','O','U','U','O','O','O']
        };
        // DISPLAY-ONLY corrected matrix shown in the top strategy bar. The live
        // _DOW_SIG above is left UNCHANGED on purpose so picks keep being run,
        // graded and collected exactly as before. Nothing reads _DOW_DISP except
        // the two display renderers below (day-of-week table + today lean banner).
        var _DOW_DISP={
          0:['U','U','U','U','U','O','O','U','U','O'],
          1:['U','U','U','U','U','O','O','U','U','U'],
          2:['O','O','O','O','O','U','U','O','O','O'],
          3:['O','O','O','O','O','U','U','O','O','O'],
          4:['U','U','U','U','U','O','O','U','U','U'],
          5:['O','O','O','O','O','U','U','O','O','U'],
          6:['O','O','O','O','O','U','U','O','O','O']
        };
        var _DOW_IDX={hits:0,hits_over:0,hits_under:0,tb:1,tb_under:1,tb_over:1,hrr:2,hr:2,runs:3,rbi:4,k:5,outs:6,hits_allowed:7,er:8,walks:9};
        var _DOW_BAT=[0,1,2,3,4,9];
        var _DOW_PIT=[5,7,6,8,9];
        var _DOW_NAMES=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
        var _DOW_WHY=[
          'it is a getaway day game with regulars resting and travel ahead',
          'it follows a frequent off-day with fresh arms and series openers',
          'lineups have settled into the series',
          'hitters are dialed in midweek',
          'it is a frequent getaway day game with regulars resting and travel ahead',
          'the weekend opens with high energy and softer back-end arms',
          'the weekend slate runs at full strength'
        ];
        // The matrix keys off the SLATE date of the picks you ran (window._lastResult.date),
        // not the wall clock \u2014 so simulating a future day shows that day&#39;s leans. Falls
        // back to today before any run.
        function _slateDay(){
          try{ var ds=(window._lastResult&&window._lastResult.date); if(ds){ var w=new Date(ds+'T12:00:00').getDay(); if(!isNaN(w)) return w; } }catch(e){}
          return new Date().getDay();
        }
        function _dayName(){return _DOW_NAMES[_slateDay()];}
        function _dayLean(isPit,catIdx){
          if(catIdx==null) return '';
          var map=isPit?_DOW_PIT:_DOW_BAT;
          var idx=map[catIdx]; if(idx==null) return '';
          var row=_DOW_SIG[_slateDay()]||[];
          return row[idx]||'';
        }
        var SLOTS=[null,
          {name:'Game 1',label:'Series Opener',bat:['U','U','U','U','U','U'],pit:['O','U','O','U','U']},
          {name:'Game 2',label:'Mid-Series',bat:['O','O','O','O','O','O'],pit:['U','O','O','O','O']},
          {name:'Game 3+',label:'Late Series',bat:['O','O','O','O','O','O'],pit:['U','O','U','O','O']}
        ];
        window.__MPA_SLOTS__=SLOTS;
        function _buildDowTable(isPit){
          var cols=isPit?[['K',5],['Hits Allowed',7],['Outs',6],['ER',8],['BB',9]]:[['Hits',0],['TB',1],['HRR',2],['Runs',3],['RBI',4],['Walks',9]];
          var h='<table style="width:100%;border-collapse:collapse;font-size:.71rem;min-width:560px"><thead><tr style="border-bottom:2px solid #1e293b"><th style="text-align:left;padding:7px 8px;color:#94a3b8;font-size:.62rem;letter-spacing:.06em;white-space:nowrap;font-weight:700">DAY</th>';
          cols.forEach(function(c){h+='<th style="text-align:center;padding:7px 6px;color:#94a3b8;font-size:.6rem;letter-spacing:.03em;font-weight:700">'+c[0]+'</th>';});
          h+='</tr></thead><tbody>';
          [1,2,3,4,5,6,0].forEach(function(d){
            var sig=_DOW_DISP[d]||[];
            h+='<tr data-dow="'+d+'" style="border-bottom:1px solid #1e1e1e"><td title="'+_DOW_WHY[d]+'" style="padding:8px 8px;color:#cbd5e1;font-weight:700;white-space:nowrap;cursor:help">'+_DOW_NAMES[d]+'</td>';
            cols.forEach(function(c){var v=sig[c[1]];var clr=v==='O'?'#4ade80':'#ff8a65';h+='<td style="text-align:center;padding:8px 6px"><b style="color:'+clr+'">'+(v==='O'?'OVER':'UNDER')+'</b></td>';});
            h+='</tr>';
          });
          return h+'</tbody></table>';
        }
        function _renderLeanBanner(){
          var day=_slateDay();
          var sig=_DOW_DISP[day]||[];
          var oc=function(v){return v==='O'?'<b style="color:#4ade80">OVER</b>':'<b style="color:#ff8a65">UNDER</b>';};
          var el=document.getElementById('bc-today-lean'), tx=document.getElementById('bc-lean-text');
          if(el&&tx){
            el.style.display='block';
            tx.innerHTML='<b style="color:#fbbf24">'+_dayName()+'</b> day-of-week lean \u2014 each pick also carries its series game (G1/G2/G3), and the two combine on the card:'
              +'<br><span style="color:#94a3b8">Batters:</span> Hits '+oc(sig[0])+' \u00b7 TB '+oc(sig[1])+' \u00b7 HRR '+oc(sig[2])+' \u00b7 Runs '+oc(sig[3])+' \u00b7 RBI '+oc(sig[4])+' \u00b7 Walks '+oc(sig[9])
              +'<br><span style="color:#94a3b8">Pitchers:</span> K '+oc(sig[5])+' \u00b7 Hits Allowed '+oc(sig[7])+' \u00b7 Outs '+oc(sig[6])+' \u00b7 ER '+oc(sig[8])+' \u00b7 BB '+oc(sig[9]);
          }
          document.querySelectorAll('[data-dow]').forEach(function(tr){
            tr.style.background=(tr.dataset.dow===String(day))?'rgba(245,158,11,.08)':'';
          });
        }
        window._renderLeanBanner=_renderLeanBanner;
        document.addEventListener('DOMContentLoaded',function(){
          var bd=document.getElementById('bc-bat-dow'); if(bd) bd.innerHTML=_buildDowTable(false);
          var pd=document.getElementById('bc-pit-dow'); if(pd) pd.innerHTML=_buildDowTable(true);
          _renderLeanBanner();
        });
        </script>
        <div style="display:flex;gap:8px;margin:4px 0 12px">
          <button id="bc-tab-bat" onclick="_bcTab('bat')" style="padding:5px 16px;border-radius:8px;border:1px solid #4ade80;background:rgba(74,222,128,.1);color:#4ade80;font-weight:800;font-size:.74rem;cursor:pointer;letter-spacing:.04em">&#9918; BATTERS</button>
          <button id="bc-tab-pit" onclick="_bcTab('pit')" style="padding:5px 16px;border-radius:8px;border:1px solid #334155;background:transparent;color:#64748b;font-weight:800;font-size:.74rem;cursor:pointer;letter-spacing:.04em">&#128142; PITCHERS</button>
        </div>
        <div id="bc-bat" style="overflow-x:auto">
          <div style="font-size:.65rem;color:#64748b;margin-bottom:8px"><b style="color:#4ade80">OVER</b> = Over signal &nbsp;&#183;&nbsp; <b style="color:#ff8a65">UNDER</b> = Under signal</div>
          <div style="font-size:.68rem;color:#fbbf24;font-weight:800;letter-spacing:.05em;text-transform:uppercase;margin:4px 0 8px">Series Position &middot; G1 / G2 / G3</div>
          <table style="width:100%;border-collapse:collapse;font-size:.71rem;min-width:680px">
            <thead>
              <tr style="border-bottom:2px solid #1e293b">
                <th style="text-align:left;padding:7px 8px;color:#94a3b8;font-size:.62rem;letter-spacing:.06em;white-space:nowrap;font-weight:700;width:9%">GAME SLOT</th>
                <th style="text-align:left;padding:7px 8px;color:#4ade80;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:15%">&#9918; HITS<br><span style="color:#64748b;font-weight:400">O/U 0.5</span></th>
                <th style="text-align:left;padding:7px 8px;color:#a78bfa;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:15%">&#128995; TOTAL BASES<br><span style="color:#64748b;font-weight:400">O/U 1.5</span></th>
                <th style="text-align:left;padding:7px 8px;color:#fb923c;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:15%">&#128293; HRR &middot; H+R+RBI<br><span style="color:#64748b;font-weight:400">O/U 1.5</span></th>
                <th style="text-align:left;padding:7px 8px;color:#60a5fa;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:15%">&#128309; RUNS SCORED<br><span style="color:#64748b;font-weight:400">O/U 0.5</span></th>
                <th style="text-align:left;padding:7px 8px;color:#fbbf24;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:15%">&#128993; RBIs RECORDED<br><span style="color:#64748b;font-weight:400">O/U 0.5</span></th>
                <th style="text-align:left;padding:7px 8px;color:#34d399;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:16%">&#128694; WALKS DRAWN<br><span style="color:#64748b;font-weight:400">O/U 0.5</span></th>
              </tr>
            </thead>
            <tbody>
              <tr data-slot="1" style="border-bottom:1px solid #1e1e1e">
                <td style="padding:8px 8px;color:#34d399;font-weight:700;white-space:nowrap;vertical-align:top">Game 1<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Series Opener</span></td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> The opposing ace takes the mound. Hitters are seeing him fresh for the first time in the series — contact drops against top starters.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Aces keep the ball in the park. Extra-base hits are rare when the best arm is pitching in Game 1.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Hard to stack hits, runs, and RBIs against a fresh ace. All three are tough to hit in the first game of a series.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Fewer baserunners means fewer runs. Top starters shut down the top of the lineup early in a series.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> No runners on base means no RBI chances. Lean Under until hitters get a real look at the pitcher.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Aces pound the zone with sharp command. Hitters rarely draw a free pass against a fresh top starter.</td>
              </tr>
              <tr data-slot="2" style="border-bottom:1px solid #1e1e1e;background:rgba(255,255,255,.015)">
                <td style="padding:8px 8px;color:#fbbf24;font-weight:700;white-space:nowrap;vertical-align:top">Game 2<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Mid-Series</span></td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Hitters have now seen the opposing staff once. They adjust fast — contact and hit rate climb in the second game.</td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Softer No. 2-3 starters get squared up more. Extra-base opportunities increase compared to Game 1.</td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> More hits means more runs and RBIs. Game 2 is the sweet spot for multi-stat combo props.</td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> A tired bullpen from Game 1 leaks runs late. Hitters cash in more often as the series goes on.</td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Middle-of-the-order bats find their rhythm. Runners score at a higher clip in the second game.</td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> The No. 2-3 starter has shakier command. Hitters work deeper counts and draw more walks in Game 2.</td>
              </tr>
              <tr data-slot="3" style="border-bottom:1px solid #1e1e1e">
                <td style="padding:8px 8px;color:#f87171;font-weight:700;white-space:nowrap;vertical-align:top">Game 3<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Late Series</span></td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> The third starter is usually the weakest arm on the staff. Hitters have seen the whole rotation — lean hard on hits.</td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Back-end starters give up the most hard contact. Extra-base hits climb in the final game of the series.</td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> All three stats trend up against the weakest starter. Strong play for hits, runs, and RBI props in Game 3.</td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Deep counts and walks fill the bases. Runs score easily against a tiring rotation in the late games of a series.</td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Cleanup spots feast on weak starters. Target RBI props for hitters batting 3rd through 5th in Game 3.</td>
                <td style="padding:8px 8px;color:#86efac;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Back-end starters lose the zone as pitch counts climb. Walks drawn pile up against the weakest arm.</td>
              </tr>
            </tbody>
          </table>
          <div style="font-size:.68rem;color:#fbbf24;font-weight:800;letter-spacing:.05em;text-transform:uppercase;margin:18px 0 8px">Day Of Week &middot; Mon&ndash;Sun</div>
          <div style="font-size:.63rem;color:#64748b;margin-bottom:8px">Hover a day for the reasoning. Today is highlighted. Each pick combines its series game with this day lean.</div>
          <div id="bc-bat-dow" style="overflow-x:auto"></div>
        </div>
        <div id="bc-pit" style="overflow-x:auto;display:none">
          <div style="font-size:.65rem;color:#64748b;margin-bottom:8px"><b style="color:#4ade80">OVER</b> = Over signal &nbsp;&#183;&nbsp; <b style="color:#ff8a65">UNDER</b> = Under signal</div>
          <div style="font-size:.68rem;color:#fbbf24;font-weight:800;letter-spacing:.05em;text-transform:uppercase;margin:4px 0 8px">Series Position &middot; G1 / G2 / G3</div>
          <table style="width:100%;border-collapse:collapse;font-size:.71rem;min-width:680px">
            <thead>
              <tr style="border-bottom:2px solid #1e293b">
                <th style="text-align:left;padding:7px 8px;color:#94a3b8;font-size:.62rem;letter-spacing:.06em;white-space:nowrap;font-weight:700;width:9%">GAME SLOT</th>
                <th style="text-align:left;padding:7px 8px;color:#63cab7;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#129518; PITCHER K<br><span style="color:#64748b;font-weight:400">Strikeouts O/U</span></th>
                <th style="text-align:left;padding:7px 8px;color:#93c5fd;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#127919; HITS ALLOWED<br><span style="color:#64748b;font-weight:400">Hits Allowed O/U</span></th>
                <th style="text-align:left;padding:7px 8px;color:#94a3b8;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#9200; OUTS RECORDED<br><span style="color:#64748b;font-weight:400">Outs Recorded O/U</span></th>
                <th style="text-align:left;padding:7px 8px;color:#ff8a65;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:18%">&#128165; EARNED RUNS<br><span style="color:#64748b;font-weight:400">Earned Runs O/U</span></th>
                <th style="text-align:left;padding:7px 8px;color:#fbbf24;font-size:.62rem;letter-spacing:.06em;font-weight:700;width:19%">&#128694; WALKS ALLOWED<br><span style="color:#64748b;font-weight:400">Walks Allowed O/U</span></th>
              </tr>
            </thead>
            <tbody>
              <tr data-slot="1" style="border-bottom:1px solid #1e1e1e">
                <td style="padding:8px 8px;color:#34d399;font-weight:700;white-space:nowrap;vertical-align:top">Game 1<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Series Opener</span></td>
                <td style="padding:8px 8px;color:#a7f3d0;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> The ace takes the mound on full rest. Peak velocity and movement generate high strikeout totals — lean Over on Ks.</td>
                <td style="padding:8px 8px;color:#bfdbfe;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Hitters are cold and seeing this pitcher fresh. Top starters limit hard contact early — lean Under on hits allowed.</td>
                <td style="padding:8px 8px;color:#cbd5e1;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Managers trust their ace to go deep. Expect 6-7 innings and a solid out total when the best arm is on the mound.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> The best arm controls damage. Even hot lineups struggle to score against an ace on full rest.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Full-rest command is at its sharpest. The ace pounds the strike zone and keeps walks low all game.</td>
              </tr>
              <tr data-slot="2" style="border-bottom:1px solid #1e1e1e;background:rgba(255,255,255,.015)">
                <td style="padding:8px 8px;color:#fbbf24;font-weight:700;white-space:nowrap;vertical-align:top">Game 2<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Mid-Series</span></td>
                <td style="padding:8px 8px;color:#a7f3d0;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> The No. 2 or 3 starter does not have the same swing-and-miss stuff as the ace. Fewer strikeouts — lean Under on Ks.</td>
                <td style="padding:8px 8px;color:#bfdbfe;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Softer velocity gets squared up more easily. Hitters make better contact in Game 2 — lean Over on hits allowed.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> No. 2-3 starters get squared up and pulled earlier than the ace. Shorter outings mean fewer outs recorded — lean Under in Game 2.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Patient lineups sit on softer pitches and punish mistakes. Earned runs climb in the middle of a series.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Less precise command than the ace. Hitters work deeper counts and draw more walks in Game 2.</td>
              </tr>
              <tr data-slot="3" style="border-bottom:1px solid #1e1e1e">
                <td style="padding:8px 8px;color:#f87171;font-weight:700;white-space:nowrap;vertical-align:top">Game 3<br><span style="font-size:.66rem;color:#64748b;font-weight:400">Late Series</span></td>
                <td style="padding:8px 8px;color:#a7f3d0;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> The weakest arm on the staff takes the ball. Back-end starters rarely rack up big strikeout totals — lean Under on Ks.</td>
                <td style="padding:8px 8px;color:#bfdbfe;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> The No. 5 or spot starter gives up the most hard contact of any game slot. Hits allowed trend Over in Game 3.</td>
                <td style="padding:8px 8px;color:#cbd5e1;line-height:1.6;vertical-align:top"><b style="color:#ff8a65">U</b> Managers pull back-end starters at the first sign of trouble. Shortest outings of the series — fewer outs total.</td>
                <td style="padding:8px 8px;color:#fca5a5;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Back-end starters give up earned runs at the highest rate. Play the Over on earned runs by default in Game 3.</td>
                <td style="padding:8px 8px;color:#fde68a;line-height:1.6;vertical-align:top"><b style="color:#4ade80">O</b> Spot starters lose command as pitch counts climb. Walk totals pile up in the later innings of Game 3.</td>
              </tr>
            </tbody>
          </table>
          <div style="font-size:.68rem;color:#fbbf24;font-weight:800;letter-spacing:.05em;text-transform:uppercase;margin:18px 0 8px">Day Of Week &middot; Mon&ndash;Sun</div>
          <div style="font-size:.63rem;color:#64748b;margin-bottom:8px">Hover a day for the reasoning. Today is highlighted. Each pick combines its series game with this day lean.</div>
          <div id="bc-pit-dow" style="overflow-x:auto"></div>
        </div>
      </div>
    </div>
    <div class="run-box" id="runBox" style="text-align:center;max-width:600px;margin:0 auto 20px">
      <h2 style="font-family:'Playfair Display',serif;font-size:1.5rem;font-weight:700;color:#fff;margin-bottom:20px">Run Today's Picks</h2>
      <div class="date-row" style="justify-content:center;margin-bottom:20px">
        <label>Date</label>
        <input type="date" id="date-picker" max="" onchange="checkLockStatus()"/>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:10px;justify-content:center;margin-bottom:8px">
        <button class="btn-primary" id="get-btn" onclick="getPicks()">🎯 Get Picks</button>
        <button class="btn-primary admin-only admin-run-only" id="run-btn" onclick="startRun()">Run Picks</button>
        <button class="btn-primary admin-only admin-run-only" id="force-btn" onclick="startRun(true)" style="background:#dc2626;color:#fff" title="Bypass cache and rebuild today's picks from scratch">Force Refresh</button>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:10px;justify-content:center;margin-bottom:8px">
        <button class="btn-primary" id="ev-btn" onclick="toggleEvOnly()" style="background:#1f2937;color:#fff" title="Show only hit picks where our matchup model (your hitter vs this pitcher) beats the sportsbook price. Default off — all picks shown, ranked by value.">&#10003; +EV Only</button>
        <button class="btn-primary" id="proj-edge-btn" onclick="_openProjEdge()" style="background:#1f2937;color:#38bdf8;border:1px solid #1e3a5f" title="All picks where our model projection beats the book line. Pitchers: count proj vs line. Hitters: win probability vs 50%. All plays shown.">&#9650; Proj Edge</button>
        <select id="odds-range-sel" onchange="onOddsRangeChange()" style="background:#1f2937;color:#fff;border:1px solid #374151;border-radius:10px;padding:9px 14px;font-size:.82rem;font-weight:700;cursor:pointer;min-width:150px;outline:none" title="Filter all picks to a specific odds range">
          <option value="">All Odds</option>
          <option value="le-500">&#8804; &#x2212;500</option>
          <option value="-500to-450">&#x2212;500 to &#x2212;450</option>
          <option value="-450to-400">&#x2212;450 to &#x2212;400</option>
          <option value="-400to-350">&#x2212;400 to &#x2212;350</option>
          <option value="-350to-300">&#x2212;350 to &#x2212;300</option>
          <option value="-300to-250">&#x2212;300 to &#x2212;250</option>
          <option value="-250to-200">&#x2212;250 to &#x2212;200</option>
          <option value="-200to-150">&#x2212;200 to &#x2212;150</option>
          <option value="-150to-100">&#x2212;150 to &#x2212;100</option>
          <option value="+100to+150">+100 to +150</option>
          <option value="+150to+200">+150 to +200</option>
          <option value="+200to+250">+200 to +250</option>
          <option value="+250to+300">+250 to +300</option>
          <option value="ge+300">&#8805; +300</option>
        </select>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:10px;justify-content:center;margin-bottom:12px">
        <button class="btn-primary admin-only admin-run-only" id="lock-btn" onclick="lockPicks()" style="background:#7c3aed;color:#fff" title="Lock these picks into the Track Record. Re-runs after locking won\'t affect grading.">&#128274; Lock Picks</button>
        <button class="btn-primary admin-only" id="unlock-btn" onclick="unlockPicks()" style="background:#b45309;color:#fff;display:none" title="Remove lock — re-runs will update Track Record again.">&#128275; Unlock Picks</button>
        <span id="lock-status-badge" style="font-size:.75rem;font-weight:700;padding:4px 10px;border-radius:9999px;display:none"></span>
      </div>
      <div id="run-spinner" class="hidden" style="margin-top:12px;color:#6b7280;font-size:13px">
        <span class="spinner"></span> Analyzing player histories…
      </div>
    </div>
    <div id="rotation-card" class="card p-6 admin-only">
      <div class="section-hdr" style="color:#f59e0b">🔧 Rotation Order <span style="font-size:.7rem;color:#777;font-weight:400">admin only</span></div>
      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:12px">
        <button class="btn-primary" onclick="loadRotation()">Load Rotations</button>
        <button class="btn-primary" id="rot-save-btn" onclick="saveRotation()" style="background:#16a34a;color:#fff">Save Overrides</button>
        <button class="btn-primary" id="rot-collapse-btn" onclick="_rotCollapseAll()" style="background:#334155;color:#e5e7eb">Collapse all</button>
        <span id="rot-status" style="font-size:.78rem;color:#9ca3af"></span>
      </div>
      <div id="rotation-list" style="display:flex;flex-direction:column;gap:14px"></div>
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
          <select id="parlayOddsRange" onchange="_parlayOddsRangeChange()" style="background:#0f0f0f;border:1px solid #262626;border-radius:8px;color:#fff;padding:8px 10px;font-size:.8rem">
            <option value="all">All Odds</option>
            <option value="p100">+100 to +149</option>
            <option value="p150">+150 to +299</option>
            <option value="p300">+300+</option>
          </select>
          <div style="position:relative;display:inline-block">
            <button class="btn-primary" id="parlay-cats-btn" onclick="toggleCatMenu(event)" style="background:#1f2937;color:#fff">&#9776; Categories (13/13) &#9662;</button>
            <div id="parlay-cats-menu" style="display:none;position:absolute;z-index:60;top:calc(100% + 6px);left:0;background:#0e0e0e;border:1px solid #2a2a2a;border-radius:10px;padding:10px 12px;min-width:190px;box-shadow:0 12px 34px rgba(0,0,0,.55)">
              <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px">
                <span style="font-size:.63rem;color:#888;font-weight:800;letter-spacing:.06em">PARLAY CATEGORIES</span>
                <span style="font-size:.63rem"><a onclick="_catSetAll(true)" style="color:#63cab7;cursor:pointer;font-weight:800">All</a> <span style="color:#444">·</span> <a onclick="_catSetAll(false)" style="color:#ff8a65;cursor:pointer;font-weight:800">None</a></span>
              </div>
              <div id="parlay-cats-list">
                <div class="parlay-cat-section">Batters</div>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="HIT_O" checked onchange="_catChanged()"> Hits Over 0.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="HIT_U" checked onchange="_catChanged()"> Hits Under 1.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="TB_O" checked onchange="_catChanged()"> Total Bases Over 1.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="TB_U" checked onchange="_catChanged()"> Total Bases Under 1.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="RUN_O" checked onchange="_catChanged()"> Runs Over 0.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="RUN_U" checked onchange="_catChanged()"> Runs Under 0.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="RBI_O" checked onchange="_catChanged()"> RBI Over 0.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="RBI_U" checked onchange="_catChanged()"> RBI Under 0.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="HR_O" checked onchange="_catChanged()"> HR Over 0.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="HR_U" checked onchange="_catChanged()"> HR Under 0.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="HRR_O" checked onchange="_catChanged()"> H+R+RBI Over 1.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="HRR_U" checked onchange="_catChanged()"> H+R+RBI Under 1.5</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="HRR_SP" checked onchange="_catChanged()"> ⭐ HRR Special (Over)</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="TSC" checked onchange="_catChanged()"> 🔱 Triple Split Club</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="FSS" checked onchange="_catChanged()"> ⭐ 5 Star Split</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="BWALK_O" checked onchange="_catChanged()"> Batter Walks Over</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="BWALK_U" checked onchange="_catChanged()"> Batter Walks Under</label>
                <div class="parlay-cat-section">Pitchers</div>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="K_O" checked onchange="_catChanged()"> Ks Over</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="K_U" checked onchange="_catChanged()"> Ks Under</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="PHA_O" checked onchange="_catChanged()"> Hits Allowed Over</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="PHA_U" checked onchange="_catChanged()"> Hits Allowed Under</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="POUT_O" checked onchange="_catChanged()"> Outs Over</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="POUT_U" checked onchange="_catChanged()"> Outs Under</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="PER_O" checked onchange="_catChanged()"> Earned Runs Over</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="PER_U" checked onchange="_catChanged()"> Earned Runs Under</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="PWK_O" checked onchange="_catChanged()"> Walks Allowed Over</label>
                <label class="parlay-cat-row"><input type="checkbox" class="parlay-cat-cb" value="PWK_U" checked onchange="_catChanged()"> Walks Allowed Under</label>
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
      <div class="card p-6" id="player-search-card">
        <div class="section-hdr">🔍 Player Lookup</div>
        <p class="text-xs text-slate-400 mb-3">Type a hitter or pitcher's name — see where they rank and why.</p>
        <input id="player-search-input" type="text" placeholder="e.g. Aaron Judge, Gerrit Cole..."
               style="width:100%;padding:12px 16px;background:#0f0f0f;border:1px solid #262626;border-radius:10px;color:#fff;font-size:.95rem;outline:none"
               oninput="runPlayerSearch(this.value)">
        <div id="player-search-result" class="mt-3"></div>
      </div>
      <!-- SECTION: GAME PREDICTOR -->
      <div class="card p-6 hidden" id="game-pred-card" style="border-color:rgba(167,139,250,.35)">
        <div class="section-hdr" style="color:#a78bfa;font-size:1.05rem;margin-top:0">&#128302; Game Predictor &#8212; Today&#39;s Winners</div>
        <p class="text-xs text-slate-400 mb-3">Model picks each game&#39;s winner from the same signals that drive the props &#8212; lineup vs starter, bullpen, park, weather &amp; umpire. Tap a game for the full factor-by-factor breakdown.</p>
        <div id="game-pred-body"></div>
      </div>
      <!-- SECTION 1: HITTERS -->
        <div class="section-hdr" style="color:#facc15;font-size:1.05rem;margin-top:8px">⚾ HITTERS</div>
        <div class="card p-6 hidden" id="top10-plays-card" style="border-color:rgba(250,204,21,.35)">
          <div class="section-hdr" style="color:#facc15;margin:0">⭐ Top 10 Hitter Plays of the Day</div>
          <p class="text-xs text-slate-400 mb-3" style="margin-top:6px">All categories ranked by Expected Value (Wilson edge × odds). Recorded daily in Track Record. Click any card for recent history.</p>
          <div id="top10-plays-body" class="mlb-picks-grid"></div>
          <div id="top10-more-wrap"></div>
        </div>
        <div class="card p-6" id="top-picks-card">
          <div class="section-hdr">🏆 Top 10 Plays to Record a Hit</div>
          <div id="picks-body" class="mlb-picks-grid"></div>
          <div id="also-ran-wrap"></div>
        </div>
        <div class="card p-6 hidden" id="value-plays-card" style="border-color:rgba(34,211,238,.35)">
          <div class="section-hdr" style="color:#22d3ee">&#128142; Top 10 Value Plays of the Day</div>
          <p class="text-xs text-slate-400 mb-3" style="margin-top:-4px">Each top hitter&#39;s plus-money (+odds) value markets &mdash; RBI &middot; Total Bases &middot; Runs &middot; Walks &middot; H+R+RBI. Ranked by 3 standards: hot recent form, career vs the pitcher, and rate vs the opponent. Scored on the data we have; &ldquo;never faced&rdquo; means no career at-bats vs today&#39;s starter. Click any card for recent history.</p>
          <div id="value-plays-body" class="mlb-picks-grid"></div>
          <div id="value-more-wrap"></div>
        </div>
        <div class="card p-6 hidden" id="under-picks-card" style="border-color:rgba(255,107,107,.25)">
          <div class="section-hdr" style="color:#ff8a65">⬇️ Top 10 U1.5 Hits</div>
          <div id="under-picks-body" class="mlb-picks-grid"></div>
          <div id="under-more-wrap"></div>
        </div>
        <div class="card p-6 hidden" id="tb-over-picks-card" style="border-color:rgba(74,222,128,.25)">
          <div class="section-hdr" style="color:#4ade80">📈 Top 10 Over 1.5 Total Bases</div>
          <div id="tb-over-picks-body" class="mlb-picks-grid"></div>
          <div id="tb-over-more-wrap"></div>
        </div>
        <div class="card p-6 hidden" id="tb-picks-card" style="border-color:rgba(167,139,250,.25)">
          <div class="section-hdr" style="color:#a78bfa">⬇️ Top 10 Under 1.5 Total Bases</div>
          <div id="tb-picks-body" class="mlb-picks-grid"></div>
          <div id="tb-more-wrap"></div>
        </div>
        <div class="card p-6 hidden" id="hrr-special-card" style="border-color:rgba(167,139,250,.4)">
          <div class="section-hdr" style="color:#a78bfa">⭐ HRR Special — Parlay Confluence</div>
          <div style="font-size:.72rem;color:#94a3b8;margin:-4px 0 8px;line-height:1.6">All 4 must clear: BA &ge; .275 vs pitcher &middot; 65%+ vs team (H/A) &middot; 65%+ last-10 H/A &middot; BA &ge; .275 in today&#39;s day/night split</div>
          <div id="hrr-special-body" class="mlb-picks-grid"></div>
          <div id="hrr-special-more"></div>
        </div>
        <div class="card p-6 hidden" id="triple-split-card" style="border-color:rgba(34,211,238,.4)">
          <div class="section-hdr" style="color:#22d3ee">🔱 Triple Split Club</div>
          <div style="font-size:.72rem;color:#94a3b8;margin:-4px 0 8px;line-height:1.6">Hitters batting over .275 in ALL THREE of today&#39;s splits: Home/Away &middot; Day/Night &middot; Game of series. Bet: to record a hit.</div>
          <div id="triple-split-body" class="mlb-picks-grid"></div>
          <div id="triple-split-more"></div>
        </div>
        <div class="card p-6 hidden" id="five-star-card" style="border-color:rgba(167,139,250,.5)">
          <div class="section-hdr" style="color:#a78bfa">⭐ 5 Star Split</div>
          <div style="font-size:.72rem;color:#94a3b8;margin:-4px 0 8px;line-height:1.6">Triple Split hitters (over .275 in Home/Away, Day/Night &amp; series-game splits) who ALSO clear 60%+ games with a hit vs today&#39;s opponent AND 60%+ over their last 10 games. Each carries its single best production market by last-10 over-rate. Fully tracked. Click any card for recent history.</div>
          <div id="five-star-body" class="mlb-picks-grid"></div>
          <div id="five-star-more"></div>
        </div>
        <div class="card p-6 hidden" id="hrr-over-card" style="border-color:rgba(251,146,60,.25)">
          <div class="section-hdr" style="color:#fb923c">🔥 Top 10 Over 1.5 HRR</div>
          <div id="hrr-over-body" class="mlb-picks-grid"></div>
          <div id="hrr-over-more"></div>
        </div>
        <div class="card p-6 hidden" id="hrr-under-card" style="border-color:rgba(255,138,101,.25)">
          <div class="section-hdr" style="color:#ff8a65">🔥 Top 10 Under 1.5 HRR</div>
          <div id="hrr-under-body" class="mlb-picks-grid"></div>
          <div id="hrr-under-more"></div>
        </div>
        <div class="card p-6 hidden" id="rbi-over-card" style="border-color:rgba(245,158,11,.25)">
          <div class="section-hdr" style="color:#f59e0b">💥 Top 10 Over 0.5 RBI</div>
          <div id="rbi-over-body" class="mlb-picks-grid"></div>
          <div id="rbi-over-more"></div>
        </div>
        <div class="card p-6 hidden" id="rbi-under-card" style="border-color:rgba(255,138,101,.25)">
          <div class="section-hdr" style="color:#ff8a65">💥 Top 10 Under 0.5 RBI</div>
          <div id="rbi-under-body" class="mlb-picks-grid"></div>
          <div id="rbi-under-more"></div>
        </div>
        <div class="card p-6 hidden" id="hr-over-card" style="border-color:rgba(244,63,94,.25)">
          <div class="section-hdr" style="color:#f43f5e">💣 Top 10 Over 0.5 HR</div>
          <div id="hr-over-body" class="mlb-picks-grid"></div>
          <div id="hr-over-more"></div>
        </div>
        <div class="card p-6 hidden" id="hr-under-card" style="border-color:rgba(255,138,101,.25)">
          <div class="section-hdr" style="color:#ff8a65">💣 Top 10 Under 0.5 HR</div>
          <div id="hr-under-body" class="mlb-picks-grid"></div>
          <div id="hr-under-more"></div>
        </div>
        <div class="card p-6 hidden" id="runs-over-card" style="border-color:rgba(96,165,250,.25)">
          <div class="section-hdr" style="color:#60a5fa">🏃 Top 10 Over 0.5 Run</div>
          <div id="runs-over-body" class="mlb-picks-grid"></div>
          <div id="runs-over-more"></div>
        </div>
        <div class="card p-6 hidden" id="runs-under-card" style="border-color:rgba(255,138,101,.25)">
          <div class="section-hdr" style="color:#ff8a65">🏃 Top 10 Under 0.5 Run</div>
          <div id="runs-under-body" class="mlb-picks-grid"></div>
          <div id="runs-under-more"></div>
        </div>
        <div class="card p-6 hidden" id="bwalk-over-card" style="border-color:rgba(52,211,153,.25)">
          <div class="section-hdr" style="color:#34d399">🚶 Top 10 Over 0.5 Walks</div>
          <div id="bwalk-over-body" class="mlb-picks-grid"></div>
          <div id="bwalk-over-more"></div>
        </div>
        <div class="card p-6 hidden" id="bwalk-under-card" style="border-color:rgba(255,138,101,.25)">
          <div class="section-hdr" style="color:#ff8a65">🚶 Top 10 Under 0.5 Walks</div>
          <div id="bwalk-under-body" class="mlb-picks-grid"></div>
          <div id="bwalk-under-more"></div>
        </div>
        <!-- SECTION 2: PITCHING -->
        <div class="section-hdr" style="color:#63cab7;font-size:1.05rem;margin-top:8px">⚾ PITCHING</div>
        <div class="card p-6 hidden" id="pitch-day-card" style="border-color:rgba(99,202,183,.35)">
          <div class="section-hdr" style="color:#63cab7">🎯 Top 10 Pitching Props of the Day</div>
          <p class="text-xs text-slate-400 mb-3" style="margin-top:-4px">Best pitcher plays across all markets (K · Hits · Outs · ER · Walks) ranked by Expected Value. Click any card for recent history.</p>
          <div id="pitch-day-body" class="mlb-picks-grid"></div>
          <div id="pitch-day-more"></div>
        </div>
        <div class="card p-6 hidden" id="pitcher-all-card" style="border-color:rgba(99,202,183,.25)">
          <div class="section-hdr" style="color:#63cab7">⚾ All Pitcher Cards</div>
          <p class="text-xs text-slate-400 mb-3" style="margin-top:-4px">Every starter today. Click any pitcher for all 5 markets (K · Hits · Outs · ER · Walks).</p>
          <div id="pitcher-all-body" class="mlb-picks-grid"></div>
          <div id="pitcher-all-more"></div>
        </div>
        <div class="card p-6 hidden" id="k-over-card" style="border-color:rgba(99,202,183,.25)">
          <div class="section-hdr" style="color:#63cab7">⚡ Top Strikeout Overs</div>
          <div id="pitcher-k-over-body" class="mlb-picks-grid"></div>
          <div id="pitcher-k-over-more"></div>
        </div>
        <div class="card p-6 hidden" id="k-under-card" style="border-color:rgba(255,138,101,.25)">
          <div class="section-hdr" style="color:#ff8a65">⬇ Top Strikeout Unders</div>
          <div id="pitcher-k-under-body" class="mlb-picks-grid"></div>
          <div id="pitcher-k-under-more"></div>
        </div>
        <div class="card p-6 hidden" id="prop-ha-over-card" style="border-color:rgba(248,113,113,.25)">
          <div class="section-hdr" style="color:#f87171">🎯 Top 10 Over Hits Allowed</div>
          <div id="prop-ha-over-body" class="mlb-picks-grid"></div>
          <div id="prop-ha-over-more"></div>
        </div>
        <div class="card p-6 hidden" id="prop-ha-under-card" style="border-color:rgba(255,138,101,.25)">
          <div class="section-hdr" style="color:#ff8a65">🎯 Top 10 Under Hits Allowed</div>
          <div id="prop-ha-under-body" class="mlb-picks-grid"></div>
          <div id="prop-ha-under-more"></div>
        </div>
        <div class="card p-6 hidden" id="prop-outs-over-card" style="border-color:rgba(167,139,250,.25)">
          <div class="section-hdr" style="color:#a78bfa">🔢 Top 10 Over Outs</div>
          <div id="prop-outs-over-body" class="mlb-picks-grid"></div>
          <div id="prop-outs-over-more"></div>
        </div>
        <div class="card p-6 hidden" id="prop-outs-under-card" style="border-color:rgba(255,138,101,.25)">
          <div class="section-hdr" style="color:#ff8a65">🔢 Top 10 Under Outs</div>
          <div id="prop-outs-under-body" class="mlb-picks-grid"></div>
          <div id="prop-outs-under-more"></div>
        </div>
        <div class="card p-6 hidden" id="prop-er-over-card" style="border-color:rgba(251,146,60,.25)">
          <div class="section-hdr" style="color:#fb923c">🔥 Top 10 Over Earned Runs</div>
          <div id="prop-er-over-body" class="mlb-picks-grid"></div>
          <div id="prop-er-over-more"></div>
        </div>
        <div class="card p-6 hidden" id="prop-er-under-card" style="border-color:rgba(255,138,101,.25)">
          <div class="section-hdr" style="color:#ff8a65">🔥 Top 10 Under Earned Runs</div>
          <div id="prop-er-under-body" class="mlb-picks-grid"></div>
          <div id="prop-er-under-more"></div>
        </div>
        <div class="card p-6 hidden" id="prop-bb-over-card" style="border-color:rgba(52,211,153,.25)">
          <div class="section-hdr" style="color:#34d399">🚶 Top 10 Over Walks Allowed</div>
          <div id="prop-bb-over-body" class="mlb-picks-grid"></div>
          <div id="prop-bb-over-more"></div>
        </div>
        <div class="card p-6 hidden" id="prop-bb-under-card" style="border-color:rgba(255,138,101,.25)">
          <div class="section-hdr" style="color:#ff8a65">🚶 Top 10 Under Walks Allowed</div>
          <div id="prop-bb-under-body" class="mlb-picks-grid"></div>
          <div id="prop-bb-under-more"></div>
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
      .then(d => {
        if (d && d.is_admin)  { window.IS_ADMIN = true; document.body.classList.add('is-admin'); }
        else if (d && d.is_tester) { window.IS_TESTER = true; document.body.classList.add('is-tester'); }
      })
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
    checkLockStatus();
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

// ── Lock Picks ─────────────────────────────────────────────────────────
// Saves the current picks as the permanent Track Record snapshot for today.
// Once locked, re-runs update the live display but grading always uses the lock.
async function checkLockStatus() {
  if (!window.IS_ADMIN) return;
  const dateStr = document.getElementById('date-picker').value;
  if (!dateStr) return;
  try {
    const r = await fetch(`/api/lock-status?date=${dateStr}&token=${encodeURIComponent(token)}`);
    if (!r.ok) return;
    const d = await r.json();
    const badge = document.getElementById('lock-status-badge');
    const btn = document.getElementById('lock-btn');
    const ubtn = document.getElementById('unlock-btn');
    if (!badge) return;
    badge.style.display = 'inline-block';
    if (d.locked) {
      badge.textContent = '🔒 Picks Locked';
      badge.style.background = '#16a34a'; badge.style.color = '#fff';
      if (btn) { btn.disabled = true; btn.style.opacity = '0.5'; btn.title = 'Already locked for this date'; }
      if (ubtn) ubtn.style.display = 'inline-block';
    } else {
      badge.textContent = '🔓 Not Locked';
      badge.style.background = '#374151'; badge.style.color = '#9ca3af';
      if (btn) { btn.disabled = false; btn.style.opacity = '1'; }
      if (ubtn) ubtn.style.display = 'none';
    }
  } catch(e) {}
}
async function unlockPicks() {
  if (!window.IS_ADMIN) return;
  const dateStr = document.getElementById('date-picker').value;
  if (!dateStr) { alert('Select a date first.'); return; }
  if (!confirm('Remove the lock for ' + dateStr + '? Re-runs will update Track Record again.')) return;
  const ubtn = document.getElementById('unlock-btn');
  if (ubtn) { ubtn.disabled = true; ubtn.textContent = 'Unlocking...'; }
  try {
    const r = await fetch(`/api/unlock-picks?date=${dateStr}&token=${encodeURIComponent(token)}`, { method: 'DELETE' });
    const d = await r.json();
    if (d.ok) {
      alert('Unlocked! ' + dateStr + ' picks are no longer locked.');
      checkLockStatus();
    } else {
      alert(d.msg || 'Unlock failed.');
      if (ubtn) { ubtn.disabled = false; ubtn.textContent = '🔓 Unlock Picks'; }
    }
  } catch(e) {
    alert('Unlock request failed: ' + e.message);
    if (ubtn) { ubtn.disabled = false; ubtn.textContent = '🔓 Unlock Picks'; }
  }
}
async function lockPicks() {
  if (!window.IS_ADMIN) return;
  const dateStr = document.getElementById('date-picker').value;
  if (!dateStr) { alert('Select a date first.'); return; }
  if (!confirm('Lock these picks as the Track Record for ' + dateStr + '? This cannot be undone. Re-runs after locking will update the live display only.')) return;
  const btn = document.getElementById('lock-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'Locking...'; }
  try {
    const r = await fetch(`/api/lock-picks?date=${dateStr}&token=${encodeURIComponent(token)}`, { method: 'POST' });
    const d = await r.json();
    if (d.ok) {
      alert('Locked! These picks are now the Track Record for ' + dateStr + '.');
      checkLockStatus();
    } else {
      alert(d.msg || 'Lock failed.');
      if (btn) { btn.disabled = false; btn.textContent = '🔒 Lock Picks'; }
    }
  } catch(e) {
    alert('Lock request failed: ' + e.message);
    if (btn) { btn.disabled = false; btn.textContent = '🔒 Lock Picks'; }
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
  r.top9=f(r.top9); r.also_ran=f(r.also_ran); r.under_picks=f(r.under_picks); r.runs_picks=f(r.runs_picks); r.tb_picks=f(r.tb_picks); r.tb_over_picks=f(r.tb_over_picks||[]); r.hrr_picks=f(r.hrr_picks||[]); r.hrr_special_picks=f(r.hrr_special_picks||[]); r.triple_split_picks=f(r.triple_split_picks||[]); r.five_star_split_picks=f(r.five_star_split_picks||[]); r.rbi_picks=f(r.rbi_picks||[]); r.hr_picks=f(r.hr_picks||[]); r.walks_picks=f(r.walks_picks||[]);
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
// ── Game Predictor ──────────────────────────────────────────────────────────
// Renders the per-game team win model (result.game_predictions) as its own
// section: one card per game (win% bars + top-3 drivers), tap for the full
// factor-by-factor breakdown + verdict. Reads the raw predictions (never the
// odds/EV-filtered view) so it always shows the whole slate.
function _gpConfClr(c){ return ({STRONG:'#7c3aed',MODERATE:'#2563eb',LEAN:'#64748b'})[c]||'#64748b'; }
function _gpCard(g,i){
  var cc=_gpConfClr(g.conf);
  function teamRow(abbr,sp,proj,win,isPick){
    var barClr=isPick?'#a78bfa':'#334155';
    return '<div style="display:flex;align-items:center;gap:8px;padding:5px 0">'
      +'<div style="width:44px;font-weight:900;color:'+(isPick?'#e9d5ff':'#cbd5e1')+';font-size:.9rem">'+_esc(abbr)+'</div>'
      +'<div style="flex:1;min-width:0"><div style="height:8px;background:#0f172a;border-radius:5px;overflow:hidden"><div style="height:100%;width:'+win+'%;background:'+barClr+'"></div></div>'
      +'<div style="font-size:.6rem;color:#64748b;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+_esc(sp||'TBD')+'</div></div>'
      +'<div style="width:32px;text-align:right;font-weight:800;color:#e2e8f0;font-size:.82rem">'+g_gpFix(proj)+'</div>'
      +'<div style="width:42px;text-align:right;font-weight:900;color:'+(isPick?'#4ade80':'#94a3b8')+';font-size:.82rem">'+win+'%</div>'
      +'</div>';
  }
  var drivers=(g.drivers||[]).map(function(d){return _esc(d);}).join(' &#183; ');
  var vb=g.value_flag?('<span style="background:#166534;color:#fff;font-weight:900;font-size:.62rem;border-radius:6px;padding:2px 7px;letter-spacing:.04em">VALUE +'+g.mkt_edge+'%</span>'):'';
  return '<div onclick="_openGamePred('+i+')" style="background:#0a1120;border:1px solid '+(g.value_flag?'#166534':'#1e293b')+';border-radius:14px;padding:13px 15px;cursor:pointer" onmouseover="this.style.borderColor=&#39;#3b2c63&#39;" onmouseout="this.style.borderColor=&#39;'+(g.value_flag?'#166534':'#1e293b')+'&#39;">'
    +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:7px">'
    +'<div style="font-weight:800;color:#94a3b8;font-size:.72rem;letter-spacing:.04em">'+_esc(g.away_abbr)+' @ '+_esc(g.home_abbr)+'</div>'
    +'<div style="display:flex;gap:6px;align-items:center">'
    +vb
    +'<span style="background:'+cc+';color:#fff;font-weight:900;font-size:.62rem;border-radius:6px;padding:2px 7px;letter-spacing:.04em">'+_esc(g.conf)+'</span>'
    +'<span style="background:rgba(167,139,250,.15);color:#c4b5fd;font-weight:900;font-size:.68rem;border-radius:6px;padding:2px 8px">PICK '+_esc(g.pick_abbr)+'</span>'
    +'</div></div>'
    +teamRow(g.away_abbr,g.away_sp,g.proj_away,g.win_away,!g.pick_home)
    +teamRow(g.home_abbr,g.home_sp,g.proj_home,g.win_home,g.pick_home)
    +_gpTotalRow(g)
    +_gpMktRow(g)
    +'<div style="margin-top:6px;font-size:.66rem;color:#94a3b8;line-height:1.5"><span style="color:#7c3aed;font-weight:800">Why:</span> '+drivers+'</div>'
    +'</div>';
}
function _gpMktRow(g){
  if(g.mkt_edge==null) return '';
  var mp=(g.pick_home?g.mkt_home_pct:g.mkt_away_pct), md=(g.pick_home?g.win_home:g.win_away);
  var col=(g.mkt_edge>0?'#166534':(g.mkt_edge<0?'#7f1d1d':'#334155')), sign=(g.mkt_edge>0?'+':'');
  return '<div style="margin-top:6px;padding-top:6px;border-top:1px solid #111c2e;display:flex;align-items:center;justify-content:space-between">'
    +'<span style="font-size:.66rem;color:#64748b;font-weight:700">MARKET '+_esc(g.pick_abbr)+' <span style="color:#cbd5e1">'+mp+'%</span> vs model '+md+'%</span>'
    +'<span style="background:'+col+';color:#fff;font-weight:900;font-size:.62rem;border-radius:6px;padding:2px 8px">EDGE '+sign+g.mkt_edge+'%</span>'
    +'</div>';
}
function g_gpFix(v){ return (v==null||v==='')?'&#8212;':(Math.round(Number(v)*10)/10).toFixed(1); }
function _gpTotalRow(g){
  var base='margin-top:8px;padding-top:7px;border-top:1px solid #111c2e';
  if(g.total_line==null){
    return '<div style="'+base+';font-size:.66rem;color:#64748b">RUN TOTAL <span style="color:#cbd5e1;font-weight:800">'+g_gpFix(g.proj_total)+'</span> proj &#183; no line posted</div>';
  }
  var ov=g.total_pick==='OVER', ec=(g.total_edge>0?'+':'')+g_gpFix(g.total_edge);
  return '<div style="'+base+';display:flex;align-items:center;justify-content:space-between">'
    +'<span style="font-size:.66rem;color:#64748b;font-weight:700">RUN TOTAL <span style="color:#cbd5e1">'+g_gpFix(g.proj_total)+'</span> vs line '+g_gpFix(g.total_line)+'</span>'
    +'<span style="background:'+(ov?'#166534':'#7f1d1d')+';color:#fff;font-weight:900;font-size:.62rem;border-radius:6px;padding:2px 8px">'+g.total_pick+' '+ec+'</span>'
    +'</div>';
}
function _renderGamePredictor(result){
  var body=document.getElementById('game-pred-body'), card=document.getElementById('game-pred-card');
  if(!body||!card) return;
  var gp=(result&&result.game_predictions)||[];
  if(!gp.length){ card.classList.add('hidden'); body.innerHTML=''; return; }
  window.__GAME_PRED__=gp;
  var html='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px">';
  for(var i=0;i<gp.length;i++) html+=_gpCard(gp[i],i);
  html+='</div>';
  body.innerHTML=html;
  card.classList.remove('hidden');
}
function _openGamePred(i){
  var g=(window.__GAME_PRED__||[])[i]; if(!g) return;
  var cc=_gpConfClr(g.conf);
  function gpBig(abbr,proj,win,isPick){
    return '<div style="flex:1;text-align:center;background:'+(isPick?'rgba(167,139,250,.12)':'#0a1120')+';border:1px solid '+(isPick?'#4c3a7a':'#1e293b')+';border-radius:12px;padding:12px 8px">'
      +'<div style="font-weight:900;color:'+(isPick?'#e9d5ff':'#cbd5e1')+';font-size:1.05rem">'+_esc(abbr)+'</div>'
      +'<div style="font-weight:900;color:'+(isPick?'#4ade80':'#94a3b8')+';font-size:1.5rem;margin:2px 0">'+win+'%</div>'
      +'<div style="color:#64748b;font-size:.7rem">proj '+g_gpFix(proj)+' R</div></div>';
  }
  var rows='';
  (g.factors||[]).forEach(function(f,idx){
    var eA=f.edge===g.away_abbr, eH=f.edge===g.home_abbr;
    rows+='<div style="display:grid;grid-template-columns:1fr 100px 100px;gap:0;padding:8px 12px;border-bottom:1px solid #0f172a;background:'+(idx%2?'#070e1b':'#050c18')+'">'
      +'<div style="color:#cbd5e1;font-size:.76rem;font-weight:700;align-self:center">'+_esc(f.name)+'</div>'
      +'<div style="text-align:right;font-size:.72rem;align-self:center;color:'+(eA?'#4ade80':'#94a3b8')+';font-weight:'+(eA?'800':'500')+'">'+_esc(f.away)+(eA?' &#9664;':'')+'</div>'
      +'<div style="text-align:right;font-size:.72rem;align-self:center;color:'+(eH?'#4ade80':'#94a3b8')+';font-weight:'+(eH?'800':'500')+'">'+_esc(f.home)+(eH?' &#9664;':'')+'</div>'
      +'</div>';
  });
  var hdrCols='<div style="display:grid;grid-template-columns:1fr 100px 100px;gap:0;padding:6px 12px;border-bottom:1px solid #1e293b;font-size:.62rem;color:#475569;font-weight:800;letter-spacing:.05em"><span>FACTOR</span><span style="text-align:right">'+_esc(g.away_abbr)+'</span><span style="text-align:right">'+_esc(g.home_abbr)+'</span></div>';
  var ov=document.getElementById('game-pred-modal');
  if(!ov){ ov=document.createElement('div'); ov.id='game-pred-modal'; ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.85);z-index:10000;display:flex;align-items:center;justify-content:center;padding:16px'; ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; }; document.body.appendChild(ov); }
  ov.innerHTML='<div style="background:#080f1e;border:1px solid #3b2c63;border-radius:18px;width:100%;max-width:520px;max-height:90vh;display:flex;flex-direction:column;box-shadow:0 24px 80px rgba(0,0,0,.7)" onclick="event.stopPropagation()">'
    +'<div style="display:flex;align-items:center;justify-content:space-between;padding:16px 20px;border-bottom:1px solid #1e293b;flex-shrink:0">'
    +'<div><div style="font-weight:900;color:#a78bfa;font-size:1.05rem">&#128302; '+_esc(g.away_abbr)+' @ '+_esc(g.home_abbr)+'</div>'
    +'<div style="color:#64748b;font-size:.72rem;margin-top:2px">'+_esc(g.away_sp||'TBD')+' vs '+_esc(g.home_sp||'TBD')+'</div></div>'
    +'<button onclick="document.getElementById(&#39;game-pred-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:32px;height:32px;border-radius:8px;cursor:pointer;font-size:1.1rem;flex-shrink:0">&#215;</button>'
    +'</div>'
    +'<div style="overflow-y:auto;flex:1">'
    +'<div style="padding:16px 20px;display:flex;gap:12px;align-items:stretch">'
    + gpBig(g.away_abbr,g.proj_away,g.win_away,!g.pick_home)
    + '<div style="align-self:center;color:#475569;font-weight:800">vs</div>'
    + gpBig(g.home_abbr,g.proj_home,g.win_home,g.pick_home)
    +'</div>'
    +'<div style="padding:0 20px 10px"><span style="background:'+cc+';color:#fff;font-weight:900;font-size:.66rem;border-radius:6px;padding:3px 9px">'+_esc(g.conf)+'</span> <span style="color:#94a3b8;font-size:.74rem;margin-left:6px">edge '+g_gpFix(g.edge_runs)+' runs</span></div>'
    +'<div style="padding:0 20px 12px"><div style="background:#0a1120;border:1px solid #1e293b;border-radius:10px;padding:10px 12px;display:flex;align-items:center;justify-content:space-between">'
      +'<div><div style="font-size:.62rem;color:#475569;font-weight:800;letter-spacing:.05em">RUN TOTAL O/U</div>'
      +'<div style="color:#e2e8f0;font-size:.8rem;margin-top:2px">proj <b>'+g_gpFix(g.proj_total)+'</b>'+(g.total_line!=null?(' &#183; book line <b>'+g_gpFix(g.total_line)+'</b>'):' &#183; no line posted')+'</div></div>'
      +(g.total_line!=null?('<span style="background:'+(g.total_pick==='OVER'?'#166534':'#7f1d1d')+';color:#fff;font-weight:900;font-size:.74rem;border-radius:8px;padding:4px 11px">'+g.total_pick+' '+(g.total_edge>0?'+':'')+g_gpFix(g.total_edge)+'</span>'):'')
      +'</div></div>'
    +(g.mkt_edge!=null?('<div style="padding:0 20px 12px"><div style="background:#0a1120;border:1px solid '+(g.value_flag?'#166534':'#1e293b')+';border-radius:10px;padding:10px 12px;display:flex;align-items:center;justify-content:space-between">'
      +'<div><div style="font-size:.62rem;color:#475569;font-weight:800;letter-spacing:.05em">MARKET vs MODEL</div>'
      +'<div style="color:#e2e8f0;font-size:.8rem;margin-top:2px">model <b>'+_esc(g.pick_abbr)+' '+(g.pick_home?g.win_home:g.win_away)+'%</b> &#183; market <b>'+(g.pick_home?g.mkt_home_pct:g.mkt_away_pct)+'%</b></div></div>'
      +'<span style="background:'+(g.mkt_edge>0?'#166534':(g.mkt_edge<0?'#7f1d1d':'#334155'))+';color:#fff;font-weight:900;font-size:.74rem;border-radius:8px;padding:4px 11px">'+(g.value_flag?'VALUE ':'EDGE ')+(g.mkt_edge>0?'+':'')+g.mkt_edge+'%</span>'
      +'</div></div>'):'')
    +hdrCols+rows
    +'<div style="padding:14px 20px;color:#cbd5e1;font-size:.78rem;line-height:1.6"><span style="color:#a78bfa;font-weight:800">Verdict &#183; </span>'+_esc(g.verdict)+'</div>'
    +'</div></div>';
  ov.style.display='flex';
}
function showResults(result) {
  result = _filterStarted(result);
  window._lastResult = result;
  if(typeof _renderLeanBanner==='function') _renderLeanBanner();
  if(typeof _renderGamePredictor==='function') _renderGamePredictor(result);
  // Hide all section cards FIRST — before any filtering — so stale cards from a
  // previous render can never persist if the filter or any later code throws.
  ['top10-plays-card','value-plays-card','under-picks-card','tb-picks-card','tb-over-picks-card','hrr-special-card','triple-split-card','five-star-card','hrr-over-card','hrr-under-card','rbi-over-card','rbi-under-card','hr-over-card','hr-under-card','runs-over-card','runs-under-card','bwalk-over-card','bwalk-under-card','pitch-day-card','pitcher-all-card','k-over-card','k-under-card','prop-ha-over-card','prop-ha-under-card','prop-outs-over-card','prop-outs-under-card','prop-er-over-card','prop-er-under-card','prop-bb-over-card','prop-bb-under-card'].forEach(hide);
  // Odds-range filter: self-contained, uses the EXACT field each card displays.
  // Applied directly to the source data before _vBase / EV-filter so every
  // category is covered and there is nothing to guess or chain.
  var _renderSrc = result;
  if(window.ODDS_RANGE){
    var _rok=function(v){
      if(v==null||v==='') return false;
      var o=parseFloat(v); if(isNaN(o)) return false;
      var r=window.ODDS_RANGE;
      if(r==='le-500')      return o<=-500;
      if(r==='-500to-450')  return o>-500  && o<=-450;
      if(r==='-450to-400')  return o>-450  && o<=-400;
      if(r==='-400to-350')  return o>-400  && o<=-350;
      if(r==='-350to-300')  return o>-350  && o<=-300;
      if(r==='-300to-250')  return o>-300  && o<=-250;
      if(r==='-250to-200')  return o>-250  && o<=-200;
      if(r==='-200to-150')  return o>-200  && o<=-150;
      if(r==='-150to-100')  return o>-150  && o<=-100;
      if(r==='+100to+150')  return o>=100  && o<=150;
      if(r==='+150to+200')  return o>150   && o<=200;
      if(r==='+200to+250')  return o>200   && o<=250;
      if(r==='+250to+300')  return o>250   && o<=300;
      if(r==='ge+300')      return o>=300;
      return true;
    };
    var _rpk=result.pitcher_k;
    var _rpp=result.pitcher_props||{};
    _renderSrc=Object.assign({},result,{
      top9:        (result.top9||[]).filter(function(p){return _rok(p.hit_odds);}),
      also_ran:    (result.also_ran||[]).filter(function(p){return _rok(p.hit_odds);}),
      under_picks: (result.under_picks||[]).filter(function(p){return _rok(p.under_odds);}),
      tb_picks:    (result.tb_picks||[]).filter(function(p){return _rok(p.tb_under_odds);}),
      tb_over_picks:(result.tb_over_picks||[]).filter(function(p){return _rok(p.tb_over_odds);}),
      rbi_picks:   (result.rbi_picks||[]).filter(function(p){return p.pick==='OVER'?_rok(p.over_odds):_rok(p.under_odds);}),
      hr_picks:    (result.hr_picks||[]).filter(function(p){return p.pick==='OVER'?_rok(p.over_odds):_rok(p.under_odds);}),
      runs_picks:  (result.runs_picks||[]).filter(function(p){return p.pick==='OVER'?_rok(p.over_odds):_rok(p.under_odds);}),
      walks_picks: (result.walks_picks||[]).filter(function(p){return p.pick==='OVER'?_rok(p.over_odds):_rok(p.under_odds);}),
      hrr_picks:   (result.hrr_picks||[]).filter(function(p){return p.pick==='UNDER'?_rok(p.hrr_under_odds):_rok(p.hrr_over_odds);}),
      hrr_special_picks: (result.hrr_special_picks||[]).filter(function(p){return _rok(p.hrr_over_odds);}),
      triple_split_picks: (result.triple_split_picks||[]).filter(function(p){return _rok(p.hit_odds);}),
      pitcher_k:   _rpk?Object.assign({},_rpk,{picks:(_rpk.picks||[]).filter(function(p){return _rok(p.odds);}),all:(_rpk.all||[]).filter(function(p){return _rok(p.odds);})}):_rpk,
      pitcher_props:(function(){var out={};Object.keys(_rpp).forEach(function(m){var b=_rpp[m]||{};out[m]={picks:(b.picks||[]).filter(function(p){return _rok(p.odds);}),all:(b.all||[]).filter(function(p){return _rok(p.odds);})}; });return out;})(),
    });
  }
  // Admin-only "Unders Only" view — applied on top of the odds-filtered source.
  const _vBase = (window.UNDERS_ONLY && (window.IS_ADMIN||window.IS_TESTER))
    ? Object.assign({}, _renderSrc, {
        top9: [],
        also_ran: [],
        hrr_special_picks: [],
        triple_split_picks: [],
        five_star_split_picks: [],
        pitcher_k: _renderSrc.pitcher_k ? Object.assign({}, _renderSrc.pitcher_k, {
          all: (_renderSrc.pitcher_k.all || []).filter(p => p.pick === 'UNDER'),
          picks: (_renderSrc.pitcher_k.picks || []).filter(p => p.pick === 'UNDER'),
        }) : _renderSrc.pitcher_k,
        runs_picks: (_renderSrc.runs_picks || []).filter(p => p.pick === 'UNDER'),
        walks_picks: (_renderSrc.walks_picks || []).filter(p => p.pick === 'UNDER'),
        pitcher_props: (function(){
          var src=_renderSrc.pitcher_props||{}, out={};
          Object.keys(src).forEach(function(m){
            var b=src[m]||{};
            out[m]={picks:(b.picks||[]).filter(p=>p.pick==='UNDER'),
                    all:(b.all||[]).filter(p=>p.pick==='UNDER')};
          });
          return out;
        })(),
      })
    : _renderSrc;
  // "+EV Only" filter on top; odds filter already applied above.
  var view = window.EV_ONLY ? _evFilterView(_vBase) : _vBase;
  const { top9, stats, pitcher_k } = view;

  document.getElementById('stats-row').innerHTML = _renderCatBar(view);
  if(!window.__CATMENU_DOC__){ window.__CATMENU_DOC__=true; document.addEventListener('click',function(e){ if(!(e.target.closest&&e.target.closest('.catmenu-wrap'))) _catClose(); }); }

  // Top 10 Hitter Plays — all categories ranked by Wilson-EV (Current model only).
  window.__T10_CUR__ = _buildTop10All(view);
  var _t10Has = ((window.__T10_CUR__||[]).length > 0);
  if (_t10Has && !(window.UNDERS_ONLY && (window.IS_ADMIN||window.IS_TESTER))) {
    show('top10-plays-card');
    _renderT10Section();
  }

  if (window.UNDERS_ONLY && (window.IS_ADMIN||window.IS_TESTER)) { hide('top-picks-card'); hide('top10-plays-card'); } else { show('top-picks-card'); }
  window.__HIT_REG__={};
  // Value re-rank: merge Top Picks + More Hit Picks, order by EV (default keeps
  // ALL plays), then re-split 10 / rest. "+EV Only" toggle filters to ev>0.
  var _hitAll=_evSortFilter((top9||[]).concat(view.also_ran||[]));
  // One card per player: the same hitter can slip into the pool twice (e.g. two
  // lineup-slot / facing-pitcher rows). Keep the first (best-ranked) and drop the
  // rest so nobody appears twice; slice(0,10) then backfills the freed slot.
  (function(){ var seen={}, dd=[]; _hitAll.forEach(function(p){ var k=String((p&&(p.player_id!=null?p.player_id:(p.full_name||p.name)))||''); if(k&&seen[k]) return; if(k) seen[k]=1; dd.push(p); }); _hitAll=dd; })();
  var _hitTop=_hitAll.slice(0,10), _hitMore=_hitAll.slice(10);
  document.getElementById('picks-body').innerHTML = _hitTop.length>0
    ? _hitTop.map((p,i) => _mlbCard(p, i+1)).join('')
    : '<p class="text-slate-500 text-center" style="padding:16px">No hit picks'+(window.EV_ONLY?' with a positive edge':'')+' today</p>';
  document.getElementById('also-ran-wrap').innerHTML = _hitMore.length > 0
    ? _moreWrap(_hitMore, function(p,r){ return _mlbCard(p, r, true); }, 11, 'More Hit Picks', '#f59e0b')
    : '';

  // SECTION 2 — Value Plays board: each top hitter's plus-money (+odds) value
  // markets, ranked by the 3 partial standards (hot / vs pitcher / vs team).
  // Built off the full slate so it is stable regardless of the +EV / odds-range
  // toolbar. Hidden in admin "Unders Only" mode (the board is all overs).
  var _valAll = _buildValuePlays(result);
  if (_valAll.length && !(window.UNDERS_ONLY && (window.IS_ADMIN||window.IS_TESTER))) {
    show('value-plays-card');
    window.__VAL_REG__={};
    document.getElementById('value-plays-body').innerHTML = _valAll.slice(0,10).map(function(p,i){ return _valueCard(p, i+1); }).join('');
    document.getElementById('value-more-wrap').innerHTML = _valAll.length>10
      ? _moreWrap(_valAll.slice(10,20), function(p,r){ return _valueCard(p, r); }, 11, 'Value Plays', '#22d3ee')
      : '';
  } else { hide('value-plays-card'); }

  const underPicks = (view.under_picks || []).filter(function(p){ return _oddsOk(p.under_odds); });
  if (underPicks.length > 0) {
    show('under-picks-card');
    document.getElementById('under-picks-body').innerHTML = underPicks.slice(0, 10).map((p,i) => _underCard(p, i+1)).join('');
    document.getElementById('under-more-wrap').innerHTML = underPicks.length > 10
      ? _moreWrap(underPicks.slice(10), function(p,r){ return _underCard(p, r); }, 11, 'Under Picks', '#ff8a65')
      : '';
  }

  const pkData=view.pitcher_k||{}, pkAll=pkData.all||[];
    window.__TEAM_K_RANKS__=(pkData.team_k_ranks||[]);
    window.__PK_REG__={};
    window.__PK_BY_NAME__={};
    (pkAll||[]).forEach(function(_p){
      var _nm=String(_p.name||'').toLowerCase().trim(); if(!_nm) return;
      var _ex=window.__PK_BY_NAME__[_nm];
      if(!_ex||(!_ex.pick&&_p.pick)) window.__PK_BY_NAME__[_nm]=_p;
    });
    if (pkAll.length > 0) {
      const pkSorted = pkAll.filter(p=>p.pick).sort((a,b)=>{
        const ga=Math.abs((a.blended_avg_k!=null?a.blended_avg_k:(a.avg_k||0))-(a.line||0))*_umpKMul(a);
        const gb=Math.abs((b.blended_avg_k!=null?b.blended_avg_k:(b.avg_k||0))-(b.line||0))*_umpKMul(b);
        return gb-ga;
      });
      const pkOvers=pkSorted.filter(p=>p.pick==='OVER');
      const pkUnders=pkSorted.filter(p=>p.pick==='UNDER');
      _fillCard('k-over-card','pitcher-k-over-body','pitcher-k-over-more',pkOvers,function(p,r){return _pitcherCard(p,r,'pk');},'Strikeout Overs','#63cab7');
      _fillCard('k-under-card','pitcher-k-under-body','pitcher-k-under-more',pkUnders,function(p,r){return _pitcherCard(p,r,'pu');},'Strikeout Unders','#ff8a65');
      const pkAllSorted=[...pkAll].sort((a,b)=>{
        const ka=a.blended_avg_k!=null?a.blended_avg_k:(a.avg_k||0);
        const kb=b.blended_avg_k!=null?b.blended_avg_k:(b.avg_k||0);
        return kb-ka;
      });
      _fillCard('pitcher-all-card','pitcher-all-body','pitcher-all-more',pkAllSorted,function(p,r){return _pitcherCard(p,r,'pa');},'pitchers','#63cab7');
    } else {
      hide('k-over-card'); hide('k-under-card'); hide('pitcher-all-card');
    }

  window.__RBI_REG__={};
    const rbiPicks = view.rbi_picks || [];
    const rbiOver = rbiPicks.filter(function(p){ return p.pick==='OVER' && _oddsOk(p.over_odds); });
    const rbiUnder = rbiPicks.filter(function(p){ return p.pick==='UNDER' && _oddsOk(p.under_odds); });
    _fillCard('rbi-over-card','rbi-over-body','rbi-over-more',rbiOver,function(p,r){return _rbiCard(p,r,'rbo');},'RBI Over','#f59e0b');
    _fillCard('rbi-under-card','rbi-under-body','rbi-under-more',rbiUnder,function(p,r){return _rbiCard(p,r,'rbu');},'RBI Under','#ff8a65');

  window.__HR_REG__={};
    const hrPicks = view.hr_picks || [];
    const hrOver = hrPicks.filter(function(p){ return p.pick==='OVER' && _oddsOk(p.over_odds); });
    const hrUnder = hrPicks.filter(function(p){ return p.pick==='UNDER' && _oddsOk(p.under_odds); });
    _fillCard('hr-over-card','hr-over-body','hr-over-more',hrOver,function(p,r){return _hrCard(p,r,'hro');},'HR Over','#f43f5e');
    _fillCard('hr-under-card','hr-under-body','hr-under-more',hrUnder,function(p,r){return _hrCard(p,r,'hru');},'HR Under','#ff8a65');

  window.__RUNS_REG__={};
    const runsPicks = view.runs_picks || [];
    const runsOver = runsPicks.filter(function(p){ return p.pick==='OVER' && _oddsOk(p.over_odds); });
    const runsUnder = runsPicks.filter(function(p){ return p.pick==='UNDER' && _oddsOk(p.under_odds); });
    _fillCard('runs-over-card','runs-over-body','runs-over-more',runsOver,function(p,r){return _runsCard(p,r,'rno');},'Runs Over','#60a5fa');
    _fillCard('runs-under-card','runs-under-body','runs-under-more',runsUnder,function(p,r){return _runsCard(p,r,'rnu');},'Runs Under','#ff8a65');

  window.__WALKS_REG__={};
  const walksPicks = view.walks_picks || [];
  const walksOver = walksPicks.filter(function(p){ return p.pick==='OVER' && _oddsOk(p.over_odds); });
  const walksUnder = walksPicks.filter(function(p){ return p.pick==='UNDER' && _oddsOk(p.under_odds); });
  _fillCard('bwalk-over-card','bwalk-over-body','bwalk-over-more',walksOver,function(p,r){return _walksCard(p,r,'bwo');},'Walks Over','#34d399');
  _fillCard('bwalk-under-card','bwalk-under-body','bwalk-under-more',walksUnder,function(p,r){return _walksCard(p,r,'bwu');},'Walks Under','#ff8a65');

  const tbPicks = (view.tb_picks || []).filter(function(p){ return _oddsOk(p.tb_under_odds); });
  if (tbPicks.length > 0) {
    show('tb-picks-card');
    window.__TB_REG__={};
    document.getElementById('tb-picks-body').innerHTML = tbPicks.slice(0,10).map(function(p,i){ return _tbCard(p, i+1); }).join('');
    document.getElementById('tb-more-wrap').innerHTML = tbPicks.length > 10
      ? _moreWrap(tbPicks.slice(10), function(p,r){ return _tbCard(p, r); }, 11, 'TB Under', '#a78bfa')
      : '';
  }

  const tbOverPicks = (view.tb_over_picks || []).filter(function(p){ return _oddsOk(p.tb_over_odds); });
  if (tbOverPicks.length > 0) {
    show('tb-over-picks-card');
    window.__TBO_REG__={};
    document.getElementById('tb-over-picks-body').innerHTML = tbOverPicks.slice(0,10).map(function(p,i){ return _tbOverCard(p, i+1); }).join('');
    document.getElementById('tb-over-more-wrap').innerHTML = tbOverPicks.length > 10
      ? _moreWrap(tbOverPicks.slice(10), function(p,r){ return _tbOverCard(p, r); }, 11, 'TB Over', '#4ade80')
      : '';
  }

  window.__HRR_REG__={};
    const hrrPicks = view.hrr_picks || [];
    const hrrOver = hrrPicks.filter(function(p){ return p.pick!=='UNDER' && _oddsOk(p.hrr_over_odds); });
    const hrrUnder = hrrPicks.filter(function(p){ return p.pick==='UNDER' && _oddsOk(p.hrr_under_odds); });
    _fillCard('hrr-over-card','hrr-over-body','hrr-over-more',hrrOver,function(p,r){return _hrrCard(p,r,'hro');},'HRR Over','#fb923c');
    _fillCard('hrr-under-card','hrr-under-body','hrr-under-more',hrrUnder,function(p,r){return _hrrCard(p,r,'hru');},'HRR Under','#ff8a65');
    const hrrSpecial = (view.hrr_special_picks||[]).filter(function(p){ return _oddsOk(p.hrr_over_odds); });
    _fillCard('hrr-special-card','hrr-special-body','hrr-special-more',hrrSpecial,function(p,r){return _hrrSpCard(p,r,'hrsp');},'HRR Special','#a78bfa');
    window.__TSC_REG__={};
    const tripleSplit = (view.triple_split_picks||[]).filter(function(p){ return _oddsOk(p.hit_odds); });
    _fillCard('triple-split-card','triple-split-body','triple-split-more',tripleSplit,function(p,r){return _tscCard(p,r,'tsc');},'Triple Split Club','#22d3ee');
    window.__FSS_REG__={};
    _fillCard('five-star-card','five-star-body','five-star-more',(view.five_star_split_picks||[]),function(p,r){return _fssCard(p,r,'fss');},'5 Star Split','#a78bfa');

  renderPitcherProps(view);
  renderByGame(view);
  _syncParlayCats(); _paintCatBtn();  // keep the Categories button count matching the live checkboxes
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
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px';
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
  var _adm=!!(window.IS_ADMIN||window.IS_TESTER);
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
      ${_matrixWriteup(p,((p.sugg_line!=null||p.pick==='OVER')?'O':'U'),0,true,'strikeouts',pickTxt)}
      ${p.blend_src?('<div style="margin-top:10px;color:#64748b;font-size:.74rem">'+p.blend_src+'</div>'):''}
    </div>
  </div>`;
  ov.style.display='flex';
}

// ── Pitcher PROP categories (Hits Allowed / Outs / Earned Runs) ────────
// Generic, data-driven mirror of the Pitcher K table. Each market is an
// Over/Under pick built from the blended (career-vs-opp + recent) average vs
// the posted line. Renders per-market/side Top 10 cards; rows open _ppForm.
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
  var hasPP=p.proj!=null;
  var ppf=p.proj_factors||{};
  var ppDriver=hasPP?p.proj:p.blended;
  var projDisp=hasPP?(p.proj+(p.unit?' '+p.unit:'')):'';
  var gap=null;
  if(ppDriver!=null&&p.line!=null){
    if(isOver){ gap=ppDriver-(Math.floor(p.line)+1); }   // Over 4.5 must hit 5 to win
    else { gap=(Math.floor(p.line)+1)-ppDriver; }         // Under 5.5 loses at 6 — edge = that number minus proj
  }
  var gapDisp=gap!=null?('edge '+(gap>=0?'+':'')+gap.toFixed(1)+(p.unit?' '+p.unit:'')):'';
  var blendDisp=p.blended!=null?p.blended+(p.unit?' '+p.unit:''):'—';
  var lineDisp=p.line!=null?p.line+(p.unit?' '+p.unit:''):'—';
  var sideLabel=p.side?`<span style="font-size:.62rem;background:rgba(255,255,255,.07);border-radius:4px;padding:1px 5px;color:#94a3b8">${p.homeRoad||p.side}</span>`:'';
  var oppLabel=p.opp?`<span style="font-size:.62rem;color:#64748b">vs ${p.opp}</span>`:'';
  var gapHtml=gapDisp?`<div style="margin-top:4px;font-size:.66rem;color:#fbbf24">${gapDisp}</div>`:'';
  var oddsHtml=odds?`<div style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.85rem;margin-top:2px">${odds}${_bookTag(p)}</div>`:'';
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
        ${_seriesTag(p,(isOver?'O':'U'),true,({pitcher_hits_allowed:1,pitcher_outs:2,pitcher_earned_runs:3,pitcher_walks:4}[p.market]))}
      </div>
      ${teamLogo?`<img src="${teamLogo}" alt="${p.team||''}" style="height:30px;width:30px;object-fit:contain" onerror="this.style.display='none'"/>`:''}
    </div>
    <div class="mlb-card-name">${String(p.name||'')}</div>
    <div style="padding:10px 14px;flex:1;display:flex;flex-direction:column">
      <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px">${sideLabel}${oppLabel}</div>
      <div style="display:flex;align-items:center;justify-content:space-between;border-top:1px solid #1f2d3d;padding-top:6px;margin-top:2px">
        <span style="font-size:.7rem;color:#64748b">Line ${lineDisp} · ${hasPP?('Proj '+projDisp):('Blend '+blendDisp)}</span>
        <span style="color:${isOver?'#63cab7':'#ff8a65'};font-weight:900;font-size:.9rem">${p.pick||'—'}</span>
      </div>
      ${gapHtml}
      ${oddsHtml}
      ${hasPP?`<div style="margin-top:2px;font-size:.6rem;color:#475569">blend ${blendDisp} · hand x${ppf.hand!=null?ppf.hand:1} · whiff x${ppf.whiff!=null?ppf.whiff:1}</div>`:''}
      ${_veloBadge(p)}
      ${p.market==='pitcher_walks'&&p.opp_bb_rank!=null?`<div style="margin-top:6px;display:flex;align-items:center;gap:6px;flex-wrap:wrap"><span style="font-size:.62rem;color:#94a3b8">Opp BB/G rank:</span><span style="font-size:.72rem;font-weight:800;color:#34d399">#${p.opp_bb_rank}<span style="color:#64748b;font-weight:400"> of ${p.opp_bb_total||30}</span></span><span style="font-size:.68rem;color:#cbd5e1;font-family:monospace">${p.opp_bb_pg!=null?p.opp_bb_pg+' BB/G':''}</span></div>`:''}
      ${_evBadge(p)}
    </div>
  ${_betBtn(p,'Top 10 Pitcher',p.pick,_propStatKey,String(p.label||'Prop'),p.line,_propOdds)}
  </div>`;
}
function _fillCard(cardId,bodyId,moreId,arr,cardFn,label,color){
    if(!arr||!arr.length){ hide(cardId); return; }
    show(cardId);
    var body=document.getElementById(bodyId);
    if(body) body.innerHTML=arr.slice(0,10).map(function(p,i){return cardFn(p,i+1);}).join('');
    var me=document.getElementById(moreId);
    if(me) me.innerHTML=arr.length>10?_moreWrap(arr.slice(10),function(p,r){return cardFn(p,r);},11,label,color):'';
  }
  function _fillPropCard(cardId,bodyId,moreId,arr,label,color){
    if(!arr||!arr.length){ hide(cardId); return; }
    show(cardId);
    window.__PP_REG__=window.__PP_REG__||{}; if(window.__PP_SEQ__==null) window.__PP_SEQ__=0;
    var render=function(p,rank){ var k='pmk'+(window.__PP_SEQ__++); window.__PP_REG__[k]=p; return _propBestCard(p,k,rank); };
    document.getElementById(bodyId).innerHTML=arr.slice(0,10).map(function(p,i){return render(p,i+1);}).join('');
    var me=document.getElementById(moreId);
    if(me) me.innerHTML=arr.length>10?_moreWrap(arr.slice(10),function(p,r){return render(p,r);},11,label,color):'';
  }
  function renderPitcherProps(view){
    var props=(view&&view.pitcher_props)||{};
    window.__PP_REG__={}; window.__PP_BY_NAME__={}; window.__PP_SEQ__=0;
    var _ppN=0;
    PROP_ORDER.forEach(function(_mkt){
      ((props[_mkt]||{}).all||[]).forEach(function(_p){
        var _nm=String(_p.name||'').toLowerCase().trim(); if(!_nm) return;
        var _key='pp'+(_ppN++); window.__PP_REG__[_key]=_p;
        if(!window.__PP_BY_NAME__[_nm]) window.__PP_BY_NAME__[_nm]={};
        var _ex=window.__PP_BY_NAME__[_nm][_mkt];
        if(!_ex||(!_ex.obj.pick&&_p.pick)) window.__PP_BY_NAME__[_nm][_mkt]={obj:_p,key:_key};
      });
    });
    var dayList=_buildPitchDay(view);
    var _dayCard=function(x,rank){
      if(x.kind==='K') return _pitcherCard(x.p,rank,'pd');
      var k='pdp'+(window.__PP_SEQ__++); window.__PP_REG__[k]=x.p; return _propBestCard(x.p,k,rank);
    };
    var dayEl=document.getElementById('pitch-day-card');
    if(dayList.length){
      if(dayEl) dayEl.classList.remove('hidden');
      document.getElementById('pitch-day-body').innerHTML=dayList.slice(0,10).map(function(x,i){return _dayCard(x,i+1);}).join('');
      var dm=document.getElementById('pitch-day-more');
      if(dm) dm.innerHTML=dayList.length>10?'<details style="margin-top:14px"><summary class="more-btn" style="color:#63cab7;border-color:#63cab733">&#9655; '+(dayList.length-10)+' more Pitching Props</summary><div class="mlb-picks-grid mt-3">'+dayList.slice(10).map(function(x,i){return _dayCard(x,11+i);}).join('')+'</div></details>':'';
    } else if(dayEl){ dayEl.classList.add('hidden'); }
    var _mktCfg=[
      {m:'pitcher_hits_allowed',over:'prop-ha-over',under:'prop-ha-under',label:'Hits Allowed',clr:'#f87171'},
      {m:'pitcher_outs',        over:'prop-outs-over',under:'prop-outs-under',label:'Outs',clr:'#a78bfa'},
      {m:'pitcher_earned_runs', over:'prop-er-over',under:'prop-er-under',label:'Earned Runs',clr:'#fb923c'},
      {m:'pitcher_walks',       over:'prop-bb-over',under:'prop-bb-under',label:'Walks Allowed',clr:'#34d399'}
    ];
    _mktCfg.forEach(function(c){
      var allPicks=((props[c.m]||{}).picks)||[];
      var overs=allPicks.filter(function(p){return (p.pick||'').toUpperCase()==='OVER';});
      var unders=allPicks.filter(function(p){return (p.pick||'').toUpperCase()==='UNDER';});
      _fillPropCard(c.over+'-card',c.over+'-body',c.over+'-more',overs,c.label+' Over',c.clr);
      _fillPropCard(c.under+'-card',c.under+'-body',c.under+'-more',unders,c.label+' Under','#ff8a65');
    });
  }
// Generic prop recent-form popup (mirrors _pkForm, market-agnostic).
function _ppForm(key){
  var p=(key&&typeof key==='object')?key:(window.__PP_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('pp-modal');
  if(!ov){
    ov=document.createElement('div'); ov.id='pp-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px';
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
  var _ppCat={pitcher_hits_allowed:1,pitcher_outs:2,pitcher_earned_runs:3,pitcher_walks:4}[p.market];
  var _ppLbl=(p.pick?(p.pick+' '+(line!=null?line:'')+(unitW?(' '+unitW):'')):'this play');
  var _ppWu=_matrixWriteup(p,(p.pick==='OVER'?'O':'U'),_ppCat,true,String(p.label||'').toLowerCase().replace('pitcher ',''),_ppLbl);
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
      +_ppWu
      +(p.blend_src?('<div style="margin-top:10px;color:#64748b;font-size:.74rem">'+p.blend_src+'</div>'):'')
    +'</div>'
  +'</div>';
  ov.style.display='flex';
}

// ── Opposing-pitcher matchup block (the OPPOSITE of a hitter prop) ──────
// For a hitter prop popup, look up the starter the batter faces (p.pitcher)
// in the per-name pitcher-prop index and show that pitcher&#39;s last 5 games
// of the matching ALLOWED stat (batter walks -> pitcher walks allowed, batter
// hits -> pitcher hits allowed). Data already lives on the page (recent_log).
function _oppPitObj(pitName, market){
  var idx=window.__PP_BY_NAME__||{};
  var nm=String(pitName||'').toLowerCase().trim();
  if(!nm) return null;
  var rec=idx[nm];
  if(!rec){
    var last=nm.split(/ +/).pop();
    for(var k in idx){ if(k.split(/ +/).pop()===last){ rec=idx[k]; break; } }
  }
  if(!rec) return null;
  var e=rec[market];
  return (e&&e.obj)?e.obj:null;
}
// Batter's career head-to-head record vs the starter he faces today. Shows in
// EVERY hitter popup (prepended inside _oppPitBlock). vs_pit = {display,ab,hr};
// "No prior at-bats" when they've never met. Distinct from _oppPitBlock, which
// shows that starter's OWN prop line.
function _vsPitBlock(p){
  var pit=(p.pitcher&&p.pitcher!=='TBD')?p.pitcher:'';
  if(!pit) return '';
  var vp=p&&p.vs_pit;
  var inner, lbl;
  if(p.s1_tag&&(p.s1_ab||0)>0){
    // Statcast venue-split — matches the card face display exactly
    lbl=(p.s1_tag||'Career')+' vs';
    inner='<span style="font-family:monospace;font-weight:800;color:#e2e8f0">'+_esc(p.s1_disp||'')+'</span>';
    // Show combined MLB Stats API career as a secondary footnote when available
    if(vp&&(vp.ab||0)>0){
      var hr=vp.hr||0;
      inner+='<span style="color:#64748b;font-size:.76rem;margin-left:10px">'+_esc(vp.display||'')+' career'+(hr?(' \u00b7 '+hr+' HR'):'')+'</span>';
    }
  } else if(vp){
    lbl='Career vs';
    var ab=vp.ab||0;
    if(ab>0){
      var hr=vp.hr||0;
      inner='<span style="font-family:monospace;font-weight:800;color:#e2e8f0">'+_esc(vp.display||'')+'</span>'
        +(hr>0?('<span style="color:#fbbf24;font-weight:800;margin-left:8px">'+hr+' HR</span>'):'');
    } else {
      inner='<span style="font-size:.78rem;color:#64748b">No prior at-bats vs this starter</span>';
    }
  } else {
    return '';
  }
  return '<div style="margin-top:12px;padding:10px 12px;background:#0c1622;border-radius:8px;border:1px solid #1e2f3a">'
    +'<div style="font-size:.68rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">'+lbl+' &middot; <span style="color:#cbd5e1">'+_esc(pit)+'</span></div>'
    +'<div style="font-size:.9rem">'+inner+'</div></div>';
}
// Compact career-vs-starter LINE for the top-left of the left popup box
// (replaces the old full-width banner). Same data as _vsPitBlock, one tight line.
function _vsPitLine(p){
  var pit=(p.pitcher&&p.pitcher!=='TBD')?p.pitcher:'';
  if(!pit) return '';
  var vp=p&&p.vs_pit;
  var inner, lbl;
  if(p.s1_tag&&(p.s1_ab||0)>0){
    lbl=(p.s1_tag||'Career')+' vs';
    inner='<span style="font-family:monospace;font-weight:800;color:#e2e8f0;font-size:1rem">'+_esc(p.s1_disp||'')+'</span>';
    if(vp&&(vp.ab||0)>0){
      var hr=vp.hr||0;
      inner+='<span style="color:#64748b;font-size:.66rem;margin-left:8px">'+_esc(vp.display||'')+' career'+(hr?(' \u00b7 '+hr+' HR'):'')+'</span>';
    }
  } else if(vp){
    lbl='Career vs';
    var ab=vp.ab||0;
    if(ab>0){
      var hr=vp.hr||0;
      inner='<span style="font-family:monospace;font-weight:800;color:#e2e8f0;font-size:1rem">'+_esc(vp.display||'')+'</span>'
        +(hr>0?('<span style="color:#fbbf24;font-weight:800;margin-left:8px;font-size:.82rem">'+hr+' HR</span>'):'');
    } else {
      inner='<span style="font-size:.72rem;color:#64748b">No prior at-bats vs this starter</span>';
    }
  } else {
    return '';
  }
  return '<div style="margin-bottom:8px;padding-bottom:8px;border-bottom:1px solid #1f2937">'
    +'<div style="font-size:.62rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">'+lbl+' &middot; <span style="color:#cbd5e1">'+_esc(pit)+'</span></div>'
    +'<div>'+inner+'</div></div>';
}
// Full facing-starter block shown in EVERY hitter popup. Renders: the batter&#39;s
// career H2H vs this starter (via _vsPitBlock), the starter name/team/hand/ERA,
// ALL 5 pitcher prop markets (Strikeouts + Hits Allowed/Outs/Earned Runs/Walks
// with line + model PROJ + side + odds), and that starter&#39;s LAST 5 STARTS for
// the market matching THIS hitter&#39;s category (passed as market/statLabel/unit).
function _oppPitBlock(p, market, statLabel, unit){
  var pit=(p.pitcher&&p.pitcher!=='TBD')?p.pitcher:'';
  if(!pit) return '';
  var nmFull=String(pit).toLowerCase().trim();
  function _byName(idx){
    if(!idx) return null;
    var r=idx[nmFull];
    if(!r){ var last=nmFull.split(/ +/).pop(); for(var k in idx){ if(k.split(/ +/).pop()===last){ r=idx[k]; break; } } }
    return r||null;
  }
  var ppRec=_byName(window.__PP_BY_NAME__||{})||{};
  var kObj=_byName(window.__PK_BY_NAME__||{});
  // header meta: team / hand / ERA (omit any piece we do not have)
  var hand=((p.platoon||{}).pit_hand)||'';
  var handTxt=hand==='R'?'RHP':(hand==='L'?'LHP':'');
  var era=(kObj&&kObj.era!=null)?kObj.era:null;
  var team=(kObj&&kObj.team)?kObj.team:'';
  if(!team){ for(var mk in ppRec){ if(ppRec[mk]&&ppRec[mk].obj&&ppRec[mk].obj.team){ team=ppRec[mk].obj.team; break; } } }
  var metaBits=[];
  if(team) metaBits.push(_esc(team));
  if(handTxt) metaBits.push(handTxt);
  if(era!=null) metaBits.push(_esc(era)+' ERA');
  var headMeta=metaBits.length?(' <span style="color:#64748b;font-weight:600">\u00b7 '+metaBits.join(' \u00b7 ')+'</span>'):'';
  var head='<div style="margin-top:12px;padding:10px 12px;background:#0c1622;border-radius:8px;border:1px solid #1e2f3a">'
    +'<div style="font-size:.66rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px">Facing Starter \u00b7 <span style="color:#63cab7;font-weight:800">'+_esc(pit)+'</span>'+headMeta+'</div>';
  // ── ALL 5 MARKETS (line / model proj / pick + odds) ───────────────────
  function _mRow(lbl,ln,proj,pk,od){
    var pc=pk==='OVER'?'#63cab7':(pk==='UNDER'?'#ff8a65':'#64748b');
    var odStr=od!=null?((od>0?'+':'')+od):'\u2014';
    return '<tr>'
      +'<td style="padding:4px 8px;color:#e2e8f0;font-weight:600;font-size:.76rem">'+lbl+'</td>'
      +'<td style="padding:4px 8px;font-family:monospace;color:#fff;font-size:.76rem">'+(ln!=null?ln:'\u2014')+'</td>'
      +'<td style="padding:4px 8px;font-family:monospace;color:#7dd3fc;font-size:.76rem">'+(proj!=null?proj:'\u2014')+'</td>'
      +'<td style="padding:4px 8px;font-weight:800;font-size:.76rem;color:'+pc+'">'+(pk||'\u2014')+'</td>'
      +'<td style="padding:4px 8px;text-align:right;font-family:monospace;font-size:.76rem;color:#94a3b8">'+odStr+'</td>'
    +'</tr>';
  }
  var mkBody='';
  if(kObj){
    var kHasSugg=kObj.sugg_line!=null;
    var kLine=kHasSugg?kObj.sugg_line:kObj.line;
    var kPick=kHasSugg?'OVER':kObj.pick;
    var kOd=kHasSugg?kObj.sugg_odds:(kObj.pick==='OVER'?kObj.over_odds:(kObj.pick==='UNDER'?kObj.under_odds:null));
    var kProj=(kObj.blended_avg_k!=null?kObj.blended_avg_k:kObj.avg_k);
    mkBody+=_mRow('Strikeouts',kLine,kProj,kPick,kOd);
  } else { mkBody+=_mRow('Strikeouts',null,null,null,null); }
  [['pitcher_hits_allowed','Hits Allowed'],['pitcher_outs','Outs'],['pitcher_earned_runs','Earned Runs'],['pitcher_walks','Walks']].forEach(function(mm){
    var e=ppRec[mm[0]];
    if(e&&e.obj){ var o2=e.obj; var od=o2.pick==='OVER'?o2.over_odds:(o2.pick==='UNDER'?o2.under_odds:null);
      mkBody+=_mRow(mm[1],o2.line,o2.blended,o2.pick,od);
    } else { mkBody+=_mRow(mm[1],null,null,null,null); }
  });
  var mkTable='<div style="font-size:.62rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:5px">All 5 Markets</div>'
    +'<table style="width:100%;border-collapse:collapse">'
    +'<thead><tr>'
      +'<th style="text-align:left;padding:3px 8px;color:#64748b;font-size:.6rem;font-weight:600">Market</th>'
      +'<th style="text-align:left;padding:3px 8px;color:#64748b;font-size:.6rem;font-weight:600">Line</th>'
      +'<th style="text-align:left;padding:3px 8px;color:#64748b;font-size:.6rem;font-weight:600">Proj</th>'
      +'<th style="text-align:left;padding:3px 8px;color:#64748b;font-size:.6rem;font-weight:600">Pick</th>'
      +'<th style="text-align:right;padding:3px 8px;color:#64748b;font-size:.6rem;font-weight:600">Odds</th>'
    +'</tr></thead><tbody>'+mkBody+'</tbody></table>';
  // ── LAST 5 STARTS for THIS hitter category&#39;s market ───────────────────
  var o=_oppPitObj(pit, market);
  var last5;
  if(o){
    var line=o.line;
    var log=(o.recent_log||[]).slice(0,5);
    var rows=log.length?log.map(function(g){
      var v=g.v;
      var clr=(line!=null&&v!=null)?(v>line?'#63cab7':'#ff8a65'):'#e2e8f0';
      var oppTxt=g.opp?('vs '+g.opp):'';
      return '<tr>'
        +'<td style="padding:4px 8px;color:#94a3b8;font-family:monospace;font-size:.74rem">'+(g.d||'\u2014')+'</td>'
        +'<td style="padding:4px 8px;color:#cbd5e1;font-size:.74rem">'+oppTxt+'</td>'
        +'<td style="padding:4px 8px;text-align:right;font-family:monospace;font-weight:800;color:'+clr+'">'+(v!=null?(v+' '+unit):'\u2014')+'</td>'
      +'</tr>';
    }).join(''):'<tr><td colspan="3" style="padding:8px;color:#64748b;text-align:center;font-size:.74rem">No recent starts on record</td></tr>';
    var shownAvg=null;
    if(log.length){ var s=0,c=0; for(var i=0;i<log.length;i++){ var vv=log[i].v; if(vv!=null){ s+=Number(vv); c++; } } if(c) shownAvg=(s/c).toFixed(1); }
    var avgTxt=(shownAvg!=null)?(' \u00b7 avg '+shownAvg+' '+unit):'';
    last5='<div style="font-size:.62rem;letter-spacing:.05em;color:#64748b;text-transform:uppercase;margin-bottom:5px">Last '+log.length+' Starts \u00b7 '+statLabel+(line!=null?(' (line '+line+' '+unit+')'):'')+avgTxt+'</div>'
      +'<table style="width:100%;border-collapse:collapse"><tbody>'+rows+'</tbody></table>';
  } else {
    last5='<div style="font-size:.74rem;color:#64748b">No '+statLabel+' line posted for this starter</div>';
  }
  return head
    +'<div style="display:flex;gap:18px;flex-wrap:wrap;align-items:flex-start">'
      +'<div style="flex:1;min-width:300px">'+mkTable+'</div>'
      +'<div style="flex:1;min-width:240px">'+last5+'</div>'
    +'</div></div>';
}

// ── Shared popup signal header — mirrors the card face (chips + rate +
// converged + hot-hand + odds) so every hitter popup leads with the same
// information shown in the approved mockup, above the facing-starter block.
function _popSig(p, rateLbl, oddsLbl, oddsVal, isOver){
  var chips=(_envChip(p)||'')+(_umpChip(p)||'')+(_bpChip(p)||'')+(_platoonChip(p)||'');
  var chipRow=chips?('<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:8px">'+chips+'</div>'):'';
  var rate=(rateLbl&&p.rate_disp)?('<div style="margin-top:2px;font-size:.82rem"><span style="color:#94a3b8">'+rateLbl+'</span> <span style="font-family:monospace;font-weight:700;color:#86efac;font-size:1rem">'+p.rate_disp+'</span> <span style="color:#64748b;font-size:.66rem">'+(p.basis||'')+'</span></div>'):'';
  var conv=p.conv_flag?'<div style="font-size:.82rem;color:#4ade80;font-weight:600;margin-top:3px">&#10003; Converged &middot; L10 '+(p.recent_l10||'N/A')+' L5 '+(p.recent_l5||'N/A')+'</div>':(p.cold_flag?'<div style="font-size:.82rem;color:#fb923c;font-weight:600;margin-top:3px">&#9888; Recent diverges &middot; L5 '+(p.recent_l5||'N/A')+'</div>':((p.recent_l10||p.recent_l5)?'<div style="font-size:.82rem;color:#64748b;margin-top:3px">L10 '+(p.recent_l10||'N/A')+' &middot; L5 '+(p.recent_l5||'N/A')+'</div>':''));
  var hot=(isOver&&p.hot_disp)?'<div style="font-size:.82rem;color:#fbbf24;font-weight:700;margin-top:3px">&#128293; Hot hand &middot; '+p.hot_disp+' (+'+p.hot_bonus+')</div>':'';
  var dn=(typeof _dnChip==='function')?_dnChip(p):'';
  var odStr=(oddsVal!=null)?((oddsVal>0?'+':'')+oddsVal):'\u2014';
  var odds='<div style="margin-top:8px;padding-top:8px;border-top:1px solid #1f2937"><span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">'+(oddsLbl||'Odds')+'</span> <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:1rem">'+odStr+_bookTag(p)+'</span></div>';
  return chipRow+rate+conv+hot+dn+odds;
}

// ── Two even boxes: matchup signals (left) + series splits & last games (right) ──
function _twoBox(p, rateLbl, oddsLbl, oddsVal, isOver, lastLabel, rowsHtml){
  var sig=_popSig(p, rateLbl, oddsLbl, oddsVal, isOver);
  var ssIn=_ssInner(p);
  var bx='background:#0b1220;border:1px solid #1e293b;border-radius:10px;padding:12px';
  var lbl='font-size:.62rem;letter-spacing:.06em;color:#64748b;text-transform:uppercase';
  var last='<div style="'+lbl+';margin:'+(ssIn?'12px':'0')+' 0 6px">'+(lastLabel||'')+'</div>'
    +'<table style="width:100%;border-collapse:collapse;font-size:.82rem"><tbody>'+rowsHtml+'</tbody></table>';
  var left='<div style="flex:1 1 240px;min-width:0;'+bx+'">'+_vsPitLine(p)+'<div style="'+lbl+';margin-bottom:8px">Matchup Signals</div>'+sig+'</div>';
  var right='<div style="flex:1 1 240px;min-width:0;'+bx+'">'+ssIn+last+'</div>';
  return '<div style="display:flex;gap:12px;align-items:stretch;flex-wrap:wrap;margin-bottom:4px">'+left+right+'</div>';
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
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px';
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
  var wu=_matrixWriteup(p,(isUnder?'U':'O'),0,false,'hits',(isUnder?'under 1.5 hits':'to record a hit'));
  ov.innerHTML=`<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:820px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">
      <div>
        <div style="font-weight:800;font-size:1.05rem;color:#fff">${name}</div>
        <div style="color:#94a3b8;font-size:.78rem">${p.side||''} vs ${p.opp||''} · ${goal}</div>
      </div>
      <button onclick="document.getElementById('hit-modal').style.display='none'" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">&#x2715;</button>
    </div>
    <div style="padding:14px 18px">
      ${_twoBox(p,(isUnder?'':'Hit Rate vs Opp'),(isUnder?'Under 1.5 Hits':'Hit Odds'),(isUnder?p.under_odds:p.hit_odds),!isUnder,'Last '+(log.length||0)+' Games',rows)}
      ${_oppPitBlock(p,'pitcher_hits_allowed','Hits Allowed',  'H')}
      ${wu}
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
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px';
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
  ov.innerHTML=`<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:820px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">
      <div>
        <div style="font-weight:800;font-size:1.05rem;color:#fff">${name}</div>
        <div style="color:#94a3b8;font-size:.78rem">${p.side||''} vs ${p.opp||''} · ${goal}</div>
      </div>
      <button onclick="document.getElementById('runs-modal').style.display='none'" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">✕</button>
    </div>
    <div style="padding:14px 18px">
      ${_twoBox(p,'Runs Rate vs Opp','Runs Odds',(isOver?p.over_odds:p.under_odds),isOver,'Last '+(log.length||0)+' Games',rows)}
      ${_oppPitBlock(p,'pitcher_earned_runs','Earned Runs','ER')}
      ${_matrixWriteup(p,(isOver?'O':'U'),3,false,'runs',goal)}
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
  else if(p.recent_tb_log!==undefined){ if(p.pick==='OVER'){ _tbOverForm(p); } else { _tbForm(p); } }
  else if(p.recent_hr_log!==undefined && p.recent_hit_log===undefined && p.recent_runs_log===undefined){ _hrForm(p); }
  else if(p.recent_rbi_log!==undefined && p.recent_hit_log===undefined && p.recent_runs_log===undefined){ _rbiForm(p); }
  else if(p.recent_runs_log!==undefined && p.recent_hit_log===undefined){ _runsForm(p); }
  else if(p.recent_walks_log!==undefined){ _walksForm(p); }
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
  (r.hr_picks||[]).forEach(function(p,i){
    var isOver=p.pick==='OVER';
    var od=isOver?p.over_odds:p.under_odds;
    rows.push(['HR Pick', i+1, p.name||'', p.team||'', '', p.side||'', p.opp||'', '',
      (isOver?'Over':'Under')+' '+(p.line!=null?p.line:0.5)+' HR', (p.line!=null?p.line:0.5), _csvOdds(od), '', (p.score!=null?p.score+'% blended':'')]);
  });
  (r.runs_picks||[]).forEach(function(p,i){
    var isOver=p.pick==='OVER';
    var od=isOver?p.over_odds:p.under_odds;
    rows.push(['Runs Pick', i+1, p.name||'', p.team||'', '', p.side||'', p.opp||'', '',
      (isOver?'Over':'Under')+' '+(p.line!=null?p.line:0.5)+' Runs', (p.line!=null?p.line:0.5), _csvOdds(od), '', (p.rate_disp||'')+(p.basis?(' '+p.basis):'')]);
  });
  (r.walks_picks||[]).forEach(function(p,i){
    var isOver=p.pick==='OVER';
    var od=isOver?p.over_odds:p.under_odds;
    rows.push(['Batter Walks Pick', i+1, p.name||'', p.team||'', '', p.side||'', p.opp||'', '',
      (isOver?'Over':'Under')+' '+(p.line!=null?p.line:0.5)+' Walks', (p.line!=null?p.line:0.5), _csvOdds(od), '', (p.rate_disp||'')+(p.basis?(' '+p.basis):'')]);
  });
  var pk=(r.pitcher_k&&r.pitcher_k.all)||[];
  pk.filter(function(p){return p.pick;}).sort(function(a,b){
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
  pk.filter(function(p){return p.pick;}).sort(function(a,b){var ga=Math.abs((a.avg_k||0)-(a.line||0)),gb=Math.abs((b.avg_k||0)-(b.line||0));return gb-ga;}).forEach(function(p,i){
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
  (r.hr_picks||[]).forEach(function(p,i){
    var isOver=p.pick==='OVER';
    var od=isOver?p.over_odds:p.under_odds;
    cands.push({type:'HR',dir:p.pick,player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'HR',line:(p.line!=null?p.line:0.5),odds:(od!=null?od:''),conf:clampConf(78,i),reason:'💣 '+p.pick+' '+(p.line!=null?p.line:0.5)+' HR · '+(p.score!=null?p.score+'%':'')+' blended vs '+(p.opp||''),src:p});
  });
  (r.runs_picks||[]).forEach(function(p,i){
    var isOver=p.pick==='OVER';
    var od=isOver?p.over_odds:p.under_odds;
    cands.push({type:'RUN',dir:p.pick,player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Runs',line:(p.line!=null?p.line:0.5),odds:(od!=null?od:''),conf:clampConf(80,i),reason:'🏃 '+p.pick+' '+(p.line!=null?p.line:0.5)+' runs · '+(p.rate_disp||'')+' vs '+(p.opp||''),src:p});
  });
  (r.walks_picks||[]).forEach(function(p,i){
    var isOver=p.pick==='OVER';
    var od=isOver?p.over_odds:p.under_odds;
    cands.push({type:'BWALK',dir:p.pick,player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Walks',line:(p.line!=null?p.line:0.5),odds:(od!=null?od:''),conf:clampConf(80,i),reason:'🚶 '+p.pick+' '+(p.line!=null?p.line:0.5)+' walks · '+(p.rate_disp||'')+' vs '+(p.opp||''),src:p});
  });
  (r.tb_picks||[]).forEach(function(p,i){
    if(_underOk(p.tb_under_odds)){
      cands.push({type:'TB',dir:'UNDER',player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Total Bases',line:1.5,odds:p.tb_under_odds,conf:clampConf(88,i),reason:'⬇️ Under 1.5 TB · '+(p.rate_disp||'')+' rate vs '+(p.opp||''),src:p});
    }
  });
  (r.tb_over_picks||[]).forEach(function(p,i){
    if(_underOk(p.tb_over_odds)){
      cands.push({type:'TBO',dir:'OVER',player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Total Bases',line:1.5,odds:p.tb_over_odds,conf:clampConf(88,i),reason:'📈 Over 1.5 TB · '+(p.rate_disp||'')+' rate vs '+(p.opp||''),src:p});
    }
  });
  (r.hrr_picks||[]).forEach(function(p,i){
    var isOver=p.pick!=='UNDER';
    var od=isOver?p.hrr_over_odds:p.hrr_under_odds;
    if(_underOk(od)){
      cands.push({type:'HRR',dir:(isOver?'OVER':'UNDER'),player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'H+R+RBI',line:1.5,odds:od,conf:clampConf(86,i),reason:'🔥 '+(isOver?'Over':'Under')+' 1.5 HRR · '+(p.rate_disp||'')+' rate vs '+(p.opp||''),src:p});
    }
  });
  (r.hrr_special_picks||[]).forEach(function(p,i){
    var od=p.hrr_over_odds;
    if(_underOk(od)){
      cands.push({type:'HRRSP',dir:'OVER',player:(p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'H+R+RBI',line:1.5,odds:od,conf:clampConf(92,i),reason:'⭐ Special Over 1.5 HRR · BA '+(p.vsp_ba_disp||'')+' vs P · '+(p.vsteam_score!=null?p.vsteam_score+'% vs team':'')+' · '+(p.l10_score!=null?p.l10_score+'% L10':''),src:p});
    }
  });
  (r.triple_split_picks||[]).forEach(function(p,i){
    cands.push({type:'TSC',dir:'OVER',player:(p.full_name||p.name||''),team:(p.team||''),opp:(p.opp||''),stat:'Hits',line:0.5,odds:(p.hit_odds!=null?p.hit_odds:''),conf:clampConf(94,i),reason:'🔱 Triple Split · >.275 H/A, D/N & series · to record a hit vs '+(p.opp||''),src:p});
  });
  (r.five_star_split_picks||[]).forEach(function(p,i){
    var _fb={tb:'TBO',runs:'RUN',rbi:'RBI',hrr:'HRR'}[p.pick_market]||'TBO';
    cands.push({type:'FSS',_fssBase:_fb,dir:'OVER',player:(p.full_name||p.name||''),team:(p.team||''),opp:(p.opp||''),stat:(p.stat_label||'Total Bases'),line:(p.line!=null?p.line:1.5),odds:(p.odds!=null?p.odds:''),conf:clampConf(95,i),reason:'⭐ 5 Star Split · '+(p.pick_rate!=null?(p.pick_rate+'% L10 '+(p.stat_label||'')):'')+' vs '+(p.opp||''),src:p});
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
  // Parlay-builder odds-range filter — restricts to a specific American-odds window.
  if(window.PARLAY_ODDS_RANGE && window.PARLAY_ODDS_RANGE!=='all'){
    var _or=window.PARLAY_ODDS_RANGE;
    cands=cands.filter(function(c){
      var o=Number(c.odds);
      if(_or==='p100') return o>=100 && o<=149;
      if(_or==='p150') return o>=150 && o<=299;
      if(_or==='p300') return o>=300;
      return true;
    });
  }
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
  cands.forEach(function(c){ if(!c.player) return; var _ty=(c.type==='HRRSP'?'HRR':(c.type==='TSC'?'HIT':(c.type==='FSS'?(c._fssBase||'TBO'):c.type))); var k=c.player+'|'+_ty+'|'+c.stat; var cur=byKey[k]; if(!cur||_legScoreP(c)>_legScoreP(cur)) byKey[k]=c; });
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
  _syncParlayCats();  // re-read the live checkboxes so the build uses exactly what is checked
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
  var tagBg={HIT:'rgba(245,158,11,.16)',UNDER:'rgba(255,138,101,.16)',K:'rgba(99,202,183,.16)',RUN:'rgba(96,165,250,.16)',RBI:'rgba(251,191,36,.16)',HR:'rgba(244,63,94,.16)',HRR:'rgba(251,146,60,.16)',TB:'rgba(167,139,250,.16)',TBO:'rgba(74,222,128,.16)',BWALK:'rgba(52,211,153,.16)',FSS:'rgba(167,139,250,.16)',pitcher_hits_allowed:'rgba(248,113,113,.16)',pitcher_outs:'rgba(167,139,250,.16)',pitcher_earned_runs:'rgba(251,146,60,.16)',pitcher_walks:'rgba(52,211,153,.16)'};
  var tagFg={HIT:'#f59e0b',UNDER:'#ff8a65',K:'#63cab7',RUN:'#60a5fa',RBI:'#fbbf24',HR:'#f43f5e',HRR:'#fb923c',TB:'#a78bfa',TBO:'#4ade80',BWALK:'#34d399',FSS:'#a78bfa',pitcher_hits_allowed:'#f87171',pitcher_outs:'#a78bfa',pitcher_earned_runs:'#fb923c',pitcher_walks:'#34d399'};
  var tagLbl={HIT:'HIT',UNDER:'U1.5',K:'K',RUN:'RUNS',RBI:'RBI',HR:'HR',HRR:'HRR',TB:'U1.5 TB',TBO:'O1.5 TB',BWALK:'BB (BAT)',FSS:'5 STAR',pitcher_hits_allowed:'H ALLOW',pitcher_outs:'OUTS',pitcher_earned_runs:'ER',pitcher_walks:'BB (PIT)'};
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
  var logBtn=(window.IS_ADMIN||window.IS_TESTER)?('<button class="admin-only" onclick="_parlayBetForm()" style="margin-top:7px;background:rgba(67,56,202,.18);border:1px solid rgba(129,140,248,.55);color:#c7d2fe;border-radius:7px;padding:5px 11px;font-size:.72rem;font-weight:800;cursor:pointer;display:block">&#128221; Log This Parlay</button>'):'';
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
  _syncParlayCats();  // swap must respect exactly what is checked, too
  var cur=legs[idx];
  var _aty=function(c){return c.type==='HRRSP'?'HRR':(c.type==='TSC'?'HIT':(c.type==='FSS'?(c._fssBase||'TBO'):c.type));};
  var curKey=cur.player+'|'+_aty(cur)+'|'+cur.stat;
  var used={}; legs.forEach(function(l,i){ if(i!==idx) used[l.player+'|'+_aty(l)+'|'+l.stat]=1; });
  var pool=_mlbPool().filter(function(c){ var k=c.player+'|'+_aty(c)+'|'+c.stat; return k!==curKey && !used[k]; });
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
window.EV_ONLY = false;
window.PARLAY_UNDERS = false;
window.PARLAY_OVERS = false;
window.PARLAY_MINUS = false;
window.PARLAY_PLUS = false;
window.PARLAY_ODDS_RANGE = 'all';
// Parlay category checkboxes — which pick categories feed the parlay pool (all on by default).
window.PARLAY_CATS = {HIT_O:true,HIT_U:true,TB_O:true,TB_U:true,RUN_O:true,RUN_U:true,RBI_O:true,RBI_U:true,HR_O:true,HR_U:true,HRR_O:true,HRR_U:true,HRR_SP:true,TSC:true,FSS:true,BWALK_O:true,BWALK_U:true,K_O:true,K_U:true,PHA_O:true,PHA_U:true,POUT_O:true,POUT_U:true,PER_O:true,PER_U:true,PWK_O:true,PWK_U:true};
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

// Parlay-builder odds-range select — restricts pool to a specific American-odds window.
// Selecting any range implicitly requires positive odds, so it is compatible with "+ Odds Only".
function _parlayOddsRangeChange(){
  var sel=document.getElementById('parlayOddsRange');
  window.PARLAY_ODDS_RANGE = sel ? sel.value : 'all';
  if((document.getElementById('parlayResult').innerHTML||'').trim()) buildParlay();
}

// Maps a parlay candidate leg to its category key (the unders split by stat so
// Under 1.5 Hits and Under 1.5 Total Bases are independently checkable).
function _legCat(c){
  var dir=(c.dir||'').split(' ')[0]==='OVER'?'O':'U';
  if(c.type==='HIT') return 'HIT_O';
  if(c.type==='TSC') return 'TSC';
  if(c.type==='FSS') return 'FSS';
  if(c.type==='UNDER') return c.stat==='Total Bases'?'TB_U':'HIT_U';
  if(c.type==='TBO') return 'TB_O';
  if(c.type==='RUN') return 'RUN_'+dir;
  if(c.type==='RBI') return 'RBI_'+dir;
  if(c.type==='HR') return 'HR_'+dir;
  if(c.type==='HRRSP') return 'HRR_SP';
  if(c.type==='HRR') return 'HRR_'+dir;
  if(c.type==='BWALK') return 'BWALK_'+dir;
  if(c.type==='K') return 'K_'+dir;
  if(c.type==='pitcher_hits_allowed') return 'PHA_'+dir;
  if(c.type==='pitcher_outs') return 'POUT_'+dir;
  if(c.type==='pitcher_earned_runs') return 'PER_'+dir;
  if(c.type==='pitcher_walks') return 'PWK_'+dir;
  return c.type;
}
function _catCount(){ var n=0,t=0; for(var k in window.PARLAY_CATS){ t++; if(window.PARLAY_CATS[k]) n++; } return n+'/'+t; }
function _paintCatBtn(){ var b=document.getElementById('parlay-cats-btn'); if(b) b.innerHTML='&#9776; Categories ('+_catCount()+') &#9662;'; }
function toggleCatMenu(e){ if(e){ e.stopPropagation(); } var m=document.getElementById('parlay-cats-menu'); if(m) m.style.display=(m.style.display==='block')?'none':'block'; }
// SINGLE SOURCE OF TRUTH for parlay categories: copy the LIVE checkbox states into
// window.PARLAY_CATS. Browsers restore checkbox checked-state across reloads / back-
// forward navigation independently of our JS, which left PARLAY_CATS (all-true on load)
// out of sync with the boxes the user actually sees — so unchecked categories still fed
// the parlay. Re-reading the DOM right before every build guarantees the parlay uses
// exactly what is checked, 100%.
function _syncParlayCats(){
  var cbs=document.querySelectorAll('.parlay-cat-cb');
  if(!cbs.length) return;
  for(var i=0;i<cbs.length;i++){ window.PARLAY_CATS[cbs[i].value]=cbs[i].checked; }
}
function _catChanged(){
  _syncParlayCats();
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
  ((((r.pitcher_k||{}).all)||[]).filter(function(p){return p.pick;})).forEach(function(p){all.push(p);});
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
  if(!(window.IS_ADMIN||window.IS_TESTER)) return;
  window.UNDERS_ONLY = !window.UNDERS_ONLY;
  var b=document.getElementById('unders-btn');
  if(b){
    if(window.UNDERS_ONLY){ b.style.background='#ff8a65'; b.style.color='#1a1a1a'; b.innerHTML='&#11015; Unders Only: ON'; }
    else { b.style.background='#1f2937'; b.style.color='#fff'; b.innerHTML='&#11015; Unders Only'; }
  }
  if(window._lastResult) showResults(window._lastResult);
}

// Hit-list filter. Default (EV_ONLY off) leaves order UNTOUCHED — no value
// re-rank. "+EV Only" (window.EV_ONLY) filters to ev>0. Other categories use
// _evFilterView (applied to the whole render view in showResults).
function _evPos(p){ return !!(p && p.ev!=null && p.ev>0); }
function _evSortFilter(arr){
  arr=(arr||[]).slice();
  if(window.EV_ONLY) arr=arr.filter(_evPos);
  return arr;
}
// Returns a shallow copy of the render view with EVERY category filtered to
// +EV plays (ev>0). Used only when window.EV_ONLY is on.
function _evFilterView(v){
  var pk=v.pitcher_k, pp=v.pitcher_props||{};
  return Object.assign({}, v, {
    top9:(v.top9||[]).filter(_evPos),
    also_ran:(v.also_ran||[]).filter(_evPos),
    under_picks:(v.under_picks||[]).filter(_evPos),
    tb_picks:(v.tb_picks||[]).filter(_evPos),
    tb_over_picks:(v.tb_over_picks||[]).filter(_evPos),
    hrr_picks:(v.hrr_picks||[]).filter(_evPos),
    rbi_picks:(v.rbi_picks||[]).filter(_evPos),
    hr_picks:(v.hr_picks||[]).filter(_evPos),
    walks_picks:(v.walks_picks||[]).filter(_evPos),
    runs_picks:(v.runs_picks||[]).filter(_evPos),
    pitcher_k: pk?Object.assign({},pk,{
      picks:(pk.picks||[]).filter(_evPos),
      all:(pk.all||[]).filter(_evPos)
    }):pk,
    pitcher_props:(function(){
      var o={}; Object.keys(pp).forEach(function(m){
        var b=pp[m]||{};
        o[m]={picks:(b.picks||[]).filter(_evPos), all:(b.all||[]).filter(_evPos)};
      }); return o;
    })()
  });
}

// "+EV Only" toggle — keeps only hit picks where our matchup probability beats
// the book's price. Default OFF (all plays shown, value-ranked).
function toggleEvOnly(){
  window.EV_ONLY=!window.EV_ONLY;
  var b=document.getElementById('ev-btn');
  if(b){
    if(window.EV_ONLY){ b.style.background='#22c55e'; b.style.color='#06240f'; b.innerHTML='\u2713 +EV Only: ON'; }
    else { b.style.background='#1f2937'; b.style.color='#fff'; b.innerHTML='\u2713 +EV Only'; }
  }
  if(window._lastResult) showResults(window._lastResult);
}

// ── Edge Plays ─────────────────────────────────────────────────────────────
// Collect all picks from _lastResult with edge >= minEdge (0.05 = 5%).
// Returns up to 10 items sorted by edge descending, each carrying the bet params
// needed to call _betBtn so Track Bet / + Parlay work exactly as on any card.
function _edgeAllPicks(r, minEdge, opts){
  if(!r) return [];
  minEdge = minEdge||0.05;
  opts = opts||{};
  var _mo=(opts.minOdds!=null)?opts.minOdds:null;
  var _cap=(opts.cap!=null)?opts.cap:20;
  function _oddsOK(od){ return _mo==null || (od!=null && isFinite(od) && Number(od)>=_mo); }
  var out=[];
  function add(arr,cat,side,sk,sl,lineFn,odsFn){
    (arr||[]).forEach(function(p){
      if(!(p.edge>=minEdge)) return;
      var ln=lineFn?lineFn(p):(p.line!=null?p.line:0.5);
      var od=odsFn?odsFn(p):null;
      var sd=side||p.pick||'OVER';
      if(ln==null||!sd||!sk||!_oddsOK(od)) return;
      out.push({p:p,cat:cat,side:sd,sk:sk,sl:sl,line:ln,ods:od});
    });
  }
  // Hitter categories
  add(r.top9,         'Hitter Hits','OVER',  'hits',        'Hits',        function(){ return 0.5; }, function(p){ return p.hit_odds; });
  add(r.also_ran,     'Hitter Hits','OVER',  'hits',        'Hits',        function(){ return 0.5; }, function(p){ return p.hit_odds; });
  add(r.under_picks,  'Hitter Hits','UNDER', 'hits',        'Hits',        function(){ return 1.5; }, function(p){ return p.under_odds; });
  add(r.tb_picks,     'TB Under',   'UNDER', 'total_bases', 'Total Bases', function(){ return 1.5; }, function(p){ return p.tb_under_odds; });
  add(r.tb_over_picks,'TB Over',    'OVER',  'total_bases', 'Total Bases', function(){ return 1.5; }, function(p){ return p.tb_over_odds; });
  add(r.hrr_picks,    'HRR',        null,    'hrr',         'H+R+RBI',     function(){ return 1.5; }, function(p){ return p.pick==='UNDER'?p.hrr_under_odds:p.hrr_over_odds; });
  add(r.rbi_picks,    'RBI',        null,    'rbi',         'RBI',         function(p){ return p.line||0.5; }, function(p){ return p.pick==='OVER'?p.over_odds:p.under_odds; });
  add(r.hr_picks,     'HR',         null,    'homeRuns',    'HR',          function(p){ return p.line||0.5; }, function(p){ return p.pick==='OVER'?p.over_odds:p.under_odds; });
  add(r.walks_picks,  'Batter Walks',null,   'walks_bat',   'Walks',       function(p){ return p.line||0.5; }, function(p){ return p.pick==='OVER'?p.over_odds:p.under_odds; });
  add(r.runs_picks,   'Runs',       null,    'runs',        'Runs',        function(p){ return p.line||0.5; }, function(p){ return p.pick==='OVER'?p.over_odds:p.under_odds; });
  // Pitcher Ks
  var pk=(r.pitcher_k||{}); (pk.picks||[]).forEach(function(p){
    if(!(p.edge>=minEdge)) return;
    var ln=p.line!=null?p.line:(p.k_line!=null?p.k_line:5.5);
    var od=p.pick==='UNDER'?p.under_odds:p.over_odds;
    var sd=p.pick||'OVER';
    if(ln==null||!sd||!_oddsOK(od)) return;
    out.push({p:p,cat:'Pitcher Ks',side:sd,sk:'strikeOuts',sl:'Strikeouts',line:ln,ods:od});
  });
  // Pitcher props
  var pp=(r.pitcher_props||{});
  var _pmap={'pitcher_hits_allowed':{sk:'hits_allowed',sl:'Hits Allowed',cat:'Pitcher Hits Allowed'},
             'pitcher_outs':{sk:'outs',sl:'Outs',cat:'Pitcher Outs'},
             'pitcher_earned_runs':{sk:'earnedRuns',sl:'Earned Runs',cat:'Pitcher Earned Runs'},
             'pitcher_walks':{sk:'walks',sl:'Walks Allowed',cat:'Pitcher Walks'}};
  Object.keys(pp).forEach(function(m){
    var cfg=_pmap[m]; if(!cfg) return;
    ((pp[m]||{}).picks||[]).forEach(function(p){
      if(!(p.edge>=minEdge)) return;
      var ln=p.line; var sd=p.pick||'OVER'; var od=p.pick==='UNDER'?p.under_odds:p.over_odds;
      if(ln==null||!sd||!_oddsOK(od)) return;
      out.push({p:p,cat:cfg.cat,side:sd,sk:cfg.sk,sl:cfg.sl,line:ln,ods:od});
    });
  });
  out.sort(function(a,b){ return (b.p.edge||0)-(a.p.edge||0); });
  return out.slice(0,_cap);
}

// ── Proj Edge popup ─────────────────────────────────────────────────────────
// Pitchers: count projection vs line (real stat counts).
// Hitters: ev_prob vs 0.5 (model win probability vs 50/50 baseline).
// All qualifying picks shown, no cap.
function _projEdgePicks(r){
  if(!r) return [];
  var all=[];
  var _pmap={
    'pitcher_hits_allowed':{sk:'hits_allowed',sl:'Hits Allowed',cat:'Pitcher Hits Allowed'},
    'pitcher_outs':{sk:'outs',sl:'Outs',cat:'Pitcher Outs'},
    'pitcher_earned_runs':{sk:'earnedRuns',sl:'Earned Runs',cat:'Pitcher Earned Runs'},
    'pitcher_walks':{sk:'walks',sl:'Walks Allowed',cat:'Pitcher Walks'}
  };
  // Pitcher Ks
  var pk=(r.pitcher_k||{}); ((pk.all||[]).length?(pk.all):(pk.picks||[])).forEach(function(p){
    var proj=p.proj_k!=null?p.proj_k:(p.blended_avg_k!=null?p.blended_avg_k:null);
    var ln=p.line!=null?p.line:(p.k_line!=null?p.k_line:null);
    if(proj==null||ln==null) return;
    var sd=(p.pick||'OVER').toUpperCase();
    var gap=sd==='OVER'?proj-ln:ln-proj; if(gap<=0) return;
    var od=sd==='UNDER'?p.under_odds:p.over_odds;
    all.push({p:p,cat:'Pitcher Ks',side:sd,sk:'strikeOuts',sl:'Strikeouts',line:ln,ods:od,proj:proj,gap:gap,unit:'K',isProb:false});
  });
  // Pitcher props
  var pp=(r.pitcher_props||{});
  Object.keys(pp).forEach(function(m){
    var cfg=_pmap[m]; if(!cfg) return;
    var bucket=pp[m]||{}; ((bucket.all||[]).length?(bucket.all):(bucket.picks||[])).forEach(function(p){
      var proj=p.proj!=null?p.proj:(p.blended!=null?p.blended:null);
      var ln=p.line; if(proj==null||ln==null) return;
      var sd=(p.pick||'OVER').toUpperCase();
      var gap=sd==='OVER'?proj-ln:ln-proj; if(gap<=0) return;
      var od=sd==='UNDER'?p.under_odds:p.over_odds;
      all.push({p:p,cat:cfg.cat,side:sd,sk:cfg.sk,sl:cfg.sl,line:ln,ods:od,proj:proj,gap:gap,unit:(p.unit||''),isProb:false});
    });
  });
  // Hitter categories — rank by our model EDGE (side-aware win prob minus the
  // book&#39;s implied prob). ANY positive edge qualifies, however small, so every
  // category surfaces its top 5; ev_prob drives the displayed win%.
  function addH(arr,cat,sk,sl,lineFn,odsFn){
    (arr||[]).forEach(function(p){
      if(p.edge==null||p.ev_prob==null) return;
      if(p.edge<=0) return;
      var sd=(p.pick||'OVER').toUpperCase();
      var ln=lineFn?lineFn(p):(p.line!=null?p.line:0.5);
      var od=odsFn?odsFn(p):null;
      all.push({p:p,cat:cat,side:sd,sk:sk,sl:sl,line:ln,ods:od,proj:p.ev_prob,gap:p.edge,unit:'',isProb:true});
    });
  }
  addH(r.top9,         'Hitter Hits', 'hits',        'Hits',        function(){return 0.5;}, function(p){return p.hit_odds;});
  addH(r.also_ran,     'Hitter Hits', 'hits',        'Hits',        function(){return 0.5;}, function(p){return p.hit_odds;});
  addH(r.under_picks,  'U1.5 Hits',  'hits',        'Hits',        function(){return 1.5;}, function(p){return p.under_odds;});
  addH(r.tb_over_picks,'TB Over',    'total_bases', 'Total Bases', function(){return 1.5;}, function(p){return p.tb_over_odds;});
  addH(r.tb_picks,     'TB Under',   'total_bases', 'Total Bases', function(){return 1.5;}, function(p){return p.tb_under_odds;});
  addH(r.hrr_picks,    'HRR',        'hrr',         'H+R+RBI',     function(){return 1.5;}, function(p){return p.pick==='UNDER'?p.hrr_under_odds:p.hrr_over_odds;});
  addH(r.rbi_picks,    'RBI',        'rbi',         'RBI',         function(p){return p.line||0.5;}, function(p){return p.pick==='OVER'?p.over_odds:p.under_odds;});
  // HR intentionally NOT on the Proj Edge board (model uncalibrated). HR keeps its own separate tracker.
  addH(r.walks_picks,  'Batter Walks','walks_bat',  'Walks',       function(p){return p.line||0.5;}, function(p){return p.pick==='OVER'?p.over_odds:p.under_odds;});
  addH(r.runs_picks,   'Runs',       'runs',        'Runs',        function(p){return p.line||0.5;}, function(p){return p.pick==='OVER'?p.over_odds:p.under_odds;});
  window.__PROJ_EDGE__=all;
  return all;
}
// Open the recent-form popup for an edge-popup row, routed by its category so it
// shows exactly what a click on that player in his own category would show.
function _edgeRouteForm(p,cat){
  if(!p) return;
  cat=cat||'';
  if(cat==='Pitcher Ks'){ _pkForm(p); return; }
  if(cat.indexOf('Pitcher ')===0){ _ppForm(p); return; }
  if(cat==='TB Over'){ _tbOverForm(p); return; }
  if(cat==='TB Under'){ _tbForm(p); return; }
  if(cat==='HRR'){ _hrrForm(p); return; }
  if(cat==='RBI'){ _rbiForm(p); return; }
  if(cat==='HR'){ _hrForm(p); return; }
  if(cat==='Batter Walks'){ _walksForm(p); return; }
  if(cat==='Runs'){ _runsForm(p); return; }
  _hitForm(p);
}
function _projEdgeForm(i){
  var ep=(window.__PROJ_EDGE__||[])[i]; if(!ep) return;
  _edgeRouteForm(ep.p,ep.cat);
}
function _openProjEdge(){
  var r=window._lastResult;
  if(!r){ alert('Run picks first, then click Proj Edge.'); return; }
  var all=_projEdgePicks(r);
  function renderRow(item,globalIdx,rowIdx){
    var p=item.p, nm=p.full_name||p.name||'';
    var pickLbl=(item.side||'')+(item.line!=null?' '+item.line:'')+(item.unit?' '+item.unit:'');
    var od=item.ods, odsTxt=od!=null?((od>0?'+':'')+od):'&#x2014;';
    var projTxt,lineTxt,gapTxt,gapColor;
    if(!item.isProb){
      projTxt=(+item.proj.toFixed(2))+(item.unit?' '+item.unit:'');
      lineTxt=item.line+(item.unit?' '+item.unit:'');
      gapTxt='+'+(+item.gap.toFixed(2))+(item.unit?' '+item.unit:'');
      gapColor='#4ade80';
    } else {
      projTxt=(item.proj!=null?Math.round(item.proj*100)+'%':'&#x2014;');
      lineTxt=(item.proj!=null?Math.round((item.proj-item.gap)*100)+'%':'mkt');
      gapTxt='+'+(item.gap*100).toFixed(1)+'%';
      gapColor='#93c5fd';
    }
    var bb=_betBtn(p,item.cat,item.side,item.sk,item.sl,item.line,item.ods);
    return '<div onclick="event.stopPropagation()" style="border-bottom:1px solid #111c2e;background:'+(rowIdx%2?'#070e1b':'#050c18')+'">'
      +'<div style="display:grid;grid-template-columns:1fr 96px 56px 48px 62px;gap:0;padding:9px 14px;align-items:center">'
      +'<div><div style="color:#e2e8f0;font-weight:800;font-size:.82rem;cursor:pointer;text-decoration:underline;text-decoration-color:#334155" onclick="event.stopPropagation();_projEdgeForm('+globalIdx+')">'+_esc(nm)+'</div>'
      +'<div style="color:#64748b;font-size:.66rem;margin-top:1px">'+_esc(item.cat)+'</div></div>'
      +'<div style="text-align:right;font-size:.76rem;font-weight:700;color:#93c5fd">'+_esc(pickLbl)+'</div>'
      +'<div style="text-align:right;font-size:.75rem;color:#e2e8f0;font-weight:700">'+projTxt+'<br><span style="font-size:.6rem;color:#475569;font-weight:400">vs '+lineTxt+'</span></div>'
      +'<div style="text-align:right;font-size:.74rem;color:#64748b">'+odsTxt+'</div>'
      +'<div style="text-align:right"><span style="background:#052e16;color:'+gapColor+';font-weight:800;font-size:.78rem;border-radius:6px;padding:2px 7px">'+gapTxt+'</span></div>'
      +'</div>'+bb+'</div>';
  }
  // Group every play by category, keep each item's index in __PROJ_EDGE__ (for the
  // form lookup), then render the top 5 per category in a fixed pitching->hitting order.
  var byCat={};
  all.forEach(function(it,gi){ (byCat[it.cat]=byCat[it.cat]||[]).push({it:it,gi:gi}); });
  var CAT_ORDER=[
    ['Pitcher Ks','Pitcher \u2014 Strikeouts','#f59e0b'],
    ['Pitcher Hits Allowed','Pitcher \u2014 Hits Allowed','#f59e0b'],
    ['Pitcher Outs','Pitcher \u2014 Outs','#f59e0b'],
    ['Pitcher Earned Runs','Pitcher \u2014 Earned Runs','#f59e0b'],
    ['Pitcher Walks','Pitcher \u2014 Walks Allowed','#f59e0b'],
    ['Hitter Hits','Hitting \u2014 To Record a Hit','#38bdf8'],
    ['U1.5 Hits','Hitting \u2014 Under 1.5 Hits','#38bdf8'],
    ['TB Over','Hitting \u2014 Total Bases Over','#38bdf8'],
    ['TB Under','Hitting \u2014 Total Bases Under','#38bdf8'],
    ['HRR','Hitting \u2014 H+R+RBI','#38bdf8'],
    ['RBI','Hitting \u2014 RBI','#38bdf8'],
    ['Batter Walks','Hitting \u2014 Batter Walks','#38bdf8'],
    ['Runs','Hitting \u2014 Runs','#38bdf8']
  ];
  var inner='', any=false, di=0;
  CAT_ORDER.forEach(function(co){
    var bucket=byCat[co[0]]; if(!bucket||!bucket.length) return;
    bucket.sort(function(a,b){ return b.it.gap-a.it.gap; });
    bucket=bucket.slice(0,5);
    any=true;
    inner+='<div style="padding:6px 14px 4px;font-size:.62rem;font-weight:900;letter-spacing:.08em;color:'+co[2]+';border-bottom:1px solid #1e293b;background:#0a0f1e">'+co[1].toUpperCase()+' ('+bucket.length+')</div>';
    bucket.forEach(function(x){ inner+=renderRow(x.it,x.gi,di); di++; });
  });
  if(!any){
    inner='<div style="color:#94a3b8;padding:24px 16px;text-align:center">No projection edges found today.</div>';
  } else {
    inner='<div style="font-size:.62rem;color:#475569;font-weight:800;letter-spacing:.06em;display:grid;grid-template-columns:1fr 96px 56px 48px 62px;gap:0;padding:6px 14px 4px;border-bottom:1px solid #1e293b">'
      +'<span>PLAYER / MARKET</span><span style="text-align:right">PICK</span>'
      +'<span style="text-align:right">MODEL/MKT</span><span style="text-align:right">ODDS</span><span style="text-align:right">EDGE</span></div>'+inner;
  }
  var ov=document.getElementById('proj-edge-modal');
  if(!ov){
    ov=document.createElement('div'); ov.id='proj-edge-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.85);z-index:10000;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  ov.innerHTML='<div style="background:#080f1e;border:1px solid #1e3a5f;border-radius:18px;width:100%;max-width:600px;max-height:90vh;display:flex;flex-direction:column;box-shadow:0 24px 80px rgba(0,0,0,.7)" onclick="event.stopPropagation()">'
    +'<div style="display:flex;align-items:center;justify-content:space-between;padding:16px 20px;border-bottom:1px solid #1e293b;flex-shrink:0">'
    +'<div><div style="font-weight:900;color:#38bdf8;font-size:1.05rem">&#9650; Proj Edge Plays</div>'
    +'<div style="color:#64748b;font-size:.72rem;margin-top:2px">top 5 per category by edge &#xB7; pitchers: proj vs line &#xB7; hitters: win% vs book &#xB7; click any name for recent form &#xB7; Track Bet any row</div></div>'
    +'<button onclick="document.getElementById(&#39;proj-edge-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:32px;height:32px;border-radius:8px;cursor:pointer;font-size:1.1rem;flex-shrink:0">&#215;</button>'
    +'</div>'
    +'<div style="overflow-y:auto;flex:1">'+inner+'</div>'
    +'</div>';
  ov.style.display='flex';
}

// ── Odds-range filter ──────────────────────────────────────────────────────
window.ODDS_RANGE='';
// Extract the PRIMARY odds for the pick's direction (not the opposing side).
// Checks p.pick direction first so UNDER picks don't accidentally return
// the OVER side's odds (which could fall in a different range entirely).
function _pickOdds(p){
  if(!p) return null;
  function _num(v){ return (v!=null && v!=='' && !isNaN(parseFloat(v)))?parseFloat(v):null; }
  var dir=(p.pick||'').toUpperCase();
  var n;
  if(dir==='UNDER'){
    n=_num(p.under_odds)||_num(p.hrr_under_odds)||_num(p.tb_under_odds);
    if(n!=null) return n;
  } else if(dir==='OVER'){
    n=_num(p.over_odds)||_num(p.hrr_over_odds)||_num(p.tb_over_odds);
    if(n!=null) return n;
  }
  // No direction or fallback: try these in order (hit_odds / raw odds first)
  var fields=['hit_odds','odds','tb_over_odds','tb_under_odds',
              'hrr_over_odds','hrr_under_odds','over_odds','under_odds'];
  for(var i=0;i<fields.length;i++){
    n=_num(p[fields[i]]);
    if(n!=null) return n;
  }
  return null;
}
function _oddsMatchRange(p){
  var r=window.ODDS_RANGE; if(!r) return true;
  var o=_pickOdds(p); if(o==null) return false;
  if(r==='le-500')      return o<=-500;
  if(r==='-500to-450')  return o>-500  && o<=-450;
  if(r==='-450to-400')  return o>-450  && o<=-400;
  if(r==='-400to-350')  return o>-400  && o<=-350;
  if(r==='-350to-300')  return o>-350  && o<=-300;
  if(r==='-300to-250')  return o>-300  && o<=-250;
  if(r==='-250to-200')  return o>-250  && o<=-200;
  if(r==='-200to-150')  return o>-200  && o<=-150;
  if(r==='-150to-100')  return o>-150  && o<=-100;
  if(r==='+100to+150')  return o>=100  && o<=150;
  if(r==='+150to+200')  return o>150   && o<=200;
  if(r==='+200to+250')  return o>200   && o<=250;
  if(r==='+250to+300')  return o>250   && o<=300;
  if(r==='ge+300')      return o>=300;
  return true;
}
// Direct-value odds checker — takes the raw numeric odds field, no pick-object guessing.
// Use this at render time where you already know which field to check.
function _oddsOk(v){
  if(!window.ODDS_RANGE) return true;
  if(v==null||v==='') return false;
  var o=parseFloat(v); if(isNaN(o)) return false;
  var r=window.ODDS_RANGE;
  if(r==='le-500')      return o<=-500;
  if(r==='-500to-450')  return o>-500  && o<=-450;
  if(r==='-450to-400')  return o>-450  && o<=-400;
  if(r==='-400to-350')  return o>-400  && o<=-350;
  if(r==='-350to-300')  return o>-350  && o<=-300;
  if(r==='-300to-250')  return o>-300  && o<=-250;
  if(r==='-250to-200')  return o>-250  && o<=-200;
  if(r==='-200to-150')  return o>-200  && o<=-150;
  if(r==='-150to-100')  return o>-150  && o<=-100;
  if(r==='+100to+150')  return o>=100  && o<=150;
  if(r==='+150to+200')  return o>150   && o<=200;
  if(r==='+200to+250')  return o>200   && o<=250;
  if(r==='+250to+300')  return o>250   && o<=300;
  if(r==='ge+300')      return o>=300;
  return true;
}
function _oddsFilterView(v){
  var f=function(arr){ return (arr||[]).filter(_oddsMatchRange); };
  var pk=v.pitcher_k, pp=v.pitcher_props||{};
  return Object.assign({},v,{
    top9:f(v.top9), also_ran:f(v.also_ran),
    under_picks:f(v.under_picks), tb_picks:f(v.tb_picks),
    tb_over_picks:f(v.tb_over_picks), hrr_picks:f(v.hrr_picks),
    rbi_picks:f(v.rbi_picks), hr_picks:f(v.hr_picks),
    walks_picks:f(v.walks_picks), runs_picks:f(v.runs_picks),
    pitcher_k:pk?Object.assign({},pk,{picks:f(pk.picks),all:f(pk.all)}):pk,
    pitcher_props:(function(){
      var o={}; Object.keys(pp).forEach(function(m){
        var b=pp[m]||{};
        o[m]={picks:f(b.picks),all:f(b.all)};
      }); return o;
    })()
  });
}
function onOddsRangeChange(){
  var sel=document.getElementById('odds-range-sel');
  window.ODDS_RANGE=sel?sel.value:'';
  // Highlight the select when a filter is active
  if(sel){
    sel.style.background=window.ODDS_RANGE?'#0369a1':'#1f2937';
    sel.style.color='#fff';
  }
  if(window._lastResult) showResults(window._lastResult);
}

function _srchOpen(k){ var e=(window.__SRCH_REG__||{})[k]; if(!e) return; if(e.pitcher){ if(e.fn){ try{ e.fn(e.p); }catch(err){} } return; } openPlayerDeep(e.name||(e.p&&(e.p.full_name||e.p.name))||''); }
function runPlayerSearch(raw){
  var box = document.getElementById('player-search-result');
  var q = (raw||'').trim().toLowerCase();
  clearTimeout(window.__lkTimer__);
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
  // 6) Prop-market picks (RBI / HR / Runs / Walks / TB / HRR) — full card on click
  function _addCat(arr, market, fn, oddsFn, lineFn){
    (arr||[]).forEach(function(p,i){
      if(_matchName(p,q)) hits.push({bucket:market, rank:'#'+(i+1), kind:'CAT', p:p, fn:fn,
        market:market, side:(p.pick||''), line:lineFn(p), odds:oddsFn(p)});
    });
  }
  _addCat(r.rbi_picks,     'RBI',          _rbiForm,    function(p){return p.pick==='OVER'?p.over_odds:p.under_odds;}, function(p){return p.line!=null?p.line:0.5;});
  _addCat(r.hr_picks,      'HR',           _hrForm,     function(p){return p.pick==='OVER'?p.over_odds:p.under_odds;}, function(p){return p.line!=null?p.line:0.5;});
  _addCat(r.runs_picks,    'Runs',         _runsForm,   function(p){return p.pick==='OVER'?p.over_odds:p.under_odds;}, function(p){return p.line!=null?p.line:0.5;});
  _addCat(r.walks_picks,   'Batter Walks', _walksForm,  function(p){return p.pick==='OVER'?p.over_odds:p.under_odds;}, function(p){return p.line!=null?p.line:0.5;});
  _addCat(r.tb_over_picks, 'TB Over',      _tbOverForm, function(p){return p.tb_over_odds;}, function(){return 1.5;});
  _addCat(r.tb_picks,      'TB Under',     _tbForm,     function(p){return p.tb_under_odds;}, function(){return 1.5;});
  _addCat(r.hrr_picks,     'HRR',          _hrrForm,    function(p){return p.pick==='UNDER'?p.hrr_under_odds:p.hrr_over_odds;}, function(){return 1.5;});

  if(!hits.length){
    box.innerHTML='<div class="text-slate-500 text-sm" style="margin-bottom:10px">"<strong>'+_esc(raw)+'</strong>" isn&#39;t one of today&#39;s ranked picks &#8212; pulling whatever data we have&#8230;</div>'
      +'<div class="text-slate-600 text-xs" style="margin-bottom:10px">Searching a pitcher? Expand "All today&#39;s pitchers" below the K Picks table.</div>'
      +'<div id="search-extra"></div>';
    if(q.length>=3){ window.__lkTimer__=setTimeout(function(){ _findMorePlayers(raw, {}, true); }, 300); }
    return;
  }

  box.innerHTML = hits.map(function(h){
    var p=h.p, kind=h.kind;
    var fn = h.fn || (kind==='PITCHER'?_pkForm:_hitForm);
    window.__SRCH_REG__=window.__SRCH_REG__||{};
    var sk='sr'+(window.__SRCH_N__=(window.__SRCH_N__||0)+1);
    window.__SRCH_REG__[sk]={p:p, fn:fn, pitcher:(kind==='PITCHER'), name:(p.full_name||p.name||'')};
    var color = kind==='CAT'?'#a78bfa':
                h.bucket==='Top Picks'?'#fbbf24':
                h.bucket==='Money Ball Picks'?'#94a3b8':
                h.bucket==='Under Picks'?'#ef4444':
                h.bucket==='Pitcher K Picks'?'#63cab7':
                h.bucket==='Did Not Qualify'?'#6b7280':'#9ca3af';
    var html='<div onclick="_srchOpen(&#39;'+sk+'&#39;)" style="background:#0f0f0f;border:1px solid #262626;border-left:4px solid '+color+';border-radius:10px;padding:14px 18px;margin-bottom:10px;cursor:pointer">';
    html+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">';
    html+='<div><span style="color:#fff;font-weight:700;font-size:1.05rem">'+_esc(p.full_name||p.name||'')+'</span>';
    html+=' <span style="color:'+color+';font-weight:700;margin-left:8px">'+h.bucket+((h.rank&&h.rank.charAt(0)==='#')?(' '+h.rank):'')+'</span></div>';
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
        html+='<div style="margin-top:8px;color:#fca5a5;font-size:.82rem"><strong>Why DQ&#39;d:</strong> '+p.dq_reason+'</div>';
      } else if(h.bucket==='Top Picks'){
        html+='<div style="margin-top:8px;color:#cbd5e1;font-size:.82rem">Cleared every filter and ranks in the top 10 by total score.</div>';
      } else if(h.bucket==='Money Ball Picks'){
        html+='<div style="margin-top:8px;color:#cbd5e1;font-size:.82rem">Passed all 5 filters — solid play just outside the Top 10.</div>';
      } else if(h.bucket==='Under Picks'){
        html+='<div style="margin-top:8px;color:#cbd5e1;font-size:.82rem">Cold bat vs today&#39;s pitcher \u2014 model likes the UNDER.</div>';
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
    } else if(kind==='CAT'){
      var oddTxt=(h.odds!=null)?((h.odds>0?'+':'')+h.odds):'';
      html+='<div style="display:flex;flex-wrap:wrap;gap:14px;font-size:.82rem;color:#cbd5e1">';
      html+='<span><strong style="color:#94a3b8">Market</strong> '+h.market+'</span>';
      if(h.side) html+='<span><strong style="color:#94a3b8">Pick</strong> '+h.side+' '+h.line+'</span>';
      if(oddTxt) html+='<span><strong style="color:#94a3b8">Odds</strong> '+oddTxt+'</span>';
      html+='</div>';
    }
    html+='<div style="margin-top:10px;color:'+color+';font-size:.8rem;font-weight:700">View full card &#8594;</div>';
    html+='</div>';
    return html;
  }).join('') + '<div id="search-extra" style="margin-top:6px"></div>';
  var shown={}; hits.forEach(function(h){ var n=((h.p.full_name||h.p.name||'')+'').toLowerCase(); if(n) shown[n]=1; });
  if(q.length>=3){ window.__lkTimer__=setTimeout(function(){ _findMorePlayers(raw, shown, false); }, 300); }
}

async function lookupAnyPlayer(){
  var inp=document.getElementById('player-search-input');
  var name=(inp?inp.value:'').trim();
  var out=document.getElementById('lookup-any-result');
  if(!out) return;
  if(name.length<3){ out.innerHTML='<div class="text-slate-500 text-sm">Type at least 3 letters.</div>'; return; }
  var date=(window._lastResult&&window._lastResult.date)||'';
  out.innerHTML='<div class="text-slate-500 text-sm">Checking '+name+' across today&#39;s games&#8230;</div>';
  try{
    var r=await fetch('/api/lookup?name='+encodeURIComponent(name)+'&date_str='+encodeURIComponent(date));
    var d=await r.json();
    if(!d.found){ out.innerHTML='<div class="text-slate-400 text-sm">'+(d.msg||'No match.')+'</div>'; return; }
    if(d.verdict==='NOT_PLAYING'){ out.innerHTML='<div class="text-slate-400 text-sm">'+(d.msg||'')+'</div>'; return; }
    out.innerHTML=_lkCard(d, name);
  }catch(e){ out.innerHTML='<div class="text-red-400 text-sm">Lookup failed. Try again.</div>'; }
}

function _lkCard(d, name){
  var color=d.verdict==='GOOD'?'#22c55e':d.verdict==='DECENT'?'#fbbf24':d.verdict==='UNDER'?'#ff8a65':(d.verdict==='UNKNOWN'||d.verdict==='INSUFFICIENT')?'#9ca3af':'#ef4444';
  var html='<div style="background:#0f0f0f;border:1px solid #262626;border-left:4px solid '+color+';border-radius:10px;padding:14px 18px">';
  html+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">';
  html+='<span style="color:#fff;font-weight:700;font-size:1.05rem">'+_esc(d.full_name||name||'')+'</span>';
  if(d.side) html+='<span class="badge '+(d.side==='HOME'?'badge-home':'badge-away')+'">'+d.side+' vs '+_esc(d.opp||'')+'</span>';
  html+='</div>';
  html+='<div style="color:'+color+';font-weight:700;font-size:1rem;margin-bottom:6px">'+_esc(d.headline||'')+'</div>';
  html+='<div style="color:#cbd5e1;font-size:.85rem">'+_esc(d.blurb||'')+'</div>';
  if(d.pitcher) html+='<div style="color:#94a3b8;font-size:.8rem;margin-top:6px">Facing '+_esc(d.pitcher)+'</div>';
  html+='</div>';
  return html;
}

async function lookupNamed(rid){
  var e=(window.__LKM_REG__||{})[rid]; if(!e) return;
  var out=document.getElementById(e.out); if(!out) return;
  var date=(window._lastResult&&window._lastResult.date)||'';
  out.innerHTML='<div class="text-slate-500 text-sm">Checking '+_esc(e.name)+'&#8230;</div>';
  try{
    var r=await fetch('/api/lookup?name='+encodeURIComponent(e.name)+'&date_str='+encodeURIComponent(date));
    var d=await r.json();
    if(!d.found){ out.innerHTML='<div class="text-slate-400 text-sm">'+_esc(d.msg||'No match.')+'</div>'; return; }
    if(d.verdict==='NOT_PLAYING'){ out.innerHTML='<div class="text-slate-400 text-sm">'+_esc(d.msg||'')+'</div>'; return; }
    out.innerHTML=_lkCard(d, e.name);
  }catch(err){ out.innerHTML='<div class="text-red-400 text-sm">Lookup failed. Try again.</div>'; }
}

async function _findMorePlayers(raw, shown, isEmpty){
  var ex=document.getElementById('search-extra'); if(!ex) return;
  var q=(raw||'').trim();
  var date=(window._lastResult&&window._lastResult.date)||'';
  try{
    var r=await fetch('/api/lookup_matches?name='+encodeURIComponent(q)+'&date_str='+encodeURIComponent(date));
    var d=await r.json();
    var list=(d.players||[]).filter(function(p){ return !shown[(p.full_name||'').toLowerCase()]; });
    if(!list.length){
      ex.innerHTML = isEmpty ? '<div class="text-slate-400 text-sm">No one matching "'+_esc(raw)+'" is in a game today.</div>' : '';
      return;
    }
    window.__LKM_REG__=window.__LKM_REG__||{};
    var html = isEmpty ? '' : '<div class="text-slate-500 text-xs" style="margin:4px 0 8px">Others in today&#8217;s games</div>';
    list.forEach(function(p){
      var rid='lkm'+(window.__LKM_N__=(window.__LKM_N__||0)+1);
      window.__LKM_REG__[rid]={name:p.full_name};
      html+='<div style="background:#0f0f0f;border:1px solid #262626;border-left:4px solid #9ca3af;border-radius:10px;padding:14px 18px;margin-bottom:10px">';
      html+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">';
      html+='<span style="color:#fff;font-weight:700;font-size:1.05rem">'+_esc(p.full_name||'')+'</span>';
      if(p.side) html+='<span class="badge '+(p.side==='HOME'?'badge-home':'badge-away')+'">'+p.side+' vs '+_esc(p.opp||'')+'</span>';
      html+='</div>';
      html+='<div style="color:#94a3b8;font-size:.82rem">Not in today&#8217;s ranked picks'+(p.pitcher?(' &#183; facing '+_esc(p.pitcher)):'')+'</div>';
      html+='<div onclick="_deepFromReg(&#39;'+rid+'&#39;)" style="margin-top:8px;color:#fbbf24;font-size:.8rem;font-weight:700;cursor:pointer">Tap for last 10 &amp; all stats &#8594;</div>';
      html+='</div>';
    });
    ex.innerHTML=html;
  }catch(e){ ex.innerHTML = isEmpty ? '<div class="text-red-400 text-sm">Lookup failed. Try again.</div>' : ''; }
}

// ── Player Deep Dive (SEARCH BAR ONLY) ───────────────────────────────
// One consolidated pop-up per hitter: last 10 games (H/R/RBI/BB/HR/TB/HRR)
// + TOTALS + 7 over-rate tiles + matchup verdict. Any hitter in today's
// games (picked or not). Pitchers keep _pkForm.
function _deepFromReg(rid){ var e=(window.__LKM_REG__||{})[rid]; if(e) openPlayerDeep(e.name); }
function _deepDate(s){
  if(!s) return '&#8212;';
  var m=String(s).split('-'); if(m.length<3) return _esc(s);
  var mo=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][parseInt(m[1],10)-1]||'';
  return mo+' '+parseInt(m[2],10);
}
async function openPlayerDeep(name){
  if(!name) return;
  var ov=document.getElementById('deep-modal');
  if(!ov){
    ov=document.createElement('div'); ov.id='deep-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  ov.innerHTML='<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:520px;width:100%;padding:26px;color:#94a3b8">Loading '+_esc(name)+' &#8230;</div>';
  ov.style.display='flex';
  var date=(window._lastResult&&window._lastResult.date)||'';
  try{
    var r=await fetch('/api/player_deep?name='+encodeURIComponent(name)+'&date_str='+encodeURIComponent(date));
    var d=await r.json();
    if(!d||!d.found){
      ov.innerHTML='<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:520px;width:100%;padding:24px;color:#cbd5e1">'+_esc((d&&d.msg)||'No data.')+'<div style="margin-top:14px"><button onclick="document.getElementById(&#39;deep-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;padding:8px 16px;border-radius:8px;cursor:pointer">Close</button></div></div>';
      return;
    }
    ov.innerHTML=_deepCard(d);
  }catch(e){
    ov.innerHTML='<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:520px;width:100%;padding:24px;color:#fca5a5">Lookup failed. Try again.<div style="margin-top:14px"><button onclick="document.getElementById(&#39;deep-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;padding:8px 16px;border-radius:8px;cursor:pointer">Close</button></div></div>';
  }
}
function _deepSplit(lbl,o,active){
  var br=active?'#38bdf8':'#1e293b', bg=active?'#0c1a2e':'#0b1220';
  var box='flex:1;min-width:66px;background:'+bg+';border:1px solid '+br+';border-radius:9px;padding:'+(active?'4px':'8px')+' 4px 8px;text-align:center'+(active?';box-shadow:0 0 0 1px #38bdf8':'');
  var tag=active?'<div style="color:#38bdf8;font-size:.5rem;font-weight:800;letter-spacing:.06em;margin-bottom:1px">TODAY</div>':'';
  if(!o||!o.ab){
    return '<div style="'+box+'">'+tag
      +'<div style="color:#94a3b8;font-size:.6rem;font-weight:700;letter-spacing:.03em">'+lbl+'</div>'
      +'<div style="color:#475569;font-size:1.05rem;font-weight:800;margin:2px 0">&#8212;</div>'
      +'<div style="color:#475569;font-size:.56rem">no data</div></div>';
  }
  var av=parseFloat(o.avg)||0, col=av>=.300?'#22c55e':(av>=.250?'#fbbf24':'#94a3b8');
  return '<div style="'+box+'">'+tag
    +'<div style="color:#94a3b8;font-size:.6rem;font-weight:700;letter-spacing:.03em">'+lbl+'</div>'
    +'<div style="color:'+col+';font-size:1.05rem;font-weight:800;margin:2px 0">'+(o.avg||'&#8212;')+'</div>'
    +'<div style="color:#cbd5e1;font-size:.58rem">'+o.ab+' AB</div></div>';
}
function _deepCard(d){
  var games=(d.games||[]).map(function(g){ g.hrr=(g.h||0)+(g.r||0)+(g.rbi||0); return g; });
  var n=games.length;
  var T={ab:0,h:0,r:0,rbi:0,bb:0,hr:0,tb:0,hrr:0};
  games.forEach(function(g){ T.ab+=g.ab||0;T.h+=g.h||0;T.r+=g.r||0;T.rbi+=g.rbi||0;T.bb+=g.bb||0;T.hr+=g.hr||0;T.tb+=g.tb||0;T.hrr+=g.hrr||0; });
  var l10=T.ab?(T.h/T.ab):0;
  var ba=T.ab?('.'+('00'+Math.round(l10*1000)).slice(-3)):'&#8212;';
  var vcol=l10>=.300?'#22c55e':(l10>=.250?'#fbbf24':'#ff8a65');
  var vtxt=l10>=.300?'HOT BAT':(l10>=.250?'DECENT':'COLD BAT');
  if(!n){ vcol='#64748b'; vtxt='NO RECENT GAMES'; }
  var careerLine=(d.s1_ba!=null&&d.s1_ab)?(_fmtBA(d.s1_ba)+' career vs '+_esc(d.pitcher||'pitcher')+' ('+d.s1_ab+' AB)'):(d.pitcher?('no career AB vs '+_esc(d.pitcher)):'');
  var hot5=0; games.slice(0,5).forEach(function(g){ hot5+=g.hr||0; });
  var rows=n?games.map(function(g){
    var opp=(g.home?'vs ':'@ ')+_esc(g.opp||'');
    var hc=g.h>=2?'#63cab7':(g.h>=1?'#e2e8f0':'#64748b');
    var hrc=g.hr>=1?'#fbbf24':'#64748b';
    function td(v,col,bold){ return '<td style="padding:6px 8px;text-align:right;font-family:monospace;color:'+col+(bold?';font-weight:800':'')+'">'+v+'</td>'; }
    return '<tr style="border-top:1px solid #1e293b">'
      +'<td style="padding:6px 8px;color:#94a3b8;font-family:monospace;font-size:.76rem">'+_deepDate(g.date)+'</td>'
      +'<td style="padding:6px 8px;color:#cbd5e1;font-size:.78rem">'+opp+'</td>'
      +td(g.ab||0,'#64748b')+td(g.h||0,hc,true)+td(g.r||0,g.r>=1?'#e2e8f0':'#64748b')
      +td(g.rbi||0,g.rbi>=1?'#e2e8f0':'#64748b')+td(g.bb||0,g.bb>=1?'#e2e8f0':'#64748b')
      +td(g.hr||0,hrc,true)+td(g.tb||0,g.tb>=2?'#e2e8f0':'#64748b')+td(g.hrr||0,g.hrr>=2?'#e2e8f0':'#64748b')
      +'</tr>';
  }).join(''):'<tr><td colspan="10" style="padding:16px;text-align:center;color:#64748b">No recent games on record</td></tr>';
  function rate(f){ var c=0; games.forEach(function(g){ if(f(g)) c++; }); return n?Math.round(c/n*100):0; }
  var tiles=[
    ['HITS', rate(function(g){return (g.h||0)>=1;}), T.h, 'O0.5'],
    ['RUNS', rate(function(g){return (g.r||0)>=1;}), T.r, 'O0.5'],
    ['RBI',  rate(function(g){return (g.rbi||0)>=1;}), T.rbi, 'O0.5'],
    ['WALKS',rate(function(g){return (g.bb||0)>=1;}), T.bb, 'O0.5'],
    ['HR',   rate(function(g){return (g.hr||0)>=1;}), T.hr, 'O0.5'],
    ['TB',   rate(function(g){return (g.tb||0)>=2;}), T.tb, 'O1.5'],
    ['HRR',  rate(function(g){return (g.hrr||0)>=2;}), T.hrr, 'O1.5']
  ];
  var tileHtml=tiles.map(function(t){
    var pct=t[1], col=pct>=60?'#22c55e':(pct>=40?'#fbbf24':'#64748b'), av=n?(t[2]/n):0;
    return '<div style="flex:1;min-width:60px;background:#0b1220;border:1px solid #1e293b;border-radius:9px;padding:8px 3px;text-align:center">'
      +'<div style="color:#94a3b8;font-size:.6rem;font-weight:700;letter-spacing:.03em">'+t[0]+'</div>'
      +'<div style="color:'+col+';font-size:1.15rem;font-weight:800;margin:2px 0">'+pct+'%</div>'
      +'<div style="color:#cbd5e1;font-size:.6rem">'+av.toFixed(1)+'/gm</div>'
      +'<div style="color:#475569;font-size:.56rem">'+t[3]+'</div>'
      +'</div>';
  }).join('');
  var sideBadge=d.side?('<span class="badge '+(d.side==='HOME'?'badge-home':'badge-away')+'">'+d.side+' vs '+_esc(d.opp_abbr||d.opp||'')+'</span>'):'<span style="color:#64748b;font-size:.78rem">Not in a game today</span>';
  var tside=d.side||'', tdn=d.today_dn||'', tsg=d.today_series||'';
  var sp=d.splits||{};
  var hasSplit=(sp.home&&sp.home.ab)||(sp.away&&sp.away.ab)||(sp.day&&sp.day.ab)||(sp.night&&sp.night.ab);
  var splitRow=hasSplit?('<div style="color:#fbbf24;font-weight:700;font-size:.76rem;letter-spacing:.04em;margin-bottom:6px">SEASON SPLITS</div>'
    +'<div style="display:flex;gap:5px;flex-wrap:nowrap;margin-bottom:14px">'
    +_deepSplit('HOME',sp.home,tside==='HOME')+_deepSplit('AWAY',sp.away,tside==='AWAY')+_deepSplit('DAY',sp.day,tdn==='day')+_deepSplit('NIGHT',sp.night,tdn==='night')
    +'</div>'):'';
  var sr=d.series||{};
  var hasSeries=(sr.g1&&sr.g1.ab)||(sr.g2&&sr.g2.ab)||(sr.g3&&sr.g3.ab);
  var seriesRow=hasSeries?('<div style="color:#fbbf24;font-weight:700;font-size:.76rem;letter-spacing:.04em;margin-bottom:6px">SERIES SPLIT &#183; GAME OF SERIES</div>'
    +'<div style="display:flex;gap:5px;flex-wrap:nowrap;margin-bottom:14px">'
    +_deepSplit('GAME 1',sr.g1,tsg==='g1')+_deepSplit('GAME 2',sr.g2,tsg==='g2')+_deepSplit('GAME 3+',sr.g3,tsg==='g3')
    +'</div>'):'';
  function th(t,al){ return '<th style="padding:6px 8px;text-align:'+al+';color:#94a3b8;font-size:.66rem">'+t+'</th>'; }
  function ft(v){ return '<td style="padding:6px 8px;text-align:right;color:#fbbf24;font-weight:800;font-family:monospace">'+v+'</td>'; }
  return '<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:900px;width:100%;max-height:90vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">'
    +'<div style="display:flex;justify-content:space-between;align-items:flex-start;padding:16px 18px;border-bottom:1px solid #1e293b">'
      +'<div><div style="font-weight:800;font-size:1.15rem;color:#fff">'+_esc(d.full_name||'')+'</div>'
      +'<div style="color:#94a3b8;font-size:.78rem;margin-top:2px">'+_esc(d.team||'')+(d.pitcher?(' &#183; Facing '+_esc(d.pitcher)):'')+'</div></div>'
      +'<div style="display:flex;align-items:center;gap:10px">'+sideBadge
      +'<button onclick="document.getElementById(&#39;deep-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">&#x2715;</button></div>'
    +'</div>'
    +'<div style="padding:14px 18px">'
      +splitRow
      +seriesRow
      +'<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:12px">'
        +'<span style="background:'+vcol+'22;color:'+vcol+';border:1px solid '+vcol+'55;border-radius:6px;padding:3px 10px;font-weight:800;font-size:.78rem">'+vtxt+'</span>'
        +'<span style="color:#cbd5e1;font-size:.82rem">'+ba+' last '+n+(careerLine?(' &#183; '+careerLine):'')+(hot5>=2?(' &#183; &#128293; '+hot5+' HR in L5'):'')+'</span>'
      +'</div>'
      +'<div style="color:#fbbf24;font-weight:700;font-size:.76rem;letter-spacing:.04em;margin-bottom:6px">LAST '+n+' GAMES</div>'
      +'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:.82rem">'
        +'<thead><tr style="background:#0b1220">'+th('Date','left')+th('Opp','left')+th('AB','right')+th('H','right')+th('R','right')+th('RBI','right')+th('BB','right')+th('HR','right')+th('TB','right')+th('HRR','right')+'</tr></thead>'
        +'<tbody>'+rows
        +'<tr style="border-top:2px solid #334155;background:#0b1220">'
          +'<td style="padding:6px 8px;color:#fbbf24;font-weight:800;font-size:.72rem">TOTALS</td>'
          +'<td style="padding:6px 8px;color:#fbbf24;font-weight:800;font-family:monospace;font-size:.72rem">'+ba+'</td>'
          +ft(T.ab)+ft(T.h)+ft(T.r)+ft(T.rbi)+ft(T.bb)+ft(T.hr)+ft(T.tb)+ft(T.hrr)
        +'</tr></tbody></table></div>'
      +'<div style="color:#fbbf24;font-weight:700;font-size:.76rem;letter-spacing:.04em;margin:14px 0 6px">LAST '+n+' &#8212; HOW OFTEN HE HIT THE OVER</div>'
      +'<div style="display:flex;gap:5px;flex-wrap:nowrap">'+tileHtml+'</div>'
    +'</div>'
  +'</div>';
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
  var tbOvers=(result.tb_over_picks||[]).map(function(p){return Object.assign({_kind:'TB OVER'},p);});
  var hrrs=(result.hrr_picks||[]).map(function(p){return Object.assign({_kind:'HRR'},p);});
  var ks=((result.pitcher_k||{}).picks||[]).map(function(p){return Object.assign({_kind:'PITCHER K'},p);});
  var runs=(result.runs_picks||[]).map(function(p){return Object.assign({_kind:'RUNS'},p);});
  var propLegs=[];
  var _ppBG=(result.pitcher_props)||{};
  PROP_ORDER.forEach(function(mkt){
    var cfg=PROP_CFG[mkt]; var picks=((_ppBG[mkt]||{}).picks)||[];
    var statLbl=(cfg.label||'').replace('Pitcher ','').toUpperCase();
    picks.forEach(function(p){ propLegs.push(Object.assign({_kind:statLbl},p)); });
  });
  var all=hitters.concat(unders, tbUnders, tbOvers, hrrs, ks, runs, propLegs);
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
  var rank = (p.opp_k_rank_ha != null) ? p.opp_k_rank_ha : p.opp_k_rank;
  var kg   = (p.opp_k_pg_ha  != null) ? p.opp_k_pg_ha  : p.opp_k_pg;
  var ctx  = p.opp_k_context || '';
  var total = p.opp_k_total || 30;
  if (rank == null || kg == null) return '';
  var ctxTxt = ctx ? (' ' + ctx) : '';
  if (rank <= 10) {
    return '<div class="env-chip" style="border-color:#22c55e44;color:#22c55e">&#9650; High-K Lineup &middot; ' + kg + ' K/g' + ctxTxt + ' &middot; #' + rank + '</div>';
  }
  if (rank >= total - 9) {
    return '<div class="env-chip" style="border-color:#ef444444;color:#ef4444">&#9660; Low-K Lineup &middot; ' + kg + ' K/g' + ctxTxt + ' &middot; #' + rank + '</div>';
  }
  return '<div class="env-chip" style="border-color:#64748b55;color:#94a3b8">#' + rank + ctxTxt + ' &middot; ' + kg + ' K/g</div>';
}
function _bpChip(p){
  // hitter cards carry bp_opp (opponent bullpen); pitcher cards carry bp_own
  var bp=null, isPitcher=false;
  if(p&&p.bp_opp!=null){ bp=p.bp_opp; isPitcher=false; }
  else if(p&&p.bp_own!=null){ bp=p.bp_own; isPitcher=true; }
  if(!bp) return '';
  var out='';
  // ── quality chip (hitter cards only): opponent bullpen ERA lean ──
  if(!isPitcher&&bp.era!=null&&(bp.lean==='weak'||bp.lean==='elite')){
    var weak=bp.lean==='weak';
    var qclr=weak?'#4ade80':'#f87171';
    var qlbl=(weak?'\u25B2 Weak Opp Pen':'\u25BC Elite Opp Pen')+' \u00b7 '+bp.era+' ERA';
    var det=[];
    if(bp.era_l14!=null) det.push('L14 '+bp.era_l14);
    if(bp.era_szn!=null) det.push('Szn '+bp.era_szn);
    var qtip=(weak
      ?'Opponent bullpen ERA '+bp.era+' (above league) \u2014 boosts the hitter / over side'
      :'Opponent bullpen ERA '+bp.era+' (well below league) \u2014 fades the over side')
      +(det.length?' \u2014 '+det.join(' / '):'');
    out+='<div class="env-chip" title="'+_esc(qtip)+'" style="border-color:'+qclr+'44;color:'+qclr+'">'+qlbl+'</div>';
  }
  // ── fatigue chip (last 3 days IP) ──
  if(bp.bp_ip!=null){
    var ip=bp.bp_ip, taxed=bp.taxed;
    if(taxed){
      var lbl=isPitcher?'🔥 Taxed Own BP':'🔥 Taxed Opp BP';
      var clr=isPitcher?'#fbbf24':'#63cab7';
      var tip=isPitcher
        ?'Own bullpen threw '+ip+' IP in last 3 days \u2014 starter may be asked to go deeper'
        :'Opponent bullpen threw '+ip+' IP in last 3 days \u2014 late-game pitching may be weaker';
      out+='<div class="env-chip" title="'+_esc(tip)+'" style="border-color:'+clr+'44;color:'+clr+'">'+lbl+' \u00b7 '+ip+' IP/3d</div>';
    } else if(!isPitcher&&(p.pick==='UNDER'||(p.under_basis!=null))){
      var tip2='Opponent bullpen fresh ('+ip+' IP in last 3 days) \u2014 supports under lean';
      out+='<div class="env-chip" title="'+_esc(tip2)+'" style="border-color:#60a5fa44;color:#60a5fa">\u2744\uFE0F Fresh Opp BP \u00b7 '+ip+' IP/3d</div>';
    }
  }
  return out;
}
function _ssInner(p){
  var ss=p.series_splits||{}; var sp=p.series_game||ss.today_pos||1;
  if(!ss.g1_ab&&!ss.g2_ab&&!ss.g3_ab) return '';
  var slots=[{lbl:'Game 1',ba:ss.g1_ba,ab:ss.g1_ab||0,pos:1},{lbl:'Game 2',ba:ss.g2_ba,ab:ss.g2_ab||0,pos:2},{lbl:'Game 3+',ba:ss.g3_ba,ab:ss.g3_ab||0,pos:3}];
  function _sba(ba){ return ba!=null?ba.toFixed(3).replace('0.','.'):'\u2014'; }
  function _sclr(ba){ return ba==null?'#64748b':ba>=0.300?'#4ade80':ba>=0.250?'#fbbf24':'#f87171'; }
  var cols=slots.map(function(s){
    var isNow=s.pos===sp;
    var bdr=isNow?'border:1px solid rgba(250,204,21,.5);background:rgba(250,204,21,.08)':'border:1px solid #1e293b';
    var lclr=isNow?'#facc15':'#94a3b8';
    var vclr=isNow?'#facc15':_sclr(s.ba);
    return '<div style="flex:1;text-align:center;padding:8px 4px;border-radius:8px;'+bdr+'">'
      +'<div style="font-size:.62rem;color:'+lclr+';font-weight:'+(isNow?'800':'600')+';margin-bottom:4px">'+s.lbl+(isNow?' &#9654; TODAY':'')+'</div>'
      +'<div style="font-size:1rem;font-weight:900;color:'+vclr+';font-family:monospace">'+_sba(s.ba)+'</div>'
      +'<div style="font-size:.66rem;color:#475569;margin-top:2px">'+s.ab+' AB</div>'
      +'</div>';
  }).join('');
  var haLbl=ss.ha?(' \u00b7 '+ss.ha):'';
  return '<div style="font-size:.62rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px">Series Splits \u00b7 This Season'+haLbl+'</div>'
    +'<div style="display:flex;gap:8px">'+cols+'</div>';
}
function _ssBlock(p){
  var inner=_ssInner(p); if(!inner) return '';
  return '<div style="margin-top:14px;border-top:1px solid #1e293b;padding-top:12px">'+inner+'</div>';
}
function _seriesChip(p){
  var ss=p.series_splits; if(!ss) return '';
  var pos=p.series_game||ss.today_pos||1;
  var slots=[{lbl:'G1',ba:ss.g1_ba,ab:ss.g1_ab},{lbl:'G2',ba:ss.g2_ba,ab:ss.g2_ab},{lbl:'G3+',ba:ss.g3_ba,ab:ss.g3_ab}];
  var parts=slots.map(function(s,i){
    var isToday=(i+1)===pos;
    var baStr=s.ba!=null?(s.ba).toFixed(3).replace('0.','.'):'\u2014';
    var clr=s.ba==null?'#64748b':s.ba>=0.300?'#4ade80':s.ba>=0.250?'#fbbf24':'#f87171';
    if(isToday) return '<span style="font-size:.68rem;font-weight:900;color:#facc15;background:rgba(250,204,21,.15);border:1px solid rgba(250,204,21,.4);border-radius:4px;padding:1px 5px">'+s.lbl+' &#9656; '+baStr+'</span>';
    return '<span style="font-size:.65rem;color:'+clr+';opacity:.8">'+s.lbl+' '+baStr+'</span>';
  }).join('<span style="color:#334155;margin:0 3px">&middot;</span>');
  return '<div style="margin-top:5px;display:flex;align-items:center;gap:4px;flex-wrap:wrap">'
    +'<span style="font-size:.6rem;color:#475569">series</span>'+parts+'</div>';
}
function _rotInfo(p,isPit){
  if(!p) return null;
  var rank=isPit?p.rot_rank:p.opp_rot_rank;
  var rookie=isPit?p.rot_rookie:p.opp_rot_rookie;
  var tovr=isPit?p.rot_tier:p.opp_rot_tier;   // admin tier override (1/2/3); 0=auto
  if((rank==null||rank===0)&&!rookie&&!(tovr&&tovr>0)) return null;
  var tier;
  if(tovr&&tovr>0){
    tier=tovr;                     // admin-set tier wins (e.g. a weak staff all-mid)
  } else if(rank!=null&&rank>0){
    if(rank<=2) tier=1;            // SP1-2 = ace (two-ace staffs both read ace)
    else if(rank<=4) tier=2;       // SP3-4 = mid
    else tier=3;                   // SP5+  = back-end
  } else if(rookie) tier=3;        // unranked rookie = back-end fallback
  else return null;
  return {rank:rank,rookie:!!rookie,tier:tier};
}
function _seriesBadge(p,isPit){
  var ri=_rotInfo(p,isPit); if(!ri) return '';
  var lbl=(ri.rank!=null&&ri.rank>0)?('SP'+ri.rank):'SPOT';
  var bg=ri.tier===1?'rgba(16,185,129,.2)':ri.tier===2?'rgba(245,158,11,.18)':'rgba(239,68,68,.2)';
  var clr=ri.tier===1?'#34d399':ri.tier===2?'#fbbf24':'#f87171';
  var rk=ri.rookie?'<span style="font-size:.55rem;font-weight:900;color:#fca5a5;margin-left:3px">R</span>':'';
  var tip=ri.tier===1?('Staff ace (SP'+ri.rank+') on the mound')
    :ri.tier===2?('Mid-rotation arm (SP'+ri.rank+')')
    :(((ri.rank!=null&&ri.rank>0)?('Back-end starter (SP'+ri.rank+')'):'Spot starter')+(ri.rookie?' \u2014 rookie':''));
  return '<span title="'+tip+'" style="font-size:.62rem;font-weight:900;padding:2px 6px;border-radius:4px;background:'+bg+';color:'+clr+';letter-spacing:.06em">'+lbl+rk+'</span>';
}
// Day+series combined verdict dot. Green = both lean this side, amber = mixed,
// red = both lean against. Only shows when series_game is 1/2/3 AND both
// signals are available. Same verdict logic as the Track Record matrix.
function _matrixDot(p, side, isPit, catIdx){
  var pos=p&&(p.series_game||0);
  if(pos!==1&&pos!==2&&pos!==3) return '';
  var wd=new Date().getDay();
  var dLean=_mtxDayLean(wd,isPit,catIdx); if(!dLean) return '';
  var sLean=_mtxSeriesLean(pos,isPit,catIdx); if(!sLean) return '';
  var clr,glow,tip;
  if(dLean!==sLean){
    clr='#f59e0b'; glow='rgba(245,158,11,.75)';
    tip='Day + series lean opposite \u2014 mixed signal, lean light.';
  } else if(sLean===side){
    clr='#22c55e'; glow='rgba(34,197,94,.75)';
    tip='Day + series both lean this side \u2014 good spot.';
  } else {
    clr='#ef4444'; glow='rgba(239,68,68,.75)';
    tip='Day + series both lean against this pick \u2014 chart says fade.';
  }
  return '<span title="'+tip+'" style="display:inline-block;width:9px;height:9px;border-radius:50%;background:'+clr+';box-shadow:0 0 5px '+glow+';margin-left:5px;vertical-align:middle"></span>';
}
// True when _slotDot would render a RED light (depth chart fades the pick).
// Used to keep the Top 10 plays-of-the-day cards green/amber only.
function _t10DotIsRed(p, side, isPit, catIdx){
  var ri=_rotInfo(p,isPit); if(!ri) return false;
  if(ri.tier===2) return false;                 // mid-rotation = amber, never red
  var pos=ri.tier===1?1:3;
  var slots=window.__MPA_SLOTS__; if(!slots||!slots[pos]) return false;
  var arr=isPit?slots[pos].pit:slots[pos].bat;
  if(!arr||catIdx==null||arr[catIdx]==null) return false;
  return arr[catIdx]!==side;                     // chart leans opposite the pick = red
}
function _gameNoChip(p){
  var g=p&&p.series_game; if(!g) return '';
  var of=p.series_of||0;
  var lbl='G'+g+(of?('/'+of):'');
  var tip='Game '+g+(of?(' of '+of):'')+' of this series';
  return '<span title="'+tip+'" style="font-size:.58rem;font-weight:800;padding:1px 5px;border-radius:4px;background:rgba(148,163,184,.16);color:#cbd5e1;letter-spacing:.04em;margin-right:4px">'+lbl+'</span>';
}
function _seriesTag(p, side, isPit, catIdx){
  return _gameNoChip(p)+_seriesBadge(p,isPit)+_matrixDot(p, side, isPit, catIdx);
}
// Muted "MLB · CAT" label (gray MLB + accent category). Empty cat => just "MLB".
function _catLbl(cat, accent){
  var dot = cat ? (' <span style="color:'+accent+'">&#183; '+cat+'</span>') : '';
  return '<span style="font-size:.7rem;letter-spacing:.08em;font-weight:800;white-space:nowrap"><span style="color:#64748b">MLB</span>'+dot+'</span>';
}
// Shared clean card header: team logo + label on the left (label truncates),
// series/rotation chips grouped on the right. The rank now lives in the name
// bar (see _nameBar), so it is not drawn here. rank/rc kept for signature
// stability with the call sites.
function _cardHdr(rank, rc, labelHtml, teamLogo, teamName, tagHtml){
  var logo = teamLogo ? ('<img src="'+teamLogo+'" alt="'+(teamName||'')+'" style="height:22px;width:22px;object-fit:contain;flex:0 0 auto" onerror="this.remove()">') : '';
  return '<div style="display:flex;align-items:center;gap:7px;min-width:0;flex:1 1 auto">'
    + logo
    + '<span style="min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:0 1 auto">'+labelHtml+'</span>'
    + '</div>'
    + '<div style="display:flex;align-items:center;gap:4px;flex:0 0 auto">'+(tagHtml||'')+'</div>';
}
// Orange name bar, left-aligned: rank badge + player headshot + name. Rank
// number color follows the podium accent (gold/silver/bronze for ranks 1-3,
// else the category accent). Missing/broken photo id => no image.
function _nameBar(rank, rc, photoId, name){
  var nc = (rc && rc[0]==='#1e1e1e') ? rc[1] : (rc?rc[0]:'#f59e0b');
  var face = photoId ? ('<img src="https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/'+photoId+'/headshot/67/current" alt="" style="width:27px;height:27px;border-radius:50%;object-fit:cover;object-position:center 18%;border:1px solid rgba(0,0,0,.28);flex:0 0 auto" onerror="this.remove()">') : '';
  return '<div class="mlb-card-name" style="display:flex;align-items:center;gap:8px;text-align:left">'
    + '<div style="width:23px;height:23px;border-radius:50%;background:#111;color:'+nc+';display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.86rem;flex:0 0 auto">'+rank+'</div>'
    + face
    + '<span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0">'+(name||'')+'</span>'
    + '</div>';
}
// Plain-English depth-chart writeup for a detail popup. Maps the starter&#39;s
// rotation rank (SP1-2 ace / SP3-4 mid / SP5+ back-end) to the market lean in
// window.__MPA_SLOTS__, compares to the pick side, and renders a green
// "agrees" / red "fade" / amber "lean light" block with reasoning.
function _matrixWriteup(p, side, catIdx, isPit, marketWord, pickLabel){
  return '';
  var ri=_rotInfo(p,isPit); if(!ri) return '';
  var who=isPit?'This pitcher':'The opposing starter';
  var pl='<b style="color:#fff">'+pickLabel+'</b>';
  if(ri.tier===2){
    return _mtxBox('#f59e0b','rgba(245,158,11,.85)','rgba(245,158,11,.06)','rgba(245,158,11,.3)',
      'Mid-Rotation \u2014 Lean Light',
      who+' ranks <b style="color:#fff">SP'+ri.rank+'</b> in the rotation, a middle-of-the-staff arm. There is no clean depth-chart edge on '+marketWord+' either way, so treat this '+pl+' play as a lean-light spot.');
  }
  var pos=ri.tier===1?1:3;
  var slots=window.__MPA_SLOTS__; if(!slots||!slots[pos]) return '';
  var arr=isPit?slots[pos].pit:slots[pos].bat;
  if(!arr||catIdx==null||arr[catIdx]==null) return '';
  var sLean=arr[catIdx];
  var sTxt=sLean==='O'?'OVER':'UNDER';
  var sClr=sLean==='O'?'#4ade80':'#ff8a65';
  var rankTxt=(ri.rank!=null&&ri.rank>0)?('SP'+ri.rank):'spot';
  var why=ri.tier===1
    ?(who+' is the staff <b style="color:#fff">ace (SP'+ri.rank+')</b>, the toughest arm in the rotation')
    :(who+' is a <b style="color:#fff">back-end '+rankTxt+' starter</b>'+(ri.rookie?' and a rookie':'')+', usually the most hittable arm');
  var agree=(sLean===side);
  var clr=agree?'#22c55e':'#ef4444';
  var glow=agree?'rgba(34,197,94,.85)':'rgba(239,68,68,.85)';
  var bg=agree?'rgba(34,197,94,.06)':'rgba(239,68,68,.06)';
  var bd=agree?'rgba(34,197,94,.3)':'rgba(239,68,68,.3)';
  var head=agree?'Depth Chart Agrees \u2014 Good To Play':'Depth Chart Says Fade';
  var verdict=agree
    ?(', so the chart leans <b style="color:'+sClr+'">'+sTxt+'</b> on '+marketWord+', which matches this '+pl+' play. Good spot to play.')
    :(', so the chart leans <b style="color:'+sClr+'">'+sTxt+'</b> on '+marketWord+', the opposite of this '+pl+' play. Chart says fade.');
  return _mtxBox(clr,glow,bg,bd,head,why+verdict);
}
function _mtxBox(clr,glow,bg,bd,head,body){
  return '<div style="margin-top:14px;background:'+bg+';border:1px solid '+bd+';border-radius:10px;padding:11px 13px">'
    +'<div style="display:flex;align-items:center;gap:7px;font-size:.6rem;font-weight:800;letter-spacing:.07em;text-transform:uppercase;color:'+clr+';margin-bottom:6px">'
    +'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:'+clr+';box-shadow:0 0 6px '+glow+'"></span>'+head+'</div>'
    +'<div style="font-size:.82rem;line-height:1.55;color:#cbd5e1">'+body+'</div></div>';
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
  return `<img src="https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/${id}/headshot/67/current" alt="" style="width:24px;height:24px;border-radius:50%;object-fit:cover;object-position:center 18%;border:1px solid rgba(255,255,255,.12);flex-shrink:0" onerror="this.style.display='none'">`;
}
function _teamNickJS(s){
  var w=String(s||'').toLowerCase().split('.').join('').split(/ +/).filter(Boolean);
  if(!w.length) return '';
  var n=w.length;
  if(w[n-1]==='sox'&&n>=2) return w[n-2]+' sox';
  return w[n-1];
}
function _teamMatchJS(a, b) {
  if (!a||!b) return false;
  var n1=String(a).toLowerCase().trim(), n2=String(b).toLowerCase().trim();
  if(n1===n2) return true;
  var k1=_teamNickJS(n1), k2=_teamNickJS(n2);
  return !!k1 && k1===k2;
}
function _umpKMul(p){
  var u=p&&p.ump; if(!u) return 1;
  var k=Number(u.kFactor); if(!k||k<=0) return 1;
  return p.pick==='UNDER'?(1/k):k;
}
// Matchup-value badge: our Log5 P(1+ hit) vs the book's implied price. Green
// when +EV (book underpricing the hit), gray when no edge. Hidden if no model.
function _evBadge(p){
  if(p.ev==null) return '';
  var pos=p.ev>0;
  var hasEdge=(p.edge!=null);
  var edgePct=hasEdge?((p.edge>0?'+':'')+(p.edge*100).toFixed(0)+'%'):'';
  var bg=pos?'rgba(34,197,94,.14)':'rgba(148,163,184,.10)';
  var bd=pos?'#22c55e':'#475569';
  var fg=pos?'#4ade80':'#94a3b8';
  var lbl=pos?('\u2713 +EV'+(hasEdge?(' \u00b7 edge '+edgePct):'')):('\u2013 no edge'+(hasEdge?(' '+edgePct):''));
  var prob=(p.ev_prob!=null?p.ev_prob:p.matchup_prob);
  var mp=prob!=null?(prob*100).toFixed(0)+'%':'';
  var ip=null;
  if(p.impl_prob!=null) ip=(p.impl_prob*100).toFixed(0)+'%';
  else if(prob!=null&&p.edge!=null) ip=((prob-p.edge)*100).toFixed(0)+'%';
  return '<div title="Our model win probability for this pick vs the book&#39;s implied probability from the price. Positive edge means the book is underpricing it." style="margin-top:6px;display:flex;align-items:center;justify-content:space-between;gap:6px;background:'+bg+';border:1px solid '+bd+';border-radius:8px;padding:3px 8px">'
    +'<span style="font-size:.62rem;font-weight:800;letter-spacing:.03em;color:'+fg+'">'+lbl+'</span>'
    +(mp?'<span style="font-size:.62rem;color:#cbd5e1;font-family:monospace">'+mp+(ip?(' vs '+ip):'')+'</span>':'')
    +'</div>';
}
function _krateBadge(p){
  var sp=p.k_rate; if(sp==null) return '';
  var diff=sp-22;
  if(Math.abs(diff)<3) return '';
  var hi=diff>=0;
  var bg=hi?'rgba(99,202,183,.10)':'rgba(248,113,113,.10)';
  var bd=hi?'#22c55e':'#f87171';
  var fg=hi?'#4ade80':'#fca5a5';
  var lbl='K-rate '+sp.toFixed(0)+'%'+(hi?' &#8679; elite K%':' &#8681; below avg');
  return '<div style="margin-top:4px;display:flex;align-items:center;gap:6px;background:'+bg+';border:1px solid '+bd+';border-radius:6px;padding:3px 8px">'
    +'<span style="font-size:.60rem;font-weight:700;color:'+fg+'">'+lbl+'</span></div>';
}
function _veloBadge(p){
  var v=p.velo_avg; if(v==null) return '';
  var diff=v-93.3;
  var hi=diff>=1.5, lo=diff<=-1.5;
  if(!hi&&!lo) return '';
  var bg=hi?'rgba(34,197,94,.10)':'rgba(248,113,113,.10)';
  var bd=hi?'#22c55e':'#f87171';
  var fg=hi?'#4ade80':'#fca5a5';
  var lbl=hi?('Velo '+v.toFixed(1)+' mph &#8679; K boost'):('Velo '+v.toFixed(1)+' mph &#8681; K drag');
  return '<div style="margin-top:4px;display:flex;align-items:center;gap:6px;background:'+bg+';border:1px solid '+bd+';border-radius:6px;padding:3px 8px">'
    +'<span style="font-size:.60rem;font-weight:700;color:'+fg+'">'+lbl+'</span></div>';
}
function _xbaBadge(p){
  var xba=p.xba; var hh=p.hard_hit_pct; var s1=p.s1;
  if(xba==null&&hh==null) return '';
  var parts=[];
  if(xba!=null&&s1!=null){
    var gap=xba-s1;
    if(Math.abs(gap)>=0.020){
      var pos=gap>0;
      var fg2=pos?'#4ade80':'#fca5a5';
      var gapPt=(gap*1000)|0;
      parts.push('<span style="color:'+fg2+';font-weight:700">'+(pos?'+':'')+gapPt+'pt xBA '+(pos?'&#8679; due':'&#8681; fade')+'</span>');
    }
  }
  if(hh!=null){
    var dev=hh-35;
    if(Math.abs(dev)>=3){
      var fgh=dev>0?'#34d399':'#f87171';
      parts.push('<span style="color:'+fgh+'">HH '+(dev>0?'+':'')+dev.toFixed(0)+'%</span>');
    }
  }
  if(!parts.length) return '';
  return '<div style="margin-top:4px;display:flex;align-items:center;gap:6px;background:rgba(148,163,184,.07);border:1px solid #334155;border-radius:6px;padding:3px 8px;font-size:.60rem;flex-wrap:wrap">'
    +parts.join('<span style="color:#475569"> &#183; </span>')+'</div>';
}

function _bookTag(p){ return (p&&p.book)?(' <span style="font-size:.62rem;color:#94a3b8;font-weight:600">'+p.book+'</span>'):''; }
function _dnChip(p){
  if(!p||!p.s5) return '';
  var v=(p.s5&&p.s5.display)||'';
  if(!v||v==='N/A') return '';
  if(v.charAt(0)==='0'&&v.charAt(1)==='.') v=v.slice(1);
  var lbl=p.dn_label||'D/N';
  if(lbl==='DAY') lbl='Day'; else if(lbl==='NIGHT') lbl='Night';
  return '<div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">'
    +'<span style="font-size:.72rem;color:#64748b">'+lbl+' BA</span>'
    +'<span style="font-family:monospace;font-weight:700;color:#7dd3fc;font-size:.82rem">'+v+'</span></div>';
}
function _mlbCard(p, rank, dim) {
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const headshot = p.player_id ? `https://a.espncdn.com/i/headshots/mlb/players/full/${p.player_id}.png` : '';
  const rnkColors = rank===1?['#f59e0b','#000']:rank===2?['#c0c0c0','#000']:rank===3?['#cd7f32','#fff']:['#1e1e1e','#f59e0b'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const odds = p.hit_odds!=null?(p.hit_odds>0?'+':'')+p.hit_odds:'—';
  const s5Lbl = p.dn_label||(p.s5?'D/N':'');
  const s5Val = p.s5?.display||'—';
  const s5Suffix = (p.s5 && s5Val!=='—') ? ` &#183; ${s5Lbl} BA ${s5Val}` : '';
  window.__HIT_REG__=window.__HIT_REG__||{}; window.__HIT_REG__['h'+rank]=p;
  return `<div class="mlb-pick-card" onclick="_hitForm('h${rank}')" title="Click for recent form" style="cursor:pointer;${dim?'opacity:0.85':''}">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#1a2a1a 0%,#0a1a0a 100%)">${_cardHdr(rank,rnkColors,_catLbl(p.pos||'','#f59e0b'),teamLogo,p.team,_seriesTag(p,'O',false,0))}</div>
    ${_nameBar(rank,rnkColors,p.player_id,p.full_name||p.name)}
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
      ${(p.h2h_disp||p.l10_disp||p.rate_disp)?_rateRows(p,'#86efac'):''}
      ${p.conv_flag?'<div style="font-size:.67rem;color:#4ade80;font-weight:600;margin-top:2px">&#10003; Converged &middot; L10 '+(p.recent_l10||'N/A')+' L5 '+(p.recent_l5||'N/A')+'</div>':(p.cold_flag?'<div style="font-size:.67rem;color:#fb923c;font-weight:600;margin-top:2px">&#9888; Recent diverges &middot; L5 '+(p.recent_l5||'N/A')+'</div>':((p.recent_l10||p.recent_l5)?'<div style="font-size:.67rem;color:#64748b;margin-top:2px">L10 '+(p.recent_l10||'N/A')+' &middot; L5 '+(p.recent_l5||'N/A')+'</div>':''))}
      ${p.hot_disp?'<div style="font-size:.67rem;color:#fbbf24;font-weight:700;margin-top:2px">&#128293; Hot hand &middot; '+p.hot_disp+' (+'+p.hot_bonus+')</div>':''}
      ${p.over_sourced?'<div style="font-size:.62rem;color:#a78bfa;font-weight:600;margin-top:2px">+ Hot-hitter add ('+(
        (p.vs_pit&&(p.vs_pit.ab||0)>0)?((p.vs_pit.display||'')+' vs pitcher, below gate')
        :((p.s1_ab||0)>0&&p.s1_disp)?(p.s1_disp+(p.s1_tag?' '+p.s1_tag.toLowerCase():'')+' vs pitcher, below gate')
        :'no career vs pitcher')+')</div>':''}
      ${p.facing_top_era?`<div style="margin-top:6px;font-size:.7rem;color:#fbbf24;background:rgba(245,158,11,.10);border:1px solid rgba(245,158,11,.35);border-radius:6px;padding:3px 7px">⚾ vs top-30 ERA: ${p.facing_top_era}${p.top_era_val!=null?' · '+(+p.top_era_val).toFixed(2)+' ERA':''}</div>`:''}
      ${(p.blurb||s5Suffix) ? `<div style="margin-top:5px;font-size:.72rem;color:#94a3b8;line-height:1.5;font-style:italic">${p.blurb||''}${s5Suffix}</div>` : ''}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">Hit Odds</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odds}${_bookTag(p)}</span>
      </div>
      ${_evBadge(p)}
      ${_xbaBadge(p)}
      ${_seriesChip(p)}
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
  var _ubParts=[];
  if(p.s1_disp && p.s1_ab){ _ubParts.push((p.s1_tag?(p.s1_tag+' '):'Career ')+p.s1_disp+(p.pitcher?(' vs '+p.pitcher):'')+' ('+p.s1_ab+' AB)'); }
  if(p.s3 && p.s3.display){ _ubParts.push('L10 '+p.s3.display); }
  else if(p.l7 && p.l7.display){ _ubParts.push('L7 '+p.l7.display); }
  if(p.s2 && p.s2.display){ _ubParts.push((p.opp?('vs '+p.opp+' '):'')+p.s2.display); }
  if(p.s5 && s5ValU!=='—'){ _ubParts.push(s5LblU+' BA '+s5ValU); }
  const underBlurb = _ubParts.join(' &#183; ');
  window.__HIT_REG__=window.__HIT_REG__||{}; window.__HIT_REG__['u'+rank]=p;
  return `<div class="mlb-pick-card" onclick="_hitForm('u${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#2a1414 0%,#180808 100%)">${_cardHdr(rank,rnkColors,_catLbl('UNDER','#ff8a65'),teamLogo,p.team,_seriesTag(p,'U',false,0))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,p.name)}
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
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${uOdds}${_bookTag(p)}</span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">U 1.5 Total Bases</span>
        <span style="font-family:monospace;color:#63cab7;font-weight:700;font-size:.9rem">${tbOdds}</span>
      </div>
      ${_seriesChip(p)}
      ${_evBadge(p)}
      ${underBlurb ? `<div style="margin-top:5px;font-size:.72rem;color:#94a3b8;line-height:1.5;font-style:italic">${underBlurb}</div>` : ''}
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
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#0e1f33 0%,#08111d 100%)">${_cardHdr(rank,rnkColors,_catLbl('RUN','#60a5fa'),teamLogo,p.team,_seriesTag(p,(p.pick==='OVER'?'O':'U'),false,3))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,p.name)}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      ${_rateRows(p, scoreClr)}
      ${p.conv_flag?'<div style="font-size:.67rem;color:#4ade80;font-weight:600;margin-top:2px">&#10003; Converged &middot; L10 '+(p.recent_l10||'N/A')+' L5 '+(p.recent_l5||'N/A')+'</div>':(p.cold_flag?'<div style="font-size:.67rem;color:#fb923c;font-weight:600;margin-top:2px">&#9888; Recent diverges &middot; L5 '+(p.recent_l5||'N/A')+'</div>':((p.recent_l10||p.recent_l5)?'<div style="font-size:.67rem;color:#64748b;margin-top:2px">L10 '+(p.recent_l10||'N/A')+' &middot; L5 '+(p.recent_l5||'N/A')+'</div>':''))}
      ${(isOver&&p.hot_disp)?'<div style="font-size:.67rem;color:#fbbf24;font-weight:700;margin-top:2px">&#128293; Hot hand &middot; '+p.hot_disp+' (+'+p.hot_bonus+')</div>':''}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b">Recent</span>
        <span style="font-size:.78rem;color:#cbd5e1">${log.length?recCnt+'/'+log.length:'—'}</span>
      </div>
      ${_dnChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:${pickClr};font-weight:900">${p.pick} ${p.line!=null?p.line:0.5} Runs</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odDisp}${_bookTag(p)}</span>
      </div>
      ${_evBadge(p)}
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
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#1a1200 0%,#0d0900 100%)">${_cardHdr(rank,rnkColors,_catLbl('RBI','#f59e0b'),teamLogo,p.team,_seriesTag(p,(p.pick==='OVER'?'O':'U'),false,4))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,p.name)}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      ${_rateRows(p, scoreClr)}
      ${p.conv_flag?'<div style="font-size:.67rem;color:#4ade80;font-weight:600;margin-top:2px">&#10003; Converged &middot; L10 '+(p.recent_l10||'N/A')+' L5 '+(p.recent_l5||'N/A')+'</div>':(p.cold_flag?'<div style="font-size:.67rem;color:#fb923c;font-weight:600;margin-top:2px">&#9888; Recent diverges &middot; L5 '+(p.recent_l5||'N/A')+'</div>':((p.recent_l10||p.recent_l5)?'<div style="font-size:.67rem;color:#64748b;margin-top:2px">L10 '+(p.recent_l10||'N/A')+' &middot; L5 '+(p.recent_l5||'N/A')+'</div>':''))}
      ${(isOver&&p.hot_disp)?'<div style="font-size:.67rem;color:#fbbf24;font-weight:700;margin-top:2px">&#128293; Hot hand &middot; '+p.hot_disp+' (+'+p.hot_bonus+')</div>':''}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b">Recent</span>
        <span style="font-size:.78rem;color:#cbd5e1">${log.length?recCnt+'/'+log.length:'—'}</span>
      </div>
      ${_dnChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:${pickClr};font-weight:900">${p.pick} ${p.line!=null?p.line:0.5} RBI</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odDisp}${_bookTag(p)}</span>
      </div>
      ${_evBadge(p)}
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
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px';
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
  ov.innerHTML=`<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:820px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">
      <div>
        <div style="font-weight:800;font-size:1.05rem;color:#fff">${name}</div>
        <div style="color:#94a3b8;font-size:.78rem">${p.side||''} vs ${p.opp||''} · ${goal}</div>
      </div>
      <button onclick="document.getElementById('rbi-modal').style.display='none'" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">✕</button>
    </div>
    <div style="padding:14px 18px">
      ${_twoBox(p,'RBI Rate vs Opp','RBI Odds',(isOver?p.over_odds:p.under_odds),isOver,'Last '+(log.length||0)+' Games',rows)}
      ${_oppPitBlock(p,'pitcher_earned_runs','Earned Runs','ER')}
      ${_matrixWriteup(p,(isOver?'O':'U'),4,false,'RBIs',goal)}
      <div style="margin-top:12px;border-top:1px solid #1e293b;padding-top:10px;color:${pickClr};font-weight:800;font-size:.85rem">Pick: ${goal}</div>
    </div>
  </div>`;
  ov.style.display='flex';
}

function _hrCard(p, rank, pfx) {
  pfx = pfx || 'hr';
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const isOver = p.pick==='OVER';
  const rnkColors = rank===1?['#f43f5e','#000']:rank===2?['#fb7185','#000']:rank===3?['#be123c','#fff']:['#1e1e1e','#f43f5e'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const pickClr = isOver?'#63cab7':'#ff8a65';
  const od = isOver?p.over_odds:p.under_odds;
  const odDisp = od!=null?(od>0?'+':'')+od:'—';
  const scoreClr = p.score>=35?'#63cab7':p.score>=20?'#fbbf24':'#ff8a65';
  const log = p.recent_hr_log||[];
  const recCnt = log.filter(g=>g.hr>=1).length;
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>Blended <strong style="color:#f43f5e">${p.score!=null?p.score+'%':'—'}</strong></span> &nbsp;
    <span>Recent <strong style="color:#94a3b8">${p.recent_disp||'—'}</strong></span> &nbsp;
    <span>vs Opp <strong style="color:#94a3b8">${p.team_disp||'—'}</strong></span> &nbsp;
    <span>vs Pit <strong style="color:#94a3b8">${p.pit_disp||'—'}</strong></span>
  </div>`;
  window.__HR_REG__=window.__HR_REG__||{}; window.__HR_REG__[pfx+rank]=p;
  return `<div class="mlb-pick-card" onclick="_hrForm('${pfx}${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#1a0007 0%,#0d0004 100%)">${_cardHdr(rank,rnkColors,_catLbl('HR','#f43f5e'),teamLogo,p.team,_seriesTag(p,(p.pick==='OVER'?'O':'U'),false,2))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,p.name)}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      <div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:5px">
        ${p.pit_hr9_disp?'<span style="font-size:.62rem;font-weight:700;background:#2a0a12;color:#fda4af;padding:2px 7px;border-radius:7px">Pit '+p.pit_hr9_disp+'</span>':''}
        ${p.pit_barrel_disp?'<span style="font-size:.62rem;font-weight:700;background:#2a0a12;color:#fb7185;padding:2px 7px;border-radius:7px">'+p.pit_barrel_disp+'</span>':''}
        ${p.barrel_disp?'<span style="font-size:.62rem;font-weight:700;background:#10241e;color:#6ee7b7;padding:2px 7px;border-radius:7px">'+p.barrel_disp+'</span>':''}
        ${p.platoon_disp?'<span style="font-size:.62rem;font-weight:700;background:#1c1830;color:#c4b5fd;padding:2px 7px;border-radius:7px">'+p.platoon_disp+'</span>':''}
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px">
        <span style="font-size:.78rem;color:#94a3b8">HR Likelihood</span>
        <span style="font-family:monospace;font-weight:700;color:${scoreClr}">${p.score!=null?p.score+'%':'—'} <span style="color:#64748b;font-size:.68rem">blend</span></span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b">Recent HR</span>
        <span style="font-size:.78rem;color:#cbd5e1">${log.length?recCnt+'/'+log.length:(p.recent_disp||'—')}</span>
      </div>
      ${_dnChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:${pickClr};font-weight:900">${p.pick} ${p.line!=null?p.line:0.5} HR</span>
        <span style="font-family:monospace;color:#f43f5e;font-weight:700;font-size:.95rem">${odDisp}${_bookTag(p)}</span>
      </div>
      ${_evBadge(p)}
      ${adminStats}
    </div>
  ${_betBtn(p,'HR',p.pick,'homeRuns','HR',(p.line!=null?p.line:0.5),(p.pick==='OVER'?p.over_odds:p.under_odds))}
  </div>`;
}

function _hrForm(key){
  var p=(key&&typeof key==='object')?key:(window.__HR_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('hr-modal');
  if(!ov){
    ov=document.createElement('div');
    ov.id='hr-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var log=p.recent_hr_log||[];
  var isOver=(p.pick==='OVER');
  var goal=isOver?'Over 0.5 HR (go deep)':'Under 0.5 HR (no homer)';
  var rows=log.length?log.map(function(g){
    var hit=g.hr>=1;
    var good=isOver?hit:!hit;
    var clr=good?'#63cab7':'#ff8a65';
    var oppTxt=g.opp?((g.ha==='H'?'vs ':'@ ')+g.opp):'';
    return `<tr>
      <td style="padding:6px 10px;color:#94a3b8;font-family:monospace">${g.d||'—'}</td>
      <td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">${oppTxt}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-size:.8rem;color:#93c5fd">${g.h} H</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:${clr}">${g.hr} HR</td>
    </tr>`;
  }).join(''):'<tr><td colspan="4" style="padding:14px;color:#64748b;text-align:center">No recent games on record</td></tr>';
  var name=p.full_name||p.name||'';
  var pickClr=isOver?'#63cab7':'#ff8a65';
  ov.innerHTML=`<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:820px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">
      <div>
        <div style="font-weight:800;font-size:1.05rem;color:#fff">${name}</div>
        <div style="color:#94a3b8;font-size:.78rem">${p.side||''} vs ${p.opp||''} · ${goal}</div>
      </div>
      <button onclick="document.getElementById('hr-modal').style.display='none'" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">✕</button>
    </div>
    <div style="padding:14px 18px">
      ${_twoBox(p,'HR Rate vs Opp','HR Odds',(isOver?p.over_odds:p.under_odds),isOver,'Last '+(log.length||0)+' Games',rows)}
      ${_oppPitBlock(p,'pitcher_earned_runs','Earned Runs','ER')}
      ${_matrixWriteup(p,(isOver?'O':'U'),2,false,'HRs',goal)}
      <div style="margin-top:12px;border-top:1px solid #1e293b;padding-top:10px;color:${pickClr};font-weight:800;font-size:.85rem">Pick: ${goal}</div>
    </div>
  </div>`;
  ov.style.display='flex';
}

function _walksCard(p, rank, pfx) {
  pfx = pfx || 'bw';
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const isOver = p.pick==='OVER';
  const rnkColors = rank===1?['#34d399','#000']:rank===2?['#6ee7b7','#000']:rank===3?['#10b981','#fff']:['#1e1e1e','#34d399'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const pickClr = isOver?'#63cab7':'#ff8a65';
  const od = isOver?p.over_odds:p.under_odds;
  const odDisp = od!=null?(od>0?'+':'')+od:'—';
  const scoreClr = p.score>=70?'#63cab7':p.score>=50?'#fbbf24':'#ff8a65';
  const log = p.recent_walks_log||[];
  const recCnt = log.filter(g=>g.bb>=1).length;
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>Score <strong style="color:#34d399">${p.score!=null?p.score+'%':'—'}</strong></span> &nbsp;
    <span>Games <strong style="color:#94a3b8">${p.games||0}</strong></span> &nbsp;
    <span>Wilson <strong style="color:#94a3b8">${p.wilson!=null?p.wilson:'—'}</strong></span>
  </div>`;
  window.__WALKS_REG__=window.__WALKS_REG__||{}; window.__WALKS_REG__[pfx+rank]=p;
  return `<div class="mlb-pick-card" onclick="_walksForm('${pfx}${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#022c22 0%,#01140f 100%)">${_cardHdr(rank,rnkColors,_catLbl('BB','#34d399'),teamLogo,p.team,_seriesTag(p,(p.pick==='OVER'?'O':'U'),false,5))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,p.name)}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      ${_rateRows(p,scoreClr)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b">Recent</span>
        <span style="font-size:.78rem;color:#cbd5e1">${log.length?recCnt+'/'+log.length:'—'}</span>
      </div>
      ${_dnChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:${pickClr};font-weight:900">${p.pick} ${p.line!=null?p.line:0.5} Walks</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odDisp}${_bookTag(p)}</span>
      </div>
      ${_evBadge(p)}
      ${adminStats}
    </div>
  ${_betBtn(p,'Batter Walks',p.pick,'walks_bat','Walks',(p.line!=null?p.line:0.5),(p.pick==='OVER'?p.over_odds:p.under_odds))}
  </div>`;
}

function _walksForm(key){
  var p=(key&&typeof key==='object')?key:(window.__WALKS_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('walks-modal');
  if(!ov){
    ov=document.createElement('div');
    ov.id='walks-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var log=p.recent_walks_log||[];
  var isOver=(p.pick==='OVER');
  var goal=isOver?'Over 0.5 Walks (draw a walk)':'Under 0.5 Walks (no walk)';
  var rows=log.length?log.map(function(g){
    var hit=g.bb>=1;
    var good=isOver?hit:!hit;
    var clr=good?'#63cab7':'#ff8a65';
    var oppTxt=g.opp?((g.ha==='H'?'vs ':'@ ')+g.opp):'';
    return `<tr>
      <td style="padding:6px 10px;color:#94a3b8;font-family:monospace">${g.d||'—'}</td>
      <td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">${oppTxt}</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-size:.8rem;color:#93c5fd">${g.h} H</td>
      <td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:${clr}">${g.bb} BB</td>
    </tr>`;
  }).join(''):'<tr><td colspan="4" style="padding:14px;color:#64748b;text-align:center">No recent games on record</td></tr>';
  var name=p.full_name||p.name||'';
  var pickClr=isOver?'#63cab7':'#ff8a65';
  ov.innerHTML=`<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:820px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">
      <div>
        <div style="font-weight:800;font-size:1.05rem;color:#fff">${name}</div>
        <div style="color:#94a3b8;font-size:.78rem">${p.side||''} vs ${p.opp||''} · ${goal}</div>
      </div>
      <button onclick="document.getElementById('walks-modal').style.display='none'" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">✕</button>
    </div>
    <div style="padding:14px 18px">
      ${_twoBox(p,'Walk Rate vs Opp','Walk Odds',(isOver?p.over_odds:p.under_odds),isOver,'Last '+(log.length||0)+' Games',rows)}
      ${_oppPitBlock(p,'pitcher_walks','Walks Allowed','BB')}
      ${_matrixWriteup(p,(isOver?'O':'U'),5,false,'walks',goal)}
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
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#1a1030 0%,#0e0820 100%)">${_cardHdr(rank,rnkColors,_catLbl('TB','#a78bfa'),teamLogo,p.team,_seriesTag(p,'U',false,1))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,p.name)}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      ${_rateRows(p, scoreClr)}
      ${p.conv_flag?'<div style="font-size:.67rem;color:#4ade80;font-weight:600;margin-top:2px">&#10003; Converged &middot; L10 '+(p.recent_l10||'N/A')+' L5 '+(p.recent_l5||'N/A')+'</div>':(p.cold_flag?'<div style="font-size:.67rem;color:#fb923c;font-weight:600;margin-top:2px">&#9888; Recent diverges &middot; L5 '+(p.recent_l5||'N/A')+'</div>':((p.recent_l10||p.recent_l5)?'<div style="font-size:.67rem;color:#64748b;margin-top:2px">L10 '+(p.recent_l10||'N/A')+' &middot; L5 '+(p.recent_l5||'N/A')+'</div>':''))}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b">Recent</span>
        <span style="font-size:.78rem;color:#cbd5e1">${log.length?underCnt+'/'+log.length+' under':'—'}</span>
      </div>
      ${_dnChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:#a78bfa;font-weight:900">UNDER 1.5 Total Bases</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odDisp}${_bookTag(p)}</span>
      </div>
      ${adminStats}
    </div>
  ${_betBtn(p,'TB Under','UNDER','total_bases','Total Bases',1.5,p.tb_under_odds)}
  </div>`;
}

function _tbOverCard(p, rank) {
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const rnkColors = rank===1?['#4ade80','#000']:rank===2?['#22c55e','#000']:rank===3?['#16a34a','#fff']:['#1e1e1e','#4ade80'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const odDisp = p.tb_over_odds!=null?(p.tb_over_odds>0?'+':'')+p.tb_over_odds:'—';
  const scoreClr = p.score>=80?'#4ade80':p.score>=70?'#fbbf24':'#94a3b8';
  const log = p.recent_tb_log||[];
  const overCnt = log.filter(g=>g.tb>=2).length;
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>Rate <strong style="color:#4ade80">${p.score!=null?p.score+'%':'—'}</strong></span> &nbsp;
    <span>Games <strong style="color:#94a3b8">${p.games||0}</strong></span> &nbsp;
    <span>Wilson <strong style="color:#94a3b8">${p.wilson!=null?p.wilson:'—'}</strong></span>
  </div>`;
  window.__TBO_REG__=window.__TBO_REG__||{}; window.__TBO_REG__['tbo'+rank]=p;
  return `<div class="mlb-pick-card" onclick="_tbOverForm('tbo${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#052e16 0%,#0a1a0a 100%)">${_cardHdr(rank,rnkColors,_catLbl('TBO','#4ade80'),teamLogo,p.team,_seriesTag(p,'O',false,1))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,p.name)}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      ${_rateRows(p, scoreClr)}
      ${p.conv_flag?'<div style="font-size:.67rem;color:#4ade80;font-weight:600;margin-top:2px">&#10003; Converged &middot; L10 '+(p.recent_l10||'N/A')+' L5 '+(p.recent_l5||'N/A')+'</div>':(p.cold_flag?'<div style="font-size:.67rem;color:#fb923c;font-weight:600;margin-top:2px">&#9888; Recent diverges &middot; L5 '+(p.recent_l5||'N/A')+'</div>':((p.recent_l10||p.recent_l5)?'<div style="font-size:.67rem;color:#64748b;margin-top:2px">L10 '+(p.recent_l10||'N/A')+' &middot; L5 '+(p.recent_l5||'N/A')+'</div>':''))}
      ${p.hot_disp?'<div style="font-size:.67rem;color:#fbbf24;font-weight:700;margin-top:2px">&#128293; Hot hand &middot; '+p.hot_disp+' (+'+p.hot_bonus+')</div>':''}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b">Recent</span>
        <span style="font-size:.78rem;color:#cbd5e1">${log.length?overCnt+'/'+log.length+' over':'—'}</span>
      </div>
      ${_dnChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:#4ade80;font-weight:900">OVER 1.5 Total Bases</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odDisp}${_bookTag(p)}</span>
      </div>
      ${_evBadge(p)}
      ${adminStats}
    </div>
  ${_betBtn(p,'TB Over','OVER','total_bases','Total Bases',1.5,p.tb_over_odds)}
  </div>`;
}

function _tbOverForm(key){
  var p=(key&&typeof key==='object')?key:(window.__TBO_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('tb-over-modal');
  if(!ov){
    ov=document.createElement('div');
    ov.id='tb-over-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var log=p.recent_tb_log||[];
  var rows=log.length?log.map(function(g){
    var good=(g.tb>=2);
    var clr=good?'#4ade80':'#ff8a65';
    var oppTxt=g.opp?((g.ha==='H'?'vs ':'@ ')+g.opp):'';
    return '<tr>'
      +'<td style="padding:6px 10px;color:#94a3b8;font-family:monospace">'+(g.d||'\u2014')+'</td>'
      +'<td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">'+oppTxt+'</td>'
      +'<td style="padding:6px 10px;text-align:right;font-family:monospace;font-size:.8rem;color:#93c5fd">'+g.h+' H</td>'
      +'<td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:'+clr+'">'+g.tb+' TB</td>'
    +'</tr>';
  }).join(''):'<tr><td colspan="4" style="padding:14px;color:#64748b;text-align:center">No recent games on record</td></tr>';
  var name=p.name||'';
  ov.innerHTML='<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:820px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">'
      +'<div><div style="font-weight:800;font-size:1.05rem;color:#fff">'+name+'</div>'
      +'<div style="color:#94a3b8;font-size:.78rem">'+(p.side||'')+' vs '+(p.opp||'')+' \u00b7 Over 1.5 Total Bases</div></div>'
      +'<button onclick="document.getElementById(&#39;tb-over-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">\u2715</button>'
    +'</div>'
    +'<div style="padding:16px 18px">'
    +_twoBox(p,'TB Over Rate','TB Over Odds',p.tb_over_odds,true,'Last '+log.length+' Games',rows)
    +_oppPitBlock(p,'pitcher_hits_allowed','Hits Allowed','H')
    +_matrixWriteup(p,'O',1,false,'total bases','Over 1.5 total bases')
    +'</div></div>';
  ov.style.display='flex';
}

// ── Top 10 Plays — helpers + card + form ───────────────────────────────
function _wilsonLB(hits, games) {
  if(!games||!hits) return 0;
  var z=1.96, ph=hits/games;
  var denom=1+z*z/games;
  var num=ph+z*z/(2*games)-z*Math.sqrt(ph*(1-ph)/games+z*z/(4*games*games));
  return num/denom;
}
function _amToDec2(o){ if(!o) return null; return o>0?1+o/100:1+100/Math.abs(o); }
function _t10Score(p, kind) {
  var w = 0;
  if(kind==='HITTER'){
    var s4=p.s4||{}; w = s4.games>0 ? _wilsonLB(s4.hits_games||0, s4.games) : 0;
  } else {
    w = p.wilson||0;
  }
  var od = _t10Odds(p, kind);
  var dec = _amToDec2(od);
  if(!dec) return -999;
  return w*(dec-1)-(1-w);
}
function _t10Odds(p, kind) {
  switch(kind) {
    case 'HITTER':  return p.hit_odds;
    case 'TB OVER': return p.tb_over_odds;
    case 'HRR':     return p.pick==='UNDER'?p.hrr_under_odds:p.hrr_over_odds;
    case 'RBI':     return p.pick==='OVER'?p.over_odds:p.under_odds;
    case 'HR':      return p.pick==='OVER'?p.over_odds:p.under_odds;
    case 'RUNS':    return p.pick==='OVER'?p.over_odds:p.under_odds;
    case 'BWALK':   return p.pick==='OVER'?p.over_odds:p.under_odds;
    case 'UNDER':   return p.pick==='OVER'?p.over_odds:p.under_odds;
    default: return null;
  }
}
function _rateRows(p, clr){
  clr = clr || '#facc15';
  var oppAbbr = (typeof _mlbTeamAbbr==='function' ? _mlbTeamAbbr(p.opp) : '') || (p.opp||'opp');
  var DASH = '—';
  var h2h = (p.h2h_disp && p.h2h_disp!=='N/A' && p.h2h_disp!=='ERR') ? p.h2h_disp : DASH;
  var l10 = (p.l10_disp && p.l10_disp!=='N/A' && p.l10_disp!=='ERR') ? p.l10_disp : DASH;
  function _prr(s){
    if(!s || s===DASH) return null;
    var i=s.indexOf('/');
    if(i>=0){ var d=parseFloat(s.slice(i+1)); return d? parseFloat(s.slice(0,i))/d : null; }
    if(s.indexOf('%')>=0){ var pc=parseFloat(s); return isNaN(pc)?null:pc/100; }
    var n=parseFloat(s); return isNaN(n)?null:n;
  }
  var _isUnder = (p.pick==='UNDER');
  var _rH=_prr(h2h), _rL=_prr(l10), usedH2H;
  if(_rH!=null && _rL!=null){ usedH2H = _isUnder ? (_rH < _rL) : (_rH > _rL); }
  else if(_rH!=null){ usedH2H = true; }
  else if(_rL!=null){ usedH2H = false; }
  else { usedH2H = (p.basis==='vs opp'); }
  function _rr(label, val, used, dim){
    var lblClr = dim ? '#64748b' : '#94a3b8';
    var valClr = dim ? '#94a3b8' : clr;
    var valSz  = dim ? '.78rem' : '.95rem';
    var usedB = used ? ' <span style="font-size:.56rem;background:#06240f;color:#4ade80;padding:1px 4px;border-radius:3px;font-weight:800;vertical-align:middle">USED</span>' : '';
    return '<div style="display:flex;align-items:center;justify-content:space-between;margin-top:'+(dim?'2px':'6px')+'">'
      +'<span style="font-size:.78rem;color:'+lblClr+'">'+label+'</span>'
      +'<span style="font-family:monospace;font-weight:700;color:'+valClr+';font-size:'+valSz+'">'+val+usedB+'</span>'
    +'</div>';
  }
  var hRow = _rr('vs '+oppAbbr, h2h, usedH2H, !usedH2H);
  var lRow = _rr('Last 10 games', l10, !usedH2H, usedH2H);
  return usedH2H ? (hRow+lRow) : (lRow+hRow);
}

function _t10RateDisp(p, kind) {
  if(kind==='HITTER'){ var s4=p.s4||{}; return s4.games>0?s4.hits_games+'/'+s4.games:(p.s4_display||'—'); }
  if(kind==='HR'){ return p.score!=null?p.score+'%':'—'; }
  return p.rate_disp||'—';
}
function _t10Label(kind, p) {
  switch(kind) {
    case 'HITTER':  return 'OVER 0.5 Hits';
    case 'TB OVER': return 'OVER 1.5 Total Bases';
    case 'HRR':     return (p.pick==='UNDER'?'UNDER':'OVER')+' 1.5 H+R+RBI';
    case 'RBI':     return (p.pick||'OVER')+' 0.5 RBI';
    case 'HR':      return (p.pick||'OVER')+' 0.5 HR';
    case 'RUNS':    return (p.pick||'OVER')+' 0.5 Runs';
    case 'BWALK':   return (p.pick||'OVER')+' 0.5 Walks';
    case 'UNDER':   return (p.pick||'UNDER')+' 1.5 Hits';
    default: return kind;
  }
}
function _t10KindColor(kind) {
  switch(kind) {
    case 'HITTER':  return '#f59e0b';
    case 'TB OVER': return '#4ade80';
    case 'HRR':     return '#fb923c';
    case 'RBI':     return '#fbbf24';
    case 'HR':      return '#f43f5e';
    case 'RUNS':    return '#60a5fa';
    case 'BWALK':   return '#34d399';
    case 'UNDER':   return '#ff8a65';
    default: return '#94a3b8';
  }
}
function _t10KindBadge(kind) {
  var abbrs={'HITTER':'HIT','TB OVER':'TB\u2191','HRR':'HRR','RBI':'RBI','HR':'HR','RUNS':'RUN','BWALK':'BB','UNDER':'U-HIT'};
  return abbrs[kind]||kind;
}
// Pitching plays-of-the-day list (Ks + props), EV-sorted, RED lights dropped so
// only green/amber make the Top 10. Shared by the stat count and the card render.
function _buildPitchDay(view){
  var props=(view&&view.pitcher_props)||{};
  var dayList=[];
  var _pk=(view&&view.pitcher_k)||{};
  (_pk.all||[]).forEach(function(p){
    if(!p.pick) return;
    dayList.push({p:p,kind:'K',ev:(p.ev!=null?p.ev:-999)});
  });
  PROP_ORDER.forEach(function(m){
    ((props[m]||{}).picks||[]).forEach(function(p2){
      dayList.push({p:p2,kind:'PROP',ev:(p2.ev!=null?p2.ev:-999)});
    });
  });
  dayList.sort(function(a,b){return b.ev-a.ev;});
  var _pdSeen={};
  dayList=dayList.filter(function(x){ var k=((x.p.name||x.p.full_name||'')+'').trim().toLowerCase(); if(!k) return true; if(_pdSeen[k]) return false; _pdSeen[k]=true; return true; });
  dayList=dayList.filter(function(x){
    var p=x.p;
    if(x.kind==='K'){ var sK=(p.sugg_line!=null||p.pick==='OVER')?'O':'U'; return !_t10DotIsRed(p,sK,true,0); }
    var isOver=(p.pick||'').toUpperCase()==='OVER';
    var ci={pitcher_hits_allowed:1,pitcher_outs:2,pitcher_earned_runs:3,pitcher_walks:4}[p.market];
    return !_t10DotIsRed(p,isOver?'O':'U',true,ci);
  });
  return dayList;
}
function _buildTop10All(view) {
  var plays = [];
  function _add(arr, kind) {
    (arr||[]).forEach(function(p){
      var od = _t10Odds(p, kind);
      if(od==null) return;
      var ev = _t10Score(p, kind);
      if(ev<=-999) return;
      plays.push(Object.assign({}, p, {_t10kind:kind, _t10ev:ev}));
    });
  }
  // Single Hits (OVER 0.5) is EXCLUDED here — it has its own dedicated Top 10 Hits
  // list, so it must not also appear in this combined "plays of the day" card.
  _add(view.tb_over_picks, 'TB OVER');
  _add(view.hrr_picks,     'HRR');
  _add(view.rbi_picks,     'RBI');
  _add(view.runs_picks,    'RUNS');
  _add(view.walks_picks,   'BWALK');
  plays.sort(function(a,b){ return b._t10ev - a._t10ev; });
  var _t10seen={};
  plays=plays.filter(function(p){ var k=(p.name||p.full_name||'').trim().toLowerCase(); if(_t10seen[k]) return false; _t10seen[k]=true; return true; });
  // green/amber lights only (drop ace-faced).
  plays=plays.filter(function(p){ return !_t10DotIsRed(p,'O',false,0); });
  // One pick per team — keep only the best (highest Wilson-EV) play per club so
  // teammates don't crowd the Top 10. plays is already sorted by _t10ev desc.
  var _t10teams={};
  plays=plays.filter(function(p){ var t=((p.team||'')+'').trim().toUpperCase(); if(!t) return true; if(_t10teams[t]) return false; _t10teams[t]=true; return true; });
  return plays;
}
// Top 10 (ranks 1-10). Ranks 11-20 render in the "More Hitter Plays" pulldown via
// _buildTop10All(view).slice(10,20) and are tracked under "Top 10 Batter (OVF)".
function _buildTop10(view) {
  return _buildTop10All(view).slice(0, 10);
}
// Paints the Top 10 body + overflow from __T10_CUR__ (Current model only).
function _renderT10Section(){
  var v=window.__T10_CUR__||[];
  var plays=v.slice(0,10), more=v.slice(10,20);
  window.__T10_REG__={};
  var body=document.getElementById('top10-plays-body'); if(!body) return;
  body.innerHTML = plays.length
    ? plays.map(function(p,i){ return _top10Card(p, i+1); }).join('')
    : '<p class="text-slate-500 text-center" style="padding:16px">No plays today</p>';
  var mw=document.getElementById('top10-more-wrap');
  if(mw) mw.innerHTML = more.length>0
    ? _moreWrap(more, function(p,r){ return _top10Card(p, r); }, 11, 'Hitter Plays', '#facc15')
    : '';
}
function _top10BetBtn(p, kind) {
  switch(kind) {
    case 'HITTER':  return _betBtn(p,'Top 10 Batter','OVER','hits','Hits',0.5,p.hit_odds);
    case 'TB OVER': return _betBtn(p,'Top 10 Batter','OVER','total_bases','Total Bases',1.5,p.tb_over_odds);
    case 'HRR':     { var odh=p.pick==='UNDER'?p.hrr_under_odds:p.hrr_over_odds; return _betBtn(p,'Top 10 Batter',(p.pick==='UNDER'?'UNDER':'OVER'),'hrr','H+R+RBI',1.5,odh); }
    case 'RBI':     { var od=p.pick==='OVER'?p.over:p.under; return _betBtn(p,'Top 10 Batter',(p.pick||'OVER'),'rbi','RBI',(p.line||0.5),od); }
    case 'HR':      { var odhr=p.pick==='OVER'?p.over_odds:p.under_odds; return _betBtn(p,'Top 10 Batter',(p.pick||'OVER'),'homeRuns','HR',(p.line||0.5),odhr); }
    case 'RUNS':    { var od2=p.pick==='OVER'?p.over_odds:p.under_odds; return _betBtn(p,'Top 10 Batter',(p.pick||'OVER'),'runs','Runs',(p.line||0.5),od2); }
    case 'BWALK':   { var odw=p.pick==='OVER'?p.over_odds:p.under_odds; return _betBtn(p,'Top 10 Batter',(p.pick||'OVER'),'walks_bat','Walks',(p.line||0.5),odw); }
    case 'UNDER':   { var od3=p.pick==='OVER'?p.over_odds:p.under_odds; return _betBtn(p,'Top 10 Batter',(p.pick||'UNDER'),'hits','Hits',1.5,od3); }
    default: return '';
  }
}
function _top10Form(key) {
  var rec=(window.__T10_REG__||{})[key]; if(!rec) return;
  var p=rec.p, kind=rec.kind;
  if(kind==='HITTER'||kind==='UNDER') { _hitForm(p); }
  else if(kind==='TB OVER') { _tbOverForm(p); }
  else if(kind==='HRR') { _hrrForm(p); }
  else if(kind==='RBI') { if(typeof _rbiForm==='function') _rbiForm(p); }
  else if(kind==='HR') { if(typeof _hrForm==='function') _hrForm(p); }
  else if(kind==='RUNS') { if(typeof _runsForm==='function') _runsForm(p); }
  else if(kind==='BWALK') { if(typeof _walksForm==='function') _walksForm(p); }
}
function _top10Card(p, rank) {
  var kind = p._t10kind;
  var abbr = _mlbTeamAbbr(p.team||'');
  var teamLogo = abbr ? 'https://a.espncdn.com/i/teamlogos/mlb/500/'+abbr+'.png' : '';
  var kc = _t10KindColor(kind);
  var rnkColors = rank===1?['#facc15','#000']:rank===2?['#eab308','#000']:rank===3?['#ca8a04','#fff']:['#1e1e1e','#facc15'];
  var sideCls = (p.side||'')==='HOME'?'badge-home':'badge-away';
  var od = _t10Odds(p, kind);
  var odDisp = od!=null?(od>0?'+':'')+od:'—';
  var rate = _t10RateDisp(p, kind);
  var label = _t10Label(kind, p);
  var ev = p._t10ev!=null?(p._t10ev>0?'+':'')+p._t10ev.toFixed(3):'—';
  var key = 't10r'+rank;
  window.__T10_REG__=window.__T10_REG__||{}; window.__T10_REG__[key]={p:p,kind:kind};
  return '<div class="mlb-pick-card" onclick="_top10Form(\\''+key+'\\')" title="Click for recent form" style="cursor:pointer">'
    +'<div class="mlb-card-header" style="background:linear-gradient(135deg,#2d2600 0%,#0f0e00 100%)">'
      +_cardHdr(rank,rnkColors,'<span style="font-size:.66rem;letter-spacing:.1em;background:'+kc+';color:#000;padding:2px 6px;border-radius:4px;font-weight:900">'+_t10KindBadge(kind)+'</span>',teamLogo,(p.team||''),_seriesTag(p,'O',false,0))
    +'</div>'
    +_nameBar(rank,rnkColors,p.batter_id,(p.name||p.full_name||''))
    +'<div class="mlb-card-body">'
      +'<div style="display:flex;align-items:center;justify-content:space-between">'
        +'<span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">'+(p.opp||'—')+'</strong></span>'
        +(p.side?'<span class="badge '+sideCls+'">'+(p.side)+'</span>':'')
      +'</div>'
      +_envChip(p)+_umpChip(p)+_bpChip(p)
      +((p.h2h_disp||p.l10_disp)?_rateRows(p,'#facc15'):'<div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px"><span style="font-size:.78rem;color:#94a3b8">Rate vs opp</span><span style="font-family:monospace;font-weight:700;color:#facc15">'+rate+'</span></div>')
      +_dnChip(p)
      +'<div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">'
        +'<span style="font-size:.8rem;color:'+kc+';font-weight:900">'+label+'</span>'
        +'<span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">'+odDisp+_bookTag(p)+'</span>'
      +'</div>'
      +'<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px">EV <strong style="color:#facc15">'+ev+'</strong></div>'
    +'</div>'
    +_top10BetBtn(p, kind)
    +'</div>';
}

// ── SECTION 2: Value Plays board ───────────────────────────────────────
// Per hitter, collect every plus-money (+odds) OVER value market (RBI / Total
// Bases / Runs / Walks / H+R+RBI) and rank by a 3-standard PARTIAL score that
// mirrors the approved board: (1) HOT recent form, (2) career vs the PITCHER,
// (3) rate vs the opponent TEAM. The composite is the geometric mean of only
// the standards that have data, so a missing standard is "never faced", not a
// zero. Built off the FULL slate (display only — these plays are already
// tracked under their own native categories).
function _valNum(s){
  s=String(s||''); var i=s.indexOf('/'); if(i<0) return [null,null];
  var a=parseInt(s.slice(0,i),10), b=parseInt(s.slice(i+1),10);
  return [isNaN(a)?null:a, isNaN(b)?null:b];
}
function _valRate(s){ var a=_valNum(s); return (a[1]&&a[1]>0)?a[0]/a[1]:null; }
function _valBA(disp){
  disp=String(disp||''); var i=disp.indexOf('.'); if(i<0) return null;
  var dd=''; for(var j=i+1;j<disp.length;j++){ var ch=disp.charAt(j); if(ch>='0'&&ch<='9') dd+=ch; else break; }
  return dd?parseFloat('0.'+dd.slice(0,3)):null;
}
function _buildValuePlays(result){
  if(!result) return [];
  var MK=[['1+ RBI','rbi_picks','over_odds'],
          ['2+ Total Bases','tb_over_picks','tb_over_odds'],
          ['1+ Run','runs_picks','over_odds'],
          ['1+ Walk','walks_picks','over_odds'],
          ['2+ H+R+RBI','hrr_picks','hrr_over_odds']];
  var byPid={};
  MK.forEach(function(mk){
    var label=mk[0], arr=result[mk[1]]||[], fld=mk[2];
    arr.forEach(function(p){
      var o=p[fld]; if(o==null||+o<=0) return;              // plus-money only
      var pid=p.batter_id; if(pid==null) return;
      var e=byPid[pid]||(byPid[pid]={plays:{},stat:null});
      var vp=p.vs_pit||{};
      if(!e.stat||(vp.ab&&!((e.stat.vs_pit||{}).ab))) e.stat=p;     // keep richest record
      if(!(label in e.plays)||+o<e.plays[label]) e.plays[label]=+o; // one per market, safest +odds
    });
  });
  var out=[];
  Object.keys(byPid).forEach(function(pid){
    var e=byPid[pid], s=e.stat||{};
    var l10=_valRate(s.recent_l10), l5=_valRate(s.recent_l5); if(l5==null) l5=l10;
    var streak=s.hot_bonus||0;
    var hot=(l10!=null)?100*(0.55*(l10||0)+0.30*(l5||0)+0.15*Math.min(streak/13,1)):null;
    var vp=s.vs_pit||{}, vpab=vp.ab||0, ba=_valBA(vp.display), vsP=null;
    if(vpab&&ba!=null){ var shr=(ba*vpab+0.25*5)/(vpab+5); vsP=Math.max(0,Math.min(100,(shr-0.15)/0.30*100)); }
    var hh=_valNum(s.h2h_disp), vsT=null;
    if(hh[1]) vsT=Math.max(0,Math.min(100,100*(hh[0]+0.6*2)/(hh[1]+2)));
    var avail=[hot,vsP,vsT].filter(function(v){return v!=null;});
    if(!avail.length) return;
    var prod=1; avail.forEach(function(v){prod*=Math.max(v,1);});
    var comp=Math.pow(prod,1/avail.length);
    var plays=Object.keys(e.plays).map(function(l){return [e.plays[l],l];}).sort(function(a,b){return a[0]-b[0];});
    out.push(Object.assign({}, s, {
      _hot:(hot!=null?Math.round(hot):null),
      _vsP:(vsP!=null?Math.round(vsP):null),
      _vsT:(vsT!=null?Math.round(vsT):null),
      _comp:Math.round(comp*10)/10, _ncov:avail.length, _plays:plays, _vpdisp:vp.display}));
  });
  out.sort(function(a,b){return b._comp-a._comp;});
  return out;
}
function _valForm(key){
  var p=(window.__VAL_REG__||{})[key]; if(!p) return;
  var q={}; for(var k in p){ if(Object.prototype.hasOwnProperty.call(p,k)) q[k]=p[k]; }
  q.pick='OVER';   // value-board plays are always the +odds OVER market
  if(q.recent_rbi_log!==undefined){ _rbiForm(q); }
  else if(q.recent_tb_log!==undefined){ _tbOverForm(q); }
  else if(q.recent_runs_log!==undefined){ _runsForm(q); }
  else if(q.recent_walks_log!==undefined){ _walksForm(q); }
  else if(q.recent_hrr_log!==undefined){ _hrrForm(q); }
  else if(q.recent_hr_log!==undefined){ _hrForm(q); }
  else { _hitForm(q); }
}
function _valStdRow(lbl, val, det, blank){
  var v=blank?'<span style="color:#64748b;font-weight:800">&mdash;</span>':('<span style="font-weight:800;color:#e2e8f0">'+val+'</span>');
  return '<div style="display:flex;align-items:center;justify-content:space-between;margin-top:3px">'
    +'<span style="font-size:.7rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.04em">'+lbl+'</span>'
    +'<span style="display:flex;align-items:center;gap:8px"><span style="font-size:.66rem;color:#64748b">'+(det||'')+'</span>'+v+'</span></div>';
}
function _valueCard(p, rank){
  var abbr=_mlbTeamAbbr(p.team);
  var teamLogo=abbr?('https://a.espncdn.com/i/teamlogos/mlb/500/'+abbr+'.png'):'';
  var rnkColors=rank===1?['#22d3ee','#000']:rank===2?['#67e8f9','#000']:rank===3?['#0891b2','#fff']:['#1e1e1e','#22d3ee'];
  var sideCls=p.side==='HOME'?'badge-home':'badge-away';
  var c=p._comp;
  var cClr=c>=85?'#22c55e':c>=70?'#4ade80':c>=55?'#86efac':'#94a3b8';
  var hotDet=(p.recent_l10?('L10 '+p.recent_l10):'')+(p.recent_l5?(' \u00b7 L5 '+p.recent_l5):'');
  var pitDet=p._vsP!=null?String(p._vpdisp||''):'never faced';
  var teamDet=p._vsT!=null?('hit '+(p.h2h_disp||'')+' g'):'no history';
  var rows=_valStdRow('Hot',p._hot,hotDet,p._hot==null)
    +_valStdRow('vs Pitcher',p._vsP,pitDet,p._vsP==null)
    +_valStdRow('vs Team',p._vsT,teamDet,p._vsT==null);
  // Each value market maps to its native category/stat key so Track Bet + Parlay
  // grade and settle exactly like the standalone RBI/TB/Runs/Walks/HRR cards.
  var _VAL_BET={'1+ RBI':['RBI','OVER','rbi','RBI',0.5],
    '2+ Total Bases':['TB Over','OVER','total_bases','Total Bases',1.5],
    '1+ Run':['Runs','OVER','runs','Runs',0.5],
    '1+ Walk':['Batter Walks','OVER','walks_bat','Walks',0.5],
    '2+ H+R+RBI':['HRR','OVER','hrr','H+R+RBI',1.5]};
  var plays=(p._plays||[]).map(function(pl){
    var m=_VAL_BET[pl[1]];
    var bb=m?_betBtn(p,m[0],m[1],m[2],m[3],m[4],pl[0]):'';
    return '<div style="margin-top:3px">'
      +'<div style="display:flex;align-items:center;justify-content:space-between">'
      +'<span style="font-size:.78rem;color:#cbd5e1">'+pl[1]+'</span>'
      +'<span style="font-family:monospace;font-weight:800;color:#34d399">+'+pl[0]+'</span></div>'
      +bb+'</div>';
  }).join('');
  window.__VAL_REG__=window.__VAL_REG__||{}; window.__VAL_REG__['vr'+rank]=p;
  return `<div class="mlb-pick-card" onclick="_valForm('vr${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#06303a 0%,#02161c 100%)">${_cardHdr(rank,rnkColors,'<span style="font-size:.66rem;letter-spacing:.1em;background:#22d3ee;color:#000;padding:2px 6px;border-radius:4px;font-weight:900">VALUE</span>',teamLogo,(p.team||''),_seriesTag(p,'O',false,0))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,(p.name||p.full_name||''))}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'\u2014'}</strong></span>
        ${p.side?`<span class="badge ${sideCls}">${p.side}</span>`:''}
      </div>
      ${p.pitcher?`<div style="font-size:.78rem;color:#64748b;margin-top:2px">vs ${p.pitcher}</div>`:''}
      ${_dnChip(p)}
      <div style="margin-top:8px;padding-top:8px;border-top:1px solid #1f1f1f">
        <div style="font-size:.6rem;font-weight:800;letter-spacing:.07em;color:#22d3ee;text-transform:uppercase">3 Standards</div>
        ${rows}
      </div>
      <div style="margin-top:8px;padding-top:8px;border-top:1px solid #1f1f1f">
        <div style="font-size:.6rem;font-weight:800;letter-spacing:.07em;color:#34d399;text-transform:uppercase">+Odds Value Plays</div>
        ${plays||'<div style="font-size:.72rem;color:#64748b;margin-top:3px">\u2014</div>'}
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:8px;padding-top:8px;border-top:1px solid #1f1f1f">
        <span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">3-Std Score</span>
        <span style="display:flex;align-items:baseline;gap:6px"><span style="font-weight:900;font-size:1.05rem;color:${cClr}">${c}</span><span style="font-size:.62rem;color:#64748b">${p._ncov}/3</span></span>
      </div>
    </div>
  </div>`;
}

function _hrrCard(p, rank, pfx) {
  pfx = pfx || 'hrr';
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const isOver = p.pick!=='UNDER';
  const rnkColors = rank===1?['#fb923c','#000']:rank===2?['#f97316','#000']:rank===3?['#ea580c','#fff']:['#1e1e1e','#fb923c'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const pickClr = isOver?'#fb923c':'#ff8a65';
  const od = isOver?p.hrr_over_odds:p.hrr_under_odds;
  const odDisp = od!=null?(od>0?'+':'')+od:'—';
  const scoreClr = isOver?(p.score>=80?'#fb923c':p.score>=70?'#fbbf24':'#94a3b8'):(p.score<=20?'#63cab7':p.score<=30?'#fbbf24':'#94a3b8');
  const log = p.recent_hrr_log||[];
  const overCnt = log.filter(g=>g.hrr>=2).length;
  const recDisp = log.length?(isOver?(overCnt+'/'+log.length+' over'):((log.length-overCnt)+'/'+log.length+' under')):'—';
  const adminStats = `<div class="admin-only" style="display:none;font-size:.72rem;color:#64748b;margin-top:4px;line-height:1.7">
    <span>Rate <strong style="color:#fb923c">${p.score!=null?p.score+'%':'—'}</strong></span> &nbsp;
    <span>Games <strong style="color:#94a3b8">${p.games||0}</strong></span> &nbsp;
    <span>Wilson <strong style="color:#94a3b8">${p.wilson!=null?p.wilson:'—'}</strong></span>
  </div>`;
  window.__HRR_REG__=window.__HRR_REG__||{}; window.__HRR_REG__[pfx+rank]=p;
  return `<div class="mlb-pick-card" onclick="_hrrForm('${pfx}${rank}')" title="Click for recent form" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#431407 0%,#1a0a00 100%)">${_cardHdr(rank,rnkColors,_catLbl('HRR','#fb923c'),teamLogo,p.team,_seriesTag(p,(isOver?'O':'U'),false,2))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,p.name)}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      ${_rateRows(p, scoreClr)}
      ${p.conv_flag?'<div style="font-size:.67rem;color:#4ade80;font-weight:600;margin-top:2px">&#10003; Converged &middot; L10 '+(p.recent_l10||'N/A')+' L5 '+(p.recent_l5||'N/A')+'</div>':(p.cold_flag?'<div style="font-size:.67rem;color:#fb923c;font-weight:600;margin-top:2px">&#9888; Recent diverges &middot; L5 '+(p.recent_l5||'N/A')+'</div>':((p.recent_l10||p.recent_l5)?'<div style="font-size:.67rem;color:#64748b;margin-top:2px">L10 '+(p.recent_l10||'N/A')+' &middot; L5 '+(p.recent_l5||'N/A')+'</div>':''))}
      ${(isOver&&p.hot_disp)?'<div style="font-size:.67rem;color:#fbbf24;font-weight:700;margin-top:2px">&#128293; Hot hand &middot; '+p.hot_disp+' (+'+p.hot_bonus+')</div>':''}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <span style="font-size:.72rem;color:#64748b">Recent</span>
        <span style="font-size:.78rem;color:#cbd5e1">${recDisp}</span>
      </div>
      ${_dnChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:${pickClr};font-weight:900">${isOver?'OVER':'UNDER'} 1.5 H+R+RBI</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odDisp}${_bookTag(p)}</span>
      </div>
      ${_evBadge(p)}
      ${adminStats}
    </div>
  ${_betBtn(p,'HRR',(isOver?'OVER':'UNDER'),'hrr','H+R+RBI',1.5,od)}
  </div>`;
}

function _hrrSpCard(p, rank, pfx) {
  pfx = pfx || 'hrsp';
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const rnkColors = rank===1?['#c4b5fd','#000']:rank===2?['#a78bfa','#000']:rank===3?['#8b5cf6','#fff']:['#1e1e1e','#a78bfa'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const od = p.hrr_over_odds;
  const odDisp = od!=null?(od>0?'+':'')+od:'—';
  const s5 = p.s5||{};
  const dnLbl = p.dn_label||'Day/Night';
  const dnDisp = s5.display||'—';
  window.__HRR_REG__=window.__HRR_REG__||{}; window.__HRR_REG__[pfx+rank]=p;
  function _g(lbl,val){
    return '<div style="display:flex;align-items:center;justify-content:space-between;font-size:.72rem;margin-top:4px">'
      +'<span style="color:#94a3b8"><span style="color:#4ade80">&#10003;</span> '+lbl+'</span>'
      +'<span style="color:#ddd6fe;font-weight:700;font-family:monospace">'+val+'</span></div>';
  }
  return `<div class="mlb-pick-card" onclick="_hrrForm('${pfx}${rank}')" title="Click for recent form" style="cursor:pointer;border:1px solid rgba(167,139,250,.4)">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#2e1065 0%,#10071f 100%)">${_cardHdr(rank,rnkColors,_catLbl('HRR','#a78bfa'),teamLogo,p.team,_seriesTag(p,'O',false,2))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,p.name)}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      <div style="margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <div style="font-size:.6rem;font-weight:800;letter-spacing:.07em;color:#a78bfa;text-transform:uppercase">4 Gates Cleared</div>
        ${_g('BA vs '+(p.pitcher||'pitcher'), p.vsp_ba_disp||'—')}
        ${_g('vs team (H/A)', (p.vsteam_disp||'—')+(p.vsteam_score!=null?' &middot; '+p.vsteam_score+'%':''))}
        ${_g('Last 10 (H/A)', (p.l10_disp||'—')+(p.l10_score!=null?' &middot; '+p.l10_score+'%':''))}
        ${_g(dnLbl+' BA', dnDisp)}
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:8px;padding-top:8px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:#a78bfa;font-weight:900">OVER 1.5 H+R+RBI</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odDisp}${_bookTag(p)}</span>
      </div>
    </div>
  ${_betBtn(p,'HRR','OVER','hrr','H+R+RBI',1.5,od)}
  </div>`;
}

function _tscCard(p, rank, pfx) {
  pfx = pfx || 'tsc';
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const rnkColors = rank===1?['#67e8f9','#000']:rank===2?['#22d3ee','#000']:rank===3?['#06b6d4','#fff']:['#0e2b33','#22d3ee'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const od = p.hit_odds;
  const odDisp = od!=null?(od>0?'+':'')+od:'—';
  const dnLbl = p.dn_label||'Day/Night';
  const gno = p.series_gno||p.series_game||'';
  window.__TSC_REG__=window.__TSC_REG__||{}; window.__TSC_REG__[pfx+rank]=p;
  function _g(lbl,val){
    return '<div style="display:flex;align-items:center;justify-content:space-between;font-size:.72rem;margin-top:4px">'
      +'<span style="color:#94a3b8"><span style="color:#22d3ee">&#10003;</span> '+lbl+'</span>'
      +'<span style="color:#a5f3fc;font-weight:700;font-family:monospace">'+val+'</span></div>';
  }
  return `<div class="mlb-pick-card" onclick="_hitForm(window.__TSC_REG__['${pfx}${rank}'])" title="Click for recent form" style="cursor:pointer;border:1px solid rgba(34,211,238,.4)">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#083344 0%,#04141c 100%)">${_cardHdr(rank,rnkColors,_catLbl('TRIPLE','#22d3ee'),teamLogo,p.team,_seriesTag(p,'O',false,2))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,p.name)}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      <div style="margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <div style="font-size:.6rem;font-weight:800;letter-spacing:.07em;color:#22d3ee;text-transform:uppercase">All 3 Splits &gt; .275</div>
        ${_g((p.side==='HOME'?'Home':'Away')+' BA', p.ha_disp||'—')}
        ${_g(dnLbl+' BA', p.dn_disp||'—')}
        ${_g('Series G'+(gno||'?')+' BA', p.series_disp||'—')}
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:8px;padding-top:8px;border-top:1px solid #1f1f1f">
        <span style="font-size:.8rem;color:#22d3ee;font-weight:900">TO RECORD A HIT</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.95rem">${odDisp}${_bookTag(p)}</span>
      </div>
    </div>
  ${_betBtn(p,'Triple Split Club','OVER','hits','Hits',0.5,od)}
  </div>`;
}

function _hrrForm(key){
  var p=(key&&typeof key==='object')?key:(window.__HRR_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('hrr-modal');
  if(!ov){
    ov=document.createElement('div');
    ov.id='hrr-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var isUnder=(p.pick==='UNDER');
  var log=p.recent_hrr_log||[];
  var rows=log.length?log.map(function(g){
    var good=isUnder?(g.hrr<2):(g.hrr>=2);
    var clr=good?'#fb923c':'#94a3b8';
    var oppTxt=g.opp?((g.ha==='H'?'vs ':'@ ')+g.opp):'';
    return '<tr>'
      +'<td style="padding:6px 10px;color:#94a3b8;font-family:monospace">'+(g.d||'\u2014')+'</td>'
      +'<td style="padding:6px 10px;color:#cbd5e1;font-size:.8rem">'+oppTxt+'</td>'
      +'<td style="padding:6px 10px;text-align:right;font-family:monospace;font-size:.8rem;color:#93c5fd">'+g.h+'H '+g.r+'R '+g.rbi+'RBI</td>'
      +'<td style="padding:6px 10px;text-align:right;font-family:monospace;font-weight:800;color:'+clr+'">'+g.hrr+' HRR</td>'
    +'</tr>';
  }).join(''):'<tr><td colspan="4" style="padding:14px;color:#64748b;text-align:center">No recent games on record</td></tr>';
  var name=p.name||'';
  ov.innerHTML='<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:820px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">'
      +'<div><div style="font-weight:800;font-size:1.05rem;color:#fff">'+name+'</div>'
      +'<div style="color:#94a3b8;font-size:.78rem">'+(p.side||'')+' vs '+(p.opp||'')+' \u00b7 '+(isUnder?'Under':'Over')+' 1.5 H+R+RBI</div></div>'
      +'<button onclick="document.getElementById(&#39;hrr-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">\u2715</button>'
    +'</div>'
    +'<div style="padding:16px 18px">'
    +_twoBox(p,'HRR Rate','HRR Odds',(isUnder?p.hrr_under_odds:p.hrr_over_odds),!isUnder,(isUnder?'H+R+RBI \u2264 1 = UNDER':'H+R+RBI \u2265 2 = OVER')+' \u00b7 Last '+log.length+' Games',rows)
    +_oppPitBlock(p,'pitcher_hits_allowed','Hits Allowed','H')
    +_matrixWriteup(p,(isUnder?'U':'O'),2,false,'HRR (hits+runs+RBI)',(isUnder?'Under 1.5 H+R+RBI':'Over 1.5 H+R+RBI'))
    +'</div></div>';
  ov.style.display='flex';
}

function _tbForm(key){
  var p=(key&&typeof key==='object')?key:(window.__TB_REG__||{})[key]; if(!p) return;
  var ov=document.getElementById('tb-modal');
  if(!ov){
    ov=document.createElement('div');
    ov.id='tb-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px';
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
  ov.innerHTML='<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:820px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">'
      +'<div><div style="font-weight:800;font-size:1.05rem;color:#fff">'+name+'</div>'
      +'<div style="color:#94a3b8;font-size:.78rem">'+(p.side||'')+' vs '+(p.opp||'')+' \u00b7 Under 1.5 Total Bases</div></div>'
      +'<button onclick="document.getElementById(&#39;tb-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">\u2715</button>'
    +'</div>'
    +'<div style="padding:14px 18px">'
      +_twoBox(p,'TB Under Rate','TB Under Odds',p.tb_under_odds,false,'Last '+log.length+' Games',rows)
      +_oppPitBlock(p,'pitcher_hits_allowed','Hits Allowed','H')
      +_matrixWriteup(p,'U',1,false,'total bases','Under 1.5 total bases')
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
  const pf = p.proj_factors||{};
  const hasProj = p.proj_k!=null;
  const projDisp = hasProj?(p.proj_k+'K'):'—';
  const factTxt = hasProj?('Hand x'+(pf.hand!=null?pf.hand:1)+' · Whiff x'+(pf.whiff!=null?pf.whiff:1)+' · Rest x'+(pf.rest!=null?pf.rest:1)):'';
  window.__PK_REG__=window.__PK_REG__||{}; window.__PK_REG__[keyPfx+rank]=p;
  return `<div class="mlb-pick-card" onclick="_pkForm('${keyPfx}${rank}')" title="Click for all 5 markets" style="cursor:pointer">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#0f2420 0%,#08160f 100%)">${_cardHdr(rank,rnkColors,_catLbl('P','#63cab7'),teamLogo,p.team,_seriesTag(p,((p.sugg_line!=null||p.pick==='OVER')?'O':'U'),true,0))}</div>
    ${_nameBar(rank,rnkColors,p.pid,p.name)}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      ${_envChip(p)}
      ${_umpChip(p)}
      ${_bpChip(p)}
      ${_kRankChip(p)}
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">K Line ${p.line!=null?p.line:'—'}</span>
        <span style="color:${pickClr};font-weight:900;font-size:1rem">${pickLabel}</span>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:3px">
        <span style="font-size:.72rem;color:#64748b">${hasProj?('Proj '+projDisp+' · blend '+blDisp):('Blend '+blDisp)}</span>
        <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.9rem">${odds||'—'}${_bookTag(p)}</span>
      </div>
      ${hasProj?`<div style="margin-top:2px;font-size:.62rem;color:#475569">${factTxt}</div>`:''}
      <div style="margin-top:5px;font-size:.68rem;color:#94a3b8;line-height:1.6">K <strong style="color:#cbd5e1">${p.avg_k!=null?p.avg_k:'—'}</strong> · H <strong style="color:#cbd5e1">${p.avg_hits!=null?p.avg_hits:'—'}</strong> · ER <strong style="color:#cbd5e1">${p.avg_er!=null?p.avg_er:'—'}</strong> · Outs <strong style="color:#cbd5e1">${p.avg_outs!=null?p.avg_outs:'—'}</strong> · BB <strong style="color:#cbd5e1">${p.avg_bb!=null?p.avg_bb:'—'}</strong> · IP <strong style="color:#cbd5e1">${p.avg_ip!=null?p.avg_ip:'—'}</strong> · ERA <strong style="color:#cbd5e1">${p.era||'—'}</strong> <span style="color:#64748b">vr opp</span></div>
      ${p.k_consistency==='consistent'?'<div style="font-size:.66rem;color:#4ade80;margin-top:3px">&#10003; K Consistent (std '+(p.k_std||0)+')</div>':p.k_consistency==='volatile'?'<div style="font-size:.66rem;color:#fbbf24;margin-top:3px">&#126; K Volatile (std '+(p.k_std||0)+')</div>':p.k_consistency==='boom_bust'?'<div style="font-size:.66rem;color:#fb923c;margin-top:3px">&#9888; Boom/Bust K (std '+(p.k_std||0)+')</div>':''}
      <div style="margin-top:5px;display:flex;align-items:center;justify-content:flex-end"><span style="font-size:.66rem;color:#63cab7">all 5 markets →</span></div>
      ${_krateBadge(p)}
      ${_veloBadge(p)}
      ${_evBadge(p)}
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
// Pic-2 category dropdowns (replace the long stats-tile row). Two menus -
// Hitters / Pitchers - each item jumps to its card and shows a live count.
function _catItemHTML(it){
  const tone = it.tone==='over'?'#63cab7':(it.tone==='under'?'#ff8a65':'#9fb0bd');
  return `<button onclick="_catJump('${it.target}')" onmouseenter="this.style.background='rgba(255,255,255,.05)'" onmouseleave="this.style.background='transparent'" style="width:100%;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 12px;border-radius:10px;border:none;background:transparent;color:#dce6ec;font-size:.9rem;font-weight:600;cursor:pointer;text-align:left"><span style="display:flex;align-items:center;gap:10px"><span style="font-size:1rem">${it.icon}</span>${it.label}</span><span style="min-width:30px;text-align:center;padding:2px 8px;border-radius:999px;font-size:.78rem;font-weight:800;color:${tone};background:${tone}1f">${it.count}</span></button>`;
}
function _catMenuHTML(id,title,accent,items){
  const body=items.map(_catItemHTML).join('');
  return `<div class="catmenu-wrap" style="position:relative;width:240px;max-width:100%"><button onclick="_catMenuTog('${id}')" style="width:100%;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:13px 16px;border-radius:14px;border:1px solid ${accent}55;background:rgba(255,255,255,.03);color:#e8eef2;font-size:1rem;font-weight:700;cursor:pointer"><span style="display:flex;align-items:center;gap:10px"><span style="width:10px;height:10px;border-radius:999px;background:${accent};box-shadow:0 0 10px ${accent}"></span>${title}</span><span id="${id}-chev" class="catmenu-chev" style="transition:transform .15s;opacity:.8">▾</span></button><div id="${id}" class="catmenu-panel" style="display:none;position:absolute;top:calc(100% + 8px);left:0;right:0;z-index:200;padding:6px;border-radius:14px;border:1px solid rgba(255,255,255,.08);background:#0e151c;box-shadow:0 18px 40px rgba(0,0,0,.55);max-height:360px;overflow-y:auto">${body}</div></div>`;
}
function _metaPill(icon,label,val,target){
  const clk = target?` onclick="_jumpTo('${target}')"`:'';
  const cur = target?';cursor:pointer':'';
  return `<span${clk} style="display:inline-flex;align-items:center;gap:6px;padding:9px 13px;border-radius:10px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);color:#9fb0bd;font-size:.8rem;font-weight:700${cur}"><span>${icon}</span><span style="color:#e8eef2;font-weight:800">${val}</span> ${label}</span>`;
}
function _catClose(){ document.querySelectorAll('.catmenu-panel').forEach(function(p){ p.style.display='none'; }); document.querySelectorAll('.catmenu-chev').forEach(function(c){ c.style.transform='none'; }); }
function _catMenuTog(id){ const p=document.getElementById(id); if(!p) return; const willOpen=p.style.display!=='block'; _catClose(); if(willOpen){ p.style.display='block'; const c=document.getElementById(id+'-chev'); if(c) c.style.transform='rotate(180deg)'; } }
function _catJump(t){ _catClose(); var ids=String(t).split(','); for(var i=0;i<ids.length;i++){ var el=document.getElementById(ids[i]); if(el && !el.classList.contains('hidden')){ _jumpTo(ids[i]); return; } } _jumpTo(ids[0]); }
function _renderCatBar(view){
  const top9=view.top9||[], stats=view.stats||{}, pk=(view.pitcher_k||{}), pp=(view.pitcher_props||{});
  function pc(m){ return (((pp[m]||{}).picks)||[]).length; }
  const HIT=[
    {icon:'⭐',label:'Top 10 Hitters',count:_buildTop10(view).length,target:'top10-plays-card'},
    {icon:'🎯',label:'Record a Hit',count:top9.length,target:'top-picks-card'},
    {icon:'⬇️',label:'U1.5 Hits',count:(view.under_picks||[]).length,target:'under-picks-card',tone:'under'},
    {icon:'📈',label:'TB Over',count:(view.tb_over_picks||[]).length,target:'tb-over-picks-card',tone:'over'},
    {icon:'⬇️',label:'TB Under',count:(view.tb_picks||[]).length,target:'tb-picks-card',tone:'under'},
    {icon:'⭐',label:'HRR SP',count:(view.hrr_special_picks||[]).length,target:'hrr-special-card'},
    {icon:'🔱',label:'Triple Split',count:(view.triple_split_picks||[]).length,target:'triple-split-card'},
    {icon:'⭐',label:'5 Star Split',count:(view.five_star_split_picks||[]).length,target:'five-star-card'},
    {icon:'🔥',label:'HRR',count:(view.hrr_picks||[]).length,target:'hrr-over-card'},
    {icon:'💥',label:'RBI',count:(view.rbi_picks||[]).length,target:'rbi-over-card'},
    {icon:'💣',label:'HR',count:(view.hr_picks||[]).length,target:'hr-over-card'},
    {icon:'🏃',label:'Runs',count:(view.runs_picks||[]).length,target:'runs-over-card'},
    {icon:'🚶',label:'Walks',count:(view.walks_picks||[]).length,target:'bwalk-over-card'},
  ];
  const PIT=[
    {icon:'⚾',label:'Strikeouts (K)',count:((pk.picks)||[]).length,target:'k-over-card,k-under-card'},
    {icon:'🔢',label:'Outs',count:pc('pitcher_outs'),target:'prop-outs-over-card,prop-outs-under-card'},
    {icon:'🛡️',label:'Earned Runs',count:pc('pitcher_earned_runs'),target:'prop-er-over-card,prop-er-under-card'},
    {icon:'🚶',label:'Walks Allowed',count:pc('pitcher_walks'),target:'prop-bb-over-card,prop-bb-under-card'},
    {icon:'💧',label:'Hits Allowed',count:pc('pitcher_hits_allowed'),target:'prop-ha-over-card,prop-ha-under-card'},
  ];
  const meta=_metaPill('⚾','Games',stats.games,'by-game-card');
  return `<div style="display:flex;gap:14px;flex-wrap:wrap;align-items:flex-start">${_catMenuHTML('catmenu-hit','Hitters','#63cab7',HIT)}${_catMenuHTML('catmenu-pit','Pitchers','#a78bfa',PIT)}<div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-left:auto">${meta}</div></div>`;
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
  if (result === 'VOID') return '<span style="color:#38bdf8;font-weight:700" title="Player did not play \u2014 no action, refunded">VOID</span>';
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
      '<td style="font-family:monospace;font-weight:700;color:#fff">' + actual + statusNote + '</td>' +
      '<td>' + _gradeResultBadge(res) + '</td>' +
      '</tr>';
  }).join('');
  return '<details open style="margin-bottom:20px">' +
    '<summary style="cursor:pointer;list-style:none;display:flex;align-items:center;gap:8px;padding:10px 0;border-bottom:1px solid #1f2937;margin-bottom:8px">' +
    '<span style="font-weight:700;color:' + color + ';font-size:.9rem">' + title + '</span>' +
    '<span style="font-size:.72rem;color:#64748b;background:#111;border-radius:999px;padding:2px 8px">' + rows.length + '</span>' +
    '<span style="font-size:.7rem;color:#475569;margin-left:auto">▸ toggle</span></summary>' +
    '<div style="overflow-x:auto"><table class="grade-table">' +
    '<thead><tr><th>#</th><th>Player</th><th>Pick</th><th>Odds</th><th>Actual</th><th>Result</th></tr></thead>' +
    '<tbody>' + trs + '</tbody></table></div></details>';
}

function renderGradeResults(data) {
  window.__GRADE_ROWS__ = [];
  // EVERY category renders as its OWN top-10 OVER section and its OWN top-10
  // UNDER section \u2014 never combined \u2014 so wins/losses read cleanly per side.
  var C_OVER = '#4ade80', C_UNDER = '#ff8a65';
  function _gside(rows, sd){
    return (rows||[]).filter(function(r){ return (r.side||'').toUpperCase()===sd; }).slice(0,10);
  }
  var sections = [];
  function pushOne(rows, label, color){
    if((rows||[]).length) sections.push({label:label+' (top '+rows.length+')', rows:rows, color:color});
  }
  function pushPair(rows, label){
    pushOne(_gside(rows,'OVER'),  label+' \u2014 OVER',  C_OVER);
    pushOne(_gside(rows,'UNDER'), label+' \u2014 UNDER', C_UNDER);
  }
  // Hitter hits arrive pre-split as separate keys.
  pushOne((data.hitter_overs ||[]).slice(0,10), 'Hitter Hits \u2014 OVER 0.5',  C_OVER);
  pushOne((data.hitter_unders||[]).slice(0,10), 'Hitter Hits \u2014 UNDER 1.5', C_UNDER);
  pushPair(data.runs, 'Runs 0.5');
  pushOne((data.tb_over ||[]).slice(0,10), 'Total Bases \u2014 OVER 1.5',  C_OVER);
  pushOne((data.tb_under||[]).slice(0,10), 'Total Bases \u2014 UNDER 1.5', C_UNDER);
  pushPair(data.rbi, 'RBI 0.5');
  pushPair(data.hr, 'HR 0.5');
  pushPair(data.batter_walks, 'Batter Walks 0.5');
  pushPair(data.hrr, 'HRR 1.5 (H+R+RBI)');
  pushPair(data.pitcher_ks, 'Pitcher Strikeouts');
  var propBuckets = {}, propOrder = [];
  (data.pitcher_props || []).forEach(function(r){
    var cat = r.category || 'Pitcher Props';
    if(!propBuckets[cat]){ propBuckets[cat]=[]; propOrder.push(cat); }
    propBuckets[cat].push(r);
  });
  propOrder.forEach(function(cat){ pushPair(propBuckets[cat], cat); });
  var allRows = [];
  sections.forEach(function(s){ allRows = allRows.concat(s.rows); });
  var wins    = allRows.filter(function(r) { return r.result === 'WIN'; }).length;
  var losses  = allRows.filter(function(r) { return r.result === 'LOSS'; }).length;
  var voids   = allRows.filter(function(r) { return r.result === 'VOID'; }).length;
  var pending = allRows.filter(function(r) { return r.result !== 'WIN' && r.result !== 'LOSS' && r.result !== 'VOID'; }).length;
  document.getElementById('grade-summary').innerHTML =
    '<div style="background:#111;border-radius:10px;padding:14px 18px;margin-bottom:12px;display:flex;flex-wrap:wrap;gap:12px;align-items:center">' +
    '<div><span style="color:#4ade80;font-weight:700;font-size:1.1rem">' + wins + 'W</span> ' +
    '<span style="color:#f87171;font-weight:700;font-size:1.1rem">' + losses + 'L</span>' +
    (pending > 0 ? ' <span style="color:#94a3b8;font-size:.85rem;margin-left:4px">' + pending + ' pending</span>' : '') +
    (voids > 0 ? ' <span style="color:#38bdf8;font-size:.85rem;margin-left:4px">' + voids + ' void</span>' : '') + '</div>' +
    '<div style="margin-left:auto;display:flex;gap:10px;align-items:center">' +
      '<button onclick="downloadGradeCSV()" style="background:#7c3aed;color:#fff;border:none;border-radius:8px;padding:7px 12px;font-size:.78rem;font-weight:600;cursor:pointer;white-space:nowrap">\u2b07 Results CSV</button>' +
    '</div>' +
    '</div>' +
    '<div style="background:#0a1f14;border:1px solid #16432c;border-radius:10px;padding:14px 18px;margin-bottom:16px;display:flex;flex-wrap:wrap;gap:14px;align-items:center">' +
    '<div style="font-weight:800;font-size:.92rem;color:#6ee7b7">&#x1F4B0; Potential Earnings</div>' +
    '<label style="font-size:.82rem;color:#94a3b8">Flat bet $ <input id="resBet" type="number" min="1" step="1" value="100" oninput="_recalcResEarnings()" style="width:84px;margin-left:4px;background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:5px 8px;font-size:.82rem"></label>' +
    '<div id="resNet" style="font-size:.88rem;font-weight:700;color:#e2e8f0"></div>' +
    '<button onclick="downloadResEarningsCSV()" style="margin-left:auto;background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 Earnings CSV (Excel)</button>' +
    '</div>';
  setTimeout(_recalcResEarnings, 0);
  var bodyHtml = sections.map(function(s) {
    return renderGradeSection(s.label, s.rows, s.color);
  }).join('');
  document.getElementById('grade-body').innerHTML = bodyHtml ||
    '<p style="color:#94a3b8;padding:16px">No graded picks for this date.</p>';
}

// Results CSV \u2014 one row per graded pick (mirrors the Track Record daily CSV).
function downloadGradeCSV(){
  var rows=window.__GRADE_ROWS__||[];
  if(!rows.length){ alert('No results to export yet \u2014 run Results for a date first.'); return; }
  var dateStr=(document.getElementById('date-picker')||{}).value||'';
  var out=[['Date','Category','Side','Player','Pick','Odds','Actual','Result']];
  rows.forEach(function(r){
    out.push([dateStr, r.category||'', r.side||'', r.name||'', r.pick||'',
              (r.odds!=null?r.odds:''), (r.actual!=null?r.actual:''), r.result||'']);
  });
  var csv=out.map(function(row){return row.map(_csvCell).join(',');}).join(String.fromCharCode(13)+String.fromCharCode(10));
  var blob=new Blob([String.fromCharCode(65279)+csv],{type:'text/csv;charset=utf-8;'});
  var url=URL.createObjectURL(blob);
  var a=document.createElement('a'); a.href=url; a.download='mlb-results-'+(dateStr||'today')+'.csv';
  document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
}
function _resStake(){
  var inp=document.getElementById('resBet');
  var s=inp?Number(inp.value):100;
  if(!isFinite(s)||s<=0) s=100;
  return s;
}
function _recalcResEarnings(){
  var el=document.getElementById('resNet'); if(!el) return;
  var rows=window.__GRADE_ROWS__||[];
  var decided=rows.filter(function(r){ return r.result==='WIN'||r.result==='LOSS'; });
  if(!decided.length){ el.innerHTML='<span style="color:#64748b">No decided picks yet.</span>'; return; }
  var stake=_resStake(), net=0, counted=0, skipped=0;
  decided.forEach(function(r){
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
function downloadResEarningsCSV(){
  var rows=window.__GRADE_ROWS__||[];
  if(!rows.length){ alert('No results to export yet.'); return; }
  var dateStr=(document.getElementById('date-picker')||{}).value||'';
  var stake=_resStake();
  var out=[['Date','Category','Side','Player','Pick','Odds','Result','Bet Size','Profit/Loss']];
  var net=0, counted=0;
  rows.forEach(function(r){
    var pl=_amProfit(r.odds, stake, (r.result==='WIN'));
    var plStr='';
    if(pl!==null){ plStr=pl.toFixed(2); net+=pl; counted++; }
    out.push([dateStr, r.category||'', r.side||'', r.name||'', r.pick||'',
      (r.odds!=null?((r.odds>0?'+':'')+r.odds):''), r.result||'', stake, plStr]);
  });
  out.push([]);
  out.push(['','','','','','','TOTALS ('+counted+' graded)', (counted*stake), net.toFixed(2)]);
  var csv=out.map(function(row){return row.map(_csvCell).join(',');}).join(String.fromCharCode(13)+String.fromCharCode(10));
  var blob=new Blob([String.fromCharCode(65279)+csv],{type:'text/csv;charset=utf-8;'});
  var url=URL.createObjectURL(blob);
  var a=document.createElement('a'); a.href=url; a.download='mlb-earnings-'+dateStr+'-flat'+stake+'.csv';
  document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
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
  document.getElementById('track-head').innerHTML='';
  document.getElementById('track-body').innerHTML='';
  var today=_trkTodayISO();
  window.__TRK_TODAY__=today; window.__TRK_DAILY_DATE__=today;
  window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{};
  if(!window.__TRK_MONTH__) window.__TRK_MONTH__=today.slice(0,7);
  try{
    var q='?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'');
    var trP=fetch('/api/track-record'+q).then(function(r){ if(!r.ok) return r.text().then(function(t){ throw new Error(t); }); return r.json(); });
    var grP=fetch('/api/grade/'+today+q).then(function(r){ return r.ok?r.json():null; }).catch(function(){ return null; });
    var arr=await Promise.all([trP,grP]);
    window.__TRACK__=arr[0];
    if(arr[1]) window.__TRK_GRADE_CACHE__[today]=arr[1];
    renderTrackRecord(window.__TRACK__);
  }catch(e){
    document.getElementById('track-body').innerHTML='<p style="color:#f87171;padding:16px">'+(e.message||'Error loading track record')+'</p>';
  }finally{
    btn.disabled=false; btn.textContent=lbl;
    document.getElementById('track-spinner').classList.add('hidden');
  }
}

function _twPct(w,l){ var n=w+l; return n? (w/n*100).toFixed(1)+'%' : '—'; }
function _twColor(w,l){ var n=w+l; if(!n) return '#94a3b8'; var p=w/n*100; return p>=60?'#4ade80':(p>=50?'#facc15':'#f87171'); }
function _twSide(s){ return s==='OVER' ? '<span style="color:#4ade80">OVER</span>' : '<span style="color:#ff8a65">UNDER</span>'; }

// Overflow = every pick beyond a category's top 10. Hits overflow keeps the
// legacy "Hitter Hits (More)" label; every other category gets a " (OVF)" suffix.
function _isOvfCat(c){ c=c||''; return c==='Hitter Hits (More)'||c.slice(-6)===' (OVF)'; }
function _isHrCat(c){ c=c||''; return c==='HR'||c==='HR (OVF)'; }
function _ovfBaseCat(c){ c=c||''; if(c==='Hitter Hits (More)') return 'Hitter Hits'; if(c.slice(-6)===' (OVF)') return c.slice(0,-6); return c; }
var _OVF_KEYS=['hitter_more','overflow'];
function _ovfFlatten(g){ var out=[]; if(!g||g==='LOADING'||g.__error__) return out; _OVF_KEYS.forEach(function(k){ (g[k]||[]).forEach(function(r){ if(!_isHrCat(r.category)) out.push(r); }); }); return out; }
// Single source of truth for category labels/order — shared by the main Track
// Record AND the Overflow Tracker. Overflow categories are merged in so the
// reused table/CSV helpers resolve their labels; ordering lives in __OVF_ORDER__.
function _trkBuildCfg(){
  var CAT_CFG={
    'Top 10 Batter|OVER':        {lbl:'Top 10 Hitter Plays',  icon:'⭐', abbr:'T10B+'},
    'Top 10 Batter|UNDER':       {lbl:'Top 10 Hitter Plays (Under)', icon:'⭐', abbr:'T10B-'},
    'Top 10 Pitcher|OVER':       {lbl:'Top 10 Pitcher Props (Over)', icon:'🎯', abbr:'T10P+'},
    'Top 10 Pitcher|UNDER':      {lbl:'Top 10 Pitcher Props (Under)',icon:'🎯', abbr:'T10P-'},
    'Value Plays|OVER':          {lbl:'Top 10 Value Plays', icon:'💎', abbr:'Val'},
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
    'TB Over|OVER':              {lbl:'TB Over 1.5',                icon:'📈', abbr:'TB+'},
    'RBI|OVER':                  {lbl:'RBI (Over 0.5)',             icon:'💥', abbr:'RBI+'},
    'RBI|UNDER':                 {lbl:'RBI (Under 0.5)',            icon:'💥', abbr:'RBI-'},
    'HR|OVER':                   {lbl:'HR (Over 0.5)',              icon:'💣', abbr:'HR+'},
    'HR|UNDER':                  {lbl:'HR (Under 0.5)',             icon:'💣', abbr:'HR-'},
    'Batter Walks|OVER':         {lbl:'Batter Walks (Over 0.5)',    icon:'🚶', abbr:'BBat+'},
    'Batter Walks|UNDER':        {lbl:'Batter Walks (Under 0.5)',   icon:'🚶', abbr:'BBat-'},
    'HRR|OVER':                  {lbl:'HRR (Over 1.5 H+R+RBI)',     icon:'🔥', abbr:'HRR+'},
    'HRR|UNDER':                 {lbl:'HRR (Under 1.5 H+R+RBI)',    icon:'🔥', abbr:'HRR-'},
    'Pitcher Walks|OVER':        {lbl:'Walks Allowed (Over)',       icon:'🚶', abbr:'BB+'},
    'Pitcher Walks|UNDER':       {lbl:'Walks Allowed (Under)',      icon:'🚶', abbr:'BB-'},
    'Hitter Hits (More)|OVER':   {lbl:'Hits Overflow (Over 0.5)',   icon:'⭐'},
    'Hitter Hits (More)|UNDER':  {lbl:'Under 1.5 Hits Overflow',    icon:'📉'},
    'Runs (OVF)|OVER':           {lbl:'Runs Overflow (Over 0.5)',   icon:'🏃'},
    'Runs (OVF)|UNDER':          {lbl:'Runs Overflow (Under 0.5)',  icon:'🏃'},
    'TB Under (OVF)|UNDER':      {lbl:'TB Under 1.5 Overflow',      icon:'📊'},
    'TB Over (OVF)|OVER':        {lbl:'TB Over 1.5 Overflow',       icon:'📈'},
    'RBI (OVF)|OVER':            {lbl:'RBI Overflow (Over)',        icon:'💥'},
    'RBI (OVF)|UNDER':           {lbl:'RBI Overflow (Under)',       icon:'💥'},
    'HR (OVF)|OVER':             {lbl:'HR Overflow (Over)',         icon:'💣'},
    'HR (OVF)|UNDER':            {lbl:'HR Overflow (Under)',        icon:'💣'},
    'Batter Walks (OVF)|OVER':   {lbl:'Batter Walks Overflow (Over)',  icon:'🚶'},
    'Batter Walks (OVF)|UNDER':  {lbl:'Batter Walks Overflow (Under)', icon:'🚶'},
    'HRR (OVF)|OVER':            {lbl:'HRR Overflow (Over 1.5)',    icon:'🔥'},
    'HRR (OVF)|UNDER':           {lbl:'HRR Overflow (Under 1.5)',   icon:'🔥'},
    'Pitcher Ks (OVF)|OVER':     {lbl:'Pitcher Ks Overflow (Over)', icon:'⚾'},
    'Pitcher Ks (OVF)|UNDER':    {lbl:'Pitcher Ks Overflow (Under)',icon:'⚾'},
    'Pitcher Hits Allowed (OVF)|OVER': {lbl:'Hits Allowed Overflow (Over)',  icon:'🎯'},
    'Pitcher Hits Allowed (OVF)|UNDER':{lbl:'Hits Allowed Overflow (Under)', icon:'🎯'},
    'Pitcher Outs (OVF)|OVER':   {lbl:'Pitcher Outs Overflow (Over)',  icon:'🔢'},
    'Pitcher Outs (OVF)|UNDER':  {lbl:'Pitcher Outs Overflow (Under)', icon:'🔢'},
    'Pitcher Earned Runs (OVF)|OVER':  {lbl:'Earned Runs Overflow (Over)',  icon:'🔥'},
    'Pitcher Earned Runs (OVF)|UNDER': {lbl:'Earned Runs Overflow (Under)', icon:'🔥'},
    'Pitcher Walks (OVF)|OVER':  {lbl:'Walks Allowed Overflow (Over)',  icon:'🚶'},
    'Pitcher Walks (OVF)|UNDER': {lbl:'Walks Allowed Overflow (Under)', icon:'🚶'},
    'Top 10 Batter (OVF)|OVER':  {lbl:'Top 10 Hitter Plays (OVF)',      icon:'⭐'},
    'Top 10 Batter (OVF)|UNDER': {lbl:'Top 10 Hitter Plays (OVF Under)', icon:'⭐'},
  };
  var CAT_ORDER=['Top 10 Batter|OVER','Top 10 Batter|UNDER','Top 10 Pitcher|OVER','Top 10 Pitcher|UNDER','Value Plays|OVER',
    'Hitter Hits|OVER','Hitter Hits|UNDER','Runs|OVER','Runs|UNDER',
    'TB Under|UNDER','TB Over|OVER','RBI|OVER','RBI|UNDER','HR|OVER','HR|UNDER','Batter Walks|OVER','Batter Walks|UNDER','HRR|OVER','HRR|UNDER',
    'Pitcher Ks|OVER','Pitcher Ks|UNDER','Pitcher Hits Allowed|OVER','Pitcher Hits Allowed|UNDER',
    'Pitcher Outs|OVER','Pitcher Outs|UNDER','Pitcher Earned Runs|OVER','Pitcher Earned Runs|UNDER',
    'Pitcher Walks|OVER','Pitcher Walks|UNDER'];
  var OVF_ORDER=['Top 10 Batter (OVF)|OVER','Top 10 Batter (OVF)|UNDER','Hitter Hits (More)|OVER','Hitter Hits (More)|UNDER','Runs (OVF)|OVER','Runs (OVF)|UNDER',
    'TB Under (OVF)|UNDER','TB Over (OVF)|OVER','RBI (OVF)|OVER','RBI (OVF)|UNDER',
    'Batter Walks (OVF)|OVER','Batter Walks (OVF)|UNDER','HRR (OVF)|OVER','HRR (OVF)|UNDER',
    'Pitcher Ks (OVF)|OVER','Pitcher Ks (OVF)|UNDER','Pitcher Hits Allowed (OVF)|OVER','Pitcher Hits Allowed (OVF)|UNDER',
    'Pitcher Outs (OVF)|OVER','Pitcher Outs (OVF)|UNDER','Pitcher Earned Runs (OVF)|OVER','Pitcher Earned Runs (OVF)|UNDER',
    'Pitcher Walks (OVF)|OVER','Pitcher Walks (OVF)|UNDER'];
  window.__TRK_CFG__=CAT_CFG; window.__TRK_ORDER__=CAT_ORDER; window.__OVF_ORDER__=OVF_ORDER;
}

function renderTrackRecord(d){
  var rows=d.alltime||[]; var daily=d.daily||[];
  var tw=0,tl=0;
  rows.forEach(function(r){ tw+=r.wins; tl+=r.losses; });
  _trkBuildCfg();
  if(!window.__TRK_TAB__) window.__TRK_TAB__='daily';
  if(!window.__TRK_MONTH__) window.__TRK_MONTH__=_trkTodayISO().slice(0,7);
  var _today=_trkTodayISO(); if(window.__TRK_DAILY_DATE__&&window.__TRK_DAILY_DATE__<_today) window.__TRK_DAILY_DATE__=_today;
  if(window.__TRK_GRADE_CACHE__) delete window.__TRK_GRADE_CACHE__[_today];
  var bet=(window.__TRK_BET__!=null?window.__TRK_BET__:20);
  var hdr='<div style="display:flex;flex-wrap:wrap;gap:14px;align-items:center;background:#0a1f14;border:1px solid #16432c;border-radius:12px;padding:14px 18px;margin-bottom:14px">'
    +'<span style="font-weight:800;color:#6ee7b7;font-size:1rem">💰 Bet amount $</span>'
    +'<input id="trkBet" type="number" min="1" step="1" value="'+bet+'" oninput="_trkBetInput()" style="width:104px;background:#020617;border:1px solid #334155;color:#fff;border-radius:8px;padding:8px 12px;font-size:1.05rem;font-weight:800;text-align:center">'
    +'<span style="color:#94a3b8;font-size:.8rem">flat on every pick \u2014 drives every $ below</span>'
    +'<button onclick="_trkPrintReport()" style="margin-left:auto;background:#dc2626;color:#fff;border:none;border-radius:8px;padding:9px 16px;font-size:.84rem;font-weight:800;cursor:pointer">📄 PDF Report</button>'
    +'</div>';
  var tabs='<div style="display:flex;gap:8px;margin-bottom:4px;flex-wrap:wrap">'+_trkTabBtn('daily','Daily')+_trkTabBtn('weekly','Weekly')+_trkTabBtn('monthly','Monthly')+_trkTabBtn('custom','Custom')
    +'<button onclick="_openProjEdgeStats()" title="W/L record and ROI for Proj Edge plays (pitchers: projection beats the line; hitters: positive edge), tracked forward each day &#x2014; separate from Track Record" style="background:#0c4a6e;color:#7dd3fc;border:1px solid #0ea5e9;border-radius:8px;padding:8px 14px;font-size:.8rem;font-weight:800;cursor:pointer;white-space:nowrap">&#9650; Proj Edge Record</button>'
    +'<button onclick="_openHrrSpStats()" title="W/L record for HRR Special confluence picks (all 4 gates cleared) &#x2014; separate from main Track Record" style="background:#1a0e2e;color:#c4b5fd;border:1px solid #7c3aed;border-radius:8px;padding:8px 14px;font-size:.8rem;font-weight:800;cursor:pointer;white-space:nowrap">&#11088; HRR SP Record</button>'
    +'<button onclick="_openTscStats()" title="W/L record for Triple Split Club picks (over .275 in all three of today&#39;s splits) &#x2014; to record a hit, separate from main Track Record" style="background:#04141c;color:#67e8f9;border:1px solid #0e7490;border-radius:8px;padding:8px 14px;font-size:.8rem;font-weight:800;cursor:pointer;white-space:nowrap">&#128305; Triple Split Record</button>'
    +'<button onclick="_openFssStats()" title="W/L record and ROI for 5 Star Split picks (Triple Split + 60%+ games with a hit vs team + 60%+ last 10) &#x2014; separate from main Track Record" style="background:#0c0a1a;color:#c4b5fd;border:1px solid #7c3aed;border-radius:8px;padding:8px 14px;font-size:.8rem;font-weight:800;cursor:pointer;white-space:nowrap">&#11088; 5 Star Split Record</button>'
    +'</div>';
  var sc=_matrixScorecard(d);
  var he=document.getElementById('track-head'); if(he) he.innerHTML=hdr+sc+tabs;
  var be=document.getElementById('track-body'); if(be) be.innerHTML='';
  _trkRenderActive();
}

// ── Matrix Scorecard: is the strategy chart actually predictive? ──────────
// Section 1 (day-of-week half) scores ALL graded history retroactively from
// each pick&#39;s date+category+side. Section 2 (combined series+day verdict)
// scores only rows that carry series_pos (banked going forward).
function _mtxCatInfo(cat){
  var M={
    'Hitter Hits':[false,0],'Runs':[false,3],'TB Under':[false,1],'TB Over':[false,1],
    'RBI':[false,4],'HR':[false,2],'Batter Walks':[false,5],'HRR':[false,2],
    'Pitcher Ks':[true,0],'Pitcher Hits Allowed':[true,1],'Pitcher Outs':[true,2],
    'Pitcher Earned Runs':[true,3],'Pitcher Walks':[true,4]
  };
  return M[cat]||null;
}
function _mtxDayLean(weekday,isPit,catIdx){
  if(typeof _DOW_SIG==='undefined') return '';
  var map=isPit?_DOW_PIT:_DOW_BAT; var idx=map[catIdx]; if(idx==null) return '';
  var row=_DOW_SIG[weekday]||[]; return row[idx]||'';
}
function _mtxSeriesLean(pos,isPit,catIdx){
  var slots=window.__MPA_SLOTS__; if(!slots||!slots[pos]) return '';
  var arr=isPit?slots[pos].pit:slots[pos].bat; if(!arr||arr[catIdx]==null) return '';
  return arr[catIdx];
}
function _mtxWeekday(ds){ if(!ds) return null; var dt=new Date(ds+'T12:00:00'); var w=dt.getDay(); return isNaN(w)?null:w; }
function _weekdayName(ds){ var w=_mtxWeekday(ds); return (w==null)?'':['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][w]; }
// Per-category breakdown of the green/amber/red combined verdict: same picks
// as Section 2&#39;s global tally, split out so you can see WHICH market each
// green/amber/red play came from (W-L, win%, and category ROI per row).
function _mtxVerdictRows(perVerdict,stake){
  function tot(b){ return b.w+b.l; }
  function pct(b){ var n=tot(b); return n?(b.w/n*100):0; }
  var cats=Object.keys(perVerdict).filter(function(c){ var v=perVerdict[c]; return (tot(v.g)+tot(v.a)+tot(v.r))>0; });
  if(!cats.length) return '';
  cats.sort(function(a,b){ var va=perVerdict[a], vb=perVerdict[b]; return (tot(vb.g)+tot(vb.a)+tot(vb.r))-(tot(va.g)+tot(va.a)+tot(va.r)); });
  function cell(b,clr){ if(!tot(b)) return '<span style="color:#475569">\u2014</span>'; var p=pct(b); return '<span style="color:'+clr+';font-weight:700">'+b.w+'-'+b.l+'</span> <span style="color:#64748b">'+p.toFixed(0)+'%</span>'; }
  function totCell(v){ var w=v.g.w+v.a.w+v.r.w, l=v.g.l+v.a.l+v.r.l, n=w+l; if(!n) return '<span style="color:#475569">\u2014</span>'; var net=v.g.net+v.a.net+v.r.net, cnt=v.g.counted+v.a.counted+v.r.counted; var rv=cnt?(net/(cnt*stake)*100):0; var rc=rv>=0?'#4ade80':'#f87171'; return '<span style="color:#e2e8f0;font-weight:700">'+w+'-'+l+'</span> <span style="font-weight:800;color:'+rc+'">'+(rv>=0?'+':'\u2212')+Math.abs(rv).toFixed(0)+'%</span>'; }
  var head='<div style="display:flex;gap:8px;padding:2px;font-size:.58rem;color:#64748b;font-weight:800;letter-spacing:.04em"><span style="flex:1;min-width:96px">MARKET</span><span style="width:76px;text-align:right">GREEN</span><span style="width:76px;text-align:right">AMBER</span><span style="width:76px;text-align:right">RED</span><span style="width:92px;text-align:right">CATEGORY</span></div>';
  var rows='';
  cats.forEach(function(c){ var v=perVerdict[c];
    rows+='<div style="display:flex;align-items:center;gap:8px;padding:5px 2px;border-bottom:1px solid #111c2e;font-size:.72rem">'
      +'<span style="flex:1;min-width:96px;color:#cbd5e1">'+c+'</span>'
      +'<span style="width:76px;text-align:right;font-family:monospace">'+cell(v.g,'#4ade80')+'</span>'
      +'<span style="width:76px;text-align:right;font-family:monospace">'+cell(v.a,'#fbbf24')+'</span>'
      +'<span style="width:76px;text-align:right;font-family:monospace">'+cell(v.r,'#f87171')+'</span>'
      +'<span style="width:92px;text-align:right;font-family:monospace">'+totCell(v)+'</span></div>';
  });
  return '<details style="margin-top:10px"><summary style="cursor:pointer;color:#64748b;font-size:.7rem;font-weight:700;letter-spacing:.04em">Per-category breakdown \u2014 green / amber / red by market</summary><div style="margin-top:6px;overflow-x:auto"><div style="min-width:420px">'+head+rows+'</div></div></details>';
}
function _matrixScorecard(d){
  var det=(d&&d.detail)||[]; var stake=_trkStake();
  function B(){ return {w:0,l:0,net:0,counted:0}; }
  function add(b,win,odds){ if(win) b.w++; else b.l++; var pl=_amProfit(odds,stake,win); if(pl!==null){ b.net+=pl; b.counted++; } }
  function tot(b){ return b.w+b.l; }
  function pct(b){ var n=tot(b); return n?(b.w/n*100):0; }
  function roi(b){ return b.counted?(b.net/(b.counted*stake)*100):0; }
  var dayAgree=B(), dayFade=B();
  var vGreen=B(), vRed=B(), vAmber=B();
  var perMkt={}, perVerdict={};
  det.forEach(function(r){
    if(_trkSkipMeta(r)||_isHrCat(r.category)) return;
    if(r.result!=='WIN'&&r.result!=='LOSS') return;
    var info=_mtxCatInfo(r.category); if(!info) return;
    var isPit=info[0], ci=info[1];
    var side=(r.side==='UNDER')?'U':'O';
    var wd=_mtxWeekday(r.date); if(wd==null) return;
    var dLean=_mtxDayLean(wd,isPit,ci); if(!dLean) return;
    var win=r.result==='WIN';
    var dAgree=(dLean===side);
    add(dAgree?dayAgree:dayFade, win, r.odds);
    var mk=perMkt[r.category]=perMkt[r.category]||{a:B(),f:B()};
    add(dAgree?mk.a:mk.f, win, r.odds);
    var pos=r.series_pos;
    if(pos===1||pos===2||pos===3){
      var sLean=_mtxSeriesLean(pos,isPit,ci);
      if(sLean){
        var pv=perVerdict[r.category]=perVerdict[r.category]||{g:B(),a:B(),r:B()};
        if(dLean&&sLean!==dLean){ add(vAmber,win,r.odds); add(pv.a,win,r.odds); }
        else if(sLean===side){ add(vGreen,win,r.odds); add(pv.g,win,r.odds); }
        else { add(vRed,win,r.odds); add(pv.r,win,r.odds); }
      }
    }
  });
  function statRow(label,b,clr){
    if(!tot(b)) return '';
    var p=pct(b).toFixed(0); var rv=roi(b); var rc=rv>=0?'#4ade80':'#f87171';
    return '<div style="display:flex;align-items:center;gap:10px;padding:7px 2px;border-bottom:1px solid #16233a">'
      +'<span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:'+clr+';flex:none"></span>'
      +'<span style="flex:1;color:#cbd5e1;font-size:.8rem;font-weight:700">'+label+'</span>'
      +'<span style="font-family:monospace;color:#e2e8f0;font-size:.78rem;width:54px;text-align:right">'+b.w+'-'+b.l+'</span>'
      +'<span style="font-family:monospace;color:#fff;font-weight:800;font-size:.78rem;width:46px;text-align:right">'+p+'%</span>'
      +'<span style="font-family:monospace;font-weight:800;font-size:.78rem;width:60px;text-align:right;color:'+rc+'">'+(rv>=0?'+':'\u2212')+Math.abs(rv).toFixed(0)+'%</span>'
      +'</div>';
  }
  var colHdr='<div style="display:flex;align-items:center;gap:10px;padding:2px 2px 4px">'
    +'<span style="width:9px;flex:none"></span><span style="flex:1"></span>'
    +'<span style="color:#64748b;font-size:.6rem;font-weight:800;letter-spacing:.05em;width:54px;text-align:right">W-L</span>'
    +'<span style="color:#64748b;font-size:.6rem;font-weight:800;letter-spacing:.05em;width:46px;text-align:right">WIN%</span>'
    +'<span style="color:#64748b;font-size:.6rem;font-weight:800;letter-spacing:.05em;width:60px;text-align:right">ROI</span></div>';
  // Section 1 verdict
  var n1=tot(dayAgree)+tot(dayFade);
  var diff=pct(dayAgree)-pct(dayFade);
  var vClr,vTxt;
  if(n1<10){ vClr='#94a3b8'; vTxt='Only '+n1+' graded picks so far \u2014 still gathering. Keep logging slates.'; }
  else { vClr='#94a3b8'; vTxt='Experimental day-of-week lean, shown for tracking only \u2014 not a betting recommendation while we gather results.'; }
  var s1='<div style="margin-bottom:6px;color:#93c5fd;font-size:.72rem;font-weight:800;letter-spacing:.05em;text-transform:uppercase">1 \u00b7 Day-of-Week Signal \u2014 All History</div>'
    +colHdr
    +statRow('Day lean matches this pick',dayAgree,'#22c55e')
    +statRow('Day lean opposite this pick',dayFade,'#ef4444')
    +'<div style="margin-top:8px;font-size:.74rem;line-height:1.5;color:'+vClr+'">'+vTxt+'</div>';
  // per-market breakdown (day signal)
  var mkRows='';
  Object.keys(perMkt).sort(function(a,b){ return pct(perMkt[b].a)-pct(perMkt[a].a); }).forEach(function(cat){
    var m=perMkt[cat]; if(!tot(m.a)&&!tot(m.f)) return;
    function cell(b){ if(!tot(b)) return '<span style="color:#475569">\u2014</span>'; var p=pct(b); var c=p>=55?'#4ade80':p>=45?'#fbbf24':'#f87171'; return '<span style="color:'+c+';font-weight:700">'+b.w+'-'+b.l+'</span> <span style="color:#64748b">('+p.toFixed(0)+'%)</span>'; }
    mkRows+='<div style="display:flex;align-items:center;gap:8px;padding:5px 2px;border-bottom:1px solid #111c2e;font-size:.74rem">'
      +'<span style="flex:1;color:#cbd5e1">'+cat+'</span>'
      +'<span style="width:96px;text-align:right;font-family:monospace">'+cell(m.a)+'</span>'
      +'<span style="width:96px;text-align:right;font-family:monospace">'+cell(m.f)+'</span></div>';
  });
  var s1mk = mkRows ? ('<details style="margin-top:10px"><summary style="cursor:pointer;color:#64748b;font-size:.7rem;font-weight:700;letter-spacing:.04em">Per-market breakdown (which markets the chart predicts)</summary>'
    +'<div style="margin-top:6px"><div style="display:flex;gap:8px;padding:2px;font-size:.6rem;color:#64748b;font-weight:800"><span style="flex:1"></span><span style="width:96px;text-align:right">AGREE</span><span style="width:96px;text-align:right">FADE</span></div>'+mkRows+'</div></details>') : '';
  // Section 2
  var n2=tot(vGreen)+tot(vRed)+tot(vAmber);
  var s2;
  if(!n2){
    s2='<div style="margin-bottom:6px;color:#a78bfa;font-size:.72rem;font-weight:800;letter-spacing:.05em;text-transform:uppercase">2 \u00b7 Combined Verdict \u2014 Series + Day</div>'
      +'<div style="font-size:.74rem;line-height:1.5;color:#94a3b8">Banks from your next graded slate forward. Older picks were logged before the series position (G1/G2/G3) was stored, so the full green/red/amber verdict starts filling in now.</div>';
  } else {
    s2='<div style="margin-bottom:6px;color:#a78bfa;font-size:.72rem;font-weight:800;letter-spacing:.05em;text-transform:uppercase">2 \u00b7 Combined Verdict \u2014 Series + Day</div>'
      +colHdr
      +statRow('Green \u2014 day + series both leaned this side',vGreen,'#22c55e')
      +statRow('Red \u2014 day + series leaned the other way',vRed,'#ef4444')
      +statRow('Amber \u2014 day + series split',vAmber,'#f59e0b')
      +'<div style="margin-top:8px;font-size:.7rem;color:#64748b;line-height:1.5">Experimental day-of-week + series lean, shown for tracking only \u2014 not a betting recommendation while we gather results.</div>'
      +_mtxVerdictRows(perVerdict,stake);
  }
  return '<div style="background:#0a1424;border:1px solid #1e293b;border-radius:12px;padding:14px 18px;margin-bottom:14px">'
    +'<div style="display:flex;align-items:center;gap:8px;margin-bottom:12px">'
    +'<span style="font-size:1rem">🧮</span>'
    +'<span style="font-weight:800;color:#e2e8f0;font-size:.95rem">Lean Tracker (experimental)</span>'
    +'<span style="color:#64748b;font-size:.7rem">how each lean group has done so far</span></div>'
    +s1+s1mk
    +'<div style="height:1px;background:#1e293b;margin:14px 0"></div>'
    +s2+'</div>';
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
// ===== Manual odds entry — when a pick has no posted odds (no Ontario book
// priced it), let the user paste odds grabbed from another book so P/L + ROI
// still compute. Stored per-browser in localStorage, keyed date|cat|side|name|line.
function _manOddsLoad(){ if(window.__MAN_ODDS__) return window.__MAN_ODDS__; var o={}; try{ o=JSON.parse(localStorage.getItem('mpa_manual_odds')||'{}')||{}; }catch(e){ o={}; } window.__MAN_ODDS__=o; return o; }
function _manOddsKey(p){ return [(p.__date__||p.date||''),(p.category||''),(p.side||''),(p.name||''),(p.line!=null?p.line:'')].join('|'); }
function _effOdds(p){ if(p&&p.odds!=null&&p.odds!=='') return Number(p.odds); var v=_manOddsLoad()[_manOddsKey(p)]; return (v!=null&&v!=='')?Number(v):null; }
function _isManOdds(p){ return !(p&&p.odds!=null&&p.odds!=='') && _effOdds(p)!=null; }
function _manOddsEntry(idx){
  var p=(window.__TRK_LOG_ROWS__||[])[idx]; if(!p) return;
  var st=_manOddsLoad(), key=_manOddsKey(p), cur=st[key];
  var raw=prompt('Odds for '+(p.name||'this pick')+' '+(p.pick||'')+' \u2014 American, e.g. -150 or +120 (blank to clear):',(cur!=null?String(cur):''));
  if(raw===null) return;
  raw=(''+raw).trim();
  if(raw===''){ delete st[key]; }
  else { var n=parseInt(raw,10); if(!isFinite(n)||n===0||Math.abs(n)<100){ alert('Enter valid American odds: +100 or higher, or -100 or lower.'); return; } st[key]=n; }
  try{ localStorage.setItem('mpa_manual_odds',JSON.stringify(st)); }catch(e){}
  _manOddsRerender();
}
function _manOddsRerender(){ var ov=document.getElementById('ovf-body'); if(ov&&ov.offsetParent!==null){ _ovfRenderActive(); return; } var tr=document.getElementById('track-body'); if(tr&&tr.offsetParent!==null){ _trkRenderActive(); return; } if(ov) _ovfRenderActive(); else if(tr) _trkRenderActive(); }
function _oddsCell(p,idx){ var eff=_effOdds(p); if(eff!=null){ var man=_isManOdds(p); var col=man?'#fbbf24':'#cbd5e1', bd=man?'#b45309':'#334155', bg=man?'#3a2406':'#0f1b2e'; return '<span onclick="_manOddsEntry('+idx+')" title="'+(man?'Manual odds \u2014 click to edit':'Click to edit odds')+'" style="font-family:monospace;cursor:pointer;color:'+col+';border:1px solid '+bd+';background:'+bg+';border-radius:5px;padding:1px 7px;font-size:.66rem;font-weight:800;flex-shrink:0">'+((eff>0?'+':'')+eff)+' \u270e</span>'; } return '<button onclick="_manOddsEntry('+idx+')" title="Enter odds from another book" style="background:#78350f;color:#fde68a;border:1px solid #b45309;border-radius:5px;padding:1px 7px;font-size:.66rem;font-weight:800;cursor:pointer;flex-shrink:0">+odds</button>'; }
// Single source of truth for the flat bet size — blank/zero/negative/NaN all
// fall back to the $20 default so the live total and the CSV always agree.
function _trkStake(){
  var inp=document.getElementById('trkBet');
  var s=inp?Number(inp.value):20;
  if(!isFinite(s)||s<=0) s=20;
  return s;
}
// Recompute the WHOLE Track Record from the one bet box: per-category Net P/L
// + ROI (ranked best->worst), the all-time net summary, then the daily sheet.
function _trkRecalc(){
  var d=window.__TRACK__; if(!d) return;
  var CAT_CFG=window.__TRK_CFG__||{}, CAT_ORDER=window.__TRK_ORDER__||[];
  var stake=_trkStake();
  function _rc(w,n){ if(!n) return '#64748b'; var p=w/n; return p>=0.70?'#4ade80':(p>=0.55?'#facc15':'#f87171'); }
  function _bar(pct,clr){ return '<div style="height:8px;border-radius:4px;background:#1e293b;overflow:hidden;width:88px;display:inline-block;vertical-align:middle"><div style="height:100%;width:'+pct+'%;background:'+clr+';border-radius:4px"></div></div>'; }
  var perCat={};
  (d.alltime||[]).forEach(function(r){ var k=r.category+'|'+r.side; var pc=perCat[k]=perCat[k]||{w:0,l:0,net:0,counted:0,skipped:0}; pc.w+=r.wins; pc.l+=r.losses; });
  (d.detail||[]).forEach(function(r){ var k=(r.category||'?')+'|'+(r.side||'OVER'); var pc=perCat[k]=perCat[k]||{w:0,l:0,net:0,counted:0,skipped:0}; var pl=_amProfit(r.odds,stake,r.result==='WIN'); if(pl===null){ pc.skipped++; return; } pc.net+=pl; pc.counted++; });
  var graded=[], gset={};
  Object.keys(perCat).forEach(function(k){ var pc=perCat[k]; if(pc.counted>0){ pc.roi=pc.net/(pc.counted*stake)*100; graded.push([k,pc]); gset[k]=1; } });
  graded.sort(function(a,b){ return b[1].roi-a[1].roi; });
  var oNet=0,oCnt=0,oSkip=0;
  graded.forEach(function(x){ oNet+=x[1].net; oCnt+=x[1].counted; oSkip+=x[1].skipped; });
  var oRisk=oCnt*stake, oRoi=oRisk?oNet/oRisk*100:0;
  var nEl=document.getElementById('trkNet');
  if(nEl){
    if(!oCnt){ nEl.innerHTML='No graded picks with odds yet \u2014 fills in as slates go Final.'; }
    else { var oc=oNet>=0?'#4ade80':'#f87171', rc2=oRoi>=0?'#4ade80':'#f87171';
      nEl.innerHTML='Net <span style="color:'+oc+';font-weight:900">'+(oNet>=0?'+$':'\u2212$')+Math.abs(oNet).toFixed(0)+'</span> \u00b7 ROI <span style="color:'+rc2+';font-weight:800">'+(oRoi>=0?'+':'\u2212')+Math.abs(oRoi).toFixed(1)+'%</span> on $'+oRisk.toFixed(0)+' risked'+(oSkip?(' \u00b7 '+oSkip+' no-odds win'+(oSkip===1?'':'s')+' excluded'):''); }
  }
  function _row(k,pc,rank){
    var parts=k.split('|'); var cfg=CAT_CFG[k]||{lbl:parts[0]+' ('+parts[1]+')',icon:'📊'};
    var n=pc.w+pc.l, clr=_rc(pc.w,n), pct=n?Math.round(pc.w/n*100):0;
    var hasRoi=pc.counted>0, roi=hasRoi?pc.roi:null, netClr=pc.net>=0?'#4ade80':'#f87171';
    var badge;
    if(rank===1) badge='<span style="display:inline-block;width:22px;height:22px;line-height:22px;text-align:center;border-radius:50%;background:#064e3b;color:#6ee7b7;font-weight:800;font-size:.72rem">1</span>';
    else if(rank===2||rank===3) badge='<span style="display:inline-block;width:22px;height:22px;line-height:22px;text-align:center;border-radius:50%;background:#065f46;color:#6ee7b7;font-weight:800;font-size:.72rem">'+rank+'</span>';
    else if(hasRoi&&roi<0) badge='<span style="display:inline-block;width:22px;height:22px;line-height:22px;text-align:center;border-radius:50%;background:#4c1d24;color:#fca5a5;font-weight:800;font-size:.72rem">\u2193</span>';
    else badge='<span style="display:inline-block;width:22px;color:#475569;text-align:center">\u00b7</span>';
    return '<div style="display:flex;align-items:center;padding:10px 12px;border-bottom:1px solid #131c2e">'
      +'<span style="width:30px;flex-shrink:0">'+badge+'</span>'
      +'<span style="flex:1;min-width:140px;color:#e2e8f0;font-weight:600;font-size:.85rem">'+(cfg.icon||'')+' '+cfg.lbl+'</span>'
      +'<span style="width:58px;text-align:right;font-family:monospace;font-weight:800;color:'+clr+';flex-shrink:0">'+pc.w+'/'+n+'</span>'
      +'<span style="width:100px;text-align:center;flex-shrink:0">'+_bar(pct,clr)+'</span>'
      +'<span style="width:80px;text-align:right;font-family:monospace;font-weight:800;color:'+(hasRoi?netClr:'#475569')+';flex-shrink:0">'+(hasRoi?((pc.net>=0?'+$':'\u2212$')+Math.abs(pc.net).toFixed(0)):'\u2014')+'</span>'
      +'<span style="width:72px;text-align:right;font-family:monospace;font-weight:700;color:'+(hasRoi?(roi>=0?'#4ade80':'#f87171'):'#475569')+';flex-shrink:0">'+(hasRoi?((roi>=0?'+':'\u2212')+Math.abs(roi).toFixed(1)+'%'):'\u2014')+'</span>'
      +'</div>';
  }
  var head='<div style="display:flex;align-items:center;padding:7px 12px;background:#0c1829;border-bottom:1px solid #1e293b">'
    +'<span style="width:30px;flex-shrink:0"></span>'
    +'<span style="flex:1;min-width:140px;font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.06em">Category</span>'
    +'<span style="width:58px;text-align:right;font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase;flex-shrink:0">Record</span>'
    +'<span style="width:100px;text-align:center;font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase;flex-shrink:0">Hit Rate</span>'
    +'<span style="width:80px;text-align:right;font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase;flex-shrink:0">Net P/L</span>'
    +'<span style="width:72px;text-align:right;font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase;flex-shrink:0">ROI</span>'
    +'</div>';
  var body=''; graded.forEach(function(x,i){ body+=_row(x[0],x[1],i+1); });
  CAT_ORDER.forEach(function(k){ if(gset[k]) return; var pc=perCat[k]||{w:0,l:0,net:0,counted:0,skipped:0}; body+=_row(k,pc,0); });
  var note=(d.days||0)?'':'<p style="color:#64748b;font-size:.78rem;padding:8px 2px">No graded days yet \u2014 records fill in automatically as slates go Final.</p>';
  var ce=document.getElementById('trk-cats'); if(ce) ce.innerHTML='<div style="border:1px solid #1e293b;border-radius:12px;overflow:hidden;margin-bottom:16px">'+head+body+'</div>'+note;
  _trkRenderDaily(stake);
  _trkRenderCLV();
  _trkRenderCalib(stake);
}
function _trkRenderDaily(stake){
  var d=window.__TRACK__; if(!d) return;
  var CAT_CFG=window.__TRK_CFG__||{}, CAT_ORDER=window.__TRK_ORDER__||[];
  var daily=d.daily||[];
  function _rc(w,n){ if(!n) return '#64748b'; var p=w/n; return p>=0.70?'#4ade80':(p>=0.55?'#facc15':'#f87171'); }
  var detByDate={}; (d.detail||[]).forEach(function(r){ (detByDate[r.date]=detByDate[r.date]||[]).push(r); });
  function _catRank(k){ var i=CAT_ORDER.indexOf(k); return i<0?999:i; }
  var dayBlocks=daily.slice().reverse().map(function(x){
    var groups={}; (detByDate[x.date]||[]).forEach(function(r){ var k=(r.category||'?')+'|'+(r.side||'OVER'); (groups[k]=groups[k]||[]).push(r); });
    var gkeys=Object.keys(groups).sort(function(a,b){ return _catRank(a)-_catRank(b); });
    var inner='', dayNet=0, dayHas=false;
    gkeys.forEach(function(k){
      var cfg=CAT_CFG[k]||{lbl:k.split('|').join(' '),icon:'📊'};
      var picks=groups[k]; var gw=picks.filter(function(p){return p.result==='WIN';}).length; var gn=picks.length; var gclr=_rc(gw,gn);
      inner+='<div style="margin:10px 0 4px;font-weight:800;font-size:.81rem;color:#cbd5e1">'+cfg.icon+' '+cfg.lbl+' <span style="color:'+gclr+';font-family:monospace;font-weight:900">'+gw+'/'+gn+'</span></div>';
      picks.forEach(function(p){
        var win=p.result==='WIN';
        var mk=win?'<span style="color:#4ade80">\u2713</span>':'<span style="color:#f87171">\u2717</span>';
        var act=(p.actual!=null)?('<span style="color:#cbd5e1">\u2192 '+p.actual+(p.stat?(' '+p.stat):'')+'</span>'):'';
        var odd=(p.odds!=null&&p.odds!=='')?('<span style="color:#64748b;font-family:monospace">'+((Number(p.odds)>0?'+':'')+p.odds)+'</span>'):'';
        var pl=_amProfit(p.odds,stake,win), plHtml, roiHtml;
        if(pl===null){ plHtml='<span style="color:#475569;font-family:monospace">\u2014</span>'; roiHtml=''; }
        else { dayNet+=pl; dayHas=true; var c=pl>=0?'#4ade80':'#f87171', rp=pl/stake*100;
          plHtml='<span style="font-family:monospace;font-weight:800;color:'+c+'">'+(pl>=0?'+$':'\u2212$')+Math.abs(pl).toFixed(0)+'</span>';
          roiHtml='<span style="font-family:monospace;font-weight:700;color:'+c+'">'+(rp>=0?'+':'\u2212')+Math.abs(rp).toFixed(0)+'%</span>'; }
        inner+='<div style="display:flex;gap:8px;align-items:center;padding:2px 0 2px 6px;font-size:.79rem">'
          +mk
          +'<span style="color:#e2e8f0;min-width:130px">'+p.name+'</span>'
          +'<span style="color:#94a3b8;min-width:115px">'+p.pick+'</span>'
          +act+odd
          +'<span style="margin-left:auto;display:flex;gap:12px;align-items:center"><span style="min-width:52px;text-align:right">'+plHtml+'</span><span style="min-width:44px;text-align:right">'+roiHtml+'</span></span>'
          +'</div>';
      });
    });
    if(!inner) inner='<div style="color:#64748b;font-size:.79rem;padding:4px 6px">No decided picks recorded this day.</div>';
    var dn=dayHas?(' \u00b7 Net <span style="color:'+(dayNet>=0?'#4ade80':'#f87171')+';font-weight:800">'+(dayNet>=0?'+$':'\u2212$')+Math.abs(dayNet).toFixed(0)+'</span>'):'';
    return '<details style="border-bottom:1px solid #1f2937;padding:7px 0"><summary style="cursor:pointer;font-weight:800;color:#e2e8f0;font-size:.88rem">'+x.date+' \u2014 <span style="color:#4ade80">'+x.wins+'W</span> <span style="color:#f87171">'+x.losses+'L</span> <span style="color:'+_twColor(x.wins,x.losses)+'">('+_twPct(x.wins,x.losses)+')</span>'+dn+'</summary><div style="padding:2px 0 12px">'+inner+'</div></details>';
  }).join('');
  var de=document.getElementById('trk-daily'); if(!de) return;
  de.innerHTML=daily.length?'<details open style="margin-top:0"><summary style="cursor:pointer;font-weight:700;color:#a78bfa;padding:10px 0;border-bottom:1px solid #1f2937">📅 Daily \u2014 every pick by category ('+daily.length+' day'+(daily.length===1?'':'s')+')</summary><div style="margin-top:6px">'+dayBlocks+'</div></details>':'';
}
// ===== Track Record tabs: Daily / Weekly (last 7 days) / Monthly =====
var _TRK_KEYS=['top10_batter','top10_pitcher','hitter_overs','hitter_unders','runs','tb_under','tb_over','rbi','batter_walks','hrr','pitcher_ks','pitcher_props'];
// Main Track Record flatten ALSO pulls in Value Plays so it surfaces in the daily/
// weekly/monthly views. Overflow (ranks 11-20) is deliberately NOT here — it lives
// ONLY in the Overflow tab. Kept SEPARATE from _TRK_KEYS so Best Bets (which shares
// _trkFlatten) stays a top-10-only board.
var _TRK_KEYS_FULL=_TRK_KEYS.concat(['value_plays']);
function _trkTodayISO(){ var d=new Date(); var z=d.getTimezoneOffset()*60000; return new Date(d.getTime()-z).toISOString().slice(0,10); }
function _isoShift(iso,days){ var d=new Date(iso+'T00:00:00Z'); d.setUTCDate(d.getUTCDate()+days); return d.toISOString().slice(0,10); }
function _trkRC(w,n){ if(!n) return '#64748b'; var p=w/n; return p>=0.70?'#4ade80':(p>=0.55?'#facc15':'#f87171'); }
function _trkBar(pct,clr){ return '<div style="height:8px;border-radius:4px;background:#1e293b;overflow:hidden;width:80px;display:inline-block;vertical-align:middle"><div style="height:100%;width:'+pct+'%;background:'+clr+';border-radius:4px"></div></div>'; }
function _trkTabBtn(id,label){ var active=window.__TRK_TAB__===id; return '<button onclick="_trkTab(&#39;'+id+'&#39;)" style="background:'+(active?'#7c3aed':'#1e293b')+';color:'+(active?'#fff':'#cbd5e1')+';border:none;border-radius:8px;padding:8px 20px;font-size:.86rem;font-weight:800;cursor:pointer">'+label+'</button>'; }
function _trkTab(t){ window.__TRK_BET__=_trkStake(); window.__TRK_TAB__=t; renderTrackRecord(window.__TRACK__); }
function _trkBetInput(){ window.__TRK_BET__=_trkStake(); _trkRenderActive(); }
// Every date that has tracked data (locked ledger + on-demand grade cache).
function _edgeAllDates(){
  var d=window.__TRACK__||{}, set={};
  (d.detail||[]).forEach(function(r){ if(r.date) set[r.date]=true; });
  var cache=window.__TRK_GRADE_CACHE__||{};
  Object.keys(cache).forEach(function(dt){ if(cache[dt]&&cache[dt]!=='LOADING'&&!cache[dt].__error__) set[dt]=true; });
  return Object.keys(set);
}
// Shared record-modal renderers (Edge / Best Bets / Proj Edge all use these so
// every view shows the same summary -> BY MARKET breakdown -> ALL PLAYS list).
function _recSecHdr(t){ return '<div style="font-size:.6rem;color:#64748b;font-weight:800;letter-spacing:.07em;margin:12px 0 4px 2px">'+t+'</div>'; }
function _recPlaySort(a,b){ var dc=(b.date||'').localeCompare(a.date||''); return dc!==0?dc:((b.edge||0)-(a.edge||0)); }
function _recCatRows(crows){
  if(!crows.length) return '';
  var h='<div style="font-size:.62rem;color:#475569;font-weight:800;letter-spacing:.06em;display:grid;grid-template-columns:1fr 56px 48px 72px;gap:0;padding:4px 8px;border-bottom:1px solid #1e293b"><span>MARKET</span><span style="text-align:right">W-L</span><span style="text-align:right">BETS</span><span style="text-align:right">NET</span></div>';
  crows.forEach(function(c,i){
    var cn=c.net>=0?'#4ade80':'#f87171';
    h+='<div style="display:grid;grid-template-columns:1fr 56px 48px 72px;gap:0;padding:7px 8px;border-bottom:1px solid #0f172a;background:'+(i%2?'#070e1b':'#050c18')+'"><span style="color:#cbd5e1;font-size:.77rem;font-weight:600">'+_esc(c.lbl)+'</span><span style="text-align:right;color:#e2e8f0;font-size:.77rem">'+c.w+'-'+c.l+'</span><span style="text-align:right;color:#64748b;font-size:.75rem">'+c.counted+'</span><span style="text-align:right;font-size:.77rem;font-weight:800;color:'+cn+'">$'+(c.net>=0?'+':'')+c.net.toFixed(2)+'</span></div>';
  });
  return h;
}
function _recPlaysRows(plays,lastHdr,lastClr,lastFn,catFn,showDate){
  var h='<div style="font-size:.62rem;color:#475569;font-weight:800;letter-spacing:.06em;display:grid;grid-template-columns:1fr 38px 58px 52px;gap:0;padding:4px 8px;border-bottom:1px solid #1e293b"><span>PLAY</span><span style="text-align:right">W/L</span><span style="text-align:right">ODDS</span><span style="text-align:right">'+lastHdr+'</span></div>';
  plays.forEach(function(r,i){
    var win=r.result==='WIN', rc=win?'#4ade80':'#f87171';
    var od=_effOdds(r), odStr=(od!=null&&isFinite(od))?((od>0?'+':'')+od):'\u2014';
    var bc=catFn?catFn(r.category||'?'):(r.category||'?');
    var sub=bc+' '+(r.side||'OVER')+((showDate&&r.date)?(' \u00b7 '+r.date):'');
    h+='<div style="display:grid;grid-template-columns:1fr 38px 58px 52px;gap:0;padding:7px 8px;border-bottom:1px solid #0f172a;background:'+(i%2?'#070e1b':'#050c18')+'"><div style="min-width:0"><div style="color:#e2e8f0;font-size:.78rem;font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+_esc(r.name||'?')+'</div><div style="color:#64748b;font-size:.67rem">'+_esc(sub)+'</div></div><span style="text-align:right;align-self:center;font-weight:800;color:'+rc+';font-size:.78rem">'+(win?'W':'L')+'</span><span style="text-align:right;align-self:center;color:#cbd5e1;font-size:.74rem;font-family:monospace">'+odStr+'</span><span style="text-align:right;align-self:center;color:'+lastClr+';font-size:.74rem;font-weight:700">'+lastFn(r)+'</span></div>';
  });
  return h;
}
// ── Proj Edge Tracking ──────────────────────────────────────────────────────
// Own forward-only record for the Proj Edge board, SEPARATE from Track Record.
// Qualify a graded pick the same way the live Proj Edge board does:
//   Pitchers  -> our projection beats the line (gap > 0 on the picked side).
//   Hitters   -> any positive model edge over the book price.
// Pitcher rows now carry a `proj` field (banked going forward), so this fills in
// from the day the build ships. Reuses the shared grade cache. Edge / Best Bets
// records untouched.
var _PE_KEYS=['top10_batter','top10_pitcher','hitter_overs','hitter_more','hitter_unders','runs','tb_under','tb_over','rbi','batter_walks','hrr','pitcher_ks','pitcher_props','overflow'];
function _peFlatten(g){ var out=[]; if(!g||g==='LOADING'||g.__error__) return out; _PE_KEYS.forEach(function(k){ (g[k]||[]).forEach(function(r){ out.push(r); }); }); return out; }
function _peBaseCat(c){ c=c||'?'; if(c==='Hitter Hits (More)') return 'Hitter Hits'; if(c.slice(-6)===' (OVF)') return c.slice(0,-6); return c; }
function _peIsPitcher(cat){ return (cat||'').indexOf('Pitcher')===0; }
function _peOK(r){
  if(_trkSkipMeta(r)) return false;
  var cat=r.category||'';
  if(_isHrCat(cat)) return false;   // HR removed from Proj Edge board + record (own tracker keeps it)
  if(_peIsPitcher(cat)){
    var pj=r.proj; if(pj==null||!isFinite(Number(pj))) return false;
    var ln=r.line; if(ln==null||!isFinite(Number(ln))) return false;
    var sd=(r.side||'OVER').toUpperCase();
    var gap=sd==='OVER'?(Number(pj)-Number(ln)):(Number(ln)-Number(pj));
    return gap>0;
  }
  return (r.edge||0)>0;
}
function _peVal(r){
  var cat=r.category||'';
  if(_peIsPitcher(cat)){
    var pj=r.proj, ln=r.line; if(pj==null||ln==null) return '&#x2014;';
    var sd=(r.side||'OVER').toUpperCase();
    var gap=sd==='OVER'?(Number(pj)-Number(ln)):(Number(ln)-Number(pj));
    return (gap>0?'+':'')+gap.toFixed(1);
  }
  return ((r.edge||0)*100).toFixed(1)+'%';
}
// Numeric ranking value mirroring the live Proj Edge board (_openProjEdge):
// hitters by model edge, pitchers by projection-vs-line gap on the picked side.
function _peRank(r){
  var cat=r.category||'';
  if(_peIsPitcher(cat)){
    var pj=Number(r.proj), ln=Number(r.line);
    if(!isFinite(pj)||!isFinite(ln)) return -1e9;
    var sd=(r.side||'OVER').toUpperCase();
    return sd==='OVER'?(pj-ln):(ln-pj);
  }
  return (r.edge||0);
}
// The live Proj Edge popup shows only the TOP 5 plays per category (by edge/gap).
// Cap each base category to its best 5 so the record grades the SAME set the
// button shows &#x2014; not the deep overflow ranks (11-30) the popup never lists.
function _peCapPerCat(rows){
  var by={};
  rows.forEach(function(r){ var bc=_peBaseCat(r.category||'?'); (by[bc]=by[bc]||[]).push(r); });
  var out=[];
  Object.keys(by).forEach(function(bc){
    by[bc].sort(function(a,b){ return _peRank(b)-_peRank(a); });
    for(var i=0;i<by[bc].length&&i<5;i++) out.push(by[bc][i]);
  });
  return out;
}
function _peRowsForDate(date){
  var d=window.__TRACK__||{}; var rows=[]; var have=false;
  (d.detail||[]).forEach(function(r){ if(r.date===date){ have=true; if(_peOK(r)&&(r.result==='WIN'||r.result==='LOSS')) rows.push(r); } });
  if(!have){
    var g=(window.__TRK_GRADE_CACHE__||{})[date];
    if(!g||g==='LOADING'||g.__error__||!g.all_final) return [];
    _peFlatten(g).forEach(function(r){ if(_peOK(r)&&(r.result==='WIN'||r.result==='LOSS')) rows.push(r); });
  }
  rows=_peCapPerCat(rows);
  rows.sort(function(a,b){ return (b.edge||0)-(a.edge||0); });
  return rows;
}
function _peStatsAllTime(){ window.__PE_DATE__=''; _peStatsRender(); }
function _peStatsSetDate(val){ if(!val){ _peStatsAllTime(); return; } window.__PE_DATE__=val; _peLoadDay(val); }
async function _peLoadDay(date){
  window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{};
  var d=window.__TRACK__||{};
  var inDetail=(d.detail||[]).some(function(r){ return r.date===date; });
  var cur=window.__TRK_GRADE_CACHE__[date];
  if(inDetail||(cur&&cur!=='LOADING')){ _peStatsRender(); return; }
  var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||'';
  var adm=new URLSearchParams(location.search).get('admin')||'';
  window.__TRK_GRADE_CACHE__[date]='LOADING'; _peStatsRender();
  try{ var res=await fetch('/api/grade/'+date+'?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'')); if(!res.ok){ var t=await res.text(); window.__TRK_GRADE_CACHE__[date]={__error__:(t||'No picks for this date')}; } else { window.__TRK_GRADE_CACHE__[date]=await res.json(); } }catch(e){ window.__TRK_GRADE_CACHE__[date]={__error__:String((e&&e.message)||e)}; }
  _peStatsRender();
}
function _peStatsWrap(bodyHtml){
  var ov2=document.getElementById('pe-stats-modal'); if(!ov2) return;
  var dateMode=!!window.__PE_DATE__, date=window.__PE_DATE__||'';
  var sub=dateMode?('proj edge &#xB7; projection beats the line &#xB7; '+_weekdayName(date)+' '+date):'proj edge plays tracked each day &#xB7; pitchers: proj beats line &#xB7; hitters: +edge &#xB7; tap a day for that slate&#39;s plays';
  ov2.innerHTML='<div style="background:#06141f;border:1px solid #0c4a6e;border-radius:18px;width:100%;max-width:460px;max-height:88vh;display:flex;flex-direction:column;box-shadow:0 24px 80px rgba(0,0,0,.7)" onclick="event.stopPropagation()">'
    +'<div style="display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid #1e293b;flex-shrink:0">'
    +'<div><div style="font-weight:900;color:#38bdf8;font-size:1rem">&#9650; Proj Edge Record</div>'
    +'<div style="color:#64748b;font-size:.71rem;margin-top:2px">'+sub+'</div></div>'
    +'<button onclick="document.getElementById(&#39;pe-stats-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem;flex-shrink:0">&#215;</button>'
    +'</div>'
    +'<div style="overflow-y:auto;flex:1">'+bodyHtml+'</div>'
    +'</div>';
}
function _peStatsRender(){
  var ov2=document.getElementById('pe-stats-modal'); if(!ov2) return;
  var d=window.__TRACK__||{}, stake=_trkStake();
  var dateMode=!!window.__PE_DATE__, date=window.__PE_DATE__||'';
  var today=window.__TRK_TODAY__||_trkTodayISO();
  var loadingMsg='', pool=[];
  if(dateMode){
    var cache=window.__TRK_GRADE_CACHE__||{};
    var inDetail=(d.detail||[]).some(function(r){ return r.date===date; });
    var g=cache[date];
    if(!inDetail && (g===undefined||g==='LOADING')) loadingMsg='Loading\u2026';
    else if(!inDetail && g&&g.__error__) loadingMsg=g.__error__||'No picks for this date.';
    else if(!inDetail && g && !g.all_final) loadingMsg='This slate is not final yet. The Proj Edge Record fills in once every game on '+date+' goes Final.';
    else pool=_peRowsForDate(date);
  } else {
    _edgeAllDates().forEach(function(dt){ var t=_peRowsForDate(dt); for(var i=0;i<t.length;i++){ var rr=t[i]; if(!rr.date){ var cc={}; for(var kk in rr) cc[kk]=rr[kk]; cc.date=dt; rr=cc; } pool.push(rr); } });
  }
  var ov={w:0,l:0,net:0,counted:0}, cats={};
  pool.forEach(function(r){
    var win=r.result==='WIN', od=_effOdds(r);
    var pl=_amProfit(od,stake,win); if(pl===null) return;
    if(win) ov.w++; else ov.l++; ov.net+=pl; ov.counted++;
    var bc=_peBaseCat(r.category||'?');
    var k=bc+'|'+(r.side||'OVER');
    var c=cats[k]=cats[k]||{w:0,l:0,net:0,counted:0,lbl:bc+' '+(r.side||'OVER')};
    if(win) c.w++; else c.l++; c.net+=pl; c.counted++;
  });
  var roiClr=ov.net>=0?'#4ade80':'#f87171';
  var roiStr=ov.counted?(((ov.net/(ov.counted*stake))*100).toFixed(1)+'%'):'&#x2014;';
  var netStr='$'+(ov.net>=0?'+':'')+ov.net.toFixed(2);
  var body='<div style="padding:14px 16px">';
  body+='<div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px">';
  body+='<button onclick="_peStatsAllTime()" style="background:'+(dateMode?'#1e293b':'#0369a1')+';color:'+(dateMode?'#cbd5e1':'#fff')+';border:none;border-radius:7px;padding:6px 12px;font-size:.78rem;font-weight:700;cursor:pointer">All-time</button>';
  body+='<label style="font-size:.78rem;color:#94a3b8;display:inline-flex;align-items:center;gap:6px">Day <input type="date" value="'+date+'" max="'+today+'" onchange="_peStatsSetDate(this.value)" style="background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:5px 8px;font-size:.78rem"></label>';
  if(dateMode) body+='<span style="font-weight:800;color:#7dd3fc;font-size:.85rem">'+_weekdayName(date)+'</span>';
  body+='</div>';
  if(dateMode && loadingMsg){
    body+='<div style="color:#64748b;padding:24px;text-align:center">'+_esc(loadingMsg)+'</div></div>';
    _peStatsWrap(body); return;
  }
  body+='<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:14px">';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">Record</div><div style="font-weight:900;color:#e2e8f0;font-size:1.1rem">'+ov.w+'-'+ov.l+'</div></div>';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">ROI</div><div style="font-weight:900;color:'+roiClr+';font-size:1.1rem">'+roiStr+'</div></div>';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">Net @ $'+stake+'</div><div style="font-weight:900;color:'+roiClr+';font-size:1.05rem">'+netStr+'</div></div>';
  body+='</div>';
  var _crows=Object.keys(cats).map(function(k){ return cats[k]; }).sort(function(a,b){ return b.net-a.net; });
  var _cat=_recCatRows(_crows);
  var _plays=pool.slice().sort(_recPlaySort);
  if(_cat) body+=_recSecHdr('BY MARKET')+_cat;
  if(_plays.length) body+=_recSecHdr('ALL PLAYS &#xB7; '+_plays.length)+_recPlaysRows(_plays,'VALUE','#38bdf8',function(r){ return _peVal(r); },_peBaseCat,!dateMode);
  else if(!_cat) body+='<div style="color:#64748b;padding:20px;text-align:center">No proj edge plays graded'+(dateMode?' on this date.':' yet.<br><span style="font-size:.74rem">This fills in automatically as each day&#39;s proj edge plays go Final &#x2014; pitcher rows start banking from this build forward.</span>')+'</div>';
  body+='</div>';
  _peStatsWrap(body);
}
function _openProjEdgeStats(){
  var d=window.__TRACK__; if(!d){ alert('Open Track Record first.'); return; }
  if(window.__PE_DATE__===undefined) window.__PE_DATE__='';
  var ov2=document.getElementById('pe-stats-modal');
  if(!ov2){ ov2=document.createElement('div'); ov2.id='pe-stats-modal'; ov2.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.85);z-index:10001;display:flex;align-items:center;justify-content:center;padding:16px'; ov2.onclick=function(e){ if(e.target===ov2) ov2.style.display='none'; }; document.body.appendChild(ov2); }
  _peStatsRender();
  ov2.style.display='flex';
}
// ── HRR Special Record ───────────────────────────────────────────────────────
// Forward-only record for the HRR Special confluence board (all 4 gates cleared).
// Reads from the hrr_special key in the grade cache — kept out of main Track
// Record to avoid double-counting with regular HRR overs.
function _hrrspRowsForDate(date){
  var d=window.__TRACK__||{}; var rows=[]; var have=false;
  (d.detail||[]).forEach(function(r){ if(r.date===date&&r.category==='HRR Special'){ have=true; if(r.result==='WIN'||r.result==='LOSS') rows.push(r); } });
  if(!have){
    var g=(window.__TRK_GRADE_CACHE__||{})[date];
    if(!g||g==='LOADING'||g.__error__||!g.all_final) return [];
    (g.hrr_special||[]).forEach(function(r){ if(r.result==='WIN'||r.result==='LOSS') rows.push(r); });
  }
  rows.sort(function(a,b){ return (b.edge||0)-(a.edge||0); });
  return rows;
}
function _hrrspStatsAllTime(){ window.__HRRSP_DATE__=''; _hrrspStatsRender(); }
function _hrrspStatsSetDate(val){ if(!val){ _hrrspStatsAllTime(); return; } window.__HRRSP_DATE__=val; _hrrspLoadDay(val); }
async function _hrrspLoadDay(date){
  window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{};
  var d=window.__TRACK__||{};
  var inDetail=(d.detail||[]).some(function(r){ return r.date===date&&r.category==='HRR Special'; });
  var cur=window.__TRK_GRADE_CACHE__[date];
  if(inDetail||(cur&&cur!=='LOADING')){ _hrrspStatsRender(); return; }
  var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||'';
  var adm=new URLSearchParams(location.search).get('admin')||'';
  window.__TRK_GRADE_CACHE__[date]='LOADING'; _hrrspStatsRender();
  try{ var res=await fetch('/api/grade/'+date+'?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'')); if(!res.ok){ var t=await res.text(); window.__TRK_GRADE_CACHE__[date]={__error__:(t||'No picks for this date')}; } else { window.__TRK_GRADE_CACHE__[date]=await res.json(); } }catch(e){ window.__TRK_GRADE_CACHE__[date]={__error__:String((e&&e.message)||e)}; }
  _hrrspStatsRender();
}
function _hrrspStatsWrap(bodyHtml){
  var ov2=document.getElementById('hrrsp-stats-modal'); if(!ov2) return;
  var dateMode=!!window.__HRRSP_DATE__, date=window.__HRRSP_DATE__||'';
  var sub=dateMode?('HRR Special &#xB7; all 4 confluence gates cleared &#xB7; '+_weekdayName(date)+' '+date):'HRR Special confluence picks &#xB7; all 4 gates cleared &#xB7; tap a day for that slate&#39;s plays';
  ov2.innerHTML='<div style="background:#0e0919;border:1px solid #5b21b6;border-radius:18px;width:100%;max-width:460px;max-height:88vh;display:flex;flex-direction:column;box-shadow:0 24px 80px rgba(0,0,0,.7)" onclick="event.stopPropagation()">'
    +'<div style="display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid #1e293b;flex-shrink:0">'
    +'<div><div style="font-weight:900;color:#a78bfa;font-size:1rem">&#11088; HRR Special Record</div>'
    +'<div style="color:#64748b;font-size:.71rem;margin-top:2px">'+sub+'</div></div>'
    +'<button onclick="document.getElementById(&#39;hrrsp-stats-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem;flex-shrink:0">&#215;</button>'
    +'</div>'
    +'<div style="overflow-y:auto;flex:1">'+bodyHtml+'</div>'
    +'</div>';
}
function _hrrspStatsRender(){
  var ov2=document.getElementById('hrrsp-stats-modal'); if(!ov2) return;
  var d=window.__TRACK__||{}, stake=_trkStake();
  var dateMode=!!window.__HRRSP_DATE__, date=window.__HRRSP_DATE__||'';
  var today=window.__TRK_TODAY__||_trkTodayISO();
  var loadingMsg='', pool=[];
  if(dateMode){
    var cache=window.__TRK_GRADE_CACHE__||{};
    var inDetail=(d.detail||[]).some(function(r){ return r.date===date&&r.category==='HRR Special'; });
    var g=cache[date];
    if(!inDetail && (g===undefined||g==='LOADING')) loadingMsg='Loading\u2026';
    else if(!inDetail && g&&g.__error__) loadingMsg=g.__error__||'No picks for this date.';
    else if(!inDetail && g && !g.all_final) loadingMsg='This slate is not final yet. The HRR Special Record fills in once every game on '+date+' goes Final.';
    else pool=_hrrspRowsForDate(date);
  } else {
    _edgeAllDates().forEach(function(dt){ var t=_hrrspRowsForDate(dt); for(var i=0;i<t.length;i++){ var rr=t[i]; if(!rr.date){ var cc={}; for(var kk in rr) cc[kk]=rr[kk]; cc.date=dt; rr=cc; } pool.push(rr); } });
  }
  var ov={w:0,l:0,net:0,counted:0};
  pool.forEach(function(r){
    var win=r.result==='WIN', od=_effOdds(r);
    var pl=_amProfit(od,stake,win); if(pl===null) return;
    if(win) ov.w++; else ov.l++; ov.net+=pl; ov.counted++;
  });
  var roiClr=ov.net>=0?'#4ade80':'#f87171';
  var roiStr=ov.counted?(((ov.net/(ov.counted*stake))*100).toFixed(1)+'%'):'&#x2014;';
  var netStr='$'+(ov.net>=0?'+':'')+ov.net.toFixed(2);
  var body='<div style="padding:14px 16px">';
  body+='<div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px">';
  body+='<button onclick="_hrrspStatsAllTime()" style="background:'+(dateMode?'#1e293b':'#4c1d95')+';color:'+(dateMode?'#cbd5e1':'#fff')+';border:none;border-radius:7px;padding:6px 12px;font-size:.78rem;font-weight:700;cursor:pointer">All-time</button>';
  body+='<label style="font-size:.78rem;color:#94a3b8;display:inline-flex;align-items:center;gap:6px">Day <input type="date" value="'+date+'" max="'+today+'" onchange="_hrrspStatsSetDate(this.value)" style="background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:5px 8px;font-size:.78rem"></label>';
  if(dateMode) body+='<span style="font-weight:800;color:#c4b5fd;font-size:.85rem">'+_weekdayName(date)+'</span>';
  body+='</div>';
  if(dateMode && loadingMsg){
    body+='<div style="color:#64748b;padding:24px;text-align:center">'+_esc(loadingMsg)+'</div></div>';
    _hrrspStatsWrap(body); return;
  }
  body+='<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:14px">';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">Record</div><div style="font-weight:900;color:#e2e8f0;font-size:1.1rem">'+ov.w+'-'+ov.l+'</div></div>';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">ROI</div><div style="font-weight:900;color:'+roiClr+';font-size:1.1rem">'+roiStr+'</div></div>';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">Net @ $'+stake+'</div><div style="font-weight:900;color:'+roiClr+';font-size:1.05rem">'+netStr+'</div></div>';
  body+='</div>';
  var _plays=pool.slice().sort(_recPlaySort);
  if(_plays.length) body+=_recSecHdr('ALL PLAYS &#xB7; '+_plays.length)+_recPlaysRows(_plays,'EV','#a78bfa',function(r){ return r.ev!=null?((r.ev>0?'+':'')+((r.ev*100).toFixed(1))+'%'):'&#x2014;'; },function(c){ return c; },!dateMode);
  else body+='<div style="color:#64748b;padding:20px;text-align:center">No HRR Special plays graded'+(dateMode?' on this date.':' yet.<br><span style="font-size:.74rem">This fills in automatically as each day&#39;s HRR Special picks go Final.</span>')+'</div>';
  body+='</div>';
  _hrrspStatsWrap(body);
}
function _openHrrSpStats(){
  var d=window.__TRACK__; if(!d){ alert('Open Track Record first.'); return; }
  if(window.__HRRSP_DATE__===undefined) window.__HRRSP_DATE__='';
  var ov2=document.getElementById('hrrsp-stats-modal');
  if(!ov2){ ov2=document.createElement('div'); ov2.id='hrrsp-stats-modal'; ov2.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.85);z-index:10001;display:flex;align-items:center;justify-content:center;padding:16px'; ov2.onclick=function(e){ if(e.target===ov2) ov2.style.display='none'; }; document.body.appendChild(ov2); }
  _hrrspStatsRender();
  ov2.style.display='flex';
}
// ── Triple Split Club Record ─────────────────────────────────────────────────
// Forward-only record for the Triple Split Club board (>.275 in all three of
// today's splits: home/away, day/night, series game). Reads from the
// triple_split key in the grade cache — kept out of main Track Record to avoid
// double-counting with the regular Hits board (same "to record a hit" market).
function _tscRowsForDate(date){
  var d=window.__TRACK__||{}; var rows=[]; var have=false;
  (d.detail||[]).forEach(function(r){ if(r.date===date&&r.category==='Triple Split Club'){ have=true; if(r.result==='WIN'||r.result==='LOSS') rows.push(r); } });
  if(!have){
    var g=(window.__TRK_GRADE_CACHE__||{})[date];
    if(!g||g==='LOADING'||g.__error__||!g.all_final) return [];
    (g.triple_split||[]).forEach(function(r){ if(r.result==='WIN'||r.result==='LOSS') rows.push(r); });
  }
  rows.sort(function(a,b){ return (b.edge||0)-(a.edge||0); });
  return rows;
}
function _tscStatsAllTime(){ window.__TSC_DATE__=''; _tscStatsRender(); }
function _tscStatsSetDate(val){ if(!val){ _tscStatsAllTime(); return; } window.__TSC_DATE__=val; _tscLoadDay(val); }
async function _tscLoadDay(date){
  window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{};
  var d=window.__TRACK__||{};
  var inDetail=(d.detail||[]).some(function(r){ return r.date===date&&r.category==='Triple Split Club'; });
  var cur=window.__TRK_GRADE_CACHE__[date];
  if(inDetail||(cur&&cur!=='LOADING')){ _tscStatsRender(); return; }
  var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||'';
  var adm=new URLSearchParams(location.search).get('admin')||'';
  window.__TRK_GRADE_CACHE__[date]='LOADING'; _tscStatsRender();
  try{ var res=await fetch('/api/grade/'+date+'?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'')); if(!res.ok){ var t=await res.text(); window.__TRK_GRADE_CACHE__[date]={__error__:(t||'No picks for this date')}; } else { window.__TRK_GRADE_CACHE__[date]=await res.json(); } }catch(e){ window.__TRK_GRADE_CACHE__[date]={__error__:String((e&&e.message)||e)}; }
  _tscStatsRender();
}
function _tscStatsWrap(bodyHtml){
  var ov2=document.getElementById('tsc-stats-modal'); if(!ov2) return;
  var dateMode=!!window.__TSC_DATE__, date=window.__TSC_DATE__||'';
  var sub=dateMode?('Triple Split Club &#xB7; &gt;.275 in all 3 splits &#xB7; '+_weekdayName(date)+' '+date):'Triple Split Club &#xB7; &gt;.275 home/away, day/night &amp; series &#xB7; tap a day for that slate&#39;s plays';
  ov2.innerHTML='<div style="background:#04141c;border:1px solid #0e7490;border-radius:18px;width:100%;max-width:460px;max-height:88vh;display:flex;flex-direction:column;box-shadow:0 24px 80px rgba(0,0,0,.7)" onclick="event.stopPropagation()">'
    +'<div style="display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid #1e293b;flex-shrink:0">'
    +'<div><div style="font-weight:900;color:#22d3ee;font-size:1rem">&#128305; Triple Split Record</div>'
    +'<div style="color:#64748b;font-size:.71rem;margin-top:2px">'+sub+'</div></div>'
    +'<button onclick="document.getElementById(&#39;tsc-stats-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem;flex-shrink:0">&#215;</button>'
    +'</div>'
    +'<div style="overflow-y:auto;flex:1">'+bodyHtml+'</div>'
    +'</div>';
}
function _tscStatsRender(){
  var ov2=document.getElementById('tsc-stats-modal'); if(!ov2) return;
  var d=window.__TRACK__||{}, stake=_trkStake();
  var dateMode=!!window.__TSC_DATE__, date=window.__TSC_DATE__||'';
  var today=window.__TRK_TODAY__||_trkTodayISO();
  var loadingMsg='', pool=[];
  if(dateMode){
    var cache=window.__TRK_GRADE_CACHE__||{};
    var inDetail=(d.detail||[]).some(function(r){ return r.date===date&&r.category==='Triple Split Club'; });
    var g=cache[date];
    if(!inDetail && (g===undefined||g==='LOADING')) loadingMsg='Loading\u2026';
    else if(!inDetail && g&&g.__error__) loadingMsg=g.__error__||'No picks for this date.';
    else if(!inDetail && g && !g.all_final) loadingMsg='This slate is not final yet. The Triple Split Record fills in once every game on '+date+' goes Final.';
    else pool=_tscRowsForDate(date);
  } else {
    _edgeAllDates().forEach(function(dt){ var t=_tscRowsForDate(dt); for(var i=0;i<t.length;i++){ var rr=t[i]; if(!rr.date){ var cc={}; for(var kk in rr) cc[kk]=rr[kk]; cc.date=dt; rr=cc; } pool.push(rr); } });
  }
  var ov={w:0,l:0,net:0,counted:0};
  pool.forEach(function(r){
    var win=r.result==='WIN', od=_effOdds(r);
    var pl=_amProfit(od,stake,win); if(pl===null) return;
    if(win) ov.w++; else ov.l++; ov.net+=pl; ov.counted++;
  });
  var roiClr=ov.net>=0?'#4ade80':'#f87171';
  var roiStr=ov.counted?(((ov.net/(ov.counted*stake))*100).toFixed(1)+'%'):'&#x2014;';
  var netStr='$'+(ov.net>=0?'+':'')+ov.net.toFixed(2);
  var body='<div style="padding:14px 16px">';
  body+='<div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px">';
  body+='<button onclick="_tscStatsAllTime()" style="background:'+(dateMode?'#1e293b':'#0e7490')+';color:'+(dateMode?'#cbd5e1':'#fff')+';border:none;border-radius:7px;padding:6px 12px;font-size:.78rem;font-weight:700;cursor:pointer">All-time</button>';
  body+='<label style="font-size:.78rem;color:#94a3b8;display:inline-flex;align-items:center;gap:6px">Day <input type="date" value="'+date+'" max="'+today+'" onchange="_tscStatsSetDate(this.value)" style="background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:5px 8px;font-size:.78rem"></label>';
  if(dateMode) body+='<span style="font-weight:800;color:#67e8f9;font-size:.85rem">'+_weekdayName(date)+'</span>';
  body+='</div>';
  if(dateMode && loadingMsg){
    body+='<div style="color:#64748b;padding:24px;text-align:center">'+_esc(loadingMsg)+'</div></div>';
    _tscStatsWrap(body); return;
  }
  body+='<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:14px">';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">Record</div><div style="font-weight:900;color:#e2e8f0;font-size:1.1rem">'+ov.w+'-'+ov.l+'</div></div>';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">ROI</div><div style="font-weight:900;color:'+roiClr+';font-size:1.1rem">'+roiStr+'</div></div>';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">Net @ $'+stake+'</div><div style="font-weight:900;color:'+roiClr+';font-size:1.05rem">'+netStr+'</div></div>';
  body+='</div>';
  var _plays=pool.slice().sort(_recPlaySort);
  if(_plays.length) body+=_recSecHdr('ALL PLAYS &#xB7; '+_plays.length)+_recPlaysRows(_plays,'EV','#22d3ee',function(r){ return r.ev!=null?((r.ev>0?'+':'')+((r.ev*100).toFixed(1))+'%'):'&#x2014;'; },function(c){ return c; },!dateMode);
  else body+='<div style="color:#64748b;padding:20px;text-align:center">No Triple Split plays graded'+(dateMode?' on this date.':' yet.<br><span style="font-size:.74rem">This fills in automatically as each day&#39;s Triple Split picks go Final.</span>')+'</div>';
  body+='</div>';
  _tscStatsWrap(body);
}
function _openTscStats(){
  var d=window.__TRACK__; if(!d){ alert('Open Track Record first.'); return; }
  if(window.__TSC_DATE__===undefined) window.__TSC_DATE__='';
  var ov2=document.getElementById('tsc-stats-modal');
  if(!ov2){ ov2=document.createElement('div'); ov2.id='tsc-stats-modal'; ov2.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.85);z-index:10001;display:flex;align-items:center;justify-content:center;padding:16px'; ov2.onclick=function(e){ if(e.target===ov2) ov2.style.display='none'; }; document.body.appendChild(ov2); }
  _tscStatsRender();
  ov2.style.display='flex';
}
// ── 5 Star Split card + record ───────────────────────────────────────────────
// 5 Star Split = Triple Split qualifiers that ALSO clear 60%+ games with a hit
// vs today's opponent AND 60%+ over their last 10 games, each carrying its single
// best production market (TB/Runs/RBI/HRR OVER). Career-vs-pitcher rides along as
// display-only reference. Own forward-only record (own button + modal), kept out
// of the main Track Record so it never double-counts with the per-market boards.
function _fssBoxKey(p){ return ({tb:'total_bases',runs:'runs',rbi:'rbi',hrr:'hrr'})[p.pick_market]||'total_bases'; }
function _fssCard(p, rank, pfx) {
  pfx = pfx || 'fss';
  const abbr = _mlbTeamAbbr(p.team);
  const teamLogo = abbr ? `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png` : '';
  const rnkColors = rank===1?['#c4b5fd','#000']:rank===2?['#a78bfa','#000']:rank===3?['#8b5cf6','#fff']:['#1e1b3a','#a78bfa'];
  const sideCls = p.side==='HOME'?'badge-home':'badge-away';
  const od = p.odds;
  const odDisp = od!=null?(od>0?'+':'')+od:'—';
  const dnLbl = p.dn_label||'Day/Night';
  const gno = p.series_gno||p.series_game||'';
  const vp = p.vs_pit||{};
  window.__FSS_REG__=window.__FSS_REG__||{}; window.__FSS_REG__[pfx+rank]=p;
  function _g(lbl,val){
    return '<div style="display:flex;align-items:center;justify-content:space-between;font-size:.72rem;margin-top:4px">'
      +'<span style="color:#94a3b8"><span style="color:#a78bfa">&#10003;</span> '+lbl+'</span>'
      +'<span style="color:#ddd6fe;font-weight:700;font-family:monospace">'+val+'</span></div>';
  }
  var vpRow = (vp && (vp.ab||0)>0)
    ? ('<div style="display:flex;align-items:center;justify-content:space-between;font-size:.68rem;margin-top:6px;padding-top:6px;border-top:1px dashed #2a2440"><span style="color:#64748b">vs '+_esc(p.pitcher||'pitcher')+' (career)</span><span style="color:#94a3b8;font-family:monospace">'+_esc(vp.display||'—')+' <span style="color:#475569">ref</span></span></div>')
    : '';
  return `<div class="mlb-pick-card" onclick="_hitForm(window.__FSS_REG__['${pfx}${rank}'])" title="Click for recent form" style="cursor:pointer;border:1px solid rgba(167,139,250,.5)">
    <div class="mlb-card-header" style="background:linear-gradient(135deg,#1e1b3a 0%,#0c0a1a 100%)">${_cardHdr(rank,rnkColors,_catLbl('5 STAR','#a78bfa'),teamLogo,p.team,_seriesTag(p,'O',false,2))}</div>
    ${_nameBar(rank,rnkColors,p.batter_id,p.name)}
    <div class="mlb-card-body">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <span style="font-size:.82rem;color:#94a3b8">vs <strong style="color:#fff">${p.opp||'—'}</strong></span>
        <span class="badge ${sideCls}">${p.side}</span>
      </div>
      <div style="margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <div style="font-size:.6rem;font-weight:800;letter-spacing:.07em;color:#a78bfa;text-transform:uppercase">All 3 Splits &gt; .275</div>
        ${_g((p.side==='HOME'?'Home':'Away')+' BA', p.ha_disp||'—')}
        ${_g(dnLbl+' BA', p.dn_disp||'—')}
        ${_g('Series G'+(gno||'?')+' BA', p.series_disp||'—')}
      </div>
      <div style="margin-top:6px;padding-top:6px;border-top:1px solid #1f1f1f">
        <div style="font-size:.6rem;font-weight:800;letter-spacing:.07em;color:#a78bfa;text-transform:uppercase">Consistency</div>
        ${_g('vs '+(p.opp||'opp')+' hit%', (p.vt_pct!=null?p.vt_pct+'%':'—')+' ('+(p.vt_hit_g||0)+'/'+(p.vt_g||0)+')')}
        ${_g('Last 10 hit%', (p.l10_hit_pct!=null?p.l10_hit_pct+'%':'—')+' ('+(p.l10_hit_g||0)+'/'+(p.l10_g||0)+')')}
      </div>
      <div style="margin-top:8px;padding-top:8px;border-top:1px solid #1f1f1f">
        <div style="display:flex;align-items:center;justify-content:space-between">
          <span style="font-size:.62rem;font-weight:800;letter-spacing:.06em;color:#a78bfa;text-transform:uppercase">Best Production Play</span>
          <span style="font-size:.62rem;color:#64748b">${p.pick_rate!=null?p.pick_rate+'% L10':''}</span>
        </div>
        <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
          <span style="font-size:.85rem;color:#c4b5fd;font-weight:900">OVER ${p.line} ${p.stat_label||''}</span>
          <span style="font-family:monospace;color:#fbbf24;font-weight:700;font-size:.9rem">${odDisp}${_bookTag(p)}</span>
        </div>
      </div>
      ${vpRow}
    </div>
  ${_betBtn(p,'5 Star Split','OVER',_fssBoxKey(p),(p.stat_label||'Total Bases'),(p.line!=null?p.line:1.5),od)}
  </div>`;
}
function _fssRowsForDate(date){
  var d=window.__TRACK__||{}; var rows=[]; var have=false;
  (d.detail||[]).forEach(function(r){ if(r.date===date&&r.category==='5 Star Split'){ have=true; if(r.result==='WIN'||r.result==='LOSS') rows.push(r); } });
  if(!have){
    var g=(window.__TRK_GRADE_CACHE__||{})[date];
    if(!g||g==='LOADING'||g.__error__||!g.all_final) return [];
    (g.five_star_split||[]).forEach(function(r){ if(r.result==='WIN'||r.result==='LOSS') rows.push(r); });
  }
  rows.sort(function(a,b){ return (b.edge||0)-(a.edge||0); });
  return rows;
}
function _fssStatsAllTime(){ window.__FSS_DATE__=''; _fssStatsRender(); }
function _fssStatsSetDate(val){ if(!val){ _fssStatsAllTime(); return; } window.__FSS_DATE__=val; _fssLoadDay(val); }
async function _fssLoadDay(date){
  window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{};
  var d=window.__TRACK__||{};
  var inDetail=(d.detail||[]).some(function(r){ return r.date===date&&r.category==='5 Star Split'; });
  var cur=window.__TRK_GRADE_CACHE__[date];
  if(inDetail||(cur&&cur!=='LOADING')){ _fssStatsRender(); return; }
  var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||'';
  var adm=new URLSearchParams(location.search).get('admin')||'';
  window.__TRK_GRADE_CACHE__[date]='LOADING'; _fssStatsRender();
  try{ var res=await fetch('/api/grade/'+date+'?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'')); if(!res.ok){ var t=await res.text(); window.__TRK_GRADE_CACHE__[date]={__error__:(t||'No picks for this date')}; } else { window.__TRK_GRADE_CACHE__[date]=await res.json(); } }catch(e){ window.__TRK_GRADE_CACHE__[date]={__error__:String((e&&e.message)||e)}; }
  _fssStatsRender();
}
function _fssStatsWrap(bodyHtml){
  var ov2=document.getElementById('fss-stats-modal'); if(!ov2) return;
  var dateMode=!!window.__FSS_DATE__, date=window.__FSS_DATE__||'';
  var sub=dateMode?('5 Star Split &#xB7; all 5 gates cleared &#xB7; '+_weekdayName(date)+' '+date):'5 Star Split &#xB7; Triple Split + &ge;60% vs team + &ge;60% last 10 &#xB7; tap a day for that slate&#39;s plays';
  ov2.innerHTML='<div style="background:#0c0a1a;border:1px solid #7c3aed;border-radius:18px;width:100%;max-width:460px;max-height:88vh;display:flex;flex-direction:column;box-shadow:0 24px 80px rgba(0,0,0,.7)" onclick="event.stopPropagation()">'
    +'<div style="display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid #1e293b;flex-shrink:0">'
    +'<div><div style="font-weight:900;color:#a78bfa;font-size:1rem">&#11088; 5 Star Split Record</div>'
    +'<div style="color:#64748b;font-size:.71rem;margin-top:2px">'+sub+'</div></div>'
    +'<button onclick="document.getElementById(&#39;fss-stats-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem;flex-shrink:0">&#215;</button>'
    +'</div>'
    +'<div style="overflow-y:auto;flex:1">'+bodyHtml+'</div>'
    +'</div>';
}
function _fssStatsRender(){
  var ov2=document.getElementById('fss-stats-modal'); if(!ov2) return;
  var d=window.__TRACK__||{}, stake=_trkStake();
  var dateMode=!!window.__FSS_DATE__, date=window.__FSS_DATE__||'';
  var today=window.__TRK_TODAY__||_trkTodayISO();
  var loadingMsg='', pool=[];
  if(dateMode){
    var cache=window.__TRK_GRADE_CACHE__||{};
    var inDetail=(d.detail||[]).some(function(r){ return r.date===date&&r.category==='5 Star Split'; });
    var g=cache[date];
    if(!inDetail && (g===undefined||g==='LOADING')) loadingMsg='Loading\u2026';
    else if(!inDetail && g&&g.__error__) loadingMsg=g.__error__||'No picks for this date.';
    else if(!inDetail && g && !g.all_final) loadingMsg='This slate is not final yet. The 5 Star Split Record fills in once every game on '+date+' goes Final.';
    else pool=_fssRowsForDate(date);
  } else {
    _edgeAllDates().forEach(function(dt){ var t=_fssRowsForDate(dt); for(var i=0;i<t.length;i++){ var rr=t[i]; if(!rr.date){ var cc={}; for(var kk in rr) cc[kk]=rr[kk]; cc.date=dt; rr=cc; } pool.push(rr); } });
  }
  var ov={w:0,l:0,net:0,counted:0};
  pool.forEach(function(r){
    var win=r.result==='WIN', od=_effOdds(r);
    var pl=_amProfit(od,stake,win); if(pl===null) return;
    if(win) ov.w++; else ov.l++; ov.net+=pl; ov.counted++;
  });
  var roiClr=ov.net>=0?'#4ade80':'#f87171';
  var roiStr=ov.counted?(((ov.net/(ov.counted*stake))*100).toFixed(1)+'%'):'&#x2014;';
  var netStr='$'+(ov.net>=0?'+':'')+ov.net.toFixed(2);
  var body='<div style="padding:14px 16px">';
  body+='<div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px">';
  body+='<button onclick="_fssStatsAllTime()" style="background:'+(dateMode?'#1e293b':'#7c3aed')+';color:'+(dateMode?'#cbd5e1':'#fff')+';border:none;border-radius:7px;padding:6px 12px;font-size:.78rem;font-weight:700;cursor:pointer">All-time</button>';
  body+='<label style="font-size:.78rem;color:#94a3b8;display:inline-flex;align-items:center;gap:6px">Day <input type="date" value="'+date+'" max="'+today+'" onchange="_fssStatsSetDate(this.value)" style="background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:5px 8px;font-size:.78rem"></label>';
  if(dateMode) body+='<span style="font-weight:800;color:#c4b5fd;font-size:.85rem">'+_weekdayName(date)+'</span>';
  body+='</div>';
  if(dateMode && loadingMsg){
    body+='<div style="color:#64748b;padding:24px;text-align:center">'+_esc(loadingMsg)+'</div></div>';
    _fssStatsWrap(body); return;
  }
  body+='<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:14px">';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">Record</div><div style="font-weight:900;color:#e2e8f0;font-size:1.1rem">'+ov.w+'-'+ov.l+'</div></div>';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">ROI</div><div style="font-weight:900;color:'+roiClr+';font-size:1.1rem">'+roiStr+'</div></div>';
  body+='<div style="background:#0c1622;border-radius:8px;padding:11px;text-align:center"><div style="font-size:.63rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">Net @ $'+stake+'</div><div style="font-weight:900;color:'+roiClr+';font-size:1.05rem">'+netStr+'</div></div>';
  body+='</div>';
  var _plays=pool.slice().sort(_recPlaySort);
  if(_plays.length) body+=_recSecHdr('ALL PLAYS &#xB7; '+_plays.length)+_recPlaysRows(_plays,'EV','#a78bfa',function(r){ return r.ev!=null?((r.ev>0?'+':'')+((r.ev*100).toFixed(1))+'%'):'&#x2014;'; },function(c){ return c; },!dateMode);
  else body+='<div style="color:#64748b;padding:20px;text-align:center">No 5 Star Split plays graded'+(dateMode?' on this date.':' yet.<br><span style="font-size:.74rem">This fills in automatically as each day&#39;s 5 Star Split picks go Final.</span>')+'</div>';
  body+='</div>';
  _fssStatsWrap(body);
}
function _openFssStats(){
  var d=window.__TRACK__; if(!d){ alert('Open Track Record first.'); return; }
  if(window.__FSS_DATE__===undefined) window.__FSS_DATE__='';
  var ov2=document.getElementById('fss-stats-modal');
  if(!ov2){ ov2=document.createElement('div'); ov2.id='fss-stats-modal'; ov2.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.85);z-index:10001;display:flex;align-items:center;justify-content:center;padding:16px'; ov2.onclick=function(e){ if(e.target===ov2) ov2.style.display='none'; }; document.body.appendChild(ov2); }
  _fssStatsRender();
  ov2.style.display='flex';
}
function _trkRenderActive(){ var be=document.getElementById('track-body'); if(!be) return; var stake=_trkStake(); var t=window.__TRK_TAB__||'daily'; if(t==='daily') _trkRenderDailyTab(be,stake); else _trkRenderRangeTab(be,stake,t); }
function _trkFlatten(g){ var out=[]; if(!g||g==='LOADING'||g.__error__) return out; _TRK_KEYS.forEach(function(k){ (g[k]||[]).forEach(function(r){ out.push(r); }); }); return out; }
function _trkFlattenFull(g){ var out=[]; if(!g||g==='LOADING'||g.__error__) return out; _TRK_KEYS_FULL.forEach(function(k){ (g[k]||[]).forEach(function(r){ out.push(r); }); }); return out; }
function _trkRangePool(from,to){ var d=window.__TRACK__||{}; var pool=[]; var have={}; (d.detail||[]).forEach(function(r){ if(r.date>=from&&r.date<=to){ have[r.date]=true; if(_isOvfCat(r.category)||_isHrCat(r.category)) return; pool.push(r); } }); var cache=window.__TRK_GRADE_CACHE__||{}; var cur=from; while(cur<=to){ if(!have[cur]){ _trkFlattenFull(cache[cur]).forEach(function(r){ if(r.result==='WIN'||r.result==='LOSS'){ var c={}; for(var kk in r) c[kk]=r[kk]; c.date=cur; pool.push(c); } }); } cur=_isoShift(cur,1); } return pool; }
// Days in [from,to] (up to today) that are neither locked into the permanent ledger
// nor yet fetched into the grade cache. These must be graded on demand so Weekly/
// Monthly sum EVERY day (postponed / still-pending days included), not just locked
// days + today.
function _trkRangeMissing(from,to){ var d=window.__TRACK__||{}; var have={}; (d.detail||[]).forEach(function(r){ if(r.date) have[r.date]=true; }); var cache=window.__TRK_GRADE_CACHE__||{}; var today=window.__TRK_TODAY__||_trkTodayISO(); var miss=[], cur=from; while(cur<=to){ if(cur<=today && !have[cur] && cache[cur]===undefined) miss.push(cur); cur=_isoShift(cur,1); } return miss; }
// Grade a list of dates into the cache, concurrency-limited so a wide range can't
// fire dozens of simultaneous box-score lookups (avoids ESPN/MLB-API 429s).
async function _trkFetchDays(dates){ var cache=window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{}; var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||''; var adm=new URLSearchParams(location.search).get('admin')||''; dates.forEach(function(dt){ if(cache[dt]===undefined) cache[dt]='LOADING'; }); var i=0, LIMIT=4; async function _worker(){ while(i<dates.length){ var dt=dates[i++]; try{ var res=await fetch('/api/grade/'+dt+'?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'')); if(!res.ok){ var t=await res.text(); cache[dt]={__error__:(t||'No picks for this date')}; } else { cache[dt]=await res.json(); } }catch(e){ cache[dt]={__error__:String((e&&e.message)||e)}; } } } var ws=[]; for(var w=0;w<Math.min(LIMIT,dates.length);w++) ws.push(_worker()); await Promise.all(ws); }
function _trkAgg(pool,stake){ var cats={}, overall={w:0,l:0,net:0,counted:0,skipped:0}; pool.forEach(function(r){ if(r.result!=='WIN'&&r.result!=='LOSS') return; var meta=_trkSkipMeta(r); var k=(r.category||'?')+'|'+(r.side||'OVER'); var c=cats[k]=cats[k]||{w:0,l:0,net:0,counted:0,skipped:0}; var win=r.result==='WIN'; if(win){c.w++; if(!meta)overall.w++;} else {c.l++; if(!meta)overall.l++;} var pl=_amProfit(_effOdds(r),stake,win); if(pl===null){c.skipped++; if(!meta)overall.skipped++;} else {c.net+=pl;c.counted++; if(!meta){overall.net+=pl;overall.counted++;}} }); return {cats:cats,overall:overall}; }
async function _trkLoadDaily(date){ window.__TRK_DAILY_DATE__=date; window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{}; var cur=window.__TRK_GRADE_CACHE__[date]; if(cur&&cur!=='LOADING'){ _trkRenderActive(); return; } var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||''; var adm=new URLSearchParams(location.search).get('admin')||''; window.__TRK_GRADE_CACHE__[date]='LOADING'; _trkRenderActive(); try{ var res=await fetch('/api/grade/'+date+'?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'')); if(!res.ok){ var t=await res.text(); window.__TRK_GRADE_CACHE__[date]={__error__:(t||'No picks for this date')}; } else { window.__TRK_GRADE_CACHE__[date]=await res.json(); } }catch(e){ window.__TRK_GRADE_CACHE__[date]={__error__:String((e&&e.message)||e)}; } _trkRenderActive(); }
function _trkCustomSet(which,val){ window.__TRK_BET__=_trkStake(); var today=_trkTodayISO(); var from=window.__TRK_FROM__||_isoShift(today,-6), to=window.__TRK_TO__||today; if(which==='from'){ from=val; if(from>to) to=from; } else { to=val; if(to>today) to=today; if(to<from) from=to; } window.__TRK_FROM__=from; window.__TRK_TO__=to; _trkRenderActive(); }
function _trkMonthShift(n){ window.__TRK_BET__=_trkStake(); var m=window.__TRK_MONTH__||_trkTodayISO().slice(0,7); var y=parseInt(m.slice(0,4),10), mo=parseInt(m.slice(5,7),10)-1+n; while(mo<0){mo+=12;y--;} while(mo>11){mo-=12;y++;} var nm=y+'-'+((mo+1)<10?'0':'')+(mo+1); var cur=_trkTodayISO().slice(0,7); if(nm>cur) nm=cur; window.__TRK_MONTH__=nm; _trkRenderActive(); }
// Daily sub-view toggle: "By Category" (summary table) vs "Full List" (every pick + odds).
function _trkDViewBtn(id,label){ var active=(window.__TRK_DVIEW__||'cat')===id; return '<button onclick="_trkDView(&#39;'+id+'&#39;)" style="background:'+(active?'#0e7490':'#1e293b')+';color:'+(active?'#fff':'#cbd5e1')+';border:none;border-radius:7px;padding:6px 14px;font-size:.78rem;font-weight:700;cursor:pointer">'+label+'</button>'; }
function _trkDView(v){ window.__TRK_BET__=_trkStake(); window.__TRK_DVIEW__=v; _trkRenderActive(); }
// Shared category-summary table (ranked best->worst) — used by the Daily "By Category"
// view AND the Weekly/Monthly range tabs so all three render identically.
function _trkCatTable(pool,stake,emptyMsg,clickable){
  var CAT_CFG=window.__TRK_CFG__||{};
  var ag=_trkAgg(pool,stake);
  var arr=Object.keys(ag.cats).map(function(k){ var c=ag.cats[k]; c.roi=c.counted?c.net/(c.counted*stake)*100:null; return [k,c]; });
  arr.sort(function(a,b){ var ra=a[1].counted?a[1].roi:-1e9, rb=b[1].counted?b[1].roi:-1e9; return rb-ra; });
  var head='<div style="display:flex;align-items:center;padding:7px 12px;background:#0c1829;border-bottom:1px solid #1e293b"><span style="flex:1;min-width:140px;font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase">Category</span><span style="width:64px;text-align:right;font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase">Record</span><span style="width:120px;text-align:center;font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase">Hit Rate</span><span style="width:80px;text-align:right;font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase">Net P/L</span><span style="width:72px;text-align:right;font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase">ROI</span></div>';
  if(clickable){ window.__CATV_MAP__=window.__CATV_MAP__||{}; if(window.__CATV_SEQ__==null) window.__CATV_SEQ__=0; }
  var body='';
  arr.forEach(function(x){ var k=x[0], c=x[1], n=c.w+c.l; if(!n) return; var cfg=CAT_CFG[k]||{lbl:k.split('|').join(' '),icon:'📊'}; var clr=_trkRC(c.w,n), pct=Math.round(c.w/n*100); var hasRoi=c.counted>0, netClr=c.net>=0?'#4ade80':'#f87171'; var _ca='',_cur='',_hint=''; if(clickable){ var _tok='cv'+(window.__CATV_SEQ__++); window.__CATV_MAP__[_tok]={key:k,pool:pool,stake:stake}; _ca=' onclick="_catVerdictPopup(&#39;'+_tok+'&#39;)" title="Tap for green / amber / red breakdown"'; _cur='cursor:pointer;'; _hint=' <span style="color:#475569;font-size:.72rem">\u203a</span>'; } body+='<div'+_ca+' style="display:flex;align-items:center;padding:9px 12px;border-bottom:1px solid #131c2e;'+_cur+'"><span style="flex:1;min-width:140px;color:#e2e8f0;font-weight:600;font-size:.85rem">'+(cfg.icon||'')+' '+cfg.lbl+_hint+'</span><span style="width:64px;text-align:right;font-family:monospace;font-weight:800;color:'+clr+'">'+c.w+'/'+n+'</span><span style="width:120px;display:inline-flex;align-items:center;gap:5px">'+_trkBar(pct,clr)+'<span style="font-size:.72rem;font-family:monospace;font-weight:700;color:'+clr+'">'+pct+'%</span></span><span style="width:80px;text-align:right;font-family:monospace;font-weight:800;color:'+(hasRoi?netClr:'#475569')+'">'+(hasRoi?((c.net>=0?'+$':'\u2212$')+Math.abs(c.net).toFixed(0)):'\u2014')+'</span><span style="width:72px;text-align:right;font-family:monospace;font-weight:700;color:'+(hasRoi?(c.roi>=0?'#4ade80':'#f87171'):'#475569')+'">'+(hasRoi?((c.roi>=0?'+':'\u2212')+Math.abs(c.roi).toFixed(1)+'%'):'\u2014')+'</span></div>'; });
  if(!body) body='<div style="color:#64748b;padding:16px;font-size:.83rem">'+(emptyMsg||'No graded picks in this range yet \u2014 fills in as slates go Final.')+'</div>';
  return {html:'<div style="border:1px solid #1e293b;border-radius:12px;overflow:hidden">'+head+body+'</div>', overall:ag.overall};
}
// ===== Category verdict popup — tap a category row (Weekly/Monthly, Track Record
// or Overflow) to see THAT market's green / amber / red record + ROI over the
// visible range, so you can tell when a "green" market is actually losing. =====
function _catVerdictPopup(tok){
  var R=(window.__CATV_MAP__||{})[tok]; if(!R) return;
  var key=R.key; if(key==null) return;
  var pool=R.pool||[], stake=R.stake||20;
  var CAT_CFG=window.__TRK_CFG__||{};
  var cfg=CAT_CFG[key]||{lbl:key.split('|').join(' '),icon:''};
  function B(){ return {w:0,l:0,net:0,counted:0}; }
  function add(b,win,od){ if(win)b.w++; else b.l++; var pl=_amProfit(od,stake,win); if(pl!==null){ b.net+=pl; b.counted++; } }
  function tot(b){ return b.w+b.l; }
  function pct(b){ var n=tot(b); return n?(b.w/n*100):0; }
  function roi(b){ return b.counted?(b.net/(b.counted*stake)*100):0; }
  var G=B(),A=B(),D=B(),All=B(),unrated=0;
  pool.forEach(function(r){
    if(((r.category||'?')+'|'+(r.side||'OVER'))!==key) return;
    if(r.result!=='WIN'&&r.result!=='LOSS') return;
    var win=r.result==='WIN', od=_effOdds(r);
    add(All,win,od);
    var v=_pickVerdict(r);
    if(v==='g') add(G,win,od); else if(v==='a') add(A,win,od); else if(v==='r') add(D,win,od); else unrated++;
  });
  function statRow(label,desc,b,clr){
    var n=tot(b); var rv=roi(b); var rc=rv>=0?'#4ade80':'#f87171';
    var recCol=n?('<span style="font-family:monospace;color:#e2e8f0;font-weight:800">'+b.w+'\u2212'+b.l+'</span> <span style="font-family:monospace;color:#fff;font-weight:800">'+pct(b).toFixed(0)+'%</span>'):'<span style="color:#475569;font-family:monospace">\u2014</span>';
    var roiCol=b.counted?('<span style="font-family:monospace;font-weight:800;color:'+rc+'">'+(rv>=0?'+':'\u2212')+Math.abs(rv).toFixed(0)+'%</span>'):'<span style="color:#475569;font-family:monospace">\u2014</span>';
    return '<div style="display:flex;align-items:center;gap:10px;padding:10px 2px;border-bottom:1px solid #16233a">'
      +'<span style="display:inline-block;width:11px;height:11px;border-radius:50%;background:'+clr+';flex:none"></span>'
      +'<span style="flex:1"><div style="color:#e2e8f0;font-size:.85rem;font-weight:800">'+label+'</div><div style="color:#64748b;font-size:.7rem">'+desc+'</div></span>'
      +'<span style="text-align:right;min-width:84px">'+recCol+'</span>'
      +'<span style="text-align:right;min-width:60px">'+roiCol+'</span></div>';
  }
  var gp=pct(G), rp=pct(D), gn=tot(G), rn=tot(D);
  var noteClr,noteTxt;
  if(!tot(G)&&!tot(A)&&!tot(D)){ noteClr='#94a3b8'; noteTxt='No green / amber / red picks in this range yet. The verdict needs the series-position stamp (G1/G2/G3) plus the day-of-week signal \u2014 it fills in as new slates are graded.'+(unrated?(' '+unrated+' graded pick'+(unrated>1?'s':'')+' here had no signal stamp.'):''); }
  else { noteClr='#94a3b8'; noteTxt='Experimental day-of-week + series lean, shown for tracking only \u2014 not a betting recommendation while we gather results.'; }
  var ov=document.getElementById('catv-modal');
  if(!ov){ ov=document.createElement('div'); ov.id='catv-modal'; ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:10050;display:flex;align-items:center;justify-content:center;padding:16px'; ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; }; document.body.appendChild(ov); }
  var allN=tot(All), allRoi=roi(All), allClr=allRoi>=0?'#4ade80':'#f87171';
  var hdrSub=(cfg.lbl||key)+' \u00b7 '+allN+' graded \u00b7 '+(allN?(All.w/allN*100).toFixed(0):'0')+'% \u00b7 ROI <span style="color:'+allClr+';font-weight:800">'+(allRoi>=0?'+':'\u2212')+Math.abs(allRoi).toFixed(0)+'%</span>';
  ov.innerHTML='<div style="background:#0f172a;border:1px solid #1e293b;border-radius:16px;max-width:460px;width:100%;max-height:88vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">'
      +'<div><div style="font-weight:800;font-size:1.05rem;color:#fff">'+(cfg.icon||'')+' Day + series lean</div><div style="color:#94a3b8;font-size:.76rem;margin-top:3px">'+hdrSub+'</div></div>'
      +'<button onclick="document.getElementById(&#39;catv-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">\u2715</button>'
    +'</div>'
    +'<div style="padding:14px 18px">'
      +'<div style="display:flex;align-items:center;gap:10px;padding:2px 2px 6px"><span style="width:11px;flex:none"></span><span style="flex:1"></span><span style="color:#64748b;font-size:.6rem;font-weight:800;letter-spacing:.05em;min-width:84px;text-align:right">W\u2212L / WIN%</span><span style="color:#64748b;font-size:.6rem;font-weight:800;letter-spacing:.05em;min-width:60px;text-align:right">ROI</span></div>'
      +statRow('Green','Day + series both leaned this side',G,'#22c55e')
      +statRow('Amber','Day + series split',A,'#f59e0b')
      +statRow('Red','Day + series leaned the other way',D,'#ef4444')
      +'<div style="margin-top:12px;font-size:.78rem;line-height:1.5;color:'+noteClr+'">'+noteTxt+'</div>'
      +'<div style="margin-top:10px;border-top:1px solid #1e293b;padding-top:8px;color:#64748b;font-size:.68rem;line-height:1.5">Shown for tracking only while we gather results to rebuild this from real outcomes.'+(unrated?(' '+unrated+' pick'+(unrated>1?'s':'')+' had no signal stamp and are not shown above.'):'')+'</div>'
    +'</div>'
  +'</div>';
  ov.style.display='flex';
}
// ===== Printable PDF report — opens a clean, self-contained window with the
// green/amber/red scorecard + per-category table and fires the print dialog so
// the user can &quot;Save as PDF&quot;. No libraries; pure client-side print-to-PDF. =====
function _openPrintReport(inner,title){
  var w=window.open('','_blank');
  if(!w){ alert('Please allow pop-ups for this site, then tap PDF Report again to save the file.'); return; }
  inner=inner.split('<details').join('<details open');
  var css='*{-webkit-print-color-adjust:exact !important;print-color-adjust:exact !important;box-sizing:border-box}'
    +'body{margin:0;background:#020617;color:#e2e8f0;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:22px;font-size:14px}'
    +'@media print{body{padding:0 4px}.noprint{display:none !important}}'
    +'details{margin:0}summary{list-style:none}button{font-family:inherit}';
  var doc='<!DOCTYPE html><html><head><meta charset="utf-8"><title>'+title+'</title><style>'+css+'</style></head><body>'
    +'<div class="noprint" style="display:flex;gap:10px;margin-bottom:16px;max-width:920px;margin-left:auto;margin-right:auto"><button onclick="window.print()" style="background:#dc2626;color:#fff;border:none;border-radius:8px;padding:10px 18px;font-size:.92rem;font-weight:800;cursor:pointer">\u2b07 Save as PDF</button><button onclick="window.close()" style="background:#334155;color:#fff;border:none;border-radius:8px;padding:10px 18px;font-size:.92rem;font-weight:800;cursor:pointer">Close</button></div>'
    +inner+'</body></html>';
  w.document.open(); w.document.write(doc); w.document.close(); w.focus();
  setTimeout(function(){ try{ w.print(); }catch(e){} }, 600);
}
function _trkPrintReport(){
  var d=window.__TRACK__; if(!d){ alert('Track Record is still loading \u2014 try again in a moment.'); return; }
  function overallTile(o,stake){ var on=o.w+o.l, orisk=o.counted*stake, oroi=orisk?o.net/orisk*100:0, oclr=o.net>=0?'#4ade80':'#f87171', owclr=_trkRC(o.w,on); return '<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#0c1829;border:1px solid #1e293b;border-radius:12px;padding:13px 18px;margin-bottom:14px"><div style="font-weight:900;font-size:1.02rem"><span style="color:'+owclr+'">'+o.w+'/'+on+'</span> <span style="color:#94a3b8;font-size:.84rem">('+(on?(o.w/on*100).toFixed(1):'0.0')+'%)</span></div><div style="font-size:.9rem">Net <span style="color:'+oclr+';font-weight:900">'+(o.net>=0?'+$':'\u2212$')+Math.abs(o.net).toFixed(0)+'</span> <span style="color:#64748b">\u00b7 ROI '+(oroi>=0?'+':'\u2212')+Math.abs(oroi).toFixed(1)+'% on $'+orisk.toFixed(0)+'</span></div></div>'; }
  function secHead(t,sub,clr){ return '<div style="font-size:1.08rem;font-weight:900;color:'+clr+';margin:22px 0 10px;border-bottom:1px solid #1e293b;padding-bottom:6px">'+t+' <span style="color:#64748b;font-weight:700;font-size:.74rem">'+sub+'</span></div>'; }
  var tStake=_trkStake();
  var tPool=(d.detail||[]).filter(function(r){ return !_isOvfCat(r.category)&&!_isHrCat(r.category); });
  var tCt=_trkCatTable(tPool,tStake,'No graded picks yet.');
  var now=new Date();
  var head='<div style="border-bottom:2px solid #1e293b;padding-bottom:14px;margin-bottom:6px"><div style="font-size:1.5rem;font-weight:900;color:#fff">Money Picks Arena <span style="color:#fbbf24">Performance Report</span></div><div style="color:#94a3b8;font-size:.82rem;margin-top:4px">Generated '+now.toLocaleString()+' \u00b7 MLB \u00b7 flat $'+tStake+' on every pick</div></div>';
  var trackSec=secHead('Track Record','top plays per category','#6ee7b7')+overallTile(tCt.overall,tStake)+_matrixScorecard(d)+'<div style="font-weight:800;color:#e2e8f0;font-size:.95rem;margin:4px 0 10px">Category Performance \u2014 ranked by ROI</div>'+tCt.html;
  var ovfSec='';
  var oPool=(d.detail||[]).filter(function(r){ return _isOvfCat(r.category)&&!_isHrCat(r.category); });
  if(oPool.length){ var oStake=_ovfStake(); var oCt=_trkCatTable(oPool,oStake,'No graded overflow picks yet.'); ovfSec=secHead('Overflow Tracker','ranks 11\u201330','#fcd34d')+overallTile(oCt.overall,oStake)+_ovfMatrixScorecard(d)+'<div style="font-weight:800;color:#fde68a;font-size:.95rem;margin:4px 0 10px">Overflow Category Performance \u2014 ranked by ROI</div>'+oCt.html; }
  var foot='<div style="margin-top:20px;color:#64748b;font-size:.72rem;line-height:1.55">Green / amber / red verdict needs the series-position stamp (G1/G2/G3); days logged before it was stored show the day-of-week signal only and fill in going forward. Net P/L and ROI use the flat bet above on each graded pick at the recorded odds. \u201cCategory\u201d totals exclude undecided / unpriced picks.</div>';
  var inner='<div style="max-width:920px;margin:0 auto">'+head+trackSec+ovfSec+foot+'</div>';
  _openPrintReport(inner,'MPA Performance Report');
}
function _ovfPrintReport(){
  var d=window.__TRACK__; if(!d){ alert('Overflow Tracker is still loading \u2014 try again in a moment.'); return; }
  function overallTile(o,stake){ var on=o.w+o.l, orisk=o.counted*stake, oroi=orisk?o.net/orisk*100:0, oclr=o.net>=0?'#4ade80':'#f87171', owclr=_trkRC(o.w,on); return '<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#0c1829;border:1px solid #1e293b;border-radius:12px;padding:13px 18px;margin-bottom:14px"><div style="font-weight:900;font-size:1.02rem"><span style="color:'+owclr+'">'+o.w+'/'+on+'</span> <span style="color:#94a3b8;font-size:.84rem">('+(on?(o.w/on*100).toFixed(1):'0.0')+'%)</span></div><div style="font-size:.9rem">Net <span style="color:'+oclr+';font-weight:900">'+(o.net>=0?'+$':'\u2212$')+Math.abs(o.net).toFixed(0)+'</span> <span style="color:#64748b">\u00b7 ROI '+(oroi>=0?'+':'\u2212')+Math.abs(oroi).toFixed(1)+'% on $'+orisk.toFixed(0)+'</span></div></div>'; }
  function secHead(t,sub,clr){ return '<div style="font-size:1.08rem;font-weight:900;color:'+clr+';margin:22px 0 10px;border-bottom:1px solid #1e293b;padding-bottom:6px">'+t+' <span style="color:#64748b;font-weight:700;font-size:.74rem">'+sub+'</span></div>'; }
  var oStake=_ovfStake();
  var oPool=(d.detail||[]).filter(function(r){ return _isOvfCat(r.category)&&!_isHrCat(r.category); });
  var oCt=_trkCatTable(oPool,oStake,'No graded overflow picks yet.');
  var now=new Date();
  var head='<div style="border-bottom:2px solid #1e293b;padding-bottom:14px;margin-bottom:6px"><div style="font-size:1.5rem;font-weight:900;color:#fff">Money Picks Arena <span style="color:#fbbf24">Overflow Report</span></div><div style="color:#94a3b8;font-size:.82rem;margin-top:4px">Generated '+now.toLocaleString()+' \u00b7 MLB \u00b7 ranks 11\u201330 \u00b7 flat $'+oStake+' on every pick</div></div>';
  var ovfSec=secHead('Overflow Tracker','ranks 11\u201330','#fcd34d')+overallTile(oCt.overall,oStake)+_ovfMatrixScorecard(d)+'<div style="font-weight:800;color:#fde68a;font-size:.95rem;margin:4px 0 10px">Overflow Category Performance \u2014 ranked by ROI</div>'+oCt.html;
  var foot='<div style="margin-top:20px;color:#64748b;font-size:.72rem;line-height:1.55">Net P/L and ROI use the flat bet above on each graded overflow pick at the recorded odds. \u201cCategory\u201d totals exclude undecided / unpriced picks.</div>';
  var inner='<div style="max-width:920px;margin:0 auto">'+head+ovfSec+foot+'</div>';
  _openPrintReport(inner,'MPA Overflow Report');
}
function _pickVerdict(r){
  if(_trkSkipMeta(r)||_isHrCat(r.category)) return '';
  var info=_mtxCatInfo(_ovfBaseCat(r.category)); if(!info) return '';
  var isPit=info[0], ci=info[1];
  var side=(r.side==='UNDER')?'U':'O';
  var wd=_mtxWeekday(r.date); if(wd==null) return '';
  var dLean=_mtxDayLean(wd,isPit,ci); if(!dLean) return '';
  var pos=r.series_pos; if(pos!==1&&pos!==2&&pos!==3) return '';
  var sLean=_mtxSeriesLean(pos,isPit,ci); if(!sLean) return '';
  if(sLean!==dLean) return 'a';
  if(sLean===side) return 'g';
  return 'r';
}
var _ODDS_BUCKETS=[
 {lo:-100000,hi:-500,lab:'\u2264 \u2212500'},
 {lo:-500,hi:-450,lab:'\u2212500 to \u2212450'},
 {lo:-450,hi:-400,lab:'\u2212450 to \u2212400'},
 {lo:-400,hi:-350,lab:'\u2212400 to \u2212350'},
 {lo:-350,hi:-300,lab:'\u2212350 to \u2212300'},
 {lo:-300,hi:-250,lab:'\u2212300 to \u2212250'},
 {lo:-250,hi:-200,lab:'\u2212250 to \u2212200'},
 {lo:-200,hi:-150,lab:'\u2212200 to \u2212150'},
 {lo:-150,hi:-100,lab:'\u2212150 to \u2212100'},
 {lo:100,hi:150,lab:'+100 to +150'},
 {lo:150,hi:200,lab:'+150 to +200'},
 {lo:200,hi:250,lab:'+200 to +250'},
 {lo:250,hi:300,lab:'+250 to +300'},
 {lo:300,hi:100000,lab:'\u2265 +300'}
];
function _oddsBucketIdx(o){
  if(o==null||!isFinite(o)) return -1;
  for(var i=0;i<_ODDS_BUCKETS.length;i++){ var b=_ODDS_BUCKETS[i]; if(o>=b.lo&&o<b.hi) return i; }
  if(o===-100){ for(var j=0;j<_ODDS_BUCKETS.length;j++){ if(_ODDS_BUCKETS[j].hi===-100) return j; } }
  return -1;
}
function _oddsBE(o){ return o<0?(Math.abs(o)/(Math.abs(o)+100)*100):(100/(o+100)*100); }
function _oddsTable(arr,stake,heading,clr,sub,group,interactive,token){
  var tw=0,tl=0,tnet=0,tc=0,i;
  for(i=0;i<arr.length;i++){ tw+=arr[i].w; tl+=arr[i].l; tnet+=arr[i].net; tc+=arr[i].counted; }
  var tn=tw+tl; if(!tn) return '';
  var trisk=tc*stake, troi=trisk?tnet/trisk*100:0;
  var hdr='<div style="display:flex;align-items:center;gap:10px;margin:18px 0 6px">'
    +'<span style="display:inline-block;width:11px;height:11px;border-radius:50%;background:'+clr+';flex:none"></span>'
    +'<span style="font-weight:900;color:#e2e8f0;font-size:.92rem">'+heading+'</span>'
    +'<span style="color:#64748b;font-size:.7rem;flex:1">'+sub+'</span>'
    +'<span style="font-family:monospace;font-size:.78rem;color:'+_trkRC(tw,tn)+';font-weight:800">'+tw+'/'+tn+' ('+(tw/tn*100).toFixed(0)+'%)</span>'
    +'<span style="font-family:monospace;font-size:.78rem;font-weight:800;color:'+(tnet>=0?'#4ade80':'#f87171')+';min-width:104px;text-align:right">'+(tnet>=0?'+$':'\u2212$')+Math.abs(tnet).toFixed(0)+' \u00b7 '+(troi>=0?'+':'\u2212')+Math.abs(troi).toFixed(0)+'%</span>'
    +'</div>';
  var chev=interactive?'<span style="width:16px"></span>':'';
  var colhdr='<div style="display:flex;gap:8px;padding:2px 4px 5px;font-size:.58rem;color:#64748b;font-weight:800;letter-spacing:.04em"><span style="flex:1">ODDS RANGE</span><span style="width:56px;text-align:right">W-L</span><span style="width:48px;text-align:right">HIT%</span><span style="width:48px;text-align:right">NEED</span><span style="width:72px;text-align:right">NET</span><span style="width:48px;text-align:right">ROI</span>'+chev+'</div>';
  var rows='';
  for(i=0;i<arr.length;i++){ var b=arr[i]; var n=b.w+b.l; if(!n) continue;
    var hp=b.w/n*100; var avgbe=b.beN?b.beSum/b.beN:0; var hc=hp>=avgbe?'#4ade80':'#f87171';
    var roi=b.counted?(b.net/(b.counted*stake)*100):0;
    var edge=hp-avgbe; var vc=edge>=2?'#22c55e':(edge<=-2?'#ef4444':'#f59e0b');
    var rstyle='display:flex;gap:8px;align-items:center;padding:5px 4px 5px 9px;border-bottom:1px solid #111c2e;border-left:3px solid '+vc+';font-size:.77rem'+(interactive?';cursor:pointer':'');
    var rattr=interactive?(' onclick="_oddsRowDetail(&#39;'+(token||'')+'&#39;,&#39;'+group+'&#39;,'+i+')" onmouseover="this.style.background=&#39;#0f1d33&#39;" onmouseout="this.style.background=&#39;&#39;" title="Tap to see every pick behind this row"'):'';
    var arrow=interactive?'<span style="width:16px;text-align:right;color:#64748b;font-weight:900">\u203a</span>':'';
    rows+='<div'+rattr+' style="'+rstyle+'">'
      +'<span style="flex:1;color:#cbd5e1;font-family:monospace">'+_ODDS_BUCKETS[i].lab+'</span>'
      +'<span style="width:56px;text-align:right;font-family:monospace;color:#e2e8f0">'+b.w+'-'+b.l+'</span>'
      +'<span style="width:48px;text-align:right;font-family:monospace;font-weight:800;color:'+hc+'">'+hp.toFixed(0)+'%</span>'
      +'<span style="width:48px;text-align:right;font-family:monospace;color:#94a3b8">'+avgbe.toFixed(0)+'%</span>'
      +'<span style="width:72px;text-align:right;font-family:monospace;font-weight:700;color:'+(b.net>=0?'#4ade80':'#f87171')+'">'+(b.net>=0?'+$':'\u2212$')+Math.abs(b.net).toFixed(0)+'</span>'
      +'<span style="width:48px;text-align:right;font-family:monospace;color:'+(roi>=0?'#4ade80':'#f87171')+'">'+(roi>=0?'+':'\u2212')+Math.abs(roi).toFixed(0)+'%</span>'
      +arrow
      +'</div>';
  }
  return '<div style="margin-bottom:6px">'+hdr+colhdr+rows+'</div>';
}
function _oddsReport(pool,stake,interactive){
  if(interactive===undefined) interactive=true;
  function mk(){ var a=[]; for(var i=0;i<_ODDS_BUCKETS.length;i++) a.push({w:0,l:0,net:0,counted:0,beSum:0,beN:0}); return a; }
  var G={all:mk(),g:mk(),a:mk(),r:mk()}; var any=false;
  (pool||[]).forEach(function(r){
    if(_trkSkipMeta(r)) return;
    if(r.result!=='WIN'&&r.result!=='LOSS') return;
    var o=_effOdds(r); if(o==null||!isFinite(o)) return;
    var bi=_oddsBucketIdx(o); if(bi<0) return;
    var win=r.result==='WIN'; var pl=_amProfit(o,stake,win); var be=_oddsBE(o);
    function add(b){ if(win)b.w++; else b.l++; if(pl!==null){ b.net+=pl; b.counted++; b.beSum+=be; b.beN++; } }
    add(G.all[bi]); any=true;
    var v=_pickVerdict(r); if(v==='g')add(G.g[bi]); else if(v==='a')add(G.a[bi]); else if(v==='r')add(G.r[bi]);
  });
  var tok='';
  if(interactive){ window.__ODDS_DRILL_SEQ__=(window.__ODDS_DRILL_SEQ__||0)+1; tok='d'+window.__ODDS_DRILL_SEQ__; var ctx={pool:pool,stake:stake,label:(window.__ODDS_CTX__&&window.__ODDS_CTX__.label)||''}; window.__ODDS_DRILL__=ctx; window.__ODDS_DRILLS__=window.__ODDS_DRILLS__||{}; window.__ODDS_DRILLS__[tok]=ctx; var _dk=Object.keys(window.__ODDS_DRILLS__); if(_dk.length>16) delete window.__ODDS_DRILLS__[_dk[0]]; }
  if(!any) return '<div style="background:#0a1424;border:1px solid #1e293b;border-radius:12px;padding:16px 18px;color:#94a3b8;font-size:.82rem">No graded picks with posted odds in this range yet \u2014 fills in as games go Final.</div>';
  var legend='<div style="color:#64748b;font-size:.72rem;line-height:1.5;margin-bottom:2px">HIT% = how often that price won \u00b7 NEED = break-even win% the price requires (the vig). <span style="color:#4ade80;font-weight:700">Green HIT%</span> cleared the line (made money); <span style="color:#f87171;font-weight:700">red</span> fell short. The left bar flags value at a glance \u2014 <span style="color:#22c55e;font-weight:700">green</span> beat the price, <span style="color:#f59e0b;font-weight:700">amber</span> roughly break-even, <span style="color:#ef4444;font-weight:700">red</span> no edge.'+(interactive?' <span style="color:#38bdf8;font-weight:700">Tap any row</span> to see the exact picks behind it.':'')+'</div>';
  return '<div style="background:#0a1424;border:1px solid #1e293b;border-radius:12px;padding:14px 18px;margin-bottom:14px">'
    +'<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px"><span style="font-size:1rem">🎯</span><span style="font-weight:800;color:#e2e8f0;font-size:.95rem">Hit Rate by Odds Range</span><span style="color:#64748b;font-size:.7rem">which prices actually cash</span></div>'
    +legend
    +_oddsTable(G.all,stake,'All graded picks','#38bdf8','every priced pick in range','all',interactive,tok)
    +_oddsTable(G.g,stake,'Green lean','#22c55e','day + series both leaned this side','g',interactive,tok)
    +_oddsTable(G.a,stake,'Amber lean','#f59e0b','day + series split','a',interactive,tok)
    +_oddsTable(G.r,stake,'Red lean','#ef4444','day + series leaned the other way','r',interactive,tok)
    +'<div style="margin-top:14px;color:#64748b;font-size:.7rem;line-height:1.5">Green / amber / red need the series-position stamp (G1/G2/G3) and a day-of-week lean, so they cover fewer picks than \u201cAll graded.\u201d Picks logged before those were stored appear under All only.</div>'
    +'</div>';
}
function _oddsPrintCurrent(){
  var c=window.__ODDS_CTX__; if(!c){ alert('Open an Odds report first.'); return; }
  var now=new Date();
  var head='<div style="border-bottom:2px solid #1e293b;padding-bottom:14px;margin-bottom:8px"><div style="font-size:1.5rem;font-weight:900;color:#fff">Money Picks Arena <span style="color:#fbbf24">Odds Hit-Rate Report</span></div><div style="color:#94a3b8;font-size:.82rem;margin-top:4px">'+c.label+' \u00b7 Generated '+now.toLocaleString()+' \u00b7 MLB \u00b7 flat $'+c.stake+' per pick</div></div>';
  var inner='<div style="max-width:840px;margin:0 auto">'+head+_oddsReport(c.pool,c.stake,false)+'</div>';
  _openPrintReport(inner,'MPA Odds Hit-Rate Report');
}
function _oddsCloseModal(){ var m=document.getElementById('_oddsModal'); if(m&&m.parentNode) m.parentNode.removeChild(m); }
function _oddsCatToggle(id){ var e=document.getElementById(id); if(!e) return; var open=e.style.display!=='none'; e.style.display=open?'none':''; var ar=document.getElementById(id+'_ar'); if(ar) ar.innerHTML=open?'\u25b8':'\u25be'; }
function _oddsDaySel(di){ for(var i=0;i<7;i++){ var e=document.getElementById('_oday_'+i); if(e) e.style.display=(i===di)?'':'none'; var t=document.getElementById('_odtile_'+i); if(t) t.style.outline=(i===di)?'2px solid #38bdf8':'none'; } }
function _oddsDayPlays(di,ci){ var src=document.getElementById('_odplays_'+di+'_'+ci); if(!src) return; var b=document.getElementById('_odBigBody'); var h=document.getElementById('_odBigTitle'); if(h) h.textContent=src.getAttribute('data-title')||''; if(b) b.innerHTML=src.innerHTML; var ov=document.getElementById('_odBig'); if(ov) ov.style.display='flex'; }
function _odBigClose(){ var ov=document.getElementById('_odBig'); if(ov) ov.style.display='none'; }
function _oddsRowDetail(token,group,bi){
  var c=(token&&window.__ODDS_DRILLS__&&window.__ODDS_DRILLS__[token])||window.__ODDS_DRILL__||window.__ODDS_CTX__; if(!c||!c.pool) return;
  var stake=c.stake||20; var bk=_ODDS_BUCKETS[bi]; if(!bk) return;
  var CAT_CFG=window.__TRK_CFG__||{};
  var picks=[];
  (c.pool||[]).forEach(function(r){
    if(_trkSkipMeta(r)) return;
    if(r.result!=='WIN'&&r.result!=='LOSS') return;
    var o=_effOdds(r); if(o==null||!isFinite(o)) return;
    if(_oddsBucketIdx(o)!==bi) return;
    if(group!=='all'&&_pickVerdict(r)!==group) return;
    picks.push({r:r,o:o});
  });
  picks.sort(function(a,b){ var da=(a.r.date||''),db=(b.r.date||''); if(da!==db) return da<db?1:-1; return ((a.r.name||'')<(b.r.name||''))?-1:1; });
  var w=0,l=0,net=0,counted=0,beSum=0;
  picks.forEach(function(p){ var win=p.r.result==='WIN'; if(win)w++; else l++; var pl=_amProfit(p.o,stake,win); if(pl!==null){ net+=pl; counted++; } beSum+=_oddsBE(p.o); });
  var n=w+l, hit=n?w/n*100:0, need=n?beSum/n:0, roi=counted?net/(counted*stake)*100:0, edge=hit-need;
  var gmap={all:['All graded picks','#38bdf8'],g:['Green verdict','#22c55e'],a:['Amber verdict','#f59e0b'],r:['Red verdict','#ef4444']};
  var gm=gmap[group]||gmap.all;
  var edgeTxt=(edge>=0?('\u2705 Beating break-even by +'+edge.toFixed(0)+' pts'):('\u26a0\ufe0f Trailing break-even by \u2212'+Math.abs(edge).toFixed(0)+' pts'));
  var summary=n?('<div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;background:#0c1829;border:1px solid #1e293b;border-radius:10px;padding:10px 14px;margin-bottom:10px;font-size:.82rem">'
    +'<span style="font-weight:900;color:'+_trkRC(w,n)+'">'+w+'/'+n+' <span style="color:#94a3b8;font-weight:600;font-size:.74rem">('+hit.toFixed(0)+'% hit \u00b7 need '+need.toFixed(0)+'%)</span></span>'
    +'<span style="color:'+(edge>=0?'#4ade80':'#f87171')+';font-weight:800;font-size:.74rem">'+edgeTxt+'</span>'
    +'<span style="margin-left:auto;font-weight:900;color:'+(net>=0?'#4ade80':'#f87171')+'">'+(net>=0?'+$':'\u2212$')+Math.abs(net).toFixed(0)+' <span style="color:#64748b;font-weight:600;font-size:.74rem">\u00b7 ROI '+(roi>=0?'+':'\u2212')+Math.abs(roi).toFixed(0)+'% on $'+(counted*stake).toFixed(0)+'</span></span>'
    +'</div>'):'';
  // one play row (rendered inside an expanded category)
  function _plRow(p){ var r=p.r; var win=r.result==='WIN'; var pl=_amProfit(p.o,stake,win); var od=(p.o>0?'+':'')+p.o; var wd=_weekdayName(r.date);
    return '<div style="display:flex;gap:10px;align-items:center;padding:6px 4px 6px 14px;border-bottom:1px solid #15233b;font-size:.76rem">'
      +'<span style="width:104px;flex:none;color:#94a3b8;font-family:monospace;font-size:.68rem">'+(r.date||'')+(wd?(' \u00b7 '+wd.slice(0,3)):'')+'</span>'
      +'<span style="flex:1;min-width:100px"><span style="color:#fff;font-weight:700">'+(r.name||'\u2014')+'</span><div style="color:#94a3b8;font-size:.68rem">'+(r.pick||'')+(r.actual!=null&&r.actual!==''?(' \u00b7 got '+r.actual):'')+'</div></span>'
      +'<span style="width:52px;flex:none;text-align:right;font-family:monospace;color:#cbd5e1">'+od+'</span>'
      +'<span style="width:46px;flex:none;text-align:center;font-weight:800;font-size:.7rem;color:'+(win?'#4ade80':'#f87171')+'">'+(win?'WIN':'LOSS')+'</span>'
      +'<span style="width:58px;flex:none;text-align:right;font-family:monospace;font-weight:700;color:'+((pl||0)>=0?'#4ade80':'#f87171')+'">'+(pl==null?'\u2014':((pl>=0?'+$':'\u2212$')+Math.abs(pl).toFixed(0)))+'</span>'
      +'</div>';
  }
  // BY DAY OF WEEK (top) \u2014 line up against the matrix
  var _DOW=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
  var _byDay={}; picks.forEach(function(p){ var wd=_weekdayName(p.r.date)||''; (_byDay[wd]=_byDay[wd]||[]).push(p); });
  var dayTiles='',dayLists='',odStore='';
  _DOW.forEach(function(dn,di){ var arr=_byDay[dn]||[]; var dw=0,dl=0,dnet=0; var dm={},dord=[];
    arr.forEach(function(p){ var r=p.r; var key=(r.category||'')+'|'+(r.side||'OVER'); var g=dm[key]; if(!g){ g=dm[key]={w:0,l:0,net:0,picks:[],lbl:((CAT_CFG[key]&&CAT_CFG[key].lbl)||r.category||'\u2014'),sd:(r.side==='UNDER')?'U':'O'}; dord.push(key); } var win=r.result==='WIN'; if(win){g.w++;dw++;}else{g.l++;dl++;} var pl=_amProfit(p.o,stake,win); if(pl!==null){g.net+=pl;dnet+=pl;} g.picks.push(p); });
    var has=arr.length>0; var rec=has?(dw+'-'+dl):'\u2014'; var netTxt=has?((dnet>=0?'+$':'\u2212$')+Math.abs(dnet).toFixed(0)):'';
    dayTiles+='<div id="_odtile_'+di+'" '+(has?('onclick="_oddsDaySel('+di+')" style="cursor:pointer;'):('style="opacity:.4;'))+'flex:1 0 60px;background:#0c1829;border:1px solid #1e293b;border-radius:9px;padding:7px 4px;text-align:center">'
      +'<div style="font-size:.6rem;font-weight:900;letter-spacing:.05em;color:#93c5fd">'+dn.slice(0,3).toUpperCase()+'</div>'
      +'<div style="font-size:.84rem;font-weight:900;color:'+(has?(dw>=dl?'#4ade80':'#f87171'):'#475569')+'">'+rec+'</div>'
      +(netTxt?('<div style="font-size:.64rem;font-family:monospace;color:'+(dnet>=0?'#4ade80':'#f87171')+'">'+netTxt+'</div>'):'')
      +'</div>';
    var listRows='';
    dord.forEach(function(k,ci){ var g=dm[k];
      listRows+='<div onclick="_oddsDayPlays('+di+','+ci+')" onmouseover="this.style.background=&#39;#0f1d33&#39;" onmouseout="this.style.background=&#39;&#39;" style="display:flex;gap:10px;align-items:center;padding:7px 6px;border-bottom:1px solid #15233b;font-size:.78rem;cursor:pointer">'
        +'<span style="flex:1;min-width:120px;color:#e2e8f0;font-weight:700">'+g.lbl+' <span style="color:#64748b;font-weight:600;font-size:.68rem">'+g.sd+'</span></span>'
        +'<span style="width:50px;flex:none;text-align:right;font-family:monospace;color:#cbd5e1">'+g.w+'-'+g.l+'</span>'
        +'<span style="width:58px;flex:none;text-align:right;font-family:monospace;font-weight:700;color:'+(g.net>=0?'#4ade80':'#f87171')+'">'+(g.net>=0?'+$':'\u2212$')+Math.abs(g.net).toFixed(0)+'</span>'
        +'<span style="width:14px;flex:none;text-align:right;color:#64748b;font-weight:900">\u25b8</span>'
        +'</div>';
      var pls=''; g.picks.forEach(function(p){ pls+=_plRow(p); });
      odStore+='<div id="_odplays_'+di+'_'+ci+'" data-title="'+(g.lbl+' '+g.sd+' \u00b7 '+dn).replace(/"/g,'&quot;')+'" style="display:none">'+pls+'</div>';
    });
    dayLists+='<div id="_oday_'+di+'" style="display:none;margin-top:8px;background:#08111f;border:1px solid #15233b;border-radius:8px;padding:6px 8px">'
      +'<div style="font-size:.66rem;color:#93c5fd;font-weight:900;letter-spacing:.04em;margin:2px 2px 4px">'+dn.toUpperCase()+'</div>'
      +(listRows||'<div style="padding:8px 4px;color:#475569;font-size:.76rem">No plays this day.</div>')+'</div>';
  });
  var dayBreak=n?('<div style="background:#0c1829;border:1px solid #1e293b;border-radius:10px;padding:8px 12px 10px;margin-bottom:10px">'
    +'<div style="font-size:.62rem;color:#64748b;font-weight:800;letter-spacing:.04em;margin-bottom:6px">BY DAY OF WEEK \u00b7 tap a day, then a category</div>'
    +'<div style="display:flex;flex-wrap:wrap;gap:6px">'+dayTiles+'</div>'
    +dayLists+odStore+'</div>'):'';
  // BY CATEGORY \u2014 tap a row to expand its plays
  var _cm={},_cord=[];
  picks.forEach(function(p){ var r=p.r; var key=(r.category||'')+'|'+(r.side||'OVER'); var g=_cm[key];
    if(!g){ g=_cm[key]={w:0,l:0,net:0,counted:0,beSum:0,picks:[],lbl:((CAT_CFG[key]&&CAT_CFG[key].lbl)||r.category||'\u2014'),sd:(r.side==='UNDER')?'U':'O'}; _cord.push(key); }
    var cw=r.result==='WIN'; if(cw)g.w++; else g.l++; var cpl=_amProfit(p.o,stake,cw); if(cpl!==null){ g.net+=cpl; g.counted++; } g.beSum+=_oddsBE(p.o); g.picks.push(p);
  });
  var _crows=_cord.map(function(k){ var g=_cm[k]; var gn=g.w+g.l; g.hit=gn?g.w/gn*100:0; g.need=gn?g.beSum/gn:0; g.roi=g.counted?g.net/(g.counted*stake)*100:0; return g; });
  _crows.sort(function(a,b){ return b.roi-a.roi; });
  var catBreak='';
  if(_crows.length){
    var crows='';
    _crows.forEach(function(g,gi){ var hc=g.hit>=g.need?'#4ade80':'#f87171'; var pid='_ocat_'+gi; var plays='';
      g.picks.forEach(function(p){ plays+=_plRow(p); });
      crows+='<div onclick="_oddsCatToggle(&#39;'+pid+'&#39;)" onmouseover="this.style.background=&#39;#0f1d33&#39;" onmouseout="this.style.background=&#39;&#39;" style="display:flex;gap:10px;align-items:center;padding:7px 4px;border-bottom:1px solid #15233b;font-size:.78rem;cursor:pointer">'
        +'<span style="flex:1;min-width:120px;color:#e2e8f0;font-weight:700">'+g.lbl+' <span style="color:#64748b;font-weight:600;font-size:.68rem">'+g.sd+'</span></span>'
        +'<span style="width:50px;flex:none;text-align:right;font-family:monospace;color:#cbd5e1">'+g.w+'-'+g.l+'</span>'
        +'<span style="width:84px;flex:none;text-align:right;font-family:monospace;color:'+hc+'">'+g.hit.toFixed(0)+'% <span style="color:#64748b">/ '+g.need.toFixed(0)+'%</span></span>'
        +'<span style="width:58px;flex:none;text-align:right;font-family:monospace;font-weight:700;color:'+(g.net>=0?'#4ade80':'#f87171')+'">'+(g.net>=0?'+$':'\u2212$')+Math.abs(g.net).toFixed(0)+'</span>'
        +'<span style="width:50px;flex:none;text-align:right;font-family:monospace;font-weight:700;color:'+(g.roi>=0?'#4ade80':'#f87171')+'">'+(g.roi>=0?'+':'\u2212')+Math.abs(g.roi).toFixed(0)+'%</span>'
        +'<span id="'+pid+'_ar" style="width:14px;flex:none;text-align:right;color:#64748b;font-weight:900">\u25b8</span>'
        +'</div>'
        +'<div id="'+pid+'" style="display:none;background:#08111f">'+plays+'</div>';
    });
    catBreak='<div style="background:#0c1829;border:1px solid #1e293b;border-radius:10px;padding:8px 12px 4px;margin-bottom:10px">'
      +'<div style="font-size:.62rem;color:#64748b;font-weight:800;letter-spacing:.04em;margin-bottom:4px">BY CATEGORY \u00b7 best ROI first \u2014 tap a row to see its plays</div>'
      +'<div style="display:flex;gap:10px;padding:0 4px 4px;font-size:.54rem;color:#64748b;font-weight:800;letter-spacing:.04em"><span style="flex:1;min-width:120px">CATEGORY</span><span style="width:50px;flex:none;text-align:right">W-L</span><span style="width:84px;flex:none;text-align:right">HIT / NEED</span><span style="width:58px;flex:none;text-align:right">NET</span><span style="width:50px;flex:none;text-align:right">ROI</span><span style="width:14px;flex:none"></span></div>'
      +crows+'</div>';
  }
  var noData=picks.length?'':'<div style="padding:18px;color:#94a3b8;font-size:.84rem">No graded picks in this price range.</div>';
  var odBig='<div id="_odBig" onclick="_odBigClose()" style="display:none;position:fixed;inset:0;background:rgba(2,6,23,.82);z-index:100001;align-items:center;justify-content:center;padding:16px">'
    +'<div onclick="event.stopPropagation()" style="background:#0a1424;border:1px solid #243450;border-radius:14px;max-width:560px;width:92%;max-height:80vh;display:flex;flex-direction:column;box-shadow:0 20px 60px rgba(0,0,0,.65)">'
    +'<div style="display:flex;align-items:center;gap:10px;padding:13px 16px;border-bottom:1px solid #1e293b"><div id="_odBigTitle" style="flex:1;font-weight:900;color:#fff;font-size:.96rem"></div>'
    +'<button onclick="_odBigClose()" style="background:#334155;color:#fff;border:none;border-radius:8px;padding:6px 13px;font-size:.8rem;font-weight:800;cursor:pointer">Close</button></div>'
    +'<div id="_odBigBody" style="padding:6px 14px 14px;overflow-y:auto"></div></div></div>';
  var card='<div onclick="event.stopPropagation()" style="background:#0a1424;border:1px solid #243450;border-radius:14px;max-width:680px;width:94%;max-height:84vh;display:flex;flex-direction:column;box-shadow:0 20px 60px rgba(0,0,0,.6)">'
    +'<div style="display:flex;align-items:center;gap:10px;padding:14px 16px;border-bottom:1px solid #1e293b">'
      +'<span style="display:inline-block;width:11px;height:11px;border-radius:50%;background:'+gm[1]+';flex:none"></span>'
      +'<div style="flex:1"><div style="font-weight:900;color:#fff;font-size:1.02rem">'+bk.lab+'</div><div style="color:#64748b;font-size:.72rem">'+gm[0]+(c.label?(' \u00b7 '+c.label):'')+'</div></div>'
      +'<button onclick="_oddsCloseModal()" style="background:#334155;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.82rem;font-weight:800;cursor:pointer">Close</button>'
    +'</div>'
    +'<div style="padding:14px 16px;overflow-y:auto">'+summary+dayBreak+catBreak+noData+odBig+'</div>'
  +'</div>';
  _oddsCloseModal();
  var m=document.createElement('div'); m.id='_oddsModal'; m.onclick=_oddsCloseModal;
  m.setAttribute('style','position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:99999;display:flex;align-items:center;justify-content:center;padding:16px');
  m.innerHTML=card; document.body.appendChild(m);
}
// ===== Manual "Get Results" — drop the cached grade for the day on screen and
// re-pull box scores so every pending pick settles on demand (no page reload). =====
function _trkGetResults(){ var d=window.__TRK_DAILY_DATE__||_trkTodayISO(); if(window.__TRK_GRADE_CACHE__) delete window.__TRK_GRADE_CACHE__[d]; _trkLoadDaily(d); }
function _ovfGetResults(){ var d=window.__OVF_DAILY_DATE__||_trkTodayISO(); if(window.__TRK_GRADE_CACHE__) delete window.__TRK_GRADE_CACHE__[d]; _ovfLoadDaily(d); }
function _hrtGetResults(){ var d=window.__HRT_DAILY_DATE__||_trkTodayISO(); if(window.__TRK_GRADE_CACHE__) delete window.__TRK_GRADE_CACHE__[d]; _hrtLoadDaily(d); }
function _trkRenderDailyTab(be,stake){
  var date=window.__TRK_DAILY_DATE__||_trkTodayISO();
  var cache=window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{};
  var g=cache[date];
  var CAT_CFG=window.__TRK_CFG__||{}, CAT_ORDER=window.__TRK_ORDER__||[];
  var view=window.__TRK_DVIEW__||'cat';
  var datesel='<div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin-bottom:12px"><label style="font-size:.82rem;color:#94a3b8">Day <input type="date" value="'+date+'" max="'+_trkTodayISO()+'" onchange="_trkLoadDaily(this.value)" style="margin-left:6px;background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:6px 8px;font-size:.82rem"></label><span style="font-weight:800;color:#93c5fd;font-size:.95rem">'+_weekdayName(date)+'</span><span style="display:flex;gap:6px;margin-left:auto"><button onclick="_trkGetResults()" title="Re-pull box scores and settle pending picks" style="background:#0e7490;color:#fff;border:none;border-radius:6px;padding:5px 11px;font-size:.78rem;font-weight:700;cursor:pointer">\u21bb Get Results</button>'+_trkDViewBtn('cat','By Category')+_trkDViewBtn('full','Full List')+_trkDViewBtn('odds','Odds')+'</span></div>';
  if(g===undefined){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">Loading\u2026</p>'; _trkLoadDaily(date); return; }
  if(g==='LOADING'){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">Loading\u2026</p>'; return; }
  if(g&&g.__error__){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">'+(g.__error__)+'</p>'; return; }
  var rows=_trkFlattenFull(g);
  if(!rows.length){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">'+'No picks recorded for '+date+'.'+'</p>'; return; }
  if(view==='odds'){
    var opool=rows.map(function(r){ var c={}; for(var ck in r) c[ck]=r[ck]; c.date=date; return c; });
    window.__ODDS_CTX__={pool:opool,label:'Daily \u00b7 '+_weekdayName(date)+' '+date,stake:stake};
    var opdf='<div style="display:flex;margin-bottom:12px"><button onclick="_oddsPrintCurrent()" style="margin-left:auto;background:#7c3aed;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">📄 PDF this report</button></div>';
    be.innerHTML=datesel+opdf+_oddsReport(opool,stake);
    return;
  }
  if(view==='cat'){
    var cpool=rows.map(function(r){ var c={}; for(var ck in r) c[ck]=r[ck]; c.date=date; return c; });
    var ct=_trkCatTable(cpool,stake,'No decided picks for this day yet \u2014 fills in as games go Final.');
    var co=ct.overall, con=co.w+co.l, crisk=co.counted*stake, croi=crisk?co.net/crisk*100:0, cclr=co.net>=0?'#4ade80':'#f87171', cwclr=_trkRC(co.w,con);
    var cpend=rows.filter(function(r){ return !_trkSkipMeta(r)&&r.result!=='WIN'&&r.result!=='LOSS'&&r.result!=='VOID'; }).length;
    var csum='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#0c1829;border:1px solid #1e293b;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800"><span style="color:'+cwclr+'">'+co.w+'/'+con+'</span> <span style="color:#94a3b8;font-size:.8rem">('+(con?(co.w/con*100).toFixed(1):'0.0')+'%)</span>'+(cpend?' <span style="color:#94a3b8;font-size:.8rem">'+cpend+' pending</span>':'')+'</div><div style="font-size:.86rem">Net <span style="color:'+cclr+';font-weight:900">'+(co.net>=0?'+$':'\u2212$')+Math.abs(co.net).toFixed(0)+'</span> <span style="color:#64748b">\u00b7 ROI '+(croi>=0?'+':'\u2212')+Math.abs(croi).toFixed(1)+'% on $'+crisk.toFixed(0)+'</span></div><div style="margin-left:auto;display:flex;gap:8px"><button onclick="downloadTrkDailyCatCSV()" style="background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 Category CSV</button><button onclick="downloadTrkDailyCSV()" style="background:#0e7490;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 Full List</button></div></div>';
    be.innerHTML=datesel+csum+ct.html;
    return;
  }
  var groups={}; rows.forEach(function(r){ var k=(r.category||'?')+'|'+(r.side||'OVER'); (groups[k]=groups[k]||[]).push(r); });
  function _rank(k){ var i=CAT_ORDER.indexOf(k); return i<0?999:i; }
  var keys=Object.keys(groups).sort(function(a,b){ return _rank(a)-_rank(b); });
  var win=0,loss=0,pend=0,net=0,counted=0,body='';
  window.__TRK_LOG_ROWS__=[];
  keys.forEach(function(k){
    var cfg=CAT_CFG[k]||{lbl:k.split('|').join(' '),icon:'📊'};
    var picks=groups[k];
    var gw=picks.filter(function(p){ return p.result==='WIN'; }).length;
    var gn=picks.filter(function(p){ return p.result==='WIN'||p.result==='LOSS'; }).length;
    var gclr=_trkRC(gw,gn);
    body+='<div style="margin:12px 0 4px;font-weight:800;font-size:.83rem;color:#cbd5e1">'+(cfg.icon||'')+' '+cfg.lbl+' <span style="color:'+gclr+';font-family:monospace;font-weight:900">'+gw+'/'+gn+'</span></div>';
    picks.forEach(function(p){
      var logIdx=window.__TRK_LOG_ROWS__.length; window.__TRK_LOG_ROWS__.push(p); p.__date__=date;
      var _meta=_trkSkipMeta(p);
      var rr=p.result;
      var mk=rr==='WIN'?'<span style="color:#4ade80">\u2713</span>':(rr==='LOSS'?'<span style="color:#f87171">\u2717</span>':(rr==='VOID'?'<span style="color:#38bdf8" title="Did not play \u2014 no action">\u25cb</span>':'<span style="color:#64748b">\u00b7</span>'));
      if(!_meta){ if(rr==='WIN')win++; else if(rr==='LOSS')loss++; else if(rr!=='VOID')pend++; }
      var act=(p.actual!=null)?('<span style="color:#cbd5e1">\u2192 '+p.actual+(p.stat?(' '+p.stat):'')+'</span>'):'';
      var odd=_oddsCell(p,logIdx);
      var plHtml,roiHtml='';
      if(rr==='WIN'||rr==='LOSS'){ var pl=_amProfit(_effOdds(p),stake,rr==='WIN'); if(pl===null){ plHtml='<span style="color:#475569;font-family:monospace">\u2014</span>'; } else { if(!_meta){ net+=pl; counted++; } var c=pl>=0?'#4ade80':'#f87171', rp=pl/stake*100; plHtml='<span style="font-family:monospace;font-weight:800;color:'+c+'">'+(pl>=0?'+$':'\u2212$')+Math.abs(pl).toFixed(0)+'</span>'; roiHtml='<span style="font-family:monospace;font-weight:700;color:'+c+'">'+(rp>=0?'+':'\u2212')+Math.abs(rp).toFixed(0)+'%</span>'; } }
      else { plHtml='<span style="color:'+(rr==='VOID'?'#38bdf8':'#64748b')+';font-size:.72rem">'+(rr==='VOID'?'void':'pending')+'</span>'; }
      var logBtn='<button onclick="_trkLogBet('+logIdx+')" title="Log as bet" style="background:#1e3a8a;color:#bfdbfe;border:1px solid #1d4ed8;border-radius:5px;padding:1px 7px;font-size:.66rem;font-weight:800;cursor:pointer;flex-shrink:0">+Log</button>';
      body+='<div style="display:flex;gap:8px;align-items:center;padding:2px 0 2px 6px;font-size:.79rem">'+mk+'<span style="color:#e2e8f0;min-width:130px">'+(p.name||'')+'</span><span style="color:#94a3b8;min-width:120px">'+(p.pick||'')+'</span>'+act+'<span style="margin-left:auto;display:flex;gap:8px;align-items:center"><span style="min-width:52px;text-align:right">'+plHtml+'</span><span style="min-width:44px;text-align:right">'+roiHtml+'</span>'+odd+logBtn+'</span></div>';
    });
  });
  var risk=counted*stake, roi=risk?net/risk*100:0, nclr=net>=0?'#4ade80':'#f87171';
  var summary='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#0c1829;border:1px solid #1e293b;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800"><span style="color:#4ade80">'+win+'W</span> <span style="color:#f87171">'+loss+'L</span>'+(pend?' <span style="color:#94a3b8;font-size:.82rem">'+pend+' pending</span>':'')+'</div><div style="font-size:.86rem">Net <span style="color:'+nclr+';font-weight:900">'+(net>=0?'+$':'\u2212$')+Math.abs(net).toFixed(0)+'</span> <span style="color:#64748b">\u00b7 ROI '+(roi>=0?'+':'\u2212')+Math.abs(roi).toFixed(1)+'% on $'+risk.toFixed(0)+'</span></div><button onclick="downloadTrkDailyCSV()" style="margin-left:auto;background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 CSV</button></div>';
  be.innerHTML=datesel+summary+body;
}
function _trkRenderRangeTab(be,stake,which){
  var CAT_CFG=window.__TRK_CFG__||{};
  var from,to,label,nav='';
  if(which==='weekly'){ to=window.__TRK_TODAY__||_trkTodayISO(); from=_isoShift(to,-6); label='Last 7 days'; }
  else if(which==='custom'){ from=window.__TRK_FROM__||_isoShift(_trkTodayISO(),-6); to=window.__TRK_TO__||_trkTodayISO(); window.__TRK_FROM__=from; window.__TRK_TO__=to; label='Custom'; nav='<span style="display:flex;gap:8px;align-items:center;margin-left:10px;flex-wrap:wrap"><label style="font-size:.76rem;color:#94a3b8">From <input type="date" value="'+from+'" max="'+to+'" onchange="_trkCustomSet(&#39;from&#39;,this.value)" style="margin-left:4px;background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:4px 7px;font-size:.78rem"></label><label style="font-size:.76rem;color:#94a3b8">To <input type="date" value="'+to+'" min="'+from+'" max="'+_trkTodayISO()+'" onchange="_trkCustomSet(&#39;to&#39;,this.value)" style="margin-left:4px;background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:4px 7px;font-size:.78rem"></label></span>'; }
  else { var m=window.__TRK_MONTH__||_trkTodayISO().slice(0,7); from=m+'-01'; to=m+'-31'; var mn=['January','February','March','April','May','June','July','August','September','October','November','December']; label=mn[parseInt(m.slice(5,7),10)-1]+' '+m.slice(0,4); var canNext=(m<_trkTodayISO().slice(0,7)); nav='<span style="display:flex;gap:6px;align-items:center;margin-left:10px"><button onclick="_trkMonthShift(-1)" style="background:#1e293b;color:#fff;border:none;border-radius:6px;padding:4px 11px;cursor:pointer;font-weight:800">\u25c0</button><button onclick="_trkMonthShift(1)"'+(canNext?'':' disabled')+' style="background:'+(canNext?'#1e293b':'#0f172a')+';color:'+(canNext?'#fff':'#475569')+';border:none;border-radius:6px;padding:4px 11px;cursor:'+(canNext?'pointer':'default')+';font-weight:800">\u25b6</button></span>'; }
  var _miss=_trkRangeMissing(from,to);
  if(_miss.length){ be.innerHTML='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#0a1f14;border:1px solid #16432c;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800;color:#6ee7b7;display:flex;align-items:center">'+label+nav+'</div><div style="margin-left:auto;color:#94a3b8;font-size:.84rem">Grading '+_miss.length+' day'+(_miss.length>1?'s':'')+'\u2026</div></div>'; _trkFetchDays(_miss).then(function(){ if((window.__TRK_TAB__||'daily')===which) _trkRenderActive(); }); return; }
  var rview=(window.__TRK_DVIEW__==='odds')?'odds':'cat';
  var rtoggle='<span style="display:flex;gap:6px;margin-left:auto">'+_trkDViewBtn('cat','By Category')+_trkDViewBtn('odds','Odds')+'</span>';
  var rbar='<div style="display:flex;margin-bottom:10px">'+rtoggle+'</div>';
  if(rview==='odds'){
    var rpool=_trkRangePool(from,to);
    window.__ODDS_CTX__={pool:rpool,label:label,stake:stake};
    var rhead='<div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;background:#0a1f14;border:1px solid #16432c;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800;color:#6ee7b7;display:flex;align-items:center">'+label+nav+'</div>'+rtoggle+'</div>';
    var rpdf='<div style="display:flex;margin-bottom:12px"><button onclick="_oddsPrintCurrent()" style="margin-left:auto;background:#7c3aed;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">📄 PDF this report</button></div>';
    be.innerHTML=rhead+rpdf+_oddsReport(rpool,stake);
    return;
  }
  var _rp=_trkRangePool(from,to);
  var ct=_trkCatTable(_rp,stake,null,true);
  var o=ct.overall, on=o.w+o.l, orisk=o.counted*stake, oroi=orisk?o.net/orisk*100:0, oclr=o.net>=0?'#4ade80':'#f87171', owclr=_trkRC(o.w,on);
  var summary='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#0a1f14;border:1px solid #16432c;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800;color:#6ee7b7;display:flex;align-items:center">'+label+nav+'</div><div style="margin-left:auto;text-align:right"><div style="font-weight:800;font-size:.92rem"><span style="color:'+owclr+'">'+o.w+'/'+on+'</span> <span style="color:#94a3b8;font-size:.8rem">('+(on?(o.w/on*100).toFixed(1):'0.0')+'%)</span></div><div style="font-size:.82rem">Net <span style="color:'+oclr+';font-weight:900">'+(o.net>=0?'+$':'\u2212$')+Math.abs(o.net).toFixed(0)+'</span> <span style="color:#64748b">\u00b7 ROI '+(oroi>=0?'+':'\u2212')+Math.abs(oroi).toFixed(1)+'% on $'+orisk.toFixed(0)+'</span></div></div></div>';
  var csvBtn='<div style="display:flex;margin-top:10px"><button onclick="downloadTrkRangeCSV(&#39;'+which+'&#39;)" style="margin-left:auto;background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 CSV</button></div>';
  var html=rbar+summary+ct.html+csvBtn;
  if(which==='monthly') html+='<div style="margin-top:20px;font-size:.74rem;color:#64748b;text-transform:uppercase;letter-spacing:.04em">Advanced (all-time)</div><div id="trk-clv" style="margin-top:8px"></div><div id="trk-calib" style="margin-top:22px"></div>';
  be.innerHTML=html;
  if(which==='monthly'){ _trkRenderCLV(); _trkRenderCalib(stake); }
}
function _trkDownloadCSV(out,fname){ var csv=out.map(function(row){ return row.map(_csvCell).join(','); }).join(String.fromCharCode(13)+String.fromCharCode(10)); var blob=new Blob([String.fromCharCode(65279)+csv],{type:'text/csv;charset=utf-8;'}); var url=URL.createObjectURL(blob); var a=document.createElement('a'); a.href=url; a.download=fname; document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url); }
function downloadTrkDailyCSV(){ var date=window.__TRK_DAILY_DATE__||_trkTodayISO(); var g=(window.__TRK_GRADE_CACHE__||{})[date]; var rows=_trkFlattenFull(g); if(!rows.length){ alert('No picks to export for '+date+'.'); return; } var stake=_trkStake(); var out=[['Date','Weekday','Category','Side','Player','Pick','Odds','Actual','Result','Bet','Profit/Loss']]; var net=0,counted=0; rows.forEach(function(r){ r.__date__=date; var eo=_effOdds(r); var dec=(r.result==='WIN'||r.result==='LOSS'); var pl=dec?_amProfit(eo,stake,r.result==='WIN'):null; var plStr=''; if(pl!==null){ plStr=pl.toFixed(2); net+=pl; counted++; } out.push([date,_weekdayName(date),r.category||'',r.side||'',r.name||'',r.pick||'',(eo!=null?((eo>0?'+':'')+eo):''),(r.actual!=null?r.actual:''),r.result||'pending',stake,plStr]); }); out.push([]); out.push(['','','','','','','','','TOTALS ('+counted+' graded)',(counted*stake),net.toFixed(2)]); _trkDownloadCSV(out,'mlb-daily-'+date+'-flat'+stake+'.csv'); }
function downloadTrkDailyCatCSV(){ var date=window.__TRK_DAILY_DATE__||_trkTodayISO(); var g=(window.__TRK_GRADE_CACHE__||{})[date]; var rows=_trkFlattenFull(g); if(!rows.length){ alert('No picks to export for '+date+'.'); return; } var stake=_trkStake(); var CAT_CFG=window.__TRK_CFG__||{}; var pool=rows.map(function(r){ var c={}; for(var k in r) c[k]=r[k]; c.date=date; return c; }); var ag=_trkAgg(pool,stake); var arr=Object.keys(ag.cats).map(function(k){ var c=ag.cats[k]; c.roi=c.counted?c.net/(c.counted*stake)*100:null; return [k,c]; }); arr.sort(function(a,b){ var ra=a[1].counted?a[1].roi:-1e9, rb=b[1].counted?b[1].roi:-1e9; return rb-ra; }); if(!arr.length){ alert('No graded picks for '+date+' yet.'); return; } var out=[['Date','Weekday','Category','Side','Wins','Losses','Plays','Win%','Bet','Net P/L','ROI%']]; arr.forEach(function(x){ var k=x[0], c=x[1], n=c.w+c.l; if(!n) return; var parts=k.split('|'); out.push([date,_weekdayName(date),(CAT_CFG[k]&&CAT_CFG[k].lbl)||parts[0],parts[1],c.w,c.l,n,(c.w/n*100).toFixed(1),stake,(c.counted?c.net.toFixed(2):''),(c.counted?c.roi.toFixed(1):'')]); }); var o=ag.overall, on=o.w+o.l; out.push([]); out.push([date,_weekdayName(date),'OVERALL','',o.w,o.l,on,(on?(o.w/on*100).toFixed(1):'0.0'),stake,o.net.toFixed(2),(o.counted?(o.net/(o.counted*stake)*100).toFixed(1):'')]); _trkDownloadCSV(out,'mlb-daily-cat-'+date+'-flat'+stake+'.csv'); }
function downloadTrkRangeCSV(which){ var stake=_trkStake(), from,to,tag; if(which==='weekly'){ to=window.__TRK_TODAY__||_trkTodayISO(); from=_isoShift(to,-6); tag='last7-'+from+'_'+to; } else if(which==='custom'){ from=window.__TRK_FROM__||_isoShift(_trkTodayISO(),-6); to=window.__TRK_TO__||_trkTodayISO(); tag='range-'+from+'_'+to; } else { var m=window.__TRK_MONTH__||_trkTodayISO().slice(0,7); from=m+'-01'; to=m+'-31'; tag='month-'+m; } var ag=_trkAgg(_trkRangePool(from,to),stake); var CAT_CFG=window.__TRK_CFG__||{}; var arr=Object.keys(ag.cats).map(function(k){ var c=ag.cats[k]; c.roi=c.counted?c.net/(c.counted*stake)*100:null; return [k,c]; }); arr.sort(function(a,b){ var ra=a[1].counted?a[1].roi:-1e9, rb=b[1].counted?b[1].roi:-1e9; return rb-ra; }); if(!arr.length){ alert('No graded picks in this range yet.'); return; } var out=[['Range','Category','Side','Wins','Losses','Plays','Win%','Bet','Net P/L','ROI%']]; arr.forEach(function(x){ var k=x[0], c=x[1], n=c.w+c.l; if(!n) return; var parts=k.split('|'); out.push([from+'_'+to,(CAT_CFG[k]&&CAT_CFG[k].lbl)||parts[0],parts[1],c.w,c.l,n,(c.w/n*100).toFixed(1),stake,(c.counted?c.net.toFixed(2):''),(c.counted?c.roi.toFixed(1):'')]); }); var o=ag.overall, on=o.w+o.l; out.push([]); out.push([from+'_'+to,'OVERALL','',o.w,o.l,on,(on?(o.w/on*100).toFixed(1):'0.0'),stake,o.net.toFixed(2),(o.counted?(o.net/(o.counted*stake)*100).toFixed(1):'')]); _trkDownloadCSV(out,'mlb-'+tag+'-flat'+stake+'.csv'); }
function _trkLogBet(idx){
  var p=(window.__TRK_LOG_ROWS__||[])[idx]; if(!p) return;
  window.__BET_SRC__=window.__BET_SRC__||{};
  var sl=(p.pick||'').replace(/^(OVER|UNDER)\s+[\d.]+\s*/i,'')||(p.stat||'');
  window.__BET_SRC__['__trklog__']={
    name:p.name||'',
    team:p.team||'',
    opp:p.opp||'',
    category:p.category||'',
    stat_key:p.stat_key||'hits',
    stat_label:sl,
    side:p.side||'OVER',
    line:p.line!=null?Number(p.line):0.5,
    odds:_effOdds(p),
    date:p.date||new Date().toISOString().slice(0,10)
  };
  _betForm('__trklog__');
}
// ===== Overflow Tracker (admin) — ranks 11-30 per side, its own permanent book =====
function _ovfStake(){ var inp=document.getElementById('ovfBet'); var s=inp?Number(inp.value):NaN; if(!isFinite(s)||s<=0){ s=(window.__OVF_BET__!=null?window.__OVF_BET__:(window.__TRK_BET__!=null?window.__TRK_BET__:20)); } if(!isFinite(s)||s<=0) s=20; return s; }
function _ovfBetInput(){ window.__OVF_BET__=_ovfStake(); _ovfRenderActive(); }
function _ovfTabBtn(id,label){ var active=(window.__OVF_TAB__||'daily')===id; return '<button onclick="_ovfTab(&#39;'+id+'&#39;)" style="background:'+(active?'#b45309':'#1e293b')+';color:'+(active?'#fff':'#cbd5e1')+';border:none;border-radius:8px;padding:8px 20px;font-size:.86rem;font-weight:800;cursor:pointer">'+label+'</button>'; }
function _ovfTab(t){ window.__OVF_BET__=_ovfStake(); window.__OVF_TAB__=t; renderOverflow(window.__TRACK__); }
function _ovfDViewBtn(id,label){ var active=(window.__OVF_DVIEW__||'cat')===id; return '<button onclick="_ovfDView(&#39;'+id+'&#39;)" style="background:'+(active?'#0e7490':'#1e293b')+';color:'+(active?'#fff':'#cbd5e1')+';border:none;border-radius:7px;padding:6px 14px;font-size:.78rem;font-weight:700;cursor:pointer">'+label+'</button>'; }
function _ovfDView(v){ window.__OVF_BET__=_ovfStake(); window.__OVF_DVIEW__=v; _ovfRenderActive(); }
function _ovfMonthShift(n){ window.__OVF_BET__=_ovfStake(); var m=window.__OVF_MONTH__||_trkTodayISO().slice(0,7); var y=parseInt(m.slice(0,4),10), mo=parseInt(m.slice(5,7),10)-1+n; while(mo<0){mo+=12;y--;} while(mo>11){mo-=12;y++;} var nm=y+'-'+((mo+1)<10?'0':'')+(mo+1); var cur=_trkTodayISO().slice(0,7); if(nm>cur) nm=cur; window.__OVF_MONTH__=nm; _ovfRenderActive(); }
function _ovfCustomSet(which,val){ window.__OVF_BET__=_ovfStake(); var today=_trkTodayISO(); var f=window.__OVF_FROM__||_isoShift(today,-6), t=window.__OVF_TO__||today; if(which==='from'){ f=val; if(f>t) t=f; } else { t=val; if(t>today) t=today; if(t<f) f=t; } window.__OVF_FROM__=f; window.__OVF_TO__=t; _ovfRenderActive(); }
function _ovfRenderActive(){ var be=document.getElementById('ovf-body'); if(!be) return; var stake=_ovfStake(); var t=window.__OVF_TAB__||'daily'; if(t==='daily') _ovfRenderDailyTab(be,stake); else _ovfRenderRangeTab(be,stake,t); }
// Range pool = overflow rows ONLY. Locked days come from d.detail (filtered to
// overflow); unlocked days are graded on demand into the shared grade cache.
function _ovfRangePool(from,to){ var d=window.__TRACK__||{}; var pool=[]; var have={}; (d.detail||[]).forEach(function(r){ if(r.date>=from&&r.date<=to){ have[r.date]=true; if(_isOvfCat(r.category)&&!_isHrCat(r.category)) pool.push(r); } }); var cache=window.__TRK_GRADE_CACHE__||{}; var cur=from; while(cur<=to){ if(!have[cur]){ _ovfFlatten(cache[cur]).forEach(function(r){ if(r.result==='WIN'||r.result==='LOSS'){ var c={}; for(var kk in r) c[kk]=r[kk]; c.date=cur; pool.push(c); } }); } cur=_isoShift(cur,1); } return pool; }
async function _ovfLoadDaily(date){ window.__OVF_DAILY_DATE__=date; window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{}; var cur=window.__TRK_GRADE_CACHE__[date]; if(cur&&cur!=='LOADING'){ _ovfRenderActive(); return; } var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||''; var adm=new URLSearchParams(location.search).get('admin')||''; window.__TRK_GRADE_CACHE__[date]='LOADING'; _ovfRenderActive(); try{ var res=await fetch('/api/grade/'+date+'?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'')); if(!res.ok){ var t=await res.text(); window.__TRK_GRADE_CACHE__[date]={__error__:(t||'No picks for this date')}; } else { window.__TRK_GRADE_CACHE__[date]=await res.json(); } }catch(e){ window.__TRK_GRADE_CACHE__[date]={__error__:String((e&&e.message)||e)}; } _ovfRenderActive(); }
function _ovfRenderDailyTab(be,stake){
  var date=window.__OVF_DAILY_DATE__||_trkTodayISO();
  var cache=window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{};
  var g=cache[date];
  var CAT_CFG=window.__TRK_CFG__||{}, CAT_ORDER=window.__OVF_ORDER__||[];
  var view=window.__OVF_DVIEW__||'cat';
  var datesel='<div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin-bottom:12px"><label style="font-size:.82rem;color:#94a3b8">Day <input type="date" value="'+date+'" max="'+_trkTodayISO()+'" onchange="_ovfLoadDaily(this.value)" style="margin-left:6px;background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:6px 8px;font-size:.82rem"></label><span style="font-weight:800;color:#93c5fd;font-size:.95rem">'+_weekdayName(date)+'</span><span style="display:flex;gap:6px;margin-left:auto"><button onclick="_ovfGetResults()" title="Re-pull box scores and settle pending picks" style="background:#0e7490;color:#fff;border:none;border-radius:6px;padding:5px 11px;font-size:.78rem;font-weight:700;cursor:pointer">\u21bb Get Results</button>'+_ovfDViewBtn('cat','By Category')+_ovfDViewBtn('full','Full List')+_ovfDViewBtn('odds','Odds')+'</span></div>';
  if(g===undefined){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">Loading\u2026</p>'; _ovfLoadDaily(date); return; }
  if(g==='LOADING'){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">Loading\u2026</p>'; return; }
  if(g&&g.__error__){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">'+(g.__error__)+'</p>'; return; }
  var rows=_ovfFlatten(g);
  if(!rows.length){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">No overflow picks recorded for '+date+'.</p>'; return; }
  if(view==='odds'){
    var opool=rows.map(function(r){ var c={}; for(var ck in r) c[ck]=r[ck]; c.date=date; return c; });
    window.__ODDS_CTX__={pool:opool,label:'Overflow \u00b7 '+_weekdayName(date)+' '+date,stake:stake};
    var opdf='<div style="display:flex;margin-bottom:12px"><button onclick="_oddsPrintCurrent()" style="margin-left:auto;background:#7c3aed;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">📄 PDF this report</button></div>';
    be.innerHTML=datesel+opdf+_oddsReport(opool,stake);
    return;
  }
  if(view==='cat'){
    var cpool=rows.map(function(r){ var c={}; for(var ck in r) c[ck]=r[ck]; c.date=date; return c; });
    var ct=_trkCatTable(cpool,stake,'No decided overflow picks for this day yet \u2014 fills in as games go Final.');
    var co=ct.overall, con=co.w+co.l, crisk=co.counted*stake, croi=crisk?co.net/crisk*100:0, cclr=co.net>=0?'#4ade80':'#f87171', cwclr=_trkRC(co.w,con);
    var cpend=rows.filter(function(r){ return !_trkSkipMeta(r)&&r.result!=='WIN'&&r.result!=='LOSS'&&r.result!=='VOID'; }).length;
    var csum='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#0c1829;border:1px solid #1e293b;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800"><span style="color:'+cwclr+'">'+co.w+'/'+con+'</span> <span style="color:#94a3b8;font-size:.8rem">('+(con?(co.w/con*100).toFixed(1):'0.0')+'%)</span>'+(cpend?' <span style="color:#94a3b8;font-size:.8rem">'+cpend+' pending</span>':'')+'</div><div style="font-size:.86rem">Net <span style="color:'+cclr+';font-weight:900">'+(co.net>=0?'+$':'\u2212$')+Math.abs(co.net).toFixed(0)+'</span> <span style="color:#64748b">\u00b7 ROI '+(croi>=0?'+':'\u2212')+Math.abs(croi).toFixed(1)+'% on $'+crisk.toFixed(0)+'</span></div><button onclick="downloadOvfDailyCSV()" style="margin-left:auto;background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 CSV</button></div>';
    be.innerHTML=datesel+csum+ct.html;
    return;
  }
  var groups={}; rows.forEach(function(r){ var k=(r.category||'?')+'|'+(r.side||'OVER'); (groups[k]=groups[k]||[]).push(r); });
  function _rank(k){ var i=CAT_ORDER.indexOf(k); return i<0?999:i; }
  var keys=Object.keys(groups).sort(function(a,b){ return _rank(a)-_rank(b); });
  var win=0,loss=0,pend=0,net=0,counted=0,body='';
  window.__TRK_LOG_ROWS__=[];
  keys.forEach(function(k){
    var cfg=CAT_CFG[k]||{lbl:k.split('|').join(' '),icon:'⭐'};
    var picks=groups[k];
    var gw=picks.filter(function(p){ return p.result==='WIN'; }).length;
    var gn=picks.filter(function(p){ return p.result==='WIN'||p.result==='LOSS'; }).length;
    var gclr=_trkRC(gw,gn);
    body+='<div style="margin:12px 0 4px;font-weight:800;font-size:.83rem;color:#cbd5e1">'+(cfg.icon||'')+' '+cfg.lbl+' <span style="color:'+gclr+';font-family:monospace;font-weight:900">'+gw+'/'+gn+'</span></div>';
    picks.forEach(function(p){
      var logIdx=window.__TRK_LOG_ROWS__.length; window.__TRK_LOG_ROWS__.push(p); p.__date__=date;
      var _meta=_trkSkipMeta(p);
      var rr=p.result;
      var mk=rr==='WIN'?'<span style="color:#4ade80">\u2713</span>':(rr==='LOSS'?'<span style="color:#f87171">\u2717</span>':(rr==='VOID'?'<span style="color:#38bdf8" title="Did not play \u2014 no action">\u25cb</span>':'<span style="color:#64748b">\u00b7</span>'));
      if(!_meta){ if(rr==='WIN')win++; else if(rr==='LOSS')loss++; else if(rr!=='VOID')pend++; }
      var act=(p.actual!=null)?('<span style="color:#cbd5e1">\u2192 '+p.actual+(p.stat?(' '+p.stat):'')+'</span>'):'';
      var odd=_oddsCell(p,logIdx);
      var plHtml,roiHtml='';
      if(rr==='WIN'||rr==='LOSS'){ var pl=_amProfit(_effOdds(p),stake,rr==='WIN'); if(pl===null){ plHtml='<span style="color:#475569;font-family:monospace">\u2014</span>'; } else { if(!_meta){ net+=pl; counted++; } var c=pl>=0?'#4ade80':'#f87171', rp=pl/stake*100; plHtml='<span style="font-family:monospace;font-weight:800;color:'+c+'">'+(pl>=0?'+$':'\u2212$')+Math.abs(pl).toFixed(0)+'</span>'; roiHtml='<span style="font-family:monospace;font-weight:700;color:'+c+'">'+(rp>=0?'+':'\u2212')+Math.abs(rp).toFixed(0)+'%</span>'; } }
      else { plHtml='<span style="color:'+(rr==='VOID'?'#38bdf8':'#64748b')+';font-size:.72rem">'+(rr==='VOID'?'void':'pending')+'</span>'; }
      var logBtn='<button onclick="_trkLogBet('+logIdx+')" title="Log as bet" style="background:#1e3a8a;color:#bfdbfe;border:1px solid #1d4ed8;border-radius:5px;padding:1px 7px;font-size:.66rem;font-weight:800;cursor:pointer;flex-shrink:0">+Log</button>';
      body+='<div style="display:flex;gap:8px;align-items:center;padding:2px 0 2px 6px;font-size:.79rem">'+mk+'<span style="color:#e2e8f0;min-width:130px">'+(p.name||'')+'</span><span style="color:#94a3b8;min-width:120px">'+(p.pick||'')+'</span>'+act+'<span style="margin-left:auto;display:flex;gap:8px;align-items:center"><span style="min-width:52px;text-align:right">'+plHtml+'</span><span style="min-width:44px;text-align:right">'+roiHtml+'</span>'+odd+logBtn+'</span></div>';
    });
  });
  var risk=counted*stake, roi=risk?net/risk*100:0, nclr=net>=0?'#4ade80':'#f87171';
  var summary='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#0c1829;border:1px solid #1e293b;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800"><span style="color:#4ade80">'+win+'W</span> <span style="color:#f87171">'+loss+'L</span>'+(pend?' <span style="color:#94a3b8;font-size:.82rem">'+pend+' pending</span>':'')+'</div><div style="font-size:.86rem">Net <span style="color:'+nclr+';font-weight:900">'+(net>=0?'+$':'\u2212$')+Math.abs(net).toFixed(0)+'</span> <span style="color:#64748b">\u00b7 ROI '+(roi>=0?'+':'\u2212')+Math.abs(roi).toFixed(1)+'% on $'+risk.toFixed(0)+'</span></div><button onclick="downloadOvfDailyCSV()" style="margin-left:auto;background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 CSV</button></div>';
  be.innerHTML=datesel+summary+body;
}
function _ovfRenderRangeTab(be,stake,which){
  var from,to,label,nav='';
  if(which==='weekly'){ to=window.__TRK_TODAY__||_trkTodayISO(); from=_isoShift(to,-6); label='Last 7 days'; }
  else if(which==='custom'){ from=window.__OVF_FROM__||_isoShift(_trkTodayISO(),-6); to=window.__OVF_TO__||_trkTodayISO(); window.__OVF_FROM__=from; window.__OVF_TO__=to; label='Custom'; nav='<span style="display:flex;gap:8px;align-items:center;margin-left:10px;flex-wrap:wrap"><label style="font-size:.76rem;color:#94a3b8">From <input type="date" value="'+from+'" max="'+to+'" onchange="_ovfCustomSet(&#39;from&#39;,this.value)" style="margin-left:4px;background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:4px 7px;font-size:.78rem"></label><label style="font-size:.76rem;color:#94a3b8">To <input type="date" value="'+to+'" min="'+from+'" max="'+_trkTodayISO()+'" onchange="_ovfCustomSet(&#39;to&#39;,this.value)" style="margin-left:4px;background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:4px 7px;font-size:.78rem"></label></span>'; }
  else { var m=window.__OVF_MONTH__||_trkTodayISO().slice(0,7); from=m+'-01'; to=m+'-31'; var mn=['January','February','March','April','May','June','July','August','September','October','November','December']; label=mn[parseInt(m.slice(5,7),10)-1]+' '+m.slice(0,4); var canNext=(m<_trkTodayISO().slice(0,7)); nav='<span style="display:flex;gap:6px;align-items:center;margin-left:10px"><button onclick="_ovfMonthShift(-1)" style="background:#1e293b;color:#fff;border:none;border-radius:6px;padding:4px 11px;cursor:pointer;font-weight:800">\u25c0</button><button onclick="_ovfMonthShift(1)"'+(canNext?'':' disabled')+' style="background:'+(canNext?'#1e293b':'#0f172a')+';color:'+(canNext?'#fff':'#475569')+';border:none;border-radius:6px;padding:4px 11px;cursor:'+(canNext?'pointer':'default')+';font-weight:800">\u25b6</button></span>'; }
  var _miss=_trkRangeMissing(from,to);
  if(_miss.length){ be.innerHTML='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#2a1c08;border:1px solid #5b3d12;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800;color:#fcd34d;display:flex;align-items:center">'+label+nav+'</div><div style="margin-left:auto;color:#94a3b8;font-size:.84rem">Grading '+_miss.length+' day'+(_miss.length>1?'s':'')+'\u2026</div></div>'; _trkFetchDays(_miss).then(function(){ if((window.__OVF_TAB__||'daily')===which) _ovfRenderActive(); }); return; }
  var rview=(window.__OVF_DVIEW__==='odds')?'odds':'cat';
  var rtoggle='<span style="display:flex;gap:6px;margin-left:auto">'+_ovfDViewBtn('cat','By Category')+_ovfDViewBtn('odds','Odds')+'</span>';
  var rbar='<div style="display:flex;margin-bottom:10px">'+rtoggle+'</div>';
  if(rview==='odds'){
    var rpool=_ovfRangePool(from,to);
    window.__ODDS_CTX__={pool:rpool,label:'Overflow \u00b7 '+label,stake:stake};
    var rhead='<div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;background:#2a1c08;border:1px solid #5b3d12;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800;color:#fcd34d;display:flex;align-items:center">'+label+nav+'</div>'+rtoggle+'</div>';
    var rpdf='<div style="display:flex;margin-bottom:12px"><button onclick="_oddsPrintCurrent()" style="margin-left:auto;background:#7c3aed;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">📄 PDF this report</button></div>';
    be.innerHTML=rhead+rpdf+_oddsReport(rpool,stake);
    return;
  }
  var ct=_trkCatTable(_ovfRangePool(from,to),stake,'No graded overflow picks in this range yet \u2014 fills in as slates go Final.',true);
  var o=ct.overall, on=o.w+o.l, orisk=o.counted*stake, oroi=orisk?o.net/orisk*100:0, oclr=o.net>=0?'#4ade80':'#f87171', owclr=_trkRC(o.w,on);
  var summary='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#2a1c08;border:1px solid #5b3d12;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800;color:#fcd34d;display:flex;align-items:center">'+label+nav+'</div><div style="margin-left:auto;text-align:right"><div style="font-weight:800;font-size:.92rem"><span style="color:'+owclr+'">'+o.w+'/'+on+'</span> <span style="color:#94a3b8;font-size:.8rem">('+(on?(o.w/on*100).toFixed(1):'0.0')+'%)</span></div><div style="font-size:.82rem">Net <span style="color:'+oclr+';font-weight:900">'+(o.net>=0?'+$':'\u2212$')+Math.abs(o.net).toFixed(0)+'</span> <span style="color:#64748b">\u00b7 ROI '+(oroi>=0?'+':'\u2212')+Math.abs(oroi).toFixed(1)+'% on $'+orisk.toFixed(0)+'</span></div></div></div>';
  var csvBtn='<div style="display:flex;margin-top:10px"><button onclick="downloadOvfRangeCSV(&#39;'+which+'&#39;)" style="margin-left:auto;background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 CSV</button></div>';
  be.innerHTML=rbar+summary+ct.html+csvBtn;
}
function _ovfMatrixScorecard(d){
  var det=((d&&d.detail)||[]).filter(function(r){ return _isOvfCat(r.category)&&!_isHrCat(r.category); }); var stake=_ovfStake();
  function B(){ return {w:0,l:0,net:0,counted:0}; }
  function add(b,win,odds){ if(win) b.w++; else b.l++; var pl=_amProfit(odds,stake,win); if(pl!==null){ b.net+=pl; b.counted++; } }
  function tot(b){ return b.w+b.l; }
  function pct(b){ var n=tot(b); return n?(b.w/n*100):0; }
  function roi(b){ return b.counted?(b.net/(b.counted*stake)*100):0; }
  var dayAgree=B(), dayFade=B();
  var vGreen=B(), vRed=B(), vAmber=B();
  var perMkt={}, perVerdict={};
  det.forEach(function(r){
    if(r.result!=='WIN'&&r.result!=='LOSS') return;
    var info=_mtxCatInfo(_ovfBaseCat(r.category)); if(!info) return;
    var isPit=info[0], ci=info[1];
    var side=(r.side==='UNDER')?'U':'O';
    var wd=_mtxWeekday(r.date); if(wd==null) return;
    var dLean=_mtxDayLean(wd,isPit,ci); if(!dLean) return;
    var win=r.result==='WIN';
    var dAgree=(dLean===side);
    add(dAgree?dayAgree:dayFade, win, r.odds);
    var mk=perMkt[r.category]=perMkt[r.category]||{a:B(),f:B()};
    add(dAgree?mk.a:mk.f, win, r.odds);
    var pos=r.series_pos;
    if(pos===1||pos===2||pos===3){
      var sLean=_mtxSeriesLean(pos,isPit,ci);
      if(sLean){
        var pv=perVerdict[r.category]=perVerdict[r.category]||{g:B(),a:B(),r:B()};
        if(dLean&&sLean!==dLean){ add(vAmber,win,r.odds); add(pv.a,win,r.odds); }
        else if(sLean===side){ add(vGreen,win,r.odds); add(pv.g,win,r.odds); }
        else { add(vRed,win,r.odds); add(pv.r,win,r.odds); }
      }
    }
  });
  function statRow(label,b,clr){
    if(!tot(b)) return '';
    var p=pct(b).toFixed(0); var rv=roi(b); var rc=rv>=0?'#4ade80':'#f87171';
    return '<div style="display:flex;align-items:center;gap:10px;padding:7px 2px;border-bottom:1px solid #16233a">'
      +'<span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:'+clr+';flex:none"></span>'
      +'<span style="flex:1;color:#cbd5e1;font-size:.8rem;font-weight:700">'+label+'</span>'
      +'<span style="font-family:monospace;color:#e2e8f0;font-size:.78rem;width:54px;text-align:right">'+b.w+'-'+b.l+'</span>'
      +'<span style="font-family:monospace;color:#fff;font-weight:800;font-size:.78rem;width:46px;text-align:right">'+p+'%</span>'
      +'<span style="font-family:monospace;font-weight:800;font-size:.78rem;width:60px;text-align:right;color:'+rc+'">'+(rv>=0?'+':'\u2212')+Math.abs(rv).toFixed(0)+'%</span>'
      +'</div>';
  }
  var colHdr='<div style="display:flex;align-items:center;gap:10px;padding:2px 2px 4px">'
    +'<span style="width:9px;flex:none"></span><span style="flex:1"></span>'
    +'<span style="color:#64748b;font-size:.6rem;font-weight:800;letter-spacing:.05em;width:54px;text-align:right">W-L</span>'
    +'<span style="color:#64748b;font-size:.6rem;font-weight:800;letter-spacing:.05em;width:46px;text-align:right">WIN%</span>'
    +'<span style="color:#64748b;font-size:.6rem;font-weight:800;letter-spacing:.05em;width:60px;text-align:right">ROI</span></div>';
  var n1=tot(dayAgree)+tot(dayFade);
  var diff=pct(dayAgree)-pct(dayFade);
  var vClr,vTxt;
  if(n1<10){ vClr='#94a3b8'; vTxt='Only '+n1+' graded overflow picks so far \u2014 still gathering. Banks as more slates go Final.'; }
  else { vClr='#94a3b8'; vTxt='Experimental day-of-week lean, shown for tracking only \u2014 not a betting recommendation while we gather results.'; }
  var s1='<div style="margin-bottom:6px;color:#fbbf24;font-size:.72rem;font-weight:800;letter-spacing:.05em;text-transform:uppercase">1 \u00b7 Day-of-Week Signal \u2014 Overflow History</div>'
    +colHdr
    +statRow('Day lean matches this pick',dayAgree,'#22c55e')
    +statRow('Day lean opposite this pick',dayFade,'#ef4444')
    +'<div style="margin-top:8px;font-size:.74rem;line-height:1.5;color:'+vClr+'">'+vTxt+'</div>';
  var mkRows='';
  Object.keys(perMkt).sort(function(a,b){ return pct(perMkt[b].a)-pct(perMkt[a].a); }).forEach(function(cat){
    var m=perMkt[cat]; if(!tot(m.a)&&!tot(m.f)) return;
    function cell(b){ if(!tot(b)) return '<span style="color:#475569">\u2014</span>'; var p=pct(b); var c=p>=55?'#4ade80':p>=45?'#fbbf24':'#f87171'; return '<span style="color:'+c+';font-weight:700">'+b.w+'-'+b.l+'</span> <span style="color:#64748b">('+p.toFixed(0)+'%)</span>'; }
    mkRows+='<div style="display:flex;align-items:center;gap:8px;padding:5px 2px;border-bottom:1px solid #111c2e;font-size:.74rem">'
      +'<span style="flex:1;color:#cbd5e1">'+cat+'</span>'
      +'<span style="width:96px;text-align:right;font-family:monospace">'+cell(m.a)+'</span>'
      +'<span style="width:96px;text-align:right;font-family:monospace">'+cell(m.f)+'</span></div>';
  });
  var s1mk = mkRows ? ('<details style="margin-top:10px"><summary style="cursor:pointer;color:#64748b;font-size:.7rem;font-weight:700;letter-spacing:.04em">Per-market breakdown (which overflow markets the chart predicts)</summary>'
    +'<div style="margin-top:6px"><div style="display:flex;gap:8px;padding:2px;font-size:.6rem;color:#64748b;font-weight:800"><span style="flex:1"></span><span style="width:96px;text-align:right">AGREE</span><span style="width:96px;text-align:right">FADE</span></div>'+mkRows+'</div></details>') : '';
  var n2=tot(vGreen)+tot(vRed)+tot(vAmber);
  var s2;
  if(!n2){
    s2='<div style="margin-bottom:6px;color:#a78bfa;font-size:.72rem;font-weight:800;letter-spacing:.05em;text-transform:uppercase">2 \u00b7 Combined Verdict \u2014 Series + Day</div>'
      +'<div style="font-size:.74rem;line-height:1.5;color:#94a3b8">Banks from your next graded slate forward. Older overflow picks were logged before the series position (G1/G2/G3) was stored, so the full verdict starts filling in now.</div>';
  } else {
    s2='<div style="margin-bottom:6px;color:#a78bfa;font-size:.72rem;font-weight:800;letter-spacing:.05em;text-transform:uppercase">2 \u00b7 Combined Verdict \u2014 Series + Day</div>'
      +colHdr
      +statRow('Green \u2014 day + series both leaned this side',vGreen,'#22c55e')
      +statRow('Red \u2014 day + series leaned the other way',vRed,'#ef4444')
      +statRow('Amber \u2014 day + series split',vAmber,'#f59e0b')
      +'<div style="margin-top:8px;font-size:.7rem;color:#64748b;line-height:1.5">Experimental day-of-week + series lean, shown for tracking only \u2014 not a betting recommendation while we gather results.</div>'
      +_mtxVerdictRows(perVerdict,stake);
  }
  return '<div style="background:#1a1206;border:1px solid #5b3d12;border-radius:12px;padding:14px 18px;margin-bottom:14px">'
    +'<div style="display:flex;align-items:center;gap:8px;margin-bottom:12px">'
    +'<span style="font-size:1rem">🧮</span>'
    +'<span style="font-weight:800;color:#fde68a;font-size:.95rem">Overflow Lean Tracker (experimental)</span>'
    +'<span style="color:#64748b;font-size:.7rem">how each lean group has done so far</span></div>'
    +s1+s1mk
    +'<div style="height:1px;background:#5b3d12;margin:14px 0"></div>'
    +s2+'</div>';
}
function downloadOvfDailyCSV(){ var date=window.__OVF_DAILY_DATE__||_trkTodayISO(); var g=(window.__TRK_GRADE_CACHE__||{})[date]; var rows=_ovfFlatten(g); if(!rows.length){ alert('No overflow picks to export for '+date+'.'); return; } var stake=_ovfStake(); var CAT_CFG=window.__TRK_CFG__||{}; var out=[['Date','Weekday','Category','Side','Player','Pick','Odds','Actual','Result','Bet','Profit/Loss']]; var net=0,counted=0; rows.forEach(function(r){ r.__date__=date; var eo=_effOdds(r); var dec=(r.result==='WIN'||r.result==='LOSS'); var pl=dec?_amProfit(eo,stake,r.result==='WIN'):null; var plStr=''; if(pl!==null){ plStr=pl.toFixed(2); net+=pl; counted++; } var cfg=CAT_CFG[(r.category||'')+'|'+(r.side||'OVER')]; out.push([date,_weekdayName(date),(cfg&&cfg.lbl)||r.category||'',r.side||'',r.name||'',r.pick||'',(eo!=null?((eo>0?'+':'')+eo):''),(r.actual!=null?r.actual:''),r.result||'pending',stake,plStr]); }); out.push([]); out.push(['','','','','','','','','TOTALS ('+counted+' graded)',(counted*stake),net.toFixed(2)]); _trkDownloadCSV(out,'mlb-overflow-'+date+'-flat'+stake+'.csv'); }
function downloadOvfRangeCSV(which){ var stake=_ovfStake(), from,to,tag; if(which==='weekly'){ to=window.__TRK_TODAY__||_trkTodayISO(); from=_isoShift(to,-6); tag='last7-'+from+'_'+to; } else if(which==='custom'){ from=window.__OVF_FROM__||_isoShift(_trkTodayISO(),-6); to=window.__OVF_TO__||_trkTodayISO(); tag='range-'+from+'_'+to; } else { var m=window.__OVF_MONTH__||_trkTodayISO().slice(0,7); from=m+'-01'; to=m+'-31'; tag='month-'+m; } var ag=_trkAgg(_ovfRangePool(from,to),stake); var CAT_CFG=window.__TRK_CFG__||{}; var arr=Object.keys(ag.cats).map(function(k){ var c=ag.cats[k]; c.roi=c.counted?c.net/(c.counted*stake)*100:null; return [k,c]; }); arr.sort(function(a,b){ var ra=a[1].counted?a[1].roi:-1e9, rb=b[1].counted?b[1].roi:-1e9; return rb-ra; }); if(!arr.length){ alert('No graded overflow picks in this range yet.'); return; } var out=[['Range','Category','Side','Wins','Losses','Plays','Win%','Bet','Net P/L','ROI%']]; arr.forEach(function(x){ var k=x[0], c=x[1], n=c.w+c.l; if(!n) return; var parts=k.split('|'); out.push([from+'_'+to,(CAT_CFG[k]&&CAT_CFG[k].lbl)||parts[0],parts[1],c.w,c.l,n,(c.w/n*100).toFixed(1),stake,(c.counted?c.net.toFixed(2):''),(c.counted?c.roi.toFixed(1):'')]); }); var o=ag.overall, on=o.w+o.l; out.push([]); out.push([from+'_'+to,'OVERALL','',o.w,o.l,on,(on?(o.w/on*100).toFixed(1):'0.0'),stake,o.net.toFixed(2),(o.counted?(o.net/(o.counted*stake)*100).toFixed(1):'')]); _trkDownloadCSV(out,'mlb-overflow-'+tag+'-flat'+stake+'.csv'); }
async function openOverflow(){
  var btn=document.getElementById('ovf-btn');
  var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||'';
  var adm=new URLSearchParams(location.search).get('admin')||'';
  var lbl=btn?btn.textContent:''; if(btn){ btn.disabled=true; btn.textContent='Loading...'; }
  show('ovf-card');
  document.getElementById('ovf-card').scrollIntoView({behavior:'smooth',block:'start'});
  document.getElementById('ovf-spinner').classList.remove('hidden');
  document.getElementById('ovf-head').innerHTML='';
  document.getElementById('ovf-body').innerHTML='';
  var today=_trkTodayISO();
  window.__TRK_TODAY__=today; window.__OVF_DAILY_DATE__=today;
  window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{};
  if(!window.__OVF_MONTH__) window.__OVF_MONTH__=today.slice(0,7);
  try{
    var q='?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'');
    var trP=fetch('/api/track-record'+q).then(function(r){ if(!r.ok) return r.text().then(function(t){ throw new Error(t); }); return r.json(); });
    var grP=fetch('/api/grade/'+today+q).then(function(r){ return r.ok?r.json():null; }).catch(function(){ return null; });
    var arr=await Promise.all([trP,grP]);
    window.__TRACK__=arr[0];
    if(arr[1]) window.__TRK_GRADE_CACHE__[today]=arr[1];
    renderOverflow(window.__TRACK__);
  }catch(e){
    document.getElementById('ovf-body').innerHTML='<p style="color:#f87171;padding:16px">'+(e.message||'Error loading overflow tracker')+'</p>';
  }finally{
    if(btn){ btn.disabled=false; btn.textContent=lbl; }
    document.getElementById('ovf-spinner').classList.add('hidden');
  }
}
function renderOverflow(d){
  _trkBuildCfg();
  if(!window.__OVF_TAB__) window.__OVF_TAB__='daily';
  if(!window.__OVF_MONTH__) window.__OVF_MONTH__=_trkTodayISO().slice(0,7);
  var bet=(window.__OVF_BET__!=null?window.__OVF_BET__:(window.__TRK_BET__!=null?window.__TRK_BET__:20));
  var hdr='<div style="display:flex;flex-wrap:wrap;gap:14px;align-items:center;background:#2a1c08;border:1px solid #5b3d12;border-radius:12px;padding:14px 18px;margin-bottom:14px">'
    +'<span style="font-weight:800;color:#fcd34d;font-size:1rem">💰 Bet amount $</span>'
    +'<input id="ovfBet" type="number" min="1" step="1" value="'+bet+'" oninput="_ovfBetInput()" style="width:104px;background:#020617;border:1px solid #334155;color:#fff;border-radius:8px;padding:8px 12px;font-size:1.05rem;font-weight:800;text-align:center">'
    +'<span style="color:#94a3b8;font-size:.8rem">flat on every overflow pick</span>'
    +'<button onclick="_ovfPrintReport()" style="margin-left:auto;background:#dc2626;color:#fff;border:none;border-radius:8px;padding:9px 16px;font-size:.84rem;font-weight:800;cursor:pointer">📄 PDF Report</button>'
    +'</div>';
  var tabs='<div style="display:flex;gap:8px;margin-bottom:4px;flex-wrap:wrap">'+_ovfTabBtn('daily','Daily')+_ovfTabBtn('weekly','Weekly')+_ovfTabBtn('monthly','Monthly')+_ovfTabBtn('custom','Custom')+'</div>';
  var sc=_ovfMatrixScorecard(d);
  var he=document.getElementById('ovf-head'); if(he) he.innerHTML=hdr+sc+tabs;
  var be=document.getElementById('ovf-body'); if(be) be.innerHTML='';
  _ovfRenderActive();
}
// ===== HR Tracker (admin) — Home Run Over/Under, its own permanent book. Kept
//       OUT of BOTH the main Track Record and the Overflow Tracker; covers the
//       main HR top-10/side AND HR overflow (ranks 11-20). =====
var HRT_ORDER=['HR|OVER','HR|UNDER','HR (OVF)|OVER','HR (OVF)|UNDER'];
function _hrtStake(){ var inp=document.getElementById('hrtBet'); var s=inp?Number(inp.value):NaN; if(!isFinite(s)||s<=0){ s=(window.__HRT_BET__!=null?window.__HRT_BET__:(window.__TRK_BET__!=null?window.__TRK_BET__:20)); } if(!isFinite(s)||s<=0) s=20; return s; }
function _hrtBetInput(){ window.__HRT_BET__=_hrtStake(); _hrtRenderActive(); }
function _hrtTabBtn(id,label){ var active=(window.__HRT_TAB__||'daily')===id; return '<button onclick="_hrtTab(&#39;'+id+'&#39;)" style="background:'+(active?'#be123c':'#1e293b')+';color:'+(active?'#fff':'#cbd5e1')+';border:none;border-radius:8px;padding:8px 20px;font-size:.86rem;font-weight:800;cursor:pointer">'+label+'</button>'; }
function _hrtTab(t){ window.__HRT_BET__=_hrtStake(); window.__HRT_TAB__=t; renderHRTracker(window.__TRACK__); }
function _hrtDViewBtn(id,label){ var active=(window.__HRT_DVIEW__||'cat')===id; return '<button onclick="_hrtDView(&#39;'+id+'&#39;)" style="background:'+(active?'#0e7490':'#1e293b')+';color:'+(active?'#fff':'#cbd5e1')+';border:none;border-radius:7px;padding:6px 14px;font-size:.78rem;font-weight:700;cursor:pointer">'+label+'</button>'; }
function _hrtDView(v){ window.__HRT_BET__=_hrtStake(); window.__HRT_DVIEW__=v; _hrtRenderActive(); }
function _hrtMonthShift(n){ window.__HRT_BET__=_hrtStake(); var m=window.__HRT_MONTH__||_trkTodayISO().slice(0,7); var y=parseInt(m.slice(0,4),10), mo=parseInt(m.slice(5,7),10)-1+n; while(mo<0){mo+=12;y--;} while(mo>11){mo-=12;y++;} var nm=y+'-'+((mo+1)<10?'0':'')+(mo+1); var cur=_trkTodayISO().slice(0,7); if(nm>cur) nm=cur; window.__HRT_MONTH__=nm; _hrtRenderActive(); }
function _hrtRenderActive(){ var be=document.getElementById('hrtrk-body'); if(!be) return; var stake=_hrtStake(); var t=window.__HRT_TAB__||'daily'; if(t==='daily') _hrtRenderDailyTab(be,stake); else _hrtRenderRangeTab(be,stake,t); }
function _hrFlatten(g){ var out=[]; if(!g||g==='LOADING'||g.__error__) return out; (g.hr||[]).forEach(function(r){ out.push(r); }); (g.overflow||[]).forEach(function(r){ if(_isHrCat(r.category)) out.push(r); }); return out; }
function _hrtRangePool(from,to){ var d=window.__TRACK__||{}; var pool=[]; var have={}; (d.detail||[]).forEach(function(r){ if(r.date>=from&&r.date<=to){ have[r.date]=true; if(_isHrCat(r.category)) pool.push(r); } }); var cache=window.__TRK_GRADE_CACHE__||{}; var cur=from; while(cur<=to){ if(!have[cur]){ _hrFlatten(cache[cur]).forEach(function(r){ if(r.result==='WIN'||r.result==='LOSS'){ var c={}; for(var kk in r) c[kk]=r[kk]; c.date=cur; pool.push(c); } }); } cur=_isoShift(cur,1); } return pool; }
async function _hrtLoadDaily(date){ window.__HRT_DAILY_DATE__=date; window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{}; var cur=window.__TRK_GRADE_CACHE__[date]; if(cur&&cur!=='LOADING'){ _hrtRenderActive(); return; } var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||''; var adm=new URLSearchParams(location.search).get('admin')||''; window.__TRK_GRADE_CACHE__[date]='LOADING'; _hrtRenderActive(); try{ var res=await fetch('/api/grade/'+date+'?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'')); if(!res.ok){ var t=await res.text(); window.__TRK_GRADE_CACHE__[date]={__error__:(t||'No picks for this date')}; } else { window.__TRK_GRADE_CACHE__[date]=await res.json(); } }catch(e){ window.__TRK_GRADE_CACHE__[date]={__error__:String((e&&e.message)||e)}; } _hrtRenderActive(); }
function _hrtRenderDailyTab(be,stake){
  var date=window.__HRT_DAILY_DATE__||_trkTodayISO();
  var cache=window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{};
  var g=cache[date];
  var CAT_CFG=window.__TRK_CFG__||{};
  var view=window.__HRT_DVIEW__||'cat';
  var datesel='<div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin-bottom:12px"><label style="font-size:.82rem;color:#94a3b8">Day <input type="date" value="'+date+'" max="'+_trkTodayISO()+'" onchange="_hrtLoadDaily(this.value)" style="margin-left:6px;background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:6px 8px;font-size:.82rem"></label><span style="display:flex;gap:6px;margin-left:auto"><button onclick="_hrtGetResults()" title="Re-pull box scores and settle pending picks" style="background:#0e7490;color:#fff;border:none;border-radius:6px;padding:5px 11px;font-size:.78rem;font-weight:700;cursor:pointer">\u21bb Get Results</button>'+_hrtDViewBtn('cat','By Category')+_hrtDViewBtn('full','Full List')+'</span></div>';
  if(g===undefined){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">Loading\u2026</p>'; _hrtLoadDaily(date); return; }
  if(g==='LOADING'){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">Loading\u2026</p>'; return; }
  if(g&&g.__error__){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">'+(g.__error__)+'</p>'; return; }
  var rows=_hrFlatten(g);
  if(!rows.length){ be.innerHTML=datesel+'<p style="color:#94a3b8;padding:12px">No HR picks recorded for '+date+'.</p>'; return; }
  if(view==='cat'){
    var cpool=rows.map(function(r){ var c={}; for(var ck in r) c[ck]=r[ck]; c.date=date; return c; });
    var ct=_trkCatTable(cpool,stake,'No decided HR picks for this day yet \u2014 fills in as games go Final.');
    var co=ct.overall, con=co.w+co.l, crisk=co.counted*stake, croi=crisk?co.net/crisk*100:0, cclr=co.net>=0?'#4ade80':'#f87171', cwclr=_trkRC(co.w,con);
    var cpend=rows.filter(function(r){ return !_trkSkipMeta(r)&&r.result!=='WIN'&&r.result!=='LOSS'&&r.result!=='VOID'; }).length;
    var csum='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#0c1829;border:1px solid #1e293b;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800"><span style="color:'+cwclr+'">'+co.w+'/'+con+'</span> <span style="color:#94a3b8;font-size:.8rem">('+(con?(co.w/con*100).toFixed(1):'0.0')+'%)</span>'+(cpend?' <span style="color:#94a3b8;font-size:.8rem">'+cpend+' pending</span>':'')+'</div><div style="font-size:.86rem">Net <span style="color:'+cclr+';font-weight:900">'+(co.net>=0?'+$':'\u2212$')+Math.abs(co.net).toFixed(0)+'</span> <span style="color:#64748b">\u00b7 ROI '+(croi>=0?'+':'\u2212')+Math.abs(croi).toFixed(1)+'% on $'+crisk.toFixed(0)+'</span></div><button onclick="downloadHrtDailyCSV()" style="margin-left:auto;background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 CSV</button></div>';
    be.innerHTML=datesel+csum+ct.html;
    return;
  }
  var groups={}; rows.forEach(function(r){ var k=(r.category||'?')+'|'+(r.side||'OVER'); (groups[k]=groups[k]||[]).push(r); });
  function _rank(k){ var i=HRT_ORDER.indexOf(k); return i<0?999:i; }
  var keys=Object.keys(groups).sort(function(a,b){ return _rank(a)-_rank(b); });
  var win=0,loss=0,pend=0,net=0,counted=0,body='';
  window.__TRK_LOG_ROWS__=[];
  keys.forEach(function(k){
    var cfg=CAT_CFG[k]||{lbl:k.split('|').join(' '),icon:'💣'};
    var picks=groups[k];
    var gw=picks.filter(function(p){ return p.result==='WIN'; }).length;
    var gn=picks.filter(function(p){ return p.result==='WIN'||p.result==='LOSS'; }).length;
    var gclr=_trkRC(gw,gn);
    body+='<div style="margin:12px 0 4px;font-weight:800;font-size:.83rem;color:#cbd5e1">'+(cfg.icon||'')+' '+cfg.lbl+' <span style="color:'+gclr+';font-family:monospace;font-weight:900">'+gw+'/'+gn+'</span></div>';
    picks.forEach(function(p){
      var logIdx=window.__TRK_LOG_ROWS__.length; window.__TRK_LOG_ROWS__.push(p); p.__date__=date;
      var _meta=_trkSkipMeta(p);
      var rr=p.result;
      var mk=rr==='WIN'?'<span style="color:#4ade80">\u2713</span>':(rr==='LOSS'?'<span style="color:#f87171">\u2717</span>':(rr==='VOID'?'<span style="color:#38bdf8" title="Did not play \u2014 no action">\u25cb</span>':'<span style="color:#64748b">\u00b7</span>'));
      if(!_meta){ if(rr==='WIN')win++; else if(rr==='LOSS')loss++; else if(rr!=='VOID')pend++; }
      var act=(p.actual!=null)?('<span style="color:#cbd5e1">\u2192 '+p.actual+(p.stat?(' '+p.stat):'')+'</span>'):'';
      var odd=_oddsCell(p,logIdx);
      var plHtml,roiHtml='';
      if(rr==='WIN'||rr==='LOSS'){ var pl=_amProfit(_effOdds(p),stake,rr==='WIN'); if(pl===null){ plHtml='<span style="color:#475569;font-family:monospace">\u2014</span>'; } else { if(!_meta){ net+=pl; counted++; } var c=pl>=0?'#4ade80':'#f87171', rp=pl/stake*100; plHtml='<span style="font-family:monospace;font-weight:800;color:'+c+'">'+(pl>=0?'+$':'\u2212$')+Math.abs(pl).toFixed(0)+'</span>'; roiHtml='<span style="font-family:monospace;font-weight:700;color:'+c+'">'+(rp>=0?'+':'\u2212')+Math.abs(rp).toFixed(0)+'%</span>'; } }
      else { plHtml='<span style="color:'+(rr==='VOID'?'#38bdf8':'#64748b')+';font-size:.72rem">'+(rr==='VOID'?'void':'pending')+'</span>'; }
      var logBtn='<button onclick="_trkLogBet('+logIdx+')" title="Log as bet" style="background:#1e3a8a;color:#bfdbfe;border:1px solid #1d4ed8;border-radius:5px;padding:1px 7px;font-size:.66rem;font-weight:800;cursor:pointer;flex-shrink:0">+Log</button>';
      body+='<div style="display:flex;gap:8px;align-items:center;padding:2px 0 2px 6px;font-size:.79rem">'+mk+'<span style="color:#e2e8f0;min-width:130px">'+(p.name||'')+'</span><span style="color:#94a3b8;min-width:120px">'+(p.pick||'')+'</span>'+act+'<span style="margin-left:auto;display:flex;gap:8px;align-items:center"><span style="min-width:52px;text-align:right">'+plHtml+'</span><span style="min-width:44px;text-align:right">'+roiHtml+'</span>'+odd+logBtn+'</span></div>';
    });
  });
  var risk=counted*stake, roi=risk?net/risk*100:0, nclr=net>=0?'#4ade80':'#f87171';
  var summary='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#0c1829;border:1px solid #1e293b;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800"><span style="color:#4ade80">'+win+'W</span> <span style="color:#f87171">'+loss+'L</span>'+(pend?' <span style="color:#94a3b8;font-size:.82rem">'+pend+' pending</span>':'')+'</div><div style="font-size:.86rem">Net <span style="color:'+nclr+';font-weight:900">'+(net>=0?'+$':'\u2212$')+Math.abs(net).toFixed(0)+'</span> <span style="color:#64748b">\u00b7 ROI '+(roi>=0?'+':'\u2212')+Math.abs(roi).toFixed(1)+'% on $'+risk.toFixed(0)+'</span></div><button onclick="downloadHrtDailyCSV()" style="margin-left:auto;background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 CSV</button></div>';
  be.innerHTML=datesel+summary+body;
}
function _hrtRenderRangeTab(be,stake,which){
  var from,to,label,nav='';
  if(which==='weekly'){ to=window.__TRK_TODAY__||_trkTodayISO(); from=_isoShift(to,-6); label='Last 7 days'; }
  else { var m=window.__HRT_MONTH__||_trkTodayISO().slice(0,7); from=m+'-01'; to=m+'-31'; var mn=['January','February','March','April','May','June','July','August','September','October','November','December']; label=mn[parseInt(m.slice(5,7),10)-1]+' '+m.slice(0,4); var canNext=(m<_trkTodayISO().slice(0,7)); nav='<span style="display:flex;gap:6px;align-items:center;margin-left:10px"><button onclick="_hrtMonthShift(-1)" style="background:#1e293b;color:#fff;border:none;border-radius:6px;padding:4px 11px;cursor:pointer;font-weight:800">\u25c0</button><button onclick="_hrtMonthShift(1)"'+(canNext?'':' disabled')+' style="background:'+(canNext?'#1e293b':'#0f172a')+';color:'+(canNext?'#fff':'#475569')+';border:none;border-radius:6px;padding:4px 11px;cursor:'+(canNext?'pointer':'default')+';font-weight:800">\u25b6</button></span>'; }
  var _miss=_trkRangeMissing(from,to);
  if(_miss.length){ be.innerHTML='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#2a0a14;border:1px solid #5b1228;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800;color:#fda4af;display:flex;align-items:center">'+label+nav+'</div><div style="margin-left:auto;color:#94a3b8;font-size:.84rem">Grading '+_miss.length+' day'+(_miss.length>1?'s':'')+'\u2026</div></div>'; _trkFetchDays(_miss).then(function(){ if((window.__HRT_TAB__||'daily')===which) _hrtRenderActive(); }); return; }
  var ct=_trkCatTable(_hrtRangePool(from,to),stake,'No graded HR picks in this range yet \u2014 fills in as slates go Final.');
  var o=ct.overall, on=o.w+o.l, orisk=o.counted*stake, oroi=orisk?o.net/orisk*100:0, oclr=o.net>=0?'#4ade80':'#f87171', owclr=_trkRC(o.w,on);
  var summary='<div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;background:#2a0a14;border:1px solid #5b1228;border-radius:12px;padding:12px 16px;margin-bottom:12px"><div style="font-weight:800;color:#fda4af;display:flex;align-items:center">'+label+nav+'</div><div style="margin-left:auto;text-align:right"><div style="font-weight:800;font-size:.92rem"><span style="color:'+owclr+'">'+o.w+'/'+on+'</span> <span style="color:#94a3b8;font-size:.8rem">('+(on?(o.w/on*100).toFixed(1):'0.0')+'%)</span></div><div style="font-size:.82rem">Net <span style="color:'+oclr+';font-weight:900">'+(o.net>=0?'+$':'\u2212$')+Math.abs(o.net).toFixed(0)+'</span> <span style="color:#64748b">\u00b7 ROI '+(oroi>=0?'+':'\u2212')+Math.abs(oroi).toFixed(1)+'% on $'+orisk.toFixed(0)+'</span></div></div></div>';
  var csvBtn='<div style="display:flex;margin-top:10px"><button onclick="downloadHrtRangeCSV(&#39;'+which+'&#39;)" style="margin-left:auto;background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-size:.78rem;font-weight:700;cursor:pointer">\u2b07 CSV</button></div>';
  be.innerHTML=summary+ct.html+csvBtn;
}
function downloadHrtDailyCSV(){ var date=window.__HRT_DAILY_DATE__||_trkTodayISO(); var g=(window.__TRK_GRADE_CACHE__||{})[date]; var rows=_hrFlatten(g); if(!rows.length){ alert('No HR picks to export for '+date+'.'); return; } var stake=_hrtStake(); var CAT_CFG=window.__TRK_CFG__||{}; var out=[['Date','Category','Side','Player','Pick','Odds','Actual','Result','Bet','Profit/Loss']]; var net=0,counted=0; rows.forEach(function(r){ var dec=(r.result==='WIN'||r.result==='LOSS'); var pl=dec?_amProfit(r.odds,stake,r.result==='WIN'):null; var plStr=''; if(pl!==null){ plStr=pl.toFixed(2); net+=pl; counted++; } var cfg=CAT_CFG[(r.category||'')+'|'+(r.side||'OVER')]; out.push([date,(cfg&&cfg.lbl)||r.category||'',r.side||'',r.name||'',r.pick||'',(r.odds!=null&&r.odds!==''?((Number(r.odds)>0?'+':'')+r.odds):''),(r.actual!=null?r.actual:''),r.result||'pending',stake,plStr]); }); out.push([]); out.push(['','','','','','','','TOTALS ('+counted+' graded)',(counted*stake),net.toFixed(2)]); _trkDownloadCSV(out,'mlb-hr-'+date+'-flat'+stake+'.csv'); }
function downloadHrtRangeCSV(which){ var stake=_hrtStake(), from,to,tag; if(which==='weekly'){ to=window.__TRK_TODAY__||_trkTodayISO(); from=_isoShift(to,-6); tag='last7-'+from+'_'+to; } else { var m=window.__HRT_MONTH__||_trkTodayISO().slice(0,7); from=m+'-01'; to=m+'-31'; tag='month-'+m; } var ag=_trkAgg(_hrtRangePool(from,to),stake); var CAT_CFG=window.__TRK_CFG__||{}; var arr=Object.keys(ag.cats).map(function(k){ var c=ag.cats[k]; c.roi=c.counted?c.net/(c.counted*stake)*100:null; return [k,c]; }); arr.sort(function(a,b){ var ra=a[1].counted?a[1].roi:-1e9, rb=b[1].counted?b[1].roi:-1e9; return rb-ra; }); if(!arr.length){ alert('No graded HR picks in this range yet.'); return; } var out=[['Range','Category','Side','Wins','Losses','Plays','Win%','Bet','Net P/L','ROI%']]; arr.forEach(function(x){ var k=x[0], c=x[1], n=c.w+c.l; if(!n) return; var parts=k.split('|'); out.push([from+'_'+to,(CAT_CFG[k]&&CAT_CFG[k].lbl)||parts[0],parts[1],c.w,c.l,n,(c.w/n*100).toFixed(1),stake,(c.counted?c.net.toFixed(2):''),(c.counted?c.roi.toFixed(1):'')]); }); var o=ag.overall, on=o.w+o.l; out.push([]); out.push([from+'_'+to,'OVERALL','',o.w,o.l,on,(on?(o.w/on*100).toFixed(1):'0.0'),stake,o.net.toFixed(2),(o.counted?(o.net/(o.counted*stake)*100).toFixed(1):'')]); _trkDownloadCSV(out,'mlb-hr-'+tag+'-flat'+stake+'.csv'); }
async function openHRTracker(){
  var btn=document.getElementById('hrtrk-btn');
  var tok=localStorage.getItem('__mpa_token')||localStorage.getItem('hub_token')||'';
  var adm=new URLSearchParams(location.search).get('admin')||'';
  var lbl=btn?btn.textContent:''; if(btn){ btn.disabled=true; btn.textContent='Loading...'; }
  show('hrtrk-card');
  document.getElementById('hrtrk-card').scrollIntoView({behavior:'smooth',block:'start'});
  document.getElementById('hrtrk-spinner').classList.remove('hidden');
  document.getElementById('hrtrk-head').innerHTML='';
  document.getElementById('hrtrk-body').innerHTML='';
  var today=_trkTodayISO();
  window.__TRK_TODAY__=today; window.__HRT_DAILY_DATE__=today;
  window.__TRK_GRADE_CACHE__=window.__TRK_GRADE_CACHE__||{};
  if(!window.__HRT_MONTH__) window.__HRT_MONTH__=today.slice(0,7);
  try{
    var q='?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'');
    var trP=fetch('/api/track-record'+q).then(function(r){ if(!r.ok) return r.text().then(function(t){ throw new Error(t); }); return r.json(); });
    var grP=fetch('/api/grade/'+today+q).then(function(r){ return r.ok?r.json():null; }).catch(function(){ return null; });
    var arr=await Promise.all([trP,grP]);
    window.__TRACK__=arr[0];
    if(arr[1]) window.__TRK_GRADE_CACHE__[today]=arr[1];
    renderHRTracker(window.__TRACK__);
  }catch(e){
    document.getElementById('hrtrk-body').innerHTML='<p style="color:#f87171;padding:16px">'+(e.message||'Error loading HR tracker')+'</p>';
  }finally{
    if(btn){ btn.disabled=false; btn.textContent=lbl; }
    document.getElementById('hrtrk-spinner').classList.add('hidden');
  }
}
function renderHRTracker(d){
  _trkBuildCfg();
  if(!window.__HRT_TAB__) window.__HRT_TAB__='daily';
  if(!window.__HRT_MONTH__) window.__HRT_MONTH__=_trkTodayISO().slice(0,7);
  var bet=(window.__HRT_BET__!=null?window.__HRT_BET__:(window.__TRK_BET__!=null?window.__TRK_BET__:20));
  var hdr='<div style="display:flex;flex-wrap:wrap;gap:14px;align-items:center;background:#2a0a14;border:1px solid #5b1228;border-radius:12px;padding:14px 18px;margin-bottom:14px">'
    +'<span style="font-weight:800;color:#fda4af;font-size:1rem">💰 Bet amount $</span>'
    +'<input id="hrtBet" type="number" min="1" step="1" value="'+bet+'" oninput="_hrtBetInput()" style="width:104px;background:#020617;border:1px solid #334155;color:#fff;border-radius:8px;padding:8px 12px;font-size:1.05rem;font-weight:800;text-align:center">'
    +'<span style="color:#94a3b8;font-size:.8rem">flat on every HR pick</span>'
    +'</div>';
  var tabs='<div style="display:flex;gap:8px;margin-bottom:4px">'+_hrtTabBtn('daily','Daily')+_hrtTabBtn('weekly','Weekly')+_hrtTabBtn('monthly','Monthly')+'</div>';
  var he=document.getElementById('hrtrk-head'); if(he) he.innerHTML=hdr+tabs;
  var be=document.getElementById('hrtrk-body'); if(be) be.innerHTML='';
  _hrtRenderActive();
}
// American odds -> decimal payout multiplier (incl. stake). null if unpriceable.
function _amDec(o){ if(o==null||o==='') return null; o=Number(o); if(!isFinite(o)||o===0) return null; return o>0?(1+o/100):(1+100/Math.abs(o)); }
// Meta-ranking buckets duplicate the per-category picks — exclude them so CLV
// and calibration never double-count the same bet.
function _trkSkipMeta(r){ return r.category==='Top 10 Batter'||r.category==='Top 10 Pitcher'||r.category==='Value Plays'; }
function _trkAmOdds(o){ o=Number(o); return (o>0?'+':'')+o; }

// CLOSING LINE VALUE — did you get a better price than the market settled at?
// open_odds = first run of the day (what you would have bet), close_odds = the
// locked last run (the closing line). CLV>0 means you beat the close.
function _trkRenderCLV(){
  var d=window.__TRACK__; var el=document.getElementById('trk-clv'); if(!el||!d) return;
  var CAT_CFG=window.__TRK_CFG__||{};
  var det=(d.detail||[]).filter(function(r){ return !_trkSkipMeta(r)&&!_isOvfCat(r.category)&&!_isHrCat(r.category); });
  var rows=[];
  det.forEach(function(r){
    var od=_amDec(r.open_odds), cd=_amDec(r.close_odds);
    if(od==null||cd==null) return;
    rows.push({date:r.date,name:r.name,catKey:(r.category||'')+'|'+(r.side||'OVER'),
      pick:r.pick,open:r.open_odds,close:r.close_odds,clv:(od/cd-1)*100});
  });
  var ttl='<div style="font-size:.9rem;color:#e2e8f0;font-weight:800;margin:2px 2px 4px">📈 Closing Line Value</div>'
    +'<div style="font-size:.74rem;color:#64748b;margin:0 2px 10px">Did you get a better price than the market settled at? Beating the close is the earliest sign of a real edge \u2014 it shows up before the wins do.</div>';
  if(!rows.length){
    el.innerHTML=ttl+'<p style="color:#64748b;font-size:.8rem;padding:4px 2px;border:1px dashed #1e293b;border-radius:10px">No CLV captured yet. It needs two odds reads per day: run picks once when they post (captures your opening price), then run again near first pitch (the closing line). CLV fills in after those games go Final.</p>';
    return;
  }
  var beat=0,sum=0,byCat={};
  rows.forEach(function(x){ if(x.clv>0.01)beat++; sum+=x.clv;
    var c=byCat[x.catKey]=byCat[x.catKey]||{n:0,beat:0,sum:0}; c.n++; if(x.clv>0.01)c.beat++; c.sum+=x.clv; });
  var n=rows.length, avg=sum/n, beatPct=beat/n*100;
  var avgClr=avg>=0?'#4ade80':'#f87171';
  var summary='<div style="display:flex;flex-wrap:wrap;gap:18px;align-items:center;background:#0c1829;border:1px solid #1e293b;border-radius:12px;padding:12px 16px;margin-bottom:12px">'
    +'<div><div style="font-size:.64rem;color:#64748b;text-transform:uppercase;letter-spacing:.05em">Beat the close</div><div style="font-weight:900;font-size:1.15rem;color:'+(beatPct>=50?'#4ade80':'#f87171')+'">'+beat+' / '+n+' <span style="font-size:.85rem;color:#94a3b8">('+beatPct.toFixed(0)+'%)</span></div></div>'
    +'<div><div style="font-size:.64rem;color:#64748b;text-transform:uppercase;letter-spacing:.05em">Avg CLV</div><div style="font-weight:900;font-size:1.15rem;color:'+avgClr+'">'+(avg>=0?'+':'\u2212')+Math.abs(avg).toFixed(1)+'%</div></div>'
    +'<div style="flex:1;min-width:160px;color:#64748b;font-size:.75rem">'+(beatPct>=52?'You are consistently getting prices the market later shortens \u2014 a genuine long-run edge.':'Mixed \u2014 watch which categories beat the close and lean there.')+'</div>'
    +'</div>';
  var cats=Object.keys(byCat).map(function(k){ var c=byCat[k]; return [k,c,c.sum/c.n]; }).sort(function(a,b){ return b[2]-a[2]; });
  var catRows=cats.map(function(x){
    var cfg=CAT_CFG[x[0]]||{lbl:x[0].split('|').join(' '),icon:'📊'};
    var a=x[2], clr=a>=0?'#4ade80':'#f87171';
    return '<div style="display:flex;align-items:center;padding:7px 12px;border-bottom:1px solid #131c2e;font-size:.82rem">'
      +'<span style="flex:1;min-width:150px;color:#e2e8f0;font-weight:600">'+(cfg.icon||'')+' '+cfg.lbl+'</span>'
      +'<span style="width:90px;text-align:right;color:#94a3b8;font-family:monospace">'+x[1].beat+'/'+x[1].n+'</span>'
      +'<span style="width:80px;text-align:right;font-family:monospace;font-weight:800;color:'+clr+'">'+(a>=0?'+':'\u2212')+Math.abs(a).toFixed(1)+'%</span>'
      +'</div>';
  }).join('');
  var catHead='<div style="display:flex;padding:6px 12px;background:#0c1829;border-bottom:1px solid #1e293b;font-size:.64rem;color:#64748b;font-weight:700;text-transform:uppercase">'
    +'<span style="flex:1;min-width:150px">Category</span><span style="width:90px;text-align:right">Beat / Total</span><span style="width:80px;text-align:right">Avg CLV</span></div>';
  var recent=rows.slice().sort(function(a,b){ return (b.date||'').localeCompare(a.date||''); }).slice(0,12);
  var recRows=recent.map(function(x){
    var clr=x.clv>0.01?'#4ade80':(x.clv<-0.01?'#f87171':'#94a3b8');
    var mk=x.clv>0.01?'\u2713':(x.clv<-0.01?'\u2717':'\u00b7');
    return '<div style="display:flex;gap:8px;align-items:center;padding:3px 8px;font-size:.77rem;border-bottom:1px solid #111a2b">'
      +'<span style="color:'+clr+';width:14px">'+mk+'</span>'
      +'<span style="color:#64748b;width:78px;font-family:monospace">'+(x.date||'')+'</span>'
      +'<span style="color:#e2e8f0;flex:1;min-width:120px">'+x.name+'</span>'
      +'<span style="color:#94a3b8;min-width:110px">'+(x.pick||'')+'</span>'
      +'<span style="font-family:monospace;color:#cbd5e1;width:54px;text-align:right">'+_trkAmOdds(x.open)+'</span>'
      +'<span style="color:#475569;width:18px;text-align:center">\u2192</span>'
      +'<span style="font-family:monospace;color:#94a3b8;width:54px;text-align:right">'+_trkAmOdds(x.close)+'</span>'
      +'<span style="font-family:monospace;font-weight:800;color:'+clr+';width:62px;text-align:right">'+(x.clv>=0?'+':'\u2212')+Math.abs(x.clv).toFixed(1)+'%</span>'
      +'</div>';
  }).join('');
  el.innerHTML=ttl+summary
    +'<div style="border:1px solid #1e293b;border-radius:12px;overflow:hidden;margin-bottom:12px">'+catHead+catRows+'</div>'
    +'<details><summary style="cursor:pointer;font-size:.78rem;color:#a78bfa;font-weight:700;padding:4px 2px">Recent picks \u2014 your price vs the close ('+recent.length+')</summary>'
    +'<div style="border:1px solid #1e293b;border-radius:10px;overflow:hidden;margin-top:6px">'+recRows+'</div></details>';
}

// MODEL CHECK (calibration) — does the +EV tag actually win, and do the model
// probabilities match real-world hit rates? Built purely from graded detail.
function _trkRenderCalib(stake){
  var d=window.__TRACK__; var el=document.getElementById('trk-calib'); if(!el||!d) return;
  var CAT_CFG=window.__TRK_CFG__||{};
  var det=(d.detail||[]).filter(function(r){ return !_trkSkipMeta(r)&&!_isOvfCat(r.category)&&!_isHrCat(r.category); });
  var ttl='<div style="font-size:.9rem;color:#e2e8f0;font-weight:800;margin:2px 2px 4px">🔬 Model Check</div>'
    +'<div style="font-size:.74rem;color:#64748b;margin:0 2px 10px">Is the +EV badge real, and do the model probabilities match what actually happens? This proves (or exposes) the model from your own results.</div>';
  var withEv=det.filter(function(r){ return r.ev!=null; });
  if(!withEv.length){
    el.innerHTML=ttl+'<p style="color:#64748b;font-size:.8rem;padding:4px 2px;border:1px dashed #1e293b;border-radius:10px">No model data yet. Calibration builds from graded days going forward (each pick now stores its EV + predicted probability). Check back after a few slates go Final.</p>';
    return;
  }
  // Part A: +EV vs no-edge
  var B={pos:{w:0,l:0,net:0,cnt:0},neg:{w:0,l:0,net:0,cnt:0}};
  withEv.forEach(function(r){
    var b=r.ev>0?B.pos:B.neg, win=r.result==='WIN';
    if(win)b.w++; else b.l++;
    var pl=_amProfit(r.odds,stake,win); if(pl!==null){ b.net+=pl; b.cnt++; }
  });
  function _aRow(lbl,b,hi){
    var n=b.w+b.l, wp=n?b.w/n*100:0, roi=b.cnt?b.net/(b.cnt*stake)*100:0;
    var rc=roi>=0?'#4ade80':'#f87171';
    return '<div style="display:flex;align-items:center;padding:9px 12px;border-bottom:1px solid #131c2e;font-size:.84rem;'+(hi?'background:rgba(34,197,94,.06)':'')+'">'
      +'<span style="flex:1;color:#e2e8f0;font-weight:700">'+lbl+'</span>'
      +'<span style="width:54px;text-align:right;color:#94a3b8;font-family:monospace">'+n+'</span>'
      +'<span style="width:72px;text-align:right;color:#cbd5e1;font-family:monospace">'+b.w+'-'+b.l+'</span>'
      +'<span style="width:60px;text-align:right;color:#cbd5e1;font-family:monospace">'+wp.toFixed(0)+'%</span>'
      +'<span style="width:72px;text-align:right;font-family:monospace;font-weight:800;color:'+rc+'">'+(roi>=0?'+':'\u2212')+Math.abs(roi).toFixed(1)+'%</span>'
      +'</div>';
  }
  var aHead='<div style="display:flex;padding:6px 12px;background:#0c1829;border-bottom:1px solid #1e293b;font-size:.64rem;color:#64748b;font-weight:700;text-transform:uppercase">'
    +'<span style="flex:1">Group</span><span style="width:54px;text-align:right">Picks</span><span style="width:72px;text-align:right">Record</span><span style="width:60px;text-align:right">Win%</span><span style="width:72px;text-align:right">ROI</span></div>';
  var posRoi=B.pos.cnt?B.pos.net/(B.pos.cnt*stake)*100:0, negRoi=B.neg.cnt?B.neg.net/(B.neg.cnt*stake)*100:0;
  var posN=B.pos.w+B.pos.l, negN=B.neg.w+B.neg.l;
  var verdict;
  if(!posN){ verdict='No +EV picks graded yet.'; }
  else if(posRoi>negRoi+1){ verdict='\u2713 +EV picks outperform no-edge picks \u2014 the model has real signal.'; }
  else { verdict='\u26a0 +EV picks are not beating no-edge picks yet \u2014 treat the badge with caution on this sample.'; }
  var partA='<div style="font-size:.78rem;color:#cbd5e1;font-weight:800;margin:2px 2px 6px">Does the +EV tag beat no-edge?</div>'
    +'<div style="border:1px solid #1e293b;border-radius:12px;overflow:hidden">'+aHead+_aRow('\u2713 +EV picks',B.pos,true)+_aRow('\u2013 No edge',B.neg,false)+'</div>'
    +'<div style="font-size:.76rem;color:'+(posN&&posRoi>negRoi+1?'#4ade80':'#facc15')+';padding:8px 2px 14px">'+verdict+'</div>';
  // Part B: predicted vs actual
  var pbDef=[{lo:0.70,hi:1.01,lbl:'70%+'},{lo:0.60,hi:0.70,lbl:'60\u201369%'},{lo:0.50,hi:0.60,lbl:'50\u201359%'},{lo:0.40,hi:0.50,lbl:'40\u201349%'},{lo:0,hi:0.40,lbl:'<40%'}];
  var withP=withEv.filter(function(r){ return r.ev_prob!=null; });
  var partB='';
  if(withP.length){
    var brows=pbDef.map(function(bk){
      var inb=withP.filter(function(r){ return r.ev_prob>=bk.lo&&r.ev_prob<bk.hi; });
      if(!inb.length) return '';
      var pred=inb.reduce(function(s,r){ return s+r.ev_prob; },0)/inb.length*100;
      var act=inb.filter(function(r){ return r.result==='WIN'; }).length/inb.length*100;
      var gap=Math.abs(pred-act), mk=gap<=6?'\u2713':(gap<=12?'~':'\u26a0'), mc=gap<=6?'#4ade80':(gap<=12?'#facc15':'#f87171');
      return '<div style="display:flex;align-items:center;padding:7px 12px;border-bottom:1px solid #131c2e;font-size:.82rem">'
        +'<span style="width:70px;color:#e2e8f0;font-weight:700">'+bk.lbl+'</span>'
        +'<span style="flex:1;color:#64748b;font-size:.72rem">model said \u2248'+pred.toFixed(0)+'%</span>'
        +'<span style="width:96px;text-align:right;color:#cbd5e1;font-family:monospace">hit '+act.toFixed(0)+'%</span>'
        +'<span style="width:78px;text-align:right;color:#94a3b8;font-family:monospace">('+inb.length+' pk)</span>'
        +'<span style="width:24px;text-align:right;color:'+mc+'">'+mk+'</span>'
        +'</div>';
    }).join('');
    partB='<div style="font-size:.78rem;color:#cbd5e1;font-weight:800;margin:14px 2px 6px">Predicted probability vs actual hit rate</div>'
      +'<div style="border:1px solid #1e293b;border-radius:12px;overflow:hidden">'+brows+'</div>';
  }
  // Part C: by-category +EV ROI
  var cc={};
  withEv.forEach(function(r){ if(!(r.ev>0)) return; var k=(r.category||'')+'|'+(r.side||'OVER');
    var c=cc[k]=cc[k]||{w:0,l:0,net:0,cnt:0}; var win=r.result==='WIN'; if(win)c.w++; else c.l++;
    var pl=_amProfit(r.odds,stake,win); if(pl!==null){ c.net+=pl; c.cnt++; } });
  var ckeys=Object.keys(cc).filter(function(k){ return cc[k].cnt>0; })
    .map(function(k){ return [k,cc[k],cc[k].net/(cc[k].cnt*stake)*100]; }).sort(function(a,b){ return b[2]-a[2]; });
  var partC='';
  if(ckeys.length){
    var crows=ckeys.map(function(x){
      var cfg=CAT_CFG[x[0]]||{lbl:x[0].split('|').join(' '),icon:'📊'};
      var roi=x[2], clr=roi>=0?'#4ade80':'#f87171', tag=roi>=3?'trust it':(roi>=-3?'marginal':'fade it');
      return '<div style="display:flex;align-items:center;padding:7px 12px;border-bottom:1px solid #131c2e;font-size:.82rem">'
        +'<span style="flex:1;min-width:150px;color:#e2e8f0;font-weight:600">'+(cfg.icon||'')+' '+cfg.lbl+'</span>'
        +'<span style="width:72px;text-align:right;color:#94a3b8;font-family:monospace">'+x[1].w+'-'+x[1].l+'</span>'
        +'<span style="width:74px;text-align:right;font-family:monospace;font-weight:800;color:'+clr+'">'+(roi>=0?'+':'\u2212')+Math.abs(roi).toFixed(1)+'%</span>'
        +'<span style="width:74px;text-align:right;color:'+clr+';font-size:.72rem">'+tag+'</span>'
        +'</div>';
    }).join('');
    partC='<div style="font-size:.78rem;color:#cbd5e1;font-weight:800;margin:14px 2px 6px">Where the +EV edge actually lives (by category)</div>'
      +'<div style="border:1px solid #1e293b;border-radius:12px;overflow:hidden">'+crows+'</div>';
  }
  el.innerHTML=ttl+partA+partB+partC;
}
function downloadTrackEarningsCSV(){
  var d=window.__TRACK__; if(!d){ alert('Open Track Record first.'); return; }
  var det=(d.detail||[]).filter(function(r){ return !_isHrCat(r.category); });
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
  if(which==='daily'){
    // One row per graded pick (long & narrow) \u2014 mirrors the Results CSV
    // instead of a 20-column-wide per-day pivot.
    rows=[['Date','Day','Category','Side','Player','Pick','Odds','Actual','Result']];
    (d.detail||[]).forEach(function(r){
      if(_isHrCat(r.category)) return;
      var dow='';
      try{ var dt=new Date(r.date+'T12:00:00'); dow=DAYS[dt.getDay()]||''; }catch(e){}
      rows.push([r.date,dow,r.category||'',r.side||'',r.name||'',r.pick||'',
                 (r.odds!=null?r.odds:''), (r.actual!=null?r.actual:''), r.result||'']);
    });
  } else {
    rows=[['Category','Side','Wins','Losses','Win %']];
    var full=window.__TRACK_ALLTIME_FULL__;
    if(full&&full.length){
      full.forEach(function(r){ var n=r.wins+r.losses; rows.push([r.label,r.side,r.wins,r.losses, n?(r.wins/n*100).toFixed(1):'']); });
    } else {
      (d.alltime||[]).forEach(function(r){ var n=r.wins+r.losses; rows.push([r.category,r.side,r.wins,r.losses, n?(r.wins/n*100).toFixed(1):'']); });
    }
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
// ── Rotation Order editor (admin-only) ─────────────────────────────────
// Loads today's per-team rotation, lets an admin pin the true SP1..SPn order
// with up/down arrows, and saves it to Supabase. SP1-2 = ace, SP3-4 = mid,
// SP5+ = back-end. Reorder marks a team dirty; Save sends every dirty/pinned
// team plus any teams reset to auto. Server merges so off-slate pins survive.
async function loadRotation(){
  var st=document.getElementById('rot-status');
  st.textContent='Loading rotations...'; st.style.color='#9ca3af';
  var ds=document.getElementById('date-picker').value||'';
  try{
    var url='/api/rotation'+_betAuthQS()+(ds?('&date_str='+encodeURIComponent(ds)):'');
    var r=await fetch(url);
    if(!r.ok){ var t=await r.text(); throw new Error(t||'load failed'); }
    var d=await r.json();
    window.__ROT__={reset:[],teams:(d.teams||[]).map(function(t){
      return {team_id:String(t.team_id),team_name:t.team_name||String(t.team_id),
        has_override:!!t.has_override,dirty:false,collapsed:false,
        pitchers:(t.pitchers||[]).map(function(p){return {id:p.id,name:p.name||String(p.id),tier:p.tier||0};}),
        injured:(t.injured||[]).map(function(p){return {id:p.id,name:p.name||String(p.id)};})};
    })};
    _rotRender();
    var box=document.getElementById('rotation-list'); if(box) box.style.display='flex';
    var cb=document.getElementById('rot-collapse-btn'); if(cb) cb.textContent='Collapse all';
    st.textContent=window.__ROT__.teams.length+' teams loaded for '+(d.date||ds);
    st.style.color='#9ca3af';
  }catch(e){ st.textContent='Could not load: '+((e&&e.message)||e); st.style.color='#f87171'; }
}
function _rotRender(){
  var box=document.getElementById('rotation-list'); if(!box) return;
  var R=window.__ROT__; if(!R){ box.innerHTML=''; return; }
  box.innerHTML=R.teams.map(function(t,ti){
    var rows=t.pitchers.map(function(p,pi){
      var rank=pi+1;
      var et=(p.tier&&p.tier>0)?p.tier:(rank<=2?1:rank<=4?2:3);
      var clr=et===1?'#34d399':et===2?'#fbbf24':'#f87171';
      var bg=et===1?'rgba(16,185,129,.15)':et===2?'rgba(245,158,11,.15)':'rgba(239,68,68,.15)';
      var up=pi===0?'<span style="width:26px;display:inline-block"></span>':'<button onclick="_rotMove('+ti+','+pi+',-1)" style="background:#1f2937;color:#fff;border:none;border-radius:5px;width:26px;height:26px;cursor:pointer;font-size:.8rem">&#9650;</button>';
      var dn=pi===t.pitchers.length-1?'<span style="width:26px;display:inline-block"></span>':'<button onclick="_rotMove('+ti+','+pi+',1)" style="background:#1f2937;color:#fff;border:none;border-radius:5px;width:26px;height:26px;cursor:pointer;font-size:.8rem">&#9660;</button>';
      var inj='<button onclick="_rotToInj('+ti+','+pi+')" title="Move to Injured / Out &#8212; drops this arm out of the rotation count so the rest re-rank" style="background:#3f1d1d;color:#fca5a5;border:1px solid #7f1d1d;border-radius:5px;height:26px;padding:0 8px;cursor:pointer;font-size:.62rem;font-weight:800;white-space:nowrap">INJ &#8595;</button>';
      var tsel='<select onchange="_rotTier('+ti+','+pi+',this.value)" title="Tier shown on the cards \u2014 independent of the SP order. Auto = SP1-2 ace, SP3-4 mid, SP5+ back-end." style="background:#0b0f17;border:1px solid #334155;border-radius:5px;color:'+clr+';height:26px;font-size:.66rem;font-weight:800;padding:0 4px;cursor:pointer">'
        +'<option value="0"'+(p.tier?'':' selected')+' style="color:#e5e7eb">Auto</option>'
        +'<option value="1"'+(p.tier===1?' selected':'')+' style="color:#e5e7eb">Ace</option>'
        +'<option value="2"'+(p.tier===2?' selected':'')+' style="color:#e5e7eb">Mid</option>'
        +'<option value="3"'+(p.tier===3?' selected':'')+' style="color:#e5e7eb">Back</option>'
        +'</select>';
      return '<div style="display:flex;align-items:center;gap:8px;padding:4px 0">'
        +'<span style="font-size:.62rem;font-weight:900;padding:2px 7px;border-radius:5px;background:'+bg+';color:'+clr+';min-width:40px;text-align:center">SP'+rank+'</span>'
        +'<span style="flex:1;color:#e5e7eb;font-size:.9rem">'+p.name+'</span>'
        +'<span style="display:flex;gap:5px;align-items:center">'+tsel+up+dn+inj+'</span></div>';
    }).join('');
    var injRows=(t.injured||[]).map(function(p,ii){
      return '<div style="display:flex;align-items:center;gap:8px;padding:3px 0">'
        +'<span style="font-size:.6rem;font-weight:900;padding:2px 7px;border-radius:5px;background:rgba(127,29,29,.35);color:#fca5a5;min-width:40px;text-align:center">INJ</span>'
        +'<span style="flex:1;color:#9ca3af;font-size:.88rem;text-decoration:line-through">'+p.name+'</span>'
        +'<button onclick="_rotFromInj('+ti+','+ii+')" title="Return to the active rotation" style="background:#14321f;color:#86efac;border:1px solid #166534;border-radius:5px;height:26px;padding:0 9px;cursor:pointer;font-size:.62rem;font-weight:800;white-space:nowrap">&#8593; Active</button></div>';
    }).join('');
    var injBlock=(t.injured&&t.injured.length)?('<div style="margin-top:8px;padding-top:8px;border-top:1px dashed #3f3f46"><div style="font-size:.58rem;color:#f87171;font-weight:800;letter-spacing:.06em;margin-bottom:3px">INJURED / OUT &#8212; not ranked</div>'+injRows+'</div>'):'';
    var tag=t.has_override?'<span style="font-size:.6rem;color:#34d399;font-weight:800;margin-left:8px">PINNED</span>':(t.dirty?'<span style="font-size:.6rem;color:#fbbf24;font-weight:800;margin-left:8px">EDITED</span>':'');
    var resetLink=(t.has_override||t.dirty)?'<a onclick="event.stopPropagation();_rotReset('+ti+')" style="color:#ff8a65;cursor:pointer;font-size:.7rem;font-weight:700">Reset to auto</a>':'';
    var addBlock='<div style="margin-top:10px;padding-top:8px;border-top:1px dashed #3f3f46;display:flex;gap:6px;align-items:center;flex-wrap:wrap">'
      +'<input id="rotadd-'+ti+'" type="text" placeholder="Add a starter by name" onkeydown="if(event.key===&#39;Enter&#39;){_rotSearch('+ti+');}" style="flex:1;min-width:160px;background:#0b0f17;border:1px solid #334155;border-radius:6px;color:#e5e7eb;padding:6px 9px;font-size:.82rem" />'
      +'<button onclick="_rotSearch('+ti+')" style="background:#1d4ed8;color:#fff;border:none;border-radius:6px;height:30px;padding:0 12px;cursor:pointer;font-size:.74rem;font-weight:800">Search</button>'
      +'<div id="rotres-'+ti+'" style="width:100%"></div></div>';
    var chev=t.collapsed?'&#9656;':'&#9662;';   // collapsed = right arrow, open = down arrow
    var summ=t.collapsed?('<span style="font-size:.7rem;color:#64748b;margin-left:8px;font-weight:600">'+t.pitchers.length+' SP'+((t.injured&&t.injured.length)?(' &middot; '+t.injured.length+' INJ'):'')+'</span>'):'';
    var body=t.collapsed?'':((rows||'<div style="color:#64748b;font-size:.8rem">No active starters.</div>')+injBlock+addBlock);
    return '<div style="background:rgba(255,255,255,.03);border:1px solid #262626;border-radius:10px;padding:12px 14px">'
      +'<div onclick="_rotToggle('+ti+')" title="Click to expand or collapse this team" style="display:flex;justify-content:space-between;align-items:center;cursor:pointer;'+(t.collapsed?'':'margin-bottom:6px')+'">'
      +'<div style="font-weight:800;color:#fff;font-size:.95rem"><span style="color:#94a3b8;margin-right:7px;font-size:.78rem">'+chev+'</span>'+t.team_name+tag+summ+'</div>'+resetLink+'</div>'
      +body+'</div>';
  }).join('')||'<div style="color:#64748b;font-size:.85rem">No teams on this date. Pick a slate date and Load again.</div>';
}
function _rotMove(ti,pi,dir){
  var R=window.__ROT__; if(!R) return;
  var t=R.teams[ti]; if(!t) return;
  var j=pi+dir; if(j<0||j>=t.pitchers.length) return;
  var tmp=t.pitchers[pi]; t.pitchers[pi]=t.pitchers[j]; t.pitchers[j]=tmp;
  t.dirty=true;
  var ri=(R.reset||[]).indexOf(t.team_id); if(ri>=0) R.reset.splice(ri,1);
  _rotRender();
}
function _rotTier(ti,pi,val){
  var R=window.__ROT__; if(!R) return;
  var t=R.teams[ti]; if(!t) return;
  var p=t.pitchers[pi]; if(!p) return;
  p.tier=parseInt(val,10)||0; t.dirty=true;
  var ri=(R.reset||[]).indexOf(t.team_id); if(ri>=0) R.reset.splice(ri,1);
  _rotRender();
}
function _rotToggle(ti){
  var R=window.__ROT__; if(!R) return;
  var t=R.teams[ti]; if(!t) return;
  t.collapsed=!t.collapsed;
  _rotRender();
}
function _rotCollapseAll(){
  var box=document.getElementById('rotation-list'); if(!box) return;
  var b=document.getElementById('rot-collapse-btn');
  var hidden=(box.style.display==='none');
  if(hidden){ box.style.display='flex'; if(b) b.textContent='Collapse all'; }
  else { box.style.display='none'; if(b) b.textContent='Expand all'; }
}
function _rotToInj(ti,pi){
  var R=window.__ROT__; if(!R) return;
  var t=R.teams[ti]; if(!t) return;
  var p=t.pitchers.splice(pi,1)[0]; if(!p) return;
  t.injured=t.injured||[]; t.injured.push(p); t.dirty=true;
  var ri=(R.reset||[]).indexOf(t.team_id); if(ri>=0) R.reset.splice(ri,1);
  _rotRender();
}
function _rotFromInj(ti,ii){
  var R=window.__ROT__; if(!R) return;
  var t=R.teams[ti]; if(!t) return;
  var p=(t.injured||[]).splice(ii,1)[0]; if(!p) return;
  t.pitchers.push(p); t.dirty=true;
  var ri=(R.reset||[]).indexOf(t.team_id); if(ri>=0) R.reset.splice(ri,1);
  _rotRender();
}
function _rotReset(ti){
  var R=window.__ROT__; if(!R) return;
  var t=R.teams[ti]; if(!t) return;
  if(t.injured&&t.injured.length){ t.pitchers=t.pitchers.concat(t.injured); t.injured=[]; }
  t.pitchers.forEach(function(p){ p.tier=0; });
  t.dirty=false; t.has_override=false;
  R.reset=R.reset||[];
  if(R.reset.indexOf(t.team_id)<0) R.reset.push(t.team_id);
  _rotRender();
}
async function _rotSearch(ti){
  var R=window.__ROT__; if(!R) return;
  var t=R.teams[ti]; if(!t) return;
  var inp=document.getElementById('rotadd-'+ti);
  var res=document.getElementById('rotres-'+ti);
  var q=inp?(inp.value||'').trim():'';
  if(q.length<3){ if(res) res.innerHTML='<span style="color:#f87171;font-size:.72rem">Type at least 3 letters.</span>'; return; }
  if(res) res.innerHTML='<span style="color:#9ca3af;font-size:.72rem">Searching...</span>';
  try{
    var r=await fetch('/api/rotation/search'+_betAuthQS()+'&q='+encodeURIComponent(q));
    if(!r.ok){ var tx=await r.text(); throw new Error(tx||'search failed'); }
    var d=await r.json(); var ps=d.players||[];
    window.__ROTRES__=window.__ROTRES__||{}; window.__ROTRES__[ti]=ps;
    if(!ps.length){ if(res) res.innerHTML='<span style="color:#9ca3af;font-size:.72rem">No pitchers found.</span>'; return; }
    if(res) res.innerHTML='<div style="display:flex;flex-direction:column;gap:4px;margin-top:6px">'+ps.map(function(p,idx){
      var team=p.team?('<span style="color:#64748b"> &#8212; '+p.team+'</span>'):'';
      return '<button onclick="_rotAdd('+ti+','+idx+')" style="text-align:left;background:#0b0f17;border:1px solid #334155;border-radius:6px;color:#e5e7eb;padding:6px 9px;cursor:pointer;font-size:.8rem"><b style="color:#34d399">&#43;</b> '+p.name+team+'</button>';
    }).join('')+'</div>';
  }catch(e){ if(res) res.innerHTML='<span style="color:#f87171;font-size:.72rem">Search failed: '+((e&&e.message)||e)+'</span>'; }
}
function _rotAdd(ti,idx){
  var R=window.__ROT__; if(!R) return;
  var t=R.teams[ti]; if(!t) return;
  var ps=(window.__ROTRES__||{})[ti]||[]; var p=ps[idx]; if(!p) return;
  var res=document.getElementById('rotres-'+ti);
  var dup=t.pitchers.some(function(x){return String(x.id)===String(p.id);})
       ||(t.injured||[]).some(function(x){return String(x.id)===String(p.id);});
  if(dup){ if(res) res.innerHTML='<span style="color:#fbbf24;font-size:.72rem">Already in this rotation.</span>'; return; }
  t.pitchers.push({id:p.id,name:p.name}); t.dirty=true;
  var ri=(R.reset||[]).indexOf(t.team_id); if(ri>=0) R.reset.splice(ri,1);
  _rotRender();
}
async function saveRotation(){
  var R=window.__ROT__;
  var st=document.getElementById('rot-status');
  var btn=document.getElementById('rot-save-btn');
  if(!R){ st.textContent='Load rotations first.'; st.style.color='#f87171'; return; }
  var set={};
  R.teams.forEach(function(t){
    if(t.dirty||t.has_override){
      var tier={};
      t.pitchers.forEach(function(p){ if(p.tier&&p.tier>0) tier[String(p.id)]=p.tier; });
      set[t.team_id]={order:t.pitchers.map(function(p){return {id:p.id,name:p.name};}),
                      inj:(t.injured||[]).map(function(p){return {id:p.id,name:p.name};}),
                      tier:tier};
    }
  });
  var reset=(R.reset||[]).filter(function(tid){ return !set[tid]; });
  var orig=btn.textContent; btn.disabled=true; btn.textContent='Saving...';
  st.textContent='Saving...'; st.style.color='#9ca3af';
  try{
    var r=await fetch('/api/rotation'+_betAuthQS(),{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({set:set,reset:reset})});
    if(!r.ok){ var t=await r.text(); throw new Error(t||'save failed'); }
    var d=await r.json();
    R.teams.forEach(function(t){ if(t.dirty){ t.has_override=true; t.dirty=false; } });
    R.reset=[];
    _rotRender();
    st.textContent='Saved '+(d.teams!=null?d.teams:'')+' team overrides. Now click Force Refresh to apply to today\u2019s cards.';
    st.style.color='#34d399';
  }catch(e){ st.textContent='Save failed: '+((e&&e.message)||e); st.style.color='#f87171'; }
  finally{ btn.disabled=false; btn.textContent=orig; }
}
// Builds the "＋ Track Bet" control (admin-only). Registers the pick in
// __BET_SRC__ and opens the stake form. No line ⇒ no button (can't grade).
function _betBtn(p,cat,side,statKey,statLabel,line,odds){
  if(!(window.IS_ADMIN||window.IS_TESTER)) return '';
  if(line==null||!side||!statKey) return '';
  window.__BET_SRC__=window.__BET_SRC__||{}; window.__BET_N__=(window.__BET_N__||0)+1;
  var k='bs'+window.__BET_N__;
  window.__BET_SRC__[k]={name:(p.full_name||p.name||''),team:(p.team||''),opp:(p.opp||''),
    category:cat,side:side,stat_key:statKey,stat_label:statLabel,line:line,
    odds:(odds!=null?odds:null),edge:(p.edge!=null?p.edge:0),date:((window._lastResult&&window._lastResult.date)||'')};
  return `<div style="display:flex;flex-direction:row;align-items:stretch;border-top:1px solid #1e293b;flex-shrink:0;width:100%;box-sizing:border-box">
    <button onclick="event.stopPropagation();_betForm('${k}')" style="width:50%;box-sizing:border-box;background:#1a1740;color:#a5b4fc;border:none;border-right:1px solid #1e293b;padding:5px 0;font-size:.72rem;font-weight:800;cursor:pointer;white-space:nowrap;text-align:center">Track Bet</button>
    <button onclick="event.stopPropagation();_addToCart('${k}')" style="width:50%;box-sizing:border-box;background:#0d2318;color:#6ee7b7;border:none;padding:5px 0;font-size:.72rem;font-weight:800;cursor:pointer;white-space:nowrap;text-align:center">+ Parlay</button>
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
      +'<button onclick="event.stopPropagation();_removeFromCart(&#39;'+l._key+'&#39;)" style="background:none;border:none;color:#f87171;cursor:pointer;font-size:.75rem;padding:0 1px;line-height:1">\u2715</button>'
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
function _mcatsList(){
  return [
    {label:'Hits Over 0.5',        cat:'Hitter Hits',   sk:'hits',         sl:'Hits',          line:0.5, side:'OVER'},
    {label:'Hits Under 1.5',       cat:'Hits Under',    sk:'hits',         sl:'Hits',          line:1.5, side:'UNDER'},
    {label:'TB Over 1.5',          cat:'TB Over',       sk:'total_bases',  sl:'Total Bases',   line:1.5, side:'OVER'},
    {label:'TB Under 1.5',         cat:'TB Under',      sk:'total_bases',  sl:'Total Bases',   line:1.5, side:'UNDER'},
    {label:'Runs Over 0.5',        cat:'Runs',          sk:'runs',         sl:'Runs',          line:0.5, side:'OVER'},
    {label:'Runs Under 0.5',       cat:'Runs',          sk:'runs',         sl:'Runs',          line:0.5, side:'UNDER'},
    {label:'RBI Over 0.5',         cat:'RBI',           sk:'rbi',          sl:'RBI',           line:0.5, side:'OVER'},
    {label:'RBI Under 0.5',        cat:'RBI',           sk:'rbi',          sl:'RBI',           line:0.5, side:'UNDER'},
    {label:'HR Over 0.5',          cat:'HR',            sk:'homeRuns',     sl:'HR',            line:0.5, side:'OVER'},
    {label:'HR Under 0.5',         cat:'HR',            sk:'homeRuns',     sl:'HR',            line:0.5, side:'UNDER'},
    {label:'HRR Over 1.5',         cat:'HRR',           sk:'hrr',          sl:'H+R+RBI',       line:1.5, side:'OVER'},
    {label:'HRR Under 1.5',        cat:'HRR',           sk:'hrr',          sl:'H+R+RBI',       line:1.5, side:'UNDER'},
    {label:'Batter Walks Over 0.5',cat:'Batter Walks',  sk:'walks_bat',    sl:'Walks',         line:0.5, side:'OVER'},
    {label:'Batter Walks Under 0.5',cat:'Batter Walks', sk:'walks_bat',    sl:'Walks',         line:0.5, side:'UNDER'},
    {label:'Pitcher K Over',       cat:'Pitcher K',     sk:'strikeOuts',   sl:'Strikeouts',    line:null,side:'OVER'},
    {label:'Pitcher K Under',      cat:'Pitcher K',     sk:'strikeOuts',   sl:'Strikeouts',    line:null,side:'UNDER'},
    {label:'Hits Allowed Over',    cat:'Pitcher Props', sk:'hits_allowed', sl:'Hits Allowed',  line:null,side:'OVER'},
    {label:'Hits Allowed Under',   cat:'Pitcher Props', sk:'hits_allowed', sl:'Hits Allowed',  line:null,side:'UNDER'},
    {label:'Outs Over',            cat:'Pitcher Props', sk:'outs',         sl:'Outs',          line:null,side:'OVER'},
    {label:'Outs Under',           cat:'Pitcher Props', sk:'outs',         sl:'Outs',          line:null,side:'UNDER'},
    {label:'Earned Runs Over',     cat:'Pitcher Props', sk:'earnedRuns',   sl:'Earned Runs',   line:null,side:'OVER'},
    {label:'Earned Runs Under',    cat:'Pitcher Props', sk:'earnedRuns',   sl:'Earned Runs',   line:null,side:'UNDER'},
    {label:'Walks Allowed Over',   cat:'Pitcher Props', sk:'walks',        sl:'Walks Allowed', line:null,side:'OVER'},
    {label:'Walks Allowed Under',  cat:'Pitcher Props', sk:'walks',        sl:'Walks Allowed', line:null,side:'UNDER'}
  ];
}
function _manualBetForm(){
  var MCATS=_mcatsList();
  window.__MCATS__=MCATS;
  var ov=document.getElementById('mbet-modal');
  if(!ov){ ov=document.createElement('div'); ov.id='mbet-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.85);z-index:10000;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){ if(e.target===ov) ov.style.display='none'; };
    document.body.appendChild(ov);
  }
  var today=new Date().toISOString().slice(0,10);
  var opts=MCATS.map(function(c,i){ return '<option value="'+i+'">'+c.label+'</option>'; }).join('');
  var inp='display:block;width:100%;margin-top:5px;background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 11px;color:#fff;font-size:.9rem;box-sizing:border-box';
  ov.innerHTML='<div style="background:#0f172a;border:1px solid #312e81;border-radius:16px;max-width:380px;width:100%;box-shadow:0 20px 60px rgba(0,0,0,.6);max-height:90vh;overflow-y:auto">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;padding:16px 18px;border-bottom:1px solid #1e293b">'
      +'<div style="font-weight:800;color:#fff;font-size:1rem">Manual Bet Entry</div>'
      +'<button onclick="document.getElementById(&#39;mbet-modal&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">&#215;</button>'
    +'</div>'
    +'<div style="padding:16px 18px;display:grid;gap:11px">'
      +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Player Name<input id="mbet-name" type="text" placeholder="e.g. Yordan Alvarez" style="'+inp+'"></label>'
      +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Opponent<input id="mbet-opp" type="text" placeholder="e.g. Red Sox" style="'+inp+'"></label>'
      +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Category<select id="mbet-cat" onchange="_mbetCatChange()" style="'+inp+';color:#e2e8f0">'+opts+'</select></label>'
      +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Line<input id="mbet-line" type="number" step="0.5" min="0" style="'+inp+';font-family:monospace;font-weight:700;font-size:.95rem"></label>'
      +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Odds (American)<input id="mbet-odds" type="number" placeholder="e.g. -150 or +110" style="'+inp+';color:#fbbf24;font-family:monospace;font-weight:700;font-size:.95rem"></label>'
      +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Bet size ($)<input id="mbet-stake" type="number" min="0" step="0.01" placeholder="e.g. 50" style="'+inp+';font-weight:700;font-size:.95rem"></label>'
      +'<label style="font-size:.72rem;color:#94a3b8;font-weight:600">Date<input id="mbet-date" type="date" value="'+today+'" style="'+inp+'"></label>'
      +'<div id="mbet-payout" style="font-size:.78rem;color:#64748b;min-height:1em"></div>'
      +'<div id="mbet-msg" style="font-size:.76rem;color:#f87171;min-height:1em"></div>'
      +'<button id="mbet-save" onclick="_saveManualBet()" style="background:#4338ca;color:#fff;border:none;border-radius:9px;padding:11px;font-weight:800;cursor:pointer;font-size:.92rem">Log Bet</button>'
    +'</div></div>';
  ov.style.display='flex';
  _mbetCatChange();
  var so=document.getElementById('mbet-odds'),ss=document.getElementById('mbet-stake');
  function _calc(){ var o=parseFloat(so.value),s=parseFloat(ss.value),pay=document.getElementById('mbet-payout'); if(!isFinite(o)||!isFinite(s)||s<=0){pay.textContent='';return;} var win=o>0?s*(o/100):s*(100/Math.abs(o)); pay.innerHTML='To win <strong style="color:#4ade80">$'+win.toFixed(2)+'</strong> &middot; total <strong style="color:#cbd5e1">$'+(s+win).toFixed(2)+'</strong>'; }
  so.oninput=_calc; ss.oninput=_calc; _calc();
  setTimeout(function(){ document.getElementById('mbet-name').focus(); },50);
}
function _mbetCatChange(){
  var sel=document.getElementById('mbet-cat'); if(!sel) return;
  var c=(window.__MCATS__||[])[parseInt(sel.value)]; if(!c) return;
  var li=document.getElementById('mbet-line');
  if(li && c.line!=null) li.value=c.line;
}
async function _saveManualBet(){
  var name=(document.getElementById('mbet-name').value||'').trim();
  var opp=(document.getElementById('mbet-opp').value||'').trim();
  var catSel=document.getElementById('mbet-cat');
  var c=(window.__MCATS__||[])[parseInt(catSel?catSel.value:0)];
  var line=parseFloat(document.getElementById('mbet-line').value);
  var o=parseFloat(document.getElementById('mbet-odds').value);
  var s=parseFloat(document.getElementById('mbet-stake').value);
  var dt=(document.getElementById('mbet-date').value||new Date().toISOString().slice(0,10));
  var msg=document.getElementById('mbet-msg');
  if(!name){ msg.textContent='Enter a player name.'; return; }
  if(!c){ msg.textContent='Select a category.'; return; }
  if(!isFinite(line)||line<=0){ msg.textContent='Enter a valid line.'; return; }
  if(!isFinite(o)){ msg.textContent='Enter the odds.'; return; }
  if(!isFinite(s)||s<=0){ msg.textContent='Enter a bet size greater than 0.'; return; }
  var btn=document.getElementById('mbet-save'); btn.disabled=true; btn.textContent='Saving...';
  try{
    var body={name:name,opp:opp,team:'',category:c.cat,stat_key:c.sk,stat_label:c.sl,side:c.side,line:line,odds:Math.round(o),stake:s,date:dt,placed_at:new Date().toISOString()};
    var res=await fetch('/api/bets'+_betAuthQS(),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!res.ok){ throw new Error(await res.text()); }
    document.getElementById('mbet-modal').style.display='none';
    _betToast('Bet logged');
    var mb=document.getElementById('mybets-card');
    if(mb && !mb.classList.contains('hidden')) openMyBets(false);
  }catch(e){ msg.textContent=(e.message||'Save failed'); btn.disabled=false; btn.textContent='Log Bet'; }
}
function _legStatKey(l){
  var lbl=((l.stat||'')+'').toLowerCase().trim();
  var byLabel={'hits':'hits','runs':'runs','total bases':'total_bases','rbi':'rbi',
    'hr':'homeRuns','home runs':'homeRuns','walks':'walks_bat','batter walks':'walks_bat',
    'h+r+rbi':'hrr','ks':'strikeOuts','strikeouts':'strikeOuts','outs':'outs',
    'hits allowed':'hits_allowed','earned runs':'earnedRuns','walks allowed':'walks'};
  if(byLabel[lbl]) return byLabel[lbl];
  var byType={HIT:'hits',HITS:'hits',TSC:'hits',K:'strikeOuts',RUN:'runs',RUNS:'runs',
    RBI:'rbi',HRR:'hrr',HRRSP:'hrr',HR:'homeRuns',BWALK:'walks_bat',TB:'total_bases',
    TBO:'total_bases',TBU:'total_bases',
    pitcher_hits_allowed:'hits_allowed',pitcher_outs:'outs',
    pitcher_earned_runs:'earnedRuns',pitcher_walks:'walks'};
  return byType[l.type]||'';
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
var _MP_TYPES=[['HIT','Hits'],['TB','Total Bases'],['RUN','Runs'],['RBI','RBI'],['HR','Home Runs'],
  ['HRR','Hits + Runs + RBIs'],['BWALK','Batter Walks'],['K','Pitcher Strikeouts'],['PHITS','Pitcher Hits Allowed'],
  ['POUTS','Pitcher Outs'],['PER','Pitcher Earned Runs'],['PWALK','Pitcher Walks Allowed'],['OTHER','Other']];
function _mpStatKey(t){return({HIT:'hits',TB:'total_bases',RUN:'runs',RBI:'rbi',HR:'homeRuns',HRR:'hrr',BWALK:'walks_bat',K:'strikeOuts',PHITS:'hits_allowed',POUTS:'outs',PER:'earnedRuns',PWALK:'walks',UNDER_HITS:'hits'})[t]||'';}
function _mpSide(t){return t==='UNDER_HITS'?'UNDER':'OVER';}
function _mpDefLine(t){var d={HIT:0.5,TB:1.5,RUN:0.5,RBI:0.5,HR:0.5,HRR:1.5,BWALK:0.5,K:5.5,PHITS:5.5,POUTS:17.5,PER:2.5,PWALK:1.5}; return (t in d)?d[t]:null;}
function _mpTypeChange(){var t=(document.getElementById('mp-type')||{}).value; var d=_mpDefLine(t); var le=document.getElementById('mp-line'); if(le&&d!=null) le.value=d;}
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
          +'<select id="mp-type" onchange="_mpTypeChange()" style="background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 10px;color:#f1f5f9;font-size:.82rem">'+typeOpts+'</select>'
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
  if(done&&!name&&window._mpLegs.length>=2){window._mpPhase=2;_mpRender();return;}
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
// _DOW_SIG / _DOW_IDX defined in the strategy-chart script block near the top.
function _dowChip(mkt,pickDir){
  var day=_slateDay();
  var idx=_DOW_IDX[mkt]; if(idx===undefined) return '';
  var sig=(_DOW_SIG[day]||[])[idx]; if(!sig) return '';
  var match=sig===(pickDir||'').toUpperCase().charAt(0);
  var dn=['SUN','MON','TUE','WED','THU','FRI','SAT'][day];
  return match
    ?'<span style="font-size:.61rem;background:rgba(74,222,128,.1);border:1px solid rgba(74,222,128,.28);color:#4ade80;border-radius:5px;padding:1px 6px;letter-spacing:.04em;font-weight:700">'+dn+' \u2714</span>'
    :'<span style="font-size:.61rem;background:rgba(251,191,36,.07);border:1px solid rgba(251,191,36,.22);color:#fbbf24;border-radius:5px;padding:1px 6px;letter-spacing:.04em;font-weight:700">'+dn+' \u2195</span>';
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
function _resColor(r){ return r==='WIN'?'#4ade80':(r==='LOSS'?'#f87171':(r==='PUSH'?'#facc15':(r==='VOID'?'#38bdf8':'#94a3b8'))); }
function _statBox(lbl,val,clr){ return '<div style="background:#111;border-radius:10px;padding:10px 14px;min-width:92px"><div style="font-size:.64rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">'+lbl+'</div><div style="font-size:1.12rem;font-weight:800;color:'+(clr||'#e2e8f0')+'">'+val+'</div></div>'; }
function renderMyBets(d){
  var bets=d.bets||[];
  var singles=bets.filter(function(b){ return b.bet_type!=='parlay'; });
  var parlays=bets.filter(function(b){ return b.bet_type==='parlay'; });
  var tab=(window.__MYBETS_TAB__==='parlays')?'parlays':'singles'; window.__MYBETS_TAB__=tab;
  var activeAll=(tab==='parlays')?parlays:singles;
  var s=_mbSumm(activeAll);
  var _bdates={}; activeAll.forEach(function(b){ if(b.date) _bdates[b.date]=1; });
  var _dlist=Object.keys(_bdates).sort();
  var _maxd=_dlist.length?_dlist[_dlist.length-1]:_trkTodayISO();
  var selDate=window.__MYBETS_DATE__;
  if(!selDate||!_bdates[selDate]) selDate=_maxd;
  window.__MYBETS_DATE__=selDate;
  var shownBets=activeAll.filter(function(b){ return b.date===selDate; });
  var roiTxt=s.roi!=null?((s.roi>0?'+':'')+s.roi+'%'):'—';
  var roiClr=s.roi==null?'#94a3b8':(s.roi>0?'#4ade80':(s.roi<0?'#f87171':'#facc15'));
  var netClr=(s.profit||0)>0?'#4ade80':((s.profit||0)<0?'#f87171':'#cbd5e1');
  var recTxt=(s.wins||0)+'-'+(s.losses||0)+(s.push?('-'+s.push+'P'):'');
  var tabBtn=function(id,lab,cnt){ var on=(tab===id); return '<button onclick="_myBetsTab(&#39;'+id+'&#39;)" style="padding:7px 16px;border-radius:9px;border:1px solid '+(on?'#818cf8':'#334155')+';background:'+(on?'rgba(129,140,248,.14)':'transparent')+';color:'+(on?'#c7d2fe':'#94a3b8')+';font-weight:800;font-size:.8rem;cursor:pointer">'+lab+' <span style="font-size:.7rem;color:'+(on?'#a5b4fc':'#64748b')+'">('+cnt+')</span></button>'; };
  var tabBar='<div style="display:flex;gap:8px;margin-bottom:16px">'+tabBtn('singles','Singles',singles.length)+tabBtn('parlays','Parlays',parlays.length)+'</div>';
  var head='<div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin-bottom:18px">'
    +_statBox('Record',recTxt,'#e2e8f0')
    +_statBox('Pending',(s.pending||0),'#94a3b8')
    +(((s.void||0)>0)?_statBox('Void',(s.void||0),'#38bdf8'):'')
    +_statBox('Staked',_money(s.staked||0),'#cbd5e1')
    +_statBox('Net',_money(s.profit||0),netClr)
    +_statBox('Returned',_money(s.returned||0),'#cbd5e1')
    +_statBox('ROI',roiTxt,roiClr)
    +'<div style="margin-left:auto"><button onclick="downloadMyBetsCSV()" style="background:#4338ca;color:#fff;border:none;border-radius:8px;padding:8px 12px;font-size:.78rem;font-weight:700;cursor:pointer">⬇ CSV</button></div>'
    +'</div>';
  var bcHtml='';
  if(tab==='singles'){
    var bc=(((d.summary&&d.summary.by_category)||[]).filter(function(c){ return (c.category||'')!=='Parlay'; })).map(function(c){
      var croi=c.roi!=null?((c.roi>0?'+':'')+c.roi+'%'):'—';
      var cclr=c.roi==null?'#94a3b8':(c.roi>0?'#4ade80':(c.roi<0?'#f87171':'#facc15'));
      return '<tr><td style="font-weight:600">'+(c.label||c.category)+'</td>'
        +'<td style="font-family:monospace">'+c.wins+'-'+c.losses+(c.push?('-'+c.push+'P'):'')+'</td>'
        +'<td style="font-family:monospace;color:#94a3b8">'+(c.pending||0)+'</td>'
        +'<td style="font-family:monospace">'+_money(c.staked)+'</td>'
        +'<td style="font-family:monospace;color:'+((c.profit||0)>=0?'#4ade80':'#f87171')+'">'+_money(c.profit)+'</td>'
        +'<td style="font-family:monospace;font-weight:700;color:'+cclr+'">'+croi+'</td></tr>';
    }).join('');
    bcHtml=bc?'<div style="overflow-x:auto;margin-bottom:18px"><table class="grade-table"><thead><tr><th>Category</th><th>W-L</th><th>Pend</th><th>Staked</th><th>Net</th><th>ROI</th></tr></thead><tbody>'+bc+'</tbody></table></div>':'';
  }
  var rows=shownBets.map(function(b){
    var res=b.result||'pending';
    var delBtn='<button onclick="_deleteBet(&#39;'+b.id+'&#39;)" title="Remove" style="background:none;border:none;color:#64748b;cursor:pointer;font-size:1rem">\u2716</button>';
    var editSel='<select onchange="_setBetResult(&#39;'+b.id+'&#39;,this.value)" title="Manually set this result" style="background:#0b1120;border:1px solid #334155;color:#cbd5e1;border-radius:6px;padding:3px 5px;font-size:.7rem;cursor:pointer;margin-right:6px">'
      +'<option value="">edit\u2026</option>'
      +'<option value="WIN">Win</option>'
      +'<option value="LOSS">Loss</option>'
      +'<option value="PUSH">Push</option>'
      +'<option value="VOID">Void</option>'
      +'<option value="PENDING">Pending</option>'
      +'</select>';
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
      return '<tr onclick="var e=document.getElementById(&#39;'+lid+'&#39;);e.style.display=e.style.display===&#39;none&#39;?&#39;table-row&#39;:&#39;none&#39;" style="cursor:pointer">'
        +'<td style="white-space:nowrap;color:#94a3b8;font-family:monospace;font-size:.76rem">'+(b.date||'')+'</td>'
        +'<td style="font-weight:700;color:#fbbf24">'+n+'-Leg Parlay <span style="font-size:.66rem;color:#475569;font-weight:400">&#9658; expand</span></td>'
        +'<td style="font-size:.78rem;color:#64748b">Combined</td>'
        +'<td style="font-family:monospace">'+_betOddsDisp(b.odds)+'</td>'
        +'<td style="font-family:monospace">'+_money(b.stake)+'</td>'
        +'<td style="font-weight:800;color:'+_resColor(res)+'">'+(res==='pending'?'pending':res)+(b.manual?' <span title="Set manually" style="color:#fbbf24;font-size:.72rem">\u270e</span>':'')+'</td>'
        +'<td style="font-family:monospace;font-weight:700;color:'+((b.profit||0)>=0?'#4ade80':'#f87171')+'">'+(b.profit!=null?_money(b.profit):'—')+'</td>'
        +'<td onclick="event.stopPropagation()" style="white-space:nowrap">'+editSel+delBtn+'</td>'
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
      +'<td style="font-weight:800;color:'+_resColor(res)+'">'+(res==='pending'?'pending':res)+actTxt+(b.manual?' <span title="Set manually" style="color:#fbbf24;font-size:.72rem">\u270e</span>':'')+'</td>'
      +'<td style="font-family:monospace;font-weight:700;color:'+((b.profit||0)>=0?'#4ade80':'#f87171')+'">'+(b.profit!=null?_money(b.profit):'—')+'</td>'
      +'<td onclick="event.stopPropagation()" style="white-space:nowrap">'+editSel+delBtn+'</td>'
      +'</tr>';
  }).join('');
  var dateBar='<div style="display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-bottom:14px"><label style="font-size:.85rem;color:#94a3b8;font-weight:700">Show day <input type="date" value="'+selDate+'" max="'+_maxd+'" onchange="_myBetsDate(this.value)" style="margin-left:8px;background:#020617;border:1px solid #334155;color:#fff;border-radius:7px;padding:7px 10px;font-size:.85rem"></label><span style="color:#64748b;font-size:.78rem">'+shownBets.length+' bet'+(shownBets.length===1?'':'s')+' on this day</span></div>';
  var tbl='<div style="overflow-x:auto"><table class="grade-table"><thead><tr><th>Date</th><th>'+(tab==='parlays'?'Parlay':'Player')+'</th><th>Pick</th><th>Odds</th><th>Stake</th><th>Result</th><th>Profit</th><th></th></tr></thead><tbody>'+rows+'</tbody></table></div>';
  var emptyMsg=(tab==='parlays')?'No parlays logged yet. Build one in the parlay cart, then log it.':'No single bets logged yet. Click <strong style="color:#c7d2fe">＋ Track Bet</strong> on any pick card to start.';
  var rowsHtml=!activeAll.length?'<p style="color:#94a3b8;padding:16px">'+emptyMsg+'</p>':(!shownBets.length?'<p style="color:#94a3b8;padding:16px">No '+(tab==='parlays'?'parlays':'bets')+' on '+selDate+'. Pick another day above.</p>':tbl);
  document.getElementById('mybets-body').innerHTML=tabBar+head+bcHtml+(activeAll.length?dateBar:'')+rowsHtml;
}
function _myBetsDate(v){ window.__MYBETS_DATE__=v; renderMyBets(window.__MYBETS__); }
function _myBetsTab(t){ window.__MYBETS_TAB__=(t==='parlays')?'parlays':'singles'; window.__MYBETS_DATE__=null; renderMyBets(window.__MYBETS__); }
function _mbSumm(list){
  var w=0,l=0,pu=0,pend=0,vo=0,staked=0,profit=0;
  (list||[]).forEach(function(b){
    var res=b.result||'pending'; var stake=parseFloat(b.stake||0)||0;
    if(res==='WIN') w++; else if(res==='LOSS') l++; else if(res==='PUSH') pu++; else if(res==='VOID') vo++; else pend++;
    if(res==='WIN'||res==='LOSS'||res==='PUSH'){ staked+=stake; profit+=(parseFloat(b.profit||0)||0); }
  });
  var roi=staked>0?(profit/staked*100):null;
  return {wins:w,losses:l,push:pu,pending:pend,void:vo,staked:Math.round(staked*100)/100,profit:Math.round(profit*100)/100,returned:Math.round((staked+profit)*100)/100,roi:(roi==null?null:Math.round(roi*10)/10)};
}
async function _deleteBet(id){
  if(!confirm('Remove this bet from your log?')) return;
  try{
    var res=await fetch('/api/bets/'+encodeURIComponent(id)+_betAuthQS(),{method:'DELETE'});
    if(!res.ok){ throw new Error(await res.text()); }
    openMyBets(false);
  }catch(e){ alert(e.message||'Delete failed'); }
}
async function _setBetResult(id,val){
  if(!val) return;
  var lbl=(val==='PENDING')?'reset this bet to pending (auto-grading re-enabled)':('mark this bet '+val+' (locks it so auto-grading won\u2019t change it)');
  if(!confirm('Manually '+lbl+'?')){ openMyBets(false); return; }
  try{
    var res=await fetch('/api/bets/'+encodeURIComponent(id)+'/result'+_betAuthQS(),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({result:val})});
    if(!res.ok){ throw new Error(await res.text()); }
    openMyBets(false);
  }catch(e){ alert(e.message||'Update failed'); openMyBets(false); }
}
async function _wipeMyBets(){
  var d=window.__MYBETS__;
  var n=(d&&d.bets)?d.bets.length:0;
  if(!n){ alert('No bets to wipe — your record is already empty.'); return; }
  if(!confirm('Permanently delete ALL '+n+' of your logged bets and start fresh? This cannot be undone.')) return;
  if(!confirm('Last chance — this wipes your entire My Bets record. Continue?')) return;
  try{
    var res=await fetch('/api/bets/clear'+_betAuthQS(),{method:'POST'});
    if(!res.ok){ throw new Error(await res.text()); }
    var j=await res.json();
    openMyBets(false);
    alert('Wiped '+(j.removed||n)+' bet'+((j.removed||n)===1?'':'s')+'. Fresh start.');
  }catch(e){ alert(e.message||'Wipe failed'); }
}
// ── Day-of-Week Report: does the matrix lean actually win? ──────────────
// Pure client-side analytics over /api/track-record detail (every graded pick
// carries date + category + side + result + odds). Buckets by weekday, scores
// each pick against the matrix lean (displayed _DOW_DISP or original _DOW_SIG),
// and ranks the best categories per day. Reads banked data only \u2014 no new
// backend, no effect on picks, cards, or the Track Record.
var _DOW_CATIDX={'Hitter Hits':0,'TB Over':1,'TB Under':1,'HRR':2,'HR':2,'Runs':3,'RBI':4,'Batter Walks':9,'Pitcher Ks':5,'Pitcher Outs':6,'Pitcher Hits Allowed':7,'Pitcher Earned Runs':8,'Pitcher Walks':9};
var _DOW_CATLBL={'Hitter Hits':'Hits','TB Over':'TB Over','TB Under':'TB Under','HRR':'H+R+RBI','HR':'Home Runs','Runs':'Runs','RBI':'RBI','Batter Walks':'Batter BB','Pitcher Ks':'Pitcher K','Pitcher Outs':'Outs','Pitcher Hits Allowed':'Hits Allowed','Pitcher Earned Runs':'Earned Runs','Pitcher Walks':'Pitcher BB'};
function _dowCatLabel(c){ var p=String(c).split('|'),b=p[0],sd=p[1]||'',l=_DOW_CATLBL[b]||b; if(l.indexOf('Over')>=0||l.indexOf('Under')>=0) return l; if(sd==='OVER') return l+' Over'; if(sd==='UNDER') return l+' Under'; return l; }
function _dowProfit(odds,win){ if(!win) return -1; var o=parseFloat(odds); if(isNaN(o)) return 0; return o>0? o/100 : 100/Math.abs(o); }
function _dowMatrix(){ var useSig=(window.__DOW_MX__==='sig'); var m=useSig?(typeof _DOW_SIG!=='undefined'?_DOW_SIG:null):(typeof _DOW_DISP!=='undefined'?_DOW_DISP:null); return m||{}; }
function _dowUColor(u){ return u>0.0001?'#4ade80':(u<-0.0001?'#f87171':'#94a3b8'); }
function _dowUFmt(u){ return (u>=0?'+':'')+u.toFixed(1)+'u'; }
function _dowCompute(detail){
  var days={},dayMx={},agree={w:0,l:0},against={w:0,l:0},mx=_dowMatrix();
  for(var i=0;i<7;i++){ days[i]={w:0,l:0,u:0,cats:{}}; dayMx[i]={aw:0,al:0,gw:0,gl:0}; }
  (detail||[]).forEach(function(r){
    var res=r.result; if(res!=='WIN'&&res!=='LOSS') return;
    var cat=r.category||''; var idx=_DOW_CATIDX[cat]; if(idx===undefined) return;
    if(r.odds===null||r.odds===''||r.odds===undefined) return;
    var ds=r.date; if(!ds) return;
    var dow=new Date(ds+'T12:00:00').getDay(); if(isNaN(dow)) return;
    var win=(res==='WIN'), prof=_dowProfit(r.odds,win), D=days[dow];
    if(win) D.w++; else D.l++; D.u+=prof;
    var side=(r.side||'OVER').toUpperCase();
    var catKey=cat+'|'+side;
    var C=D.cats[catKey]||(D.cats[catKey]={w:0,l:0,u:0}); if(win) C.w++; else C.l++; C.u+=prof;
    var lean=(mx[dow]||[])[idx];
    if(lean==='O'||lean==='U'){
      var followed=(side==='OVER'&&lean==='O')||(side==='UNDER'&&lean==='U');
      if(followed){ if(win){agree.w++;dayMx[dow].aw++;}else{agree.l++;dayMx[dow].al++;} }
      else{ if(win){against.w++;dayMx[dow].gw++;}else{against.l++;dayMx[dow].gl++;} }
    }
  });
  return {days:days,dayMx:dayMx,agree:agree,against:against};
}
function renderDowReport(tr){
  var allDetail=(tr&&tr.detail)||[];
  var sel=_dowWinSelector(allDetail);
  var c=_dowCompute(_dowFilterDetail(allDetail)), order=[1,2,3,4,5,6,0], today=new Date().getDay();
  var totN=0; order.forEach(function(d){ totN+=c.days[d].w+c.days[d].l; });
  if(!totN){ document.getElementById('dow-body').innerHTML=sel+'<p style="color:#94a3b8;padding:16px">No graded picks in this window yet \u2014 try All-time or a different month.</p>'; return; }
  var best=null,worst=null;
  order.forEach(function(d){ var D=c.days[d],n=D.w+D.l; if(n<5) return; var p=D.w/n*100;
    if(best===null||p>best.p) best={d:d,p:p,u:D.u}; if(worst===null||p<worst.p) worst={d:d,p:p}; });
  var head;
  if(best){ head='<div style="background:rgba(34,211,238,.07);border:1px solid rgba(34,211,238,.25);border-radius:10px;padding:12px 14px;margin-bottom:16px;font-size:.82rem;color:#e2e8f0;line-height:1.7">'
    +'Best day so far: <b style="color:#4ade80">'+_DOW_NAMES[best.d]+'</b> at '+best.p.toFixed(0)+'% ('+_dowUFmt(best.u)+').'
    +(worst&&worst.d!==best.d?' Worst: <b style="color:#f87171">'+_DOW_NAMES[worst.d]+'</b> at '+worst.p.toFixed(0)+'%.':'')+'</div>'; }
  else { head='<div style="color:#94a3b8;font-size:.78rem;margin-bottom:14px">Building sample \u2014 days with at least 5 graded picks get flagged best / worst.</div>'; }
  var aRows=order.map(function(d){
    var D=c.days[d],n=D.w+D.l,bc='&mdash;',bcp=-1;
    Object.keys(D.cats).forEach(function(k){ var C=D.cats[k],nn=C.w+C.l; if(nn<4) return; var p=C.w/nn*100; if(p>bcp){ bcp=p; bc=_dowCatLabel(k)+' '+C.w+'-'+C.l+' ('+p.toFixed(0)+'%)'; } });
    var mark=(best&&d===best.d)?' &#129351;':((worst&&worst.d!==(best&&best.d)&&d===worst.d)?' &#128078;':'');
    var bg=(d===today)?'background:rgba(245,158,11,.07)':'';
    return '<tr style="border-bottom:1px solid #1e1e1e;'+bg+'">'
      +'<td style="padding:8px 8px;color:#cbd5e1;font-weight:700;white-space:nowrap">'+_DOW_NAMES[d]+mark+'</td>'
      +'<td style="text-align:center;color:#94a3b8">'+n+'</td>'
      +'<td style="text-align:center;color:#e2e8f0">'+D.w+'-'+D.l+'</td>'
      +'<td style="text-align:center;font-weight:800;color:'+_twColor(D.w,D.l)+'">'+_twPct(D.w,D.l)+'</td>'
      +'<td style="text-align:center;font-weight:700;color:'+_dowUColor(D.u)+'">'+_dowUFmt(D.u)+'</td>'
      +'<td style="padding:8px 8px;color:#94a3b8;font-size:.74rem">'+bc+'</td></tr>';
  }).join('');
  var secA='<div style="font-weight:800;color:#22d3ee;font-size:.82rem;margin:4px 0 8px">Record by day of week</div>'
    +'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:.78rem;min-width:540px"><thead><tr style="border-bottom:2px solid #1e293b">'
    +'<th style="text-align:left;padding:7px 8px;color:#94a3b8;font-size:.64rem;letter-spacing:.05em">DAY</th>'
    +'<th style="padding:7px 6px;color:#94a3b8;font-size:.64rem">PICKS</th><th style="padding:7px 6px;color:#94a3b8;font-size:.64rem">W-L</th>'
    +'<th style="padding:7px 6px;color:#94a3b8;font-size:.64rem">WIN%</th><th style="padding:7px 6px;color:#94a3b8;font-size:.64rem">NET (1u)</th>'
    +'<th style="text-align:left;padding:7px 8px;color:#94a3b8;font-size:.64rem">BEST CATEGORY</th></tr></thead><tbody>'+aRows+'</tbody></table></div>';
  var ag=c.agree,ga=c.against, agN=ag.w+ag.l, gaN=ga.w+ga.l;
  var agP=agN?ag.w/agN*100:0, gaP=gaN?ga.w/gaN*100:0, edge=agP-gaP;
  var which=(window.__DOW_MX__==='sig')?'sig':'disp';
  var tabBtn=function(id,lab){ var on=(which===id); return '<button onclick="_dowSetMatrix(&#39;'+id+'&#39;)" style="padding:5px 14px;border-radius:8px;border:1px solid '+(on?'#22d3ee':'#334155')+';background:'+(on?'rgba(34,211,238,.12)':'transparent')+';color:'+(on?'#22d3ee':'#64748b')+';font-weight:800;font-size:.72rem;cursor:pointer">'+lab+'</button>'; };
  var verdict;
  if(agN<20||gaN<20){ verdict='<span style="color:#94a3b8">Not enough graded picks yet \u2014 keep banking days before trusting this.</span>'; }
  else if(edge>=3){ verdict='<span style="color:#4ade80;font-weight:800">Matrix shows an edge (+'+edge.toFixed(1)+' pts when you follow it)</span>'; }
  else if(edge<=-3){ verdict='<span style="color:#f87171;font-weight:800">Matrix is backwards ('+edge.toFixed(1)+' pts) \u2014 fading it would have done better</span>'; }
  else { verdict='<span style="color:#facc15;font-weight:800">No real edge ('+(edge>=0?'+':'')+edge.toFixed(1)+' pts) \u2014 looks like noise</span>'; }
  var mxRows=order.map(function(d){
    var M=c.dayMx[d],aN=M.aw+M.al,gN=M.gw+M.gl; if(!aN&&!gN) return '';
    var aP=aN?M.aw/aN*100:0,gP=gN?M.gw/gN*100:0,e=(aN&&gN)?aP-gP:null;
    return '<tr style="border-bottom:1px solid #1e1e1e"><td style="padding:7px 8px;color:#cbd5e1;font-weight:700">'+_DOW_NAMES[d]+'</td>'
      +'<td style="text-align:center;color:'+(aN?_twColor(M.aw,M.al):'#64748b')+'">'+(aN?M.aw+'-'+M.al+' ('+aP.toFixed(0)+'%)':'&mdash;')+'</td>'
      +'<td style="text-align:center;color:'+(gN?_twColor(M.gw,M.gl):'#64748b')+'">'+(gN?M.gw+'-'+M.gl+' ('+gP.toFixed(0)+'%)':'&mdash;')+'</td>'
      +'<td style="text-align:center;font-weight:700;color:'+(e===null?'#64748b':(e>=3?'#4ade80':(e<=-3?'#f87171':'#facc15')))+'">'+(e===null?'&mdash;':((e>=0?'+':'')+e.toFixed(0)))+'</td></tr>';
  }).join('');
  var secB='<div style="margin-top:22px;font-weight:800;color:#22d3ee;font-size:.82rem;margin-bottom:8px">Matrix reality check</div>'
    +'<div style="display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap"><span style="font-size:.72rem;color:#64748b">Compare against:</span>'+tabBtn('disp','Displayed matrix')+tabBtn('sig','Original matrix')+'</div>'
    +'<div style="background:#0b1220;border:1px solid #1e293b;border-radius:10px;padding:12px 14px;margin-bottom:12px;font-size:.8rem;line-height:1.8">'
    +'Followed the matrix: <b style="color:#e2e8f0">'+ag.w+'-'+ag.l+'</b> ('+(agN?agP.toFixed(0):'0')+'%) \u00b7 Went against it: <b style="color:#e2e8f0">'+ga.w+'-'+ga.l+'</b> ('+(gaN?gaP.toFixed(0):'0')+'%)<br>'+verdict+'</div>'
    +'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:.76rem;min-width:480px"><thead><tr style="border-bottom:2px solid #1e293b">'
    +'<th style="text-align:left;padding:7px 8px;color:#94a3b8;font-size:.64rem">DAY</th><th style="padding:7px 6px;color:#94a3b8;font-size:.64rem">FOLLOWED</th>'
    +'<th style="padding:7px 6px;color:#94a3b8;font-size:.64rem">AGAINST</th><th style="padding:7px 6px;color:#94a3b8;font-size:.64rem">EDGE (pts)</th></tr></thead><tbody>'+(mxRows||'<tr><td colspan="4" style="padding:12px;color:#64748b">No matrix-eligible picks graded yet.</td></tr>')+'</tbody></table></div>';
  var secC='<div style="margin-top:22px;font-weight:800;color:#22d3ee;font-size:.82rem;margin-bottom:8px">Top categories by day (70%+, min 4 graded)</div>';
  secC+=order.map(function(d){
    var D=c.days[d],arr=[];
    Object.keys(D.cats).forEach(function(k){ var C=D.cats[k],nn=C.w+C.l; if(nn<4) return; arr.push({k:k,w:C.w,l:C.l,p:C.w/nn*100}); });
    arr.sort(function(a,b){ return b.p-a.p; });
    if(!arr.length) return '<div style="padding:6px 8px;border-bottom:1px solid #141414;font-size:.78rem"><b style="color:#cbd5e1">'+_DOW_NAMES[d]+'</b> <span style="color:#64748b">\u2014 not enough graded picks yet</span></div>';
    var hit=arr.filter(function(x){ return x.p>=70; });
    if(!hit.length) return '<div style="padding:6px 8px;border-bottom:1px solid #141414;font-size:.78rem"><b style="color:#cbd5e1">'+_DOW_NAMES[d]+'</b> <span style="color:#64748b">\u2014 no category at 70%+ yet</span></div>';
    var top=hit.map(function(x){ return '<span style="color:'+_twColor(x.w,x.l)+'">'+_dowCatLabel(x.k)+' '+x.w+'-'+x.l+' ('+x.p.toFixed(0)+'%)</span>'; }).join('  \u00b7  ');
    return '<div style="padding:6px 8px;border-bottom:1px solid #141414;font-size:.78rem"><b style="color:#a5f3fc">'+_DOW_NAMES[d]+'</b> &mdash; '+top+'</div>';
  }).join('');
  document.getElementById('dow-body').innerHTML=sel+head+secA+secB+secC;
}
function _dowSetMatrix(which){ window.__DOW_MX__=which; if(window.__DOWTR__) renderDowReport(window.__DOWTR__); }
function _dowIsMon(s){ s=String(s||''); return s.length===7 && s.charAt(4)==='-'; }
function _dowMonLabel(k){ if(!_dowIsMon(k)) return k; var names=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']; var mo=parseInt(k.slice(5,7),10); return (names[mo-1]||k.slice(5,7))+' '+k.slice(0,4); }
function _dowWinRange(win){
  var now=new Date();
  if(!win||win==='all') return null;
  if(win==='week'){ var d=new Date(now.getFullYear(),now.getMonth(),now.getDate()); var dw=d.getDay(); var diff=(dw===0?6:dw-1); var s=new Date(d); s.setDate(d.getDate()-diff); var e=new Date(s); e.setDate(s.getDate()+7); return {s:s,e:e}; }
  if(win==='month'){ return {s:new Date(now.getFullYear(),now.getMonth(),1),e:new Date(now.getFullYear(),now.getMonth()+1,1)}; }
  if(win==='lastmonth'){ return {s:new Date(now.getFullYear(),now.getMonth()-1,1),e:new Date(now.getFullYear(),now.getMonth(),1)}; }
  if(win==='custom'){ var f=window.__DOW_FROM__||_isoShift(_trkTodayISO(),-6); var t=window.__DOW_TO__||_trkTodayISO(); return {s:new Date(f+'T12:00:00'),e:new Date(_isoShift(t,1)+'T12:00:00')}; }
  if(_dowIsMon(win)){ var y=parseInt(win.slice(0,4),10),mo=parseInt(win.slice(5,7),10)-1; return {s:new Date(y,mo,1),e:new Date(y,mo+1,1)}; }
  return null;
}
function _dowFilterDetail(detail){
  var r=_dowWinRange(window.__DOW_WIN__||'all'); if(!r) return detail||[];
  return (detail||[]).filter(function(row){ if(!row.date) return false; var d=new Date(String(row.date)+'T12:00:00'); if(isNaN(d.getTime())) return false; return d>=r.s && d<r.e; });
}
function _dowWinSelector(detail){
  var win=window.__DOW_WIN__||'all';
  var btn=function(id,lab){ var on=(win===id); return '<button onclick="_dowSetWin(&#39;'+id+'&#39;)" style="padding:5px 13px;border-radius:8px;border:1px solid '+(on?'#22d3ee':'#334155')+';background:'+(on?'rgba(34,211,238,.12)':'transparent')+';color:'+(on?'#22d3ee':'#94a3b8')+';font-weight:800;font-size:.72rem;cursor:pointer;white-space:nowrap">'+lab+'</button>'; };
  var months=[],seen={};
  (detail||[]).forEach(function(r){ if(!r.date) return; var k=String(r.date).slice(0,7); if(_dowIsMon(k)&&!seen[k]){ seen[k]=1; months.push(k); } });
  months.sort(); months.reverse();
  var opts='<option value="">Jump to month\u2026</option>'+months.map(function(k){ return '<option value="'+k+'"'+(win===k?' selected':'')+'>'+_dowMonLabel(k)+'</option>'; }).join('');
  var picker=months.length?'<select onchange="if(this.value)_dowSetWin(this.value)" style="padding:6px 10px;border-radius:8px;border:1px solid '+(_dowIsMon(win)?'#22d3ee':'#334155')+';background:#0b1220;color:'+(_dowIsMon(win)?'#22d3ee':'#94a3b8')+';font-weight:700;font-size:.72rem;cursor:pointer">'+opts+'</select>':'';
  var customInputs='';
  if(win==='custom'){ var f=window.__DOW_FROM__||_isoShift(_trkTodayISO(),-6); var t=window.__DOW_TO__||_trkTodayISO(); customInputs='<label style="font-size:.72rem;color:#94a3b8">From <input type="date" value="'+f+'" max="'+t+'" onchange="_dowSetCustom(&#39;from&#39;,this.value)" style="margin-left:4px;background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:4px 7px;font-size:.72rem"></label><label style="font-size:.72rem;color:#94a3b8">To <input type="date" value="'+t+'" min="'+f+'" max="'+_trkTodayISO()+'" onchange="_dowSetCustom(&#39;to&#39;,this.value)" style="margin-left:4px;background:#020617;border:1px solid #334155;color:#fff;border-radius:6px;padding:4px 7px;font-size:.72rem"></label>'; }
  return '<div style="display:flex;gap:7px;align-items:center;margin-bottom:14px;flex-wrap:wrap"><span style="font-size:.72rem;color:#64748b;font-weight:700">Window:</span>'+btn('all','All-time')+btn('week','This Week')+btn('month','This Month')+btn('lastmonth','Last Month')+btn('custom','Custom')+picker+customInputs+'</div>';
}
function _dowSetWin(win){ window.__DOW_WIN__=win; if(window.__DOWTR__) renderDowReport(window.__DOWTR__); }
function _dowSetCustom(which,val){ var today=_trkTodayISO(); var f=window.__DOW_FROM__||_isoShift(today,-6), t=window.__DOW_TO__||today; if(which==='from'){ f=val; if(f>t) t=f; } else { t=val; if(t>today) t=today; if(t<f) f=t; } window.__DOW_FROM__=f; window.__DOW_TO__=t; if(window.__DOWTR__) renderDowReport(window.__DOWTR__); }
async function openDowReport(){
  var btn=document.getElementById('dow-btn'); var lbl=btn.textContent; btn.disabled=true; btn.textContent='Loading...';
  show('dow-card'); document.getElementById('dow-card').scrollIntoView({behavior:'smooth',block:'start'});
  document.getElementById('dow-spinner').classList.remove('hidden'); document.getElementById('dow-body').innerHTML='';
  try{
    var res=await fetch('/api/track-record'+_betAuthQS());
    if(!res.ok){ throw new Error(await res.text()); }
    window.__DOWTR__=await res.json();
    if(!window.__DOW_MX__) window.__DOW_MX__='disp';
    if(!window.__DOW_WIN__) window.__DOW_WIN__='all';
    renderDowReport(window.__DOWTR__);
  }catch(e){
    document.getElementById('dow-body').innerHTML='<p style="color:#f87171;padding:16px">'+(e.message||'Error loading day-of-week report')+'</p>';
  }finally{
    btn.disabled=false; btn.textContent=lbl; document.getElementById('dow-spinner').classList.add('hidden');
  }
}
function downloadDowCSV(){
  var tr=window.__DOWTR__; if(!tr){ alert('Open the Day-of-Week report first.'); return; }
  var c=_dowCompute(_dowFilterDetail(tr.detail||[])), order=[1,2,3,4,5,6,0];
  var rows=[['Day','Category','Picks','Wins','Losses','Win%','NetUnits1u']];
  order.forEach(function(d){
    var D=c.days[d];
    Object.keys(D.cats).forEach(function(k){ var C=D.cats[k],n=C.w+C.l; rows.push([_DOW_NAMES[d],_dowCatLabel(k),n,C.w,C.l,(n?(C.w/n*100).toFixed(1):'0'),C.u.toFixed(2)]); });
    var dn=D.w+D.l; rows.push([_DOW_NAMES[d],'ALL',dn,D.w,D.l,(dn?(D.w/dn*100).toFixed(1):'0'),D.u.toFixed(2)]);
  });
  var csv=rows.map(function(row){ return row.map(_csvCell).join(','); }).join(String.fromCharCode(13)+String.fromCharCode(10));
  var blob=new Blob([String.fromCharCode(65279)+csv],{type:'text/csv;charset=utf-8;'});
  var url=URL.createObjectURL(blob); var a=document.createElement('a'); a.href=url; a.download='mlb-day-of-week-'+(window.__DOW_WIN__||'all')+'.csv';
  document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
}
function downloadMyBetsCSV(){
  var d=window.__MYBETS__; if(!d){ alert('Open My Bets first.'); return; }
  var rows=[['Date','Player','Category','Pick','Odds','Stake','Result','Actual','Profit']];
  var _tab=window.__MYBETS_TAB__==='parlays'?'parlays':'singles';
  (d.bets||[]).filter(function(b){ return _tab==='parlays'?(b.bet_type==='parlay'):(b.bet_type!=='parlay'); }).forEach(function(b){
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
  var a=document.createElement('a'); a.href=url; a.download='mlb-my-'+_tab+'.csv';
  document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
}
</script>
<div id="track-card" class="hidden space-y-6" style="max-width:960px;margin:0 auto 24px;padding:0 16px">
  <div class="card p-6">
    <div class="section-hdr" style="color:#a78bfa;margin-bottom:16px">🏆 Track Record</div>
    <div id="track-spinner" class="hidden" style="color:#94a3b8;font-size:.9rem;margin-bottom:12px;display:flex;align-items:center;gap:8px"><span class="spinner"></span> Grading history…</div>
    <div id="track-head"></div>
    <div id="track-body"></div>
  </div>
</div>
<div id="ovf-card" class="hidden space-y-6" style="max-width:960px;margin:0 auto 24px;padding:0 16px">
  <div class="card p-6">
    <div class="section-hdr" style="color:#fbbf24;margin-bottom:8px">⭐ Overflow Tracker</div>
    <div style="font-size:.78rem;color:#94a3b8;margin:0 0 14px">Every pick BEYOND each category&#39;s top 10 (ranks 11-30 per side), graded &amp; banked permanently &mdash; kept separate from the main Track Record.</div>
    <div id="ovf-spinner" class="hidden" style="color:#94a3b8;font-size:.9rem;margin-bottom:12px;display:flex;align-items:center;gap:8px"><span class="spinner"></span> Grading overflow history&hellip;</div>
    <div id="ovf-head"></div>
    <div id="ovf-body"></div>
  </div>
</div>
<div id="hrtrk-card" class="hidden space-y-6" style="max-width:960px;margin:0 auto 24px;padding:0 16px">
  <div class="card p-6">
    <div class="section-hdr" style="color:#fb7185;margin-bottom:8px">💣 HR Tracker</div>
    <div style="font-size:.78rem;color:#94a3b8;margin:0 0 14px">Every Home Run Over/Under pick, graded &amp; banked permanently &mdash; kept OUT of both the main Track Record and the Overflow Tracker.</div>
    <div id="hrtrk-spinner" class="hidden" style="color:#94a3b8;font-size:.9rem;margin-bottom:12px;display:flex;align-items:center;gap:8px"><span class="spinner"></span> Grading HR history&hellip;</div>
    <div id="hrtrk-head"></div>
    <div id="hrtrk-body"></div>
  </div>
</div>
<div id="dow-card" class="hidden space-y-6" style="max-width:960px;margin:0 auto 24px;padding:0 16px">
  <div class="card p-6">
    <div class="section-hdr" style="color:#22d3ee;margin-bottom:8px">📅 Day-of-Week Report</div>
    <div style="font-size:.78rem;color:#94a3b8;margin:0 0 14px">How every graded pick has performed by day of the week &mdash; and whether following the matrix lean would have helped. Reads banked results only; does not change any picks. Builds from deploy forward.</div>
    <div id="dow-spinner" class="hidden" style="color:#94a3b8;font-size:.9rem;margin-bottom:12px;display:flex;align-items:center;gap:8px"><span class="spinner"></span> Crunching day-of-week history&hellip;</div>
    <div id="dow-body"></div>
    <div style="margin-top:18px;padding-top:14px;border-top:1px solid #1e293b;display:flex;align-items:center;gap:12px;flex-wrap:wrap">
      <button onclick="downloadDowCSV()" style="background:#0e7490;color:#fff;border:none;border-radius:10px;padding:9px 20px;font-size:.84rem;font-weight:800;cursor:pointer">&#11015; Download CSV</button>
      <span style="font-size:.74rem;color:#64748b">Per-day, per-category record &amp; net units (flat 1u)</span>
    </div>
  </div>
</div>
<div id="mybets-card" class="hidden space-y-6" style="max-width:960px;margin:0 auto 24px;padding:0 16px">
  <div class="card p-6">
    <div class="section-hdr" style="color:#a5b4fc;margin-bottom:16px">💰 My Bets — Record &amp; ROI</div>
    <div id="mybets-body"></div>
    <div style="margin-top:18px;padding-top:14px;border-top:1px solid #1e293b;display:flex;align-items:center;gap:12px;flex-wrap:wrap">
      <button onclick="_manualBetForm()" style="background:#0f766e;color:#fff;border:none;border-radius:10px;padding:10px 22px;font-size:.88rem;font-weight:800;cursor:pointer">+ Manual Entry</button>
      <button onclick="_manualParlayForm()" style="background:#4338ca;color:#fff;border:none;border-radius:10px;padding:10px 22px;font-size:.88rem;font-weight:800;cursor:pointer">+ Manual Parlay</button>
      <button id="mybets-results-btn" onclick="getMyBetsResults()" style="background:#22c55e;color:#0f172a;border:none;border-radius:10px;padding:10px 22px;font-size:.88rem;font-weight:800;cursor:pointer">🔄 Get Results</button>
      <span style="font-size:.78rem;color:#64748b">Fetches box scores and grades all pending bets</span>
      <span id="mybets-spinner-wrap"></span>
      <button onclick="_wipeMyBets()" title="Permanently delete ALL your logged bets and start fresh — does not touch the Track Record" style="background:#7f1d1d;color:#fff;border:1px solid #b91c1c;border-radius:10px;padding:10px 18px;font-size:.84rem;font-weight:800;cursor:pointer;margin-left:auto">🗑 Wipe Record</button>
    </div>
  </div>
</div>
<footer style="text-align:center;padding:32px 24px;color:#4b5563;font-size:.78rem;border-top:1px solid #1c1c1c;margin-top:24px;font-family:'Source Sans Pro',sans-serif">
  <div style="font-family:'Playfair Display',serif;color:#f59e0b;font-weight:700;font-size:.95rem;margin-bottom:6px">Money Picks Arena</div>
  <div>MLB MoneyBall &middot; Daily Picks</div>
  <div style="margin-top:8px;font-size:.7rem">For entertainment only. Not a betting service. Must be 18+. Please gamble responsibly.</div>
</footer>
<button id="back-to-top" onclick="window.scrollTo({top:0,behavior:'smooth'})" title="Back to top"
  style="position:fixed;bottom:22px;right:22px;z-index:9999;display:none;width:48px;height:48px;border-radius:50%;border:none;cursor:pointer;background:#f59e0b;color:#0a0a0a;font-size:1.4rem;font-weight:900;box-shadow:0 4px 14px rgba(0,0,0,.45);line-height:1">&#8593;</button>
<script>
(function(){
  var b=document.getElementById('back-to-top');
  if(!b) return;
  function _t(){ b.style.display = (window.pageYOffset||document.documentElement.scrollTop) > 400 ? 'block' : 'none'; }
  window.addEventListener('scroll',_t,{passive:true});
  _t();
})();
</script>
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
        _snap = _freeze_started_picks(date_str, result)
        _save_disk_cache(date_str, _snap)
        _save_sb_picks(date_str, _snap)
        _save_open_snapshot(date_str, result)
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


def _start_auto_run_scheduler():
    t = _threading.Thread(target=_scheduler_loop, name="mlb-autorun", daemon=True)
    t.start()
    print("[scheduler] auto-run thread started — slots 11:00, 14:00 & 17:40 ET")
