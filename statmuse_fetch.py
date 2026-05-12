"""
StatMuse BA Fetcher — v3 (authenticated session)
=================================================
StatMuse blocks unauthenticated requests from cloud/datacenter IPs.
A logged-in session cookie bypasses this.

Set env var:  STATMUSE_COOKIES="<paste full Cookie header from your browser>"

How to get your cookie string:
  1. Log into statmuse.com in Chrome/Firefox
  2. Open DevTools → Network tab → reload the page
  3. Click any statmuse.com request → Headers → Request Headers
  4. Copy the entire value of the "Cookie:" header
  5. Paste it into the STATMUSE_COOKIES environment variable on Render

STEP 2: LIFETIME last-10 H/A games vs today's opponent.  Min 3 games.
STEP 3: 2026 SEASON last-10 H/A games vs all teams.      Min 3 games.
"""

import os, re, time
from bs4 import BeautifulSoup

SEASON  = "2026"
MIN_G   = 3

# Use curl_cffi to mimic Chrome TLS fingerprint — bypasses StatMuse bot detection
# Falls back to requests if curl_cffi not available
try:
    from curl_cffi import requests as _req_lib
    _session = _req_lib.Session(impersonate="chrome120")
    _USING_CFFI = True
except ImportError:
    import requests as _req_lib
    _session = _req_lib.Session()
    _session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    })
    _USING_CFFI = False


def _apply_auth_cookies():
    """
    Pull the current StatMuse session cookies from statmuse_auth
    and apply them to our requests.Session.
    Also falls back to the legacy STATMUSE_COOKIES env var.
    """
    _session.cookies.clear()

    # Primary: cookies from Playwright login
    try:
        from statmuse_auth import get_cookie_str
        cookie_str = get_cookie_str()
    except ImportError:
        cookie_str = ""

    # Fallback: manual cookie env var
    if not cookie_str:
        cookie_str = os.environ.get("STATMUSE_COOKIES", "").strip()

    if cookie_str:
        for part in cookie_str.split(";"):
            part = part.strip()
            if "=" in part:
                name, _, value = part.partition("=")
                _session.cookies.set(name.strip(), value.strip(), domain=".statmuse.com")
        return True
    return False


def refresh_session():
    """Re-apply latest cookies (call after a new Playwright login)."""
    return _apply_auth_cookies()

# Apply whatever cookies are available at import time
_apply_auth_cookies()


def test_connection() -> dict:
    """
    Verify the StatMuse session is working.
    Returns {"ok": bool, "authenticated": bool, "message": str}
    """
    url = "https://www.statmuse.com/mlb/ask/freddie-freeman-batting-average-in-last-10-home-games-in-2026"
    try:
        r    = _session.get(url, timeout=14)
        soup = BeautifulSoup(r.text, "html.parser")

        # Check for paywall / login wall
        page_text = r.text.lower()
        blocked   = any(k in page_text for k in ["you must be signed in", "subscribe to view",
                                                   "upgrade your plan", "sign in to continue"])

        # Check for stats table
        result = _parse_statmuse_page(r.text)

        if blocked:
            return {"ok": False, "authenticated": False,
                    "message": "StatMuse requires login — add your STATMUSE_COOKIES env var."}
        has_cookies = bool(_session.cookies)
        if result["ok"]:
            return {"ok": True, "authenticated": has_cookies,
                    "message": f"Connected \u2705 \u2014 test query returned {result['display']} in {result['games']}g"}
        return {"ok": False, "authenticated": has_cookies,
                "message": "StatMuse responded but returned no stats. Check credentials or try again later."}
    except Exception as e:
        return {"ok": False, "authenticated": False, "message": f"Connection error: {e}"}


# ── Core HTML parser ──────────────────────────────────────────────────

def _parse_statmuse_page(html: str) -> dict:
    """
    Parse a StatMuse stats page and extract the batting average.
    Reads the AVG column and G (games) column from the first stats table.
    """
    soup = BeautifulSoup(html, "html.parser")

    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if "AVG" not in headers:
            continue

        avg_idx    = headers.index("AVG")
        g_idx      = headers.index("G")      if "G"      in headers else -1
        season_idx = headers.index("SEASON") if "SEASON" in headers else -1

        for row in table.find_all("tr"):
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if not cols or len(cols) <= avg_idx:
                continue
            avg_str = cols[avg_idx]
            if not re.match(r"^\.\d{3}$", avg_str):
                continue

            ba     = float(avg_str)
            games  = int(cols[g_idx]) if g_idx >= 0 and cols[g_idx].isdigit() else 0
            season = cols[season_idx] if season_idx >= 0 and season_idx < len(cols) else ""

            return {"ba": ba, "games": games, "display": avg_str, "season": season,
                    "ok": games >= MIN_G}

    return {"ba": None, "games": 0, "display": "N/A", "season": "", "ok": False}


def _fetch_and_parse(url: str) -> dict:
    try:
        r = _session.get(url, timeout=14)
        if r.status_code != 200:
            return {"ba": None, "games": 0, "display": "N/A", "season": "", "ok": False}
        return _parse_statmuse_page(r.text)
    except Exception:
        return {"ba": None, "games": 0, "display": "N/A", "season": "", "ok": False}


# ── Public API ────────────────────────────────────────────────────────

def fetch_step2_ba(first: str, last: str, side: str, opp: str, session=None) -> dict:
    """STEP 2: Lifetime last-10 H/A games vs today's opponent. Min 3 games."""
    side_word = "away" if side == "AWAY" else "home"
    urls = [
        f"https://www.statmuse.com/mlb/ask/{first}-{last}-batting-average-in-last-10-{side_word}-games-vs-{opp}",
        f"https://www.statmuse.com/mlb/ask/{first}-{last}-batting-average-in-last-3-games-vs-{opp}",
    ]
    return _run_cascade(urls)


def fetch_step3_ba(first: str, last: str, side: str, session=None) -> dict:
    """STEP 3: 2026 season last-10 H/A games vs all teams. Min 3 games."""
    side_word = "away" if side == "AWAY" else "home"
    urls = [
        f"https://www.statmuse.com/mlb/ask/{first}-{last}-batting-average-in-last-10-{side_word}-games-in-{SEASON}",
        f"https://www.statmuse.com/mlb/ask/{first}-{last}-batting-average-in-last-3-{side_word}-games-in-{SEASON}",
    ]
    return _run_cascade(urls)


def _run_cascade(urls: list) -> dict:
    for i, url in enumerate(urls):
        parsed = _fetch_and_parse(url)
        time.sleep(0.3)
        if parsed["ok"]:
            flag = "✅" if i == 0 else "✅ (3+g)"
            return {"ba": parsed["ba"], "score_ba": parsed["ba"],
                    "display": parsed["display"], "flag": flag,
                    "games": parsed["games"], "url": url}
    return {"ba": None, "score_ba": 0.0, "display": "N/A",
            "flag": "❌ N/A", "games": 0, "url": urls[0]}


def fetch_statmuse_ba(first, last, side, opp=None, session=None):
    if opp:
        return fetch_step2_ba(first, last, side, opp, session)
    else:
        return fetch_step3_ba(first, last, side, session)
