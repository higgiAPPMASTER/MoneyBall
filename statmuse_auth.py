"""
statmuse_auth.py - Automatic StatMuse login via Playwright headless browser.

Flow:
  1. App starts -> background thread opens headless Chrome
  2. Navigates to statmuse.com/auth/signin
  3. Clicks "Sign in with email" -> types email -> clicks "Use password"
  4. Fills email + password in the password form -> clicks Sign in
  5. Extracts session cookies -> closes browser
  6. All subsequent StatMuse requests use those cookies (lightweight requests.Session)
  7. Session auto-refreshes every 8 hours

Required env vars:
    STATMUSE_EMAIL     your StatMuse account email
    STATMUSE_PASSWORD  your StatMuse account password
"""

import os, time, threading

EMAIL    = os.environ.get("STATMUSE_EMAIL",    "")
PASSWORD = os.environ.get("STATMUSE_PASSWORD", "")
LOGIN_URL       = "https://www.statmuse.com/auth/signin"
SESSION_MAX_AGE = 3600 * 8   # re-login every 8 hours
BROWSER_ARGS    = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--single-process",
]

_lock       = threading.Lock()
_cookie_str = ""
_last_login = 0.0
_status     = {"ok": False, "message": "Not started", "email": EMAIL}


def get_cookie_str() -> str:
    return _cookie_str


def get_status() -> dict:
    age = int(time.time() - _last_login) if _last_login else None
    return {**_status, "session_age_seconds": age}


def needs_refresh() -> bool:
    return (time.time() - _last_login) > SESSION_MAX_AGE


def _playwright_login(email: str, password: str) -> dict:
    """
    Open headless Chrome, complete the StatMuse sign-in flow, return cookies.

    StatMuse sign-in flow (discovered via Playwright inspection):
      /auth/signin
        -> click "Sign in with email"   (reveals magic-link email form)
        -> type email in input[name=email]
        -> "Use password" span appears  (click it to switch to password form)
        -> input[name=password] becomes visible
        -> fill email-password + password fields
        -> click submit button
        -> redirects away from /auth/
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=BROWSER_ARGS)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = ctx.new_page()

        try:
            # 1. Load sign-in page
            page.goto(LOGIN_URL, wait_until="networkidle", timeout=30000)

            # 2. Click "Sign in with email" to show the magic-link email form
            page.click("button:has-text('Sign in with email')", timeout=8000)
            page.wait_for_selector("input[name='email']", state="visible", timeout=8000)

            # 3. Type email — this makes "Use password" appear
            page.fill("input[name='email']", email)
            page.wait_for_selector("span:has-text('Use password')", state="visible", timeout=8000)

            # 4. Click "Use password" to reveal the email+password form
            page.click("span:has-text('Use password')", timeout=5000)
            page.wait_for_selector("input[name='password']", state="visible", timeout=8000)

            # 5. Fill in credentials (email-password field may need force=True)
            page.fill("input[name='email-password']", email, force=True)
            page.fill("input[name='password']", password)

            # 6. Submit
            page.click("button[type='submit']:has-text('Sign in')", timeout=5000)

            # 7. Wait to redirect away from /auth/
            try:
                page.wait_for_url(
                    lambda url: "/auth/" not in url,
                    timeout=18000,
                )
            except PWTimeout:
                body = page.inner_text("body")
                if any(k in body.lower() for k in ["incorrect", "invalid", "wrong", "error"]):
                    raise Exception("Login failed - check your email/password")
                raise Exception(f"Login timed out, still at: {page.url}")

            # 8. Grab all cookies
            raw = ctx.cookies("https://www.statmuse.com")
            if not raw:
                raise Exception("Login succeeded but no cookies returned")

            return {c["name"]: c["value"] for c in raw}

        finally:
            browser.close()


def login(force: bool = False) -> bool:
    """
    Log into StatMuse. Skips if session is still fresh unless force=True.
    Thread-safe. Returns True on success.
    """
    global _cookie_str, _last_login, _status

    if not EMAIL or not PASSWORD:
        with _lock:
            _status = {
                "ok": False,
                "message": "Set STATMUSE_EMAIL and STATMUSE_PASSWORD env vars",
                "email": "",
            }
        return False

    if not force and not needs_refresh() and _status["ok"]:
        return True

    with _lock:
        _status = {"ok": False, "message": "Logging in to StatMuse...", "email": EMAIL}

    last_error = None
    for attempt in range(1, 4):  # retry up to 3 times
        try:
            if attempt > 1:
                time.sleep(5 * attempt)  # wait longer between retries
            cookies = _playwright_login(EMAIL, PASSWORD)
            cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())

            with _lock:
                _cookie_str = cookie_str
                _last_login = time.time()
                _status = {
                    "ok":      True,
                    "message": f"Logged in as {EMAIL}",
                    "email":   EMAIL,
                    "cookies": len(cookies),
                }
            return True
        except Exception as exc:
            last_error = exc
            print(f"[StatMuse] Login attempt {attempt} failed: {exc} — retrying...")

    # All 3 attempts failed
    with _lock:
        _status = {"ok": False, "message": f"Login failed after 3 attempts: {last_error}", "email": EMAIL}
        return False


def refresh_if_needed() -> bool:
    if needs_refresh():
        return login(force=True)
    return _status["ok"]


if __name__ == "__main__":
    import sys
    print("Testing StatMuse auto-login...")
    if not EMAIL or not PASSWORD:
        print("ERROR: Set STATMUSE_EMAIL and STATMUSE_PASSWORD env vars first.")
        sys.exit(1)
    ok = login()
    s  = get_status()
    print(f"Result : {s['message']}")
    if ok:
        print(f"Cookies: {s.get('cookies', 0)} cookies extracted")
        print(f"Preview: {get_cookie_str()[:80]}...")
