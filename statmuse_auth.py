"""
statmuse_auth.py — No longer needed! Steps 2 & 3 now use MLB Stats API directly.
Kept as a stub so existing imports don't break.
"""

def login(force=False): return True
def get_status(): return {"ok": True, "message": "MLB API active (no login needed)", "email": ""}
def get_cookie_str(): return ""
def needs_refresh(): return False
def refresh_if_needed(): return True
