"""
auth.py — Simple JWT authentication for MLB Daily Picks App
Users are configured via env var: USERS="alice:pass1,bob:pass2"
"""
import os, secrets
from datetime import datetime, timedelta, timezone
from typing import Optional
from jose import JWTError, jwt
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

SECRET_KEY   = os.environ.get("JWT_SECRET", os.environ.get("SECRET_KEY", "CHANGE_THIS_SECRET_123_change_me"))
ALGORITHM    = "HS256"
TOKEN_HOURS  = 24

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/login")


def _get_users() -> dict:
    raw   = os.environ.get("USERS", "admin:picks2026")
    users = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" in pair:
            u, p = pair.split(":", 1)
            users[u.strip().lower()] = p.strip()
    return users


def verify_user(username: str, password: str) -> bool:
    stored = _get_users().get(username.lower())
    if not stored:
        return False
    return secrets.compare_digest(stored, password)


def create_token(username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=TOKEN_HOURS)
    return jwt.encode({"sub": username, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)


def _decode_token(token: str) -> str:
    """Raises HTTPException on failure. Returns username."""
    try:
        payload  = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    """FastAPI dependency — reads token from Authorization: Bearer header."""
    return _decode_token(token)


def get_user_from_query(token: Optional[str] = None) -> str:
    """For SSE endpoints where we can't set custom headers (query param)."""
    if not token:
        raise HTTPException(status_code=401, detail="Token required")
    return _decode_token(token)
