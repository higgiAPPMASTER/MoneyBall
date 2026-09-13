"""Optional, price-only MLB fallback: hits Under 1.5 and batter Ks Under 0.5."""
import hashlib
import json
import math
import os
from pathlib import Path
import threading
import time
import unicodedata
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

_LOCK = threading.Lock()
_CACHE = Path(".pick_cache/oddspapi")
# theScore currently carries the broadest MLB Hits 1.5 board. Query it first so
# a later bookmaker-specific rate limit cannot strand the whole Hit fallback.
_BOOKS = {"thescore": "theScore", "betano.ca": "Betano Canada", "bet99": "Bet99"}
_MARKETS = {"playertotals-hits": ("hits", 1.5),
            "playertotals-strikeouts": ("ks", 0.5)}
_auth_blocked_until = 0
_request_blocked_until = {}


def _log(emit, message):
    if emit:
        emit({"type": "log", "msg": "OddsPapi: " + message})


def _name(value):
    # Provider names are often "Surname, Given"; never use surname-only matches.
    if "," in value:
        last, first = value.split(",", 1)
        value = first.strip() + " " + last.strip()
    value = "".join(c for c in unicodedata.normalize("NFKD", value)
                    if not unicodedata.combining(c))
    words = "".join(c if c.isalnum() else " " for c in value.lower()).split()
    if words and words[-1] in ("jr", "sr", "ii", "iii", "iv"):
        words.pop()
    return " ".join(words)


def _instant(value):
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.timestamp() if dt.tzinfo else None
    except (ValueError, TypeError):
        return None


def _get(endpoint, params, ttl, key, deadline, emit):
    """No URLs/exception bodies in logs: request query strings contain secrets."""
    global _auth_blocked_until
    cache_id = hashlib.sha256(
        json.dumps([endpoint, params], sort_keys=True).encode()).hexdigest()
    path = _CACHE / (cache_id + ".json")
    now = time.time()
    try:
        saved = json.loads(path.read_text())
        if 0 <= now - saved["fetched_at"] < ttl:
            return saved["data"], saved["fetched_at"]
    except (OSError, ValueError, KeyError, TypeError):
        pass
    if (now < _auth_blocked_until
            or now < _request_blocked_until.get(cache_id, 0)
            or time.monotonic() >= deadline):
        return None, None
    try:
        response = requests.get(
            "https://api.oddspapi.io/v4/" + endpoint,
            params={**params, "apiKey": key},
            timeout=max(1, min(15, deadline - time.monotonic())))
        if response.status_code != 200:
            _log(emit, f"{endpoint} HTTP {response.status_code}; missing prices remain unpriced")
            if response.status_code in (401, 403):
                _auth_blocked_until = time.time() + 3600
            elif response.status_code == 429:
                # Do not let one bookmaker response block the remaining books.
                # A brief per-request cooldown avoids a retry storm while the
                # next configured bookmaker can still supply the same market.
                _request_blocked_until[cache_id] = time.time() + 60
            return None, None
        data = response.json()
        if not isinstance(data, list):
            _log(emit, f"{endpoint} returned an unexpected format")
            return None, None
        fetched = time.time()
        try:
            _CACHE.mkdir(parents=True, exist_ok=True)
            temp = path.with_suffix(".tmp")
            temp.write_text(json.dumps({"fetched_at": fetched, "data": data}))
            temp.replace(path)
        except OSError:
            _log(emit, "disk cache unavailable; using this response only")
        return data, fetched
    except (requests.RequestException, ValueError) as exc:
        _log(emit, f"{endpoint} {type(exc).__name__}; keeping existing prices")
        return None, None


def _definitions(data):
    result = {}
    for market in data:
        target = _MARKETS.get(market.get("marketType"))
        if (not target or market.get("sportId") != 13
                or market.get("playerProp") is not True
                or market.get("period") != "result"
                or market.get("handicap") != target[1]):
            continue
        under = {str(o["outcomeId"]) for o in market.get("outcomes", [])
                 if o.get("outcomeName") == "Under"}
        if under:
            result[str(market["marketId"])] = (target[0], under)
    return result


def _quotes(fixtures, book, definitions, run_date, fetched_at):
    """Only active, exact-line, pregame selections, with their real source."""
    rows = []
    now = time.time()
    for fixture in fixtures:
        start = _instant(fixture.get("startTime"))
        if (fixture.get("sportId") != 13 or fixture.get("statusId") != 0
                or start is None or start <= now or fetched_at >= start
                or datetime.fromtimestamp(start, ZoneInfo("America/New_York")).date().isoformat() != run_date):
            continue
        source = fixture.get("bookmakerOdds", {}).get(book, {})
        if source.get("bookmakerIsActive") is not True or source.get("suspended") is True:
            continue
        for market_id, market in source.get("markets", {}).items():
            definition = definitions.get(str(market_id))
            if not definition or market.get("marketActive") is not True:
                continue
            kind, under_ids = definition
            for outcome_id, outcome in market.get("outcomes", {}).items():
                if str(outcome_id) not in under_ids:
                    continue
                for player in outcome.get("players", {}).values():
                    name = player.get("playerName")
                    if not name or player.get("active") is not True:
                        continue
                    try:
                        price = float(player.get("priceAmerican"))
                    except (TypeError, ValueError):
                        continue
                    if not math.isfinite(price) or not (-1000 <= price <= -100 or price >= 100):
                        continue
                    rows.append({
                        "name": _name(name), "kind": kind, "price": price,
                        "book": _BOOKS[book], "book_key": book, "start": start,
                        "teams": (fixture.get("participant1Name", ""),
                                  fixture.get("participant2Name", "")),
                        "fixture_id": fixture.get("fixtureId"),
                        "fetched_at": fetched_at,
                        "changed_at": player.get("changedAt"),
                    })
    return rows


def _apply(targets, quotes, team_match):
    count = 0
    for kind, pick in targets:
        if pick.get("under_odds") not in (None, ""):
            continue
        start = _instant(pick.get("game_start"))
        if start is None or start <= time.time():
            continue
        name = _name(pick.get("name") or pick.get("full_name") or "")
        team, opp = pick.get("team", ""), pick.get("opp", "")
        if not name or not team or not opp:
            continue
        matches = []
        for quote in quotes:
            a, b = quote["teams"]
            if (quote["kind"] != kind or quote["name"] != name
                    or abs(quote["start"] - start) > 300
                    or quote["fetched_at"] >= start):
                continue
            if ((team_match(team, a) and team_match(opp, b)) or
                    (team_match(team, b) and team_match(opp, a))):
                matches.append(quote)
        if not matches:
            continue
        chosen = max(matches, key=lambda q: q["price"])
        pick["under_odds"] = chosen["price"]
        pick["book"] = chosen["book"]
        pick["under_odds_book"] = chosen["book_key"]
        pick["odds_source"] = "oddspapi"
        pick["odds_fetched_at"] = datetime.fromtimestamp(
            chosen["fetched_at"], timezone.utc).isoformat()
        pick["odds_changed_at"] = chosen["changed_at"]
        pick["odds_fixture_id"] = chosen["fixture_id"]
        count += 1
    return count


def enrich_under_odds(run_date, hit_picks, batter_k_picks, emit=None):
    """Fill existing generated picks only. Never add candidates or change scores."""
    key = os.environ.get("ODDSPAPI_API_KEY", "")
    if not key:
        _log(emit, "ODDSPAPI_API_KEY not set; fallback disabled")
        return
    if run_date != datetime.now(ZoneInfo("America/New_York")).date().isoformat():
        return  # Live quotes never backfill historical dates.
    targets = [("hits", p) for p in hit_picks if p.get("line") == 1.5]
    targets += [("ks", p) for p in batter_k_picks
                if p.get("pick") == "UNDER" and p.get("line") == 0.5]
    targets = [(kind, p) for kind, p in targets
               if p.get("under_odds") in (None, "")
               and (_instant(p.get("game_start")) or 0) > time.time()]
    if not targets:
        return
    if not _LOCK.acquire(blocking=False):
        _log(emit, "another fallback fetch is running; keeping current prices")
        return
    try:
        from under_picks import _team_match
        deadline = time.monotonic() + 60
        metadata, _ = _get("markets", {"language": "en"}, 604800, key, deadline, emit)
        if metadata is None:
            return
        definitions = _definitions(metadata)
        if not definitions:
            _log(emit, "required exact-line market definitions unavailable")
            return
        # Resolve MLB by metadata rather than trusting a permanent numeric ID.
        leagues, _ = _get("tournaments", {"sportId": 13, "language": "en"},
                          604800, key, deadline, emit)
        if leagues is None:
            return
        mlb = [x for x in leagues if x.get("tournamentName") == "MLB"]
        if len(mlb) != 1:
            _log(emit, "MLB league identity not unique; skipping fallback")
            return
        all_quotes = []
        for book in _BOOKS:
            # Actual v4 API requires singular bookmaker despite plural in docs.
            data, fetched = _get(
                "odds-by-tournaments",
                {"tournamentIds": str(mlb[0]["tournamentId"]), "bookmaker": book,
                 "language": "en", "verbosity": 3},
                3600, key, deadline, emit)
            if data is not None:
                all_quotes.extend(_quotes(data, book, definitions, run_date, fetched))
        filled = _apply(targets, all_quotes, _team_match)
        _log(emit, f"filled {filled}/{len(targets)} missing Under prices "
             "(Hits 1.5 / Batter Ks 0.5); existing prices and picks unchanged")
    except Exception as exc:
        _log(emit, f"fallback failed ({type(exc).__name__}); existing picks retained")
    finally:
        _LOCK.release()
