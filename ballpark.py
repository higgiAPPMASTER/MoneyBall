"""
ballpark.py — Game-environment factor for MLB props (DISPLAY ONLY).

Combines:
  1. Open-Meteo weather (https://open-meteo.com) — FREE, no API key, no rate
     limits. Hourly temperature / wind speed / wind direction by lat/lon.
  2. Static Baseball Savant park factors (runs index, 1.00 = league average).
     Refresh seasonally from the Statcast Park Factors leaderboard.

Rule of thumb (user spec): every 10°F over a 70°F baseline adds ~1% fly-ball
carry. Wind blowing OUT toward center field adds carry; blowing IN suppresses it
(only meaningful with each park's home-plate -> center-field bearing).

This module produces a per-GAME environment factor + a short summary string that
the frontend shows as a chip on every pick card. It DOES NOT change picks,
scores, or rankings (Phase A). All values are best-effort indicators.

Keys are ESPN displayNames (e.g. "Colorado Rockies") so they match the schedule
map built in pipeline.py from the ESPN scoreboard.
"""
import os, json, math, datetime
import requests

CACHE_DIR = ".weather_cache"
BASELINE_TEMP_F = 70.0          # carry baseline
TEMP_PCT_PER_10F = 0.01         # +1% carry per 10F (user rule of thumb)
WIND_PCT_PER_MPH = 0.006        # ~0.6% per mph of out/in component
WIND_CAP_MPH     = 20.0         # cap the wind component used
ENV_CLAMP        = (0.85, 1.20) # final factor clamp
OVER_CUT         = 1.04         # env >= -> favors OVER
UNDER_CUT        = 0.96         # env <= -> favors UNDER

# roof: "open" | "dome" (fixed; weather neutral) | "retract" (assume open / apply weather)
# cf  : approximate bearing (deg, 0=N, 90=E) from home plate toward center field.
#       Used only to resolve wind out/in; approximate is acceptable for a chip.
# park: Baseball Savant runs park factor (1.00 = average). Approx 2024-2025.
STADIUMS = {
    "Arizona Diamondbacks": {"lat": 33.4455, "lon": -112.0667, "roof": "retract", "cf": 0,  "park": 1.03},
    "Atlanta Braves":       {"lat": 33.8908, "lon": -84.4678,  "roof": "open",    "cf": 51, "park": 1.01},
    "Baltimore Orioles":    {"lat": 39.2839, "lon": -76.6217,  "roof": "open",    "cf": 30, "park": 1.02},
    "Boston Red Sox":       {"lat": 42.3467, "lon": -71.0972,  "roof": "open",    "cf": 45, "park": 1.09},
    "Chicago Cubs":         {"lat": 41.9484, "lon": -87.6553,  "roof": "open",    "cf": 33, "park": 1.00},
    "Chicago White Sox":    {"lat": 41.8300, "lon": -87.6339,  "roof": "open",    "cf": 39, "park": 1.00},
    "Cincinnati Reds":      {"lat": 39.0975, "lon": -84.5069,  "roof": "open",    "cf": 30, "park": 1.10},
    "Cleveland Guardians":  {"lat": 41.4962, "lon": -81.6852,  "roof": "open",    "cf": 0,  "park": 0.98},
    "Colorado Rockies":     {"lat": 39.7559, "lon": -104.9942, "roof": "open",    "cf": 0,  "park": 1.12},
    "Detroit Tigers":       {"lat": 42.3390, "lon": -83.0485,  "roof": "open",    "cf": 27, "park": 0.98},
    "Houston Astros":       {"lat": 29.7570, "lon": -95.3555,  "roof": "retract", "cf": 19, "park": 1.00},
    "Kansas City Royals":   {"lat": 39.0517, "lon": -94.4803,  "roof": "open",    "cf": 45, "park": 1.04},
    "Los Angeles Angels":   {"lat": 33.8003, "lon": -117.8827, "roof": "open",    "cf": 45, "park": 1.01},
    "Los Angeles Dodgers":  {"lat": 34.0739, "lon": -118.2400, "roof": "open",    "cf": 25, "park": 0.98},
    "Miami Marlins":        {"lat": 25.7780, "lon": -80.2197,  "roof": "retract", "cf": 38, "park": 0.97},
    "Milwaukee Brewers":    {"lat": 43.0280, "lon": -87.9712,  "roof": "retract", "cf": 8,  "park": 0.99},
    "Minnesota Twins":      {"lat": 44.9817, "lon": -93.2776,  "roof": "open",    "cf": 81, "park": 1.00},
    "New York Mets":        {"lat": 40.7571, "lon": -73.8458,  "roof": "open",    "cf": 27, "park": 0.99},
    "New York Yankees":     {"lat": 40.8296, "lon": -73.9262,  "roof": "open",    "cf": 24, "park": 1.00},
    "Athletics":            {"lat": 38.5800, "lon": -121.5180, "roof": "open",    "cf": 60, "park": 0.98},
    "Philadelphia Phillies":{"lat": 39.9061, "lon": -75.1665,  "roof": "open",    "cf": 15, "park": 1.02},
    "Pittsburgh Pirates":   {"lat": 40.4469, "lon": -80.0057,  "roof": "open",    "cf": 57, "park": 1.00},
    "San Diego Padres":     {"lat": 32.7073, "lon": -117.1566, "roof": "open",    "cf": 0,  "park": 0.96},
    "San Francisco Giants": {"lat": 37.7786, "lon": -122.3893, "roof": "open",    "cf": 49, "park": 0.92},
    "Seattle Mariners":     {"lat": 47.5914, "lon": -122.3325, "roof": "retract", "cf": 45, "park": 0.93},
    "St. Louis Cardinals":  {"lat": 38.6226, "lon": -90.1928,  "roof": "open",    "cf": 62, "park": 1.00},
    "Tampa Bay Rays":       {"lat": 27.9803, "lon": -82.5066,  "roof": "open",    "cf": 45, "park": 0.99},  # Steinbrenner Field (2025-26)
    "Texas Rangers":        {"lat": 32.7473, "lon": -97.0847,  "roof": "retract", "cf": 0,  "park": 1.01},
    "Toronto Blue Jays":    {"lat": 43.6414, "lon": -79.3894,  "roof": "retract", "cf": 0,  "park": 1.02},
    "Washington Nationals": {"lat": 38.8730, "lon": -77.0074,  "roof": "open",    "cf": 30, "park": 1.01},
}
# Common alternate display names -> canonical key above
ALIASES = {
    "Oakland Athletics": "Athletics",
    "Sacramento Athletics": "Athletics",
    "St Louis Cardinals": "St. Louis Cardinals",
}


def _stadium(home_team):
    if not home_team:
        return None, None
    key = ALIASES.get(home_team, home_team)
    if key in STADIUMS:
        return key, STADIUMS[key]
    tl = home_team.lower()
    for k, v in STADIUMS.items():
        if tl in k.lower() or k.lower() in tl:
            return k, v
    return None, None


def _parse_start(game_start):
    """ISO UTC like '2026-05-30T23:05Z' -> (date 'YYYY-MM-DD', hour int) in UTC,
    rounded to the nearest hour. Returns (None, None) on failure."""
    if not game_start:
        return None, None
    try:
        s = game_start.replace("Z", "+00:00")
        dt = datetime.datetime.fromisoformat(s).astimezone(datetime.timezone.utc)
        if dt.minute >= 30:
            dt = dt + datetime.timedelta(hours=1)
        return dt.strftime("%Y-%m-%d"), dt.hour
    except Exception:
        return None, None


def _fetch_weather(lat, lon, date_str, hour):
    """One Open-Meteo GET; pick the hour matching first pitch (UTC).
    Returns {temp_f, wind_mph, wind_dir} or None."""
    try:
        url = ("https://api.open-meteo.com/v1/forecast"
               f"?latitude={lat}&longitude={lon}"
               "&hourly=temperature_2m,wind_speed_10m,wind_direction_10m"
               "&temperature_unit=fahrenheit&wind_speed_unit=mph"
               "&timezone=GMT&forecast_days=3&past_days=1")
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        h = r.json().get("hourly", {})
        times = h.get("time", [])
        target = f"{date_str}T{hour:02d}:00"
        idx = times.index(target) if target in times else None
        if idx is None:
            # fall back to the closest available hour
            if not times:
                return None
            idx = min(range(len(times)),
                      key=lambda i: abs(_hrdiff(times[i], date_str, hour)))
        return {
            "temp_f":   h.get("temperature_2m",     [None])[idx],
            "wind_mph": h.get("wind_speed_10m",      [None])[idx],
            "wind_dir": h.get("wind_direction_10m",  [None])[idx],
        }
    except Exception:
        return None


def _hrdiff(t, date_str, hour):
    try:
        dt = datetime.datetime.fromisoformat(t)
        tgt = datetime.datetime.fromisoformat(f"{date_str}T{hour:02d}:00")
        return (dt - tgt).total_seconds() / 3600.0
    except Exception:
        return 1e9


def _wind_component(wind_mph, wind_dir, cf_bearing):
    """Out(+)/in(-) wind component along the home-plate -> CF axis.
    wind_dir is the direction the wind blows FROM (meteorological)."""
    if wind_mph is None or wind_dir is None:
        return 0.0, "—"
    blow_to = (wind_dir + 180.0) % 360.0          # direction wind blows TOWARD
    ang = math.radians(blow_to - cf_bearing)
    comp = wind_mph * math.cos(ang)               # + toward CF (out), - (in)
    lbl = "out" if comp > 3 else "in" if comp < -3 else "cross"
    return comp, lbl


def _cache_path(date_str):
    return os.path.join(CACHE_DIR, f"{date_str}.json")


def _load_cache(date_str):
    try:
        with open(_cache_path(date_str)) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_cache(date_str, data):
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(_cache_path(date_str), "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def game_env(home_team, game_start):
    """Return a small dict describing the game environment for the chip, or None.
    {factor, pct, lean, summary, temp_f, wind_mph, wind_lbl, park, indoor}"""
    key, st = _stadium(home_team)
    if not st:
        return None
    date_str, hour = _parse_start(game_start)
    park = st.get("park", 1.0)

    # disk cache (survives Render spin-down); keyed by date|home|hour so a
    # same-stadium doubleheader with different first-pitch weather is distinct
    cache_key = f"{date_str or 'na'}|{key}|{hour if hour is not None else 'na'}"
    cache = _load_cache(date_str or "na")
    if cache_key in cache:
        return cache[cache_key]

    # Fixed dome -> weather neutral, park factor only.
    if st.get("roof") == "dome":
        factor = max(ENV_CLAMP[0], min(ENV_CLAMP[1], park))
        out = _pack(factor, park, None, None, None, indoor=True)
        cache[cache_key] = out; _save_cache(date_str or "na", cache)
        return out

    w = _fetch_weather(st["lat"], st["lon"], date_str, hour) if date_str is not None else None
    if not w or w.get("temp_f") is None:
        # No weather -> park factor only (still useful), no temp/wind shown.
        factor = max(ENV_CLAMP[0], min(ENV_CLAMP[1], park))
        out = _pack(factor, park, None, None, None, indoor=False)
        cache[cache_key] = out; _save_cache(date_str or "na", cache)
        return out

    temp_adj = (w["temp_f"] - BASELINE_TEMP_F) / 10.0 * TEMP_PCT_PER_10F
    comp, wind_lbl = _wind_component(w.get("wind_mph"), w.get("wind_dir"), st.get("cf", 0))
    comp = max(-WIND_CAP_MPH, min(WIND_CAP_MPH, comp))
    wind_adj = comp * WIND_PCT_PER_MPH
    factor = park * (1.0 + temp_adj + wind_adj)
    factor = max(ENV_CLAMP[0], min(ENV_CLAMP[1], factor))
    out = _pack(factor, park, w.get("temp_f"), w.get("wind_mph"), wind_lbl, indoor=False)
    cache[cache_key] = out; _save_cache(date_str or "na", cache)
    return out


def _pack(factor, park, temp_f, wind_mph, wind_lbl, indoor):
    pct = round((factor - 1.0) * 100)
    lean = "OVER" if factor >= OVER_CUT else "UNDER" if factor <= UNDER_CUT else "NEUTRAL"
    sign = "+" if pct > 0 else ""
    parts = []
    if temp_f is not None:
        parts.append(f"{round(temp_f)}\u00b0F")
    if wind_mph is not None and wind_lbl and wind_lbl != "—":
        parts.append(f"\U0001F4A8 {round(wind_mph)} {wind_lbl}")
    if indoor:
        parts.append("indoor")
    parts.append(f"\U0001F3DF {sign}{pct}%")
    return {
        "factor": round(factor, 3),
        "pct": pct,
        "lean": lean,
        "park": round(park, 2),
        "temp_f": None if temp_f is None else round(temp_f),
        "wind_mph": None if wind_mph is None else round(wind_mph),
        "wind_lbl": wind_lbl,
        "indoor": indoor,
        "summary": " \u00b7 ".join(parts),
    }
