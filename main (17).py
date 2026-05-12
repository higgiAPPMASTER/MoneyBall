"""
MoneyBall Backend — main.py
============================
FastAPI server — runs the full 4-step MLB Daily Picks algorithm.
Step 1: MLB Stats API — career BA vs today's probable pitcher (min 4 AB, min .250)
        No FIC dependency — works on Render without Cloudflare issues.
START:  uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import requests
from bs4 import BeautifulSoup
import re, time, threading, uuid
from datetime import datetime
from typing import Dict, Any, Optional

app = FastAPI(title="MoneyBall API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

JOBS: Dict[str, Dict[str, Any]] = {}

BROWSER_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

MIN_AB = 4;  MIN_BA = 0.250;  MIN_STEP2_BA = 0.250
MIN_STEP3_BA = 0.250;  MIN_DN_BA = 0.200
MLB_API = "https://statsapi.mlb.com/api/v1"
SEASON  = str(datetime.now().year)

TEAM_SLUGS = {
    "St. Louis Cardinals":"cardinals","Arizona Diamondbacks":"diamondbacks",
    "Chicago Cubs":"cubs","Baltimore Orioles":"orioles",
    "Washington Nationals":"nationals","Kansas City Royals":"royals",
    "Athletics":"athletics","Oakland Athletics":"athletics",
    "Pittsburgh Pirates":"pirates","New York Yankees":"yankees",
    "Los Angeles Dodgers":"dodgers","Boston Red Sox":"red-sox",
    "Chicago White Sox":"white-sox","Toronto Blue Jays":"blue-jays",
    "Tampa Bay Rays":"rays","Minnesota Twins":"twins",
    "Milwaukee Brewers":"brewers","San Francisco Giants":"giants",
    "San Diego Padres":"padres","Philadelphia Phillies":"phillies",
    "New York Mets":"mets","Atlanta Braves":"braves",
    "Cincinnati Reds":"reds","Cleveland Guardians":"guardians",
    "Detroit Tigers":"tigers","Houston Astros":"astros",
    "Los Angeles Angels":"angels","Miami Marlins":"marlins",
    "Seattle Mariners":"mariners","Texas Rangers":"rangers",
    "Colorado Rockies":"rockies",
}

PITCHER_POSITIONS = {"P", "SP", "RP", "1"}

def normalize(text):
    for a,p in {'á':'a','é':'e','í':'i','ó':'o','ú':'u','ñ':'n','ç':'c'}.items():
        text = text.replace(a, p)
    return text.lower()

def name_to_slug(full_name):
    name = normalize(full_name)
    return re.sub(r"[^a-z0-9]+", "-", name).strip("-")

# ── STEP 1: MLB Stats API — Career BA vs Today's Pitcher ──────────────────────
def step1_mlb_api(date_str):
    """
    Fetch today's probable pitchers via MLB Stats API,
    then check every active roster batter's career BA vs that pitcher.
    Returns players with min 4 AB and .250+ BA.
    """
    # 1. Get today's schedule + probable pitchers
    try:
        resp = requests.get(
            f"{MLB_API}/schedule",
            params={"sportId": 1, "date": date_str,
                    "hydrate": "probablePitcher,team,lineups"},
            timeout=15
        )
        data = resp.json()
    except Exception:
        return []

    games = []
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            ht = game.get("teams", {}).get("home", {})
            at = game.get("teams", {}).get("away", {})
            hp = ht.get("probablePitcher", {})
            ap = at.get("probablePitcher", {})
            games.append({
                "home_team_id":   ht.get("team", {}).get("id"),
                "home_team_name": ht.get("team", {}).get("name", ""),
                "away_team_id":   at.get("team", {}).get("id"),
                "away_team_name": at.get("team", {}).get("name", ""),
                "home_pitcher_id":   hp.get("id"),
                "home_pitcher_name": hp.get("fullName", "TBD"),
                "away_pitcher_id":   ap.get("id"),
                "away_pitcher_name": ap.get("fullName", "TBD"),
            })

    if not games:
        return []

    players = []
    seen_ids = set()

    for game in games:
        # Home batters face Away pitcher; Away batters face Home pitcher
        matchups = [
            {
                "team_id":   game["home_team_id"],
                "team_name": game["home_team_name"],
                "side":      "HOME",
                "opp_team":  game["away_team_name"],
                "opp_slug":  TEAM_SLUGS.get(game["away_team_name"],
                             game["away_team_name"].lower().split()[-1] if game["away_team_name"] else ""),
                "pitcher_id":   game["away_pitcher_id"],
                "pitcher_name": game["away_pitcher_name"],
            },
            {
                "team_id":   game["away_team_id"],
                "team_name": game["away_team_name"],
                "side":      "AWAY",
                "opp_team":  game["home_team_name"],
                "opp_slug":  TEAM_SLUGS.get(game["home_team_name"],
                             game["home_team_name"].lower().split()[-1] if game["home_team_name"] else ""),
                "pitcher_id":   game["home_pitcher_id"],
                "pitcher_name": game["home_pitcher_name"],
            },
        ]

        for mu in matchups:
            if not mu["team_id"] or not mu["pitcher_id"]:
                continue

            # Get active roster for batting team
            try:
                rost = requests.get(
                    f"{MLB_API}/teams/{mu['team_id']}/roster",
                    params={"rosterType": "active", "season": SEASON},
                    timeout=10
                ).json().get("roster", [])
            except Exception:
                continue

            for entry in rost:
                person = entry.get("person", {})
                pid    = person.get("id")
                fname  = person.get("fullName", "")
                pos    = entry.get("position", {}).get("abbreviation", "")

                # Skip pitchers and already-seen players
                if pos in PITCHER_POSITIONS or not pid or pid in seen_ids:
                    continue

                # Career stats vs this pitcher
                try:
                    s = requests.get(
                        f"{MLB_API}/people/{pid}/stats",
                        params={
                            "stats": "vsPlayer",
                            "opposingPlayerId": mu["pitcher_id"],
                            "group": "hitting",
                            "sportId": 1,
                        },
                        timeout=8
                    ).json()
                    splits = s.get("stats", [{}])[0].get("splits", [])
                    if not splits:
                        time.sleep(0.08)
                        continue
                    stat = splits[0].get("stat", {})
                    ab   = stat.get("atBats", 0)
                    h    = stat.get("hits", 0)
                    hr   = stat.get("homeRuns", 0)
                    avg  = stat.get("avg", ".000")
                    try:
                        ba = float(avg) if avg and avg != ".---" else (h/ab if ab else 0.0)
                    except Exception:
                        ba = h / ab if ab else 0.0

                    if ab >= MIN_AB and ba >= MIN_BA:
                        seen_ids.add(pid)
                        players.append({
                            "name":        fname,
                            "full_name":   fname,
                            "pos":         pos,
                            "pitcher":     mu["pitcher_name"],
                            "ab":          ab,
                            "h":           h,
                            "hr":          hr,
                            "step1_ba":    ba,
                            "team_name":   mu["team_name"],
                            "side":        mu["side"],
                            "opp_team":    mu["opp_team"],
                            "opp_slug":    mu["opp_slug"],
                            "slug":        name_to_slug(fname),
                        })
                    time.sleep(0.08)
                except Exception:
                    time.sleep(0.08)
                    continue

    players.sort(key=lambda x: x["step1_ba"], reverse=True)
    return players

# ── ESPN Schedule ──────────────────────────────────────────────────────────────
def get_espn_schedule(date_nodash):
    url = (f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
           f"?dates={date_nodash}")
    try:
        data   = requests.get(url, timeout=12).json()
        events = data.get("events", [])
    except Exception:
        return {}, {}, []
    matchups = {}; team_to_side = {}
    for event in events:
        comps = event.get("competitions",[{}])[0].get("competitors",[])
        if len(comps) != 2: continue
        home = comps[0]["team"]["displayName"]
        away = comps[1]["team"]["displayName"]
        matchups[home]=away; matchups[away]=home
        team_to_side[home]="HOME"; team_to_side[away]="AWAY"
    return matchups, team_to_side, events

# ── StatMuse Fetcher (Steps 2 & 3) ────────────────────────────────────────────
def _statmuse_fetch(url):
    try:
        r    = requests.get(url, headers=BROWSER_HEADERS, timeout=14)
        soup = BeautifulSoup(r.text, "html.parser")
        td   = soup.find("td", class_=lambda c: c and "bg-team-primary" in " ".join(c))
        if td:
            m = re.match(r"^\.(\d{3})$", td.get_text(strip=True))
            return (float(f"0.{m.group(1)}") if m else None), "CAREER"
        for tag in soup.find_all(True):
            txt = tag.get_text(strip=True)
            if re.match(r"^\.\d{3}$", txt):
                return float(f"0.{txt[1:]}"), "LIVE"
        return None, "N/A"
    except Exception:
        return None, "ERROR"

def _cascade(urls):
    for url, label in urls:
        ba, source = _statmuse_fetch(url)
        if source == "CAREER":
            return {"ba":None,"display":"CAREER","flag":"⚠️","passed":None}
        if source == "LIVE" and ba is not None:
            flag = "✅" if label=="LAST-10" else "✅(3+g)"
            return {"ba":ba,"display":f".{int(round(ba*1000)):03d}","flag":flag,"passed":True}
        time.sleep(0.4)
    return {"ba":None,"display":"N/A","flag":"N/A","passed":None}

def fetch_step2(slug, side, opp_slug):
    sw = "away" if side=="AWAY" else "home"
    return _cascade([
        (f"https://www.statmuse.com/mlb/ask/{slug}-batting-average-in-last-10-{sw}-games-vs-{opp_slug}","LAST-10"),
        (f"https://www.statmuse.com/mlb/ask/{slug}-batting-average-in-last-3-{sw}-games-vs-{opp_slug}","MIN-3"),
    ])

def fetch_step3(slug, side):
    sw = "away" if side=="AWAY" else "home"
    return _cascade([
        (f"https://www.statmuse.com/mlb/ask/{slug}-batting-average-in-last-10-{sw}-games-in-{SEASON}","LAST-10"),
        (f"https://www.statmuse.com/mlb/ask/{slug}-batting-average-in-last-3-{sw}-games-in-{SEASON}","MIN-3"),
    ])

# ── ESPN Day/Night Filter (Step 4) ────────────────────────────────────────────
def get_game_time_type(team_name, events):
    team_lower = team_name.lower()
    for event in events:
        comps = event.get("competitions",[{}])[0]
        tnames = [t["team"]["displayName"] for t in comps.get("competitors",[])]
        if (team_name in tnames or
                any(team_lower in tn.lower() or tn.lower() in team_lower for tn in tnames)):
            gd = event.get("date","")
            if gd:
                dt = datetime.fromisoformat(gd.replace("Z","+00:00"))
                return "night" if (dt.hour >= 21 or dt.hour <= 5) else "day"
    return "unknown"

def find_espn_id(full_name):
    try:
        url  = (f"https://site.web.api.espn.com/apis/search/v2"
                f"?query={full_name.replace(' ','+')}&limit=5&sport=mlb")
        data = requests.get(url, headers=BROWSER_HEADERS, timeout=8).json()
        tgt  = normalize(full_name)
        for result in data.get("results",[]):
            if result.get("type") != "player": continue
            for c in result.get("contents",[]):
                if normalize(c.get("displayName","")) == tgt:
                    m = re.search(r"a:(\d+)", c.get("uid",""))
                    if m: return m.group(1)
                    m2 = re.search(r"/id/(\d+)", c.get("link",{}).get("web",""))
                    if m2: return m2.group(1)
    except Exception:
        pass
    return None

def fetch_dn_ba(espn_id, game_type):
    if not espn_id:
        return {"ba":None,"ab":None,"display":"N/A","flag":"N/A","dq":False}
    label = "Day" if game_type=="day" else "Night"
    other = "Night" if label=="Day" else "Day"
    try:
        url  = (f"https://site.web.api.espn.com/apis/common/v3/sports/baseball"
                f"/mlb/athletes/{espn_id}/splits")
        data = requests.get(url, headers=BROWSER_HEADERS, timeout=10).json()
        for cat in data.get("splitCategories",[]):
            if cat.get("displayName") != "Breakdown": continue
            splits = cat.get("splits",[])
            for try_label in [label, other]:
                for s in splits:
                    if s.get("displayName") != try_label: continue
                    stats = s.get("stats",[])
                    if len(stats) > 12:
                        try:
                            ba  = float(stats[12]); ab = int(stats[0])
                            dq  = ba < MIN_DN_BA
                            used_fb = try_label != label
                            return {"ba":ba,"ab":ab,"display":stats[12],
                                    "flag":(f"❌{stats[12]}<.200" if dq
                                            else f"✅{' ('+other+')' if used_fb else ''}"),
                                    "dq":dq}
                        except (ValueError,TypeError):
                            pass
        return {"ba":None,"ab":None,"display":"N/A","flag":"N/A","dq":False}
    except Exception:
        return {"ba":None,"ab":None,"display":"N/A","flag":"ERR","dq":False}

# ── Main Algorithm ─────────────────────────────────────────────────────────────
def run_algorithm(date_str, job_id):
    def upd(step, msg):
        JOBS[job_id].update({"step":step,"message":msg})

    try:
        date_nodash = date_str.replace("-","")

        # STEP 1 — MLB Stats API (career BA vs today's probable pitcher)
        upd(1,"Step 1: Checking career BA vs today's pitchers (MLB Stats API)...")
        step1 = step1_mlb_api(date_str)

        if not step1:
            JOBS[job_id].update({"status":"done","result":{
                "date":date_str,"top9":[],"eliminated":{"step2":[],"step3":[],"step4":[]},
                "summary":{"step1_count":0,"step2_eliminated":0,"step3_eliminated":0,
                            "step4_dq":0,"final_pool":0},
                "note":"No players qualified in Step 1 (min 4 AB, .250+ vs today's pitcher)."}})
            return
        upd(1, f"Step 1 done: {len(step1)} players qualify")
        time.sleep(0.3)

        # ESPN schedule (needed for Step 4 day/night filter)
        upd(1,"Loading ESPN schedule...")
        matchups, team_to_side, events = get_espn_schedule(date_nodash)

        # STEP 2
        upd(2,"Running Step 2: Lifetime H/A last 10 vs opponent...")
        s2_pass, s2_fail = [], []
        for i,p in enumerate(step1):
            upd(2, f"Step 2 ({i+1}/{len(step1)}): {p['name']}...")
            if not p.get("opp_slug"):
                p["step2_ba"]=None; p["step2_flag"]="N/A"; s2_pass.append(p); continue
            res = fetch_step2(p["slug"], p["side"], p["opp_slug"])
            p["step2_ba"]=res.get("ba"); p["step2_flag"]=res.get("flag","N/A")
            (s2_pass if (res["ba"] is None or res["ba"]>=MIN_STEP2_BA) else s2_fail).append(p)
            time.sleep(0.55)
        upd(2, f"Step 2 done: {len(s2_pass)} pass, {len(s2_fail)} eliminated")
        time.sleep(0.3)

        # STEP 3
        upd(3,f"Running Step 3: {SEASON} H/A last 10 vs all teams...")
        s3_pass, s3_fail = [], []
        for i,p in enumerate(s2_pass):
            upd(3, f"Step 3 ({i+1}/{len(s2_pass)}): {p['name']}...")
            res = fetch_step3(p["slug"], p["side"])
            p["step3_ba"]=res.get("ba"); p["step3_flag"]=res.get("flag","N/A")
            (s3_pass if (res["ba"] is None or res["ba"]>=MIN_STEP3_BA) else s3_fail).append(p)
            time.sleep(0.55)
        upd(3, f"Step 3 done: {len(s3_pass)} pass, {len(s3_fail)} eliminated")
        time.sleep(0.3)

        # STEP 4
        upd(4,"Running Step 4: ESPN Day/Night BA filter...")
        s4_pass, s4_dq = [], []
        for i,p in enumerate(s3_pass):
            upd(4, f"Step 4 ({i+1}/{len(s3_pass)}): {p['name']}...")
            gt  = get_game_time_type(p.get("team_name",""), events)
            eid = find_espn_id(p["full_name"])
            time.sleep(0.25)
            if gt=="unknown" or not eid:
                p.update({"dn_display":"N/A","dn_type":"?","dn_flag":"N/A"})
                s4_pass.append(p); continue
            res = fetch_dn_ba(eid, gt)
            time.sleep(0.3)
            p.update({"dn_display":res.get("display","N/A"),
                       "dn_type":"DAY" if gt=="day" else "NIGHT",
                       "dn_flag":res.get("flag","N/A")})
            if res.get("dq"):
                s4_dq.append(p)
            else:
                s4_pass.append(p)
        upd(4, f"Step 4 done: {len(s4_pass)} pass, {len(s4_dq)} DQ'd")

        # Scoring
        def fmt(v): return f".{int(round(v*1000)):03d}" if v is not None else "N/A"
        for p in s4_pass:
            p["score"] = round((p.get("step1_ba") or 0)+(p.get("step2_ba") or 0)+(p.get("step3_ba") or 0),3)
        s4_pass.sort(key=lambda x: x["score"], reverse=True)
        top9 = [{
            "rank":i+1,"name":p["name"],"full_name":p["full_name"],
            "pos":p["pos"],"pitcher":p["pitcher"],"team":p.get("team_name",""),
            "side":p.get("side",""),"step1_ba":fmt(p.get("step1_ba")),
            "step2_ba":fmt(p.get("step2_ba")),"step3_ba":fmt(p.get("step3_ba")),
            "dn":p.get("dn_display","N/A"),"dn_type":p.get("dn_type","?"),
            "score":p["score"],
        } for i,p in enumerate(s4_pass[:9])]

        JOBS[job_id].update({"status":"done","result":{
            "date":date_str,"top9":top9,
            "eliminated":{"step2":[p["name"] for p in s2_fail],
                           "step3":[p["name"] for p in s3_fail],
                           "step4":[p["name"] for p in s4_dq]},
            "summary":{"step1_count":len(step1),"step2_eliminated":len(s2_fail),
                        "step3_eliminated":len(s3_fail),"step4_dq":len(s4_dq),
                        "final_pool":len(s4_pass)},
        }})

    except Exception as e:
        JOBS[job_id].update({"status":"error","error":str(e)})

# ── API Endpoints ──────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status":"ok","server":"MoneyBall API","version":"1.0.0"}

@app.post("/api/picks")
def start_picks(date: str):
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD")
    job_id = str(uuid.uuid4())[:8]
    JOBS[job_id] = {"status":"running","step":0,"message":"Starting...","result":None,"error":None}
    threading.Thread(target=run_algorithm, args=(date, job_id), daemon=True).start()
    return {"job_id": job_id}

@app.get("/api/picks/{job_id}")
def get_picks(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(status_code=404, detail="Job not found")
    return JOBS[job_id]

@app.delete("/api/picks/{job_id}")
def cancel_picks(job_id: str):
    JOBS.pop(job_id, None)
    return {"deleted": job_id}
