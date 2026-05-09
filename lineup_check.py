"""
Lineup Check -- MLB Stats API
Checks whether each Top 9 player is confirmed in today's starting lineup.
Status: IN_LINEUP / NOT_IN_LINEUP / TBD
"""
import requests

MLB_API = "https://statsapi.mlb.com/api/v1"


def build_lineup_map(date_str):
    if len(date_str) == 8:
        fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    else:
        fmt = date_str
    id_map = {}
    name_map = {}
    teams_with_lineups = set()
    try:
        url  = f"{MLB_API}/schedule?sportId=1&date={fmt}&hydrate=lineups"
        data = requests.get(url, timeout=12).json()
        for date_obj in data.get("dates", []):
            for game in date_obj.get("games", []):
                lineups   = game.get("lineups", {})
                home_pl   = lineups.get("homePlayers", [])
                away_pl   = lineups.get("awayPlayers", [])
                home_name = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
                away_name = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
                if home_pl: teams_with_lineups.add(home_name)
                if away_pl: teams_with_lineups.add(away_name)
                for p in home_pl + away_pl:
                    pid  = p.get("id")
                    name = p.get("fullName", "").lower().strip()
                    if pid:  id_map[int(pid)] = True
                    if name: name_map[name]   = True
    except Exception as e:
        print(f"[lineup_check] Error: {e}")
    return id_map, name_map, teams_with_lineups


def get_lineup_status(player_id, full_name, team_name, id_map, name_map, teams_with_lineups):
    if team_name not in teams_with_lineups:
        return "TBD"
    if player_id and int(player_id) in id_map:
        return "IN_LINEUP"
    if full_name and full_name.lower().strip() in name_map:
        return "IN_LINEUP"
    return "NOT_IN_LINEUP"


def check_top9_lineups(top9, roster, date_str):
    print("[lineup_check] Fetching lineups...")
    id_map, name_map, teams_with_lineups = build_lineup_map(date_str)
    print(f"[lineup_check] Lineups posted for {len(teams_with_lineups)} teams")
    for p in top9:
        name      = p.get("name", "")
        info      = roster.get(name, {})
        player_id = info.get("player_id")
        full_name = p.get("full_name") or info.get("full_name", name)
        team_name = info.get("team_name", "")
        status    = get_lineup_status(player_id, full_name, team_name, id_map, name_map, teams_with_lineups)
        p["lineup_status"] = status
        print(f"  {full_name:<25} -> {status}")
    return top9
