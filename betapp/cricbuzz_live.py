import re
import requests

API_URL = "https://www.cricbuzz.com/api/mcenter/livescore/{match_id}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.cricbuzz.com/",
    "Origin": "https://www.cricbuzz.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def fetch_score(match_id: str) -> dict | None:
    url = API_URL.format(match_id=match_id)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        print(f"[Cricbuzz] status={resp.status_code}")
    except Exception as e:
        print(f"[Cricbuzz] fetch error: {e}")
    return None
def parse_live_data(raw_json: dict, source_match_id: str | None = None) -> dict:
    """
    Parse Cricbuzz live JSON into flat structure for Redis / predictor.

    FIXED:
    - wickets now comes correctly from miniscore.batTeam.teamWkts
    - score now stored as "runs/wickets" format
    - safe fallback handling added
    """

    if not raw_json:
        return {}

    miniscore = raw_json.get("miniscore", {}) or {}
    match_score = miniscore.get("matchScoreDetails", {}) or {}
    innings_list = match_score.get("inningsScoreList", []) or []
    commentary_list = raw_json.get("commentaryList", []) or []
    latest_perf = miniscore.get("latestPerformance", []) or []
    pp_data = miniscore.get("ppData", {}) or {}
    toss_results = match_score.get("tossResults", {}) or {}
    match_info = match_score.get("matchTeamInfo", []) or []

    # ---------------------------------------------------------
    # Basic score data
    # ---------------------------------------------------------
    innings = miniscore.get("inningsId", 1) or 1
    bat_team = miniscore.get("batTeam", {}) or {}

    team_score = bat_team.get("teamScore", 0) or 0
    team_wkts = bat_team.get("teamWkts", 0) or 0
    overs = miniscore.get("overs", 0) or 0

    # IMPORTANT FIX
    score = f"{team_score}/{team_wkts}"

    # ---------------------------------------------------------
    # Batsmen
    # ---------------------------------------------------------
    striker = miniscore.get("batsmanStriker", {}) or {}
    non_striker = miniscore.get("batsmanNonStriker", {}) or {}

    # ---------------------------------------------------------
    # Bowlers
    # ---------------------------------------------------------
    bowler_1 = miniscore.get("bowlerStriker", {}) or {}
    bowler_2 = miniscore.get("bowlerNonStriker", {}) or {}

    # ---------------------------------------------------------
    # Match/toss/status
    # ---------------------------------------------------------
    toss_winner = toss_results.get("tossWinnerName", "")
    toss_decision = toss_results.get("decision", "")
    toss = ""
    if toss_winner and toss_decision:
        toss = f"{toss_winner} won toss · chose to {str(toss_decision).lower()}"

    status = raw_json.get("status") or miniscore.get("status") or match_score.get("customStatus") or ""
    state = match_score.get("state", "")

    # ---------------------------------------------------------
    # Last ball / recent
    # ---------------------------------------------------------
    latest_ball = ""
    p_runs = 0
    p_balls = 0
    recent = miniscore.get("recentOvsStats", "") or ""

    if commentary_list:
        latest_comment = commentary_list[0] or {}
        latest_ball = latest_comment.get("commText", "") or ""

        partnership = miniscore.get("partnerShip", {}) or {}
        p_runs = partnership.get("runs", 0) or 0
        p_balls = partnership.get("balls", 0) or 0

    # ---------------------------------------------------------
    # Run rates
    # ---------------------------------------------------------
    crr = miniscore.get("currentRunRate", 0) or 0
    rrr = miniscore.get("requiredRunRate", 0) or 0
    rem_runs_to_win = miniscore.get("remRunsToWin", 0) or 0

    # ---------------------------------------------------------
    # Powerplay
    # ---------------------------------------------------------
    pp_from = 0.1
    pp_to = 6
    pp_runs = 0

    if "pp_1" in pp_data:
        pp1 = pp_data.get("pp_1", {}) or {}
        pp_from = pp1.get("ppOversFrom", 0.1) or 0.1
        pp_to = pp1.get("ppOversTo", 6) or 6
        pp_runs = pp1.get("runsScored", 0) or 0

    # ---------------------------------------------------------
    # Last 3 / last 5 overs performance
    # ---------------------------------------------------------
    last5_runs = 0.0
    last5_wkts = 0.0
    last3_runs = 0.0

    for item in latest_perf:
        label = str(item.get("label", "")).lower()
        if "last 5" in label:
            last5_runs = float(item.get("runs", 0) or 0)
            last5_wkts = float(item.get("wkts", 0) or 0)
        elif "last 3" in label:
            last3_runs = float(item.get("runs", 0) or 0)

    # ---------------------------------------------------------
    # Target (innings 2 only)
    # ---------------------------------------------------------
    target = 0
    if innings == 2 and innings_list:
        first_innings = innings_list[0] if len(innings_list) > 0 else {}
        first_score = first_innings.get("score", 0) or 0
        target = first_score + 1

    # ---------------------------------------------------------
    # Phase
    # ---------------------------------------------------------
    try:
        overs_float = float(overs)
    except Exception:
        overs_float = 0.0

    if overs_float <= 6:
        phase = "powerplay"
    elif overs_float <= 15:
        phase = "middle"
    else:
        phase = "death"

    # ---------------------------------------------------------
    # Innings type
    # ---------------------------------------------------------
    innings_type = "defending" if innings == 1 else "chasing"

    # ---------------------------------------------------------
    # Build parsed response
    # ---------------------------------------------------------
    parsed_data = {
        "score": score,                      # FIXED -> e.g. "181/5"
        "score_num": team_score,
        "wickets": team_wkts,               # FIXED -> correct wickets
        "overs": str(overs),
        "overs_float": overs_float,
        "crr": float(crr),
        "rrr": float(rrr),
        "innings": int(innings),
        "status": status,
        "state": state,
        "toss": toss,
        "target": int(target),

        # batsmen
        "b1_name": striker.get("batName", ""),
        "b1_runs": striker.get("batRuns", 0) or 0,
        "b1_balls": striker.get("batBalls", 0) or 0,
        "b1_4s": striker.get("batFours", 0) or 0,
        "b1_6s": striker.get("batSixes", 0) or 0,
        "b1_sr": striker.get("batStrikeRate", 0) or 0,
        "b1_dots": striker.get("batDots", 0) or 0,

        "b2_name": non_striker.get("batName", ""),
        "b2_runs": non_striker.get("batRuns", 0) or 0,
        "b2_balls": non_striker.get("batBalls", 0) or 0,
        "b2_4s": non_striker.get("batFours", 0) or 0,
        "b2_6s": non_striker.get("batSixes", 0) or 0,
        "b2_sr": non_striker.get("batStrikeRate", 0) or 0,
        "b2_dots": non_striker.get("batDots", 0) or 0,

        # bowlers
        "bw1_name": bowler_1.get("bowlName", ""),
        "bw1_overs": bowler_1.get("bowlOvs", 0) or 0,
        "bw1_runs": bowler_1.get("bowlRuns", 0) or 0,
        "bw1_wkts": bowler_1.get("bowlWkts", 0) or 0,
        "bw1_eco": bowler_1.get("bowlEcon", 0) or 0,

        "bw2_name": bowler_2.get("bowlName", ""),
        "bw2_overs": bowler_2.get("bowlOvs", 0) or 0,
        "bw2_runs": bowler_2.get("bowlRuns", 0) or 0,
        "bw2_wkts": bowler_2.get("bowlWkts", 0) or 0,
        "bw2_eco": bowler_2.get("bowlEcon", 0) or 0,

        # match extras
        "p_runs": p_runs,
        "p_balls": p_balls,
        "recent": recent,
        "last5_runs": last5_runs,
        "last5_wkts": last5_wkts,
        "last3_runs": last3_runs,
        "pp_from": pp_from,
        "pp_to": pp_to,
        "pp_runs": pp_runs,
        "latest_ball": latest_ball,
        "phase": phase,
        "innings_type": innings_type,
        "striker_name": striker.get("batName", ""),
        "source_match_id": str(source_match_id) if source_match_id else "",
        "raw_json": raw_json,
    }

    # ---------------------------------------------------------
    # Debug print
    # ---------------------------------------------------------
    print(
        f"[parse_live_data] score={parsed_data['score']} "
        f"wkts={parsed_data['wickets']} "
        f"overs={parsed_data['overs']} "
        f"latest_ball={parsed_data['latest_ball']}"
    )

    return parsed_data

# def parse_live_data(raw: dict) -> dict:
#     ms = raw.get("miniscore", {})
#     if not ms:
#         return {}

#     bat_team = ms.get("batTeam", {})
#     score = bat_team.get("teamScore", 0)
#     wickets = bat_team.get("teamWkts", 0)
#     overs = ms.get("overs", 0)
#     crr = ms.get("currentRunRate", 0)
#     rrr = ms.get("requiredRunRate", 0)

#     msd = ms.get("matchScoreDetails", {})
#     toss = msd.get("tossResults", {})
#     toss_team = toss.get("tossWinnerName", "")
#     toss_dec = toss.get("decision", "")
#     status = msd.get("customStatus", ms.get("status", ""))
#     state = msd.get("state", "")

#     innings_list = msd.get("inningsScoreList", [])
#     target = 0
#     innings = len(innings_list) if innings_list else 1
#     if len(innings_list) >= 2:
#         first = innings_list[0]
#         target = int(first.get("score", 0) or 0) + 1

#     striker = ms.get("batsmanStriker", {})
#     non_striker = ms.get("batsmanNonStriker", {})
#     bowl_striker = ms.get("bowlerStriker", {})
#     bowl_non_striker = ms.get("bowlerNonStriker", {})
#     partnership = ms.get("partnerShip", {})
#     recent = ms.get("recentOvsStats", "")

#     perf = ms.get("latestPerformance", [])
#     perf_map = {p.get("label", ""): p for p in perf if isinstance(p, dict)}

#     pp_data = ms.get("ppData", {})
#     pp1 = pp_data.get("pp_1", {})

#     comm_list = raw.get("commentaryList", [])
#     latest_ball = ""
#     if comm_list:
#         c = comm_list[0]
#         latest_ball = c.get("commText", "")
#         latest_ball = re.sub(r"[A-Z]\d+\$,?\s*", "", latest_ball).strip()
#         if len(latest_ball) > 200:
#             latest_ball = latest_ball[:200] + "…"

#     return {
#         "score": int(score or 0),
#         "wickets": int(wickets or 0),
#         "overs": str(overs),
#         "crr": float(crr or 0),
#         "rrr": float(rrr or 0),
#         "innings": int(innings or 1),
#         "status": status,
#         "state": state,
#         "toss": f"{toss_team} won toss · chose to {toss_dec.lower()}" if toss_team else "",
#         "target": int(target or 0),

#         "b1_name": striker.get("batName", ""),
#         "b1_runs": int(striker.get("batRuns", 0) or 0),
#         "b1_balls": int(striker.get("batBalls", 0) or 0),
#         "b1_4s": int(striker.get("batFours", 0) or 0),
#         "b1_6s": int(striker.get("batSixes", 0) or 0),
#         "b1_sr": float(striker.get("batStrikeRate", 0) or 0),
#         "b1_dots": int(striker.get("batDots", 0) or 0),

#         "b2_name": non_striker.get("batName", ""),
#         "b2_runs": int(non_striker.get("batRuns", 0) or 0),
#         "b2_balls": int(non_striker.get("batBalls", 0) or 0),
#         "b2_4s": int(non_striker.get("batFours", 0) or 0),
#         "b2_6s": int(non_striker.get("batSixes", 0) or 0),
#         "b2_sr": float(non_striker.get("batStrikeRate", 0) or 0),
#         "b2_dots": int(non_striker.get("batDots", 0) or 0),

#         "bw1_name": bowl_striker.get("bowlName", ""),
#         "bw1_overs": bowl_striker.get("bowlOvs", 0),
#         "bw1_runs": int(bowl_striker.get("bowlRuns", 0) or 0),
#         "bw1_wkts": int(bowl_striker.get("bowlWkts", 0) or 0),
#         "bw1_eco": float(bowl_striker.get("bowlEcon", 0) or 0),

#         "bw2_name": bowl_non_striker.get("bowlName", ""),
#         "bw2_overs": bowl_non_striker.get("bowlOvs", 0),
#         "bw2_runs": int(bowl_non_striker.get("bowlRuns", 0) or 0),
#         "bw2_wkts": int(bowl_non_striker.get("bowlWkts", 0) or 0),
#         "bw2_eco": float(bowl_non_striker.get("bowlEcon", 0) or 0),

#         "p_runs": int(partnership.get("runs", 0) or 0),
#         "p_balls": int(partnership.get("balls", 0) or 0),

#         "recent": recent,
#         "last5_runs": float(perf_map.get("Last 5 overs", {}).get("runs", 0) or 0),
#         "last5_wkts": float(perf_map.get("Last 5 overs", {}).get("wkts", 0) or 0),
#         "last3_runs": float(perf_map.get("Last 3 overs", {}).get("runs", 0) or 0),

#         "pp_from": pp1.get("ppOversFrom", ""),
#         "pp_to": pp1.get("ppOversTo", ""),
#         "pp_runs": pp1.get("runsScored", ""),

#         "latest_ball": latest_ball,
#     }


def get_live_payload(match_id: str) -> dict:
    raw = fetch_score(match_id)
    if not raw:
        return {}
    return parse_live_data(raw)