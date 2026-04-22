# import re
# import requests

# API_URL = "https://www.cricbuzz.com/api/mcenter/livescore/{match_id}"

# HEADERS = {
#     "User-Agent": (
#         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#         "AppleWebKit/537.36 (KHTML, like Gecko) "
#         "Chrome/124.0.0.0 Safari/537.36"
#     ),
#     "Accept": "application/json, text/plain, */*",
#     "Accept-Language": "en-US,en;q=0.9",
#     "Referer": "https://www.cricbuzz.com/",
#     "Origin": "https://www.cricbuzz.com",
#     "Cache-Control": "no-cache",
#     "Pragma": "no-cache",
# }


# def fetch_score(match_id: str) -> dict | None:
#     url = API_URL.format(match_id=match_id)
#     try:
#         resp = requests.get(url, headers=HEADERS, timeout=10)
#         if resp.status_code == 200:
#             return resp.json()
#         print(f"[Cricbuzz] status={resp.status_code}")
#     except Exception as e:
#         print(f"[Cricbuzz] fetch error: {e}")
#     return None
# def parse_live_data(raw_json: dict, source_match_id: str | None = None) -> dict:
#     """
#     Parse Cricbuzz live JSON into flat structure for Redis / predictor.

#     FIXED:
#     - wickets now comes correctly from miniscore.batTeam.teamWkts
#     - score now stored as "runs/wickets" format
#     - safe fallback handling added
#     """

#     if not raw_json:
#         return {}

#     miniscore = raw_json.get("miniscore", {}) or {}
#     match_score = miniscore.get("matchScoreDetails", {}) or {}
#     innings_list = match_score.get("inningsScoreList", []) or []
#     commentary_list = raw_json.get("commentaryList", []) or []
#     latest_perf = miniscore.get("latestPerformance", []) or []
#     pp_data = miniscore.get("ppData", {}) or {}
#     toss_results = match_score.get("tossResults", {}) or {}
#     match_info = match_score.get("matchTeamInfo", []) or []

#     # ---------------------------------------------------------
#     # Basic score data
#     # ---------------------------------------------------------
#     innings = miniscore.get("inningsId", 1) or 1
#     bat_team = miniscore.get("batTeam", {}) or {}

#     team_score = bat_team.get("teamScore", 0) or 0
#     team_wkts = bat_team.get("teamWkts", 0) or 0
#     overs = miniscore.get("overs", 0) or 0

#     # IMPORTANT FIX
#     score = f"{team_score}/{team_wkts}"

#     # ---------------------------------------------------------
#     # Batsmen
#     # ---------------------------------------------------------
#     striker = miniscore.get("batsmanStriker", {}) or {}
#     non_striker = miniscore.get("batsmanNonStriker", {}) or {}

#     # ---------------------------------------------------------
#     # Bowlers
#     # ---------------------------------------------------------
#     bowler_1 = miniscore.get("bowlerStriker", {}) or {}
#     bowler_2 = miniscore.get("bowlerNonStriker", {}) or {}

#     # ---------------------------------------------------------
#     # Match/toss/status
#     # ---------------------------------------------------------
#     toss_winner = toss_results.get("tossWinnerName", "")
#     toss_decision = toss_results.get("decision", "")
#     toss = ""
#     if toss_winner and toss_decision:
#         toss = f"{toss_winner} won toss · chose to {str(toss_decision).lower()}"

#     status = raw_json.get("status") or miniscore.get("status") or match_score.get("customStatus") or ""
#     state = match_score.get("state", "")

#     # ---------------------------------------------------------
#     # Last ball / recent
#     # ---------------------------------------------------------
#     latest_ball = ""
#     p_runs = 0
#     p_balls = 0
#     recent = miniscore.get("recentOvsStats", "") or ""

#     if commentary_list:
#         latest_comment = commentary_list[0] or {}
#         latest_ball = latest_comment.get("commText", "") or ""

#         partnership = miniscore.get("partnerShip", {}) or {}
#         p_runs = partnership.get("runs", 0) or 0
#         p_balls = partnership.get("balls", 0) or 0

#     # ---------------------------------------------------------
#     # Run rates
#     # ---------------------------------------------------------
#     crr = miniscore.get("currentRunRate", 0) or 0
#     rrr = miniscore.get("requiredRunRate", 0) or 0
#     rem_runs_to_win = miniscore.get("remRunsToWin", 0) or 0

#     # ---------------------------------------------------------
#     # Powerplay
#     # ---------------------------------------------------------
#     pp_from = 0.1
#     pp_to = 6
#     pp_runs = 0

#     if "pp_1" in pp_data:
#         pp1 = pp_data.get("pp_1", {}) or {}
#         pp_from = pp1.get("ppOversFrom", 0.1) or 0.1
#         pp_to = pp1.get("ppOversTo", 6) or 6
#         pp_runs = pp1.get("runsScored", 0) or 0

#     # ---------------------------------------------------------
#     # Last 3 / last 5 overs performance
#     # ---------------------------------------------------------
#     last5_runs = 0.0
#     last5_wkts = 0.0
#     last3_runs = 0.0

#     for item in latest_perf:
#         label = str(item.get("label", "")).lower()
#         if "last 5" in label:
#             last5_runs = float(item.get("runs", 0) or 0)
#             last5_wkts = float(item.get("wkts", 0) or 0)
#         elif "last 3" in label:
#             last3_runs = float(item.get("runs", 0) or 0)

#     # ---------------------------------------------------------
#     # Target (innings 2 only)
#     # ---------------------------------------------------------
#     target = 0
#     if innings == 2 and innings_list:
#         first_innings = innings_list[0] if len(innings_list) > 0 else {}
#         first_score = first_innings.get("score", 0) or 0
#         target = first_score + 1

#     # ---------------------------------------------------------
#     # Phase
#     # ---------------------------------------------------------
#     try:
#         overs_float = float(overs)
#     except Exception:
#         overs_float = 0.0

#     if overs_float <= 6:
#         phase = "powerplay"
#     elif overs_float <= 15:
#         phase = "middle"
#     else:
#         phase = "death"

#     # ---------------------------------------------------------
#     # Innings type
#     # ---------------------------------------------------------
#     innings_type = "defending" if innings == 1 else "chasing"

#     # ---------------------------------------------------------
#     # Build parsed response
#     # ---------------------------------------------------------
#     parsed_data = {
#         "score": score,                      # FIXED -> e.g. "181/5"
#         "score_num": team_score,
#         "wickets": team_wkts,               # FIXED -> correct wickets
#         "overs": str(overs),
#         "overs_float": overs_float,
#         "crr": float(crr),
#         "rrr": float(rrr),
#         "innings": int(innings),
#         "status": status,
#         "state": state,
#         "toss": toss,
#         "target": int(target),

#         # batsmen
#         "b1_name": striker.get("batName", ""),
#         "b1_runs": striker.get("batRuns", 0) or 0,
#         "b1_balls": striker.get("batBalls", 0) or 0,
#         "b1_4s": striker.get("batFours", 0) or 0,
#         "b1_6s": striker.get("batSixes", 0) or 0,
#         "b1_sr": striker.get("batStrikeRate", 0) or 0,
#         "b1_dots": striker.get("batDots", 0) or 0,

#         "b2_name": non_striker.get("batName", ""),
#         "b2_runs": non_striker.get("batRuns", 0) or 0,
#         "b2_balls": non_striker.get("batBalls", 0) or 0,
#         "b2_4s": non_striker.get("batFours", 0) or 0,
#         "b2_6s": non_striker.get("batSixes", 0) or 0,
#         "b2_sr": non_striker.get("batStrikeRate", 0) or 0,
#         "b2_dots": non_striker.get("batDots", 0) or 0,

#         # bowlers
#         "bw1_name": bowler_1.get("bowlName", ""),
#         "bw1_overs": bowler_1.get("bowlOvs", 0) or 0,
#         "bw1_runs": bowler_1.get("bowlRuns", 0) or 0,
#         "bw1_wkts": bowler_1.get("bowlWkts", 0) or 0,
#         "bw1_eco": bowler_1.get("bowlEcon", 0) or 0,

#         "bw2_name": bowler_2.get("bowlName", ""),
#         "bw2_overs": bowler_2.get("bowlOvs", 0) or 0,
#         "bw2_runs": bowler_2.get("bowlRuns", 0) or 0,
#         "bw2_wkts": bowler_2.get("bowlWkts", 0) or 0,
#         "bw2_eco": bowler_2.get("bowlEcon", 0) or 0,

#         # match extras
#         "p_runs": p_runs,
#         "p_balls": p_balls,
#         "recent": recent,
#         "last5_runs": last5_runs,
#         "last5_wkts": last5_wkts,
#         "last3_runs": last3_runs,
#         "pp_from": pp_from,
#         "pp_to": pp_to,
#         "pp_runs": pp_runs,
#         "latest_ball": latest_ball,
#         "phase": phase,
#         "innings_type": innings_type,
#         "striker_name": striker.get("batName", ""),
#         "source_match_id": str(source_match_id) if source_match_id else "",
#         "raw_json": raw_json,
#     }

#     # ---------------------------------------------------------
#     # Debug print
#     # ---------------------------------------------------------
#     print(
#         f"[parse_live_data] score={parsed_data['score']} "
#         f"wkts={parsed_data['wickets']} "
#         f"overs={parsed_data['overs']} "
#         f"latest_ball={parsed_data['latest_ball']}"
#     )

#     return parsed_data



# def get_live_payload(match_id: str) -> dict:
#     raw = fetch_score(match_id)
#     if not raw:
#         return {}
#     return parse_live_data(raw)



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


def _safe_float(value, default=0.0):
    try:
        if value in (None, "", "null"):
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value, default=0):
    try:
        if value in (None, "", "null"):
            return default
        return int(float(value))
    except Exception:
        return default


def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"[A-Z]\d+\$,?\s*", "", str(text)).strip()
    if len(text) > 300:
        text = text[:300] + "…"
    return text


def _extract_team_names(match_score: dict, raw_json: dict) -> tuple[str, str]:
    """
    Try multiple places to find the 2 team names.
    """
    team1 = ""
    team2 = ""

    match_team_info = match_score.get("matchTeamInfo", []) or []
    if isinstance(match_team_info, list) and len(match_team_info) >= 2:
        t1 = match_team_info[0] or {}
        t2 = match_team_info[1] or {}

        team1 = (
            t1.get("teamName")
            or (t1.get("batTeamDetails") or {}).get("batTeamName")
            or (t1.get("team") or {}).get("teamName")
            or ""
        )
        team2 = (
            t2.get("teamName")
            or (t2.get("batTeamDetails") or {}).get("batTeamName")
            or (t2.get("team") or {}).get("teamName")
            or ""
        )

    if not team1 or not team2:
        match_info = raw_json.get("matchInfo", {}) or {}
        if not team1:
            team1 = (match_info.get("team1") or {}).get("teamName", "") or ""
        if not team2:
            team2 = (match_info.get("team2") or {}).get("teamName", "") or ""

    return team1, team2


def _resolve_first_innings_teams(
    toss_winner_name: str,
    toss_decision: str,
    team1: str,
    team2: str,
) -> tuple[str, str]:
    """
    Determine batting first and bowling first before innings starts.
    """
    batting_first_team = ""
    bowling_first_team = ""

    if not toss_winner_name or not toss_decision:
        return batting_first_team, bowling_first_team

    decision = str(toss_decision).strip().lower()

    if decision == "batting":
        batting_first_team = toss_winner_name
        if toss_winner_name == team1:
            bowling_first_team = team2
        elif toss_winner_name == team2:
            bowling_first_team = team1

    elif decision == "bowling":
        bowling_first_team = toss_winner_name
        if toss_winner_name == team1:
            batting_first_team = team2
        elif toss_winner_name == team2:
            batting_first_team = team1

    return batting_first_team, bowling_first_team


def parse_live_data(raw_json: dict, source_match_id: str | None = None) -> dict:
    """
    Parse Cricbuzz live JSON into flat structure for Redis / predictor.

    Includes:
    - toss winner / toss decision
    - batting first / bowling first BEFORE first ball
    - live batting team / bowling team AFTER innings starts
    - latest commentary / phase / run rate / players / target
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

    # ---------------------------------------------------------
    # Toss / match status
    # ---------------------------------------------------------
    toss_winner_id = toss_results.get("tossWinnerId")
    toss_winner_name = toss_results.get("tossWinnerName", "") or ""
    toss_decision = toss_results.get("decision", "") or ""

    toss = ""
    if toss_winner_name and toss_decision:
        toss = f"{toss_winner_name} won toss · chose to {str(toss_decision).lower()}"

    status = (
        raw_json.get("status")
        or miniscore.get("status")
        or match_score.get("customStatus")
        or ""
    )
    state = (
        raw_json.get("state")
        or match_score.get("state")
        or ""
    )

    # ---------------------------------------------------------
    # Team names
    # ---------------------------------------------------------
    team1_name, team2_name = _extract_team_names(match_score, raw_json)

    batting_first_team, bowling_first_team = _resolve_first_innings_teams(
        toss_winner_name=toss_winner_name,
        toss_decision=toss_decision,
        team1=team1_name,
        team2=team2_name,
    )

    # ---------------------------------------------------------
    # Innings / score
    # ---------------------------------------------------------
    innings = miniscore.get("inningsId", 1) or 1
    bat_team = miniscore.get("batTeam", {}) or {}

    team_score = _safe_int(bat_team.get("teamScore", 0), 0)
    team_wkts = _safe_int(bat_team.get("teamWkts", 0), 0)
    overs = miniscore.get("overs", 0) or 0

    score = f"{team_score}/{team_wkts}"

    # ---------------------------------------------------------
    # Live batting/bowling team
    # ---------------------------------------------------------
    batting_team = bat_team.get("teamName", "") or ""
    bowling_team = ""

    if batting_team:
        if batting_team == team1_name:
            bowling_team = team2_name
        elif batting_team == team2_name:
            bowling_team = team1_name

    # BEFORE innings starts, use toss-based prediction
    if not batting_team and not bowling_team:
        batting_team = batting_first_team
        bowling_team = bowling_first_team

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
    # Last ball / commentary
    # ---------------------------------------------------------
    latest_ball = ""
    p_runs = 0
    p_balls = 0
    recent = miniscore.get("recentOvsStats", "") or ""

    if commentary_list:
        latest_comment = commentary_list[0] or {}
        latest_ball = _clean_text(latest_comment.get("commText", "") or "")

    partnership = miniscore.get("partnerShip", {}) or {}
    p_runs = _safe_int(partnership.get("runs", 0), 0)
    p_balls = _safe_int(partnership.get("balls", 0), 0)

    # ---------------------------------------------------------
    # Run rates
    # ---------------------------------------------------------
    crr = _safe_float(miniscore.get("currentRunRate", 0), 0.0)
    rrr = _safe_float(miniscore.get("requiredRunRate", 0), 0.0)
    rem_runs_to_win = _safe_int(miniscore.get("remRunsToWin", 0), 0)

    # ---------------------------------------------------------
    # Powerplay
    # ---------------------------------------------------------
    pp_from = 0.1
    pp_to = 6
    pp_runs = 0

    if "pp_1" in pp_data:
        pp1 = pp_data.get("pp_1", {}) or {}
        pp_from = _safe_float(pp1.get("ppOversFrom", 0.1), 0.1)
        pp_to = _safe_float(pp1.get("ppOversTo", 6), 6)
        pp_runs = _safe_int(pp1.get("runsScored", 0), 0)

    # ---------------------------------------------------------
    # Last 3 / last 5 overs performance
    # ---------------------------------------------------------
    last5_runs = 0.0
    last5_wkts = 0.0
    last3_runs = 0.0

    for item in latest_perf:
        label = str(item.get("label", "")).lower()
        if "last 5" in label:
            last5_runs = _safe_float(item.get("runs", 0), 0.0)
            last5_wkts = _safe_float(item.get("wkts", 0), 0.0)
        elif "last 3" in label:
            last3_runs = _safe_float(item.get("runs", 0), 0.0)

    # ---------------------------------------------------------
    # Target (innings 2 only)
    # ---------------------------------------------------------
    target = 0
    if _safe_int(innings, 1) == 2 and innings_list:
        first_innings = innings_list[0] if len(innings_list) > 0 else {}
        first_score = _safe_int(first_innings.get("score", 0), 0)
        target = first_score + 1

    # ---------------------------------------------------------
    # Phase
    # ---------------------------------------------------------
    overs_float = _safe_float(overs, 0.0)

    if overs_float <= 6:
        phase = "powerplay"
    elif overs_float <= 15:
        phase = "middle"
    else:
        phase = "death"

    # ---------------------------------------------------------
    # Innings type
    # ---------------------------------------------------------
    innings_type = "defending" if _safe_int(innings, 1) == 1 else "chasing"

    # ---------------------------------------------------------
    # Build parsed response
    # ---------------------------------------------------------
    parsed_data = {
        "score": score,
        "score_num": team_score,
        "wickets": team_wkts,
        "overs": str(overs),
        "overs_float": overs_float,
        "crr": crr,
        "rrr": rrr,
        "innings": _safe_int(innings, 1),
        "status": status,
        "state": state,

        "team1_name": team1_name,
        "team2_name": team2_name,

        "toss": toss,
        "toss_winner_id": toss_winner_id,
        "toss_winner_name": toss_winner_name,
        "toss_decision": toss_decision,
        "batting_first_team": batting_first_team,
        "bowling_first_team": bowling_first_team,

        "batting_team": batting_team,
        "bowling_team": bowling_team,

        "target": _safe_int(target, 0),
        "rem_runs_to_win": rem_runs_to_win,

        # batsmen
        "b1_name": striker.get("batName", "") or "",
        "b1_runs": _safe_int(striker.get("batRuns", 0), 0),
        "b1_balls": _safe_int(striker.get("batBalls", 0), 0),
        "b1_4s": _safe_int(striker.get("batFours", 0), 0),
        "b1_6s": _safe_int(striker.get("batSixes", 0), 0),
        "b1_sr": _safe_float(striker.get("batStrikeRate", 0), 0.0),
        "b1_dots": _safe_int(striker.get("batDots", 0), 0),

        "b2_name": non_striker.get("batName", "") or "",
        "b2_runs": _safe_int(non_striker.get("batRuns", 0), 0),
        "b2_balls": _safe_int(non_striker.get("batBalls", 0), 0),
        "b2_4s": _safe_int(non_striker.get("batFours", 0), 0),
        "b2_6s": _safe_int(non_striker.get("batSixes", 0), 0),
        "b2_sr": _safe_float(non_striker.get("batStrikeRate", 0), 0.0),
        "b2_dots": _safe_int(non_striker.get("batDots", 0), 0),

        # bowlers
        "bw1_name": bowler_1.get("bowlName", "") or "",
        "bw1_overs": _safe_float(bowler_1.get("bowlOvs", 0), 0.0),
        "bw1_runs": _safe_int(bowler_1.get("bowlRuns", 0), 0),
        "bw1_wkts": _safe_int(bowler_1.get("bowlWkts", 0), 0),
        "bw1_eco": _safe_float(bowler_1.get("bowlEcon", 0), 0.0),

        "bw2_name": bowler_2.get("bowlName", "") or "",
        "bw2_overs": _safe_float(bowler_2.get("bowlOvs", 0), 0.0),
        "bw2_runs": _safe_int(bowler_2.get("bowlRuns", 0), 0),
        "bw2_wkts": _safe_int(bowler_2.get("bowlWkts", 0), 0),
        "bw2_eco": _safe_float(bowler_2.get("bowlEcon", 0), 0.0),

        # extras
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
        "striker_name": striker.get("batName", "") or "",
        "source_match_id": str(source_match_id) if source_match_id else "",
        "raw_json": raw_json,
    }

    print(
        f"[parse_live_data] "
        f"state={parsed_data['state']} "
        f"status={parsed_data['status']} "
        f"toss_winner={parsed_data['toss_winner_name']} "
        f"decision={parsed_data['toss_decision']} "
        f"bat_first={parsed_data['batting_first_team']} "
        f"bowl_first={parsed_data['bowling_first_team']} "
        f"live_batting={parsed_data['batting_team']} "
        f"live_bowling={parsed_data['bowling_team']} "
        f"score={parsed_data['score']} "
        f"overs={parsed_data['overs']}"
    )

    return parsed_data


def get_live_payload(match_id: str) -> dict:
    raw = fetch_score(match_id)
    if not raw:
        return {}
    return parse_live_data(raw, match_id)