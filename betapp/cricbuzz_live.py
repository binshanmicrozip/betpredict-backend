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


def parse_live_data(raw: dict) -> dict:
    ms = raw.get("miniscore", {})
    if not ms:
        return {}

    bat_team = ms.get("batTeam", {})
    score = bat_team.get("teamScore", 0)
    wickets = bat_team.get("teamWkts", 0)
    overs = ms.get("overs", 0)
    crr = ms.get("currentRunRate", 0)
    rrr = ms.get("requiredRunRate", 0)

    msd = ms.get("matchScoreDetails", {})
    toss = msd.get("tossResults", {})
    toss_team = toss.get("tossWinnerName", "")
    toss_dec = toss.get("decision", "")
    status = msd.get("customStatus", ms.get("status", ""))
    state = msd.get("state", "")

    innings_list = msd.get("inningsScoreList", [])
    target = 0
    innings = len(innings_list) if innings_list else 1
    if len(innings_list) >= 2:
        first = innings_list[0]
        target = int(first.get("score", 0) or 0) + 1

    striker = ms.get("batsmanStriker", {})
    non_striker = ms.get("batsmanNonStriker", {})
    bowl_striker = ms.get("bowlerStriker", {})
    bowl_non_striker = ms.get("bowlerNonStriker", {})
    partnership = ms.get("partnerShip", {})
    recent = ms.get("recentOvsStats", "")

    perf = ms.get("latestPerformance", [])
    perf_map = {p.get("label", ""): p for p in perf if isinstance(p, dict)}

    pp_data = ms.get("ppData", {})
    pp1 = pp_data.get("pp_1", {})

    comm_list = raw.get("commentaryList", [])
    latest_ball = ""
    if comm_list:
        c = comm_list[0]
        latest_ball = c.get("commText", "")
        latest_ball = re.sub(r"[A-Z]\d+\$,?\s*", "", latest_ball).strip()
        if len(latest_ball) > 200:
            latest_ball = latest_ball[:200] + "…"

    return {
        "score": f"{score}/{wickets}",
        "overs": str(overs),
        "crr": float(crr or 0),
        "rrr": float(rrr or 0),
        "innings": int(innings or 1),
        "status": status,
        "state": state,
        "toss": f"{toss_team} won toss · chose to {toss_dec.lower()}" if toss_team else "",
        "target": int(target or 0),

        "b1_name": striker.get("batName", ""),
        "b1_runs": int(striker.get("batRuns", 0) or 0),
        "b1_balls": int(striker.get("batBalls", 0) or 0),
        "b1_4s": int(striker.get("batFours", 0) or 0),
        "b1_6s": int(striker.get("batSixes", 0) or 0),
        "b1_sr": float(striker.get("batStrikeRate", 0) or 0),
        "b1_dots": int(striker.get("batDots", 0) or 0),

        "b2_name": non_striker.get("batName", ""),
        "b2_runs": int(non_striker.get("batRuns", 0) or 0),
        "b2_balls": int(non_striker.get("batBalls", 0) or 0),
        "b2_4s": int(non_striker.get("batFours", 0) or 0),
        "b2_6s": int(non_striker.get("batSixes", 0) or 0),
        "b2_sr": float(non_striker.get("batStrikeRate", 0) or 0),
        "b2_dots": int(non_striker.get("batDots", 0) or 0),

        "bw1_name": bowl_striker.get("bowlName", ""),
        "bw1_overs": bowl_striker.get("bowlOvs", 0),
        "bw1_runs": int(bowl_striker.get("bowlRuns", 0) or 0),
        "bw1_wkts": int(bowl_striker.get("bowlWkts", 0) or 0),
        "bw1_eco": float(bowl_striker.get("bowlEcon", 0) or 0),

        "bw2_name": bowl_non_striker.get("bowlName", ""),
        "bw2_overs": bowl_non_striker.get("bowlOvs", 0),
        "bw2_runs": int(bowl_non_striker.get("bowlRuns", 0) or 0),
        "bw2_wkts": int(bowl_non_striker.get("bowlWkts", 0) or 0),
        "bw2_eco": float(bowl_non_striker.get("bowlEcon", 0) or 0),

        "p_runs": int(partnership.get("runs", 0) or 0),
        "p_balls": int(partnership.get("balls", 0) or 0),

        "recent": recent,
        "last5_runs": float(perf_map.get("Last 5 overs", {}).get("runs", 0) or 0),
        "last5_wkts": float(perf_map.get("Last 5 overs", {}).get("wkts", 0) or 0),
        "last3_runs": float(perf_map.get("Last 3 overs", {}).get("runs", 0) or 0),

        "pp_from": pp1.get("ppOversFrom", ""),
        "pp_to": pp1.get("ppOversTo", ""),
        "pp_runs": pp1.get("runsScored", ""),

        "latest_ball": latest_ball,
    }


def get_live_payload(match_id: str) -> dict:
    raw = fetch_score(match_id)
    if not raw:
        return {}
    return parse_live_data(raw)