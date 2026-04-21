import json
import time
import redis
from django.conf import settings

r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)


# =========================================================
# CRICKET BALL HISTORY
# =========================================================

def build_ball_history_item(source_match_id: str, raw_json: dict) -> dict | None:
    try:
        commentary_list = raw_json.get("commentaryList", [])
        miniscore = raw_json.get("miniscore", {})

        if not commentary_list:
            return None

        c = commentary_list[0]

        innings_id = c.get("inningsId") or miniscore.get("inningsId")
        over_number = c.get("overNumber")
        ball_nbr = c.get("ballNbr")
        event = c.get("event")
        commentary = c.get("commText", "")
        ts = c.get("timestamp")

        if innings_id is None or over_number is None or ball_nbr is None:
            return None

        striker = miniscore.get("batsmanStriker", {})
        non_striker = miniscore.get("batsmanNonStriker", {})
        bowler = miniscore.get("bowlerStriker", {})
        bat_team = miniscore.get("batTeam", {})
        partnership = miniscore.get("partnerShip", {})
        match_score_details = miniscore.get("matchScoreDetails", {})
        innings_score_list = match_score_details.get("inningsScoreList", [])

        score = bat_team.get("teamScore")
        wickets = bat_team.get("teamWkts")
        overs = miniscore.get("overs")

        if innings_score_list:
            row = innings_score_list[0]
            score = row.get("score", score)
            wickets = row.get("wickets", wickets)
            overs = row.get("overs", overs)

        ball_key = f"{source_match_id}:{innings_id}:{over_number}:{ball_nbr}"

        return {
            "ball_key": ball_key,
            "source_match_id": str(source_match_id),
            "innings": innings_id,
            "over": over_number,
            "ball_number": ball_nbr,
            "event": event,
            "commentary": commentary,
            "score": score,
            "wickets": wickets,
            "overs": overs,
            "current_run_rate": miniscore.get("currentRunRate"),
            "required_run_rate": miniscore.get("requiredRunRate"),
            "recent": miniscore.get("recentOvsStats"),
            "partnership": {
                "runs": partnership.get("runs"),
                "balls": partnership.get("balls"),
            },
            "batter_striker": {
                "name": striker.get("batName"),
                "runs": striker.get("batRuns"),
                "balls": striker.get("batBalls"),
                "fours": striker.get("batFours"),
                "sixes": striker.get("batSixes"),
                "strike_rate": striker.get("batStrikeRate"),
            },
            "batter_non_striker": {
                "name": non_striker.get("batName"),
                "runs": non_striker.get("batRuns"),
                "balls": non_striker.get("batBalls"),
                "fours": non_striker.get("batFours"),
                "sixes": non_striker.get("batSixes"),
                "strike_rate": non_striker.get("batStrikeRate"),
            },
            "bowler": {
                "name": bowler.get("bowlName"),
                "overs": bowler.get("bowlOvs"),
                "runs": bowler.get("bowlRuns"),
                "wickets": bowler.get("bowlWkts"),
                "economy": bowler.get("bowlEcon"),
            },
            "timestamp": ts,
        }
    except Exception as e:
        print("build_ball_history_item error:", e)
        return None


def is_new_ball(source_match_id: str, ball_key: str) -> bool:
    redis_key = f"last_ball_key:{source_match_id}"
    last_key = r.get(redis_key)

    if last_key == ball_key:
        return False

    r.set(redis_key, ball_key)
    return True


def save_ball_history(source_match_id: str, item: dict, max_items: int = 120):
    if not item:
        return
    key = f"history:balls:{source_match_id}"
    r.rpush(key, json.dumps(item))
    r.ltrim(key, -max_items, -1)


def get_ball_history(source_match_id: str, limit: int = 60) -> list:
    key = f"history:balls:{source_match_id}"
    rows = r.lrange(key, -limit, -1)
    data = []

    for row in rows:
        try:
            data.append(json.loads(row))
        except Exception:
            continue

    return data


# =========================================================
# MARKET HISTORY
# =========================================================

def build_market_history_item(market_id: str, runner_id: str, price: dict) -> dict | None:
    try:
        ltp = price.get("ltp")
        prev_ltp = price.get("prev_ltp")
        tv = price.get("tv")
        updated_at = price.get("updated_at") or time.time()

        if ltp is None:
            return None

        change = None
        change_pct = None

        if prev_ltp not in (None, 0):
            change = round(float(ltp) - float(prev_ltp), 4)
            change_pct = round((change / float(prev_ltp)) * 100, 4)

        direction = "SAME"
        if change is not None:
            if change > 0:
                direction = "UP"
            elif change < 0:
                direction = "DOWN"

        history_key = f"{market_id}:{runner_id}:{ltp}:{tv}"

        return {
            "history_key": history_key,
            "market_id": str(market_id),
            "runner_id": str(runner_id),
            "ltp": ltp,
            "prev_ltp": prev_ltp,
            "tv": tv,
            "change": change,
            "change_pct": change_pct,
            "direction": direction,
            "timestamp": updated_at,
        }
    except Exception as e:
        print("build_market_history_item error:", e)
        return None


def is_new_market_tick(market_id: str, runner_id: str, history_key: str) -> bool:
    redis_key = f"last_market_key:{market_id}:{runner_id}"
    last_key = r.get(redis_key)

    if last_key == history_key:
        return False

    r.set(redis_key, history_key)
    return True


def save_market_history(market_id: str, runner_id: str, item: dict, max_items: int = 120):
    if not item:
        return
    key = f"history:market:{market_id}:{runner_id}"
    r.rpush(key, json.dumps(item))
    r.ltrim(key, -max_items, -1)


def get_market_history(market_id: str, runner_id: str, limit: int = 60) -> list:
    key = f"history:market:{market_id}:{runner_id}"
    rows = r.lrange(key, -limit, -1)
    data = []

    for row in rows:
        try:
            data.append(json.loads(row))
        except Exception:
            continue

    return data


# =========================================================
# COMBINED HISTORY
# =========================================================

def get_combined_history(
    source_match_id: str,
    market_id: str,
    runner_id: str,
    ball_limit: int = 60,
    market_limit: int = 60,
) -> dict:
    return {
        "balls": get_ball_history(source_match_id, limit=ball_limit),
        "market": get_market_history(market_id, runner_id, limit=market_limit),
    }