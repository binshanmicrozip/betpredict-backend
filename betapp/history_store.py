# import json
# import time
# import redis
# from django.conf import settings

# r = redis.Redis(
#     host=settings.REDIS_HOST,
#     port=settings.REDIS_PORT,
#     db=settings.REDIS_DB,
#     decode_responses=True,
# )


# # =========================================================
# # CRICKET BALL HISTORY
# # =========================================================

# def build_ball_history_item(source_match_id: str, raw_json: dict) -> dict | None:
#     try:
#         commentary_list = raw_json.get("commentaryList", [])
#         miniscore = raw_json.get("miniscore", {})

#         if not commentary_list:
#             return None

#         c = commentary_list[0]

#         innings_id = c.get("inningsId") or miniscore.get("inningsId")
#         over_number = c.get("overNumber")
#         ball_nbr = c.get("ballNbr")
#         event = c.get("event")
#         commentary = c.get("commText", "")
#         ts = c.get("timestamp")

#         if innings_id is None or over_number is None or ball_nbr is None:
#             return None

#         striker = miniscore.get("batsmanStriker", {})
#         non_striker = miniscore.get("batsmanNonStriker", {})
#         bowler = miniscore.get("bowlerStriker", {})
#         bat_team = miniscore.get("batTeam", {})
#         partnership = miniscore.get("partnerShip", {})
#         match_score_details = miniscore.get("matchScoreDetails", {})
#         innings_score_list = match_score_details.get("inningsScoreList", [])

#         score = bat_team.get("teamScore")
#         wickets = bat_team.get("teamWkts")
#         overs = miniscore.get("overs")

#         if innings_score_list:
#             row = innings_score_list[0]
#             score = row.get("score", score)
#             wickets = row.get("wickets", wickets)
#             overs = row.get("overs", overs)

#         ball_key = f"{source_match_id}:{innings_id}:{over_number}:{ball_nbr}"

#         return {
#             "ball_key": ball_key,
#             "source_match_id": str(source_match_id),
#             "innings": innings_id,
#             "over": over_number,
#             "ball_number": ball_nbr,
#             "event": event,
#             "commentary": commentary,
#             "score": score,
#             "wickets": wickets,
#             "overs": overs,
#             "current_run_rate": miniscore.get("currentRunRate"),
#             "required_run_rate": miniscore.get("requiredRunRate"),
#             "recent": miniscore.get("recentOvsStats"),
#             "partnership": {
#                 "runs": partnership.get("runs"),
#                 "balls": partnership.get("balls"),
#             },
#             "batter_striker": {
#                 "name": striker.get("batName"),
#                 "runs": striker.get("batRuns"),
#                 "balls": striker.get("batBalls"),
#                 "fours": striker.get("batFours"),
#                 "sixes": striker.get("batSixes"),
#                 "strike_rate": striker.get("batStrikeRate"),
#             },
#             "batter_non_striker": {
#                 "name": non_striker.get("batName"),
#                 "runs": non_striker.get("batRuns"),
#                 "balls": non_striker.get("batBalls"),
#                 "fours": non_striker.get("batFours"),
#                 "sixes": non_striker.get("batSixes"),
#                 "strike_rate": non_striker.get("batStrikeRate"),
#             },
#             "bowler": {
#                 "name": bowler.get("bowlName"),
#                 "overs": bowler.get("bowlOvs"),
#                 "runs": bowler.get("bowlRuns"),
#                 "wickets": bowler.get("bowlWkts"),
#                 "economy": bowler.get("bowlEcon"),
#             },
#             "timestamp": ts,
#         }
#     except Exception as e:
#         print("build_ball_history_item error:", e)
#         return None


# def is_new_ball(source_match_id: str, ball_key: str) -> bool:
#     redis_key = f"last_ball_key:{source_match_id}"
#     last_key = r.get(redis_key)

#     if last_key == ball_key:
#         return False

#     r.set(redis_key, ball_key)
#     return True


# def save_ball_history(source_match_id: str, item: dict, max_items: int = 120):
#     if not item:
#         return
#     key = f"history:balls:{source_match_id}"
#     r.rpush(key, json.dumps(item))
#     r.ltrim(key, -max_items, -1)


# def get_ball_history(source_match_id: str, limit: int = 60) -> list:
#     key = f"history:balls:{source_match_id}"
#     rows = r.lrange(key, -limit, -1)
#     data = []

#     for row in rows:
#         try:
#             data.append(json.loads(row))
#         except Exception:
#             continue

#     return data


# # =========================================================
# # MARKET HISTORY
# # =========================================================

# def build_market_history_item(market_id: str, runner_id: str, price: dict) -> dict | None:
#     try:
#         ltp = price.get("ltp")
#         prev_ltp = price.get("prev_ltp")
#         tv = price.get("tv")
#         updated_at = price.get("updated_at") or time.time()

#         if ltp is None:
#             return None

#         change = None
#         change_pct = None

#         if prev_ltp not in (None, 0):
#             change = round(float(ltp) - float(prev_ltp), 4)
#             change_pct = round((change / float(prev_ltp)) * 100, 4)

#         direction = "SAME"
#         if change is not None:
#             if change > 0:
#                 direction = "UP"
#             elif change < 0:
#                 direction = "DOWN"

#         history_key = f"{market_id}:{runner_id}:{ltp}:{tv}"

#         return {
#             "history_key": history_key,
#             "market_id": str(market_id),
#             "runner_id": str(runner_id),
#             "ltp": ltp,
#             "prev_ltp": prev_ltp,
#             "tv": tv,
#             "change": change,
#             "change_pct": change_pct,
#             "direction": direction,
#             "timestamp": updated_at,
#         }
#     except Exception as e:
#         print("build_market_history_item error:", e)
#         return None


# def is_new_market_tick(market_id: str, runner_id: str, history_key: str) -> bool:
#     redis_key = f"last_market_key:{market_id}:{runner_id}"
#     last_key = r.get(redis_key)

#     if last_key == history_key:
#         return False

#     r.set(redis_key, history_key)
#     return True


# def save_market_history(market_id: str, runner_id: str, item: dict, max_items: int = 120):
#     if not item:
#         return
#     key = f"history:market:{market_id}:{runner_id}"
#     r.rpush(key, json.dumps(item))
#     r.ltrim(key, -max_items, -1)


# def get_market_history(market_id: str, runner_id: str, limit: int = 60) -> list:
#     key = f"history:market:{market_id}:{runner_id}"
#     rows = r.lrange(key, -limit, -1)
#     data = []

#     for row in rows:
#         try:
#             data.append(json.loads(row))
#         except Exception:
#             continue

#     return data


# # =========================================================
# # COMBINED HISTORY
# # =========================================================

# def get_combined_history(
#     source_match_id: str,
#     market_id: str,
#     runner_id: str,
#     ball_limit: int = 60,
#     market_limit: int = 60,
# ) -> dict:
#     return {
#         "balls": get_ball_history(source_match_id, limit=ball_limit),
#         "market": get_market_history(market_id, runner_id, limit=market_limit),
#     }


# import json
# import time
# import redis
# from django.conf import settings

# r = redis.Redis(
#     host=settings.REDIS_HOST,
#     port=settings.REDIS_PORT,
#     db=settings.REDIS_DB,
#     decode_responses=True,
# )


# # =========================================================
# # CRICKET BALL HISTORY
# # =========================================================

# def _safe_float(value, default=None):
#     try:
#         if value in ("", None):
#             return default
#         return float(value)
#     except Exception:
#         return default


# def _safe_int(value, default=None):
#     try:
#         if value in ("", None):
#             return default
#         return int(value)
#     except Exception:
#         return default


# def _sort_ball_key(item: dict):
#     innings = _safe_int(item.get("innings"), 0) or 0
#     ball_number = _safe_int(item.get("ball_number"), 0) or 0
#     over = _safe_float(item.get("over"), 0) or 0
#     timestamp = _safe_int(item.get("timestamp"), 0) or 0
#     return (innings, ball_number, over, timestamp)


# def build_ball_history_item(source_match_id: str, raw_json: dict) -> dict | None:
#     """
#     Backward-compatible helper.
#     Returns only the latest/first commentary item if needed by old callers.
#     """
#     items = build_ball_history_items(source_match_id, raw_json)
#     if not items:
#         return None
#     return items[0]


# def build_ball_history_items(source_match_id: str, raw_json: dict) -> list[dict]:
#     """
#     Build history items from ALL commentary entries available in raw_json.
#     This is the main fix:
#     - before: only commentaryList[0] was saved
#     - now: every commentary row is saved
#     """
#     items = []

#     try:
#         commentary_list = raw_json.get("commentaryList", []) or []
#         miniscore = raw_json.get("miniscore", {}) or {}

#         if not commentary_list:
#             return []

#         striker = miniscore.get("batsmanStriker", {}) or {}
#         non_striker = miniscore.get("batsmanNonStriker", {}) or {}
#         bowler = miniscore.get("bowlerStriker", {}) or {}
#         bat_team = miniscore.get("batTeam", {}) or {}
#         partnership = miniscore.get("partnerShip", {}) or {}
#         match_score_details = miniscore.get("matchScoreDetails", {}) or {}
#         innings_score_list = match_score_details.get("inningsScoreList", []) or []

#         current_innings_id = miniscore.get("inningsId")

#         for c in commentary_list:
#             innings_id = c.get("inningsId") or current_innings_id
#             over_number = c.get("overNumber")
#             ball_nbr = c.get("ballNbr")
#             event = c.get("event")
#             commentary = c.get("commText", "")
#             ts = c.get("timestamp")

#             if innings_id is None or over_number is None or ball_nbr is None:
#                 continue

#             score = bat_team.get("teamScore")
#             wickets = bat_team.get("teamWkts")
#             overs = miniscore.get("overs")

#             # pick correct innings row if available
#             innings_row = None
#             for row in innings_score_list:
#                 if row.get("inningsId") == innings_id:
#                     innings_row = row
#                     break

#             if innings_row:
#                 score = innings_row.get("score", score)
#                 wickets = innings_row.get("wickets", wickets)
#                 overs = innings_row.get("overs", overs)

#             ball_key = f"{source_match_id}:{innings_id}:{over_number}:{ball_nbr}"

#             item = {
#                 "ball_key": ball_key,
#                 "source_match_id": str(source_match_id),
#                 "innings": innings_id,
#                 "over": over_number,
#                 "ball_number": ball_nbr,
#                 "event": event,
#                 "commentary": commentary,
#                 "score": score,
#                 "wickets": wickets,
#                 "overs": overs,
#                 "current_run_rate": miniscore.get("currentRunRate"),
#                 "required_run_rate": miniscore.get("requiredRunRate"),
#                 "recent": miniscore.get("recentOvsStats"),
#                 "partnership": {
#                     "runs": partnership.get("runs"),
#                     "balls": partnership.get("balls"),
#                 },
#                 "batter_striker": {
#                     "name": striker.get("batName"),
#                     "runs": striker.get("batRuns"),
#                     "balls": striker.get("batBalls"),
#                     "fours": striker.get("batFours"),
#                     "sixes": striker.get("batSixes"),
#                     "strike_rate": striker.get("batStrikeRate"),
#                 },
#                 "batter_non_striker": {
#                     "name": non_striker.get("batName"),
#                     "runs": non_striker.get("batRuns"),
#                     "balls": non_striker.get("batBalls"),
#                     "fours": non_striker.get("batFours"),
#                     "sixes": non_striker.get("batSixes"),
#                     "strike_rate": non_striker.get("batStrikeRate"),
#                 },
#                 "bowler": {
#                     "name": bowler.get("bowlName"),
#                     "overs": bowler.get("bowlOvs"),
#                     "runs": bowler.get("bowlRuns"),
#                     "wickets": bowler.get("bowlWkts"),
#                     "economy": bowler.get("bowlEcon"),
#                 },
#                 "timestamp": ts,
#             }

#             items.append(item)

#         items.sort(key=_sort_ball_key)
#         return items

#     except Exception as e:
#         print("build_ball_history_items error:", e)
#         return []


# def is_new_ball(source_match_id: str, ball_key: str) -> bool:
#     """
#     Backward-compatible helper for old callers.
#     """
#     redis_key = f"last_ball_key:{source_match_id}"
#     last_key = r.get(redis_key)

#     if last_key == ball_key:
#         return False

#     r.set(redis_key, ball_key)
#     return True


# def save_ball_history(source_match_id: str, item: dict):
#     """
#     Save one ball permanently in Redis hash.
#     No trim.
#     No loss of old balls.
#     Unique by ball_key.
#     """
#     if not item:
#         return

#     ball_key = item.get("ball_key")
#     if not ball_key:
#         return

#     key = f"history:balls:{source_match_id}"
#     r.hset(key, ball_key, json.dumps(item))


# def save_ball_history_items(source_match_id: str, items: list[dict]):
#     """
#     Save all commentary items.
#     """
#     if not items:
#         return

#     key = f"history:balls:{source_match_id}"
#     pipe = r.pipeline()

#     latest_item = None

#     for item in items:
#         if not item:
#             continue

#         ball_key = item.get("ball_key")
#         if not ball_key:
#             continue

#         pipe.hset(key, ball_key, json.dumps(item))
#         latest_item = item

#     if latest_item:
#         pipe.set(f"last_ball_key:{source_match_id}", latest_item["ball_key"])

#     pipe.execute()


# def build_and_save_ball_history(source_match_id: str, raw_json: dict) -> list[dict]:
#     """
#     Main helper you should call from process_and_push_live_update().
#     Builds all available commentary rows and saves them.
#     """
#     items = build_ball_history_items(source_match_id, raw_json)
#     if items:
#         save_ball_history_items(source_match_id, items)
#     return items


# def get_ball_history(source_match_id: str, limit: int | None = None) -> list:
#     """
#     Return full ball history sorted from start to end.
#     If limit is given, returns only the latest N balls.
#     """
#     key = f"history:balls:{source_match_id}"
#     rows = r.hgetall(key)
#     data = []

#     for _, row in rows.items():
#         try:
#             data.append(json.loads(row))
#         except Exception:
#             continue

#     data.sort(key=_sort_ball_key)

#     if limit:
#         return data[-limit:]

#     return data


# # =========================================================
# # MARKET HISTORY
# # =========================================================

# def build_market_history_item(market_id: str, runner_id: str, price: dict) -> dict | None:
#     try:
#         ltp = price.get("ltp")
#         prev_ltp = price.get("prev_ltp")
#         tv = price.get("tv")
#         updated_at = price.get("updated_at") or time.time()

#         if ltp is None:
#             return None

#         change = None
#         change_pct = None

#         if prev_ltp not in (None, 0):
#             change = round(float(ltp) - float(prev_ltp), 4)
#             change_pct = round((change / float(prev_ltp)) * 100, 4)

#         direction = "SAME"
#         if change is not None:
#             if change > 0:
#                 direction = "UP"
#             elif change < 0:
#                 direction = "DOWN"

#         history_key = f"{market_id}:{runner_id}:{ltp}:{tv}"

#         return {
#             "history_key": history_key,
#             "market_id": str(market_id),
#             "runner_id": str(runner_id),
#             "ltp": ltp,
#             "prev_ltp": prev_ltp,
#             "tv": tv,
#             "change": change,
#             "change_pct": change_pct,
#             "direction": direction,
#             "timestamp": updated_at,
#         }
#     except Exception as e:
#         print("build_market_history_item error:", e)
#         return None


# def is_new_market_tick(market_id: str, runner_id: str, history_key: str) -> bool:
#     redis_key = f"last_market_key:{market_id}:{runner_id}"
#     last_key = r.get(redis_key)

#     if last_key == history_key:
#         return False

#     r.set(redis_key, history_key)
#     return True


# def save_market_history(market_id: str, runner_id: str, item: dict, max_items: int = 120):
#     if not item:
#         return
#     key = f"history:market:{market_id}:{runner_id}"
#     r.rpush(key, json.dumps(item))
#     r.ltrim(key, -max_items, -1)


# def get_market_history(market_id: str, runner_id: str, limit: int = 60) -> list:
#     key = f"history:market:{market_id}:{runner_id}"
#     rows = r.lrange(key, -limit, -1)
#     data = []

#     for row in rows:
#         try:
#             data.append(json.loads(row))
#         except Exception:
#             continue

#     return data


# # =========================================================
# # COMBINED HISTORY
# # =========================================================

# def get_combined_history(
#     source_match_id: str,
#     market_id: str,
#     runner_id: str,
#     ball_limit: int | None = None,
#     market_limit: int = 60,
# ) -> dict:
#     return {
#         "balls": get_ball_history(source_match_id, limit=ball_limit),
#         "market": get_market_history(market_id, runner_id, limit=market_limit),
#     }


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
# HELPERS
# =========================================================

def _safe_float(value, default=None):
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value, default=None):
    try:
        if value in ("", None):
            return default
        return int(value)
    except Exception:
        return default


def _sort_ball_key(item: dict):
    innings = _safe_int(item.get("innings"), 0) or 0
    ball_number = _safe_int(item.get("ball_number"), 0) or 0
    over = _safe_float(item.get("over"), 0) or 0
    timestamp = _safe_int(item.get("timestamp"), 0) or 0
    return (innings, ball_number, over, timestamp)


# =========================================================
# CRICKET BALL HISTORY
# =========================================================

def build_ball_history_item(source_match_id: str, raw_json: dict) -> dict | None:
    items = build_ball_history_items(source_match_id, raw_json)
    if not items:
        return None
    return items[-1]


def build_ball_history_items(source_match_id: str, raw_json: dict) -> list[dict]:
    items = []

    try:
        commentary_list = raw_json.get("commentaryList", []) or []
        miniscore = raw_json.get("miniscore", {}) or {}

        if not commentary_list:
            return []

        striker = miniscore.get("batsmanStriker", {}) or {}
        non_striker = miniscore.get("batsmanNonStriker", {}) or {}
        bowler = miniscore.get("bowlerStriker", {}) or {}
        bat_team = miniscore.get("batTeam", {}) or {}
        partnership = miniscore.get("partnerShip", {}) or {}
        match_score_details = miniscore.get("matchScoreDetails", {}) or {}
        innings_score_list = match_score_details.get("inningsScoreList", []) or []

        current_innings_id = miniscore.get("inningsId")

        for c in commentary_list:
            innings_id = c.get("inningsId") or current_innings_id
            over_number = c.get("overNumber")
            ball_nbr = c.get("ballNbr")
            event = c.get("event")
            commentary = c.get("commText", "")
            ts = c.get("timestamp")

            if innings_id is None or over_number is None or ball_nbr is None:
                continue

            score = bat_team.get("teamScore")
            wickets = bat_team.get("teamWkts")
            overs = miniscore.get("overs")

            innings_row = None
            for row in innings_score_list:
                if row.get("inningsId") == innings_id:
                    innings_row = row
                    break

            if innings_row:
                score = innings_row.get("score", score)
                wickets = innings_row.get("wickets", wickets)
                overs = innings_row.get("overs", overs)

            ball_key = f"{source_match_id}:{innings_id}:{over_number}:{ball_nbr}"

            item = {
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

            items.append(item)

        items.sort(key=_sort_ball_key)
        return items

    except Exception as e:
        print("build_ball_history_items error:", e)
        return []


def is_new_ball(source_match_id: str, ball_key: str) -> bool:
    redis_key = f"last_ball_key:{source_match_id}"
    last_key = r.get(redis_key)

    if last_key == ball_key:
        return False

    r.set(redis_key, ball_key)
    return True


def save_ball_history(source_match_id: str, item: dict):
    if not item:
        return

    ball_key = item.get("ball_key")
    if not ball_key:
        return

    key = f"history:balls:{source_match_id}"
    r.hset(key, ball_key, json.dumps(item))


def save_ball_history_items(source_match_id: str, items: list[dict]):
    if not items:
        return

    key = f"history:balls:{source_match_id}"
    pipe = r.pipeline()
    latest_item = None

    for item in items:
        if not item:
            continue

        ball_key = item.get("ball_key")
        if not ball_key:
            continue

        pipe.hset(key, ball_key, json.dumps(item))
        latest_item = item

    if latest_item:
        pipe.set(f"last_ball_key:{source_match_id}", latest_item["ball_key"])

    pipe.execute()


def build_and_save_ball_history(source_match_id: str, raw_json: dict) -> list[dict]:
    items = build_ball_history_items(source_match_id, raw_json)
    if items:
        save_ball_history_items(source_match_id, items)
    return items


def get_ball_history(source_match_id: str, limit: int | None = None) -> list:
    key = f"history:balls:{source_match_id}"
    rows = r.hgetall(key)
    data = []

    for _, row in rows.items():
        try:
            data.append(json.loads(row))
        except Exception:
            continue

    data.sort(key=_sort_ball_key)

    if limit:
        return data[-limit:]

    return data


# =========================================================
# BALL PATTERN / PREDICTION HISTORY
# =========================================================

def save_pattern_history(source_match_id: str, ball_key: str, pattern_payload: dict):
    if not ball_key:
        return

    key = f"history:patterns:{source_match_id}"
    r.hset(key, ball_key, json.dumps(pattern_payload or {}))


def get_pattern_history(source_match_id: str, limit: int | None = None) -> list:
    key = f"history:patterns:{source_match_id}"
    rows = r.hgetall(key)
    data = []

    for ball_key, row in rows.items():
        try:
            item = json.loads(row)
            if isinstance(item, dict) and "ball_key" not in item:
                item["ball_key"] = ball_key
            data.append(item)
        except Exception:
            continue

    data.sort(key=_sort_ball_key)

    if limit:
        return data[-limit:]

    return data


# =========================================================
# MARKET HISTORY
# =========================================================

def build_market_history_item(
    market_id: str,
    runner_id: str,
    price: dict,
    ball_key: str | None = None,
    innings: int | None = None,
) -> dict | None:
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
            "ball_key": ball_key,
            "innings": innings,
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


def save_market_history(market_id: str, runner_id: str, item: dict):
    if not item:
        return

    history_key = item.get("history_key")
    if not history_key:
        return

    key = f"history:market:{market_id}:{runner_id}"
    r.hset(key, history_key, json.dumps(item))


def get_market_history(market_id: str, runner_id: str, limit: int | None = None) -> list:
    key = f"history:market:{market_id}:{runner_id}"
    rows = r.hgetall(key)
    data = []

    for _, row in rows.items():
        try:
            data.append(json.loads(row))
        except Exception:
            continue

    data.sort(key=lambda x: x.get("timestamp", 0))

    if limit:
        return data[-limit:]

    return data


# =========================================================
# COMBINED HISTORY
# =========================================================

def get_combined_history(
    source_match_id: str,
    market_id: str,
    runner_id: str,
    ball_limit: int | None = None,
    market_limit: int | None = None,
    pattern_limit: int | None = None,
) -> dict:
    return {
        "balls": get_ball_history(source_match_id, limit=ball_limit),
        "market": get_market_history(market_id, runner_id, limit=market_limit),
        "patterns": get_pattern_history(source_match_id, limit=pattern_limit),
    }