import json
import redis
from django.conf import settings

from betapp.services.player_cache_service import refresh_player_cache

r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)


def _build_match_meta(parsed_data: dict) -> dict:
    overs = parsed_data.get("overs")
    overs_float = 0.0
    try:
        overs_float = float(overs) if overs is not None else 0.0
    except Exception:
        overs_float = 0.0

    if overs_float <= 6:
        phase = "powerplay"
    elif overs_float <= 15:
        phase = "middle"
    else:
        phase = "death"

    innings_num = parsed_data.get("innings_num", 1)
    innings_type = "defending" if int(innings_num or 1) == 1 else "chasing"

    parsed_data["overs_float"] = overs_float
    parsed_data["phase"] = phase
    parsed_data["innings_type"] = innings_type
    parsed_data["striker_name"] = parsed_data.get("b1_name")

    score = parsed_data.get("score")
    score_num = 0
    wickets = 0

    if score:
        score_str = str(score).replace("-", "/")
        parts = score_str.split("/")
        try:
            score_num = int(parts[0])
        except Exception:
            score_num = 0
        try:
            wickets = int(parts[1]) if len(parts) > 1 else 0
        except Exception:
            wickets = 0

    parsed_data["score_num"] = score_num
    parsed_data["wickets"] = wickets
    return parsed_data


def set_latest_cricket(match_id: str, parsed_data: dict, raw_json: dict | None = None):
    data = (parsed_data or {}).copy()
    data = _build_match_meta(data)
    data["source_match_id"] = str(match_id)
    data["raw_json"] = raw_json or {}

    key = f"cricket_live:{match_id}"

    previous_raw = r.get(key)
    previous = {}
    if previous_raw:
        try:
            previous = json.loads(previous_raw)
        except Exception:
            previous = {}

    previous_striker = previous.get("striker_name")
    new_striker = data.get("striker_name")

    r.set(key, json.dumps(data))

    phase = data.get("phase")
    innings_type = data.get("innings_type")

    if new_striker and phase and innings_type and previous_striker != new_striker:
        cache_result = refresh_player_cache(new_striker, phase, innings_type)
        print(f"[PlayerCache] Refreshed for {new_striker} | {phase} | {innings_type} => {cache_result}")


def get_latest_cricket(match_id: str) -> dict:
    key = f"cricket_live:{match_id}"
    raw = r.get(key)
    if not raw:
        print(f"[RedisCricket] No data for key={key}")
        return {}

    try:
        data = json.loads(raw)
        print(f"[RedisCricket] LOOKUP key={key}")
        return data
    except Exception as e:
        print(f"[RedisCricket] JSON decode error for key={key}: {e}")
        return {}