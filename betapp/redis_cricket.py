import json
import redis
from django.conf import settings


r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)


def make_cricket_key(source_match_id):
    return f"cricket_live:{source_match_id}"


def save_latest_cricket(source_match_id, payload: dict):
    key = make_cricket_key(source_match_id)
    r.set(key, json.dumps(payload))
    # print(f"[RedisCricket] SAVED => {key}")


def get_latest_cricket(source_match_id):
    key = make_cricket_key(source_match_id)
    raw = r.get(key)

    # print(f"[RedisCricket] READING => {key}")
    # print(f"[RedisCricket] RAW => {raw}")

    if not raw:
        return {}

    try:
        return json.loads(raw)
    except Exception as e:
        print(f"[RedisCricket] JSON parse error: {e}")
        return {}