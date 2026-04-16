import redis
from django.conf import settings


r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)


def get_latest_price(market_id, runner_id):
    key = f"price:{market_id}:{runner_id}"
    data = r.hgetall(key)

    # print(f"[RedisPrice] Reading key={key}")
    # print(f"[RedisPrice] data={data}")

    if not data:
        return {
            "market_id": str(market_id),
            "runner_id": str(runner_id),
            "ltp": 2.0,
            "prev_ltp": 2.0,
            "tv": 0,
        }

    return {
        "market_id": data.get("market_id"),
        "runner_id": data.get("runner_id"),
        "ltp": float(data.get("ltp", 2.0) or 2.0),
        "prev_ltp": float(data.get("prev_ltp", data.get("ltp", 2.0)) or 2.0),
        "tv": float(data.get("tv", 0) or 0),
    }