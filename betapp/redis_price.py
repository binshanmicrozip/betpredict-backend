import redis
from django.conf import settings


r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)


def make_price_key(market_id, runner_id):
    return f"price:{market_id}:{runner_id}"


def get_latest_price(market_id: str, runner_id: str) -> dict:
    key = make_price_key(market_id, runner_id)
    data = r.hgetall(key)

    print(f"[RedisPrice] LOOKUP key={key}")
    print(f"[RedisPrice] DATA={data}")

    if not data:
        return {}

    return {
        "market_id": data.get("market_id"),
        "runner_id": data.get("runner_id"),
        "ltp": float(data.get("ltp")) if data.get("ltp") not in [None, ""] else None,
        "prev_ltp": float(data.get("prev_ltp")) if data.get("prev_ltp") not in [None, ""] else None,
        "tv": float(data.get("tv")) if data.get("tv") not in [None, ""] else 0.0,
        "mi": data.get("mi"),
        "bmi": data.get("bmi"),
        "source": data.get("source"),
    }


def get_all_market_prices(market_id: str) -> list[dict]:
    pattern = f"price:{market_id}:*"
    keys = r.keys(pattern)

    results = []
    for key in keys:
        data = r.hgetall(key)
        if not data:
            continue

        results.append({
            "market_id": data.get("market_id"),
            "runner_id": data.get("runner_id"),
            "ltp": float(data.get("ltp")) if data.get("ltp") not in [None, ""] else None,
            "prev_ltp": float(data.get("prev_ltp")) if data.get("prev_ltp") not in [None, ""] else None,
            "tv": float(data.get("tv")) if data.get("tv") not in [None, ""] else 0.0,
            "mi": data.get("mi"),
            "bmi": data.get("bmi"),
            "source": data.get("source"),
        })

    return results