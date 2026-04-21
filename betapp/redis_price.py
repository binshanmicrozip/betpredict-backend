# import redis
# from django.conf import settings


# r = redis.Redis(
#     host=settings.REDIS_HOST,
#     port=settings.REDIS_PORT,
#     db=settings.REDIS_DB,
#     decode_responses=True,
# )


# def make_price_key(market_id, runner_id):
#     return f"price:{market_id}:{runner_id}"


# def get_latest_price(market_id: str, runner_id: str) -> dict:
#     key = make_price_key(market_id, runner_id)
#     data = r.hgetall(key)

#     print(f"[RedisPrice] LOOKUP key={key}")
#     print(f"[RedisPrice] DATA={data}")

#     if not data:
#         return {}

#     return {
#         "market_id": data.get("market_id"),
#         "runner_id": data.get("runner_id"),
#         "ltp": float(data.get("ltp")) if data.get("ltp") not in [None, ""] else None,
#         "prev_ltp": float(data.get("prev_ltp")) if data.get("prev_ltp") not in [None, ""] else None,
#         "tv": float(data.get("tv")) if data.get("tv") not in [None, ""] else 0.0,
#         "mi": data.get("mi"),
#         "bmi": data.get("bmi"),
#         "source": data.get("source"),
#     }


# def get_all_market_prices(market_id: str) -> list[dict]:
#     pattern = f"price:{market_id}:*"
#     keys = r.keys(pattern)

#     results = []
#     for key in keys:
#         data = r.hgetall(key)
#         if not data:
#             continue

#         results.append({
#             "market_id": data.get("market_id"),
#             "runner_id": data.get("runner_id"),
#             "ltp": float(data.get("ltp")) if data.get("ltp") not in [None, ""] else None,
#             "prev_ltp": float(data.get("prev_ltp")) if data.get("prev_ltp") not in [None, ""] else None,
#             "tv": float(data.get("tv")) if data.get("tv") not in [None, ""] else 0.0,
#             "mi": data.get("mi"),
#             "bmi": data.get("bmi"),
#             "source": data.get("source"),
#         })

#     return results

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


def _to_float(value, default=None):
    if value in [None, ""]:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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
        "ltp": _to_float(data.get("ltp")),
        "prev_ltp": _to_float(data.get("prev_ltp")),
        "tv": _to_float(data.get("tv"), 0.0),
        "mi": data.get("mi"),
        "bmi": data.get("bmi"),
        "eid": data.get("eid"),
        "eti": data.get("eti"),
        "market_status": data.get("market_status"),
        "tdv": _to_float(data.get("tdv"), 0.0),
        "updated_at": _to_float(data.get("updated_at")),
        "source": data.get("source"),
        "message_type": data.get("message_type"),
    }


def get_all_market_prices(market_id: str) -> list[dict]:
    pattern = f"price:{market_id}:*"
    results = []

    for key in r.scan_iter(match=pattern):
        data = r.hgetall(key)
        if not data:
            continue

        results.append({
            "market_id": data.get("market_id"),
            "runner_id": data.get("runner_id"),
            "ltp": _to_float(data.get("ltp")),
            "prev_ltp": _to_float(data.get("prev_ltp")),
            "tv": _to_float(data.get("tv"), 0.0),
            "mi": data.get("mi"),
            "bmi": data.get("bmi"),
            "eid": data.get("eid"),
            "eti": data.get("eti"),
            "market_status": data.get("market_status"),
            "tdv": _to_float(data.get("tdv"), 0.0),
            "updated_at": _to_float(data.get("updated_at")),
            "source": data.get("source"),
            "message_type": data.get("message_type"),
        })

    results.sort(key=lambda x: str(x.get("runner_id") or ""))
    print(f"[RedisPrice] MARKET LOOKUP market_id={market_id} count={len(results)}")
    return results


def get_all_prices() -> list[dict]:
    results = []

    for key in r.scan_iter(match="price:*"):
        data = r.hgetall(key)
        if not data:
            continue

        results.append({
            "market_id": data.get("market_id"),
            "runner_id": data.get("runner_id"),
            "ltp": _to_float(data.get("ltp")),
            "prev_ltp": _to_float(data.get("prev_ltp")),
            "tv": _to_float(data.get("tv"), 0.0),
            "mi": data.get("mi"),
            "bmi": data.get("bmi"),
            "eid": data.get("eid"),
            "eti": data.get("eti"),
            "market_status": data.get("market_status"),
            "tdv": _to_float(data.get("tdv"), 0.0),
            "updated_at": _to_float(data.get("updated_at")),
            "source": data.get("source"),
            "message_type": data.get("message_type"),
        })

    print(f"[RedisPrice] ALL PRICE COUNT={len(results)}")
    return results