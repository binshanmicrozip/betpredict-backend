import json
import redis
from django.conf import settings

from .redis_cricket import get_latest_cricket
from .redis_price import get_latest_price
from .predictor import predict, has_cricket_data, has_market_data
from .services.live_signal_service import process_and_push_live_update


r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)


def normalize_price(market_id: str, runner_id: str, raw_price: dict) -> dict:
    return {
        "market_id": str(market_id),
        "runner_id": str(runner_id),
        "ltp": float(raw_price.get("ltp")) if raw_price.get("ltp") is not None else None,
        "prev_ltp": float(raw_price.get("prev_ltp")) if raw_price.get("prev_ltp") is not None else None,
        "tv": float(raw_price.get("tv", 0)) if raw_price.get("tv") is not None else 0.0,
        "updated_at": raw_price.get("updated_at"),
    }


def resolve_status(cricket: dict, price: dict) -> str:
    cricket_status = cricket.get("status")

    if cricket_status == "no_new_ball":
        return "no_new_ball"

    cricket_ok = has_cricket_data(cricket)
    market_ok = has_market_data(price)

    if cricket_ok and market_ok:
        return "live"

    if market_ok:
        return "market_only"

    return "no_data"


def pretty_print_signal(result: dict):
    cricket = result.get("cricket", {})
    price = result.get("price", {})
    prediction = result.get("prediction", {})
    history = result.get("history", {})

    print("\n" + "=" * 60)
    print("📡 MATCH UPDATE")
    print("=" * 60)

    print(f"🏏 Match ID: {result.get('source_match_id')}")
    print(f"📌 Status: {result.get('status')}")
    print(f"🎯 Ball Key: {result.get('ball_key')}")
    print(f"📊 Score: {cricket.get('score')} ({cricket.get('overs')} overs)")
    print(f"⚡ CRR: {cricket.get('crr')}")

    print("\n👤 Batsmen:")
    print(f"   - {cricket.get('b1_name')}: {cricket.get('b1_runs')} ({cricket.get('b1_balls')})")
    print(f"   - {cricket.get('b2_name')}: {cricket.get('b2_runs')} ({cricket.get('b2_balls')})")

    print("\n🎯 Last Ball:")
    print(f"   {cricket.get('latest_ball')}")

    print("\n💰 Market:")
    print(f"   LTP: {price.get('ltp')} | Prev: {price.get('prev_ltp')} | TV: {price.get('tv')}")

    print("\n📜 History:")
    print(f"   Balls stored: {len(history.get('balls', []))}")
    print(f"   Market ticks stored: {len(history.get('market', []))}")

    print("\n🤖 Prediction:")
    print(json.dumps(prediction, indent=2, ensure_ascii=False))


def run_live_prediction(match_id: str, market_id: str, runner_id: str) -> dict:
    cricket = get_latest_cricket(match_id) or {}
    raw_price = get_latest_price(market_id, runner_id) or {}
    price = normalize_price(market_id, runner_id, raw_price)

    status = resolve_status(cricket, price)

    prediction_payload = {
        "status": status,
        "source_match_id": str(match_id),
        "market_id": str(market_id),
        "runner_id": str(runner_id),
        "cricket": cricket,
        "price": price,
    }

    prediction = predict(prediction_payload)

    # IMPORTANT: raw_json must be inside cricket
    raw_json = cricket.get("raw_json", {}) or {}

    payload = process_and_push_live_update(
        source_match_id=str(match_id),
        market_id=str(market_id),
        runner_id=str(runner_id),
        cricket=cricket,
        raw_json=raw_json,
        price=price,
        prediction=prediction,
    )

    payload["status"] = status
    payload["type"] = "bet_signal"

    print("\n" + "-" * 60)
    print(f"📌 STATUS: {status.upper()}")
    print("-" * 60)
    print(payload)

    print(f"[ChannelPush] Pushing to group bet_signals: {payload}")

    pretty_print_signal(payload)

    return payload