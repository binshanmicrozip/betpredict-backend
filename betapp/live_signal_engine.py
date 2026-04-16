import json
import redis
from django.conf import settings

from .redis_cricket import get_latest_cricket
from .redis_price import get_latest_price
from .predictor import predict
from .channel_push import push_signal_to_frontend


r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)
def pretty_print_signal(result):
    cricket = result.get("cricket", {})
    price = result.get("price", {})
    prediction = result.get("prediction", {})

    print("\n" + "="*55)
    print("📡 MATCH UPDATE")
    print("="*55)

    print(f"🏏 Match ID: {result.get('source_match_id')}")
    print(f"📊 Score: {cricket.get('score')} ({cricket.get('overs')} overs)")
    print(f"⚡ CRR: {cricket.get('crr')}")

    print("\n👤 Batsmen:")
    print(f"   - {cricket.get('b1_name')}: {cricket.get('b1_runs')} ({cricket.get('b1_balls')})")
    print(f"   - {cricket.get('b2_name')}: {cricket.get('b2_runs')} ({cricket.get('b2_balls')})")

    print("\n🎯 Last Ball:")
    print(f"   {cricket.get('latest_ball')}")

    print("\n💰 Market:")
    print(f"   LTP: {price.get('ltp')} | Prev: {price.get('prev_ltp')}")

    print("\n" + "-"*55)
    print(f"📌 STATUS: {result.get('status').upper()}")

    if prediction:
        print("\n🤖 PREDICTION:")
        print(f"   Signal: {prediction.get('signal')}")
        print(f"   Confidence: {round(prediction.get('confidence', 0)*100, 2)}%")
        print(f"   Reason: {prediction.get('reason')}")

    print("="*55 + "\n")

def build_ball_key(cricket: dict) -> str:
    overs = cricket.get("overs", "")
    score = cricket.get("score", "")
    recent = cricket.get("recent", "")
    latest_ball = cricket.get("latest_ball", "")
    return f"{overs}|{score}|{recent}|{latest_ball}"


def combine_live_inputs(source_match_id: str, market_id: str, runner_id: str) -> dict:
    cricket = get_latest_cricket(source_match_id)
    price = get_latest_price(market_id, runner_id)

    # print(f"[SignalEngine] source_match_id={source_match_id}")
    # print(f"[SignalEngine] market_id={market_id}, runner_id={runner_id}")
    # print(f"[SignalEngine] cricket={cricket}")
    # print(f"[SignalEngine] price={price}")

    return {
        "source_match_id": str(source_match_id),
        "market_id": str(market_id),
        "runner_id": str(runner_id),
        "cricket": cricket or {},
        "price": price or {},
    }


def run_live_prediction(source_match_id: str, market_id: str, runner_id: str) -> dict:
    combined = combine_live_inputs(source_match_id, market_id, runner_id)

    cricket = combined["cricket"]
    price = combined["price"]

    if not cricket:
        # print("[SignalEngine] No cricket data")
        return {
            "status": "no_cricket_data",
            "type": "bet_signal",
            "source_match_id": str(source_match_id),
            "market_id": str(market_id),
            "runner_id": str(runner_id),
            "cricket": {},
            "price": price,
            "prediction": None,
        }

    current_ball_key = build_ball_key(cricket)
    redis_ball_key = f"betsignal:last_ball:{source_match_id}:{market_id}:{runner_id}"
    previous_ball_key = r.get(redis_ball_key)

    # print(f"[SignalEngine] previous_ball_key={previous_ball_key}")
    # print(f"[SignalEngine] current_ball_key={current_ball_key}")

    if previous_ball_key == current_ball_key:
     
        return {
            "status": "no_new_ball",
            "type": "bet_signal",
            "source_match_id": str(source_match_id),
            "market_id": str(market_id),
            "runner_id": str(runner_id),
            "ball_key": current_ball_key,
            "cricket": cricket,
            "price": price,
            "prediction": None,
        }

    r.set(redis_ball_key, current_ball_key)


    prediction = predict(cricket, price)
    # print(f"[SignalEngine] prediction={prediction}")

    payload = {
        "status": "predicted",
        "type": "bet_signal",
        "source_match_id": str(source_match_id),
        "market_id": str(market_id),
        "runner_id": str(runner_id),
        "ball_key": current_ball_key,
        "cricket": cricket,
        "price": price,
        "prediction": prediction,
    }

    redis_latest_key = f"betsignal:latest:{source_match_id}:{market_id}:{runner_id}"
    r.set(redis_latest_key, json.dumps(payload))
    # print(f"[SignalEngine] Saved latest payload in Redis: {redis_latest_key}")

    push_signal_to_frontend(payload)
    # print("[SignalEngine] Pushed signal to frontend websocket")

    pretty_print_signal(payload)

    return payload