# import json
# from decimal import Decimal
# import redis

# from django.conf import settings

# from betapp.models import Signal, IPLMatch
# from betapp.predictor import predict
# from betapp.services.player_cache_service import get_cached_player_stats

# try:
#     from betapp.channel_push import push_signal_to_frontend
# except Exception:
#     push_signal_to_frontend = None


# r = redis.Redis(
#     host=settings.REDIS_HOST,
#     port=settings.REDIS_PORT,
#     db=settings.REDIS_DB,
#     decode_responses=True,
# )


# def get_latest_cricket_context(match_id: str) -> dict:
#     raw = r.get(f"cricket_live:{match_id}")
#     if not raw:
#         return {}
#     try:
#         return json.loads(raw)
#     except Exception:
#         return {}


# def get_latest_price_context(market_id: str, runner_id: str) -> dict:
#     raw = r.get(f"price:{market_id}:{runner_id}")
#     if not raw:
#         return {}
#     try:
#         data = json.loads(raw)
#         return {
#             "market_id": str(market_id),
#             "runner_id": str(runner_id),
#             "ltp": _to_float(data.get("ltp")),
#             "prev_ltp": _to_float(data.get("prev_ltp")),
#             "tv": _to_float(data.get("tv")),
#             "updated_at": data.get("updated_at"),
#         }
#     except Exception:
#         return {}


# def _to_float(value, default=0.0):
#     try:
#         if value in [None, ""]:
#             return default
#         return float(value)
#     except Exception:
#         return default


# def extract_probability(prediction_result: dict) -> float:
#     """
#     Try to read probability/confidence from predictor output.
#     Falls back to signal-based mapping if needed.
#     """
#     prediction = prediction_result.get("prediction", {}) or {}

#     for key in ["probability", "win_probability", "prob", "confidence", "score"]:
#         value = prediction.get(key)
#         if value is not None:
#             try:
#                 return float(value)
#             except Exception:
#                 pass

#     signal = str(prediction.get("signal", "WAIT")).upper()
#     if signal == "BACK":
#         return 0.75
#     if signal == "LAY":
#         return 0.25
#     return 0.50


# def extract_signal(prediction_result: dict) -> str:
#     prediction = prediction_result.get("prediction", {}) or {}
#     signal = prediction.get("signal")
#     if signal:
#         return str(signal).upper()
#     return "WAIT"


# def save_latest_signal_to_redis(signal_obj: Signal):
#     redis_key = f"signal:latest:{signal_obj.market_id}:{signal_obj.runner_id}"

#     payload = {
#         "id": signal_obj.id,
#         "match_id": signal_obj.match_id if signal_obj.match_id else None,
#         "market_id": signal_obj.market_id,
#         "runner_id": signal_obj.runner_id,
#         "striker_name": signal_obj.striker_name,
#         "phase": signal_obj.phase,
#         "innings_type": signal_obj.innings_type,
#         "final_probability": float(signal_obj.final_probability) if signal_obj.final_probability is not None else None,
#         "signal": signal_obj.signal,
#         "model_source": signal_obj.model_source,
#         "created_at": signal_obj.created_at.isoformat(),
#     }

#     r.set(redis_key, json.dumps(payload))

#     if push_signal_to_frontend:
#         try:
#             push_signal_to_frontend(payload)
#         except Exception:
#             pass


# def run_signal_engine(match_id: str, market_id: str, runner_id: str, price_data: dict | None = None) -> dict:
#     cricket = get_latest_cricket_context(match_id)
#     if not cricket:
#         return {
#             "status": "skipped",
#             "reason": "no_cricket_context",
#             "match_id": str(match_id),
#             "market_id": str(market_id),
#             "runner_id": str(runner_id),
#         }

#     if not price_data:
#         price_data = get_latest_price_context(market_id, runner_id)

#     if not price_data:
#         return {
#             "status": "skipped",
#             "reason": "no_price_context",
#             "match_id": str(match_id),
#             "market_id": str(market_id),
#             "runner_id": str(runner_id),
#         }

#     striker_name = cricket.get("striker_name") or cricket.get("b1_name")
#     phase = cricket.get("phase")
#     innings_type = cricket.get("innings_type")

#     player_cache = {}
#     if striker_name and phase and innings_type:
#         player_cache = get_cached_player_stats(striker_name, phase, innings_type)

#     # IMPORTANT:
#     # This assumes your predictor.py exposes:
#     # predict(cricket_data=..., price_data=..., player_stats=...)
#     # If your real function signature differs, only this block needs adjustment.
#     result = predict(
#         cricket_data=cricket,
#         price_data=price_data,
#         player_stats=player_cache,
#     )

#     probability = extract_probability(result)
#     final_signal = extract_signal(result)

#     match = IPLMatch.objects.filter(match_id=str(match_id)).first()

#     signal_obj = Signal.objects.create(
#         match=match,
#         market_id=str(market_id),
#         runner_id=str(runner_id),
#         striker_name=striker_name,
#         phase=phase,
#         innings_type=innings_type,
#         final_probability=Decimal(str(round(probability, 4))),
#         signal=final_signal,
#         model_source="betpredict_model.pkl",
#         raw_features={
#             "cricket": cricket,
#             "price": price_data,
#             "player_cache": player_cache,
#         },
#         raw_output=result,
#     )

#     save_latest_signal_to_redis(signal_obj)

#     return {
#         "status": "ok",
#         "signal_id": signal_obj.id,
#         "match_id": str(match_id),
#         "market_id": str(market_id),
#         "runner_id": str(runner_id),
#         "final_probability": probability,
#         "signal": final_signal,
#         "raw_result": result,
#     }


# import json
# import redis
# from django.conf import settings

# from .redis_cricket import get_latest_cricket
# from .redis_price import get_latest_price
# from .predictor import predict, has_cricket_data, has_market_data
# from .services.live_signal_service import process_and_push_live_update


# r = redis.Redis(
#     host=settings.REDIS_HOST,
#     port=settings.REDIS_PORT,
#     db=settings.REDIS_DB,
#     decode_responses=True,
# )


# def normalize_price(market_id: str, runner_id: str, raw_price: dict) -> dict:
#     return {
#         "market_id": str(market_id),
#         "runner_id": str(runner_id),
#         "ltp": float(raw_price.get("ltp")) if raw_price.get("ltp") is not None else None,
#         "prev_ltp": float(raw_price.get("prev_ltp")) if raw_price.get("prev_ltp") is not None else None,
#         "tv": float(raw_price.get("tv", 0)) if raw_price.get("tv") is not None else 0.0,
#         "updated_at": raw_price.get("updated_at"),
#     }


# def resolve_status(cricket: dict, price: dict) -> str:
#     cricket_status = cricket.get("status")

#     if cricket_status == "no_new_ball":
#         return "no_new_ball"

#     cricket_ok = has_cricket_data(cricket)
#     market_ok = has_market_data(price)

#     if cricket_ok and market_ok:
#         return "live"

#     if market_ok:
#         return "market_only"

#     return "no_data"


# def build_ball_key(match_id: str, cricket: dict) -> str | None:
#     """
#     Build a stable unique key for one ball.
#     Priority:
#     1) use existing cricket['ball_key'] if present
#     2) use commentaryList[0] => inningsId + overNumber + ballNbr
#     3) fallback to innings + overs + ballNbr
#     """
#     existing_ball_key = cricket.get("ball_key")
#     if existing_ball_key:
#         return str(existing_ball_key)

#     innings = cricket.get("innings")
#     overs = cricket.get("overs") or cricket.get("overs_float")
#     raw_json = cricket.get("raw_json", {}) or {}

#     commentary_list = raw_json.get("commentaryList") or []
#     latest_commentary = commentary_list[0] if commentary_list else {}

#     innings_id = latest_commentary.get("inningsId", innings)
#     over_number = latest_commentary.get("overNumber")
#     ball_number = latest_commentary.get("ballNbr")

#     if innings_id is not None and over_number is not None and ball_number is not None:
#         return f"{match_id}:{innings_id}:{over_number}:{ball_number}"

#     if innings is not None and overs is not None and ball_number is not None:
#         return f"{match_id}:{innings}:{overs}:{ball_number}"

#     return None


# def enrich_cricket_for_csv(cricket: dict) -> dict:
#     """
#     Ensure CSV-required cricket fields always exist.
#     """
#     cricket = cricket or {}
#     raw_json = cricket.get("raw_json", {}) or {}
#     miniscore = raw_json.get("miniscore", {}) or {}
#     commentary_list = raw_json.get("commentaryList") or []
#     latest_commentary = commentary_list[0] if commentary_list else {}

#     striker = miniscore.get("batsmanStriker", {}) or {}
#     non_striker = miniscore.get("batsmanNonStriker", {}) or {}
#     bowler = miniscore.get("bowlerStriker", {}) or {}
#     partnership = miniscore.get("partnerShip", {}) or {}

#     if not cricket.get("latest_ball"):
#         cricket["latest_ball"] = latest_commentary.get("commText", "")

#     if not cricket.get("innings"):
#         cricket["innings"] = miniscore.get("inningsId")

#     if not cricket.get("overs_float") and cricket.get("overs") is not None:
#         try:
#             cricket["overs_float"] = float(cricket.get("overs"))
#         except Exception:
#             cricket["overs_float"] = None

#     if not cricket.get("b1_name"):
#         cricket["b1_name"] = striker.get("batName")
#     if cricket.get("b1_runs") is None:
#         cricket["b1_runs"] = striker.get("batRuns")
#     if cricket.get("b1_balls") is None:
#         cricket["b1_balls"] = striker.get("batBalls")
#     if cricket.get("b1_4s") is None:
#         cricket["b1_4s"] = striker.get("batFours")
#     if cricket.get("b1_6s") is None:
#         cricket["b1_6s"] = striker.get("batSixes")
#     if cricket.get("b1_sr") is None:
#         cricket["b1_sr"] = striker.get("batStrikeRate")

#     if not cricket.get("b2_name"):
#         cricket["b2_name"] = non_striker.get("batName")
#     if cricket.get("b2_runs") is None:
#         cricket["b2_runs"] = non_striker.get("batRuns")
#     if cricket.get("b2_balls") is None:
#         cricket["b2_balls"] = non_striker.get("batBalls")
#     if cricket.get("b2_4s") is None:
#         cricket["b2_4s"] = non_striker.get("batFours")
#     if cricket.get("b2_6s") is None:
#         cricket["b2_6s"] = non_striker.get("batSixes")
#     if cricket.get("b2_sr") is None:
#         cricket["b2_sr"] = non_striker.get("batStrikeRate")

#     if not cricket.get("bw1_name"):
#         cricket["bw1_name"] = bowler.get("bowlName")
#     if cricket.get("bw1_overs") is None:
#         cricket["bw1_overs"] = bowler.get("bowlOvs")
#     if cricket.get("bw1_runs") is None:
#         cricket["bw1_runs"] = bowler.get("bowlRuns")
#     if cricket.get("bw1_wkts") is None:
#         cricket["bw1_wkts"] = bowler.get("bowlWkts")
#     if cricket.get("bw1_eco") is None:
#         cricket["bw1_eco"] = bowler.get("bowlEcon")

#     if cricket.get("p_runs") is None:
#         cricket["p_runs"] = partnership.get("runs")
#     if cricket.get("p_balls") is None:
#         cricket["p_balls"] = partnership.get("balls")

#     if cricket.get("score_num") is None:
#         score_value = cricket.get("score")
#         if isinstance(score_value, str) and "/" in score_value:
#             try:
#                 cricket["score_num"] = int(score_value.split("/")[0])
#             except Exception:
#                 cricket["score_num"] = None

#     return cricket


# def pretty_print_signal(result: dict):
#     cricket = result.get("cricket", {})
#     price = result.get("price", {})
#     prediction = result.get("prediction", {})
#     history = result.get("history", {})

#     print("\n" + "=" * 60)
#     print("📡 MATCH UPDATE")
#     print("=" * 60)

#     print(f"🏏 Match ID: {result.get('source_match_id')}")
#     print(f"📌 Status: {result.get('status')}")
#     print(f"🎯 Ball Key: {result.get('ball_key')}")
#     print(f"📊 Score: {cricket.get('score')} ({cricket.get('overs')} overs)")
#     print(f"⚡ CRR: {cricket.get('crr')}")

#     print("\n👤 Batsmen:")
#     print(f"   - {cricket.get('b1_name')}: {cricket.get('b1_runs')} ({cricket.get('b1_balls')})")
#     print(f"   - {cricket.get('b2_name')}: {cricket.get('b2_runs')} ({cricket.get('b2_balls')})")

#     print("\n🎯 Last Ball:")
#     print(f"   {cricket.get('latest_ball')}")

#     print("\n💰 Market:")
#     print(f"   LTP: {price.get('ltp')} | Prev: {price.get('prev_ltp')} | TV: {price.get('tv')}")

#     print("\n📜 History:")
#     print(f"   Balls stored: {len(history.get('balls', []))}")
#     print(f"   Market ticks stored: {len(history.get('market', []))}")

#     print("\n🤖 Prediction:")
#     print(json.dumps(prediction, indent=2, ensure_ascii=False))


# def run_live_prediction(match_id: str, market_id: str, runner_id: str) -> dict:
#     cricket = get_latest_cricket(match_id) or {}
#     raw_price = get_latest_price(market_id, runner_id) or {}
#     price = normalize_price(market_id, runner_id, raw_price)

#     status = resolve_status(cricket, price)

#     prediction_payload = {
#         "status": status,
#         "source_match_id": str(match_id),
#         "market_id": str(market_id),
#         "runner_id": str(runner_id),
#         "cricket": cricket,
#         "price": price,
#     }

#     prediction = predict(prediction_payload)

#     raw_json = cricket.get("raw_json", {}) or {}

#     payload = process_and_push_live_update(
#         source_match_id=str(match_id),
#         market_id=str(market_id),
#         runner_id=str(runner_id),
#         cricket=cricket,
#         raw_json=raw_json,
#         price=price,
#         prediction=prediction,
#     )

#     payload["status"] = status
#     payload["type"] = "bet_signal"
#     payload["source_match_id"] = str(match_id)
#     payload["market_id"] = str(market_id)
#     payload["runner_id"] = str(runner_id)

#     # Ensure cricket exists
#     payload["cricket"] = payload.get("cricket", {}) or {}
#     payload["price"] = payload.get("price", {}) or {}
#     payload["prediction"] = payload.get("prediction", {}) or {}

#     # Ensure raw_json remains inside cricket
#     if "raw_json" not in payload["cricket"]:
#         payload["cricket"]["raw_json"] = raw_json

#     # Ensure stable ball_key
#     final_ball_key = payload.get("ball_key") or build_ball_key(match_id, payload["cricket"])
#     payload["ball_key"] = final_ball_key

#     # Enrich cricket section so CSV saver always has expected keys
#     payload["cricket"] = enrich_cricket_for_csv(payload["cricket"])

#     print("\n" + "-" * 60)
#     print(f"📌 STATUS: {status.upper()}")
#     print("-" * 60)
#     print(payload)

#     print(f"[ChannelPush] Pushing to group bet_signals: {payload}")

#     pretty_print_signal(payload)

#     return payload



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


def build_ball_key(match_id: str, cricket: dict) -> str | None:
    """
    Build a stable unique key for one ball.
    Priority:
    1) use existing cricket['ball_key'] if present
    2) use commentaryList[0] => inningsId + overNumber + ballNbr
    3) fallback to innings + overs + ballNbr
    """
    existing_ball_key = cricket.get("ball_key")
    if existing_ball_key:
        return str(existing_ball_key)

    innings = cricket.get("innings")
    overs = cricket.get("overs") or cricket.get("overs_float")
    raw_json = cricket.get("raw_json", {}) or {}

    commentary_list = raw_json.get("commentaryList") or []
    latest_commentary = commentary_list[0] if commentary_list else {}

    innings_id = latest_commentary.get("inningsId", innings)
    over_number = latest_commentary.get("overNumber")
    ball_number = latest_commentary.get("ballNbr")

    if innings_id is not None and over_number is not None and ball_number is not None:
        return f"{match_id}:{innings_id}:{over_number}:{ball_number}"

    if innings is not None and overs is not None and ball_number is not None:
        return f"{match_id}:{innings}:{overs}:{ball_number}"

    return None


def enrich_cricket_for_csv(cricket: dict) -> dict:
    """
    Ensure CSV-required cricket fields always exist.
    """
    cricket = cricket or {}
    raw_json = cricket.get("raw_json", {}) or {}
    miniscore = raw_json.get("miniscore", {}) or {}
    commentary_list = raw_json.get("commentaryList") or []
    latest_commentary = commentary_list[0] if commentary_list else {}

    striker = miniscore.get("batsmanStriker", {}) or {}
    non_striker = miniscore.get("batsmanNonStriker", {}) or {}
    bowler = miniscore.get("bowlerStriker", {}) or {}
    partnership = miniscore.get("partnerShip", {}) or {}

    if not cricket.get("latest_ball"):
        cricket["latest_ball"] = latest_commentary.get("commText", "")

    if not cricket.get("innings"):
        cricket["innings"] = miniscore.get("inningsId")

    if not cricket.get("overs_float") and cricket.get("overs") is not None:
        try:
            cricket["overs_float"] = float(cricket.get("overs"))
        except Exception:
            cricket["overs_float"] = None

    if not cricket.get("b1_name"):
        cricket["b1_name"] = striker.get("batName")
    if cricket.get("b1_runs") is None:
        cricket["b1_runs"] = striker.get("batRuns")
    if cricket.get("b1_balls") is None:
        cricket["b1_balls"] = striker.get("batBalls")
    if cricket.get("b1_4s") is None:
        cricket["b1_4s"] = striker.get("batFours")
    if cricket.get("b1_6s") is None:
        cricket["b1_6s"] = striker.get("batSixes")
    if cricket.get("b1_sr") is None:
        cricket["b1_sr"] = striker.get("batStrikeRate")

    if not cricket.get("b2_name"):
        cricket["b2_name"] = non_striker.get("batName")
    if cricket.get("b2_runs") is None:
        cricket["b2_runs"] = non_striker.get("batRuns")
    if cricket.get("b2_balls") is None:
        cricket["b2_balls"] = non_striker.get("batBalls")
    if cricket.get("b2_4s") is None:
        cricket["b2_4s"] = non_striker.get("batFours")
    if cricket.get("b2_6s") is None:
        cricket["b2_6s"] = non_striker.get("batSixes")
    if cricket.get("b2_sr") is None:
        cricket["b2_sr"] = non_striker.get("batStrikeRate")

    if not cricket.get("bw1_name"):
        cricket["bw1_name"] = bowler.get("bowlName")
    if cricket.get("bw1_overs") is None:
        cricket["bw1_overs"] = bowler.get("bowlOvs")
    if cricket.get("bw1_runs") is None:
        cricket["bw1_runs"] = bowler.get("bowlRuns")
    if cricket.get("bw1_wkts") is None:
        cricket["bw1_wkts"] = bowler.get("bowlWkts")
    if cricket.get("bw1_eco") is None:
        cricket["bw1_eco"] = bowler.get("bowlEcon")

    if cricket.get("p_runs") is None:
        cricket["p_runs"] = partnership.get("runs")
    if cricket.get("p_balls") is None:
        cricket["p_balls"] = partnership.get("balls")

    if cricket.get("score_num") is None:
        score_value = cricket.get("score")
        if isinstance(score_value, str) and "/" in score_value:
            try:
                cricket["score_num"] = int(score_value.split("/")[0])
            except Exception:
                cricket["score_num"] = None

    return cricket


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
    payload["source_match_id"] = str(match_id)
    payload["market_id"] = str(market_id)
    payload["runner_id"] = str(runner_id)

    # Ensure cricket exists
    payload["cricket"] = payload.get("cricket", {}) or {}
    payload["price"] = payload.get("price", {}) or {}
    payload["prediction"] = payload.get("prediction", {}) or {}

    # Ensure raw_json remains inside cricket
    if "raw_json" not in payload["cricket"]:
        payload["cricket"]["raw_json"] = raw_json

    # Ensure stable ball_key
    final_ball_key = payload.get("ball_key") or build_ball_key(match_id, payload["cricket"])
    payload["ball_key"] = final_ball_key

    # Enrich cricket section so CSV saver always has expected keys
    payload["cricket"] = enrich_cricket_for_csv(payload["cricket"])

    print("\n" + "-" * 60)
    print(f"📌 STATUS: {status.upper()}")
    print("-" * 60)
    print(payload)

    print(f"[ChannelPush] Pushing to group bet_signals: {payload}")

    pretty_print_signal(payload)

    return payload