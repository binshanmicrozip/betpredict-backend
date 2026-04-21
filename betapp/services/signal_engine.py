import json
from decimal import Decimal
import redis

from django.conf import settings

from betapp.models import Signal, IPLMatch
from betapp.predictor import predict
from betapp.services.player_cache_service import get_cached_player_stats

try:
    from betapp.channel_push import push_signal_to_frontend
except Exception:
    push_signal_to_frontend = None


r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)


def get_latest_cricket_context(match_id: str) -> dict:
    raw = r.get(f"cricket_live:{match_id}")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def get_latest_price_context(market_id: str, runner_id: str) -> dict:
    raw = r.get(f"price:{market_id}:{runner_id}")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return {
            "market_id": str(market_id),
            "runner_id": str(runner_id),
            "ltp": _to_float(data.get("ltp")),
            "prev_ltp": _to_float(data.get("prev_ltp")),
            "tv": _to_float(data.get("tv")),
            "updated_at": data.get("updated_at"),
        }
    except Exception:
        return {}


def _to_float(value, default=0.0):
    try:
        if value in [None, ""]:
            return default
        return float(value)
    except Exception:
        return default


def extract_probability(prediction_result: dict) -> float:
    """
    Try to read probability/confidence from predictor output.
    Falls back to signal-based mapping if needed.
    """
    prediction = prediction_result.get("prediction", {}) or {}

    for key in ["probability", "win_probability", "prob", "confidence", "score"]:
        value = prediction.get(key)
        if value is not None:
            try:
                return float(value)
            except Exception:
                pass

    signal = str(prediction.get("signal", "WAIT")).upper()
    if signal == "BACK":
        return 0.75
    if signal == "LAY":
        return 0.25
    return 0.50


def extract_signal(prediction_result: dict) -> str:
    prediction = prediction_result.get("prediction", {}) or {}
    signal = prediction.get("signal")
    if signal:
        return str(signal).upper()
    return "WAIT"


def save_latest_signal_to_redis(signal_obj: Signal):
    redis_key = f"signal:latest:{signal_obj.market_id}:{signal_obj.runner_id}"

    payload = {
        "id": signal_obj.id,
        "match_id": signal_obj.match_id if signal_obj.match_id else None,
        "market_id": signal_obj.market_id,
        "runner_id": signal_obj.runner_id,
        "striker_name": signal_obj.striker_name,
        "phase": signal_obj.phase,
        "innings_type": signal_obj.innings_type,
        "final_probability": float(signal_obj.final_probability) if signal_obj.final_probability is not None else None,
        "signal": signal_obj.signal,
        "model_source": signal_obj.model_source,
        "created_at": signal_obj.created_at.isoformat(),
    }

    r.set(redis_key, json.dumps(payload))

    if push_signal_to_frontend:
        try:
            push_signal_to_frontend(payload)
        except Exception:
            pass


def run_signal_engine(match_id: str, market_id: str, runner_id: str, price_data: dict | None = None) -> dict:
    cricket = get_latest_cricket_context(match_id)
    if not cricket:
        return {
            "status": "skipped",
            "reason": "no_cricket_context",
            "match_id": str(match_id),
            "market_id": str(market_id),
            "runner_id": str(runner_id),
        }

    if not price_data:
        price_data = get_latest_price_context(market_id, runner_id)

    if not price_data:
        return {
            "status": "skipped",
            "reason": "no_price_context",
            "match_id": str(match_id),
            "market_id": str(market_id),
            "runner_id": str(runner_id),
        }

    striker_name = cricket.get("striker_name") or cricket.get("b1_name")
    phase = cricket.get("phase")
    innings_type = cricket.get("innings_type")

    player_cache = {}
    if striker_name and phase and innings_type:
        player_cache = get_cached_player_stats(striker_name, phase, innings_type)

    # IMPORTANT:
    # This assumes your predictor.py exposes:
    # predict(cricket_data=..., price_data=..., player_stats=...)
    # If your real function signature differs, only this block needs adjustment.
    result = predict(
        cricket_data=cricket,
        price_data=price_data,
        player_stats=player_cache,
    )

    probability = extract_probability(result)
    final_signal = extract_signal(result)

    match = IPLMatch.objects.filter(match_id=str(match_id)).first()

    signal_obj = Signal.objects.create(
        match=match,
        market_id=str(market_id),
        runner_id=str(runner_id),
        striker_name=striker_name,
        phase=phase,
        innings_type=innings_type,
        final_probability=Decimal(str(round(probability, 4))),
        signal=final_signal,
        model_source="betpredict_model.pkl",
        raw_features={
            "cricket": cricket,
            "price": price_data,
            "player_cache": player_cache,
        },
        raw_output=result,
    )

    save_latest_signal_to_redis(signal_obj)

    return {
        "status": "ok",
        "signal_id": signal_obj.id,
        "match_id": str(match_id),
        "market_id": str(market_id),
        "runner_id": str(runner_id),
        "final_probability": probability,
        "signal": final_signal,
        "raw_result": result,
    }