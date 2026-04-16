from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any

import redis
from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.utils.dateparse import parse_datetime

from .models import Market, Runner, PriceTick
from .cricbuzz_live import get_live_payload
from .redis_price import get_latest_price
from .prediction_service import run_prediction
from .channel_push import push_signal_to_frontend
from .live_signal_engine import run_live_prediction


# Redis connection
r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)

LAST_BALL_KEY = "cricbuzz:last_ball_key"
LATEST_CRICKET_KEY = "cricbuzz:latest"
LATEST_PREDICTION_KEY = "prediction:latest"

# # Temporary hardcoded values
# CRICBUZZ_MATCH_ID = "149779"
# MARKET_ID = "1312"
# RUNNER_ID = "228749"


def _to_decimal(value: Any):
    if value in (None, "", "null"):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_int(value: Any):
    if value in (None, "", "null"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_ball_key(cricket: dict) -> str:
    overs = cricket.get("overs", "")
    score = cricket.get("score", "")
    recent = cricket.get("recent", "")
    latest_ball = cricket.get("latest_ball", "")
    return f"{overs}|{score}|{recent}|{latest_ball}"


@shared_task
def test_add(x, y):
    return x + y


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def insert_ticks(self, ticks: list[dict], batch_size: int = 2000) -> dict:
    """
    Bulk insert price tick rows.
    """
    if not ticks:
        return {
            "received": 0,
            "prepared": 0,
            "inserted_estimate": 0,
            "skipped": 0,
            "errors": [],
        }

    errors: list[dict] = []

    market_ids = {str(t["market_id"]) for t in ticks if t.get("market_id")}
    selection_ids = {_to_int(t.get("selection_id")) for t in ticks if t.get("selection_id") is not None}
    selection_ids.discard(None)

    markets = Market.objects.in_bulk(market_ids, field_name="market_id")

    runners_qs = Runner.objects.filter(
        market_id__in=market_ids,
        selection_id__in=selection_ids,
    ).only("id", "market_id", "selection_id")

    runner_map = {
        (str(r.market_id), int(r.selection_id)): r
        for r in runners_qs
    }

    objects: list[PriceTick] = []

    for i, tick in enumerate(ticks):
        market_id = str(tick.get("market_id", "")).strip()
        selection_id = _to_int(tick.get("selection_id"))
        tick_time_raw = tick.get("tick_time")
        tick_time = parse_datetime(tick_time_raw) if isinstance(tick_time_raw, str) else tick_time_raw

        if not market_id or selection_id is None or tick_time is None:
            errors.append({
                "index": i,
                "reason": "missing_or_invalid_required_fields",
                "tick": tick,
            })
            continue

        market = markets.get(market_id)
        if not market:
            errors.append({
                "index": i,
                "reason": f"market_not_found:{market_id}",
                "tick": tick,
            })
            continue

        runner = runner_map.get((market_id, selection_id))
        if not runner:
            errors.append({
                "index": i,
                "reason": f"runner_not_found:{market_id}:{selection_id}",
                "tick": tick,
            })
            continue

        ltp = _to_decimal(tick.get("ltp"))
        if ltp is None:
            errors.append({
                "index": i,
                "reason": "invalid_ltp",
                "tick": tick,
            })
            continue

        obj = PriceTick(
            market_id=market.market_id,
            runner_id=runner.id,
            year=_to_int(tick.get("year")),
            month=_to_int(tick.get("month")),
            day=_to_int(tick.get("day")),
            snapshot=tick.get("snapshot"),
            tick_time=tick_time,
            ltp=ltp,
            win_prob=_to_decimal(tick.get("win_prob")),
            traded_volume=_to_decimal(tick.get("traded_volume")),
            phase=tick.get("phase"),
        )
        objects.append(obj)

    if not objects:
        return {
            "received": len(ticks),
            "prepared": 0,
            "inserted_estimate": 0,
            "skipped": len(errors),
            "errors": errors[:50],
        }

    with transaction.atomic():
        PriceTick.objects.bulk_create(
            objects,
            batch_size=batch_size,
            ignore_conflicts=True,
        )

    return {
        "received": len(ticks),
        "prepared": len(objects),
        "inserted_estimate": len(objects),
        "skipped": len(errors),
        "errors": errors[:50],
    }


@shared_task
def poll_cricbuzz_and_predict(match_id: str, market_id: str, runner_id: str):
    return run_live_prediction(match_id, market_id, runner_id)