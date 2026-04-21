from datetime import datetime, timezone
from betapp.models import LiveMarketTick


def compute_values(payload):
    ltp = payload.get("ltp")
    prev_ltp = payload.get("prev_ltp")

    try:
        ltp = float(ltp)
        prev_ltp = float(prev_ltp)

        price_change = ltp - prev_ltp
        price_change_pct = (price_change / prev_ltp) * 100 if prev_ltp else None

        if price_change > 0:
            direction = "UP"
        elif price_change < 0:
            direction = "DOWN"
        else:
            direction = "SAME"

        return price_change, price_change_pct, direction
    except Exception:
        return None, None, "UNKNOWN"


def save_live_market_tick(payload: dict):
    price_change, price_change_pct, direction = compute_values(payload)

    publish_time_ms = payload.get("publish_time_ms")
    publish_time_utc = payload.get("publish_time_utc")

    if not publish_time_utc and publish_time_ms:
        publish_time_utc = datetime.fromtimestamp(
            int(publish_time_ms) / 1000, tz=timezone.utc
        )

    obj = LiveMarketTick.objects.create(
        market_id=payload.get("market_id"),
        event_id=payload.get("event_id"),
        event_name=payload.get("event_name"),
        market_type=payload.get("market_type"),
        market_time=payload.get("market_time"),
        runner_id=str(payload.get("runner_id")),
        runner_name=payload.get("runner_name"),
        publish_time_ms=publish_time_ms,
        publish_time_utc=publish_time_utc,
        ltp=payload.get("ltp"),
        prev_ltp=payload.get("prev_ltp"),
        price_change=price_change,
        price_change_pct=price_change_pct,
        price_direction=direction,
        market_status=payload.get("market_status"),
        in_play=payload.get("in_play", False),
        bet_delay=payload.get("bet_delay"),
        winner=payload.get("winner"),
        settled_time=payload.get("settled_time"),
        year=payload.get("year"),
        month=payload.get("month"),
    )

    return obj