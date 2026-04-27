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

# from datetime import datetime, timezone
# from betapp.models import LiveMarketTick


# def compute_values(payload):
#     ltp = payload.get("ltp")
#     prev_ltp = payload.get("prev_ltp")

#     try:
#         ltp = float(ltp)
#         prev_ltp = float(prev_ltp)

#         price_change = ltp - prev_ltp
#         price_change_pct = (price_change / prev_ltp) * 100 if prev_ltp else None

#         if price_change > 0:
#             direction = "UP"
#         elif price_change < 0:
#             direction = "DOWN"
#         else:
#             direction = "SAME"

#         return price_change, price_change_pct, direction
#     except Exception:
#         return None, None, "UNKNOWN"


# def save_live_market_tick(payload: dict):

#     # GUARD 1: Only save when match is LIVE
#     if not payload.get("in_play"):
#         print(
#             f"[LiveMarketTick] SKIPPED — not inplay: "
#             f"market_id={payload.get('market_id')} "
#             f"runner_id={payload.get('runner_id')}"
#         )
#         return None

#     # GUARD 2: Skip if no price
#     ltp = payload.get("ltp")
#     if ltp is None:
#         print(
#             f"[LiveMarketTick] SKIPPED — ltp is None: "
#             f"market_id={payload.get('market_id')} "
#             f"runner_id={payload.get('runner_id')}"
#         )
#         return None

#     publish_time_ms = payload.get("publish_time_ms")
#     publish_time_utc = payload.get("publish_time_utc")

#     if not publish_time_utc and publish_time_ms:
#         publish_time_utc = datetime.fromtimestamp(
#             int(publish_time_ms) / 1000, tz=timezone.utc
#         )

#     # GUARD 3: Duplicate check — same market + runner + same millisecond
#     already_exists = LiveMarketTick.objects.filter(
#         market_id=payload.get("market_id"),
#         runner_id=str(payload.get("runner_id")),
#         publish_time_ms=publish_time_ms,
#     ).exists()

#     if already_exists:
#         print(
#             f"[LiveMarketTick] SKIPPED — duplicate: "
#             f"market_id={payload.get('market_id')} "
#             f"runner_id={payload.get('runner_id')} "
#             f"publish_time_ms={publish_time_ms}"
#         )
#         return None

#     price_change, price_change_pct, direction = compute_values(payload)

#     obj = LiveMarketTick.objects.create(
#         market_id=payload.get("market_id"),
#         event_id=payload.get("event_id"),
#         event_name=payload.get("event_name"),
#         market_type=payload.get("market_type"),
#         market_time=payload.get("market_time"),
#         runner_id=str(payload.get("runner_id")),
#         runner_name=payload.get("runner_name"),
#         publish_time_ms=publish_time_ms,
#         publish_time_utc=publish_time_utc,
#         ltp=payload.get("ltp"),
#         prev_ltp=payload.get("prev_ltp"),
#         price_change=price_change,
#         price_change_pct=price_change_pct,
#         price_direction=direction,
#         market_status=payload.get("market_status"),
#         in_play=payload.get("in_play", False),
#         bet_delay=payload.get("bet_delay"),
#         winner=payload.get("winner"),
#         settled_time=payload.get("settled_time"),
#         year=payload.get("year"),
#         month=payload.get("month"),
#     )

#     print(
#         f"[LiveMarketTick] SAVED market_id={payload.get('market_id')} "
#         f"runner_id={payload.get('runner_id')} "
#         f"ltp={ltp} "
#         f"direction={direction}"
#     )

#     return obj


# def get_latest_ticks(market_id: str, runner_id: str = None, limit: int = 100):
#     """
#     Returns latest data FIRST (newest to oldest).
#     Always filters in_play=True so only live data is returned.
#     """
#     qs = LiveMarketTick.objects.filter(
#         market_id=market_id,
#         in_play=True,
#     )

#     if runner_id:
#         qs = qs.filter(runner_id=str(runner_id))

#     return qs.order_by("-publish_time_utc")[:limit]