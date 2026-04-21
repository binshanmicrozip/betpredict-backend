from betapp.channel_push import push_to_group
from betapp.history_store import (
    build_ball_history_item,
    is_new_ball,
    save_ball_history,
    build_market_history_item,
    is_new_market_tick,
    save_market_history,
)
from betapp.ws_payloads import build_signal_ws_payload


def process_and_push_live_update(
    source_match_id: str,
    market_id: str,
    runner_id: str,
    cricket: dict,
    raw_json: dict,
    price: dict,
    prediction: dict,
):
    ball_key = None

    # 1. cricket history
    ball_item = build_ball_history_item(source_match_id, raw_json)
    if ball_item:
        ball_key = ball_item["ball_key"]
        if is_new_ball(source_match_id, ball_key):
            save_ball_history(source_match_id, ball_item)

    # 2. market history
    market_item = build_market_history_item(market_id, runner_id, price)
    if market_item and is_new_market_tick(market_id, runner_id, market_item["history_key"]):
        save_market_history(market_id, runner_id, market_item)

    # 3. final websocket payload
    payload = build_signal_ws_payload(
        source_match_id=source_match_id,
        market_id=market_id,
        runner_id=runner_id,
        cricket=cricket,
        price=price,
        prediction=prediction,
        ball_key=ball_key,
    )

    # 4. websocket push
    push_to_group("bet_signals", payload)

    return payload