# from betapp.channel_push import push_to_group
# from betapp.history_store import (
#     build_ball_history_item,
#     is_new_ball,
#     save_ball_history,
#     build_market_history_item,
#     is_new_market_tick,
#     save_market_history,
# )
# from betapp.ws_payloads import build_signal_ws_payload


# def process_and_push_live_update(
#     source_match_id: str,
#     market_id: str,
#     runner_id: str,
#     cricket: dict,
#     raw_json: dict,
#     price: dict,
#     prediction: dict,
# ):
#     ball_key = None

#     # 1. cricket history
#     ball_item = build_ball_history_item(source_match_id, raw_json)
#     if ball_item:
#         ball_key = ball_item["ball_key"]
#         if is_new_ball(source_match_id, ball_key):
#             save_ball_history(source_match_id, ball_item)

#     # 2. market history
#     market_item = build_market_history_item(market_id, runner_id, price)
#     if market_item and is_new_market_tick(market_id, runner_id, market_item["history_key"]):
#         save_market_history(market_id, runner_id, market_item)

#     # 3. final websocket payload
#     payload = build_signal_ws_payload(
#         source_match_id=source_match_id,
#         market_id=market_id,
#         runner_id=runner_id,
#         cricket=cricket,
#         price=price,
#         prediction=prediction,
#         ball_key=ball_key,
#     )

#     # 4. websocket push
#     push_to_group("bet_signals", payload)

#     return payload



# from betapp.channel_push import push_to_group
# from betapp.history_store import (
#     build_and_save_ball_history,
#     build_market_history_item,
#     is_new_market_tick,
#     save_market_history,
#     get_combined_history,
# )
# from betapp.ws_payloads import build_signal_ws_payload


# def process_and_push_live_update(
#     source_match_id: str,
#     market_id: str,
#     runner_id: str,
#     cricket: dict,
#     raw_json: dict,
#     price: dict,
#     prediction: dict,
# ):
#     ball_key = None

#     # =========================================
#     # 1. SAVE FULL CRICKET BALL HISTORY
#     # =========================================
#     saved_ball_items = build_and_save_ball_history(source_match_id, raw_json)

#     if saved_ball_items:
#         latest_ball = saved_ball_items[-1]
#         ball_key = latest_ball.get("ball_key")

#     # =========================================
#     # 2. SAVE MARKET HISTORY
#     # =========================================
#     market_item = build_market_history_item(market_id, runner_id, price)
#     if market_item and is_new_market_tick(
#         market_id,
#         runner_id,
#         market_item["history_key"],
#     ):
#         save_market_history(market_id, runner_id, market_item)

#     # =========================================
#     # 3. LOAD FULL SAVED HISTORY
#     # =========================================
#     history = get_combined_history(
#         source_match_id=source_match_id,
#         market_id=market_id,
#         runner_id=runner_id,
#         ball_limit=None,   # FULL BALL HISTORY
#         market_limit=200,
#     )

#     # =========================================
#     # 4. BUILD FINAL WEBSOCKET PAYLOAD
#     # =========================================
#     payload = build_signal_ws_payload(
#         source_match_id=source_match_id,
#         market_id=market_id,
#         runner_id=runner_id,
#         cricket=cricket,
#         price=price,
#         prediction=prediction,
#         ball_key=ball_key,
#         history=history,
#     )

#     # =========================================
#     # 5. PUSH TO FRONTEND
#     # =========================================
#     push_to_group("bet_signals", payload)

#     return payload


# from betapp.channel_push import push_to_group
# from betapp.history_store import (
#     build_and_save_ball_history,
#     build_market_history_item,
#     save_market_history,
#     save_pattern_history,
#     get_combined_history,
#     save_ball_history,
#     save_ball_history_items,
# )
# from betapp.ws_payloads import build_signal_ws_payload
# import time as _time
 
 
# def process_and_push_live_update(
#     source_match_id: str,
#     market_id: str,
#     runner_id: str,
#     cricket: dict,
#     raw_json: dict,
#     price: dict,
#     prediction: dict,
# ):
#     ball_key = None
 
#     # Get runner_name from price (populated by normalize_price in live_signal_engine)
#     runner_name = (
#         price.get("runner_name")
#         or cricket.get("runner_name")
#         or str(runner_id)
#     )
 
#     # =========================================
#     # 1. SAVE BALL HISTORY
#     # =========================================
#     saved_ball_items = build_and_save_ball_history(source_match_id, raw_json)
 
#     if saved_ball_items:
#         # Add runner_name, market_id, runner_id to every ball item
#         for item in saved_ball_items:
#             item["market_id"]   = str(market_id)
#             item["runner_id"]   = str(runner_id)
#             item["runner_name"] = runner_name
#         save_ball_history_items(source_match_id, saved_ball_items)
 
#         latest_ball = saved_ball_items[-1]
#         ball_key = latest_ball.get("ball_key")
#     else:
#         # Fallback: build ball item directly from cricket dict
#         # Works even when poll_live_match is not running
#         overs = cricket.get("overs_float") or cricket.get("overs")
#         innings = cricket.get("innings")
#         if overs is not None and innings is not None:
#             try:
#                 over_float = float(overs)
#                 over_num = int(over_float)
#                 ball_in_over = round((over_float - over_num) * 10)
#                 ball_number = (over_num * 6) + ball_in_over
#                 ball_key = f"{source_match_id}:{innings}:{overs}:{ball_number}"
#                 ball_item = {
#                     "ball_key":       ball_key,
#                     "source_match_id": str(source_match_id),
#                     "market_id":      str(market_id),
#                     "runner_id":      str(runner_id),
#                     "runner_name":    runner_name,
#                     "innings":        innings,
#                     "over":           overs,
#                     "commentary":     cricket.get("latest_ball", ""),
#                     "score":          cricket.get("score_num"),
#                     "wickets":        cricket.get("wickets"),
#                     "overs":          cricket.get("overs"),
#                     "current_run_rate":  cricket.get("crr"),
#                     "required_run_rate": cricket.get("rrr"),
#                     "recent":         cricket.get("recent"),
#                     "partnership": {
#                         "runs":  cricket.get("p_runs"),
#                         "balls": cricket.get("p_balls"),
#                     },
#                     "batter_striker": {
#                         "name":        cricket.get("b1_name"),
#                         "runs":        cricket.get("b1_runs"),
#                         "balls":       cricket.get("b1_balls"),
#                         "fours":       cricket.get("b1_4s"),
#                         "sixes":       cricket.get("b1_6s"),
#                         "strike_rate": cricket.get("b1_sr"),
#                     },
#                     "batter_non_striker": {
#                         "name":        cricket.get("b2_name"),
#                         "runs":        cricket.get("b2_runs"),
#                         "balls":       cricket.get("b2_balls"),
#                         "fours":       cricket.get("b2_4s"),
#                         "sixes":       cricket.get("b2_6s"),
#                         "strike_rate": cricket.get("b2_sr"),
#                     },
#                     "bowler": {
#                         "name":    cricket.get("bw1_name"),
#                         "overs":   cricket.get("bw1_overs"),
#                         "runs":    cricket.get("bw1_runs"),
#                         "wickets": cricket.get("bw1_wkts"),
#                         "economy": cricket.get("bw1_eco"),
#                     },
#                     "timestamp": _time.time(),
#                 }
#                 save_ball_history(source_match_id, ball_item)
#             except Exception as e:
#                 print(f"[LiveSignalService] ball fallback error: {e}")
 
#     # fallback ball_key from cricket dict
#     if not ball_key:
#         ball_key = cricket.get("ball_key")
 
#     # =========================================
#     # 2. SAVE MARKET HISTORY
#     # Use timestamp key — saves every tick even when ltp/tv unchanged
#     # =========================================
#     market_item = build_market_history_item(market_id, runner_id, price)
#     if market_item:
#         ts = market_item.get("timestamp") or _time.time()
#         market_item["history_key"] = f"{market_id}:{runner_id}:{ts}"
#         market_item["runner_name"] = runner_name
#         market_item["ball_key"]    = ball_key
#         save_market_history(market_id, runner_id, market_item)
 
#     # =========================================
#     # 3. SAVE PATTERN / PREDICTION HISTORY
#     # =========================================
#     if ball_key:
#         save_pattern_history(
#             source_match_id=source_match_id,
#             ball_key=ball_key,
#             pattern_payload={
#                 "ball_key":        ball_key,
#                 "source_match_id": str(source_match_id),
#                 "market_id":       str(market_id),
#                 "runner_id":       str(runner_id),
#                 "runner_name":     runner_name,
#                 "pattern":         prediction.get("pattern"),
#                 "signal":          prediction.get("signal"),
#                 "price_going":     prediction.get("price_going"),
#                 "confidence":      prediction.get("confidence"),
#                 "reason":          prediction.get("reason"),
#                 "features_used":   prediction.get("features_used"),
#             },
#         )
 
#     # =========================================
#     # 4. LOAD FULL HISTORY
#     # =========================================
#     history = get_combined_history(
#         source_match_id=source_match_id,
#         market_id=market_id,
#         runner_id=runner_id,
#         ball_limit=None,
#         market_limit=None,
#         pattern_limit=None,
#     )
 
#     # =========================================
#     # 5. BUILD WEBSOCKET PAYLOAD
#     # =========================================
#     payload = build_signal_ws_payload(
#         source_match_id=source_match_id,
#         market_id=market_id,
#         runner_id=runner_id,
#         cricket=cricket,
#         price=price,
#         prediction=prediction,
#         ball_key=ball_key,
#         history=history,
#     )
 
#     # =========================================
#     # 6. PUSH TO FRONTEND
#     # =========================================
#     push_to_group("bet_signals", payload)
 
#     return payload

from betapp.channel_push import push_to_group
from betapp.history_store import (
    build_and_save_ball_history,
    build_market_history_item,
    is_new_market_tick,
    save_market_history,
    save_pattern_history,
    get_combined_history,
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

    # =========================================
    # 1. SAVE ALL BALL HISTORY
    # =========================================
    saved_ball_items = build_and_save_ball_history(source_match_id, raw_json)

    if saved_ball_items:
        latest_ball = saved_ball_items[-1]
        ball_key = latest_ball.get("ball_key")

    # fallback if not built right now
    if not ball_key:
        ball_key = cricket.get("ball_key")

    # =========================================
    # 2. SAVE MARKET HISTORY
    # =========================================
    market_item = build_market_history_item(market_id, runner_id, price)
    if market_item and is_new_market_tick(
        market_id,
        runner_id,
        market_item["history_key"],
    ):
        save_market_history(market_id, runner_id, market_item)

    # =========================================
    # 3. SAVE PATTERN / PREDICTION HISTORY
    # =========================================
    if ball_key:
        save_pattern_history(
            source_match_id=source_match_id,
            ball_key=ball_key,
            pattern_payload={
                "ball_key": ball_key,
                "source_match_id": str(source_match_id),
                "market_id": str(market_id),
                "runner_id": str(runner_id),
                "pattern": prediction.get("pattern"),
                "signal": prediction.get("signal"),
                "price_going": prediction.get("price_going"),
                "confidence": prediction.get("confidence"),
                "reason": prediction.get("reason"),
                "features_used": prediction.get("features_used"),
            },
        )

    # =========================================
    # 4. LOAD FULL HISTORY FROM START
    # =========================================
    history = get_combined_history(
        source_match_id=source_match_id,
        market_id=market_id,
        runner_id=runner_id,
        ball_limit=None,       # FULL BALL HISTORY
        market_limit=None,     # FULL MARKET HISTORY
        pattern_limit=None,    # FULL PATTERN HISTORY
    )

    # =========================================
    # 5. BUILD WEBSOCKET PAYLOAD
    # =========================================
    payload = build_signal_ws_payload(
        source_match_id=source_match_id,
        market_id=market_id,
        runner_id=runner_id,
        cricket=cricket,
        price=price,
        prediction=prediction,
        ball_key=ball_key,
        history=history,
    )

    # =========================================
    # 6. PUSH TO FRONTEND
    # =========================================
    push_to_group("bet_signals", payload)

    return payload