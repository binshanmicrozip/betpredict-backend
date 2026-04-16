import json
import time
import threading
import ssl
import websocket
import redis

r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)


AGENT_CODE = "aig"

# Use the socket URL your provider finally confirms.
# If they insist on the old one, change only this line.
WS_BASE_URL = "wss://socket.myzosh.com:8881"

SUBSCRIBED_MARKETS = ["1", "2", "3", "4"]


def build_ws_url():
    return f"{WS_BASE_URL}?token={AGENT_CODE}-{int(time.time())}"


def redis_key(market_id, runner_id):
    return f"match_odds:{market_id}:{runner_id}"


def get_previous_ltp(market_id, runner_id, current_ltp):
    existing = r.get(redis_key(market_id, runner_id))
    if not existing:
        return current_ltp

    try:
        existing = json.loads(existing)
        return existing.get("ltp", current_ltp)
    except Exception:
        return current_ltp


def save_runner_tick(market_id, runner_id, ltp, tdv, ip):
    prev_ltp = get_previous_ltp(market_id, runner_id, ltp)

    payload = {
        "market_id": market_id,
        "runner_id": runner_id,
        "ltp": float(ltp),
        "prev_ltp": float(prev_ltp),
        "tdv": float(tdv or 0),
        "ip": int(ip or 0),
    }

    r.set(redis_key(market_id, runner_id), json.dumps(payload))


def on_message(ws, message):
    try:
        payload = json.loads(message)
    except Exception as e:
        print("JSON parse error:", e)
        return

    # Real socket payload uses messageType
    if str(payload.get("messageType", "")).lower() != "match_odds":
        return

    markets = payload.get("data", [])
    if not isinstance(markets, list):
        return

    for market in markets:
        market_id = market.get("mi")
        tdv = market.get("tdv", 0)
        ip = market.get("ip", 0)

        ltp_list = market.get("ltp", [])
        if not isinstance(ltp_list, list):
            continue

        for item in ltp_list:
            runner_id = item.get("ri")
            ltp = item.get("ltp")

            if market_id is None or runner_id is None or ltp is None:
                continue

            save_runner_tick(
                market_id=market_id,
                runner_id=runner_id,
                ltp=ltp,
                tdv=tdv,
                ip=ip,
            )

            print(
                f"Saved -> market={market_id}, runner={runner_id}, "
                f"ltp={ltp}, tdv={tdv}, ip={ip}"
            )


def on_error(ws, error):
    print("WebSocket error:", error)


def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed:", close_status_code, close_msg)


def send_heartbeat(ws):
    while True:
        try:
            ws.send(json.dumps({"action": "heartbeat", "data": []}))
        except Exception as e:
            print("Heartbeat error:", e)
            break
        time.sleep(10)


def subscribe_markets(ws):
    ws.send(json.dumps({
        "action": "set",
        "markets": ",".join(SUBSCRIBED_MARKETS)
    }))
    print("Subscribed markets:", SUBSCRIBED_MARKETS)


def on_open(ws):
    print("WebSocket connected")
    subscribe_markets(ws)

    threading.Thread(
        target=send_heartbeat,
        args=(ws,),
        daemon=True
    ).start()


def start_websocket():
    while True:
        try:
            ws_url = build_ws_url()
            print("Connecting to:", ws_url)

            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE},
                ping_interval=20,
                ping_timeout=10,
            )
        except Exception as e:
            print("WebSocket startup error:", e)

        print("Reconnecting in 5 seconds...")
        time.sleep(5)