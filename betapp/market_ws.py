import json
import time
import threading
import websocket
import redis
from django.conf import settings

websocket.enableTrace(True)

r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)

WS_URL_TEMPLATE = "wss://sr-socket.myzosh.com:8881?token={{agent_code}}-{{timestamp}}"
HEARTBEAT_INTERVAL = 10


def make_price_key(market_id, runner_id):
    return f"price:{market_id}:{runner_id}"


def save_latest_price(market_id, runner_id, ltp, tv=None):
    key = make_price_key(market_id, runner_id)
    old = r.hgetall(key)

    prev_ltp = old.get("ltp")
    if prev_ltp is None:
        prev_ltp = ltp

    payload = {
        "market_id": str(market_id),
        "runner_id": str(runner_id),
        "ltp": str(ltp),
        "prev_ltp": str(prev_ltp),
        "tv": str(tv or 0),
        "updated_at": str(time.time()),
    }

    r.hset(key, mapping=payload)
    print(f"[MarketWS] SAVED TO REDIS => {key} => {payload}")


def parse_market_message(message: str):
    print("[MarketWS] RAW MESSAGE RECEIVED")
    print(message)

    try:
        payload = json.loads(message)
    except Exception as e:
        print("[MarketWS] JSON parse error:", repr(e))
        return

    print("[MarketWS] PARSED PAYLOAD:", payload)

    if payload.get("messageType") != "match_odds":
        print("[MarketWS] Not match_odds, skipping")
        return

    for market in payload.get("data", []):
        market_id = market.get("mi")
        ltp_list = market.get("ltp", [])

        print(f"[MarketWS] market_id={market_id}, ltp_count={len(ltp_list)}")

        for item in ltp_list:
            runner_id = item.get("ri")
            ltp = item.get("ltp")
            tv = item.get("tv")

            print(f"[MarketWS] runner_id={runner_id}, ltp={ltp}, tv={tv}")

            if market_id is None or runner_id is None or ltp is None:
                print("[MarketWS] Invalid row, skipping")
                continue

            save_latest_price(market_id, runner_id, ltp, tv)


class MarketWebSocketClient:
    def __init__(self, token: str, market_ids: list[str]):
        self.token = token
        self.market_ids = [str(x) for x in market_ids]
        self.ws = None
        self.running = True

    def on_open(self, ws):
        print("[MarketWS] CONNECTED")

        subscribe_payload = {
            "action": "set",
            "markets": ",".join(self.market_ids),
        }
        print("[MarketWS] SUBSCRIBE PAYLOAD:", subscribe_payload)

        ws.send(json.dumps(subscribe_payload))
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

    def on_message(self, ws, message):
        print("[MarketWS] on_message TRIGGERED")
        parse_market_message(message)

    def on_error(self, ws, error):
        print("[MarketWS] ERROR:", repr(error))

    def on_close(self, ws, close_status_code, close_msg):
        print(f"[MarketWS] CLOSED: code={close_status_code}, msg={close_msg}")

    def on_pong(self, ws, message):
        print("[MarketWS] PONG RECEIVED")

    def send_heartbeat(self):
        while self.running:
            try:
                if self.ws:
                    heartbeat_payload = {"action": "heartbeat", "data": []}
                    print("[MarketWS] HEARTBEAT SENT")
                    self.ws.send(json.dumps(heartbeat_payload))
            except Exception as e:
                print("[MarketWS] HEARTBEAT ERROR:", repr(e))
            time.sleep(HEARTBEAT_INTERVAL)

    def run(self):
        url = WS_URL_TEMPLATE.format(token=self.token)
        print("[MarketWS] CONNECTING URL:", url)
        print("[MarketWS] MARKET IDS:", self.market_ids)

        self.ws = websocket.WebSocketApp(
            url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_pong=self.on_pong,
        )

        self.ws.run_forever(
            sslopt={"check_hostname": False},
            ping_interval=20,
            ping_timeout=10,
        )