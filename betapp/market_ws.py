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

# Keep your real websocket URL here.
# If token is already full, use this format.
WS_HOSTS = [
    "socket.myzosh.com:443",
    "socket.myzosh.com:8881",
]
WS_URL_TEMPLATE = "wss://{host}?token={token}"

HEARTBEAT_INTERVAL = 10


def make_price_key(market_id, runner_id):
    return f"price:{market_id}:{runner_id}"


def save_latest_price(market_id, runner_id, ltp, tv=None, extra_data=None):
    """
    Save latest price into Redis hash.
    """
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

    if extra_data:
        for k, v in extra_data.items():
            payload[str(k)] = "" if v is None else str(v)

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
        bmi = market.get("bmi")   # Example: 1.256693299
        mi = market.get("mi")     # Example: 1640194
        ltp_list = market.get("ltp", [])

        # IMPORTANT:
        # Use BMI as primary market_id because your predictor / signal loop uses that.
        primary_market_id = bmi if bmi is not None else mi

        print(
            f"[MarketWS] bmi={bmi}, mi={mi}, primary_market_id={primary_market_id}, "
            f"ltp_count={len(ltp_list)}"
        )

        if primary_market_id is None:
            print("[MarketWS] No usable market id found, skipping market")
            continue

        for item in ltp_list:
            runner_id = item.get("ri")
            ltp = item.get("ltp")
            tv = item.get("tv")

            print(f"[MarketWS] runner_id={runner_id}, ltp={ltp}, tv={tv}")

            if runner_id is None or ltp is None:
                print("[MarketWS] Invalid runner row, skipping")
                continue

            extra_data = {
                "bmi": bmi,
                "mi": mi,
                "source": "market_ws",
            }

            # Save using BMI as primary key
            save_latest_price(
                market_id=primary_market_id,
                runner_id=runner_id,
                ltp=ltp,
                tv=tv,
                extra_data=extra_data,
            )

            # Optional compatibility save using MI also
            # This helps if any old code still reads using mi
            if mi is not None and str(mi) != str(primary_market_id):
                save_latest_price(
                    market_id=mi,
                    runner_id=runner_id,
                    ltp=ltp,
                    tv=tv,
                    extra_data=extra_data,
                )


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

    def build_urls(self):
        return [WS_URL_TEMPLATE.format(host=host, token=self.token) for host in WS_HOSTS]

    def run(self):
        urls = self.build_urls()
        print("[MarketWS] MARKET IDS:", self.market_ids)

        last_error = None
        for url in urls:
            print("[MarketWS] CONNECTING URL:", url)
            self.ws = websocket.WebSocketApp(
                url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_pong=self.on_pong,
            )

            try:
                self.ws.run_forever(
                    sslopt={"check_hostname": False},
                    ping_interval=20,
                    ping_timeout=10,
                )
                return
            except Exception as e:
                last_error = e
                print(f"[MarketWS] CONNECT ERROR TYPE: {type(e).__name__}")
                print(f"[MarketWS] CONNECT ERROR DETAIL: {e}")
                print("[MarketWS] Trying next endpoint if available...")

        if last_error:
            raise last_error