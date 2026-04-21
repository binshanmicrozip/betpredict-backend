import asyncio
import json
import time
from decimal import Decimal, InvalidOperation

import redis
import websockets
from django.conf import settings
from django.utils import timezone

from .models import Market, PriceTick, Runner, LiveMatchState

r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)

# keep 443 first because 8881 is timing out on your machine
WS_HOSTS = [
    "socket.myzosh.com:443",
    "socket.myzosh.com:8881",
]

WS_URL_TEMPLATE = "wss://{host}/?token={token}"
NO_MARKET_DATA_FAILOVER_SECONDS = 20


def _to_decimal(value):
    if value in (None, "", "null"):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_int(value):
    if value in (None, "", "null"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class MarketWebSocketClient:
    def __init__(
        self,
        token_or_agent: str,
        subscribe_markets: list[str] | None = None,
        save_db_without_cricket: bool = False,
        token_mode: str = "auto",   # auto | raw | agent
    ):
        self.token_or_agent = token_or_agent.strip()
        self.subscribe_markets_input = subscribe_markets or []
        self.save_db_without_cricket = save_db_without_cricket
        self.token_mode = token_mode

        self.target_market_ids = [
            str(x).strip()
            for x in self.subscribe_markets_input
            if str(x).strip()
        ]

        self.subscribe_markets = self.target_market_ids.copy()

        print(f"[MarketWS] TARGET MARKET IDS: {self.target_market_ids}")
        print(f"[MarketWS] FINAL SUBSCRIBE MARKETS: {self.subscribe_markets}")
        print(f"[MarketWS] TOKEN MODE: {self.token_mode}")

        self.ws = None
        self.market_cache: dict[str, Market | None] = {}
        self.runner_cache: dict[tuple[str, int], Runner | None] = {}
        self.saved_tick_count = 0
        self.skipped_tick_count = 0
        self.last_non_heartbeat_ts = None
        self.last_saved_market_ts = None

    def resolve_token(self):
        if self.token_mode == "raw":
            return self.token_or_agent

        if self.token_mode == "agent":
            return f"{self.token_or_agent}-{int(time.time() * 1000)}"

        # auto mode
        # if user passes something that already looks like a token, use it as-is
        if "-" in self.token_or_agent and len(self.token_or_agent) > 10:
            return self.token_or_agent

        return f"{self.token_or_agent}-{int(time.time() * 1000)}"

    def build_urls(self):
        token = self.resolve_token()
        print(f"[MarketWS] RESOLVED TOKEN: {token}")
        return [WS_URL_TEMPLATE.format(host=host, token=token) for host in WS_HOSTS]

    async def heartbeat(self):
        while True:
            await asyncio.sleep(10)
            try:
                if self.ws is not None:
                    await self.ws.send(json.dumps({
                        "action": "heartbeat",
                        "data": []
                    }))
                    print("[MarketWS] HEARTBEAT sent")
            except Exception as e:
                print(f"[MarketWS] HEARTBEAT error: {e}")
                break

    def _get_market(self, market_id: str) -> Market | None:
        if market_id in self.market_cache:
            return self.market_cache[market_id]

        market = Market.objects.filter(market_id=market_id).first()
        self.market_cache[market_id] = market
        return market

    def _get_or_create_market(
        self,
        market_id: str,
        event_id: str | None = None,
        event_type_id: str | None = None,
        status: int | None = None,
        traded_volume: Decimal | None = None,
    ) -> Market | None:
        if market_id in self.market_cache:
            return self.market_cache[market_id]

        market, created = Market.objects.get_or_create(
            market_id=market_id,
            defaults={
                "event_id": event_id or market_id,
                "event_name": f"Event {market_id}",
                "event_type_id": event_type_id or "4",
                "market_name": f"Market {market_id}",
                "market_type": "MATCH_ODDS",
                "market_start_time": timezone.now(),
                "status": "OPEN" if status != 0 else "CLOSED",
                "country_code": None,
                "timezone": "UTC",
            }
        )

        self.market_cache[market_id] = market
        return market

    def _get_or_create_runner(self, market: Market, selection_id: int) -> Runner | None:
        key = (market.market_id, selection_id)
        if key in self.runner_cache:
            return self.runner_cache[key]

        runner, _ = Runner.objects.get_or_create(
            market=market,
            selection_id=selection_id,
            defaults={
                "runner_name": f"Runner {selection_id}",
                "status": "ACTIVE",
            }
        )
        self.runner_cache[key] = runner
        return runner

    def _get_runner(self, market: Market, selection_id: int) -> Runner | None:
        key = (market.market_id, selection_id)
        if key in self.runner_cache:
            return self.runner_cache[key]

        runner = Runner.objects.filter(market=market, selection_id=selection_id).first()
        self.runner_cache[key] = runner
        return runner

    def _has_live_cricket_data(self, market_id: str) -> bool:
        from pathlib import Path

        recent_cutoff = timezone.now() - timezone.timedelta(minutes=5)
        recent_live_matches = LiveMatchState.objects.filter(fetched_at__gte=recent_cutoff).exists()
        if recent_live_matches:
            return True

        csv_dir = Path("live_match_data")
        if csv_dir.exists():
            current_time = timezone.now()
            for csv_file in csv_dir.glob("live_match_*.csv"):
                if csv_file.stat().st_mtime > (current_time - timezone.timedelta(minutes=5)).timestamp():
                    return True
        return False

    def _save_price_tick_to_db(self, market_id: str, selection_id: int, ltp: Decimal, traded_volume: Decimal | None, source: str):
        if not self.save_db_without_cricket and not self._has_live_cricket_data(market_id):
            self.skipped_tick_count += 1
            print(f"[MarketWS] SKIPPED DB SAVE - No live cricket data for market: {market_id}")
            return

        market = self._get_market(market_id)
        if not market:
            self.skipped_tick_count += 1
            print(f"[MarketWS] DB market not found: {market_id}")
            return

        runner = self._get_runner(market, selection_id)
        if not runner:
            self.skipped_tick_count += 1
            print(f"[MarketWS] DB runner not found: market={market_id}, selection_id={selection_id}")
            return

        tick_time = timezone.now()
        tick = PriceTick(
            market=market,
            runner=runner,
            year=tick_time.year,
            month=tick_time.month,
            day=tick_time.day,
            snapshot=source,
            tick_time=tick_time,
            ltp=ltp,
            win_prob=None,
            traded_volume=traded_volume,
            phase=None,
        )

        try:
            tick.save()
            self.saved_tick_count += 1
            print(f"[MarketWS] SAVED TO DB => market={market_id}, runner={selection_id}, ltp={ltp}, tv={traded_volume}")
        except Exception as e:
            self.skipped_tick_count += 1
            print(f"[MarketWS] DB save error: {e}")

    def save_latest_price_to_redis(self, market_id: str, runner_id: int | str, ltp: Decimal, tv: Decimal | None, extra_data: dict | None = None):
        key = f"price:{market_id}:{runner_id}"
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
            "source": "market_ws_async",
        }

        if extra_data:
            for k, v in extra_data.items():
                payload[str(k)] = "" if v is None else str(v)

        r.hset(key, mapping=payload)
        r.expire(key, 3600)
        self.last_saved_market_ts = time.time()
        print(f"[MarketWS] SAVED TO REDIS => {key} => {payload}")

    async def save_price_tick_to_db(self, market_id: str, selection_id: int, ltp: Decimal, traded_volume: Decimal | None, source: str):
        await asyncio.to_thread(
            self._save_price_tick_to_db,
            market_id,
            selection_id,
            ltp,
            traded_volume,
            source,
        )

    async def process_market_message(self, raw_message: str):
        try:
            payload = json.loads(raw_message)
            print("paylllllllllll:",payload)
        except Exception as e:
            print(f"[MarketWS] JSON parse error: {e}")
            print(raw_message)
            return

        message_type = str(payload.get("messageType") or "").strip().lower()
        print(f"[MarketWS] MESSAGE TYPE: {message_type}")

        if message_type == "heartbeat":
            print("[MarketWS] Ignoring heartbeat")
            return

        self.last_non_heartbeat_ts = time.time()

        data_items = payload.get("data", [])
        if isinstance(data_items, dict):
            data_items = [data_items]
        elif not isinstance(data_items, list):
            print(f"[MarketWS] Unsupported data type: {type(data_items).__name__}")
            print(f"[MarketWS] RAW PAYLOAD: {payload}")
            return

        for item in data_items:
            mi = str(item.get("mi") or "").strip()
            bmi = str(item.get("bmi") or "").strip()
            eid = str(item.get("eid") or "").strip()
            eti = str(item.get("eti") or "4").strip()
            ms = _to_int(item.get("ms"))
            tdv = _to_decimal(item.get("tdv", 0))

            ltp_items = item.get("ltp", []) or []
            rt_items = item.get("rt", []) or []

            market_id = bmi if bmi else mi

            print(f"[MarketWS] MARKET ITEM => mi={mi}, bmi={bmi}, market_id={market_id}, eid={eid}, eti={eti}, status={ms}")
            print(f"[MarketWS] LTP COUNT => {len(ltp_items)}")
            print(f"[MarketWS] RT COUNT => {len(rt_items)}")

            if not market_id:
                continue

            if self.target_market_ids and market_id not in self.target_market_ids:
                print(f"[MarketWS] Skipping market_id={market_id} because it is not in target ids")
                continue

            market = await asyncio.to_thread(
                self._get_or_create_market,
                market_id,
                eid,
                eti,
                ms,
                tdv,
            )
            if not market:
                continue

            processed_runner_ids = set()

            for ltp_item in ltp_items:
                runner_id = _to_int(ltp_item.get("ri"))
                ltp_value = _to_decimal(ltp_item.get("ltp"))
                tv_value = _to_decimal(ltp_item.get("tv", tdv or 0))

                if runner_id is None or ltp_value is None:
                    continue

                runner = await asyncio.to_thread(
                    self._get_or_create_runner,
                    market,
                    runner_id,
                )
                if not runner:
                    continue

                self.save_latest_price_to_redis(
                    market_id=market_id,
                    runner_id=runner_id,
                    ltp=ltp_value,
                    tv=tv_value,
                    extra_data={
                        "mi": mi,
                        "bmi": bmi,
                        "eid": eid,
                        "eti": eti,
                        "market_status": ms,
                        "tdv": tdv,
                        "message_type": message_type,
                        "source": "market_ws_async",
                    },
                )

                await self.save_price_tick_to_db(
                    market_id=market_id,
                    selection_id=runner_id,
                    ltp=ltp_value,
                    traded_volume=tv_value,
                    source="market_ws_async",
                )

                processed_runner_ids.add(runner_id)

            for rt_item in rt_items:
                runner_id = _to_int(rt_item.get("ri"))
                if runner_id is None or runner_id in processed_runner_ids:
                    continue

                ltp_value = _to_decimal(rt_item.get("rt"))
                tv_value = _to_decimal(rt_item.get("tv", tdv or 0))
                if ltp_value is None:
                    continue

                runner = await asyncio.to_thread(
                    self._get_or_create_runner,
                    market,
                    runner_id,
                )
                if not runner:
                    continue

                self.save_latest_price_to_redis(
                    market_id=market_id,
                    runner_id=runner_id,
                    ltp=ltp_value,
                    tv=tv_value,
                    extra_data={
                        "mi": mi,
                        "bmi": bmi,
                        "eid": eid,
                        "eti": eti,
                        "market_status": ms,
                        "tdv": tdv,
                        "message_type": message_type,
                        "source": "market_ws_async_rt_fallback",
                    },
                )

                await self.save_price_tick_to_db(
                    market_id=market_id,
                    selection_id=runner_id,
                    ltp=ltp_value,
                    traded_volume=tv_value,
                    source="market_ws_async_rt_fallback",
                )

    async def _receive_loop(self, ws, subscribe_payload):
        no_data_start = time.time()

        while True:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=5)
                print(f"[MarketWS] RAW MESSAGE: {message}")
                await self.process_market_message(message)

                if self.last_saved_market_ts:
                    no_data_start = time.time()

            except asyncio.TimeoutError:
                elapsed = int(time.time() - no_data_start)
                print(f"[MarketWS] No market data received for {elapsed}s after subscribe: {subscribe_payload}")

                if self.last_saved_market_ts:
                    age = int(time.time() - self.last_saved_market_ts)
                    print(f"[MarketWS] Last Redis save was {age}s ago")
                else:
                    print("[MarketWS] No Redis save has happened yet in this session")

                if elapsed >= NO_MARKET_DATA_FAILOVER_SECONDS:
                    print(
                        f"[MarketWS] No market packets yet for {elapsed}s. "
                        f"Keeping connection open and waiting..."
                    )
                    no_data_start = time.time()
    async def connect_once(self):
        urls = self.build_urls()
        print(f"[MarketWS] FINAL SUBSCRIBE MARKETS: {self.subscribe_markets}")

        last_error = None
        for url in urls:
            print(f"[MarketWS] CONNECTING: {url}")
            self.last_saved_market_ts = None

            try:
                async with websockets.connect(
                    url,
                    open_timeout=30,
                    ping_interval=None,
                    close_timeout=10,
                ) as ws:
                    self.ws = ws
                    print("[MarketWS] CONNECTED")

                    asyncio.create_task(self.heartbeat())

                    subscribe_payload = {"action": "set"}

                if self.subscribe_markets:
                    subscribe_payload["markets"] = ",".join(self.subscribe_markets)
                    print(f"[MarketWS] SENDING FILTERED SUBSCRIBE: {subscribe_payload}")
                else:
                    print("[MarketWS] SENDING GLOBAL SUBSCRIBE FOR ALL AVAILABLE MARKETS")
                    print(f"[MarketWS] SENDING FILTERED SUBSCRIBE: {subscribe_payload}")
                    await ws.send(json.dumps(subscribe_payload))
                    print(f"[MarketWS] SUBSCRIBED: {subscribe_payload}")

                    try:
                        first_message = await asyncio.wait_for(ws.recv(), timeout=5)
                        print(f"[MarketWS] FIRST MESSAGE AFTER SUBSCRIBE: {first_message}")
                        await self.process_market_message(first_message)
                    except asyncio.TimeoutError:
                        print("[MarketWS] No immediate message after subscribe")

                    await self._receive_loop(ws, subscribe_payload)
                    return

            except Exception as e:
                last_error = e
                print(f"[MarketWS] ENDPOINT FAILED: {url}")
                print(f"[MarketWS] CONNECT ERROR TYPE: {type(e).__name__}")
                print(f"[MarketWS] CONNECT ERROR DETAIL: {e}")
                print("[MarketWS] Trying next endpoint if available...")

        if last_error:
            raise last_error

    async def run_forever(self):
        while True:
            try:
                await self.connect_once()
            except Exception as e:
                print(f"[MarketWS] ERROR: {e}")
                print("[MarketWS] Reconnecting in 5 seconds...")
                await asyncio.sleep(5)