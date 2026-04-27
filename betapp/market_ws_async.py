
# import asyncio
# import json
# import re
# import time
# from decimal import Decimal, InvalidOperation
# from datetime import datetime, timezone as dt_timezone

# import redis
# import websockets
# from django.conf import settings
# from django.utils import timezone

# from .models import Market, PriceTick, Runner, LiveMatchState

# r = redis.Redis(
#     host=settings.REDIS_HOST,
#     port=settings.REDIS_PORT,
#     db=settings.REDIS_DB,
#     decode_responses=True,
# )

# WS_HOSTS = [
#     "socket.myzosh.com:443",
#     "socket.myzosh.com:8881",
# ]

# WS_URL_TEMPLATE = "wss://{host}/?token={token}"
# NO_MARKET_DATA_FAILOVER_SECONDS = 20


# def _to_decimal(value):
#     if value in (None, "", "null"):
#         return None
#     try:
#         return Decimal(str(value))
#     except (InvalidOperation, TypeError, ValueError):
#         return None


# def _to_int(value):
#     if value in (None, "", "null"):
#         return None
#     try:
#         return int(value)
#     except (TypeError, ValueError):
#         return None


# def _parse_dotnet_date(value):
#     if not value:
#         return None

#     text = str(value).strip()
#     match = re.search(r"/Date\((\d+)\)/", text)
#     if not match:
#         return None

#     try:
#         ms = int(match.group(1))
#         return datetime.fromtimestamp(ms / 1000, tz=dt_timezone.utc)
#     except Exception:
#         return None


# class MarketWebSocketClient:
#     def __init__(
#         self,
#         token_or_agent: str,
#         subscribe_markets: list[str] | None = None,
#         market_catalog: list[dict] | None = None,
#         save_db_without_cricket: bool = False,
#         token_mode: str = "auto",
#     ):
#         self.token_or_agent = (token_or_agent or "").strip()
#         self.save_db_without_cricket = save_db_without_cricket
#         self.token_mode = token_mode

#         self.market_catalog = market_catalog or []
#         self.market_metadata_by_id = {}

#         for item in self.market_catalog:
#             market_id = str(item.get("market_id") or "").strip()
#             if market_id:
#                 self.market_metadata_by_id[market_id] = item

#         if subscribe_markets:
#             self.subscribe_markets = [
#                 str(x).strip()
#                 for x in subscribe_markets
#                 if str(x).strip()
#             ]
#         else:
#             self.subscribe_markets = list(self.market_metadata_by_id.keys())

#         print(f"[MarketWS] FINAL SUBSCRIBE MARKETS COUNT: {len(self.subscribe_markets)}")
#         print(f"[MarketWS] TOKEN MODE: {self.token_mode}")

#         self.ws = None
#         self.market_cache: dict[str, Market | None] = {}
#         self.runner_cache: dict[tuple[str, int], Runner | None] = {}
#         self.saved_tick_count = 0
#         self.skipped_tick_count = 0
#         self.last_non_heartbeat_ts = None
#         self.last_saved_market_ts = None

#         self._seed_market_catalog()

#     def resolve_token(self):
#         if self.token_mode == "raw":
#             return self.token_or_agent

#         if self.token_mode == "agent":
#             return f"{self.token_or_agent}-{int(time.time() * 1000)}"

#         if "-" in self.token_or_agent and len(self.token_or_agent) > 10:
#             return self.token_or_agent

#         return f"{self.token_or_agent}-{int(time.time() * 1000)}"

#     def build_urls(self):
#         token = self.resolve_token()
#         print(f"[MarketWS] RESOLVED TOKEN: {token}")
#         return [WS_URL_TEMPLATE.format(host=host, token=token) for host in WS_HOSTS]

#     async def heartbeat(self):
#         while True:
#             await asyncio.sleep(10)
#             try:
#                 if self.ws is not None:
#                     await self.ws.send(json.dumps({
#                         "action": "heartbeat",
#                         "data": []
#                     }))
#                     print("[MarketWS] HEARTBEAT sent")
#             except Exception as e:
#                 print(f"[MarketWS] HEARTBEAT error: {e}")
#                 break

#     def _seed_market_catalog(self):
#         if not self.market_catalog:
#             print("[MarketWS] No market catalog provided for seeding")
#             return

#         print(f"[MarketWS] Seeding DB metadata for {len(self.market_catalog)} markets")

#         for item in self.market_catalog:
#             market_id = str(item.get("market_id") or "").strip()
#             if not market_id:
#                 continue

#             market = self._get_or_create_market(
#                 market_id=market_id,
#                 event_id=item.get("event_id"),
#                 event_type_id=item.get("sport_id"),
#                 status=1,
#                 traded_volume=None,
#                 metadata=item,
#             )
#             if not market:
#                 continue

#             for runner_data in item.get("runners", []):
#                 selection_id = _to_int(runner_data.get("selection_id"))
#                 if selection_id is None:
#                     continue

#                 self._get_or_create_runner(
#                     market=market,
#                     selection_id=selection_id,
#                     runner_data=runner_data,
#                 )

#     def _get_market(self, market_id: str) -> Market | None:
#         if market_id in self.market_cache:
#             return self.market_cache[market_id]

#         market = Market.objects.filter(market_id=market_id).first()
#         self.market_cache[market_id] = market
#         return market

#     def _get_or_create_market(
#         self,
#         market_id: str,
#         event_id: str | None = None,
#         event_type_id: str | None = None,
#         status: int | None = None,
#         traded_volume: Decimal | None = None,
#         metadata: dict | None = None,
#     ) -> Market | None:
#         if metadata is None:
#             metadata = self.market_metadata_by_id.get(str(market_id), {})

#         existing = self._get_market(market_id)
#         market_start_time = _parse_dotnet_date(metadata.get("market_time_raw")) or timezone.now()
#         suspend_time = _parse_dotnet_date(metadata.get("suspend_time_raw"))

#         defaults = {
#             "event_id": str(metadata.get("event_id") or event_id or market_id),
#             "event_name": metadata.get("event_name") or f"Event {market_id}",
#             "market_name": metadata.get("market_name") or f"Market {market_id}",
#             "market_type": metadata.get("market_type") or "MATCH_ODDS",
#             "event_type_id": str(metadata.get("sport_id") or event_type_id or "4"),
#             "country_code": metadata.get("country_code"),
#             "timezone": metadata.get("timezone") or "UTC",
#             "market_start_time": market_start_time,
#             "suspend_time": suspend_time,
#             "status": "OPEN" if status != 0 else "CLOSED",
#             "turn_in_play_enabled": bool(metadata.get("is_turn_in_play_enabled")),
#             "persistence_enabled": bool(metadata.get("is_persistence_enabled")),
#             "bsp_market": bool(metadata.get("is_bsp_market")),
#             "market_base_rate": _to_decimal(metadata.get("market_base_rate")),
#             "regulators": metadata.get("regulator"),
#             "number_of_active_runners": len(metadata.get("runners") or []),
#         }

#         if existing:
#             changed = False
#             for field, value in defaults.items():
#                 current = getattr(existing, field, None)
#                 if (current in (None, "", 0) and value not in (None, "")) or field in {
#                     "event_name", "market_name", "market_type", "country_code", "timezone",
#                     "turn_in_play_enabled", "persistence_enabled", "bsp_market",
#                     "number_of_active_runners", "status"
#                 }:
#                     if current != value and value is not None:
#                         setattr(existing, field, value)
#                         changed = True

#             if changed:
#                 existing.save()
#             self.market_cache[market_id] = existing
#             return existing

#         market = Market.objects.create(
#             market_id=market_id,
#             **defaults,
#         )
#         self.market_cache[market_id] = market
#         return market

#     def _get_or_create_runner(
#         self,
#         market: Market,
#         selection_id: int,
#         runner_data: dict | None = None,
#     ) -> Runner | None:
#         key = (market.market_id, selection_id)
#         if key in self.runner_cache:
#             runner = self.runner_cache[key]
#             if runner and runner_data:
#                 proper_name = runner_data.get("runner_name")
#                 if proper_name and runner.runner_name != proper_name:
#                     runner.runner_name = proper_name
#                     runner.save()
#             return runner

#         runner = Runner.objects.filter(market=market, selection_id=selection_id).first()
#         if runner:
#             proper_name = (runner_data or {}).get("runner_name")
#             if proper_name and runner.runner_name != proper_name:
#                 runner.runner_name = proper_name
#                 runner.save()

#             self.runner_cache[key] = runner
#             return runner

#         runner_name = (runner_data or {}).get("runner_name") or f"Runner {selection_id}"
#         runner = Runner.objects.create(
#             market=market,
#             selection_id=selection_id,
#             runner_name=runner_name,
#             status="ACTIVE",
#         )
#         self.runner_cache[key] = runner
#         return runner

#     def _has_live_cricket_data(self, market_id: str) -> bool:
#         recent_cutoff = timezone.now() - timezone.timedelta(minutes=5)
#         recent_live_matches = LiveMatchState.objects.filter(fetched_at__gte=recent_cutoff).exists()
#         return recent_live_matches

#     def _save_price_tick_to_db(self, market_id: str, selection_id: int, ltp: Decimal, traded_volume: Decimal | None, source: str):
#         if not self.save_db_without_cricket and not self._has_live_cricket_data(market_id):
#             self.skipped_tick_count += 1
#             print(f"[MarketWS] SKIPPED DB SAVE - No live cricket data for market: {market_id}")
#             return

#         market = self._get_market(market_id)
#         if not market:
#             self.skipped_tick_count += 1
#             print(f"[MarketWS] DB market not found: {market_id}")
#             return

#         runner = self._get_or_create_runner(market, selection_id)
#         if not runner:
#             self.skipped_tick_count += 1
#             print(f"[MarketWS] DB runner not found: market={market_id}, selection_id={selection_id}")
#             return

#         tick_time = timezone.now()
#         tick = PriceTick(
#             market=market,
#             runner=runner,
#             year=tick_time.year,
#             month=tick_time.month,
#             day=tick_time.day,
#             snapshot=source,
#             tick_time=tick_time,
#             ltp=ltp,
#             win_prob=None,
#             traded_volume=traded_volume,
#             phase=None,
#         )

#         try:
#             tick.save()
#             self.saved_tick_count += 1
#             print(f"[MarketWS] SAVED TO DB => market={market_id}, runner={selection_id}, ltp={ltp}, tv={traded_volume}")
#         except Exception as e:
#             self.skipped_tick_count += 1
#             print(f"[MarketWS] DB save error: {e}")

#     def save_latest_price_to_redis(self, market_id: str, runner_id: int | str, ltp: Decimal, tv: Decimal | None, extra_data: dict | None = None):
#         key = f"price:{market_id}:{runner_id}"
#         old = r.hgetall(key)
#         prev_ltp = old.get("ltp")
#         if prev_ltp is None:
#             prev_ltp = ltp

#         payload = {
#             "market_id": str(market_id),
#             "runner_id": str(runner_id),
#             "ltp": str(ltp),
#             "prev_ltp": str(prev_ltp),
#             "tv": str(tv or 0),
#             "updated_at": str(time.time()),
#             "source": "market_ws_async",
#         }

#         if extra_data:
#             for k, v in extra_data.items():
#                 payload[str(k)] = "" if v is None else str(v)

#         r.hset(key, mapping=payload)
#         r.expire(key, 3600)
#         self.last_saved_market_ts = time.time()
#         print(f"[MarketWS] SAVED TO REDIS => {key} => {payload}")

#     async def save_price_tick_to_db(self, market_id: str, selection_id: int, ltp: Decimal, traded_volume: Decimal | None, source: str):
#         await asyncio.to_thread(
#             self._save_price_tick_to_db,
#             market_id,
#             selection_id,
#             ltp,
#             traded_volume,
#             source,
#         )

#     async def process_market_message(self, raw_message: str):
#         try:
#             payload = json.loads(raw_message)
#         except Exception as e:
#             print(f"[MarketWS] JSON parse error: {e}")
#             print(raw_message)
#             return

#         message_type = str(payload.get("messageType") or "").strip().lower()
#         print(f"[MarketWS] MESSAGE TYPE: {message_type}")

#         if message_type == "heartbeat":
#             print("[MarketWS] Ignoring heartbeat")
#             return

#         self.last_non_heartbeat_ts = time.time()

#         data_items = payload.get("data", [])
#         if isinstance(data_items, dict):
#             data_items = [data_items]
#         elif not isinstance(data_items, list):
#             print(f"[MarketWS] Unsupported data type: {type(data_items).__name__}")
#             print(f"[MarketWS] RAW PAYLOAD: {payload}")
#             return

#         for item in data_items:
#             mi = str(item.get("mi") or "").strip()
#             bmi = str(item.get("bmi") or "").strip()
#             eid = str(item.get("eid") or "").strip()
#             eti = str(item.get("eti") or "4").strip()
#             ms = _to_int(item.get("ms"))
#             tdv = _to_decimal(item.get("tdv", 0))

#             ltp_items = item.get("ltp", []) or []
#             rt_items = item.get("rt", []) or []

#             market_id = bmi if bmi else mi
#             metadata = self.market_metadata_by_id.get(market_id, {})

#             print(f"[MarketWS] MARKET ITEM => mi={mi}, bmi={bmi}, market_id={market_id}, eid={eid}, eti={eti}, status={ms}")
#             print(f"[MarketWS] LTP COUNT => {len(ltp_items)}")
#             print(f"[MarketWS] RT COUNT => {len(rt_items)}")

#             if not market_id:
#                 continue

#             market = await asyncio.to_thread(
#                 self._get_or_create_market,
#                 market_id,
#                 eid,
#                 eti,
#                 ms,
#                 tdv,
#                 metadata,
#             )
#             if not market:
#                 continue

#             runner_map = {
#                 str(r.get("selection_id")): r
#                 for r in metadata.get("runners", [])
#                 if str(r.get("selection_id", "")).strip()
#             }

#             processed_runner_ids = set()

#             for ltp_item in ltp_items:
#                 runner_id = _to_int(ltp_item.get("ri"))
#                 ltp_value = _to_decimal(ltp_item.get("ltp"))
#                 tv_value = _to_decimal(ltp_item.get("tv", tdv or 0))

#                 if runner_id is None or ltp_value is None:
#                     continue

#                 runner_data = runner_map.get(str(runner_id), {})
#                 runner = await asyncio.to_thread(
#                     self._get_or_create_runner,
#                     market,
#                     runner_id,
#                     runner_data,
#                 )
#                 if not runner:
#                     continue

#                 self.save_latest_price_to_redis(
#                     market_id=market_id,
#                     runner_id=runner_id,
#                     ltp=ltp_value,
#                     tv=tv_value,
#                     extra_data={
#                         "mi": mi,
#                         "bmi": bmi,
#                         "eid": eid,
#                         "eti": eti,
#                         "market_status": ms,
#                         "tdv": tdv,
#                         "message_type": message_type,
#                         "source": "market_ws_async",
#                         "event_name": metadata.get("event_name"),
#                         "sport_id": metadata.get("sport_id"),
#                         "sport_name": metadata.get("sport_name"),
#                         "tournament_id": metadata.get("tournament_id"),
#                         "tournament_name": metadata.get("tournament_name"),
#                         "market_name": metadata.get("market_name"),
#                         "market_type": metadata.get("market_type"),
#                         "runner_name": runner.runner_name,
#                     },
#                 )

#                 await self.save_price_tick_to_db(
#                     market_id=market_id,
#                     selection_id=runner_id,
#                     ltp=ltp_value,
#                     traded_volume=tv_value,
#                     source="market_ws_async",
#                 )

#                 processed_runner_ids.add(runner_id)

#             for rt_item in rt_items:
#                 runner_id = _to_int(rt_item.get("ri"))
#                 if runner_id is None or runner_id in processed_runner_ids:
#                     continue

#                 ltp_value = _to_decimal(rt_item.get("rt"))
#                 tv_value = _to_decimal(rt_item.get("tv", tdv or 0))
#                 if ltp_value is None:
#                     continue

#                 runner_data = runner_map.get(str(runner_id), {})
#                 runner = await asyncio.to_thread(
#                     self._get_or_create_runner,
#                     market,
#                     runner_id,
#                     runner_data,
#                 )
#                 if not runner:
#                     continue

#                 self.save_latest_price_to_redis(
#                     market_id=market_id,
#                     runner_id=runner_id,
#                     ltp=ltp_value,
#                     tv=tv_value,
#                     extra_data={
#                         "mi": mi,
#                         "bmi": bmi,
#                         "eid": eid,
#                         "eti": eti,
#                         "market_status": ms,
#                         "tdv": tdv,
#                         "message_type": message_type,
#                         "source": "market_ws_async_rt_fallback",
#                         "event_name": metadata.get("event_name"),
#                         "sport_id": metadata.get("sport_id"),
#                         "sport_name": metadata.get("sport_name"),
#                         "tournament_id": metadata.get("tournament_id"),
#                         "tournament_name": metadata.get("tournament_name"),
#                         "market_name": metadata.get("market_name"),
#                         "market_type": metadata.get("market_type"),
#                         "runner_name": runner.runner_name,
#                     },
#                 )

#                 await self.save_price_tick_to_db(
#                     market_id=market_id,
#                     selection_id=runner_id,
#                     ltp=ltp_value,
#                     traded_volume=tv_value,
#                     source="market_ws_async_rt_fallback",
#                 )

#     async def _receive_loop(self, ws, subscribe_payload):
#         no_data_start = time.time()

#         while True:
#             try:
#                 message = await asyncio.wait_for(ws.recv(), timeout=5)
#                 print(f"[MarketWS] RAW MESSAGE: {message}")
#                 await self.process_market_message(message)

#                 if self.last_saved_market_ts:
#                     no_data_start = time.time()

#             except asyncio.TimeoutError:
#                 elapsed = int(time.time() - no_data_start)
#                 print(f"[MarketWS] No market data received for {elapsed}s after subscribe: {subscribe_payload}")

#                 if self.last_saved_market_ts:
#                     age = int(time.time() - self.last_saved_market_ts)
#                     print(f"[MarketWS] Last Redis save was {age}s ago")
#                 else:
#                     print("[MarketWS] No Redis save has happened yet in this session")

#                 if elapsed >= NO_MARKET_DATA_FAILOVER_SECONDS:
#                     print(f"[MarketWS] No market packets yet for {elapsed}s. Keeping connection open and waiting...")
#                     no_data_start = time.time()

#     async def connect_once(self):
#         urls = self.build_urls()
#         print(f"[MarketWS] FINAL SUBSCRIBE MARKETS COUNT: {len(self.subscribe_markets)}")

#         if not self.subscribe_markets:
#             raise Exception("No market ids available for subscription")

#         last_error = None
#         for url in urls:
#             print(f"[MarketWS] CONNECTING: {url}")
#             self.last_saved_market_ts = None

#             try:
#                 async with websockets.connect(
#                     url,
#                     open_timeout=30,
#                     ping_interval=None,
#                     close_timeout=10,
#                 ) as ws:
#                     self.ws = ws
#                     print("[MarketWS] CONNECTED")

#                     asyncio.create_task(self.heartbeat())

#                     subscribe_payload = {
#                         "action": "set",
#                         "markets": ",".join(self.subscribe_markets),
#                     }

#                     print(f"[MarketWS] SENDING SUBSCRIBE WITH {len(self.subscribe_markets)} MARKET IDS")
#                     await ws.send(json.dumps(subscribe_payload))
#                     print("[MarketWS] SUBSCRIBED SUCCESSFULLY")

#                     try:
#                         first_message = await asyncio.wait_for(ws.recv(), timeout=5)
#                         print(f"[MarketWS] FIRST MESSAGE AFTER SUBSCRIBE: {first_message}")
#                         await self.process_market_message(first_message)
#                     except asyncio.TimeoutError:
#                         print("[MarketWS] No immediate message after subscribe")

#                     await self._receive_loop(ws, subscribe_payload)
#                     return

#             except Exception as e:
#                 last_error = e
#                 print(f"[MarketWS] ENDPOINT FAILED: {url}")
#                 print(f"[MarketWS] CONNECT ERROR TYPE: {type(e).__name__}")
#                 print(f"[MarketWS] CONNECT ERROR DETAIL: {e}")
#                 print("[MarketWS] Trying next endpoint if available...")

#         if last_error:
#             raise last_error

#     async def run_forever(self):
#         while True:
#             try:
#                 await self.connect_once()
#             except Exception as e:
#                 print(f"[MarketWS] ERROR: {e}")
#                 print("[MarketWS] Reconnecting in 5 seconds...")
#                 await asyncio.sleep(5)


import asyncio
import json
import re
import time
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone as dt_timezone

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


def _parse_dotnet_date(value):
    if not value:
        return None

    text = str(value).strip()
    match = re.search(r"/Date\((\d+)\)/", text)
    if not match:
        return None

    try:
        ms = int(match.group(1))
        return datetime.fromtimestamp(ms / 1000, tz=dt_timezone.utc)
    except Exception:
        return None


class MarketWebSocketClient:
    def __init__(
        self,
        token_or_agent: str,
        subscribe_markets: list[str] | None = None,
        market_catalog: list[dict] | None = None,
        save_db_without_cricket: bool = False,
        token_mode: str = "auto",
    ):
        self.token_or_agent = (token_or_agent or "").strip()
        self.save_db_without_cricket = save_db_without_cricket
        self.token_mode = token_mode

        self.market_catalog = market_catalog or []
        self.market_metadata_by_id = {}

        for item in self.market_catalog:
            market_id = str(item.get("market_id") or "").strip()
            if market_id:
                self.market_metadata_by_id[market_id] = item

        if subscribe_markets:
            self.subscribe_markets = [
                str(x).strip()
                for x in subscribe_markets
                if str(x).strip()
            ]
        else:
            self.subscribe_markets = list(self.market_metadata_by_id.keys())

        print(f"[MarketWS] FINAL SUBSCRIBE MARKETS COUNT: {len(self.subscribe_markets)}")
        print(f"[MarketWS] TOKEN MODE: {self.token_mode}")

        self.ws = None
        self.market_cache: dict[str, Market | None] = {}
        self.runner_cache: dict[tuple[str, int], Runner | None] = {}
        self.saved_tick_count = 0
        self.skipped_tick_count = 0
        self.last_non_heartbeat_ts = None
        self.last_saved_market_ts = None

        self._seed_market_catalog()

    def resolve_token(self):
        if self.token_mode == "raw":
            return self.token_or_agent

        if self.token_mode == "agent":
            return f"{self.token_or_agent}-{int(time.time() * 1000)}"

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

    def _seed_market_catalog(self):
        if not self.market_catalog:
            print("[MarketWS] No market catalog provided for seeding")
            return

        print(f"[MarketWS] Seeding DB metadata for {len(self.market_catalog)} markets")

        for item in self.market_catalog:
            market_id = str(item.get("market_id") or "").strip()
            if not market_id:
                continue

            market = self._get_or_create_market(
                market_id=market_id,
                event_id=item.get("event_id"),
                event_type_id=item.get("sport_id"),
                status=1,
                traded_volume=None,
                metadata=item,
            )
            if not market:
                continue

            for runner_data in item.get("runners", []):
                selection_id = _to_int(runner_data.get("selection_id"))
                if selection_id is None:
                    continue

                self._get_or_create_runner(
                    market=market,
                    selection_id=selection_id,
                    runner_data=runner_data,
                )

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
        metadata: dict | None = None,
    ) -> Market | None:
        if metadata is None:
            metadata = self.market_metadata_by_id.get(str(market_id), {})

        existing = self._get_market(market_id)
        market_start_time = _parse_dotnet_date(metadata.get("market_time_raw")) or timezone.now()
        suspend_time = _parse_dotnet_date(metadata.get("suspend_time_raw"))

        defaults = {
            "event_id": str(metadata.get("event_id") or event_id or market_id),
            "event_name": metadata.get("event_name") or f"Event {market_id}",
            "market_name": metadata.get("market_name") or f"Market {market_id}",
            "market_type": metadata.get("market_type") or "MATCH_ODDS",
            "event_type_id": str(metadata.get("sport_id") or event_type_id or "4"),
            "country_code": metadata.get("country_code"),
            "timezone": metadata.get("timezone") or "UTC",
            "market_start_time": market_start_time,
            "suspend_time": suspend_time,
            "status": "OPEN" if status != 0 else "CLOSED",
            "turn_in_play_enabled": bool(metadata.get("is_turn_in_play_enabled")),
            "persistence_enabled": bool(metadata.get("is_persistence_enabled")),
            "bsp_market": bool(metadata.get("is_bsp_market")),
            "market_base_rate": _to_decimal(metadata.get("market_base_rate")),
            "regulators": metadata.get("regulator"),
            "number_of_active_runners": len(metadata.get("runners") or []),
        }

        if existing:
            changed = False
            for field, value in defaults.items():
                current = getattr(existing, field, None)
                if (current in (None, "", 0) and value not in (None, "")) or field in {
                    "event_name", "market_name", "market_type", "country_code", "timezone",
                    "turn_in_play_enabled", "persistence_enabled", "bsp_market",
                    "number_of_active_runners", "status"
                }:
                    if current != value and value is not None:
                        setattr(existing, field, value)
                        changed = True

            if changed:
                existing.save()
            self.market_cache[market_id] = existing
            return existing

        market = Market.objects.create(
            market_id=market_id,
            **defaults,
        )
        self.market_cache[market_id] = market
        return market

    def _get_or_create_runner(
        self,
        market: Market,
        selection_id: int,
        runner_data: dict | None = None,
    ) -> Runner | None:
        key = (market.market_id, selection_id)
        if key in self.runner_cache:
            runner = self.runner_cache[key]
            if runner and runner_data:
                proper_name = runner_data.get("runner_name")
                if proper_name and runner.runner_name != proper_name:
                    runner.runner_name = proper_name
                    runner.save()
            return runner

        runner = Runner.objects.filter(market=market, selection_id=selection_id).first()
        if runner:
            proper_name = (runner_data or {}).get("runner_name") or ""
            if not proper_name or proper_name.startswith("Runner "):
                # Catalog had no name — try any other Runner record with a real name
                other = Runner.objects.filter(
                    selection_id=selection_id
                ).exclude(runner_name__startswith="Runner ").first()
                proper_name = other.runner_name if other else ""

            if proper_name and runner.runner_name != proper_name:
                runner.runner_name = proper_name
                runner.save()

            self.runner_cache[key] = runner
            return runner

        catalog_name = (runner_data or {}).get("runner_name") or ""
        if not catalog_name or catalog_name.startswith("Runner "):
            # Try to find a proper name from any other Runner record with the same selection_id
            other = Runner.objects.filter(
                selection_id=selection_id
            ).exclude(runner_name__startswith="Runner ").first()
            catalog_name = other.runner_name if other else f"Runner {selection_id}"

        runner = Runner.objects.create(
            market=market,
            selection_id=selection_id,
            runner_name=catalog_name,
            status="ACTIVE",
        )
        self.runner_cache[key] = runner
        return runner

    def _has_live_cricket_data(self, market_id: str) -> bool:
        recent_cutoff = timezone.now() - timezone.timedelta(minutes=5)
        recent_live_matches = LiveMatchState.objects.filter(fetched_at__gte=recent_cutoff).exists()
        return recent_live_matches

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

        runner = self._get_or_create_runner(market, selection_id)
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
            # "ip" field is the actual in-play flag (1=inplay); ms==2 is Betfair's inplay status
            ip = _to_int(item.get("ip", 0))
            in_play = (ms == 2) or bool(ip)

            ltp_items = item.get("ltp", []) or []
            rt_items = item.get("rt", []) or []

            market_id = bmi if bmi else mi
            metadata = self.market_metadata_by_id.get(market_id, {})

            print(f"[MarketWS] MARKET ITEM => mi={mi}, bmi={bmi}, market_id={market_id}, eid={eid}, eti={eti}, status={ms}, ip={ip}, in_play={in_play}")
            print(f"[MarketWS] LTP COUNT => {len(ltp_items)}")
            print(f"[MarketWS] RT COUNT => {len(rt_items)}")

            if not market_id:
                continue

            market = await asyncio.to_thread(
                self._get_or_create_market,
                market_id,
                eid,
                eti,
                ms,
                tdv,
                metadata,
            )
            if not market:
                continue

            runner_map = {
                str(r.get("selection_id")): r
                for r in metadata.get("runners", [])
                if str(r.get("selection_id", "")).strip()
            }

            processed_runner_ids = set()

            for ltp_item in ltp_items:
                runner_id = _to_int(ltp_item.get("ri"))
                ltp_value = _to_decimal(ltp_item.get("ltp"))
                tv_value = _to_decimal(ltp_item.get("tv", tdv or 0))

                if runner_id is None or ltp_value is None:
                    continue

                runner_data = runner_map.get(str(runner_id), {})
                runner = await asyncio.to_thread(
                    self._get_or_create_runner,
                    market,
                    runner_id,
                    runner_data,
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
                        "in_play": "1" if in_play else "0",
                        "tdv": tdv,
                        "message_type": message_type,
                        "source": "market_ws_async",
                        "event_name": metadata.get("event_name"),
                        "sport_id": metadata.get("sport_id"),
                        "sport_name": metadata.get("sport_name"),
                        "tournament_id": metadata.get("tournament_id"),
                        "tournament_name": metadata.get("tournament_name"),
                        "market_name": metadata.get("market_name"),
                        "market_type": metadata.get("market_type"),
                        "runner_name": runner.runner_name,
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

                runner_data = runner_map.get(str(runner_id), {})
                runner = await asyncio.to_thread(
                    self._get_or_create_runner,
                    market,
                    runner_id,
                    runner_data,
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
                        "in_play": "1" if in_play else "0",
                        "tdv": tdv,
                        "message_type": message_type,
                        "source": "market_ws_async_rt_fallback",
                        "event_name": metadata.get("event_name"),
                        "sport_id": metadata.get("sport_id"),
                        "sport_name": metadata.get("sport_name"),
                        "tournament_id": metadata.get("tournament_id"),
                        "tournament_name": metadata.get("tournament_name"),
                        "market_name": metadata.get("market_name"),
                        "market_type": metadata.get("market_type"),
                        "runner_name": runner.runner_name,
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
                    print(f"[MarketWS] No market packets yet for {elapsed}s. Keeping connection open and waiting...")
                    no_data_start = time.time()

    async def connect_once(self):
        urls = self.build_urls()
        print(f"[MarketWS] FINAL SUBSCRIBE MARKETS COUNT: {len(self.subscribe_markets)}")

        if not self.subscribe_markets:
            raise Exception("No market ids available for subscription")

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

                    subscribe_payload = {
                        "action": "set",
                        "markets": ",".join(self.subscribe_markets),
                    }

                    print(f"[MarketWS] SENDING SUBSCRIBE WITH {len(self.subscribe_markets)} MARKET IDS")
                    await ws.send(json.dumps(subscribe_payload))
                    print("[MarketWS] SUBSCRIBED SUCCESSFULLY")

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