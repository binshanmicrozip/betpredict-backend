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

WS_HOSTS = [
    "socket.myzosh.com:443",
    "socket.myzosh.com:8881",
]

WS_URL_TEMPLATE = "wss://{host}/?token={token}"


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


DEFAULT_SUBSCRIBE_MARKETS = ["1", "2", "3", "4", "5"]


class MarketWebSocketClient:
    def __init__(self, agent_code: str, subscribe_markets: list[str]):
        self.agent_code = agent_code.strip()
        self.target_market_ids = [str(x).strip() for x in subscribe_markets if str(x).strip() and "." in str(x)]
        self.subscribe_markets = [str(x).strip() for x in subscribe_markets if str(x).strip() and "." not in str(x)]

        if self.target_market_ids and not self.subscribe_markets:
            print("[MarketWS] WARNING: subscribing to exact market IDs because market IDs were provided")
            self.subscribe_markets = self.target_market_ids.copy()

        print(f"[MarketWS] TARGET MARKET IDS: {self.target_market_ids}")
        print(f"[MarketWS] SUBSCRIBE MARKETS: {self.subscribe_markets}")

        self.ws = None
        self.market_cache: dict[str, Market | None] = {}
        self.runner_cache: dict[tuple[str, int], Runner | None] = {}
        self.saved_tick_count = 0
        self.skipped_tick_count = 0

    def build_urls(self):
        timestamp = int(time.time() * 1000)
        token = f"{self.agent_code}-{timestamp}"
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
        """Get or create a market from socket data."""
        if market_id in self.market_cache:
            return self.market_cache[market_id]

        market, created = Market.objects.get_or_create(
            market_id=market_id,
            defaults={
                "event_id": event_id or market_id,
                "event_name": f"Event {market_id}",
                "event_type_id": event_type_id or "4",  # Cricket event type ID
                "market_name": f"Market {market_id}",
                "market_type": "MATCH_ODDS",
                "market_start_time": timezone.now(),
                "status": "OPEN" if status != 0 else "CLOSED",
                "country_code": None,
                "timezone": "UTC",
            }
        )

        if created:
            print(f"[MarketWS] CREATED NEW MARKET => market_id={market_id}, event_id={event_id}")
        else:
            # Update status and traded volume for existing market
            update_fields = []
            if status is not None and market.status != ("OPEN" if status != 0 else "CLOSED"):
                market.status = "OPEN" if status != 0 else "CLOSED"
                update_fields.append("status")
            
            if traded_volume is not None:
                market.total_tick_messages += 1
                update_fields.append("total_tick_messages")

            if update_fields:
                market.save(update_fields=update_fields)

        self.market_cache[market_id] = market
        return market

    def _get_or_create_runner(self, market: Market, selection_id: int) -> Runner | None:
        key = (market.market_id, selection_id)
        if key in self.runner_cache:
            return self.runner_cache[key]

        runner, created = Runner.objects.get_or_create(
            market=market,
            selection_id=selection_id,
            defaults={
                "runner_name": f"Runner {selection_id}",
                "status": "ACTIVE",
            }
        )
        if created:
            print(f"[MarketWS] CREATED NEW RUNNER => market={market.market_id}, selection_id={selection_id}")
        
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
        """Check if there's active live cricket data for this market."""
        import os
        from pathlib import Path

        # Check if there's any recent LiveMatchState (within last 5 minutes)
        recent_cutoff = timezone.now() - timezone.timedelta(minutes=5)
        recent_live_matches = LiveMatchState.objects.filter(fetched_at__gte=recent_cutoff).exists()

        if recent_live_matches:
            print(f"[MarketWS] ✓ Live cricket data available (DB) - saving market data for {market_id}")
            return True

        # Also check for recent CSV files (within last 5 minutes)
        csv_dir = Path("live_match_data")
        if csv_dir.exists():
            current_time = timezone.now()
            for csv_file in csv_dir.glob("live_match_*.csv"):
                if csv_file.stat().st_mtime > (current_time - timezone.timedelta(minutes=5)).timestamp():
                    print(f"[MarketWS] ✓ Live cricket data available (CSV) - saving market data for {market_id}")
                    return True

        print(f"[MarketWS] ✗ No recent live cricket data - skipping market data for {market_id}")
        return False

    def _save_price_tick_to_db(self, market_id: str, selection_id: int, ltp: Decimal, traded_volume: Decimal | None, source: str):
        # FIRST: Check if there's live cricket data for this market
        if not self._has_live_cricket_data(market_id):
            self.skipped_tick_count += 1
            print(f"[MarketWS] SKIPPED - No live cricket data for market: {market_id}")
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

        message_type = payload.get("messageType")
        print(f"[MarketWS] MESSAGE TYPE: {message_type}")

        if message_type and str(message_type).lower() != "match_odds":
            print(f"[MarketWS] Ignoring messageType={message_type}")
            return

        for item in payload.get("data", []):
            mi = str(item.get("mi") or "").strip()
            bmi = str(item.get("bmi") or "").strip()
            eid = str(item.get("eid") or "").strip()
            eti = str(item.get("eti") or "4").strip()
            ms = _to_int(item.get("ms"))
            tdv = _to_decimal(item.get("tdv", 0))
            
            ltp_items = item.get("ltp", []) or []
            rt_items = item.get("rt", []) or []

            market_id = bmi if bmi else mi

            print(f"[MarketWS] MARKET ITEM => mi={mi}, bmi={bmi}, market_id={market_id}, eid={eid}, status={ms}")
            print(f"[MarketWS] LTP COUNT => {len(ltp_items)}")
            print(f"[MarketWS] RT COUNT => {len(rt_items)}")

            if not market_id:
                print("[MarketWS] No market id found, skipping")
                continue

            if self.target_market_ids and market_id not in self.target_market_ids:
                print(f"[MarketWS] Skipping market_id={market_id} because it is not in target ids")
                continue

            # Create or get the market
            market = await asyncio.to_thread(
                self._get_or_create_market,
                market_id,
                eid,
                eti,
                ms,
                tdv,
            )

            if not market:
                self.skipped_tick_count += 1
                print(f"[MarketWS] Failed to create/get market: {market_id}")
                continue

            processed_runner_ids = set()

            # Process LTP items and create/get runners
            for ltp_item in ltp_items:
                runner_id = _to_int(ltp_item.get("ri"))
                ltp_value = _to_decimal(ltp_item.get("ltp"))
                tv_value = _to_decimal(ltp_item.get("tv", tdv or 0))

                print(f"[MarketWS] LTP ITEM => runner_id={runner_id}, ltp={ltp_value}, tv={tv_value}")

                if runner_id is None or ltp_value is None:
                    continue

                # Create or get the runner
                runner = await asyncio.to_thread(
                    self._get_or_create_runner,
                    market,
                    runner_id,
                )

                if not runner:
                    self.skipped_tick_count += 1
                    continue

                self.save_latest_price_to_redis(
                    market_id=market_id,
                    runner_id=runner_id,
                    ltp=ltp_value,
                    tv=tv_value,
                    extra_data={"mi": mi, "bmi": bmi, "source": "market_ws_async", "eid": eid},
                )

                await self.save_price_tick_to_db(
                    market_id=market_id,
                    selection_id=runner_id,
                    ltp=ltp_value,
                    traded_volume=tv_value,
                    source="market_ws_async",
                )

                processed_runner_ids.add(runner_id)

            # Process RT (rate) items, only if runner not already handled by ltp
            for rt_item in rt_items:
                runner_id = _to_int(rt_item.get("ri"))
                if runner_id is None or runner_id in processed_runner_ids:
                    continue

                ltp_value = _to_decimal(rt_item.get("rt"))
                tv_value = _to_decimal(rt_item.get("tv", tdv or 0))

                print(f"[MarketWS] RT ITEM => runner_id={runner_id}, rt={ltp_value}, tv={tv_value}")

                if ltp_value is None:
                    continue

                # Create or get the runner
                runner = await asyncio.to_thread(
                    self._get_or_create_runner,
                    market,
                    runner_id,
                )

                if not runner:
                    self.skipped_tick_count += 1
                    continue

                self.save_latest_price_to_redis(
                    market_id=market_id,
                    runner_id=runner_id,
                    ltp=ltp_value,
                    tv=tv_value,
                    extra_data={"mi": mi, "bmi": bmi, "source": "market_ws_async_rt_fallback", "eid": eid},
                )

                await self.save_price_tick_to_db(
                    market_id=market_id,
                    selection_id=runner_id,
                    ltp=ltp_value,
                    traded_volume=tv_value,
                    source="market_ws_async_rt_fallback",
                )

        if self.saved_tick_count or self.skipped_tick_count:
            print(f"[MarketWS] Message processed: saved={self.saved_tick_count}, skipped={self.skipped_tick_count} (cricket+market sync required)")


    async def connect_once(self):
        urls = self.build_urls()
        print(f"[MarketWS] SUBSCRIBE MARKETS: {self.subscribe_markets}")

        if not self.subscribe_markets:
            print("[MarketWS] ERROR: no markets configured to subscribe")
            return

        last_error = None
        for url in urls:
            print(f"[MarketWS] CONNECTING: {url}")
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
                        "markets": ",".join(self.subscribe_markets)
                    }
                    print(f"[MarketWS] SENDING SUBSCRIBE: {subscribe_payload}")
                    await ws.send(json.dumps(subscribe_payload))
                    print(f"[MarketWS] SUBSCRIBED: {subscribe_payload}")

                    async for message in ws:
                        print(f"[MarketWS] RAW MESSAGE: {message}")
                        await self.process_market_message(message)
                    return
            except Exception as e:
                last_error = e
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