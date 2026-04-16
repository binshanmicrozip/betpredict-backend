import asyncio
import json
import time
from decimal import Decimal

import websockets
from asgiref.sync import sync_to_async
from django.db import close_old_connections
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from betapp.models import Market, Runner, PriceTick


class MarketWebSocketClient:
    def __init__(self, agent_code, subscribe_markets):
        self.agent_code = agent_code
        self.subscribe_markets = [str(x).strip() for x in subscribe_markets if str(x).strip()]
        self.ws = None

    def build_url(self):
        timestamp = int(time.time() * 1000)
        return f"wss://sr-socket.myzosh.com/?token={self.agent_code}-{timestamp}"

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

    def _get_market(self, item):
        mi = str(item.get("mi") or "").strip()
        bmi = str(item.get("bmi") or "").strip()

        market = None

        if mi:
            market = Market.objects.filter(market_id=mi).first()

        if not market and bmi:
            market = Market.objects.filter(market_id=bmi).first()

        if not market and bmi and hasattr(Market, "source_market_id"):
            market = Market.objects.filter(source_market_id=bmi).first()

        return market

    def _get_runner(self, market, runner_id):
        runner_id = str(runner_id).strip()

        runner = Runner.objects.filter(market=market, runner_id=runner_id).first()
        if runner:
            return runner

        if hasattr(Runner, "selection_id"):
            runner = Runner.objects.filter(market=market, selection_id=runner_id).first()

        return runner

    def _save_price_ticks_sync(self, raw_message):
        close_old_connections()

        try:
            payload = json.loads(raw_message)
        except Exception as e:
            print(f"[MarketWS] JSON parse error: {e}")
            print(raw_message)
            return

        message_type = payload.get("messageType")
        if message_type and message_type != "match_odds":
            return

        for item in payload.get("data", []):
            market = self._get_market(item)
            if not market:
                print(f"[MarketWS] Market not found | mi={item.get('mi')} | bmi={item.get('bmi')}")
                continue

            tick_time = parse_datetime(item.get("grt")) if item.get("grt") else timezone.now()
            total_market_tv = item.get("tdv")

            ltp_map = {}
            for x in item.get("ltp", []):
                ri = str(x.get("ri") or "").strip()
                if ri:
                    ltp_map[ri] = x

            processed_runner_ids = set()

            for ltp_item in item.get("ltp", []):
                runner_id = str(ltp_item.get("ri") or "").strip()
                if not runner_id:
                    continue

                runner = self._get_runner(market, runner_id)
                if not runner:
                    print(f"[MarketWS] Runner not found | market={market.market_id} | runner={runner_id}")
                    continue

                ltp_value = ltp_item.get("ltp")
                tv_value = ltp_item.get("tv", total_market_tv)

                if ltp_value is None:
                    continue

                PriceTick.objects.create(
                    market=market,
                    runner=runner,
                    tick_time=tick_time,
                    ltp=Decimal(str(ltp_value)),
                    traded_volume=Decimal(str(tv_value or 0)),
                    phase="live"
                )

                processed_runner_ids.add(runner_id)

            for rt_item in item.get("rt", []):
                runner_id = str(rt_item.get("ri") or "").strip()
                if not runner_id or runner_id in processed_runner_ids:
                    continue

                runner = self._get_runner(market, runner_id)
                if not runner:
                    continue

                ltp_value = rt_item.get("rt")
                tv_value = rt_item.get("tv", total_market_tv)

                if ltp_value is None:
                    continue

                PriceTick.objects.create(
                    market=market,
                    runner=runner,
                    tick_time=tick_time,
                    ltp=Decimal(str(ltp_value)),
                    traded_volume=Decimal(str(tv_value or 0)),
                    phase="live"
                )

    async def save_price_ticks(self, raw_message):
        await sync_to_async(self._save_price_ticks_sync, thread_sensitive=True)(raw_message)

    async def connect_once(self):
        url = self.build_url()

        print(f"[MarketWS] CONNECTING: {url}")
        print(f"[MarketWS] SUBSCRIBE MARKETS: {self.subscribe_markets}")

        async with websockets.connect(
            url,
            open_timeout=20,
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
            await ws.send(json.dumps(subscribe_payload))
            print(f"[MarketWS] SUBSCRIBED: {subscribe_payload}")

            async for message in ws:
                print(f"[MarketWS] RAW MESSAGE: {message}")
                await self.save_price_ticks(message)

    async def run_forever(self):
        while True:
            try:
                await self.connect_once()
            except Exception as e:
                print(f"[MarketWS] ERROR: {e}")
                print("[MarketWS] Reconnecting in 5 seconds...")
                await asyncio.sleep(5)