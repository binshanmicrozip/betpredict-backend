import asyncio
from django.core.management.base import BaseCommand

from betapp.market_ws_async import MarketWebSocketClient


class Command(BaseCommand):
    help = "Run market websocket and save live PriceTick data"

    def add_arguments(self, parser):
        parser.add_argument(
            "--token",
            type=str,
            required=True,
            help="Agent code only. Example: aig"
        )
        parser.add_argument(
            "--markets",
            type=str,
            required=True,
            help="Comma separated subscribe market ids. Example: 1.256693299"
        )

    def handle(self, *args, **options):
        agent_code = options["token"].strip()
        subscribe_markets = [x.strip() for x in options["markets"].split(",") if x.strip()]

        client = MarketWebSocketClient(
            agent_code=agent_code,
            subscribe_markets=subscribe_markets
        )

        asyncio.run(client.run_forever())