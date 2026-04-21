import asyncio
from django.core.management.base import BaseCommand

from betapp.market_ws_async import MarketWebSocketClient


class Command(BaseCommand):
    help = "Run market websocket and save live prices to Redis"

    def add_arguments(self, parser):
        parser.add_argument(
            "--token",
            type=str,
            required=True,
            help="Agent code or raw token"
        )
        parser.add_argument(
            "--markets",
            type=str,
            required=False,
            default="",
            help="Comma separated exact market ids"
        )
        parser.add_argument(
            "--save-db-without-cricket",
            action="store_true",
            help="Save DB price ticks even without cricket data"
        )
        parser.add_argument(
            "--token-mode",
            type=str,
            choices=["auto", "raw", "agent"],
            default="auto",
            help="auto=detect, raw=use token exactly, agent=append timestamp"
        )

    def handle(self, *args, **options):
        token = options["token"].strip()
        markets_raw = (options.get("markets") or "").strip()
        save_db_without_cricket = options.get("save_db_without_cricket", False)
        token_mode = options.get("token_mode", "auto")

        subscribe_markets = [x.strip() for x in markets_raw.split(",") if x.strip()] if markets_raw else []

        print(f"[MarketWS] Exact market subscription requested: {subscribe_markets}")

        client = MarketWebSocketClient(
            token_or_agent=token,
            subscribe_markets=subscribe_markets,
            save_db_without_cricket=save_db_without_cricket,
            token_mode=token_mode,
        )

        asyncio.run(client.run_forever())