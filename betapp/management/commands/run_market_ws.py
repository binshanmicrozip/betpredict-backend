# import asyncio
# from django.core.management.base import BaseCommand

# from betapp.market_ws_async import MarketWebSocketClient


# class Command(BaseCommand):
#     help = "Run market websocket and save live prices to Redis"

#     def add_arguments(self, parser):
#         parser.add_argument(
#             "--token",
#             type=str,
#             required=True,
#             help="Agent code or raw token"
#         )
#         parser.add_argument(
#             "--markets",
#             type=str,
#             required=False,
#             default="",
#             help="Comma separated exact market ids"
#         )
#         parser.add_argument(
#             "--save-db-without-cricket",
#             action="store_true",
#             help="Save DB price ticks even without cricket data"
#         )
#         parser.add_argument(
#             "--token-mode",
#             type=str,
#             choices=["auto", "raw", "agent"],
#             default="auto",
#             help="auto=detect, raw=use token exactly, agent=append timestamp"
#         )

#     def handle(self, *args, **options):
#         token = options["token"].strip()
#         markets_raw = (options.get("markets") or "").strip()
#         save_db_without_cricket = options.get("save_db_without_cricket", False)
#         token_mode = options.get("token_mode", "auto")

#         subscribe_markets = [x.strip() for x in markets_raw.split(",") if x.strip()] if markets_raw else []

#         print(f"[MarketWS] Exact market subscription requested: {subscribe_markets}")

#         client = MarketWebSocketClient(
#             token_or_agent=token,
#             subscribe_markets=subscribe_markets,
#             save_db_without_cricket=save_db_without_cricket,
#             token_mode=token_mode,
#         )

#         asyncio.run(client.run_forever())

import asyncio

from django.conf import settings
from django.core.management.base import BaseCommand

from betapp.market_ws_async import MarketWebSocketClient
from betapp.myzosh_api import MyZoshAPI


class Command(BaseCommand):
    help = "Run MyZosh market websocket for all live/open markets"

    def add_arguments(self, parser):
        parser.add_argument(
            "--markets",
            required=False,
            help="Comma-separated market ids"
        )
        parser.add_argument(
            "--all-live",
            action="store_true",
            help="Auto discover all live/open market ids from API chain"
        )
        parser.add_argument(
            "--sport-ids",
            required=False,
            help="Optional comma-separated sport ids. Example: 4,1,2"
        )
        parser.add_argument(
            "--source-id",
            required=False,
            help="Optional source_id"
        )
        parser.add_argument(
            "--save-db-without-cricket",
            action="store_true",
            help="Allow DB save even if live cricket data is missing"
        )

    def handle(self, *args, **options):
        markets_raw = options.get("markets")
        all_live = options.get("all_live")
        sport_ids_raw = options.get("sport_ids")
        source_id = options.get("source_id")
        save_db_without_cricket = options.get("save_db_without_cricket")

        token = None
        token_mode = "raw"
        market_catalog = []
        subscribe_markets = []

        agent_code = getattr(settings, "MYZOSH_AGENT_CODE", "").strip()
        secret_key = getattr(settings, "MYZOSH_SECRET_KEY", "").strip()

        if not agent_code or not secret_key:
            raise Exception("MYZOSH_AGENT_CODE / MYZOSH_SECRET_KEY missing in settings.py")

        api = MyZoshAPI(
            agent_code=agent_code,
            secret_key=secret_key,
            source_id=source_id,
        )

        if markets_raw:
            subscribe_markets = [
                x.strip()
                for x in str(markets_raw).split(",")
                if x.strip()
            ]

            token = api.get_access_token()

            # Fetch live catalog from API to get proper runner names
            self.stdout.write("[MarketWS] Fetching live catalog from API for runner names...")
            try:
                live_catalog = api.discover_live_market_catalog(only_with_market_count=False)
                api_catalog_by_id = {
                    str(item["market_id"]): item
                    for item in live_catalog
                    if item.get("market_id")
                }
                self.stdout.write(self.style.SUCCESS(
                    f"[MarketWS] API catalog fetched: {len(api_catalog_by_id)} markets"
                ))
            except Exception as e:
                self.stdout.write(self.style.WARNING(
                    f"[MarketWS] API catalog fetch failed ({e}), falling back to DB"
                ))
                api_catalog_by_id = {}

            from betapp.models import Market, Runner as RunnerModel
            for mid in subscribe_markets:
                # Use API catalog entry if available (has proper runner names)
                if mid in api_catalog_by_id:
                    api_entry = api_catalog_by_id[mid]
                    market_catalog.append(api_entry)
                    runner_names = [r.get("runner_name") for r in api_entry.get("runners", [])]
                    self.stdout.write(self.style.SUCCESS(
                        f"[MarketWS] Catalog from API for market {mid}: "
                        f"{api_entry.get('event_name')} | runners={runner_names}"
                    ))
                    # Also update DB Runner records with proper names
                    try:
                        market_obj = Market.objects.get(market_id=mid)
                        for r in api_entry.get("runners", []):
                            sel_id = r.get("selection_id")
                            rname = (r.get("runner_name") or "").strip()
                            if sel_id and rname and not rname.startswith("Runner "):
                                RunnerModel.objects.filter(
                                    market=market_obj, selection_id=sel_id
                                ).update(runner_name=rname)
                    except Exception:
                        pass
                else:
                    # Fall back to DB
                    try:
                        market_obj = Market.objects.get(market_id=mid)
                        runners_qs = RunnerModel.objects.filter(market=market_obj)
                        market_catalog.append({
                            "market_id": mid,
                            "event_id": market_obj.event_id or mid,
                            "event_name": market_obj.event_name or f"Event {mid}",
                            "sport_id": market_obj.event_type_id or "4",
                            "sport_name": "Cricket",
                            "tournament_id": "",
                            "tournament_name": "",
                            "market_name": market_obj.market_name or market_obj.market_type or "Match Odds",
                            "market_type": market_obj.market_type or "MATCH_ODDS",
                            "runners": [
                                {"selection_id": str(r.selection_id), "runner_name": r.runner_name, "handicap": None}
                                for r in runners_qs
                            ],
                        })
                        self.stdout.write(self.style.SUCCESS(
                            f"[MarketWS] Catalog from DB for market {mid}: "
                            f"{market_obj.event_name} | {runners_qs.count()} runners"
                        ))
                    except Market.DoesNotExist:
                        self.stdout.write(self.style.WARNING(
                            f"[MarketWS] Market {mid} not in API or DB — runner names will be generic"
                        ))

            self.stdout.write(self.style.SUCCESS(
                f"[MarketWS] Manual market ids provided: {len(subscribe_markets)}"
            ))

        elif all_live:
            sport_ids = None
            if sport_ids_raw:
                sport_ids = [
                    x.strip()
                    for x in str(sport_ids_raw).split(",")
                    if x.strip()
                ]

            self.stdout.write(self.style.WARNING(
                "[MarketWS] Discovering live/open markets from sports -> tournaments -> matches -> exchange markets"
            ))

            market_catalog = api.discover_live_market_catalog(
                sport_ids=sport_ids,
                only_with_market_count=True,
            )

            if not market_catalog:
                raise Exception("No live/open market metadata discovered from MyZosh API chain")

            subscribe_markets = [
                str(item.get("market_id")).strip()
                for item in market_catalog
                if str(item.get("market_id", "")).strip()
            ]

            token = api.access_token
            token_mode = "raw"

            self.stdout.write(self.style.SUCCESS(
                f"[MarketWS] Auto-discovered live/open market ids: {len(subscribe_markets)}"
            ))

        else:
            raise Exception("Use either --markets or --all-live")

        client = MarketWebSocketClient(
            token_or_agent=token,
            subscribe_markets=subscribe_markets,
            market_catalog=market_catalog,
            save_db_without_cricket=save_db_without_cricket,
            token_mode=token_mode,
        )

        asyncio.run(client.run_forever())