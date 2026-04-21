# import time
# from django.core.management.base import BaseCommand
# from betapp.live_signal_engine import run_live_prediction
# from betapp.redis_price import get_all_market_prices


# class Command(BaseCommand):
#     help = "Run live signal prediction loop"

#     def add_arguments(self, parser):
#         parser.add_argument("--match_id", type=str, required=True)
#         parser.add_argument("--market_id", type=str, required=True)
#         parser.add_argument("--runner_id", type=str, required=False, default=None)
#         parser.add_argument(
#             "--auto_select_runner",
#             action="store_true",
#             help="If no runner_id is provided and multiple live runners exist, select the first one automatically",
#         )
#         parser.add_argument("--interval", type=float, default=2.0)

#     def handle(self, *args, **options):
#         match_id = options["match_id"]
#         market_id = options["market_id"]
#         runner_id = options["runner_id"]
#         auto_select_runner = options["auto_select_runner"]
#         interval = options["interval"]

#         if not runner_id:
#             prices = get_all_market_prices(market_id)
#             if not prices:
#                 self.stdout.write(self.style.ERROR(
#                     f"No live runner prices found for market_id={market_id}. "
#                     "Start run_market_ws and verify the market subscription."
#                 ))
#                 return

#             if len(prices) == 1:
#                 runner_id = prices[0]["runner_id"]
#                 self.stdout.write(self.style.SUCCESS(
#                     f"Auto-selected runner_id={runner_id} for market_id={market_id}"
#                 ))
#             elif auto_select_runner:
#                 runner_id = prices[0]["runner_id"]
#                 self.stdout.write(self.style.WARNING(
#                     f"Multiple live runners found; auto-selected runner_id={runner_id}."
#                 ))
#             else:
#                 runner_ids = ", ".join(str(p["runner_id"]) for p in prices)
#                 self.stdout.write(self.style.ERROR(
#                     f"Multiple live runners available for market_id={market_id}: {runner_ids}. "
#                     "Provide --runner_id or use --auto_select_runner."
#                 ))
#                 return

#         self.stdout.write(self.style.SUCCESS("Live signal loop started"))
#         self.stdout.write(f"Using match_id={match_id}")
#         self.stdout.write(f"Using market_id={market_id}")
#         self.stdout.write(f"Using runner_id={runner_id}")

#         while True:
#             try:
#                 result = run_live_prediction(match_id, market_id, runner_id)
#                 self.stdout.write(str(result))
#             except Exception as e:
#                 self.stdout.write(self.style.ERROR(f"Error: {e}"))

#             time.sleep(interval)


import time
from django.core.management.base import BaseCommand
from django.utils import timezone

from betapp.live_signal_engine import run_live_prediction
from betapp.redis_price import get_all_market_prices, get_latest_price
from betapp.services.live_market_tick_service import save_live_market_tick
from betapp.services.market_metadata_service import get_market_metadata


class Command(BaseCommand):
    help = "Run live signal prediction loop and save market rows for testing"

    def add_arguments(self, parser):
        parser.add_argument("--match_id", type=str, required=True)
        parser.add_argument("--market_id", type=str, required=True)

        # NEW: support multiple runner ids
        parser.add_argument(
            "--runner_ids",
            type=str,
            required=False,
            default=None,
            help="Comma-separated runner ids, example: 7671296,22121561",
        )

        parser.add_argument(
            "--auto_select_runner",
            action="store_true",
            help="If runner_ids are not provided, automatically select all live runners for this market",
        )

        parser.add_argument("--interval", type=float, default=2.0)

    def handle(self, *args, **options):
        match_id = str(options["match_id"])
        market_id = str(options["market_id"])
        runner_ids_raw = options["runner_ids"]
        auto_select_runner = options["auto_select_runner"]
        interval = options["interval"]

        prev_ltp_map = {}

        # NEW: build runner_ids list
        runner_ids = []
        if runner_ids_raw:
            runner_ids = [r.strip() for r in str(runner_ids_raw).split(",") if r.strip()]

        # If no runner_ids passed, get from Redis
        if not runner_ids:
            prices = get_all_market_prices(market_id)

            if not prices:
                self.stdout.write(self.style.ERROR(
                    f"No live runner prices found for market_id={market_id}. "
                    "Start run_market_ws and verify the market subscription."
                ))
                return

            if auto_select_runner:
                runner_ids = [str(p["runner_id"]) for p in prices]
                self.stdout.write(self.style.SUCCESS(
                    f"Auto-selected live runner_ids for market_id={market_id}: {', '.join(runner_ids)}"
                ))
            elif len(prices) == 1:
                runner_ids = [str(prices[0]["runner_id"])]
                self.stdout.write(self.style.SUCCESS(
                    f"Auto-selected single runner_id={runner_ids[0]} for market_id={market_id}"
                ))
            else:
                available_runner_ids = ", ".join(str(p["runner_id"]) for p in prices)
                self.stdout.write(self.style.ERROR(
                    f"Multiple live runners available for market_id={market_id}: {available_runner_ids}. "
                    "Provide --runner_ids or use --auto_select_runner."
                ))
                return

        self.stdout.write(self.style.SUCCESS("Live signal loop started"))
        self.stdout.write(f"Using match_id={match_id}")
        self.stdout.write(f"Using market_id={market_id}")
        self.stdout.write(f"Using runner_ids={', '.join(runner_ids)}")

        while True:
            try:
                for runner_id in runner_ids:
                    price = get_latest_price(market_id, runner_id)

                    if not price:
                        self.stdout.write(self.style.WARNING(
                            f"No price found in Redis for market_id={market_id}, runner_id={runner_id}"
                        ))
                        continue

                    ltp = price.get("ltp")
                    tv = price.get("tv")
                    prev_ltp = prev_ltp_map.get(str(runner_id))

                    if prev_ltp is None:
                        prev_ltp = price.get("prev_ltp")

                    result = run_live_prediction(match_id, market_id, runner_id)
                    self.stdout.write(f"[Prediction] runner_id={runner_id} => {result}")

                    metadata = get_market_metadata(market_id, runner_id)

                    now = timezone.now()

                    payload = {
                        "market_id": str(market_id),
                        "event_id": metadata.get("event_id"),
                        "event_name": metadata.get("event_name"),
                        "market_type": metadata.get("market_type"),
                        "market_time": metadata.get("market_time"),
                        "runner_id": str(runner_id),
                        "runner_name": metadata.get("runner_name"),
                        "publish_time_ms": int(now.timestamp() * 1000),
                        "publish_time_utc": now,
                        "ltp": ltp,
                        "prev_ltp": prev_ltp,
                        "tv": tv,
                        "market_status": "OPEN",
                        "in_play": True,
                        "bet_delay": 0,
                        "winner": None,
                        "settled_time": None,
                        "year": now.year,
                        "month": now.month,
                    }

                    save_live_market_tick(payload)

                    self.stdout.write(self.style.SUCCESS(
                        f"[LiveMarketTick] saved market_id={market_id} "
                        f"runner_id={runner_id} "
                        f"runner_name={metadata.get('runner_name')} "
                        f"event_name={metadata.get('event_name')} "
                        f"ltp={ltp} tv={tv}"
                    ))

                    prev_ltp_map[str(runner_id)] = ltp

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error: {e}"))

            time.sleep(interval)