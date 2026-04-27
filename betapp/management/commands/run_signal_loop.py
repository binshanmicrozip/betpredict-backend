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


# import time
# from django.core.management.base import BaseCommand
# from django.utils import timezone

# from betapp.live_signal_engine import run_live_prediction
# from betapp.redis_price import get_all_market_prices, get_latest_price
# from betapp.services.live_market_tick_service import save_live_market_tick
# from betapp.services.market_metadata_service import get_market_metadata


# class Command(BaseCommand):
#     help = "Run live signal prediction loop and save market rows for testing"

#     def add_arguments(self, parser):
#         parser.add_argument("--match_id", type=str, required=True)
#         parser.add_argument("--market_id", type=str, required=True)

#         # NEW: support multiple runner ids
#         parser.add_argument(
#             "--runner_ids",
#             type=str,
#             required=False,
#             default=None,
#             help="Comma-separated runner ids, example: 7671296,22121561",
#         )

#         parser.add_argument(
#             "--auto_select_runner",
#             action="store_true",
#             help="If runner_ids are not provided, automatically select all live runners for this market",
#         )

#         parser.add_argument("--interval", type=float, default=2.0)

#     def handle(self, *args, **options):
#         match_id = str(options["match_id"])
#         market_id = str(options["market_id"])
#         runner_ids_raw = options["runner_ids"]
#         auto_select_runner = options["auto_select_runner"]
#         interval = options["interval"]

#         prev_ltp_map = {}

#         # NEW: build runner_ids list
#         runner_ids = []
#         if runner_ids_raw:
#             runner_ids = [r.strip() for r in str(runner_ids_raw).split(",") if r.strip()]

#         # If no runner_ids passed, get from Redis
#         if not runner_ids:
#             prices = get_all_market_prices(market_id)

#             if not prices:
#                 self.stdout.write(self.style.ERROR(
#                     f"No live runner prices found for market_id={market_id}. "
#                     "Start run_market_ws and verify the market subscription."
#                 ))
#                 return

#             if auto_select_runner:
#                 runner_ids = [str(p["runner_id"]) for p in prices]
#                 self.stdout.write(self.style.SUCCESS(
#                     f"Auto-selected live runner_ids for market_id={market_id}: {', '.join(runner_ids)}"
#                 ))
#             elif len(prices) == 1:
#                 runner_ids = [str(prices[0]["runner_id"])]
#                 self.stdout.write(self.style.SUCCESS(
#                     f"Auto-selected single runner_id={runner_ids[0]} for market_id={market_id}"
#                 ))
#             else:
#                 available_runner_ids = ", ".join(str(p["runner_id"]) for p in prices)
#                 self.stdout.write(self.style.ERROR(
#                     f"Multiple live runners available for market_id={market_id}: {available_runner_ids}. "
#                     "Provide --runner_ids or use --auto_select_runner."
#                 ))
#                 return

#         self.stdout.write(self.style.SUCCESS("Live signal loop started"))
#         self.stdout.write(f"Using match_id={match_id}")
#         self.stdout.write(f"Using market_id={market_id}")
#         self.stdout.write(f"Using runner_ids={', '.join(runner_ids)}")

#         while True:
#             try:
#                 for runner_id in runner_ids:
#                     price = get_latest_price(market_id, runner_id)

#                     if not price:
#                         self.stdout.write(self.style.WARNING(
#                             f"No price found in Redis for market_id={market_id}, runner_id={runner_id}"
#                         ))
#                         continue

#                     ltp = price.get("ltp")
#                     tv = price.get("tv")
#                     prev_ltp = prev_ltp_map.get(str(runner_id))

#                     if prev_ltp is None:
#                         prev_ltp = price.get("prev_ltp")

#                     result = run_live_prediction(match_id, market_id, runner_id)
#                     self.stdout.write(f"[Prediction] runner_id={runner_id} => {result}")

#                     metadata = get_market_metadata(market_id, runner_id)

#                     now = timezone.now()

#                     payload = {
#                         "market_id": str(market_id),
#                         "event_id": metadata.get("event_id"),
#                         "event_name": metadata.get("event_name"),
#                         "market_type": metadata.get("market_type"),
#                         "market_time": metadata.get("market_time"),
#                         "runner_id": str(runner_id),
#                         "runner_name": metadata.get("runner_name"),
#                         "publish_time_ms": int(now.timestamp() * 1000),
#                         "publish_time_utc": now,
#                         "ltp": ltp,
#                         "prev_ltp": prev_ltp,
#                         "tv": tv,
#                         "market_status": "OPEN",
#                         "in_play": True,
#                         "bet_delay": 0,
#                         "winner": None,
#                         "settled_time": None,
#                         "year": now.year,
#                         "month": now.month,
#                     }

#                     save_live_market_tick(payload)

#                     self.stdout.write(self.style.SUCCESS(
#                         f"[LiveMarketTick] saved market_id={market_id} "
#                         f"runner_id={runner_id} "
#                         f"runner_name={metadata.get('runner_name')} "
#                         f"event_name={metadata.get('event_name')} "
#                         f"ltp={ltp} tv={tv}"
#                     ))

#                     prev_ltp_map[str(runner_id)] = ltp

#             except Exception as e:
#                 self.stdout.write(self.style.ERROR(f"Error: {e}"))

#             time.sleep(interval)


# import time
# from django.core.management.base import BaseCommand
# from django.utils import timezone

# from betapp.live_signal_engine import run_live_prediction
# from betapp.redis_price import get_all_market_prices, get_latest_price
# from betapp.services.live_market_tick_service import save_live_market_tick
# from betapp.services.market_metadata_service import get_market_metadata
# from betapp.archive_runtime import combined_csv_archive


# class Command(BaseCommand):
#     help = "Run live signal prediction loop, save market rows, and save combined CSV history"

#     def add_arguments(self, parser):
#         parser.add_argument("--match_id", type=str, required=True)
#         parser.add_argument("--market_id", type=str, required=True)

#         parser.add_argument(
#             "--runner_ids",
#             type=str,
#             required=False,
#             default=None,
#             help="Comma-separated runner ids, example: 7671296,22121561",
#         )

#         parser.add_argument(
#             "--auto_select_runner",
#             action="store_true",
#             help="If runner_ids are not provided, automatically select all live runners for this market",
#         )

#         parser.add_argument("--interval", type=float, default=2.0)

#     def handle(self, *args, **options):
#         match_id = str(options["match_id"])
#         market_id = str(options["market_id"])
#         runner_ids_raw = options["runner_ids"]
#         auto_select_runner = options["auto_select_runner"]
#         interval = options["interval"]

#         prev_ltp_map = {}

#         runner_ids = []
#         if runner_ids_raw:
#             runner_ids = [r.strip() for r in str(runner_ids_raw).split(",") if r.strip()]

#         if not runner_ids:
#             prices = get_all_market_prices(market_id)

#             if not prices:
#                 self.stdout.write(self.style.ERROR(
#                     f"No live runner prices found for market_id={market_id}. "
#                     "Start run_market_ws and verify the market subscription."
#                 ))
#                 return

#             if auto_select_runner:
#                 runner_ids = [str(p["runner_id"]) for p in prices]
#                 self.stdout.write(self.style.SUCCESS(
#                     f"Auto-selected live runner_ids for market_id={market_id}: {', '.join(runner_ids)}"
#                 ))
#             elif len(prices) == 1:
#                 runner_ids = [str(prices[0]["runner_id"])]
#                 self.stdout.write(self.style.SUCCESS(
#                     f"Auto-selected single runner_id={runner_ids[0]} for market_id={market_id}"
#                 ))
#             else:
#                 available_runner_ids = ", ".join(str(p["runner_id"]) for p in prices)
#                 self.stdout.write(self.style.ERROR(
#                     f"Multiple live runners available for market_id={market_id}: {available_runner_ids}. "
#                     "Provide --runner_ids or use --auto_select_runner."
#                 ))
#                 return

#         self.stdout.write(self.style.SUCCESS("Live signal loop started"))
#         self.stdout.write(f"Using match_id={match_id}")
#         self.stdout.write(f"Using market_id={market_id}")
#         self.stdout.write(f"Using runner_ids={', '.join(runner_ids)}")

#         # preload old row keys from existing CSVs for all runners
#         for preload_runner_id in runner_ids:
#             csv_path = combined_csv_archive.get_csv_path(match_id, market_id, preload_runner_id)
#             combined_csv_archive.preload_existing_keys(csv_path)

#         while True:
#             try:
#                 for runner_id in runner_ids:
#                     price = get_latest_price(market_id, runner_id)

#                     if not price:
#                         self.stdout.write(self.style.WARNING(
#                             f"No price found in Redis for market_id={market_id}, runner_id={runner_id}"
#                         ))
#                         continue

#                     ltp = price.get("ltp")
#                     tv = price.get("tv")
#                     prev_ltp = prev_ltp_map.get(str(runner_id))

#                     if prev_ltp is None:
#                         prev_ltp = price.get("prev_ltp")

#                     result = run_live_prediction(match_id, market_id, runner_id)
#                     self.stdout.write(f"[Prediction] runner_id={runner_id} => {result}")

#                     # save one combined CSV row
#                     if isinstance(result, dict) and result.get("ball_key"):
#                         csv_saved = combined_csv_archive.save_combined_row(result)

#                         if csv_saved:
#                             self.stdout.write(self.style.SUCCESS(
#                                 f"[CSV COMBINED] saved ball_key={result.get('ball_key')} "
#                                 f"market_id={result.get('market_id')} "
#                                 f"runner_id={result.get('runner_id')}"
#                             ))
#                         else:
#                             self.stdout.write(
#                                 f"[CSV COMBINED] skipped duplicate or invalid row for runner_id={runner_id}"
#                             )
#                     else:
#                         self.stdout.write(self.style.WARNING(
#                             f"[CSV COMBINED] result missing ball_key for runner_id={runner_id}"
#                         ))

#                     metadata = get_market_metadata(market_id, runner_id)

#                     now = timezone.now()

#                     payload = {
#                         "market_id": str(market_id),
#                         "event_id": metadata.get("event_id"),
#                         "event_name": metadata.get("event_name"),
#                         "market_type": metadata.get("market_type"),
#                         "market_time": metadata.get("market_time"),
#                         "runner_id": str(runner_id),
#                         "runner_name": metadata.get("runner_name"),
#                         "publish_time_ms": int(now.timestamp() * 1000),
#                         "publish_time_utc": now,
#                         "ltp": ltp,
#                         "prev_ltp": prev_ltp,
#                         "tv": tv,
#                         "market_status": "OPEN",
#                         "in_play": True,
#                         "bet_delay": 0,
#                         "winner": None,
#                         "settled_time": None,
#                         "year": now.year,
#                         "month": now.month,
#                     }

#                     save_live_market_tick(payload)

#                     self.stdout.write(self.style.SUCCESS(
#                         f"[LiveMarketTick] saved market_id={market_id} "
#                         f"runner_id={runner_id} "
#                         f"runner_name={metadata.get('runner_name')} "
#                         f"event_name={metadata.get('event_name')} "
#                         f"ltp={ltp} tv={tv}"
#                     ))

#                     prev_ltp_map[str(runner_id)] = ltp

#             except Exception as e:
#                 self.stdout.write(self.style.ERROR(f"Error: {e}"))

#             time.sleep(interval)





# import time
# from django.core.management.base import BaseCommand
# from django.utils import timezone

# from betapp.live_signal_engine import run_live_prediction
# from betapp.redis_cricket import get_latest_cricket          # ← ADD THIS IMPORT
# from betapp.redis_price import get_all_market_prices, get_latest_price
# from betapp.services.live_market_tick_service import save_live_market_tick
# from betapp.services.market_metadata_service import get_market_metadata
# from betapp.archive_runtime import combined_csv_archive


# class Command(BaseCommand):
#     help = "Run live signal prediction loop, backfill history, save market rows, and save combined CSV history"

#     def add_arguments(self, parser):
#         parser.add_argument("--match_id", type=str, required=True)
#         parser.add_argument("--market_id", type=str, required=True)

#         parser.add_argument(
#             "--runner_ids",
#             type=str,
#             required=False,
#             default=None,
#             help="Comma-separated runner ids, example: 7671296,22121561",
#         )

#         parser.add_argument(
#             "--auto_select_runner",
#             action="store_true",
#             help="If runner_ids are not provided, automatically select all live runners for this market",
#         )

#         parser.add_argument("--interval", type=float, default=2.0)

#     def handle(self, *args, **options):
#         match_id = str(options["match_id"])
#         market_id = str(options["market_id"])
#         runner_ids_raw = options["runner_ids"]
#         auto_select_runner = options["auto_select_runner"]
#         interval = options["interval"]

#         prev_ltp_map = {}
#         history_backfilled = set()
#         prev_ball_key_map = {}          # ← ADD: track last ball_key per runner

#         runner_ids = []
#         if runner_ids_raw:
#             runner_ids = [r.strip() for r in str(runner_ids_raw).split(",") if r.strip()]

#         if not runner_ids:
#             prices = get_all_market_prices(market_id)

#             if not prices:
#                 self.stdout.write(self.style.ERROR(
#                     f"No live runner prices found for market_id={market_id}. "
#                     "Start run_market_ws and verify the market subscription."
#                 ))
#                 return

#             if auto_select_runner:
#                 runner_ids = [str(p["runner_id"]) for p in prices]
#                 self.stdout.write(self.style.SUCCESS(
#                     f"Auto-selected live runner_ids for market_id={market_id}: {', '.join(runner_ids)}"
#                 ))
#             elif len(prices) == 1:
#                 runner_ids = [str(prices[0]["runner_id"])]
#                 self.stdout.write(self.style.SUCCESS(
#                     f"Auto-selected single runner_id={runner_ids[0]} for market_id={market_id}"
#                 ))
#             else:
#                 available_runner_ids = ", ".join(str(p["runner_id"]) for p in prices)
#                 self.stdout.write(self.style.ERROR(
#                     f"Multiple live runners available for market_id={market_id}: {available_runner_ids}. "
#                     "Provide --runner_ids or use --auto_select_runner."
#                 ))
#                 return

#         self.stdout.write(self.style.SUCCESS("Live signal loop started"))
#         self.stdout.write(f"Using match_id={match_id}")
#         self.stdout.write(f"Using market_id={market_id}")
#         self.stdout.write(f"Using runner_ids={', '.join(runner_ids)}")

#         for preload_runner_id in runner_ids:
#             csv_path = combined_csv_archive.get_csv_path(match_id, market_id, preload_runner_id)
#             combined_csv_archive.preload_existing_keys(csv_path)

#         while True:
#             try:
#                 for runner_id in runner_ids:
#                     price = get_latest_price(market_id, runner_id)

#                     if not price:
#                         self.stdout.write(self.style.WARNING(
#                             f"No price found in Redis for market_id={market_id}, runner_id={runner_id}"
#                         ))
#                         continue

#                     ltp = price.get("ltp")
#                     tv = price.get("tv")
#                     prev_ltp = prev_ltp_map.get(str(runner_id))

#                     if prev_ltp is None:
#                         prev_ltp = price.get("prev_ltp")

#                     # ── DEDUP: peek cricket ball_key BEFORE running prediction ──
#                     raw_cricket = get_latest_cricket(match_id) or {}
#                     current_ball_key = raw_cricket.get("ball_key")
#                     prev_ball_key = prev_ball_key_map.get(str(runner_id))

#                     if current_ball_key and current_ball_key == prev_ball_key:
#                         self.stdout.write(
#                             f"[DEDUP] No new ball for runner_id={runner_id} "
#                             f"ball_key={current_ball_key} — skipping prediction"
#                         )
#                         # Market prices still move between balls — save tick only
#                         self._save_tick_only(
#                             market_id, runner_id, ltp, prev_ltp, tv
#                         )
#                         prev_ltp_map[str(runner_id)] = ltp
#                         continue   # ← skip prediction + CSV

#                     # ── NEW BALL DETECTED: run full prediction pipeline ──
#                     result = run_live_prediction(match_id, market_id, runner_id)
#                     self.stdout.write(f"[Prediction] runner_id={runner_id} => {result}")

#                     # Update ball_key tracker after prediction
#                     new_ball_key = result.get("ball_key") if isinstance(result, dict) else None
#                     if new_ball_key:
#                         prev_ball_key_map[str(runner_id)] = new_ball_key

#                     runner_history_key = f"{match_id}:{market_id}:{runner_id}"
#                     if runner_history_key not in history_backfilled:
#                         self.backfill_history_rows(result, match_id, market_id, runner_id)
#                         history_backfilled.add(runner_history_key)

#                     if isinstance(result, dict) and result.get("ball_key"):
#                         csv_saved = combined_csv_archive.save_combined_row(result)

#                         if csv_saved:
#                             self.stdout.write(self.style.SUCCESS(
#                                 f"[CSV COMBINED] saved live ball_key={result.get('ball_key')} "
#                                 f"market_id={result.get('market_id')} "
#                                 f"runner_id={result.get('runner_id')}"
#                             ))
#                         else:
#                             self.stdout.write(
#                                 f"[CSV COMBINED] skipped duplicate or invalid live row for runner_id={runner_id}"
#                             )
#                     else:
#                         self.stdout.write(self.style.WARNING(
#                             f"[CSV COMBINED] result missing ball_key for runner_id={runner_id}"
#                         ))

#                     metadata = get_market_metadata(market_id, runner_id)
#                     now = timezone.now()

#                     payload = {
#                         "market_id": str(market_id),
#                         "event_id": metadata.get("event_id"),
#                         "event_name": metadata.get("event_name"),
#                         "market_type": metadata.get("market_type"),
#                         "market_time": metadata.get("market_time"),
#                         "runner_id": str(runner_id),
#                         "runner_name": metadata.get("runner_name"),
#                         "publish_time_ms": int(now.timestamp() * 1000),
#                         "publish_time_utc": now,
#                         "ltp": ltp,
#                         "prev_ltp": prev_ltp,
#                         "tv": tv,
#                         "market_status": "OPEN",
#                         "in_play": True,
#                         "bet_delay": 0,
#                         "winner": None,
#                         "settled_time": None,
#                         "year": now.year,
#                         "month": now.month,
#                     }

#                     save_live_market_tick(payload)

#                     self.stdout.write(self.style.SUCCESS(
#                         f"[LiveMarketTick] saved market_id={market_id} "
#                         f"runner_id={runner_id} "
#                         f"runner_name={metadata.get('runner_name')} "
#                         f"event_name={metadata.get('event_name')} "
#                         f"ltp={ltp} tv={tv}"
#                     ))

#                     prev_ltp_map[str(runner_id)] = ltp

#             except Exception as e:
#                 self.stdout.write(self.style.ERROR(f"Error: {e}"))

#             time.sleep(interval)

#     # ── NEW HELPER: save market tick without prediction ──────────────────────
#     def _save_tick_only(self, market_id, runner_id, ltp, prev_ltp, tv):
#         """Save market tick without running prediction.
#         Used every loop interval even when no new ball has arrived,
#         because LTP/TV still change between deliveries."""
#         try:
#             metadata = get_market_metadata(market_id, runner_id)
#             now = timezone.now()

#             payload = {
#                 "market_id": str(market_id),
#                 "event_id": metadata.get("event_id"),
#                 "event_name": metadata.get("event_name"),
#                 "market_type": metadata.get("market_type"),
#                 "market_time": metadata.get("market_time"),
#                 "runner_id": str(runner_id),
#                 "runner_name": metadata.get("runner_name"),
#                 "publish_time_ms": int(now.timestamp() * 1000),
#                 "publish_time_utc": now,
#                 "ltp": ltp,
#                 "prev_ltp": prev_ltp,
#                 "tv": tv,
#                 "market_status": "OPEN",
#                 "in_play": True,
#                 "bet_delay": 0,
#                 "winner": None,
#                 "settled_time": None,
#                 "year": now.year,
#                 "month": now.month,
#             }

#             save_live_market_tick(payload)

#             self.stdout.write(
#                 f"[LiveMarketTick][tick-only] market_id={market_id} "
#                 f"runner_id={runner_id} ltp={ltp} tv={tv}"
#             )
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"[_save_tick_only] Error: {e}"))

#     def backfill_history_rows(self, result, match_id, market_id, runner_id):
#         if not isinstance(result, dict):
#             return

#         history = result.get("history", {}) or {}
#         balls = history.get("balls", []) or []
#         market_ticks = history.get("market", []) or []

#         if not balls:
#             self.stdout.write(self.style.WARNING(
#                 f"[CSV BACKFILL] no history balls available for runner_id={runner_id}"
#             ))
#             return

#         self.stdout.write(self.style.SUCCESS(
#             f"[CSV BACKFILL] found {len(balls)} history balls for runner_id={runner_id}"
#         ))

#         current_prediction = result.get("prediction", {}) or {}
#         current_cricket = result.get("cricket", {}) or {}

#         for ball in balls:
#             nearest_market = self.find_nearest_market_tick(
#                 ball_timestamp=ball.get("timestamp"),
#                 market_ticks=market_ticks,
#                 market_id=market_id,
#                 runner_id=runner_id,
#             )

#             cricket_payload = {
#                 "source_match_id": str(match_id),
#                 "innings": ball.get("innings"),
#                 "score": f"{ball.get('score')}/{ball.get('wickets')}",
#                 "score_num": ball.get("score"),
#                 "wickets": ball.get("wickets"),
#                 "overs": str(ball.get("overs")),
#                 "overs_float": ball.get("overs"),
#                 "crr": ball.get("current_run_rate"),
#                 "rrr": ball.get("required_run_rate"),
#                 "status": current_cricket.get("status"),
#                 "state": current_cricket.get("state"),
#                 "toss": current_cricket.get("toss"),
#                 "target": current_cricket.get("target"),
#                 "phase": self.resolve_phase(ball.get("overs"), ball.get("innings")),
#                 "innings_type": current_cricket.get("innings_type"),
#                 "recent": ball.get("recent"),
#                 "last5_runs": current_cricket.get("last5_runs"),
#                 "last5_wkts": current_cricket.get("last5_wkts"),
#                 "last3_runs": current_cricket.get("last3_runs"),
#                 "latest_ball": ball.get("commentary"),
#                 "b1_name": (ball.get("batter_striker") or {}).get("name"),
#                 "b1_runs": (ball.get("batter_striker") or {}).get("runs"),
#                 "b1_balls": (ball.get("batter_striker") or {}).get("balls"),
#                 "b1_4s": (ball.get("batter_striker") or {}).get("fours"),
#                 "b1_6s": (ball.get("batter_striker") or {}).get("sixes"),
#                 "b1_sr": (ball.get("batter_striker") or {}).get("strike_rate"),
#                 "b2_name": (ball.get("batter_non_striker") or {}).get("name"),
#                 "b2_runs": (ball.get("batter_non_striker") or {}).get("runs"),
#                 "b2_balls": (ball.get("batter_non_striker") or {}).get("balls"),
#                 "b2_4s": (ball.get("batter_non_striker") or {}).get("fours"),
#                 "b2_6s": (ball.get("batter_non_striker") or {}).get("sixes"),
#                 "b2_sr": (ball.get("batter_non_striker") or {}).get("strike_rate"),
#                 "bw1_name": (ball.get("bowler") or {}).get("name"),
#                 "bw1_overs": (ball.get("bowler") or {}).get("overs"),
#                 "bw1_runs": (ball.get("bowler") or {}).get("runs"),
#                 "bw1_wkts": (ball.get("bowler") or {}).get("wickets"),
#                 "bw1_eco": (ball.get("bowler") or {}).get("economy"),
#                 "p_runs": (ball.get("partnership") or {}).get("runs"),
#                 "p_balls": (ball.get("partnership") or {}).get("balls"),
#                 "raw_json": {},
#             }

#             combined_row = {
#                 "source_match_id": str(match_id),
#                 "market_id": str(market_id),
#                 "runner_id": str(runner_id),
#                 "ball_key": ball.get("ball_key"),
#                 "cricket": cricket_payload,
#                 "price": nearest_market,
#                 "prediction": current_prediction,
#             }

#             csv_saved = combined_csv_archive.save_combined_row(combined_row)
#             if csv_saved:
#                 self.stdout.write(self.style.SUCCESS(
#                     f"[CSV BACKFILL] saved ball_key={ball.get('ball_key')}"
#                 ))

#     def find_nearest_market_tick(self, ball_timestamp, market_ticks, market_id, runner_id):
#         if not market_ticks:
#             return {
#                 "market_id": str(market_id),
#                 "runner_id": str(runner_id),
#                 "ltp": None,
#                 "prev_ltp": None,
#                 "tv": None,
#                 "updated_at": None,
#             }

#         chosen = None
#         for tick in market_ticks:
#             if str(tick.get("market_id")) != str(market_id):
#                 continue
#             if str(tick.get("runner_id")) != str(runner_id):
#                 continue

#             tick_ts = tick.get("timestamp")
#             if tick_ts is None:
#                 continue

#             if ball_timestamp is None:
#                 chosen = tick
#             elif tick_ts <= ball_timestamp:
#                 chosen = tick
#             else:
#                 break

#         if not chosen:
#             chosen = market_ticks[-1]

#         return {
#             "market_id": str(chosen.get("market_id") or market_id),
#             "runner_id": str(chosen.get("runner_id") or runner_id),
#             "ltp": chosen.get("ltp"),
#             "prev_ltp": chosen.get("prev_ltp"),
#             "tv": chosen.get("tv"),
#             "updated_at": chosen.get("timestamp"),
#         }

#     def resolve_phase(self, overs, innings):
#         try:
#             overs = float(overs or 0)
#         except Exception:
#             overs = 0

#         if overs <= 6:
#             return "powerplay"
#         if overs <= 15:
#             return "middle"
#         return "death"




# import time
# from django.core.management.base import BaseCommand
# from django.utils import timezone

# from betapp.live_signal_engine import run_live_prediction
# from betapp.redis_price import get_all_market_prices, get_latest_price
# from betapp.services.live_market_tick_service import save_live_market_tick
# from betapp.services.market_metadata_service import get_market_metadata
# from betapp.archive_runtime import combined_csv_archive


# class Command(BaseCommand):
#     help = "Run live signal prediction loop, backfill history, save market rows, and save combined CSV history"

#     def add_arguments(self, parser):
#         parser.add_argument("--match_id", type=str, required=True)
#         parser.add_argument("--market_id", type=str, required=True)

#         parser.add_argument(
#             "--runner_ids",
#             type=str,
#             required=False,
#             default=None,
#             help="Comma-separated runner ids, example: 7671296,22121561",
#         )

#         parser.add_argument(
#             "--auto_select_runner",
#             action="store_true",
#             help="If runner_ids are not provided, automatically select all live runners for this market",
#         )

#         parser.add_argument("--interval", type=float, default=2.0)

#     def handle(self, *args, **options):
#         match_id = str(options["match_id"])
#         market_id = str(options["market_id"])
#         runner_ids_raw = options["runner_ids"]
#         auto_select_runner = options["auto_select_runner"]
#         interval = options["interval"]

#         prev_ltp_map = {}
#         history_backfilled = set()

#         runner_ids = []
#         if runner_ids_raw:
#             runner_ids = [r.strip() for r in str(runner_ids_raw).split(",") if r.strip()]

#         if not runner_ids:
#             prices = get_all_market_prices(market_id)

#             if not prices:
#                 self.stdout.write(self.style.ERROR(
#                     f"No live runner prices found for market_id={market_id}. "
#                     "Start run_market_ws and verify the market subscription."
#                 ))
#                 return

#             if auto_select_runner:
#                 runner_ids = [str(p["runner_id"]) for p in prices]
#                 self.stdout.write(self.style.SUCCESS(
#                     f"Auto-selected live runner_ids for market_id={market_id}: {', '.join(runner_ids)}"
#                 ))
#             elif len(prices) == 1:
#                 runner_ids = [str(prices[0]["runner_id"])]
#                 self.stdout.write(self.style.SUCCESS(
#                     f"Auto-selected single runner_id={runner_ids[0]} for market_id={market_id}"
#                 ))
#             else:
#                 available_runner_ids = ", ".join(str(p["runner_id"]) for p in prices)
#                 self.stdout.write(self.style.ERROR(
#                     f"Multiple live runners available for market_id={market_id}: {available_runner_ids}. "
#                     "Provide --runner_ids or use --auto_select_runner."
#                 ))
#                 return

#         self.stdout.write(self.style.SUCCESS("Live signal loop started"))
#         self.stdout.write(f"Using match_id={match_id}")
#         self.stdout.write(f"Using market_id={market_id}")
#         self.stdout.write(f"Using runner_ids={', '.join(runner_ids)}")

#         for preload_runner_id in runner_ids:
#             csv_path = combined_csv_archive.get_csv_path(match_id, market_id, preload_runner_id)
#             combined_csv_archive.preload_existing_keys(csv_path)

#         while True:
#             try:
#                 for runner_id in runner_ids:
#                     price = get_latest_price(market_id, runner_id)

#                     if not price:
#                         self.stdout.write(self.style.WARNING(
#                             f"No price found in Redis for market_id={market_id}, runner_id={runner_id}"
#                         ))
#                         continue

#                     ltp = price.get("ltp")
#                     tv = price.get("tv")
#                     prev_ltp = prev_ltp_map.get(str(runner_id))

#                     if prev_ltp is None:
#                         prev_ltp = price.get("prev_ltp")

#                     result = run_live_prediction(match_id, market_id, runner_id)
#                     self.stdout.write(f"[Prediction] runner_id={runner_id} => {result}")

#                     runner_history_key = f"{match_id}:{market_id}:{runner_id}"
#                     if runner_history_key not in history_backfilled:
#                         self.backfill_history_rows(result, match_id, market_id, runner_id)
#                         history_backfilled.add(runner_history_key)

#                     if isinstance(result, dict) and result.get("ball_key"):
#                         csv_saved = combined_csv_archive.save_combined_row(result)

#                         if csv_saved:
#                             self.stdout.write(self.style.SUCCESS(
#                                 f"[CSV COMBINED] saved live ball_key={result.get('ball_key')} "
#                                 f"market_id={result.get('market_id')} "
#                                 f"runner_id={result.get('runner_id')}"
#                             ))
#                         else:
#                             self.stdout.write(
#                                 f"[CSV COMBINED] skipped duplicate or invalid live row for runner_id={runner_id}"
#                             )
#                     else:
#                         self.stdout.write(self.style.WARNING(
#                             f"[CSV COMBINED] result missing ball_key for runner_id={runner_id}"
#                         ))

#                     metadata = get_market_metadata(market_id, runner_id)
#                     now = timezone.now()

#                     payload = {
#                         "market_id": str(market_id),
#                         "event_id": metadata.get("event_id"),
#                         "event_name": metadata.get("event_name"),
#                         "market_type": metadata.get("market_type"),
#                         "market_time": metadata.get("market_time"),
#                         "runner_id": str(runner_id),
#                         "runner_name": metadata.get("runner_name"),
#                         "publish_time_ms": int(now.timestamp() * 1000),
#                         "publish_time_utc": now,
#                         "ltp": ltp,
#                         "prev_ltp": prev_ltp,
#                         "tv": tv,
#                         "market_status": "OPEN",
#                         "in_play": True,
#                         "bet_delay": 0,
#                         "winner": None,
#                         "settled_time": None,
#                         "year": now.year,
#                         "month": now.month,
#                     }

#                     save_live_market_tick(payload)

#                     self.stdout.write(self.style.SUCCESS(
#                         f"[LiveMarketTick] saved market_id={market_id} "
#                         f"runner_id={runner_id} "
#                         f"runner_name={metadata.get('runner_name')} "
#                         f"event_name={metadata.get('event_name')} "
#                         f"ltp={ltp} tv={tv}"
#                     ))

#                     prev_ltp_map[str(runner_id)] = ltp

#             except Exception as e:
#                 self.stdout.write(self.style.ERROR(f"Error: {e}"))

#             time.sleep(interval)

#     def backfill_history_rows(self, result, match_id, market_id, runner_id):
#         if not isinstance(result, dict):
#             return

#         history = result.get("history", {}) or {}
#         balls = history.get("balls", []) or []
#         market_ticks = history.get("market", []) or []

#         if not balls:
#             self.stdout.write(self.style.WARNING(
#                 f"[CSV BACKFILL] no history balls available for runner_id={runner_id}"
#             ))
#             return

#         self.stdout.write(self.style.SUCCESS(
#             f"[CSV BACKFILL] found {len(balls)} history balls for runner_id={runner_id}"
#         ))

#         current_prediction = result.get("prediction", {}) or {}
#         current_cricket = result.get("cricket", {}) or {}

#         for ball in balls:
#             nearest_market = self.find_nearest_market_tick(
#                 ball_timestamp=ball.get("timestamp"),
#                 market_ticks=market_ticks,
#                 market_id=market_id,
#                 runner_id=runner_id,
#             )

#             cricket_payload = {
#                 "source_match_id": str(match_id),
#                 "innings": ball.get("innings"),
#                 "score": f"{ball.get('score')}/{ball.get('wickets')}",
#                 "score_num": ball.get("score"),
#                 "wickets": ball.get("wickets"),
#                 "overs": str(ball.get("overs")),
#                 "overs_float": ball.get("overs"),
#                 "crr": ball.get("current_run_rate"),
#                 "rrr": ball.get("required_run_rate"),
#                 "status": current_cricket.get("status"),
#                 "state": current_cricket.get("state"),
#                 "toss": current_cricket.get("toss"),
#                 "target": current_cricket.get("target"),
#                 "phase": self.resolve_phase(ball.get("overs"), ball.get("innings")),
#                 "innings_type": current_cricket.get("innings_type"),
#                 "recent": ball.get("recent"),
#                 "last5_runs": current_cricket.get("last5_runs"),
#                 "last5_wkts": current_cricket.get("last5_wkts"),
#                 "last3_runs": current_cricket.get("last3_runs"),
#                 "latest_ball": ball.get("commentary"),
#                 "b1_name": (ball.get("batter_striker") or {}).get("name"),
#                 "b1_runs": (ball.get("batter_striker") or {}).get("runs"),
#                 "b1_balls": (ball.get("batter_striker") or {}).get("balls"),
#                 "b1_4s": (ball.get("batter_striker") or {}).get("fours"),
#                 "b1_6s": (ball.get("batter_striker") or {}).get("sixes"),
#                 "b1_sr": (ball.get("batter_striker") or {}).get("strike_rate"),
#                 "b2_name": (ball.get("batter_non_striker") or {}).get("name"),
#                 "b2_runs": (ball.get("batter_non_striker") or {}).get("runs"),
#                 "b2_balls": (ball.get("batter_non_striker") or {}).get("balls"),
#                 "b2_4s": (ball.get("batter_non_striker") or {}).get("fours"),
#                 "b2_6s": (ball.get("batter_non_striker") or {}).get("sixes"),
#                 "b2_sr": (ball.get("batter_non_striker") or {}).get("strike_rate"),
#                 "bw1_name": (ball.get("bowler") or {}).get("name"),
#                 "bw1_overs": (ball.get("bowler") or {}).get("overs"),
#                 "bw1_runs": (ball.get("bowler") or {}).get("runs"),
#                 "bw1_wkts": (ball.get("bowler") or {}).get("wickets"),
#                 "bw1_eco": (ball.get("bowler") or {}).get("economy"),
#                 "p_runs": (ball.get("partnership") or {}).get("runs"),
#                 "p_balls": (ball.get("partnership") or {}).get("balls"),
#                 "raw_json": {},
#             }

#             combined_row = {
#                 "source_match_id": str(match_id),
#                 "market_id": str(market_id),
#                 "runner_id": str(runner_id),
#                 "ball_key": ball.get("ball_key"),
#                 "cricket": cricket_payload,
#                 "price": nearest_market,
#                 "prediction": current_prediction,
#             }

#             csv_saved = combined_csv_archive.save_combined_row(combined_row)
#             if csv_saved:
#                 self.stdout.write(self.style.SUCCESS(
#                     f"[CSV BACKFILL] saved ball_key={ball.get('ball_key')}"
#                 ))

#     def find_nearest_market_tick(self, ball_timestamp, market_ticks, market_id, runner_id):
#         if not market_ticks:
#             return {
#                 "market_id": str(market_id),
#                 "runner_id": str(runner_id),
#                 "ltp": None,
#                 "prev_ltp": None,
#                 "tv": None,
#                 "updated_at": None,
#             }

#         chosen = None
#         for tick in market_ticks:
#             if str(tick.get("market_id")) != str(market_id):
#                 continue
#             if str(tick.get("runner_id")) != str(runner_id):
#                 continue

#             tick_ts = tick.get("timestamp")
#             if tick_ts is None:
#                 continue

#             if ball_timestamp is None:
#                 chosen = tick
#             elif tick_ts <= ball_timestamp:
#                 chosen = tick
#             else:
#                 break

#         if not chosen:
#             chosen = market_ticks[-1]

#         return {
#             "market_id": str(chosen.get("market_id") or market_id),
#             "runner_id": str(chosen.get("runner_id") or runner_id),
#             "ltp": chosen.get("ltp"),
#             "prev_ltp": chosen.get("prev_ltp"),
#             "tv": chosen.get("tv"),
#             "updated_at": chosen.get("timestamp"),
#         }

#     def resolve_phase(self, overs, innings):
#         try:
#             overs = float(overs or 0)
#         except Exception:
#             overs = 0

#         if overs <= 6:
#             return "powerplay"
#         if overs <= 15:
#             return "middle"
#         return "death"


import time
from django.core.management.base import BaseCommand
from django.utils import timezone

from betapp.live_signal_engine import run_live_prediction
from betapp.redis_price import get_all_market_prices, get_latest_price
from betapp.services.live_market_tick_service import save_live_market_tick
from betapp.services.market_metadata_service import get_market_metadata
from betapp.archive_runtime import combined_csv_archive


class Command(BaseCommand):
    help = "Run live signal prediction loop, backfill history, save market rows, and save combined CSV history"

    def add_arguments(self, parser):
        parser.add_argument("--match_id", type=str, required=True)
        parser.add_argument("--market_id", type=str, required=True)

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
        prev_ball_key_map = {}
        history_backfilled = set()

        runner_ids = []
        if runner_ids_raw:
            runner_ids = [r.strip() for r in str(runner_ids_raw).split(",") if r.strip()]

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

        for preload_runner_id in runner_ids:
            csv_path = combined_csv_archive.get_csv_path(match_id, market_id, preload_runner_id)
            combined_csv_archive.preload_existing_keys(csv_path)

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

                    runner_history_key = f"{match_id}:{market_id}:{runner_id}"
                    if runner_history_key not in history_backfilled:
                        self.backfill_history_rows(result, match_id, market_id, runner_id)
                        history_backfilled.add(runner_history_key)

                    if isinstance(result, dict) and result.get("ball_key"):
                        csv_saved = combined_csv_archive.save_combined_row(result)

                        if csv_saved:
                            self.stdout.write(self.style.SUCCESS(
                                f"[CSV COMBINED] saved live ball_key={result.get('ball_key')} "
                                f"market_id={result.get('market_id')} "
                                f"runner_id={result.get('runner_id')}"
                            ))
                        else:
                            self.stdout.write(
                                f"[CSV COMBINED] skipped duplicate or invalid live row for runner_id={runner_id}"
                            )
                    else:
                        self.stdout.write(self.style.WARNING(
                            f"[CSV COMBINED] result missing ball_key for runner_id={runner_id}"
                        ))

                    current_ball_key = result.get("ball_key") if isinstance(result, dict) else None
                    last_ball_key = prev_ball_key_map.get(str(runner_id))

                    if current_ball_key and current_ball_key != last_ball_key:
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
                        prev_ball_key_map[str(runner_id)] = current_ball_key

                        self.stdout.write(self.style.SUCCESS(
                            f"[LiveMarketTick] saved ball_key={current_ball_key} "
                            f"market_id={market_id} runner_id={runner_id} "
                            f"runner_name={metadata.get('runner_name')} "
                            f"ltp={ltp} tv={tv}"
                        ))
                    else:
                        self.stdout.write(
                            f"[LiveMarketTick] skipped — same ball_key={current_ball_key} runner_id={runner_id}"
                        )

                    prev_ltp_map[str(runner_id)] = ltp

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error: {e}"))

            time.sleep(interval)

    def backfill_history_rows(self, result, match_id, market_id, runner_id):
        if not isinstance(result, dict):
            return

        history = result.get("history", {}) or {}
        balls = history.get("balls", []) or []
        market_ticks = history.get("market", []) or []

        if not balls:
            self.stdout.write(self.style.WARNING(
                f"[CSV BACKFILL] no history balls available for runner_id={runner_id}"
            ))
            return

        self.stdout.write(self.style.SUCCESS(
            f"[CSV BACKFILL] found {len(balls)} history balls for runner_id={runner_id}"
        ))

        current_prediction = result.get("prediction", {}) or {}
        current_cricket = result.get("cricket", {}) or {}

        for ball in balls:
            nearest_market = self.find_nearest_market_tick(
                ball_timestamp=ball.get("timestamp"),
                market_ticks=market_ticks,
                market_id=market_id,
                runner_id=runner_id,
            )

            cricket_payload = {
                "source_match_id": str(match_id),
                "innings": ball.get("innings"),
                "score": f"{ball.get('score')}/{ball.get('wickets')}",
                "score_num": ball.get("score"),
                "wickets": ball.get("wickets"),
                "overs": str(ball.get("overs")),
                "overs_float": ball.get("overs"),
                "crr": ball.get("current_run_rate"),
                "rrr": ball.get("required_run_rate"),
                "status": current_cricket.get("status"),
                "state": current_cricket.get("state"),
                "toss": current_cricket.get("toss"),
                "target": current_cricket.get("target"),
                "phase": self.resolve_phase(ball.get("overs"), ball.get("innings")),
                "innings_type": current_cricket.get("innings_type"),
                "recent": ball.get("recent"),
                "last5_runs": current_cricket.get("last5_runs"),
                "last5_wkts": current_cricket.get("last5_wkts"),
                "last3_runs": current_cricket.get("last3_runs"),
                "latest_ball": ball.get("commentary"),
                "b1_name": (ball.get("batter_striker") or {}).get("name"),
                "b1_runs": (ball.get("batter_striker") or {}).get("runs"),
                "b1_balls": (ball.get("batter_striker") or {}).get("balls"),
                "b1_4s": (ball.get("batter_striker") or {}).get("fours"),
                "b1_6s": (ball.get("batter_striker") or {}).get("sixes"),
                "b1_sr": (ball.get("batter_striker") or {}).get("strike_rate"),
                "b2_name": (ball.get("batter_non_striker") or {}).get("name"),
                "b2_runs": (ball.get("batter_non_striker") or {}).get("runs"),
                "b2_balls": (ball.get("batter_non_striker") or {}).get("balls"),
                "b2_4s": (ball.get("batter_non_striker") or {}).get("fours"),
                "b2_6s": (ball.get("batter_non_striker") or {}).get("sixes"),
                "b2_sr": (ball.get("batter_non_striker") or {}).get("strike_rate"),
                "bw1_name": (ball.get("bowler") or {}).get("name"),
                "bw1_overs": (ball.get("bowler") or {}).get("overs"),
                "bw1_runs": (ball.get("bowler") or {}).get("runs"),
                "bw1_wkts": (ball.get("bowler") or {}).get("wickets"),
                "bw1_eco": (ball.get("bowler") or {}).get("economy"),
                "p_runs": (ball.get("partnership") or {}).get("runs"),
                "p_balls": (ball.get("partnership") or {}).get("balls"),
                "raw_json": {},
            }

            combined_row = {
                "source_match_id": str(match_id),
                "market_id": str(market_id),
                "runner_id": str(runner_id),
                "ball_key": ball.get("ball_key"),
                "cricket": cricket_payload,
                "price": nearest_market,
                "prediction": current_prediction,
            }

            csv_saved = combined_csv_archive.save_combined_row(combined_row)
            if csv_saved:
                self.stdout.write(self.style.SUCCESS(
                    f"[CSV BACKFILL] saved ball_key={ball.get('ball_key')}"
                ))

    def find_nearest_market_tick(self, ball_timestamp, market_ticks, market_id, runner_id):
        if not market_ticks:
            return {
                "market_id": str(market_id),
                "runner_id": str(runner_id),
                "ltp": None,
                "prev_ltp": None,
                "tv": None,
                "updated_at": None,
            }

        chosen = None
        for tick in market_ticks:
            if str(tick.get("market_id")) != str(market_id):
                continue
            if str(tick.get("runner_id")) != str(runner_id):
                continue

            tick_ts = tick.get("timestamp")
            if tick_ts is None:
                continue

            if ball_timestamp is None:
                chosen = tick
            elif tick_ts <= ball_timestamp:
                chosen = tick
            else:
                break

        if not chosen:
            chosen = market_ticks[-1]

        return {
            "market_id": str(chosen.get("market_id") or market_id),
            "runner_id": str(chosen.get("runner_id") or runner_id),
            "ltp": chosen.get("ltp"),
            "prev_ltp": chosen.get("prev_ltp"),
            "tv": chosen.get("tv"),
            "updated_at": chosen.get("timestamp"),
        }

    def resolve_phase(self, overs, innings):
        try:
            overs = float(overs or 0)
        except Exception:
            overs = 0

        if overs <= 6:
            return "powerplay"
        if overs <= 15:
            return "middle"
        return "death"