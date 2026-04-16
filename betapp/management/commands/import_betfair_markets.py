import pandas as pd
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.dateparse import parse_datetime

from betapp.models import Market, Runner, PriceTick


DUBAI_TZ = ZoneInfo("Asia/Dubai")


class Command(BaseCommand):
    help = "Import filtered IPL Betfair CSV into markets, runners, and price_ticks"

    def add_arguments(self, parser):
        parser.add_argument(
            "--file",
            type=str,
            required=True,
            help="Full path to betfair_ipl_only.csv"
        )
        parser.add_argument(
            "--clear-old",
            action="store_true",
            help="Delete old markets, runners, and price_ticks before import"
        )

    def handle(self, *args, **options):
        file_path = options["file"]
        clear_old = options["clear_old"]

        self.stdout.write(f"Reading CSV: {file_path}")

        df = pd.read_csv(file_path, low_memory=False)

        if df.empty:
            self.stdout.write(self.style.ERROR("CSV is empty"))
            return

        # Parse timestamps
        df["tick_time"] = pd.to_datetime(df["publish_time_utc"], utc=True, errors="coerce")
        df["market_start_time"] = pd.to_datetime(df["market_time"], utc=True, errors="coerce")

        # Convert to Dubai timezone for human-readable local date fields
        df["tick_time_dubai"] = df["tick_time"].dt.tz_convert(DUBAI_TZ)
        df["market_start_time_dubai"] = df["market_start_time"].dt.tz_convert(DUBAI_TZ)

        df = df.dropna(subset=["market_id", "runner_id", "tick_time", "ltp"])

        self.stdout.write(f"Rows after cleaning: {len(df)}")

        with transaction.atomic():
            if clear_old:
                self.stdout.write("Deleting old price ticks...")
                PriceTick.objects.all().delete()

                self.stdout.write("Deleting old runners...")
                Runner.objects.all().delete()

                self.stdout.write("Deleting old markets...")
                Market.objects.all().delete()

            self.load_markets(df)
            self.load_runners(df)
            self.load_price_ticks(df)

        self.stdout.write(self.style.SUCCESS("✅ IPL data imported successfully"))

    def load_markets(self, df: pd.DataFrame):
        self.stdout.write("Loading markets...")

        market_rows = (
            df.groupby("market_id", as_index=False)
              .agg(
                  event_id=("event_id", "first"),
                  event_name=("event_name", "first"),
                  market_type=("market_type", "first"),
                  market_start_time=("market_start_time", "first"),
                  status=("market_status", "last"),
                  bet_delay=("bet_delay", "max"),
                  settled_time=("settled_time", "last"),
                  total_tick_messages=("market_id", "size"),
                  in_play_seen=("in_play", "max"),
              )
        )

        for _, row in market_rows.iterrows():
            market_start_dt = row["market_start_time"].to_pydatetime() if pd.notna(row["market_start_time"]) else None
            settled_dt = parse_datetime(str(row["settled_time"])) if pd.notna(row["settled_time"]) else None

            Market.objects.update_or_create(
                market_id=str(row["market_id"]),
                defaults={
                    "event_id": str(row["event_id"]) if pd.notna(row["event_id"]) else "",
                    "event_name": row["event_name"] if pd.notna(row["event_name"]) else "",
                    "market_name": row["event_name"] if pd.notna(row["event_name"]) else "",
                    "market_type": row["market_type"] if pd.notna(row["market_type"]) else "",
                    "event_type_id": "4",
                    "market_start_time": market_start_dt,
                    "settled_time": settled_dt,
                    "bet_delay": int(row["bet_delay"]) if pd.notna(row["bet_delay"]) else 0,
                    "status": row["status"] if pd.notna(row["status"]) else "OPEN",
                    "in_play_seen": bool(row["in_play_seen"]) if pd.notna(row["in_play_seen"]) else False,
                    "total_tick_messages": int(row["total_tick_messages"]),
                }
            )

        self.stdout.write(self.style.SUCCESS(f"Loaded {len(market_rows)} markets"))

    def load_runners(self, df: pd.DataFrame):
        self.stdout.write("Loading runners...")

        runner_rows = (
            df.sort_values(["market_id", "runner_id", "tick_time"])
              .groupby(["market_id", "runner_id"], as_index=False)
              .agg(
                  runner_name=("runner_name", "first"),
                  opening_price=("ltp", "first"),
                  closing_price=("ltp", "last"),
                  lowest_price=("ltp", "min"),
                  highest_price=("ltp", "max"),
                  total_ticks=("ltp", "size"),
                  pre_match_ticks=("in_play", lambda s: int((s == 0).sum())),
                  in_play_ticks=("in_play", lambda s: int((s == 1).sum())),
                  first_tick_time=("tick_time", "first"),
                  last_tick_time=("tick_time", "last"),
              )
        )

        for _, row in runner_rows.iterrows():
            market = Market.objects.get(market_id=str(row["market_id"]))

            opening_price = Decimal(str(row["opening_price"]))
            closing_price = Decimal(str(row["closing_price"]))
            lowest_price = Decimal(str(row["lowest_price"]))
            highest_price = Decimal(str(row["highest_price"]))

            first_tick_dt = row["first_tick_time"].to_pydatetime() if pd.notna(row["first_tick_time"]) else None
            last_tick_dt = row["last_tick_time"].to_pydatetime() if pd.notna(row["last_tick_time"]) else None

            Runner.objects.update_or_create(
                market=market,
                selection_id=int(row["runner_id"]),
                defaults={
                    "runner_name": row["runner_name"] if pd.notna(row["runner_name"]) else str(row["runner_id"]),
                    "status": "ACTIVE",
                    "opening_price": opening_price,
                    "closing_price": closing_price,
                    "lowest_price": lowest_price,
                    "highest_price": highest_price,
                    "price_range": highest_price - lowest_price,
                    "price_change": closing_price - opening_price,
                    "total_ticks": int(row["total_ticks"]),
                    "pre_match_ticks": int(row["pre_match_ticks"]),
                    "in_play_ticks": int(row["in_play_ticks"]),
                    "first_tick_time": first_tick_dt,
                    "last_tick_time": last_tick_dt,
                }
            )

        self.stdout.write(self.style.SUCCESS(f"Loaded {len(runner_rows)} runners"))

    def load_price_ticks(self, df: pd.DataFrame):
        self.stdout.write("Loading price ticks...")

        market_map = {
            m.market_id: m
            for m in Market.objects.filter(market_id__in=df["market_id"].astype(str).unique().tolist())
        }

        runner_qs = Runner.objects.filter(
            market_id__in=df["market_id"].astype(str).unique().tolist()
        )
        runner_map = {
            (r.market_id, r.selection_id): r
            for r in runner_qs
        }

        tick_objects = []

        for _, row in df.iterrows():
            market_id = str(row["market_id"])
            runner_id = int(row["runner_id"])

            market = market_map.get(market_id)
            runner = runner_map.get((market_id, runner_id))

            if not market or not runner:
                continue

            tick_dt_utc = row["tick_time"].to_pydatetime()
            tick_dt_dubai = row["tick_time_dubai"]

            ltp_val = Decimal(str(row["ltp"]))
            win_prob = None
            if pd.notna(row["ltp"]) and float(row["ltp"]) > 0:
                win_prob = Decimal(str(round(100 / float(row["ltp"]), 3)))

            tick_objects.append(
                PriceTick(
                    market=market,
                    runner=runner,
                    year=int(tick_dt_dubai.year),
                    month=int(tick_dt_dubai.month),
                    day=int(tick_dt_dubai.day),
                    snapshot=row["price_direction"] if pd.notna(row["price_direction"]) else "SAME",
                    tick_time=tick_dt_utc,
                    ltp=ltp_val,
                    win_prob=win_prob,
                    traded_volume=None,
                    phase="IN-PLAY" if int(row["in_play"]) == 1 else "PRE-PLAY",
                )
            )

            if len(tick_objects) >= 5000:
                PriceTick.objects.bulk_create(
                    tick_objects,
                    batch_size=5000,
                    ignore_conflicts=True
                )
                tick_objects = []

        if tick_objects:
            PriceTick.objects.bulk_create(
                tick_objects,
                batch_size=5000,
                ignore_conflicts=True
            )

        self.stdout.write(self.style.SUCCESS("Loaded price ticks"))