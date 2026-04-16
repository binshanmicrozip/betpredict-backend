import time
from django.core.management.base import BaseCommand
from betapp.live_signal_engine import run_live_prediction


class Command(BaseCommand):
    help = "Run live signal prediction loop"

    def add_arguments(self, parser):
        parser.add_argument("--source_match_id", type=str, required=True)
        parser.add_argument("--market_id", type=str, required=True)
        parser.add_argument("--runner_id", type=str, required=True)
        parser.add_argument("--interval", type=float, default=2.0)

    def handle(self, *args, **options):
        source_match_id = options["source_match_id"]
        market_id = options["market_id"]
        runner_id = options["runner_id"]
        interval = options["interval"]

        self.stdout.write(self.style.SUCCESS("Live signal loop started"))

        while True:
            try:
                result = run_live_prediction(source_match_id, market_id, runner_id)
                self.stdout.write(str(result))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error: {e}"))

            time.sleep(interval)