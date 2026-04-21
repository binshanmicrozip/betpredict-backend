from django.core.management.base import BaseCommand

from betapp.redis_price import get_all_market_prices


class Command(BaseCommand):
    help = "List live runner IDs and prices for a subscribed market"

    def add_arguments(self, parser):
        parser.add_argument(
            "--market_id",
            type=str,
            required=True,
            help="Live market id to inspect, e.g. 1.256693141"
        )

    def handle(self, *args, **options):
        market_id = options["market_id"]
        prices = get_all_market_prices(market_id)

        if not prices:
            self.stdout.write(self.style.ERROR(
                f"No live runners found for market_id={market_id}. "
                "Make sure run_market_ws is running and subscribed to this market."
            ))
            return

        self.stdout.write(self.style.SUCCESS(
            f"Found {len(prices)} live runner(s) for market_id={market_id}")
        )
        self.stdout.write("-")
        for price in prices:
            self.stdout.write(
                f"runner_id={price.get('runner_id')} "
                f"ltp={price.get('ltp')} prev_ltp={price.get('prev_ltp')} tv={price.get('tv')}"
            )
