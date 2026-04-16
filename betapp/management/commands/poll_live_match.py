import time
from django.core.management.base import BaseCommand
from betapp.cricbuzz_live import fetch_score, parse_live_data
from betapp.redis_cricket import save_latest_cricket
from betapp.services.live_ingest import save_live_snapshot, save_live_commentary


class Command(BaseCommand):
    help = "Poll Cricbuzz live match and save live data"

    def add_arguments(self, parser):
        parser.add_argument("--match-id", required=True, help="Database match id")
        parser.add_argument("--source-match-id", required=True, help="Cricbuzz match id")
        parser.add_argument("--interval", type=int, default=10)
        parser.add_argument("--once", action="store_true")

    def handle(self, *args, **options):
        match_id = options["match_id"]
        source_match_id = options["source_match_id"]
        interval = options["interval"]
        once = options["once"]

        while True:
            raw = fetch_score(source_match_id)

            if raw:
                parsed = parse_live_data(raw)

                save_live_snapshot(
                    match_id=match_id,
                    source_match_id=source_match_id,
                    parsed_data=parsed,
                    raw_json=raw,
                )

                saved_count = save_live_commentary(
                    match_id=match_id,
                    source_match_id=source_match_id,
                    raw_json=raw,
                    parsed_data=parsed,
                )

                save_latest_cricket(source_match_id, parsed)

                self.stdout.write(
                    self.style.SUCCESS(
                        f"Saved live match={match_id}, commentary_rows={saved_count}"
                    )
                )
            else:
                self.stdout.write(self.style.WARNING("No live data fetched"))

            if once:
                break

            time.sleep(interval)