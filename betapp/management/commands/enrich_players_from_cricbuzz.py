from django.core.management.base import BaseCommand

from betapp.models import Player
from betapp.services.cricbuzz_enricher import update_player_from_cricbuzz
from betapp.services.cricbuzz_fetcher import fetch_player_from_cricbuzz


class Command(BaseCommand):
    help = "Enrich player fields from Cricbuzz"

    def add_arguments(self, parser):
        parser.add_argument("--only-missing", action="store_true")

    def handle(self, *args, **options):
        only_missing = options["only_missing"]

        qs = Player.objects.all().order_by("player_name")

        if only_missing:
            qs = qs.filter(cricbuzz_profile_id__isnull=True)

        total = qs.count()
        updated = 0
        skipped = 0
        failed = 0

        self.stdout.write(f"Total players to enrich: {total}")

        for player in qs:
            try:
                payload = fetch_player_from_cricbuzz(player.player_name)

                if not payload:
                    skipped += 1
                    self.stdout.write(self.style.WARNING(f"Skipped: {player.player_name} (no payload)"))
                    continue

                changed = update_player_from_cricbuzz(player, payload)

                if changed:
                    updated += 1
                    self.stdout.write(self.style.SUCCESS(f"Updated: {player.player_name}"))
                else:
                    skipped += 1
                    self.stdout.write(f"Skipped: {player.player_name} (already filled)")

            except Exception as e:
                failed += 1
                self.stdout.write(self.style.ERROR(f"Failed: {player.player_name} -> {e}"))

        self.stdout.write(self.style.SUCCESS(
            f"Done. updated={updated}, skipped={skipped}, failed={failed}"
        ))