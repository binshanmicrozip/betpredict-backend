from django.core.management.base import BaseCommand
from django.db.models import Q

from betapp.models import Player
from betapp.services.player_profile_updater import PlayerProfileUpdater


class Command(BaseCommand):
    help = "Update Cricbuzz profile details for IPL 2026 players"

    def add_arguments(self, parser):
        parser.add_argument("--player_id", type=str, help="Update one player by player_id")
        parser.add_argument("--player_name", type=str, help="Update one player by player_name")
        parser.add_argument("--season", type=int, default=2026, help="Season filter")
        parser.add_argument("--force", action="store_true", help="Force overwrite existing fields")
        parser.add_argument("--limit", type=int, default=None, help="Limit number of players")

    def handle(self, *args, **options):
        player_id = options.get("player_id")
        player_name = options.get("player_name")
        season = options.get("season", 2026)
        force = options.get("force", False)
        limit = options.get("limit")

        updater = PlayerProfileUpdater()

        queryset = (
            Player.objects
            .filter(
                Q(match_players__match__season=season) |
                Q(ipl_teams__season=season)
            )
            .distinct()
            .order_by("player_name")
        )

        if player_id:
            queryset = queryset.filter(player_id=player_id)

        if player_name:
            queryset = queryset.filter(player_name__iexact=player_name)

        if limit:
            queryset = queryset[:limit]

        players = list(queryset)
        total = len(players)

        self.stdout.write(self.style.SUCCESS(f"Found {total} IPL {season} player(s)"))

        updated_count = 0
        no_change_count = 0
        error_count = 0

        for idx, player in enumerate(players, start=1):
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(f"[{idx}/{total}] PLAYER: {player.player_name} ({player.player_id})")
            self.stdout.write(
                f"Before => normalized_name={player.normalized_name}, country={player.country}, role={player.role}, "
                f"ipl_debut={player.ipl_debut}, debut_year={player.debut_year}, "
                f"last_season={player.last_season}, cricbuzz_id={player.cricbuzz_profile_id}"
            )

            try:
                result = updater.update_player(player, force=force)

                self.stdout.write(f"Status => {result['status']}")
                self.stdout.write(f"Updated fields => {result['updated_fields']}")
                self.stdout.write(
                    f"After => normalized_name={player.normalized_name}, country={result['country']}, role={result['role']}, "
                    f"ipl_debut={result['ipl_debut']}, debut_year={result['debut_year']}, "
                    f"last_season={result['last_season']}, cricbuzz_id={result['cricbuzz_profile_id']}"
                )

                if result["status"] == "updated":
                    updated_count += 1
                else:
                    no_change_count += 1

            except Exception as exc:
                error_count += 1
                self.stdout.write(self.style.ERROR(f"Error => {exc}"))

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("DONE"))
        self.stdout.write(f"Updated: {updated_count}")
        self.stdout.write(f"No change: {no_change_count}")
        self.stdout.write(f"Errors: {error_count}")