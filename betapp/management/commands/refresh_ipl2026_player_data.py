import operator
from functools import reduce

from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.db.models import Q

from betapp.models import IPLMatch, MatchPlayer, Player
from betapp.services.player_profile_updater import PlayerProfileUpdater


COMPLETED_STATUS_KEYWORDS = [
    "won by",
    "match tied",
    "no result",
    "abandoned",
    "complete",
    "completed",
]


class Command(BaseCommand):
    help = "Refresh Cricbuzz profile details and rebuild player stats for IPL 2026 players when matches end."

    def add_arguments(self, parser):
        parser.add_argument(
            "--season",
            type=int,
            default=2026,
            help="IPL season to refresh (default: 2026)",
        )
        parser.add_argument(
            "--match_id",
            type=str,
            help="Specific match ID to refresh player data for",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Force overwrite existing profile fields from Cricbuzz",
        )
        parser.add_argument(
            "--rebuild-stats",
            action="store_true",
            help="Rebuild player stats after refreshing profiles",
        )
        parser.add_argument(
            "--clear-stats-first",
            action="store_true",
            help="Clear old stats before rebuilding stats",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Limit number of players to refresh",
        )

    def handle(self, *args, **options):
        season = options["season"]
        match_id = options.get("match_id")
        force = options.get("force", False)
        rebuild_stats = options.get("rebuild_stats", False)
        clear_stats_first = options.get("clear_stats_first", False)
        limit = options.get("limit")

        updater = PlayerProfileUpdater()

        if match_id:
            matches = IPLMatch.objects.filter(match_id=match_id, season=season)
        else:
            status_filters = [Q(status__icontains=kw) for kw in COMPLETED_STATUS_KEYWORDS]
            matches = IPLMatch.objects.filter(season=season).filter(
                reduce(operator.or_, status_filters)
            )

        match_count = matches.count()
        if match_count == 0 and not match_id:
            self.stdout.write(self.style.WARNING(
                f"No completed IPL {season} matches found by status keywords. "
                "Falling back to all season player data."
            ))
            players = Player.objects.filter(
                Q(ipl_teams__season=season) | Q(match_players__match__season=season)
            ).distinct().order_by("player_name")
        else:
            match_ids = list(matches.values_list("match_id", flat=True))
            self.stdout.write(self.style.SUCCESS(
                f"Refreshing players for {match_count} match(es): {', '.join(match_ids)}"
            ))
            player_ids = (
                MatchPlayer.objects.filter(match__in=matches)
                .values_list("player_id", flat=True)
                .distinct()
            )
            players = Player.objects.filter(player_id__in=player_ids).order_by("player_name")

        if limit:
            players = players[:limit]

        total = players.count()
        self.stdout.write(self.style.SUCCESS(f"Found {total} player(s) to refresh."))

        updated_count = 0
        no_change_count = 0
        error_count = 0

        for idx, player in enumerate(players, start=1):
            self.stdout.write(
                f"[{idx}/{total}] Refreshing {player.player_name} ({player.player_id})"
            )
            try:
                result = updater.update_player(player, force=force)
                if result["status"] == "updated":
                    updated_count += 1
                else:
                    no_change_count += 1
            except Exception as exc:
                error_count += 1
                self.stdout.write(self.style.ERROR(
                    f"Failed to update {player.player_name}: {exc}"
                ))

        self.stdout.write(self.style.SUCCESS(
            f"Profile refresh complete: updated={updated_count}, no_change={no_change_count}, errors={error_count}"
        ))

        if rebuild_stats:
            call_args = {}
            if clear_stats_first:
                call_args["clear_first"] = True

            self.stdout.write(self.style.SUCCESS("Rebuilding player stats..."))
            call_command("rebuild_player_stats", **call_args)
            self.stdout.write(self.style.SUCCESS("Player stats rebuild completed."))
