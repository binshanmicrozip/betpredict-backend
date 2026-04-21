from django.core.management.base import BaseCommand
from django.db import transaction

from betapp.models import (
    Player,
    MatchPlayer,
    Delivery,
    PlayerMatchBatting,
    PlayerMatchBowling,
    PlayerSituationStats,
)
from betapp.services.player_profile_updater import PlayerProfileUpdater
from betapp.utils.player_profile_utils import normalize_player_name

from decimal import Decimal

class Command(BaseCommand):
    help = "Repair all player rows, merge duplicates, refresh Cricbuzz profiles"

    def add_arguments(self, parser):
        parser.add_argument("--season", type=int, default=2026)
        parser.add_argument("--force-cricbuzz", action="store_true")
        parser.add_argument("--limit", type=int, default=None)

    def handle(self, *args, **options):
        season = options["season"]
        force_cricbuzz = options["force_cricbuzz"]
        limit = options["limit"]

        self.stdout.write(self.style.SUCCESS(f"Starting full player repair for season {season}"))

        self.fix_normalized_names(limit=limit)
        self.merge_duplicate_players()
        self.refresh_cricbuzz_profiles(season=season, force=force_cricbuzz, limit=limit)

        self.stdout.write(self.style.SUCCESS("Repair stage completed."))

    def fix_normalized_names(self, limit=None):
        self.stdout.write("Step 1: Fixing normalized_name values...")

        qs = Player.objects.all().order_by("player_name")
        if limit:
            qs = qs[:limit]

        updated = 0
        for player in qs:
            correct = normalize_player_name(player.player_name)
            if player.normalized_name != correct:
                player.normalized_name = correct
                player.save(update_fields=["normalized_name"])
                updated += 1

        self.stdout.write(self.style.SUCCESS(f"normalized_name fixed for {updated} players"))


    @transaction.atomic
    def merge_duplicate_players(self):
        self.stdout.write("Step 2: Merging duplicate players...")

        players = list(Player.objects.all().order_by("created_at", "player_name"))
        groups = {}

        for p in players:
            key = p.cricbuzz_profile_id or p.normalized_name or normalize_player_name(p.player_name)
            groups.setdefault(key, []).append(p)

        removed = 0

        for _, group in groups.items():
            if len(group) <= 1:
                continue

            keeper = self.select_best_player(group)
            duplicates = [p for p in group if p.player_id != keeper.player_id]

            for dup in duplicates:
                # Merge PlayerSituationStats safely
                for stat in PlayerSituationStats.objects.filter(player=dup):
                    keeper_stat = PlayerSituationStats.objects.filter(
                        player=keeper,
                        phase=stat.phase,
                        innings_type=stat.innings_type,
                    ).first()

                    if keeper_stat:
                        keeper_stat.matches_played = (keeper_stat.matches_played or 0) + (stat.matches_played or 0)
                        keeper_stat.runs = (keeper_stat.runs or 0) + (stat.runs or 0)
                        keeper_stat.balls = (keeper_stat.balls or 0) + (stat.balls or 0)
                        keeper_stat.boundary_count = (keeper_stat.boundary_count or 0) + (stat.boundary_count or 0)
                        keeper_stat.wickets_lost = (keeper_stat.wickets_lost or 0) + (stat.wickets_lost or 0)

                        if keeper_stat.balls:
                            keeper_stat.strike_rate = Decimal(str(round((keeper_stat.runs / keeper_stat.balls) * 100, 2)))
                            keeper_stat.boundary_pct = Decimal(str(round((keeper_stat.boundary_count / keeper_stat.balls) * 100, 2)))
                            keeper_stat.dismissal_rate = Decimal(str(round((keeper_stat.wickets_lost / keeper_stat.balls), 4)))
                        else:
                            keeper_stat.strike_rate = Decimal("0.00")
                            keeper_stat.boundary_pct = Decimal("0.00")
                            keeper_stat.dismissal_rate = Decimal("0.0000")

                        keeper_stat.save()
                        stat.delete()
                    else:
                        stat.player = keeper
                        stat.save()

                # Merge MatchPlayer safely
                for mp in MatchPlayer.objects.filter(player=dup):
                    exists = MatchPlayer.objects.filter(match=mp.match, player=keeper).exists()
                    if exists:
                        mp.delete()
                    else:
                        mp.player = keeper
                        mp.save()

                # Merge PlayerMatchBatting safely
                for bat in PlayerMatchBatting.objects.filter(player=dup):
                    exists = PlayerMatchBatting.objects.filter(
                        match=bat.match,
                        player=keeper,
                        innings=bat.innings,
                    ).exists()
                    if exists:
                        bat.delete()
                    else:
                        bat.player = keeper
                        bat.save()

                # Merge PlayerMatchBowling safely
                for bowl in PlayerMatchBowling.objects.filter(player=dup):
                    exists = PlayerMatchBowling.objects.filter(
                        match=bowl.match,
                        player=keeper,
                        innings=bowl.innings,
                    ).exists()
                    if exists:
                        bowl.delete()
                    else:
                        bowl.player = keeper
                        bowl.save()

                # Move Delivery rows
                Delivery.objects.filter(batter=dup).update(batter=keeper)
                Delivery.objects.filter(bowler=dup).update(bowler=keeper)
                Delivery.objects.filter(non_striker=dup).update(non_striker=keeper)
                Delivery.objects.filter(player_out=dup).update(player_out=keeper)

                dup.delete()
                removed += 1

        self.stdout.write(self.style.SUCCESS(f"Duplicate players merged: {removed}"))

    def select_best_player(self, players):
        def score(p):
            return (
                1 if p.cricbuzz_profile_id else 0,
                1 if p.country else 0,
                1 if p.role and p.role != "Unknown" else 0,
                1 if p.debut_year else 0,
                1 if p.ipl_debut else 0,
                1 if p.last_season else 0,
                p.total_runs or 0,
                p.wickets or 0,
            )
        return sorted(players, key=score, reverse=True)[0]

    def refresh_cricbuzz_profiles(self, season=2026, force=False, limit=None):
        self.stdout.write("Step 3: Refreshing Cricbuzz profile fields...")

        updater = PlayerProfileUpdater()

        player_ids = (
            MatchPlayer.objects
            .filter(match__season=season)
            .values_list("player_id", flat=True)
            .distinct()
        )

        qs = Player.objects.filter(player_id__in=player_ids).order_by("player_name")
        if limit:
            qs = qs[:limit]

        updated = 0
        no_change = 0
        errors = 0

        for idx, player in enumerate(qs, start=1):
            self.stdout.write(f"[{idx}] {player.player_name}")
            try:
                result = updater.update_player(player, force=force)
                if result["status"] == "updated":
                    updated += 1
                else:
                    no_change += 1
            except Exception as exc:
                errors += 1
                self.stdout.write(self.style.ERROR(f"Failed {player.player_name}: {exc}"))

        self.stdout.write(self.style.SUCCESS(
            f"Cricbuzz refresh done: updated={updated}, no_change={no_change}, errors={errors}"
        ))