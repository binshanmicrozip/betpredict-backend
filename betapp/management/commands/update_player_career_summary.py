from decimal import Decimal, ROUND_HALF_UP

from django.core.management.base import BaseCommand
from django.db.models import Min, Max, Sum, Count, Q

from betapp.models import Player, MatchPlayer, PlayerMatchBatting, PlayerMatchBowling


def safe_decimal(value, places="0.01"):
    if value is None:
        return None
    return Decimal(str(value)).quantize(Decimal(places), rounding=ROUND_HALF_UP)


class Command(BaseCommand):
    help = "Update player career summary from all IPL historical data"

    def handle(self, *args, **options):
        self.stdout.write("Updating player career summary...")

        participation = (
            MatchPlayer.objects.values("player_id")
            .annotate(
                first_season=Min("match__season"),
                last_season=Max("match__season"),
                first_match_date=Min("match__match_date"),
                total_matches=Count("match_id", distinct=True),
            )
        )
        participation_map = {row["player_id"]: row for row in participation}

        batting = (
            PlayerMatchBatting.objects.values("player_id")
            .annotate(
                total_innings=Count("id"),
                total_runs=Sum("runs"),
                total_balls=Sum("balls_faced"),
                max_score=Max("runs"),
                total_fours=Sum("fours"),
                total_sixes=Sum("sixes"),
                total_fifties=Count("id", filter=Q(runs__gte=50, runs__lt=100)),
                total_hundreds=Count("id", filter=Q(runs__gte=100)),
                total_not_outs=Count("id", filter=Q(is_not_out=True)),
            )
        )
        batting_map = {row["player_id"]: row for row in batting}

        bowling = (
            PlayerMatchBowling.objects.values("player_id")
            .annotate(
                total_balls_bowled=Sum("balls_bowled_calc"),
                total_wickets=Sum("wickets"),
                total_runs_given=Sum("runs_given"),
                total_wides=Sum("wides"),
                total_noballs=Sum("noballs"),
            )
        )
        bowling_map = {row["player_id"]: row for row in bowling}

        updated_count = 0

        for player in Player.objects.all():
            part = participation_map.get(player.player_id, {})
            bat = batting_map.get(player.player_id, {})
            bowl = bowling_map.get(player.player_id, {})

            player.ipl_debut = part.get("first_match_date")
            player.debut_year = part.get("first_season")
            player.last_season = part.get("last_season")
            player.total_matches = part.get("total_matches") or 0

            runs = bat.get("total_runs") or 0
            balls = bat.get("total_balls") or 0
            innings = bat.get("total_innings") or 0
            not_outs = bat.get("total_not_outs") or 0

            player.innings = innings
            player.total_runs = runs
            player.balls_faced_total = balls
            player.highscore = bat.get("max_score") or 0
            player.fours = bat.get("total_fours") or 0
            player.sixes = bat.get("total_sixes") or 0
            player.fifties = bat.get("total_fifties") or 0
            player.hundreds = bat.get("total_hundreds") or 0
            player.not_outs = not_outs

            player.strike_rate = safe_decimal((runs / balls) * 100) if balls else None

            dismissals = innings - not_outs
            player.batting_average = safe_decimal(runs / dismissals) if dismissals > 0 else None

            balls_bowled = bowl.get("total_balls_bowled") or 0
            wickets = bowl.get("total_wickets") or 0
            runs_given = bowl.get("total_runs_given") or 0

            player.balls_bowled = balls_bowled
            player.wickets = wickets
            player.runs_given = runs_given
            player.wides = bowl.get("total_wides") or 0
            player.noballs = bowl.get("total_noballs") or 0
            player.economy = safe_decimal((runs_given / balls_bowled) * 6) if balls_bowled else None

            player.save()
            updated_count += 1

        self.stdout.write(self.style.SUCCESS(f"Updated {updated_count} players successfully"))