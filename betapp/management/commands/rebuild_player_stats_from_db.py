from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Max

from betapp.models import (
    Player,
    IPLMatch,
    MatchPlayer,
    Delivery,
    PlayerMatchBatting,
    PlayerMatchBowling,
    PlayerSituationStats,
)

WICKET_EXCLUDE_FOR_BOWLER = {
    "run out",
    "retired hurt",
    "retired out",
    "obstructing the field",
}

WICKET_EXCLUDE_FOR_BATTER_OUT = {
    "retired hurt",
}

EXTRA_BALL_NOT_COUNT_FOR_BATTER = {"wide"}
EXTRA_BALL_NOT_COUNT_FOR_BOWLER = {"wide", "noball"}


def to_decimal(value, places="0.01"):
    return Decimal(str(value)).quantize(Decimal(places), rounding=ROUND_HALF_UP)


def balls_to_overs_decimal(legal_balls: int) -> Decimal:
    overs = legal_balls // 6
    balls = legal_balls % 6
    return Decimal(f"{overs}.{balls}")


def phase_from_over(over_number: int) -> str:
    if over_number <= 6:
        return "powerplay"
    elif over_number <= 15:
        return "middle"
    return "death"


class Command(BaseCommand):
    help = "Rebuild player stats from Delivery table"

    def add_arguments(self, parser):
        parser.add_argument("--clear-first", action="store_true")

    def handle(self, *args, **options):
        if options["clear_first"]:
            self.stdout.write("Clearing old stats...")
            PlayerMatchBatting.objects.all().delete()
            PlayerMatchBowling.objects.all().delete()
            PlayerSituationStats.objects.all().delete()

        deliveries = Delivery.objects.select_related(
            "match", "batter", "bowler", "non_striker", "player_out"
        ).order_by("match_id", "innings", "over_number", "ball_number")

        self.stdout.write(f"Total deliveries: {deliveries.count()}")

        batting_data = {}
        bowling_data = {}
        situation_data = {}
        match_tracker = defaultdict(set)

        for d in deliveries:

            # =========================
            # BATTING
            # =========================
            b_key = (d.match_id, d.batter_id, d.innings)

            if b_key not in batting_data:
                batting_data[b_key] = {
                    "runs": 0,
                    "balls": 0,
                    "fours": 0,
                    "sixes": 0,
                    "dismissal": None,
                }

            b = batting_data[b_key]
            b["runs"] += d.runs_batter

            if d.extra_type not in EXTRA_BALL_NOT_COUNT_FOR_BATTER:
                b["balls"] += 1

            if d.runs_batter == 4:
                b["fours"] += 1
            elif d.runs_batter == 6:
                b["sixes"] += 1

            if d.player_out_id == d.batter_id and d.wicket_kind not in WICKET_EXCLUDE_FOR_BATTER_OUT:
                b["dismissal"] = d.wicket_kind

            # =========================
            # BOWLING
            # =========================
            bo_key = (d.match_id, d.bowler_id, d.innings)

            if bo_key not in bowling_data:
                bowling_data[bo_key] = {
                    "balls": 0,
                    "runs": 0,
                    "wickets": 0,
                    "wides": 0,
                    "noballs": 0,
                }

            bo = bowling_data[bo_key]
            bo["runs"] += d.runs_total

            if d.extra_type == "wide":
                bo["wides"] += d.runs_extras
            elif d.extra_type == "noball":
                bo["noballs"] += d.runs_extras

            if d.extra_type not in EXTRA_BALL_NOT_COUNT_FOR_BOWLER:
                bo["balls"] += 1

            if d.is_wicket and d.wicket_kind and d.wicket_kind.lower() not in WICKET_EXCLUDE_FOR_BOWLER:
                bo["wickets"] += 1

            # =========================
            # SITUATION
            # =========================
            phase = phase_from_over(d.over_number)
            innings_type = "defending" if d.innings == 1 else "chasing"

            s_key = (d.batter_id, phase, innings_type)

            if s_key not in situation_data:
                situation_data[s_key] = {
                    "runs": 0,
                    "balls": 0,
                    "boundaries": 0,
                    "wickets_lost": 0,
                }

            s = situation_data[s_key]
            s["runs"] += d.runs_batter

            if d.extra_type not in EXTRA_BALL_NOT_COUNT_FOR_BATTER:
                s["balls"] += 1

            if d.runs_batter in (4, 6):
                s["boundaries"] += 1

            if d.player_out_id == d.batter_id:
                s["wickets_lost"] += 1

            match_tracker[s_key].add(d.match_id)

        # =========================
        # SAVE BATTING
        # =========================
        self.stdout.write("Saving batting stats...")

        for (match_id, player_id, innings), row in batting_data.items():
            sr = (row["runs"] / row["balls"]) * 100 if row["balls"] else 0

            PlayerMatchBatting.objects.update_or_create(
                match_id=match_id,
                player_id=player_id,
                innings=innings,
                defaults={
                    "runs": row["runs"],
                    "balls_faced": row["balls"],
                    "fours": row["fours"],
                    "sixes": row["sixes"],
                    "strike_rate": to_decimal(sr),
                    "dismissal_kind": row["dismissal"],
                    "is_not_out": row["dismissal"] is None,
                },
            )

        # =========================
        # SAVE BOWLING
        # =========================
        self.stdout.write("Saving bowling stats...")

        for (match_id, player_id, innings), row in bowling_data.items():
            eco = (row["runs"] / row["balls"]) * 6 if row["balls"] else 0

            PlayerMatchBowling.objects.update_or_create(
                match_id=match_id,
                player_id=player_id,
                innings=innings,
                defaults={
                    "overs_bowled": balls_to_overs_decimal(row["balls"]),
                    "runs_given": row["runs"],
                    "wickets": row["wickets"],
                    "economy": to_decimal(eco),
                    "wides": row["wides"],
                    "noballs": row["noballs"],
                },
            )

        # =========================
        # SAVE SITUATION
        # =========================
        self.stdout.write("Saving situation stats...")

        PlayerSituationStats.objects.all().delete()

        objs = []
        for (player_id, phase, innings_type), row in situation_data.items():
            matches = len(match_tracker[(player_id, phase, innings_type)])

            sr = (row["runs"] / row["balls"]) * 100 if row["balls"] else 0
            bp = (row["boundaries"] / row["balls"]) * 100 if row["balls"] else 0
            dr = row["wickets_lost"] / row["balls"] if row["balls"] else 0

            objs.append(
                PlayerSituationStats(
                    player_id=player_id,
                    phase=phase,
                    innings_type=innings_type,
                    matches_played=matches,
                    runs=row["runs"],
                    balls=row["balls"],
                    strike_rate=to_decimal(sr),
                    boundary_count=row["boundaries"],
                    boundary_pct=to_decimal(bp),
                    wickets_lost=row["wickets_lost"],
                    dismissal_rate=to_decimal(dr, "0.0001"),
                )
            )

        PlayerSituationStats.objects.bulk_create(objs)

        # =========================
        # UPDATE LAST SEASON
        # =========================
        self.stdout.write("Updating last season...")

        season_map = {
            row["player_id"]: row["last_season"]
            for row in MatchPlayer.objects.values("player_id")
            .annotate(last_season=Max("match__season"))
        }

        for p in Player.objects.all():
            if p.player_id in season_map:
                p.last_season = season_map[p.player_id]
                p.save(update_fields=["last_season"])

        self.stdout.write(self.style.SUCCESS("Stats rebuild completed successfully!"))