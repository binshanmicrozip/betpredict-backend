import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from collections import defaultdict

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from betapp.models import (
    Player,
    PlayerIPLTeam,
    IPLMatch,
    MatchPlayer,
    Delivery,
    PlayerMatchBatting,
    PlayerMatchBowling,
    PlayerSituationStats,
)


TEAM_SHORT_MAP = {
    "Chennai Super Kings": "CSK",
    "Delhi Capitals": "DC",
    "Gujarat Titans": "GT",
    "Kolkata Knight Riders": "KKR",
    "Lucknow Super Giants": "LSG",
    "Mumbai Indians": "MI",
    "Punjab Kings": "PBKS",
    "Rajasthan Royals": "RR",
    "Royal Challengers Bangalore": "RCB",
    "Royal Challengers Bengaluru": "RCB",
    "Sunrisers Hyderabad": "SRH",
}


def d2(value):
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def d4(value):
    return Decimal(str(value)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def get_phase(over_number: int) -> str:
    if over_number <= 5:
        return "powerplay"
    if over_number <= 14:
        return "middle"
    return "death"


def innings_type_for_team(match_obj, batting_team: str) -> str:
    # innings 1 batting team is defending total
    # innings 2 batting team is chasing target
    if batting_team == match_obj.team_home:
        return "defending"
    if batting_team == match_obj.team_away:
        return "chasing"
    return "defending"


def balls_to_overs(ball_count: int) -> Decimal:
    whole_overs = ball_count // 6
    balls = ball_count % 6
    return Decimal(f"{whole_overs}.{balls}")


class Command(BaseCommand):
    help = "Import Cricsheet IPL JSON files into new player tables only."

    def add_arguments(self, parser):
        parser.add_argument(
            "input_path",
            type=str,
            help="Path to one JSON file or folder containing JSON files",
        )
        parser.add_argument(
            "--season",
            type=int,
            default=None,
            help="Optional: override season or filter imported season",
        )

    def handle(self, *args, **options):
        input_path = Path(options["input_path"])
        season_filter = options.get("season")

        if not input_path.exists():
            raise CommandError(f"Path not found: {input_path}")

        if input_path.is_file():
            files = [input_path]
        else:
            files = sorted(input_path.rglob("*.json"))

        if not files:
            raise CommandError("No JSON files found.")

        imported = 0
        skipped = 0

        for fp in files:
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    payload = json.load(f)

                info = payload.get("info", {})
                event_name = (info.get("event") or {}).get("name", "")
                match_type = info.get("match_type", "")
                season = int(info.get("season")) if info.get("season") else None

                if event_name != "Indian Premier League":
                    self.stdout.write(self.style.WARNING(f"Skipping non-IPL file: {fp.name}"))
                    skipped += 1
                    continue

                if match_type != "T20":
                    self.stdout.write(self.style.WARNING(f"Skipping non-T20 file: {fp.name}"))
                    skipped += 1
                    continue

                if season_filter and season != season_filter:
                    skipped += 1
                    continue

                self.import_single_match(fp, payload)
                imported += 1
                self.stdout.write(self.style.SUCCESS(f"Imported: {fp.name}"))

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Failed {fp.name}: {e}"))

        self.stdout.write(self.style.SUCCESS(
            f"Done. Imported={imported}, Skipped={skipped}"
        ))

    @transaction.atomic
    def import_single_match(self, fp: Path, payload: dict):
        info = payload.get("info", {})
        registry = (info.get("registry") or {}).get("people", {})
        teams = info.get("teams", [])
        players_by_team = info.get("players", {})
        dates = info.get("dates", [])

        if len(teams) < 2:
            raise ValueError("teams missing in JSON")

        team_home = teams[0]
        team_away = teams[1]

        outcome = info.get("outcome", {}) or {}
        by_info = outcome.get("by", {}) or {}
        toss = info.get("toss", {}) or {}
        event = info.get("event", {}) or {}
        pom = info.get("player_of_match", []) or []

        match_id = fp.stem
        season = int(info["season"])
        match_date = dates[0]

        match_obj, _ = IPLMatch.objects.update_or_create(
            match_id=match_id,
            defaults={
                "season": season,
                "match_number": event.get("match_number"),
                "match_date": match_date,
                "venue": info.get("venue"),
                "city": info.get("city"),
                "team_home": team_home,
                "team_away": team_away,
                "toss_winner": toss.get("winner"),
                "toss_decision": toss.get("decision"),
                "winner": outcome.get("winner"),
                "win_by_runs": by_info.get("runs"),
                "win_by_wickets": by_info.get("wickets"),
                "player_of_match": pom[0] if pom else None,
            },
        )

        # Create/update players and team-season mappings
        for team_name, player_names in players_by_team.items():
            for player_name in player_names:
                player_id = registry.get(player_name)
                if not player_id:
                    continue

                player_obj, created = Player.objects.get_or_create(
                    player_id=player_id,
                    defaults={
                        "player_name": player_name,
                        "nationality": None,
                        "role": "Unknown",
                        "ipl_debut": match_date,
                        "last_season": season,
                    },
                )

                changed = False
                if not player_obj.ipl_debut or str(match_date) < str(player_obj.ipl_debut):
                    player_obj.ipl_debut = match_date
                    changed = True
                if not player_obj.last_season or season > player_obj.last_season:
                    player_obj.last_season = season
                    changed = True
                if player_obj.player_name != player_name:
                    player_obj.player_name = player_name
                    changed = True
                if changed:
                    player_obj.save()

                PlayerIPLTeam.objects.get_or_create(
                    player=player_obj,
                    team_name=team_name,
                    season=season,
                    defaults={
                        "team_short": TEAM_SHORT_MAP.get(team_name),
                        "is_current": season == 2026,
                    },
                )

                MatchPlayer.objects.get_or_create(
                    match=match_obj,
                    player=player_obj,
                    defaults={
                        "team_name": team_name,
                        "batting_position": None,
                    },
                )

        # Remove old derived records for re-import safety
        Delivery.objects.filter(match=match_obj).delete()
        PlayerMatchBatting.objects.filter(match=match_obj).delete()
        PlayerMatchBowling.objects.filter(match=match_obj).delete()

        batting_stats = {}
        bowling_stats = {}
        batting_order_seen = defaultdict(list)
        situation_stats = defaultdict(lambda: {
            "matches": set(),
            "runs": 0,
            "balls": 0,
            "boundaries": 0,
            "wickets_lost": 0,
        })

        innings_list = payload.get("innings", [])

        for innings_index, innings_data in enumerate(innings_list, start=1):
            batting_team = innings_data.get("team")
            innings_type = "defending" if innings_index == 1 else "chasing"

            overs = innings_data.get("overs", [])
            for over_data in overs:
                over_number = int(over_data.get("over", 0))
                deliveries = over_data.get("deliveries", [])

                legal_ball_count_in_over = 0

                for delivery in deliveries:
                    batter_name = delivery.get("batter")
                    bowler_name = delivery.get("bowler")
                    non_striker_name = delivery.get("non_striker")

                    batter_id = registry.get(batter_name)
                    bowler_id = registry.get(bowler_name)
                    non_striker_id = registry.get(non_striker_name)

                    if not batter_id or not bowler_id or not non_striker_id:
                        continue

                    batter = Player.objects.get(player_id=batter_id)
                    bowler = Player.objects.get(player_id=bowler_id)
                    non_striker = Player.objects.get(player_id=non_striker_id)

                    extras_detail = delivery.get("extras", {}) or {}
                    runs = delivery.get("runs", {}) or {}

                    extra_type = None
                    if "wides" in extras_detail:
                        extra_type = "wide"
                    elif "noballs" in extras_detail:
                        extra_type = "noball"
                    elif "legbyes" in extras_detail:
                        extra_type = "legbye"
                    elif "byes" in extras_detail:
                        extra_type = "bye"

                    is_legal_ball = extra_type not in ("wide", "noball")
                    if is_legal_ball:
                        legal_ball_count_in_over += 1

                    ball_number = legal_ball_count_in_over if is_legal_ball else legal_ball_count_in_over + 1

                    wickets = delivery.get("wickets", []) or []
                    is_wicket = len(wickets) > 0
                    wicket_kind = None
                    player_out = None

                    if wickets:
                        wicket_kind = wickets[0].get("kind")
                        player_out_name = wickets[0].get("player_out")
                        player_out_id = registry.get(player_out_name)
                        if player_out_id:
                            player_out = Player.objects.get(player_id=player_out_id)

                    Delivery.objects.create(
                        match=match_obj,
                        innings=innings_index,
                        over_number=over_number,
                        ball_number=ball_number,
                        batter=batter,
                        bowler=bowler,
                        non_striker=non_striker,
                        runs_batter=runs.get("batter", 0),
                        runs_extras=runs.get("extras", 0),
                        runs_total=runs.get("total", 0),
                        extra_type=extra_type,
                        is_wicket=is_wicket,
                        wicket_kind=wicket_kind,
                        player_out=player_out,
                    )

                    # Track batting order
                    if batter_id not in batting_order_seen[(match_id, innings_index)]:
                        batting_order_seen[(match_id, innings_index)].append(batter_id)

                    # Batting stats
                    bat_key = (innings_index, batter_id)
                    if bat_key not in batting_stats:
                        batting_stats[bat_key] = {
                            "player": batter,
                            "runs": 0,
                            "balls_faced": 0,
                            "fours": 0,
                            "sixes": 0,
                            "dismissal_kind": None,
                            "is_not_out": True,
                            "batting_position": None,
                        }

                    batting_stats[bat_key]["runs"] += runs.get("batter", 0)

                    # wides do not count as balls faced
                    if extra_type != "wide":
                        batting_stats[bat_key]["balls_faced"] += 1

                    if runs.get("batter", 0) == 4:
                        batting_stats[bat_key]["fours"] += 1
                    if runs.get("batter", 0) == 6:
                        batting_stats[bat_key]["sixes"] += 1

                    if is_wicket and player_out and player_out.player_id == batter_id:
                        batting_stats[bat_key]["dismissal_kind"] = wicket_kind
                        batting_stats[bat_key]["is_not_out"] = False

                    # Bowling stats
                    bowl_key = (innings_index, bowler_id)
                    if bowl_key not in bowling_stats:
                        bowling_stats[bowl_key] = {
                            "player": bowler,
                            "balls": 0,
                            "runs_given": 0,
                            "wickets": 0,
                            "wides": 0,
                            "noballs": 0,
                        }

                    # bowler runs exclude byes/legbyes, include wides/noballs
                    bowler_runs_conceded = runs.get("batter", 0)
                    if "wides" in extras_detail:
                        bowler_runs_conceded += extras_detail.get("wides", 0)
                        bowling_stats[bowl_key]["wides"] += extras_detail.get("wides", 0)
                    if "noballs" in extras_detail:
                        bowler_runs_conceded += extras_detail.get("noballs", 0)
                        bowling_stats[bowl_key]["noballs"] += extras_detail.get("noballs", 0)

                    bowling_stats[bowl_key]["runs_given"] += bowler_runs_conceded

                    if is_legal_ball:
                        bowling_stats[bowl_key]["balls"] += 1

                    if is_wicket and wicket_kind not in ("run out", "retired hurt", "obstructing the field"):
                        bowling_stats[bowl_key]["wickets"] += 1

                    # Situation stats
                    phase = get_phase(over_number)
                    sit_key = (batter_id, phase, innings_type)

                    # wides do not count as batter ball
                    if extra_type != "wide":
                        situation_stats[sit_key]["balls"] += 1
                        situation_stats[sit_key]["runs"] += runs.get("batter", 0)
                        situation_stats[sit_key]["matches"].add(match_obj.match_id)

                        if runs.get("batter", 0) in (4, 6):
                            situation_stats[sit_key]["boundaries"] += 1

                        if is_wicket and player_out and player_out.player_id == batter_id:
                            situation_stats[sit_key]["wickets_lost"] += 1

        # Save batting scorecards
        for (innings_index, player_id), stats in batting_stats.items():
            batting_position = batting_order_seen[(match_id, innings_index)].index(player_id) + 1
            stats["batting_position"] = batting_position

            MatchPlayer.objects.filter(match=match_obj, player_id=player_id).update(
                batting_position=batting_position
            )

            balls_faced = stats["balls_faced"]
            strike_rate = d2((stats["runs"] / balls_faced) * 100) if balls_faced else Decimal("0.00")

            PlayerMatchBatting.objects.create(
                match=match_obj,
                player=stats["player"],
                innings=innings_index,
                runs=stats["runs"],
                balls_faced=balls_faced,
                fours=stats["fours"],
                sixes=stats["sixes"],
                strike_rate=strike_rate,
                dismissal_kind=stats["dismissal_kind"],
                is_not_out=stats["is_not_out"],
                batting_position=batting_position,
            )

        # Save bowling scorecards
        for (innings_index, _player_id), stats in bowling_stats.items():
            balls = stats["balls"]
            overs_bowled = balls_to_overs(balls)
            economy = d2((stats["runs_given"] / balls) * 6) if balls else Decimal("0.00")

            PlayerMatchBowling.objects.create(
                match=match_obj,
                player=stats["player"],
                innings=innings_index,
                overs_bowled=overs_bowled,
                runs_given=stats["runs_given"],
                wickets=stats["wickets"],
                economy=economy,
                wides=stats["wides"],
                noballs=stats["noballs"],
            )

        # Upsert cumulative player situation stats
        for (player_id, phase, innings_type), stats in situation_stats.items():
            player = Player.objects.get(player_id=player_id)

            obj, _ = PlayerSituationStats.objects.get_or_create(
                player=player,
                phase=phase,
                innings_type=innings_type,
                defaults={
                    "matches_played": 0,
                    "runs": 0,
                    "balls": 0,
                    "strike_rate": Decimal("0.00"),
                    "boundary_count": 0,
                    "boundary_pct": Decimal("0.00"),
                    "wickets_lost": 0,
                    "dismissal_rate": Decimal("0.0000"),
                }
            )

            obj.matches_played += len(stats["matches"])
            obj.runs += stats["runs"]
            obj.balls += stats["balls"]
            obj.boundary_count += stats["boundaries"]
            obj.wickets_lost += stats["wickets_lost"]

            obj.strike_rate = d2((obj.runs / obj.balls) * 100) if obj.balls else Decimal("0.00")
            obj.boundary_pct = d2((obj.boundary_count / obj.balls) * 100) if obj.balls else Decimal("0.00")
            obj.dismissal_rate = d4(obj.wickets_lost / obj.balls) if obj.balls else Decimal("0.0000")

            obj.save()