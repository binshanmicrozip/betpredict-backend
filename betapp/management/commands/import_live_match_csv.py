import csv
import json
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import transaction

from betapp.models import IPLMatch, LiveMatchState, LiveDelivery
from betapp.services.live_ingest import get_or_create_player


class Command(BaseCommand):
    help = "Import live match CSV data to database after match completion"

    def add_arguments(self, parser):
        parser.add_argument("--match-id", required=True, help="Database match id")
        parser.add_argument("--source-match-id", required=True, help="Cricbuzz match id")
        parser.add_argument("--input-dir", default="live_match_data", help="Directory containing CSV files")
        parser.add_argument("--clear-existing", action="store_true", help="Clear existing live data for this match")

    def handle(self, *args, **options):
        match_id = options["match_id"]
        source_match_id = options["source_match_id"]
        input_dir = options["input_dir"]
        clear_existing = options["clear_existing"]

        input_path = Path(input_dir)
        match_csv = input_path / f"live_match_{match_id}_{source_match_id}.csv"
        commentary_csv = input_path / f"live_commentary_{match_id}_{source_match_id}.csv"

        # Check if CSV files exist
        if not match_csv.exists():
            self.stdout.write(self.style.ERROR(f"Match CSV file not found: {match_csv}"))
            return

        if not commentary_csv.exists():
            self.stdout.write(self.style.ERROR(f"Commentary CSV file not found: {commentary_csv}"))
            return

        try:
            match_obj = IPLMatch.objects.get(match_id=match_id)
        except IPLMatch.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Match {match_id} not found in database"))
            return

        with transaction.atomic():
            if clear_existing:
                self.stdout.write("Clearing existing live data...")
                LiveMatchState.objects.filter(match=match_obj, source_match_id=source_match_id).delete()
                LiveDelivery.objects.filter(match=match_obj).delete()

            # Import match snapshots
            self.stdout.write("Importing match snapshots...")
            match_count = 0
            with open(match_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        raw_json = json.loads(row['raw_json']) if row['raw_json'] else None

                        live_state, created = LiveMatchState.objects.update_or_create(
                            match=match_obj,
                            source_match_id=str(source_match_id),
                            defaults={
                                "source": row['source'],
                                "status": row['status'],
                                "state": row['state'],
                                "innings": int(row['innings']) if row['innings'] else None,
                                "batting_team": row['batting_team'],
                                "bowling_team": row['bowling_team'],
                                "score": int(row['score']) if row['score'] else 0,
                                "wickets": int(row['wickets']) if row['wickets'] else 0,
                                "overs": float(row['overs']) if row['overs'] else 0,
                                "target": int(row['target']) if row['target'] else None,
                                "current_run_rate": float(row['crr']) if row['crr'] else None,
                                "required_run_rate": float(row['rrr']) if row['rrr'] else None,
                                "raw_json": raw_json,
                            },
                        )
                        match_count += 1
                    except Exception as e:
                        self.stdout.write(self.style.WARNING(f"Error importing match row: {e}"))

            # Import commentary/delivery data
            self.stdout.write("Importing delivery data...")
            delivery_count = 0
            with open(commentary_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        # Skip rows without required delivery info
                        if not row['over_number'] or not row['ball_number']:
                            continue

                        batter = get_or_create_player(row['batter_name']) if row['batter_name'] else None
                        bowler = get_or_create_player(row['bowler_name']) if row['bowler_name'] else None
                        non_striker = get_or_create_player(row['non_striker_name']) if row['non_striker_name'] else None
                        player_out = get_or_create_player(row['player_out_name']) if row['player_out_name'] else None

                        delivery, created = LiveDelivery.objects.update_or_create(
                            match=match_obj,
                            innings=int(row['innings']) if row['innings'] else 1,
                            over_number=int(row['over_number']),
                            ball_number=int(row['ball_number']),
                            defaults={
                                "batter": batter,
                                "bowler": bowler,
                                "non_striker": non_striker,
                                "player_out": player_out,
                                "runs_batter": int(row['runs_batter']) if row['runs_batter'] else 0,
                                "runs_extras": int(row['runs_extras']) if row['runs_extras'] else 0,
                                "runs_total": int(row['runs_total']) if row['runs_total'] else 0,
                                "is_wicket": bool(int(row['is_wicket']) if row['is_wicket'] else 0),
                                "wicket_kind": row['wicket_kind'],
                                "commentary": row['commentary'],
                            },
                        )
                        delivery_count += 1
                    except Exception as e:
                        self.stdout.write(self.style.WARNING(f"Error importing delivery row: {e}"))

        self.stdout.write(
            self.style.SUCCESS(
                f"Successfully imported {match_count} match snapshots and {delivery_count} deliveries to database"
            )
        )