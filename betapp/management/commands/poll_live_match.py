# import csv
# import json
# import time
# from datetime import datetime
# from pathlib import Path

# from django.core.management.base import BaseCommand
# from betapp.cricbuzz_live import fetch_score, parse_live_data
# from betapp.redis_cricket import set_latest_cricket


# class Command(BaseCommand):
#     help = "Poll Cricbuzz live match and save live data to CSV file"

#     def add_arguments(self, parser):
#         parser.add_argument("--match-id", required=True, help="Database match id")
#         parser.add_argument("--source-match-id", required=True, help="Cricbuzz match id")
#         parser.add_argument("--interval", type=int, default=10)
#         parser.add_argument("--once", action="store_true")
#         parser.add_argument("--output-dir", default="live_match_data", help="Directory to save CSV files")

#     def handle(self, *args, **options):
#         match_id = options["match_id"]
#         source_match_id = options["source_match_id"]
#         interval = options["interval"]
#         once = options["once"]
#         output_dir = options["output_dir"]

#         output_path = Path(output_dir)
#         output_path.mkdir(exist_ok=True)

#         match_csv = output_path / f"live_match_{match_id}_{source_match_id}.csv"
#         commentary_csv = output_path / f"live_commentary_{match_id}_{source_match_id}.csv"

#         match_file = None
#         commentary_file = None

#         try:
#             match_file = open(match_csv, "w", newline="", encoding="utf-8")
#             commentary_file = open(commentary_csv, "w", newline="", encoding="utf-8")

#             match_writer = csv.DictWriter(
#                 match_file,
#                 fieldnames=[
#                     "timestamp",
#                     "match_id",
#                     "source_match_id",
#                     "source",
#                     "status",
#                     "state",
#                     "innings",
#                     "batting_team",
#                     "bowling_team",
#                     "score",
#                     "wickets",
#                     "overs",
#                     "target",
#                     "crr",
#                     "rrr",
#                     "raw_json",
#                 ],
#             )
#             match_writer.writeheader()

#             commentary_writer = csv.DictWriter(
#                 commentary_file,
#                 fieldnames=[
#                     "timestamp",
#                     "match_id",
#                     "source_match_id",
#                     "innings",
#                     "over_number",
#                     "ball_number",
#                     "batter_name",
#                     "bowler_name",
#                     "non_striker_name",
#                     "runs_batter",
#                     "runs_extras",
#                     "runs_total",
#                     "is_wicket",
#                     "wicket_kind",
#                     "player_out_name",
#                     "commentary",
#                 ],
#             )
#             commentary_writer.writeheader()

#             self.stdout.write(self.style.SUCCESS("Saving live data to CSV files:"))
#             self.stdout.write(f"  Match data: {match_csv}")
#             self.stdout.write(f"  Commentary: {commentary_csv}")

#             while True:
#                 try:
#                     raw = fetch_score(source_match_id)

#                     if not raw:
#                         self.stdout.write(self.style.WARNING("No live data fetched"))
#                         if once:
#                             break
#                         time.sleep(interval)
#                         continue

#                     parsed = parse_live_data(raw)
#                     timestamp = datetime.now().isoformat()

#                     # ---------------------------------------------
#                     # Save match snapshot to CSV
#                     # ---------------------------------------------
#                     match_row = {
#                         "timestamp": timestamp,
#                         "match_id": match_id,
#                         "source_match_id": source_match_id,
#                         "source": "cricbuzz",
#                         "status": parsed.get("status"),
#                         "state": parsed.get("state"),
#                         "innings": parsed.get("innings"),
#                         "batting_team": parsed.get("batting_team"),
#                         "bowling_team": parsed.get("bowling_team"),
#                         "score": parsed.get("score", 0),
#                         "wickets": parsed.get("wickets", 0),
#                         "overs": str(parsed.get("overs", 0)),
#                         "target": parsed.get("target"),
#                         "crr": str(parsed.get("crr", 0)),
#                         "rrr": str(parsed.get("rrr", 0)),
#                         "raw_json": json.dumps(raw, ensure_ascii=False),
#                     }
#                     match_writer.writerow(match_row)
#                     match_file.flush()

#                     # ---------------------------------------------
#                     # Save commentary to CSV
#                     # Cricbuzz live response usually has commentaryList
#                     # ---------------------------------------------
#                     commentary_count = 0

#                     commentary_items = []
#                     if isinstance(raw, dict):
#                         if raw.get("commentaryList"):
#                             commentary_items = raw.get("commentaryList", [])
#                         elif raw.get("commentary"):
#                             commentary_items = raw.get("commentary", [])

#                     for item in commentary_items:
#                         commentary_row = {
#                             "timestamp": timestamp,
#                             "match_id": match_id,
#                             "source_match_id": source_match_id,
#                             "innings": item.get("inningsId") or item.get("innings"),
#                             "over_number": item.get("overNumber") or item.get("overNum"),
#                             "ball_number": item.get("ballNbr") or item.get("ballNumber"),
#                             "batter_name": item.get("batsmanName") or item.get("strikerName") or item.get("batName"),
#                             "bowler_name": item.get("bowlerName") or item.get("bowlName"),
#                             "non_striker_name": item.get("nonStrikerName"),
#                             "runs_batter": item.get("runs") or item.get("batRuns") or item.get("batterRuns", 0),
#                             "runs_extras": item.get("extras", 0),
#                             "runs_total": item.get("totalRuns") or item.get("runsTotal", 0),
#                             "is_wicket": 1 if item.get("isWicket") else 0,
#                             "wicket_kind": item.get("wicketKind") or item.get("dismissalType"),
#                             "player_out_name": item.get("playerOutName") or item.get("dismissedPlayer"),
#                             "commentary": item.get("commText") or item.get("commentText") or "",
#                         }
#                         commentary_writer.writerow(commentary_row)
#                         commentary_count += 1

#                     commentary_file.flush()

#                     # ---------------------------------------------
#                     # Save latest parsed cricket data into Redis
#                     # IMPORTANT: pass raw too
#                     # ---------------------------------------------
#                     set_latest_cricket(source_match_id, parsed, raw)

#                     self.stdout.write(
#                         self.style.SUCCESS(
#                             f"Saved to CSV and Redis: match={match_id}, commentary_rows={commentary_count}, source_match_id={source_match_id}"
#                         )
#                     )

#                 except Exception as e:
#                     self.stdout.write(
#                         self.style.ERROR(
#                             f"Error for source_match_id={source_match_id}: {e}"
#                         )
#                     )

#                 if once:
#                     break

#                 time.sleep(interval)

#         finally:
#             if match_file:
#                 match_file.close()
#             if commentary_file:
#                 commentary_file.close()

#             self.stdout.write(self.style.SUCCESS(f"CSV files saved to: {output_path}"))


import csv
import json
import time
from datetime import datetime
from pathlib import Path

from django.core.management.base import BaseCommand
from betapp.cricbuzz_live import fetch_score, parse_live_data
from betapp.redis_cricket import set_latest_cricket


class Command(BaseCommand):
    help = "Poll Cricbuzz live match and save live data to CSV file"

    def add_arguments(self, parser):
        parser.add_argument("--match-id", required=True, help="Database match id")
        parser.add_argument("--source-match-id", required=True, help="Cricbuzz match id")
        parser.add_argument("--interval", type=int, default=10)
        parser.add_argument("--once", action="store_true")
        parser.add_argument("--output-dir", default="live_match_data", help="Directory to save CSV files")

    def handle(self, *args, **options):
        match_id = options["match_id"]
        source_match_id = options["source_match_id"]
        interval = options["interval"]
        once = options["once"]
        output_dir = options["output_dir"]

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        match_csv = output_path / f"live_match_{match_id}_{source_match_id}.csv"
        commentary_csv = output_path / f"live_commentary_{match_id}_{source_match_id}.csv"

        match_file = None
        commentary_file = None

        try:
            match_file = open(match_csv, "w", newline="", encoding="utf-8")
            commentary_file = open(commentary_csv, "w", newline="", encoding="utf-8")

            match_writer = csv.DictWriter(
                match_file,
                fieldnames=[
                    "timestamp",
                    "match_id",
                    "source_match_id",
                    "source",
                    "status",
                    "state",

                    "team1_name",
                    "team2_name",

                    "toss_winner_id",
                    "toss_winner_name",
                    "toss_decision",
                    "batting_first_team",
                    "bowling_first_team",

                    "innings",
                    "batting_team",
                    "bowling_team",
                    "score",
                    "score_num",
                    "wickets",
                    "overs",
                    "target",
                    "rem_runs_to_win",
                    "crr",
                    "rrr",
                    "phase",
                    "innings_type",
                    "recent",
                    "latest_ball",
                    "raw_json",
                ],
            )
            match_writer.writeheader()

            commentary_writer = csv.DictWriter(
                commentary_file,
                fieldnames=[
                    "timestamp",
                    "match_id",
                    "source_match_id",
                    "innings",
                    "over_number",
                    "ball_number",
                    "batter_name",
                    "bowler_name",
                    "non_striker_name",
                    "runs_batter",
                    "runs_extras",
                    "runs_total",
                    "is_wicket",
                    "wicket_kind",
                    "player_out_name",
                    "commentary",
                ],
            )
            commentary_writer.writeheader()

            self.stdout.write(self.style.SUCCESS("Saving live data to CSV files:"))
            self.stdout.write(f"  Match data: {match_csv}")
            self.stdout.write(f"  Commentary: {commentary_csv}")

            while True:
                try:
                    raw = fetch_score(source_match_id)

                    if not raw:
                        self.stdout.write(self.style.WARNING("No live data fetched"))
                        if once:
                            break
                        time.sleep(interval)
                        continue

                    parsed = parse_live_data(raw, source_match_id=source_match_id)
                    timestamp = datetime.now().isoformat()

                    # --------------------------------------------------
                    # Save match snapshot to CSV
                    # --------------------------------------------------
                    match_row = {
                        "timestamp": timestamp,
                        "match_id": match_id,
                        "source_match_id": source_match_id,
                        "source": "cricbuzz",
                        "status": parsed.get("status"),
                        "state": parsed.get("state"),

                        "team1_name": parsed.get("team1_name"),
                        "team2_name": parsed.get("team2_name"),

                        "toss_winner_id": parsed.get("toss_winner_id"),
                        "toss_winner_name": parsed.get("toss_winner_name"),
                        "toss_decision": parsed.get("toss_decision"),
                        "batting_first_team": parsed.get("batting_first_team"),
                        "bowling_first_team": parsed.get("bowling_first_team"),

                        "innings": parsed.get("innings"),
                        "batting_team": parsed.get("batting_team"),
                        "bowling_team": parsed.get("bowling_team"),
                        "score": parsed.get("score", "0/0"),
                        "score_num": parsed.get("score_num", 0),
                        "wickets": parsed.get("wickets", 0),
                        "overs": str(parsed.get("overs", 0)),
                        "target": parsed.get("target", 0),
                        "rem_runs_to_win": parsed.get("rem_runs_to_win", 0),
                        "crr": str(parsed.get("crr", 0)),
                        "rrr": str(parsed.get("rrr", 0)),
                        "phase": parsed.get("phase"),
                        "innings_type": parsed.get("innings_type"),
                        "recent": parsed.get("recent"),
                        "latest_ball": parsed.get("latest_ball"),
                        "raw_json": json.dumps(raw, ensure_ascii=False),
                    }
                    match_writer.writerow(match_row)
                    match_file.flush()

                    # --------------------------------------------------
                    # Save commentary to CSV
                    # --------------------------------------------------
                    commentary_count = 0
                    commentary_items = []

                    if isinstance(raw, dict):
                        if raw.get("commentaryList"):
                            commentary_items = raw.get("commentaryList", [])
                        elif raw.get("commentary"):
                            commentary_items = raw.get("commentary", [])

                    for item in commentary_items:
                        commentary_row = {
                            "timestamp": timestamp,
                            "match_id": match_id,
                            "source_match_id": source_match_id,
                            "innings": item.get("inningsId") or item.get("innings"),
                            "over_number": item.get("overNumber") or item.get("overNum"),
                            "ball_number": item.get("ballNbr") or item.get("ballNumber"),
                            "batter_name": item.get("batsmanName") or item.get("strikerName") or item.get("batName"),
                            "bowler_name": item.get("bowlerName") or item.get("bowlName"),
                            "non_striker_name": item.get("nonStrikerName"),
                            "runs_batter": item.get("runs") or item.get("batRuns") or item.get("batterRuns", 0),
                            "runs_extras": item.get("extras", 0),
                            "runs_total": item.get("totalRuns") or item.get("runsTotal", 0),
                            "is_wicket": 1 if item.get("isWicket") else 0,
                            "wicket_kind": item.get("wicketKind") or item.get("dismissalType"),
                            "player_out_name": item.get("playerOutName") or item.get("dismissedPlayer"),
                            "commentary": item.get("commText") or item.get("commentText") or "",
                        }
                        commentary_writer.writerow(commentary_row)
                        commentary_count += 1

                    commentary_file.flush()

                    # --------------------------------------------------
                    # Save latest parsed cricket data into Redis
                    # --------------------------------------------------
                    set_latest_cricket(source_match_id, parsed, raw)

                    self.stdout.write(
                        self.style.SUCCESS(
                            f"[LIVE] "
                            f"match={match_id} "
                            f"source_match_id={source_match_id} "
                            f"state={parsed.get('state')} "
                            f"status={parsed.get('status')} "
                            f"toss_winner={parsed.get('toss_winner_name')} "
                            f"decision={parsed.get('toss_decision')} "
                            f"bat_first={parsed.get('batting_first_team')} "
                            f"bowl_first={parsed.get('bowling_first_team')} "
                            f"score={parsed.get('score')} "
                            f"commentary_rows={commentary_count}"
                        )
                    )

                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(
                            f"Error for source_match_id={source_match_id}: {e}"
                        )
                    )

                if once:
                    break

                time.sleep(interval)

        finally:
            if match_file:
                match_file.close()
            if commentary_file:
                commentary_file.close()

            self.stdout.write(self.style.SUCCESS(f"CSV files saved to: {output_path}"))