from django.core.management.base import BaseCommand
from django.utils.timezone import make_aware
from datetime import datetime
from zoneinfo import ZoneInfo

from betapp.models import IPLMatch
from betapp.ipl_data.ipl_matches_2026 import IPL_MATCHES_2026

IST = ZoneInfo("Asia/Kolkata")


def parse_teams(match_name: str):
    parts = match_name.split(" vs ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return match_name, ""


def parse_datetime(dt_str: str) -> datetime:
    naive = datetime.fromisoformat(dt_str)
    return make_aware(naive, IST)


class Command(BaseCommand):
    help = "Seed IPL 2026 schedule — matches by team+date OR creates new rows"

    def handle(self, *args, **options):
        created_count = 0
        updated_count = 0
        not_found = []

        for match in IPL_MATCHES_2026:
            match_id       = match["matchId"]
            match_name     = match["matchName"]
            team1, team2   = parse_teams(match_name)
            open_date_time = parse_datetime(match["openDateTime"])
            match_date     = open_date_time.date()

            # First try: find by match_id (our Betfair ID)
            existing = IPLMatch.objects.filter(match_id=match_id).first()

            # Second try: find by team1 + team2 + date (existing rows with different ID)
            if not existing:
                existing = IPLMatch.objects.filter(
                    season=2026,
                    match_date=match_date,
                    team1__icontains=team1.split()[0],   # match first word e.g. "Mumbai"
                    team2__icontains=team2.split()[0],
                ).first()

            # Third try: flip team1/team2 (home/away might be reversed)
            if not existing:
                existing = IPLMatch.objects.filter(
                    season=2026,
                    match_date=match_date,
                    team1__icontains=team2.split()[0],
                    team2__icontains=team1.split()[0],
                ).first()

            if existing:
                # Update the existing row with our schedule data
                existing.match_name     = match_name
                existing.open_date      = match["openDate"]
                existing.open_date_time = open_date_time
                existing.team1          = team1
                existing.team2          = team2
                existing.save()
                updated_count += 1
                self.stdout.write(
                    f"[UPDATED] {existing.match_id} ← betfair_id={match_id} | {match_name}"
                )
            else:
                # Create a new row with our Betfair match_id
                IPLMatch.objects.create(
                    match_id       = match_id,
                    season         = 2026,
                    match_name     = match_name,
                    open_date      = match["openDate"],
                    open_date_time = open_date_time,
                    match_date     = match_date,
                    team1          = team1,
                    team2          = team2,
                )
                created_count += 1
                not_found.append(f"{match_id} | {match_name}")
                self.stdout.write(self.style.SUCCESS(
                    f"[CREATED] {match_id} | {match_name}"
                ))

        self.stdout.write(self.style.SUCCESS(
            f"\nDone — Updated: {updated_count} | Created: {created_count}"
        ))

        if not_found:
            self.stdout.write(self.style.WARNING(
                f"\nThese had no existing match and were created fresh:"
            ))
            for item in not_found:
                self.stdout.write(f"  {item}")