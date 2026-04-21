import os
from pathlib import Path
from django.core.management.base import BaseCommand
from django.utils import timezone
from betapp.models import LiveMatchState


class Command(BaseCommand):
    help = "Check status of live cricket data storage"

    def handle(self, *args, **options):
        self.stdout.write("=== LIVE CRICKET DATA STATUS ===\n")

        # Check database records
        recent_cutoff = timezone.now() - timezone.timedelta(minutes=5)
        recent_db_records = LiveMatchState.objects.filter(fetched_at__gte=recent_cutoff).count()

        self.stdout.write(f"Recent LiveMatchState records (last 5 min): {recent_db_records}")

        if recent_db_records > 0:
            latest = LiveMatchState.objects.filter(fetched_at__gte=recent_cutoff).order_by('-fetched_at').first()
            self.stdout.write(f"Latest DB record: {latest.match.match_id} - {latest.score}/{latest.wickets} ({latest.overs})")
            self.stdout.write(f"Updated: {latest.fetched_at}")
        self.stdout.write("")

        # Check CSV files
        csv_dir = Path("live_match_data")
        if csv_dir.exists():
            csv_files = list(csv_dir.glob("live_match_*.csv"))
            self.stdout.write(f"CSV files found: {len(csv_files)}")

            current_time = timezone.now()
            recent_csv_files = []
            for csv_file in csv_files:
                mtime = csv_file.stat().st_mtime
                file_time = timezone.datetime.fromtimestamp(mtime, tz=timezone.get_current_timezone())
                age_minutes = (current_time - file_time).total_seconds() / 60

                if age_minutes <= 5:
                    recent_csv_files.append((csv_file, file_time, age_minutes))

            if recent_csv_files:
                self.stdout.write(f"Recent CSV files (last 5 min): {len(recent_csv_files)}")
                for csv_file, file_time, age in recent_csv_files[:3]:  # Show first 3
                    self.stdout.write(f"  {csv_file.name} - {age:.1f} min ago")

                    # Show last few lines of the file
                    try:
                        with open(csv_file, 'r', encoding='utf-8') as f:
                            lines = f.readlines()
                            if len(lines) > 1:  # Has header + data
                                last_line = lines[-1].strip()
                                self.stdout.write(f"    Last entry: {last_line[:100]}...")
                    except Exception as e:
                        self.stdout.write(f"    Error reading file: {e}")
            else:
                self.stdout.write("No recent CSV files (last 5 min)")
        else:
            self.stdout.write("CSV directory 'live_match_data' does not exist")

        self.stdout.write("")

        # Summary
        has_recent_data = recent_db_records > 0 or (csv_dir.exists() and any(
            csv_file.stat().st_mtime > (current_time - timezone.timedelta(minutes=5)).timestamp()
            for csv_file in csv_dir.glob("live_match_*.csv")
        ))

        if has_recent_data:
            self.stdout.write(self.style.SUCCESS("✓ Live cricket data is being collected"))
        else:
            self.stdout.write(self.style.WARNING("✗ No recent live cricket data found"))
            self.stdout.write("  - Make sure poll_live_match command is running")
            self.stdout.write("  - Check that the match has started and data is available")