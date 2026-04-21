from django.core.management.base import BaseCommand

from betapp.services.auto_updater import run_daily_sync


class Command(BaseCommand):
    help = "Download new Cricsheet matches, update CSV, and import into database"

    def handle(self, *args, **options):
        result = run_daily_sync()
        self.stdout.write(self.style.SUCCESS("===== DAILY SYNC RESULT ====="))
        self.stdout.write(self.style.SUCCESS(str(result)))