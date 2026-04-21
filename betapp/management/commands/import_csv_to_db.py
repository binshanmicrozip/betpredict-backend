import os

from django.conf import settings
from django.core.management.base import BaseCommand

from betapp.services.csv_db_importer import import_cricsheet_csv_to_db


class Command(BaseCommand):
    help = "Import Cricsheet parsed CSV into database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--csv-path",
            type=str,
            default=os.path.join(settings.BASE_DIR, "output", "cricsheet_parsed.csv"),
            help="Path to cricsheet parsed CSV file",
        )

    def handle(self, *args, **options):
        csv_path = options["csv_path"]

        if not os.path.exists(csv_path):
            self.stdout.write(self.style.ERROR(f"CSV file not found: {csv_path}"))
            return

        result = import_cricsheet_csv_to_db(csv_path)

        self.stdout.write(self.style.SUCCESS("===== IMPORT RESULT ====="))
        self.stdout.write(self.style.SUCCESS(str(result)))