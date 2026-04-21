from django.core.management.base import BaseCommand, CommandError
from betapp.utils.import_patterns import bulk_insert_ipl_patterns


class Command(BaseCommand):
    help = "Import only IPL-related patterns from CSV into patterns table"

    def add_arguments(self, parser):
        parser.add_argument(
            "--file",
            type=str,
            required=True,
            help="Path to CSV file",
        )
        parser.add_argument(
            "--batch_size",
            type=int,
            default=1000,
            help="Bulk insert batch size",
        )
        parser.add_argument(
            "--ignore_conflicts",
            action="store_true",
            help="Ignore duplicate conflicts during insert",
        )

    def handle(self, *args, **options):
        file_path = options["file"]
        batch_size = options["batch_size"]
        ignore_conflicts = options["ignore_conflicts"]

        self.stdout.write(self.style.SUCCESS("Starting IPL-only pattern import..."))

        try:
            bulk_insert_ipl_patterns(
                csv_file_path=file_path,
                batch_size=batch_size,
                ignore_conflicts=ignore_conflicts,
            )
        except Exception as exc:
            raise CommandError(f"Import failed: {exc}")

        self.stdout.write(self.style.SUCCESS("IPL-only pattern import completed successfully."))