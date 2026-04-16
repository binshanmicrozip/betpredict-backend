print("STEP 1: import_excel.py started")

import os
from decimal import Decimal, InvalidOperation

import django
import pandas as pd
from django.utils.dateparse import parse_datetime

print("STEP 2: libraries imported")

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "betpredict_project.settings")
django.setup()

print("STEP 3: django setup completed")

from betapp.models import Market

EXCEL_FOLDER = r"D:\Market Data\excel"
SHEET_NAME = "Markets"


def clean_string(value, default=""):
    if pd.isna(value):
        return default
    value = str(value).strip()
    if value.lower() == "nan":
        return default
    return value


def clean_nullable_string(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    if not value or value.lower() == "nan":
        return None
    return value


def clean_id(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    if not value or value.lower() == "nan":
        return None
    if value.endswith(".0"):
        value = value[:-2]
    return value


def clean_int(value, default=0):
    if pd.isna(value):
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def clean_decimal(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    if not value or value.lower() == "nan":
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError, TypeError):
        return None


def clean_bool(value):
    if pd.isna(value):
        return False
    value = str(value).strip().lower()
    return value in ["1", "1.0", "true", "yes", "y", "t"]


def clean_datetime(value):
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        try:
            if value.tzinfo is not None:
                value = value.tz_convert(None)
        except Exception:
            pass
        return value.to_pydatetime()

    value = str(value).strip()
    if not value or value.lower() == "nan":
        return None

    return parse_datetime(value)


def process_file(file_path):
    print(f"\n📂 Processing file: {file_path}")

    try:
        xl = pd.ExcelFile(file_path)
        print("Available sheets:", xl.sheet_names)
    except Exception as e:
        print(f"❌ ERROR opening Excel file: {e}")
        return

    try:
        df = pd.read_excel(file_path, sheet_name=SHEET_NAME)
    except Exception as e:
        print(f"❌ ERROR reading sheet '{SHEET_NAME}': {e}")
        return

    df.columns = [str(col).strip() for col in df.columns]

    print("Columns found:")
    print(list(df.columns))
    print(f"Total rows: {len(df)}")

    created_count = 0
    updated_count = 0
    skipped_count = 0
    error_count = 0

    for index, row in df.iterrows():
        excel_row_no = index + 2

        try:
            market_id = clean_id(row.get("Market ID"))
            market_start_time = clean_datetime(row.get("Market Start Time"))

            if not market_id:
                skipped_count += 1
                print(f"Skipped row {excel_row_no}: Market ID missing")
                continue

            if not market_start_time:
                skipped_count += 1
                print(f"Skipped row {excel_row_no}: Market Start Time missing/invalid")
                continue

            defaults = {
                "event_id": clean_id(row.get("Event ID")) or "",
                "event_name": clean_string(row.get("Event Name"), default=""),
                "market_name": clean_nullable_string(row.get("Market Name")),
                "market_type": clean_string(row.get("Market Type"), default=""),
                "event_type_id": clean_id(row.get("Sport ID")) or "",
                "country_code": clean_nullable_string(row.get("Country Code")),
                "timezone": clean_nullable_string(row.get("Timezone")),
                "market_open_date": clean_datetime(row.get("Market Open Date")),
                "market_start_time": market_start_time,
                "suspend_time": clean_datetime(row.get("Suspend Time")),
                "settled_time": clean_datetime(row.get("Settled Time")),
                "number_of_winners": clean_int(row.get("Number of Winners"), default=0),
                "number_of_active_runners": clean_int(row.get("Number of Active Runners"), default=0),
                "bet_delay": clean_int(row.get("Bet Delay"), default=0),
                "market_base_rate": clean_decimal(row.get("Market Base Rate")),
                "turn_in_play_enabled": clean_bool(row.get("Turn In Play Enabled")),
                "persistence_enabled": clean_bool(row.get("Persistence Enabled")),
                "bsp_market": clean_bool(row.get("BSP Market")),
                "bsp_reconciled": clean_bool(row.get("BSP Reconciled")),
                "cross_matching": clean_bool(row.get("Cross Matching")),
                "runners_voidable": clean_bool(row.get("Runners Voidable")),
                "complete": clean_bool(row.get("Complete")),
                "regulators": clean_nullable_string(row.get("Regulators")),
                "opening_status": clean_nullable_string(row.get("Opening Status")),
                "status": clean_string(row.get("Final Status"), default="OPEN"),
                "in_play_seen": clean_bool(row.get("In Play Seen")),
                "in_play_start_time": clean_datetime(row.get("In-Play Start Time")),
                "total_tick_messages": clean_int(row.get("Total Tick Messages"), default=0),
            }

            obj, created = Market.objects.update_or_create(
                market_id=market_id,
                defaults=defaults
            )

            if created:
                created_count += 1
            else:
                updated_count += 1

        except Exception as e:
            error_count += 1
            print(f"Error on row {excel_row_no}: {e}")

    print(f"✅ Done: Created={created_count}, Updated={updated_count}, Skipped={skipped_count}, Errors={error_count}")


def main():
    print(f"📁 Scanning folder: {EXCEL_FOLDER}")

    if not os.path.exists(EXCEL_FOLDER):
        print(f"❌ Folder not found: {EXCEL_FOLDER}")
        return

    files = [f for f in os.listdir(EXCEL_FOLDER) if f.lower().endswith(".xlsx")]

    if not files:
        print("❌ No Excel files found")
        return

    print(f"📊 Found {len(files)} Excel files")

    for file_name in files:
        file_path = os.path.join(EXCEL_FOLDER, file_name)
        process_file(file_path)

    print("\n🚀 All market files processed successfully")


if __name__ == "__main__":
    main()