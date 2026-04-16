import os
from decimal import Decimal, InvalidOperation

import django
import pandas as pd
from django.utils.dateparse import parse_datetime

print("STEP 1: import_runners.py started")

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "betpredict_project.settings")
django.setup()

print("STEP 2: django setup completed")

from betapp.models import Market, Runner

# 🔥 FOLDER PATH (not file)
EXCEL_FOLDER = r"D:\Market Data\excel"
SHEET_NAME = "Runners"


# ---------- CLEAN FUNCTIONS ----------
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


def clean_bigint(value):
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except:
        return None


def clean_int(value, default=0):
    if pd.isna(value):
        return default
    try:
        return int(float(value))
    except:
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


def clean_datetime(value):
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    value = str(value).strip()
    if not value or value.lower() == "nan":
        return None

    return parse_datetime(value)


# ---------- MAIN ----------
def process_file(file_path):
    print(f"\n📂 Processing file: {file_path}")

    try:
        df = pd.read_excel(file_path, sheet_name=SHEET_NAME)
    except Exception as e:
        print(f"❌ Error reading sheet: {e}")
        return

    df.columns = [str(col).strip() for col in df.columns]

    created = 0
    updated = 0
    skipped = 0

    for _, row in df.iterrows():

        market_id = clean_id(row.get("Market ID"))
        selection_id = clean_bigint(row.get("Runner ID"))

        if not market_id or selection_id is None:
            skipped += 1
            continue

        try:
            market = Market.objects.get(market_id=market_id)
        except Market.DoesNotExist:
            skipped += 1
            continue

        obj, is_created = Runner.objects.update_or_create(
            market=market,
            selection_id=selection_id,
            defaults={
                "runner_name": clean_string(row.get("Runner Name")),
                "sort_priority": clean_int(row.get("Sort Priority")),
                "status": "ACTIVE",
                "final_result": clean_nullable_string(row.get("Final Result")),
                "opening_price": clean_decimal(row.get("Opening Price")),
                "opening_win_prob": clean_decimal(row.get("Opening Win Prob %")),
                "closing_price": clean_decimal(row.get("Closing Price")),
                "closing_win_prob": clean_decimal(row.get("Closing Win Prob %")),
                "lowest_price": clean_decimal(row.get("Lowest Price")),
                "highest_price": clean_decimal(row.get("Highest Price")),
                "price_range": clean_decimal(row.get("Price Range")),
                "price_change": clean_decimal(row.get("Price Change (open→close)")),
                "total_ticks": clean_int(row.get("Total Ticks")),
                "pre_match_ticks": clean_int(row.get("Pre-Match Ticks")),
                "in_play_ticks": clean_int(row.get("In-Play Ticks")),
                "first_tick_time": clean_datetime(row.get("First Tick Time")),
                "last_tick_time": clean_datetime(row.get("Last Tick Time")),
                "in_play_first_price": clean_decimal(row.get("In-Play First Price")),
                "in_play_last_price": clean_decimal(row.get("In-Play Last Price")),
                "in_play_min_price": clean_decimal(row.get("In-Play Min Price")),
                "in_play_max_price": clean_decimal(row.get("In-Play Max Price")),
            }
        )

        if is_created:
            created += 1
        else:
            updated += 1

    print(f"✅ Done: Created={created}, Updated={updated}, Skipped={skipped}")


def main():
    print(f"📁 Scanning folder: {EXCEL_FOLDER}")

    if not os.path.exists(EXCEL_FOLDER):
        print("❌ Folder not found")
        return

    files = [f for f in os.listdir(EXCEL_FOLDER) if f.endswith(".xlsx")]

    if not files:
        print("❌ No Excel files found")
        return

    print(f"📊 Found {len(files)} Excel files")

    for file_name in files:
        file_path = os.path.join(EXCEL_FOLDER, file_name)
        process_file(file_path)

    print("\n🚀 All files processed successfully")


if __name__ == "__main__":
    main()