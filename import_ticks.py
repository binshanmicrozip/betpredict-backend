import os
from decimal import Decimal, InvalidOperation

import django
import pandas as pd
from django.utils.dateparse import parse_datetime

print("STEP 1: import_price_ticks.py started")

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "betpredict_project.settings")
django.setup()

print("STEP 2: django setup completed")

from betapp.models import Market, Runner, PriceTick

# 🔥 FOLDER PATH (not file)
EXCEL_FOLDER = r"D:\Market Data\excel"
SHEET_NAME = "Tick Snapshots"
BATCH_SIZE = 5000


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

    dt = parse_datetime(value)
    if dt:
        return dt

    try:
        return pd.to_datetime(value).to_pydatetime()
    except:
        return None


# ---------- MAIN ----------
def process_file(file_path):
    print(f"\n📂 Processing file: {file_path}")

    try:
        df = pd.read_excel(file_path, sheet_name=SHEET_NAME)
    except Exception as e:
        print(f"❌ Error reading sheet: {e}")
        return

    df.columns = [str(col).strip() for col in df.columns]

    # ✅ preload markets once
    markets_map = {
        m.market_id: m
        for m in Market.objects.only("market_id", "market_start_time")
    }

    # ✅ preload runners once
    runners_map = {
        (r.market_id, r.selection_id): r
        for r in Runner.objects.only("id", "market_id", "selection_id")
    }

    created = 0
    skipped = 0
    batch = []

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        market_id = clean_id(row.get("Market ID"))
        selection_id = clean_bigint(row.get("Runner ID"))
        tick_time = clean_datetime(row.get("Tick Time"))
        ltp = clean_decimal(row.get("Price (LTP)"))
        snapshot = clean_nullable_string(row.get("Snapshot"))
        win_prob = clean_decimal(row.get("Win Prob %"))
        phase = clean_nullable_string(row.get("Phase"))

        if not market_id or selection_id is None or tick_time is None or ltp is None:
            skipped += 1
            continue

        market = markets_map.get(market_id)
        if not market:
            skipped += 1
            continue

        runner = runners_map.get((market_id, selection_id))
        if not runner:
            skipped += 1
            continue

        # ✅ year, month, day from Market.market_start_time
        market_start = market.market_start_time
        year = market_start.year if market_start else None
        month = market_start.month if market_start else None
        day = market_start.day if market_start else None

        batch.append(
            PriceTick(
                market=market,
                runner=runner,
                tick_time=tick_time,
                snapshot=snapshot,
                year=year,
                month=month,
                day=day,
                ltp=ltp,
                win_prob=win_prob,
                traded_volume=None,   # not available in this Excel sheet
                phase=phase,
            )
        )

        if len(batch) >= BATCH_SIZE:
            PriceTick.objects.bulk_create(
                batch,
                batch_size=BATCH_SIZE,
                ignore_conflicts=True
            )
            created += len(batch)
            print(f"✅ Batch inserted: {created}")
            batch = []

        if i % 5000 == 0:
            print(f"📊 Processed rows: {i}, Skipped: {skipped}")

    if batch:
        PriceTick.objects.bulk_create(
            batch,
            batch_size=BATCH_SIZE,
            ignore_conflicts=True
        )
        created += len(batch)

    print(f"✅ Done: Created={created}, Skipped={skipped}")


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