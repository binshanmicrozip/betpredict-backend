import pandas as pd
from decimal import Decimal
from datetime import datetime, timezone

from django.utils.dateparse import parse_datetime

from betapp.models import Pattern, Market, Runner


IPL_KEYWORDS = [
    "ipl",
    "indian premier league",
    "chennai super kings",
    "mumbai indians",
    "royal challengers bengaluru",
    "royal challengers bangalore",
    "kolkata knight riders",
    "sunrisers hyderabad",
    "rajasthan royals",
    "delhi capitals",
    "punjab kings",
    "kings xi punjab",
    "gujarat titans",
    "lucknow super giants",
]


def clean_str(val):
    if pd.isna(val):
        return None
    val = str(val).strip()
    return val if val else None


def to_decimal(val):
    if pd.isna(val) or val == "":
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


def to_bool(val):
    if pd.isna(val) or val == "":
        return None

    if isinstance(val, bool):
        return val

    val = str(val).strip().lower()

    if val in ["true", "1", "yes", "y"]:
        return True
    if val in ["false", "0", "no", "n"]:
        return False

    return None


def to_datetime(val):
    if pd.isna(val) or val == "":
        return None

    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()

    parsed = parse_datetime(str(val))
    return parsed


def ms_to_datetime(val):
    if pd.isna(val) or val == "":
        return None

    try:
        return datetime.fromtimestamp(int(val) / 1000, tz=timezone.utc)
    except Exception:
        return None


def is_ipl_event(event_name):
    """
    Returns True if event_name looks IPL-related.
    """
    if not event_name:
        return False

    text = str(event_name).strip().lower()

    for keyword in IPL_KEYWORDS:
        if keyword in text:
            return True

    return False


def filter_ipl_patterns_df(df):
    """
    Keep only IPL-related rows using event_name.
    """
    if "event_name" not in df.columns:
        raise ValueError("CSV must contain 'event_name' column for IPL filtering")

    filtered_df = df[df["event_name"].apply(is_ipl_event)].copy()
    return filtered_df


def bulk_insert_ipl_patterns(csv_file_path, batch_size=1000, ignore_conflicts=False):
    df = pd.read_csv(csv_file_path)

    print(f"Total CSV rows: {len(df)}")
    print("CSV columns:", df.columns.tolist())

    required_columns = [
        "market_id",
        "runner_id",
        "runner_name",
        "event_name",
        "market_time",
        "winner",
        "runner_won",
        "window_start_ms",
        "window_end_ms",
        "window_start_utc",
        "price_at_start",
        "price_at_end",
        "price_high",
        "price_low",
        "price_change_pct",
        "momentum",
        "volatility",
        "trend_slope",
        "max_drawdown",
        "tick_count",
        "duration_sec",
        "pattern_type",
        "label",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in CSV: {missing_columns}")

    # Step 1: Filter IPL only
    df_ipl = filter_ipl_patterns_df(df)

    print(f"IPL filtered rows: {len(df_ipl)}")
    print(f"Non-IPL removed rows: {len(df) - len(df_ipl)}")

    if df_ipl.empty:
        print("No IPL rows found. Nothing to insert.")
        return

    market_ids = set(Market.objects.values_list("market_id", flat=True))
    runner_ids = set(Runner.objects.values_list("runner_id", flat=True))

    objects_to_create = []
    inserted_count = 0
    skipped_count = 0
    invalid_market_count = 0
    invalid_runner_count = 0
    invalid_label_count = 0

    valid_labels = {"UP", "DOWN", "NEUTRAL"}

    for index, row in df_ipl.iterrows():
        market_id = clean_str(row.get("market_id"))
        runner_id = clean_str(row.get("runner_id"))
        label = clean_str(row.get("label"))

        if not market_id or market_id not in market_ids:
            skipped_count += 1
            invalid_market_count += 1
            print(f"Skipping row {index}: invalid market_id = {market_id}")
            continue

        if not runner_id or runner_id not in runner_ids:
            skipped_count += 1
            invalid_runner_count += 1
            print(f"Skipping row {index}: invalid runner_id = {runner_id}")
            continue

        if label not in valid_labels:
            skipped_count += 1
            invalid_label_count += 1
            print(f"Skipping row {index}: invalid label = {label}")
            continue

        obj = Pattern(
            market_id=market_id,
            runner_id=runner_id,
            runner_name=clean_str(row.get("runner_name")),
            event_name=clean_str(row.get("event_name")),
            market_time=to_datetime(row.get("market_time")),
            winner=clean_str(row.get("winner")),
            runner_won=to_bool(row.get("runner_won")),
            window_start=ms_to_datetime(row.get("window_start_ms")),
            window_end=ms_to_datetime(row.get("window_end_ms")),
            window_start_ms=int(row.get("window_start_ms")) if pd.notna(row.get("window_start_ms")) else None,
            window_end_ms=int(row.get("window_end_ms")) if pd.notna(row.get("window_end_ms")) else None,
            window_start_utc=to_datetime(row.get("window_start_utc")),
            price_at_start=to_decimal(row.get("price_at_start")),
            price_at_end=to_decimal(row.get("price_at_end")),
            price_high=to_decimal(row.get("price_high")),
            price_low=to_decimal(row.get("price_low")),
            price_change_pct=to_decimal(row.get("price_change_pct")),
            momentum=to_decimal(row.get("momentum")),
            volatility=to_decimal(row.get("volatility")),
            trend_slope=to_decimal(row.get("trend_slope")),
            max_drawdown=to_decimal(row.get("max_drawdown")),
            tick_count=int(row.get("tick_count")) if pd.notna(row.get("tick_count")) else None,
            duration_sec=to_decimal(row.get("duration_sec")),
            pattern_type=clean_str(row.get("pattern_type")),
            label=label,
        )

        objects_to_create.append(obj)

        if len(objects_to_create) >= batch_size:
            Pattern.objects.bulk_create(
                objects_to_create,
                batch_size=batch_size,
                ignore_conflicts=ignore_conflicts,
            )
            inserted_count += len(objects_to_create)
            print(f"Inserted so far: {inserted_count}")
            objects_to_create = []

    if objects_to_create:
        Pattern.objects.bulk_create(
            objects_to_create,
            batch_size=batch_size,
            ignore_conflicts=ignore_conflicts,
        )
        inserted_count += len(objects_to_create)

    print("\n========== FINAL SUMMARY ==========")
    print(f"Total CSV rows: {len(df)}")
    print(f"IPL rows found: {len(df_ipl)}")
    print(f"Inserted rows: {inserted_count}")
    print(f"Skipped rows: {skipped_count}")
    print(f"Invalid market rows: {invalid_market_count}")
    print(f"Invalid runner rows: {invalid_runner_count}")
    print(f"Invalid label rows: {invalid_label_count}")
    print("===================================")