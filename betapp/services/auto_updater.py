import io
import os
import json
import zipfile
import hashlib
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from django.conf import settings

from betapp.services.csv_db_importer import import_cricsheet_csv_to_db


CONFIG = {
    "cricsheet_raw_dir": os.path.join(settings.BASE_DIR, "data", "cricsheet"),
    "cricsheet_parsed": os.path.join(settings.BASE_DIR, "output", "cricsheet_parsed.csv"),
    "processed_registry": os.path.join(settings.BASE_DIR, "data", "processed_matches.json"),
    "log_file": os.path.join(settings.BASE_DIR, "logs", "daily_update.log"),
    "cricsheet_url": "https://cricsheet.org/downloads/ipl_male_json.zip",
    "download_headers": {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/zip,*/*",
        "Referer": "https://cricsheet.org/",
    },
    "download_timeout_sec": 120,
}


def setup_logging():
    os.makedirs(os.path.dirname(CONFIG["log_file"]), exist_ok=True)
    logger = logging.getLogger("betpredict_auto_updater")

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        file_handler = logging.FileHandler(CONFIG["log_file"], encoding="utf-8")
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger


log = setup_logging()


def load_registry():
    path = CONFIG["processed_registry"]
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed_hashes": [], "last_run": None, "total_matches_processed": 0}


def save_registry(registry):
    path = CONFIG["processed_registry"]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2)


def file_hash(content: bytes) -> str:
    return hashlib.md5(content).hexdigest()


def download_and_extract_new_matches():
    log.info("=" * 60)
    log.info("STEP 1 - Download and extract new Cricsheet matches")
    log.info("=" * 60)

    response = requests.get(
        CONFIG["cricsheet_url"],
        headers=CONFIG["download_headers"],
        timeout=CONFIG["download_timeout_sec"],
    )
    response.raise_for_status()

    registry = load_registry()
    known_hashes = set(registry["processed_hashes"])

    raw_dir = CONFIG["cricsheet_raw_dir"]
    os.makedirs(raw_dir, exist_ok=True)

    new_files = []
    skipped = 0

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        all_json = [f for f in zf.namelist() if f.endswith(".json")]
        log.info(f"Total files in zip: {len(all_json)}")

        for filename in all_json:
            content = zf.read(filename)
            h = file_hash(content)

            if h in known_hashes:
                skipped += 1
                continue

            out_path = os.path.join(raw_dir, os.path.basename(filename))
            with open(out_path, "wb") as f:
                f.write(content)

            new_files.append(out_path)
            known_hashes.add(h)

    registry["processed_hashes"] = list(known_hashes)
    registry["last_run"] = datetime.now().isoformat()
    registry["total_matches_processed"] = len(known_hashes)
    save_registry(registry)

    log.info(f"New files extracted: {len(new_files)}")
    log.info(f"Already known files: {skipped}")

    return new_files


def safe_int(value, default=None):
    try:
        if value in ("", None):
            return default
        return int(value)
    except Exception:
        return default


def parse_match_file(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    info = data.get("info", {})
    innings_list = data.get("innings", [])

    match_id = Path(filepath).stem
    match_date = info.get("dates", [None])[0]
    venue = info.get("venue", "")
    match_number = info.get("event", {}).get("match_number", None)
    season = safe_int(info.get("season"), None)

    teams = info.get("teams", [])
    toss_winner = info.get("toss", {}).get("winner", "")
    toss_decision = info.get("toss", {}).get("decision", "")
    outcome_winner = info.get("outcome", {}).get("winner", "")
    outcome_by = info.get("outcome", {}).get("by", {})
    outcome_runs = outcome_by.get("runs", None)
    outcome_wickets = outcome_by.get("wickets", None)

    records = []

    for innings_idx, innings in enumerate(innings_list, start=1):
        batting_team = innings.get("team", "")
        bowling_team_list = [t for t in teams if t != batting_team]
        bowling_team = bowling_team_list[0] if bowling_team_list else ""

        for over_data in innings.get("overs", []):
            over_number = over_data.get("over", 0)
            legal_ball_count = 0

            for delivery in over_data.get("deliveries", []):
                batter = delivery.get("batter", "")
                bowler = delivery.get("bowler", "")
                non_striker = delivery.get("non_striker", "")

                runs = delivery.get("runs", {})
                runs_batter = runs.get("batter", 0)
                runs_extras = runs.get("extras", 0)
                runs_total = runs.get("total", 0)

                extras = delivery.get("extras", {})
                is_wide = "wides" in extras
                is_noball = "noballs" in extras
                is_legal = not is_wide and not is_noball

                wickets = delivery.get("wickets", [])
                wickets_this_ball = len(wickets)
                wicket_kind = wickets[0].get("kind", "") if wickets else ""
                player_out = wickets[0].get("player_out", "") if wickets else ""

                ball_in_over = legal_ball_count
                if is_legal:
                    legal_ball_count += 1

                records.append({
                    "match_id": match_id,
                    "match_date": match_date,
                    "season": season,
                    "venue": venue,
                    "match_number": match_number,
                    "batting_team": batting_team,
                    "bowling_team": bowling_team,
                    "toss_winner": toss_winner,
                    "toss_decision": toss_decision,
                    "outcome_winner": outcome_winner,
                    "outcome_runs": outcome_runs,
                    "outcome_wickets": outcome_wickets,
                    "innings": innings_idx,
                    "over": over_number,
                    "ball_in_over": ball_in_over,
                    "batter": batter,
                    "bowler": bowler,
                    "non_striker": non_striker,
                    "runs_batter": runs_batter,
                    "runs_extras": runs_extras,
                    "runs_total": runs_total,
                    "is_wide": int(is_wide),
                    "is_noball": int(is_noball),
                    "is_legal": int(is_legal),
                    "wickets_this_ball": wickets_this_ball,
                    "wicket_kind": wicket_kind,
                    "player_out": player_out,
                })

    return records


def parse_and_append_new_matches(new_files):
    if not new_files:
        log.info("No new match files to parse.")
        return 0

    log.info("=" * 60)
    log.info("STEP 2 - Parse new JSON files and append CSV")
    log.info("=" * 60)

    all_new_records = []

    for i, filepath in enumerate(new_files, start=1):
        try:
            records = parse_match_file(filepath)
            all_new_records.extend(records)
            log.info(f"[{i}/{len(new_files)}] Parsed {Path(filepath).name} -> {len(records)} rows")
        except Exception as e:
            log.warning(f"Failed parsing {filepath}: {e}")

    if not all_new_records:
        log.info("No new parsed rows found.")
        return 0

    new_df = pd.DataFrame(all_new_records)
    parsed_path = CONFIG["cricsheet_parsed"]
    os.makedirs(os.path.dirname(parsed_path), exist_ok=True)

    if os.path.exists(parsed_path):
        existing_df = pd.read_csv(parsed_path, low_memory=False)
        existing_match_ids = set(existing_df["match_id"].astype(str).unique())
        new_df = new_df[~new_df["match_id"].astype(str).isin(existing_match_ids)]

        if new_df.empty:
            log.info("All parsed matches already exist in CSV.")
            return 0

        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    combined_df.to_csv(parsed_path, index=False)
    log.info(f"CSV updated: {parsed_path}")
    log.info(f"New deliveries appended: {len(new_df)}")

    return len(new_df)


def run_daily_sync():
    start_time = datetime.now()

    status = {
        "new_files": 0,
        "new_csv_rows": 0,
        "db_result": None,
    }

    log.info("")
    log.info("█" * 60)
    log.info(f"BETPREDICT DAILY SYNC STARTED: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("█" * 60)

    new_files = download_and_extract_new_matches()
    status["new_files"] = len(new_files)

    new_csv_rows = parse_and_append_new_matches(new_files)
    status["new_csv_rows"] = new_csv_rows

    log.info("=" * 60)
    log.info("STEP 3 - Import CSV to database")
    log.info("=" * 60)

    db_result = import_cricsheet_csv_to_db(CONFIG["cricsheet_parsed"])
    status["db_result"] = db_result

    elapsed = (datetime.now() - start_time).seconds

    log.info("")
    log.info("█" * 60)
    log.info("DAILY SYNC COMPLETE")
    log.info(f"New JSON files   : {status['new_files']}")
    log.info(f"New CSV rows     : {status['new_csv_rows']}")
    log.info(f"DB result        : {status['db_result']}")
    log.info(f"Time taken       : {elapsed}s")
    log.info("█" * 60)

    return status