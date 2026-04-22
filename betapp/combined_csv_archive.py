import csv
import json
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
ARCHIVE_DIR = BASE_DIR / "live_csv_archive"
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


def safe_json(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


class CombinedCsvArchiveManager:
    """
    Save one combined CSV row per unique:
    source_match_id + market_id + runner_id + ball_key
    """

    def __init__(self):
        self.seen_row_keys = set()

    def get_csv_path(self, source_match_id, market_id=None, runner_id=None):
        if market_id and runner_id:
            return ARCHIVE_DIR / f"combined_history_{source_match_id}_{market_id}_{runner_id}.csv"
        return ARCHIVE_DIR / f"combined_history_{source_match_id}.csv"

    def ensure_csv(self, file_path, fieldnames):
        if not os.path.exists(file_path):
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

    def append_row(self, file_path, fieldnames, row):
        self.ensure_csv(file_path, fieldnames)
        with open(file_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(row)

    def make_row_key(self, source_match_id, market_id, runner_id, ball_key):
        return f"{source_match_id}:{market_id}:{runner_id}:{ball_key}"

    def preload_existing_keys(self, file_path):
        if not os.path.exists(file_path):
            return

        with open(file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                source_match_id = str(row.get("source_match_id") or "").strip()
                market_id = str(row.get("market_id") or "").strip()
                runner_id = str(row.get("runner_id") or "").strip()
                ball_key = str(row.get("ball_key") or "").strip()

                if source_match_id and market_id and runner_id and ball_key:
                    row_key = self.make_row_key(source_match_id, market_id, runner_id, ball_key)
                    self.seen_row_keys.add(row_key)

    def save_combined_row(self, payload: dict):
        if not payload:
            return False

        source_match_id = str(payload.get("source_match_id") or "").strip()
        market_id = str(payload.get("market_id") or "").strip()
        runner_id = str(payload.get("runner_id") or "").strip()
        ball_key = str(payload.get("ball_key") or "").strip()

        if not source_match_id or not market_id or not runner_id or not ball_key:
            return False

        row_key = self.make_row_key(source_match_id, market_id, runner_id, ball_key)

        if row_key in self.seen_row_keys:
            return False

        self.seen_row_keys.add(row_key)

        cricket = payload.get("cricket") or {}
        price = payload.get("price") or {}
        prediction = payload.get("prediction") or {}
        pattern = prediction.get("pattern") or {}
        raw_json = cricket.get("raw_json") or {}

        file_path = self.get_csv_path(source_match_id, market_id, runner_id)

        fieldnames = [
            "source_match_id",
            "market_id",
            "runner_id",
            "ball_key",
            "innings",
            "score",
            "score_num",
            "wickets",
            "overs",
            "overs_float",
            "crr",
            "rrr",
            "status",
            "state",
            "toss",
            "target",
            "phase",
            "innings_type",
            "recent",
            "last5_runs",
            "last5_wkts",
            "last3_runs",
            "latest_ball",
            "b1_name",
            "b1_runs",
            "b1_balls",
            "b1_4s",
            "b1_6s",
            "b1_sr",
            "b2_name",
            "b2_runs",
            "b2_balls",
            "b2_4s",
            "b2_6s",
            "b2_sr",
            "bw1_name",
            "bw1_overs",
            "bw1_runs",
            "bw1_wkts",
            "bw1_eco",
            "p_runs",
            "p_balls",
            "ltp",
            "prev_ltp",
            "tv",
            "market_updated_at",
            "signal",
            "signal_source",
            "mode",
            "price_going",
            "confidence",
            "p_back",
            "p_lay",
            "reason",
            "pattern_name",
            "pattern_category",
            "pattern_category_label",
            "pattern_detail",
            "pattern_description",
            "pattern_price_direction",
            "pattern_avg_price_move",
            "pattern_historical_accuracy",
            "pattern_color",
            "cricket_json",
            "price_json",
            "prediction_json",
            "raw_json",
        ]

        row = {
            "source_match_id": source_match_id,
            "market_id": market_id,
            "runner_id": runner_id,
            "ball_key": ball_key,
            "innings": cricket.get("innings"),
            "score": cricket.get("score"),
            "score_num": cricket.get("score_num"),
            "wickets": cricket.get("wickets"),
            "overs": cricket.get("overs"),
            "overs_float": cricket.get("overs_float"),
            "crr": cricket.get("crr"),
            "rrr": cricket.get("rrr"),
            "status": cricket.get("status"),
            "state": cricket.get("state"),
            "toss": cricket.get("toss"),
            "target": cricket.get("target"),
            "phase": cricket.get("phase"),
            "innings_type": cricket.get("innings_type"),
            "recent": cricket.get("recent"),
            "last5_runs": cricket.get("last5_runs"),
            "last5_wkts": cricket.get("last5_wkts"),
            "last3_runs": cricket.get("last3_runs"),
            "latest_ball": cricket.get("latest_ball"),
            "b1_name": cricket.get("b1_name"),
            "b1_runs": cricket.get("b1_runs"),
            "b1_balls": cricket.get("b1_balls"),
            "b1_4s": cricket.get("b1_4s"),
            "b1_6s": cricket.get("b1_6s"),
            "b1_sr": cricket.get("b1_sr"),
            "b2_name": cricket.get("b2_name"),
            "b2_runs": cricket.get("b2_runs"),
            "b2_balls": cricket.get("b2_balls"),
            "b2_4s": cricket.get("b2_4s"),
            "b2_6s": cricket.get("b2_6s"),
            "b2_sr": cricket.get("b2_sr"),
            "bw1_name": cricket.get("bw1_name"),
            "bw1_overs": cricket.get("bw1_overs"),
            "bw1_runs": cricket.get("bw1_runs"),
            "bw1_wkts": cricket.get("bw1_wkts"),
            "bw1_eco": cricket.get("bw1_eco"),
            "p_runs": cricket.get("p_runs"),
            "p_balls": cricket.get("p_balls"),
            "ltp": price.get("ltp"),
            "prev_ltp": price.get("prev_ltp"),
            "tv": price.get("tv"),
            "market_updated_at": price.get("updated_at"),
            "signal": prediction.get("signal"),
            "signal_source": prediction.get("signal_source"),
            "mode": prediction.get("mode"),
            "price_going": prediction.get("price_going"),
            "confidence": prediction.get("confidence"),
            "p_back": prediction.get("p_back"),
            "p_lay": prediction.get("p_lay"),
            "reason": prediction.get("reason"),
            "pattern_name": pattern.get("pattern_name"),
            "pattern_category": pattern.get("pattern_category"),
            "pattern_category_label": pattern.get("pattern_category_label"),
            "pattern_detail": pattern.get("pattern_detail"),
            "pattern_description": pattern.get("pattern_description"),
            "pattern_price_direction": pattern.get("price_direction"),
            "pattern_avg_price_move": pattern.get("avg_price_move"),
            "pattern_historical_accuracy": pattern.get("historical_accuracy"),
            "pattern_color": pattern.get("color"),
            "cricket_json": safe_json(cricket),
            "price_json": safe_json(price),
            "prediction_json": safe_json(prediction),
            "raw_json": safe_json(raw_json),
        }

        self.append_row(file_path, fieldnames, row)
        return True