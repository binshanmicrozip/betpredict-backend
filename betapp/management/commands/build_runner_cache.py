# management/commands/build_runner_cache.py
import json
import csv
from pathlib import Path
from django.core.management.base import BaseCommand
from django.conf import settings
from betapp.myzosh_api import MyZoshAPI


RUNNER_CACHE_FILE = Path(settings.BASE_DIR) / "live_csv_archive" / "runner_name_cache.json"

TARGET_TOURNAMENT_ID = "101480"


class Command(BaseCommand):
    help = "Build runner name cache for tournament_id=101480"

    def handle(self, *args, **kwargs):
        csv_dir = Path(settings.BASE_DIR) / "live_csv_archive"

        # ── Step 1: collect all unique market_ids from CSV rows ────────────
        market_ids = set()
        for csv_file in csv_dir.glob("*.csv"):
            try:
                with open(csv_file, "r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        mid = row.get("market_id", "").strip()
                        if mid:
                            market_ids.add(mid)
                        break
            except Exception as e:
                self.stdout.write(f"[SKIP] {csv_file.name}: {e}")

        self.stdout.write(f"Found {len(market_ids)} unique market_ids in CSV")

        # ── Step 2: init API ───────────────────────────────────────────────
        api = MyZoshAPI(
            agent_code=settings.MYZOSH_AGENT_CODE,
            secret_key=settings.MYZOSH_SECRET_KEY,
            source_id=getattr(settings, "MYZOSH_SOURCE_ID", None),
        )

        try:
            api.get_access_token()
            self.stdout.write("[OK] Access token obtained")
        except Exception as e:
            self.stdout.write(f"[ERROR] get_access_token: {e}")
            return

        # ── Step 3: load existing cache ────────────────────────────────────
        cache = {}
        if RUNNER_CACHE_FILE.exists():
            try:
                cache = json.loads(RUNNER_CACHE_FILE.read_text(encoding="utf-8"))
                self.stdout.write(f"Loaded existing cache: {len(cache)} markets")
            except Exception:
                cache = {}

        # ── Step 4: fetch matches only for tournament_id=101480 ───────────
        try:
            # get sports to find cricket sport_id
            sports = api.get_sports()
            self.stdout.write(f"Sports found: {len(sports)}")

            for sport in sports:
                sport_id = str(sport.get("sport_id"))
                sport_name = str(sport.get("sport_name", ""))

                try:
                    tournaments = api.get_tournaments(sport_id)
                except Exception as e:
                    self.stdout.write(f"[SKIP] get_tournaments sport_id={sport_id}: {e}")
                    continue

                # filter only our target tournament
                target = [t for t in tournaments if str(t.get("tournament_id")) == TARGET_TOURNAMENT_ID]
                if not target:
                    continue

                self.stdout.write(f"[FOUND] Tournament {TARGET_TOURNAMENT_ID} in sport {sport_name} ({sport_id})")

                try:
                    matches = api.get_matches(sport_id, TARGET_TOURNAMENT_ID)
                    self.stdout.write(f"Matches found: {len(matches)}")
                except Exception as e:
                    self.stdout.write(f"[ERROR] get_matches: {e}")
                    continue

                for match in matches:
                    match_id = str(match.get("match_id"))
                    match_name = str(match.get("match_name", ""))
                    self.stdout.write(f"  Match: {match_name} ({match_id})")

                    try:
                        exch_markets = api.get_exch_markets(sport_id, TARGET_TOURNAMENT_ID, match_id)
                    except Exception as e:
                        self.stdout.write(f"  [ERROR] get_exch_markets match_id={match_id}: {e}")
                        continue

                    for market in exch_markets:
                        market_id = str(market.get("market_id") or "").strip()
                        if not market_id:
                            continue

                        if market_id not in cache:
                            cache[market_id] = {}

                        for runner in market.get("runners", []):
                            selection_id = str(runner.get("selection_id") or "").strip()
                            runner_name = str(runner.get("runner_name") or "").strip()
                            if selection_id and runner_name:
                                cache[market_id][selection_id] = runner_name
                                self.stdout.write(f"    {market_id} → {selection_id}: {runner_name}")

                break  # found our tournament, no need to check other sports

        except Exception as e:
            self.stdout.write(f"[ERROR] {e}")
            return

        # ── Step 5: save cache ─────────────────────────────────────────────
        RUNNER_CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")
        self.stdout.write(f"\n[DONE] Cache saved: {len(cache)} markets → {RUNNER_CACHE_FILE}")