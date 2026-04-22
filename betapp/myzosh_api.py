import re
import requests
from datetime import datetime, timezone


BASE_URL = "https://staging.myzosh.com/api"


def parse_dotnet_date(value):
    if not value:
        return None

    text = str(value).strip()
    match = re.search(r"/Date\((\d+)\)/", text)
    if not match:
        return None

    try:
        ms = int(match.group(1))
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except Exception:
        return None


class MyZoshAPI:
    def __init__(self, agent_code: str, secret_key: str, source_id: str | None = None):
        self.agent_code = (agent_code or "").strip()
        self.secret_key = (secret_key or "").strip()
        self.source_id = (source_id or "").strip() or None
        self.access_token = None

    def _post(self, endpoint: str, payload: dict) -> dict:
        url = f"{BASE_URL}/{endpoint}"
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        status = data.get("status", {})

        if status.get("code") != 200:
            raise Exception(f"{endpoint} failed: {status}")

        return data

    def get_access_token(self) -> str:
        payload = {
            "agent_code": self.agent_code,
            "secret_key": self.secret_key,
        }
        data = self._post("get_access_token", payload)
        token = data.get("data", {}).get("access_token")

        if not token:
            raise Exception("No access token returned from get_access_token")

        self.access_token = token
        print("[MyZoshAPI] ACCESS TOKEN GENERATED")
        return token

    def _auth_payload(self, extra: dict | None = None) -> dict:
        if not self.access_token:
            self.get_access_token()

        payload = {
            "access_token": self.access_token,
        }

        if self.source_id:
            payload["source_id"] = self.source_id

        if extra:
            payload.update(extra)

        return payload

    def get_sports(self) -> list[dict]:
        data = self._post("get_sports", self._auth_payload())
        sports = data.get("data", [])
        print(f"[MyZoshAPI] SPORTS FOUND: {len(sports)}")
        return sports

    def get_tournaments(self, sport_id: str) -> list[dict]:
        data = self._post(
            "get_tournaments",
            self._auth_payload({
                "sport_id": str(sport_id),
            }),
        )
        tournaments = data.get("data", [])
        print(f"[MyZoshAPI] TOURNAMENTS FOUND for sport_id={sport_id}: {len(tournaments)}")
        return tournaments

    def get_matches(self, sport_id: str, tournament_id: str) -> list[dict]:
        data = self._post(
            "get_matches",
            self._auth_payload({
                "sport_id": str(sport_id),
                "tournament_id": str(tournament_id),
            }),
        )
        matches = data.get("data", [])
        print(
            f"[MyZoshAPI] MATCHES FOUND for sport_id={sport_id}, "
            f"tournament_id={tournament_id}: {len(matches)}"
        )
        return matches

    def get_exch_markets(self, sport_id: str, tournament_id: str, match_id: str) -> list[dict]:
        data = self._post(
            "get_exch_markets",
            self._auth_payload({
                "sport_id": str(sport_id),
                "tournament_id": str(tournament_id),
                "match_id": str(match_id),
            }),
        )
        markets = data.get("data", [])
        print(
            f"[MyZoshAPI] EXCHANGE MARKETS FOUND for sport_id={sport_id}, "
            f"tournament_id={tournament_id}, match_id={match_id}: {len(markets)}"
        )
        return markets

    def discover_live_market_catalog(
        self,
        sport_ids: list[str] | None = None,
        only_with_market_count: bool = True,
    ) -> list[dict]:
        catalog = []
        seen_market_ids = set()
        now_utc = datetime.now(timezone.utc)

        sports = self.get_sports()

        if sport_ids:
            wanted = {str(x).strip() for x in sport_ids if str(x).strip()}
            sports = [s for s in sports if str(s.get("sport_id")) in wanted]

        for sport in sports:
            sport_id = str(sport.get("sport_id"))
            sport_name = str(sport.get("sport_name", "")).strip()
            sport_market_count = int(sport.get("market_count") or 0)

            if only_with_market_count and sport_market_count <= 0:
                print(f"[MyZoshAPI] SKIP SPORT {sport_name} ({sport_id}) market_count=0")
                continue

            print(f"[MyZoshAPI] SPORT => {sport_name} ({sport_id}) market_count={sport_market_count}")

            try:
                tournaments = self.get_tournaments(sport_id)
            except Exception as e:
                print(f"[MyZoshAPI] ERROR get_tournaments sport_id={sport_id}: {e}")
                continue

            for tournament in tournaments:
                tournament_id = str(tournament.get("tournament_id"))
                tournament_name = str(tournament.get("tournament_name", "")).strip()
                tournament_market_count = int(tournament.get("market_count") or 0)

                if only_with_market_count and tournament_market_count <= 0:
                    print(f"[MyZoshAPI] SKIP TOURNAMENT {tournament_name} ({tournament_id}) market_count=0")
                    continue

                try:
                    matches = self.get_matches(sport_id, tournament_id)
                except Exception as e:
                    print(
                        f"[MyZoshAPI] ERROR get_matches sport_id={sport_id}, "
                        f"tournament_id={tournament_id}: {e}"
                    )
                    continue

                for match in matches:
                    match_id = str(match.get("match_id"))
                    match_name = str(match.get("match_name", "")).strip()
                    match_market_count = int(match.get("market_count") or 0)
                    match_country_code = match.get("match_country_code")
                    match_timezone = match.get("match_timezone")
                    match_open_date_raw = match.get("match_open_date")
                    match_open_date = parse_dotnet_date(match_open_date_raw)

                    if only_with_market_count and match_market_count <= 0:
                        print(f"[MyZoshAPI] SKIP MATCH {match_name} ({match_id}) market_count=0")
                        continue

                    # Keep only already-open/live matches
                    if match_open_date and match_open_date > now_utc:
                        print(f"[MyZoshAPI] SKIP FUTURE MATCH {match_name} ({match_id})")
                        continue

                    print(
                        f"[MyZoshAPI] LIVE/OPEN MATCH => {match_name} ({match_id}) "
                        f"market_count={match_market_count}"
                    )

                    try:
                        exch_markets = self.get_exch_markets(sport_id, tournament_id, match_id)
                    except Exception as e:
                        print(
                            f"[MyZoshAPI] ERROR get_exch_markets sport_id={sport_id}, "
                            f"tournament_id={tournament_id}, match_id={match_id}: {e}"
                        )
                        continue

                    for market in exch_markets:
                        market_id = str(market.get("market_id") or "").strip()
                        if not market_id or market_id in seen_market_ids:
                            continue

                        seen_market_ids.add(market_id)

                        description = market.get("description") or {}
                        runners = market.get("runners") or []

                        enriched_runners = []
                        for runner in runners:
                            selection_id = str(runner.get("selection_id") or "").strip()
                            runner_name = str(runner.get("runner_name") or "").strip()
                            handicap = runner.get("handicap")

                            if not selection_id:
                                continue

                            enriched_runners.append({
                                "selection_id": selection_id,
                                "runner_name": runner_name,
                                "handicap": handicap,
                            })

                        catalog.append({
                            "market_id": market_id,
                            "event_id": match_id,
                            "event_name": match_name,
                            "sport_id": sport_id,
                            "sport_name": sport_name,
                            "tournament_id": tournament_id,
                            "tournament_name": tournament_name,
                            "country_code": match_country_code,
                            "timezone": match_timezone,
                            "market_name": market.get("market_name"),
                            "market_type": description.get("market_type") or "UNKNOWN",
                            "market_time_raw": description.get("market_time"),
                            "suspend_time_raw": description.get("suspend_time"),
                            "is_turn_in_play_enabled": bool(description.get("is_turn_in_play_enabled")),
                            "is_persistence_enabled": bool(description.get("is_persistence_enabled")),
                            "is_bsp_market": bool(description.get("is_bsp_market")),
                            "market_base_rate": description.get("market_base_rate"),
                            "regulator": description.get("regulator"),
                            "runners": enriched_runners,
                        })

        print(f"[MyZoshAPI] TOTAL UNIQUE LIVE MARKET IDS: {len(catalog)}")
        return catalog