from .cricbuzz_enrichment import CricbuzzProfileService


cricbuzz_service = CricbuzzProfileService()


def fetch_player_from_cricbuzz(player_name: str, sleep_sec: float = 0.4) -> dict:
    """Fetch Cricbuzz profile data for a player name."""
    if not player_name:
        return {}

    result = cricbuzz_service.fetch_full_profile(player_name, sleep_sec=sleep_sec)
    if not result or result.get("status") != "found":
        return {}

    profile_data = result.get("profile_data", {}) or {}
    profile_data["status"] = result.get("status")
    profile_data["player_name"] = player_name
    return profile_data
