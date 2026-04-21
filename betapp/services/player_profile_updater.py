from django.db import transaction

from betapp.models import Player, MatchPlayer
from betapp.services.cricbuzz_enrichment import CricbuzzProfileService
from betapp.utils.player_profile_utils import normalize_player_name, map_role


class PlayerProfileUpdater:
    def __init__(self):
        self.cricbuzz_service = CricbuzzProfileService()

    def get_earliest_ipl_match_date(self, player: Player):
        first_match = (
            MatchPlayer.objects
            .filter(player=player)
            .select_related("match")
            .order_by("match__match_date")
            .first()
        )

        if first_match and first_match.match and first_match.match.match_date:
            return first_match.match.match_date

        return None

    @transaction.atomic
    def update_player(self, player: Player, force: bool = False):
        changed_fields = []

        normalized_name = normalize_player_name(player.player_name)
        if player.normalized_name != normalized_name:
            player.normalized_name = normalized_name
            changed_fields.append("normalized_name")

        should_fetch_cricbuzz = force or any([
            not player.country,
            not player.cricbuzz_profile_id,
            not player.cricbuzz_profile_url,
            not player.debut_year,
            not player.last_season,
            not player.role or player.role == "Unknown",
        ])

        fetched = None
        if should_fetch_cricbuzz:
            fetched = self.cricbuzz_service.fetch_full_profile(player.player_name)

            if fetched.get("status") == "found":
                profile_data = fetched.get("profile_data", {}) or {}
                ipl_data = fetched.get("ipl_data", {}) or {}

                country = profile_data.get("country")
                raw_role = profile_data.get("role")
                mapped_role = map_role(raw_role)
                cricbuzz_profile_id = profile_data.get("cricbuzz_profile_id")
                cricbuzz_profile_url = profile_data.get("cricbuzz_profile_url")

                if country and (not player.country or force):
                    player.country = country
                    changed_fields.append("country")

                if mapped_role != "Unknown" and (not player.role or player.role == "Unknown" or force):
                    player.role = mapped_role
                    changed_fields.append("role")

                if cricbuzz_profile_id and (not player.cricbuzz_profile_id or force):
                    player.cricbuzz_profile_id = str(cricbuzz_profile_id)
                    changed_fields.append("cricbuzz_profile_id")

                if cricbuzz_profile_url and (not player.cricbuzz_profile_url or force):
                    player.cricbuzz_profile_url = cricbuzz_profile_url
                    changed_fields.append("cricbuzz_profile_url")

                cricbuzz_debut_year = ipl_data.get("debut_year")
                cricbuzz_last_season = ipl_data.get("last_season")

                if cricbuzz_debut_year and (not player.debut_year or force):
                    player.debut_year = cricbuzz_debut_year
                    changed_fields.append("debut_year")

                if cricbuzz_last_season and (not player.last_season or cricbuzz_last_season > player.last_season or force):
                    player.last_season = cricbuzz_last_season
                    changed_fields.append("last_season")

        earliest_match_date = self.get_earliest_ipl_match_date(player)
        if earliest_match_date and (not player.ipl_debut or force):
            player.ipl_debut = earliest_match_date
            if "ipl_debut" not in changed_fields:
                changed_fields.append("ipl_debut")

            if (not player.debut_year or force):
                player.debut_year = earliest_match_date.year
                if "debut_year" not in changed_fields:
                    changed_fields.append("debut_year")

        if changed_fields:
            changed_fields = list(dict.fromkeys(changed_fields))
            player.save(update_fields=changed_fields)

        return {
            "player_id": player.player_id,
            "player_name": player.player_name,
            "status": "updated" if changed_fields else "no_change",
            "updated_fields": changed_fields,
            "country": player.country,
            "role": player.role,
            "ipl_debut": player.ipl_debut,
            "debut_year": player.debut_year,
            "last_season": player.last_season,
            "cricbuzz_profile_id": player.cricbuzz_profile_id,
            "cricbuzz_profile_url": player.cricbuzz_profile_url,
            "fetched": fetched,
        }