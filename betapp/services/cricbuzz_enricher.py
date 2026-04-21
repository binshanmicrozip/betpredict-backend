from betapp.utils.player_normalizer import map_role


def update_player_from_cricbuzz(player, payload: dict):
    changed = False

    profile_id = payload.get("cricbuzz_profile_id")
    profile_url = payload.get("cricbuzz_profile_url")
    country = payload.get("country")
    raw_role = payload.get("role")

    if profile_id and not player.cricbuzz_profile_id:
        player.cricbuzz_profile_id = profile_id
        changed = True

    if profile_url and not player.cricbuzz_profile_url:
        player.cricbuzz_profile_url = profile_url
        changed = True

    if country and (not player.country or player.country == "Unknown"):
        player.country = country
        changed = True

    mapped_role = map_role(raw_role)
    if mapped_role != "Unknown" and (not player.role or player.role == "Unknown"):
        player.role = mapped_role
        changed = True

    if changed:
        player.save()

    return changed