import json
import redis
from django.conf import settings

from betapp.models import Player
from betapp.services.player_stats_service import get_situation_stats
from betapp.utils.player_profile_utils import normalize_player_name


r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)


# -------------------------------
# KEY FORMAT
# -------------------------------
def get_player_cache_key(player_name: str, phase: str, innings_type: str) -> str:
    normalized_name = normalize_player_name(player_name)
    return f"cricbuzz:player:{normalized_name}:{phase}:{innings_type}"


# -------------------------------
# STORE PLAYER STATS IN REDIS
# -------------------------------
def refresh_player_cache(player_name: str, phase: str, innings_type: str) -> dict:
    normalized_name = normalize_player_name(player_name)

    player = Player.objects.filter(normalized_name=normalized_name).first()

    # ❌ Player not found
    if not player:
        payload = {
            "found": False,
            "player_name": player_name,
            "normalized_name": normalized_name,
            "phase": phase,
            "innings_type": innings_type,
        }

        r.set(get_player_cache_key(player_name, phase, innings_type), json.dumps(payload))
        return payload

    # ✅ Get stats from DB
    stats = get_situation_stats(player.player_id, phase, innings_type)

    payload = {
        "found": True,
        "player_id": player.player_id,
        "player_name": player.player_name,
        "normalized_name": normalized_name,
        "phase": phase,
        "innings_type": innings_type,

        # stats fields (used by ML model)
        "matches_played": stats.get("matches_played", 0),
        "runs": stats.get("runs", 0),
        "balls": stats.get("balls", 0),
        "boundary_pct": stats.get("boundary_pct", 0),
        "strike_rate": stats.get("strike_rate", 0),
        "dismissal_rate": stats.get("dismissal_rate", 0),
        "wickets_lost": stats.get("wickets_lost", 0),
    }

    r.set(get_player_cache_key(player_name, phase, innings_type), json.dumps(payload))
    return payload


# -------------------------------
# READ FROM REDIS
# -------------------------------
def get_cached_player_stats(player_name: str, phase: str, innings_type: str) -> dict:
    raw = r.get(get_player_cache_key(player_name, phase, innings_type))

    if not raw:
        return {}

    try:
        return json.loads(raw)
    except Exception:
        return {}