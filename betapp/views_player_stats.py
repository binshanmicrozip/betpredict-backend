from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes

from betapp.models import Player
from betapp.utils.player_profile_utils import normalize_player_name
from betapp.services.player_stats_service import (
    get_situation_stats,
    get_role_based_stats,
)


class PlayerStatsView(APIView):
    @extend_schema(
        tags=["Players"],
        summary="Get player stats",
        parameters=[
            OpenApiParameter(
                name="player_name",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Player name"
            ),
        ],
        responses={200: dict, 400: dict, 404: dict},
    )
    def get(self, request):
        player_name = request.GET.get("player_name")

        if not player_name:
            return Response(
                {"error": "player_name is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        normalized_name = normalize_player_name(player_name)
        player = Player.objects.filter(normalized_name=normalized_name).first()

        if not player:
            return Response(
                {"error": "player not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        season = 2026
        role_stats = get_role_based_stats(player, season)

        response_data = {
            "player_name": player.player_name,
            "country": player.country,
            "role": player.role,
            "ipl_debut": player.ipl_debut,
            "debut_year": player.debut_year,
            "last_season": player.last_season,
            "career_batting_stats": role_stats["career_batting_stats"],
            "career_bowling_stats": role_stats["career_bowling_stats"],
            "current_ipl_batting_stats": role_stats["current_ipl_batting_stats"],
            "current_ipl_bowling_stats": role_stats["current_ipl_bowling_stats"],
        }

        return Response(response_data, status=status.HTTP_200_OK)