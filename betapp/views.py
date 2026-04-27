from datetime import datetime, timedelta
from django.db.models import Q
from django.utils.dateparse import parse_date
from rest_framework import viewsets
from rest_framework.decorators import action, api_view
from rest_framework.response import Response
from django.db.models import Sum, Max, Count, Q
from django.db.models.functions import Coalesce
from rest_framework.decorators import action

from .models import (Market, Runner, PriceTick, Pattern, Player,
    PlayerIPLTeam,
    IPLMatch,
    MatchPlayer,
    Delivery,
    PlayerMatchBatting,
    PlayerMatchBowling,
    PlayerSituationStats,LiveMarketTick,)
from .serializers import (MarketSerializer, RunnerSerializer, PriceTickSerializer, PatternSerializer, PlayerSerializer,
    PlayerIPLTeamSerializer,
    IPLMatchSerializer,
    MatchPlayerSerializer,
    DeliverySerializer,
    PlayerMatchBattingSerializer,
    PlayerMatchBowlingSerializer,
    PlayerSituationStatsSerializer,
    IPL2026PlayerListSerializer,
    PlayerProfileSerializer, LiveMarketTickSerializer)
from .pagination import StandardResultsSetPagination
from django_filters.rest_framework import DjangoFilterBackend

from django.http import JsonResponse
from django.db import connection
import redis
from celery import current_app
from betapp.tasks import insert_ticks

from rest_framework import viewsets, filters
from django_filters.rest_framework import DjangoFilterBackend
import json
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework_simplejwt.tokens import RefreshToken
from drf_spectacular.utils import (extend_schema, OpenApiExample,extend_schema_view,
    OpenApiParameter,
    OpenApiTypes,)

import redis
from django.conf import settings
from rest_framework import status

from .predictor import predict

from .serializers import LoginSerializer


r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True,
)


class LatestPredictionView(APIView):
    @extend_schema(
        tags=["Predictions"],
        summary="Get latest prediction",
        responses={200: dict, 404: dict},
    )
    def get(self, request):
        raw = r.get("prediction:latest")
        if not raw:
            return Response({"message": "No prediction yet"}, status=404)
        return Response(json.loads(raw))


@api_view(["GET"])
def latest_prediction(request):
    view = LatestPredictionView()
    return view.get(request)


class PredictSignalView(APIView):
    @extend_schema(
        tags=["Predictions"],
        summary="Generate prediction signal",
        request=dict,
        responses={200: dict, 400: dict, 500: dict},
    )
    def post(self, request):
        cricket = request.data.get("cricket", {})
        print("DDDDDDDDDDDDD",cricket)
        price = request.data.get("price", {})

        if not isinstance(cricket, dict):
            return Response(
                {"error": "cricket must be an object"},
                status=status.HTTP_400_BAD_REQUEST
            )

        if not isinstance(price, dict):
            return Response(
                {"error": "price must be an object"},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            result = predict(cricket, price)
            return Response(result, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


@api_view(["POST"])
def predict_signal(request):
    view = PredictSignalView()
    return view.post(request)



class LoginAPIView(APIView):
    permission_classes = [AllowAny]

    @extend_schema(
        request=LoginSerializer,
        responses={200: dict, 400: dict},
        description="Login with username and password and receive JWT access and refresh tokens.",
        examples=[
            OpenApiExample(
                "Login Request",
                value={
                    "username": "admin",
                    "password": "admin123"
                },
                request_only=True,
            ),
            OpenApiExample(
                "Login Success Response",
                value={
                    "success": True,
                    "message": "Login successful",
                    "data": {
                        "user_id": 1,
                        "username": "admin",
                        "email": "admin@example.com",
                        "is_staff": True,
                        "access": "your_access_token",
                        "refresh": "your_refresh_token"
                    }
                },
                response_only=True,
            ),
        ],
    )
    def post(self, request):
        serializer = LoginSerializer(data=request.data)

        if not serializer.is_valid():
            return Response(
                {
                    "success": False,
                    "errors": serializer.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        user = serializer.validated_data["user"]
        refresh = RefreshToken.for_user(user)

        return Response(
            {
                "success": True,
                "message": "Login successful",
                "data": {
                    "user_id": user.id,
                    "username": user.username,
                    "email": user.email,
                    "is_staff": user.is_staff,
                    "access": str(refresh.access_token),
                    "refresh": str(refresh),
                }
            },
            status=status.HTTP_200_OK
        )
    
from drf_spectacular.utils import extend_schema_view, extend_schema

@extend_schema_view(
    list=extend_schema(tags=["Markets"]),
    retrieve=extend_schema(tags=["Markets"]),
    create=extend_schema(tags=["Markets"]),
    update=extend_schema(tags=["Markets"]),
    partial_update=extend_schema(tags=["Markets"]),
    destroy=extend_schema(tags=["Markets"]),
)
class MarketViewSet(viewsets.ModelViewSet):
    queryset = Market.objects.all().order_by("-market_start_time")
    serializer_class = MarketSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [DjangoFilterBackend]

    def get_queryset(self):
        queryset = super().get_queryset()

        year = self.request.query_params.get("year")
        month = self.request.query_params.get("month")
        day = self.request.query_params.get("day")

        market_id = self.request.query_params.get("market_id")
        market_name = self.request.query_params.get("market_name")
        event_id = self.request.query_params.get("event_id")
        event_name = self.request.query_params.get("event_name")
        event_type_id = self.request.query_params.get("event_type_id")
        status_param = self.request.query_params.get("status")
        country_code = self.request.query_params.get("country_code")
        timezone = self.request.query_params.get("timezone")
        search = self.request.query_params.get("search")

        if year:
            queryset = queryset.filter(market_start_time__year=year)

        if month:
            queryset = queryset.filter(market_start_time__month=month)

        if day:
            queryset = queryset.filter(market_start_time__day=day)

        if market_id:
            queryset = queryset.filter(market_id__icontains=market_id)

        if market_name:
            queryset = queryset.filter(market_name__icontains=market_name)

        if event_id:
            queryset = queryset.filter(event_id__icontains=event_id)

        if event_name:
            queryset = queryset.filter(event_name__icontains=event_name)

        if event_type_id:
            queryset = queryset.filter(event_type_id=event_type_id)

        if status_param:
            queryset = queryset.filter(status__iexact=status_param)

        if country_code:
            queryset = queryset.filter(country_code__iexact=country_code)

        if timezone:
            queryset = queryset.filter(timezone__icontains=timezone)

        if search:
            queryset = queryset.filter(
                Q(market_id__icontains=search) |
                Q(market_name__icontains=search) |
                Q(event_id__icontains=search) |
                Q(event_name__icontains=search) |
                Q(status__icontains=search) |
                Q(country_code__icontains=search)
            )

        return queryset

@extend_schema_view(
    list=extend_schema(tags=["Runners"]),
    retrieve=extend_schema(tags=["Runners"]),
    create=extend_schema(tags=["Runners"]),
    update=extend_schema(tags=["Runners"]),
    partial_update=extend_schema(tags=["Runners"]),
    destroy=extend_schema(tags=["Runners"]),
)

class RunnerViewSet(viewsets.ModelViewSet):
    queryset = Runner.objects.select_related("market").all().order_by(
        "market__market_start_time", "sort_priority"
    )
    serializer_class = RunnerSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [DjangoFilterBackend]

    filterset_fields = [
        "selection_id",
        "status",
        "final_result",
        "market__market_id",
        "market__event_id",
        "market__event_type_id",
        "market__country_code",
    ]

    def get_queryset(self):
        queryset = super().get_queryset()

        market_id = self.request.query_params.get("market_id")
        runner_name = self.request.query_params.get("runner_name")
        event_name = self.request.query_params.get("event_name")
        market_name = self.request.query_params.get("market_name")
        year = self.request.query_params.get("year")
        month = self.request.query_params.get("month")
        day = self.request.query_params.get("day")
        search = self.request.query_params.get("search")

        if market_id:
            queryset = queryset.filter(market__market_id__icontains=market_id)

        if runner_name:
            queryset = queryset.filter(runner_name__icontains=runner_name)

        if event_name:
            queryset = queryset.filter(market__event_name__icontains=event_name)

        if market_name:
            queryset = queryset.filter(market__market_name__icontains=market_name)

        if year:
            queryset = queryset.filter(market__market_start_time__year=year)

        if month:
            queryset = queryset.filter(market__market_start_time__month=month)

        if day:
            queryset = queryset.filter(market__market_start_time__day=day)

        if search:
            queryset = queryset.filter(
                Q(runner_name__icontains=search) |
                Q(market__market_id__icontains=search) |
                Q(market__event_name__icontains=search) |
                Q(market__market_name__icontains=search)
            )

        return queryset
    
@extend_schema_view(
    list=extend_schema(tags=["Price Ticks"]),
    retrieve=extend_schema(tags=["Price Ticks"]),
    create=extend_schema(tags=["Price Ticks"]),
    update=extend_schema(tags=["Price Ticks"]),
    partial_update=extend_schema(tags=["Price Ticks"]),
    destroy=extend_schema(tags=["Price Ticks"]),
)

    
class PriceTickViewSet(viewsets.ModelViewSet):
    queryset = PriceTick.objects.select_related("market", "runner").all().order_by("-tick_time")
    serializer_class = PriceTickSerializer
    pagination_class = StandardResultsSetPagination
    filter_backends = [DjangoFilterBackend]

    filterset_fields = [
        "runner_id",
        "year",
        "month",
        "day",
        "phase",
        "snapshot",
        "runner__selection_id",
        "market__market_id",
    ]

    def get_queryset(self):
        queryset = super().get_queryset()

        market_id = self.request.query_params.get("market_id")
        runner_id = self.request.query_params.get("runner_id")
        selection_id = self.request.query_params.get("selection_id")
        runner_name = self.request.query_params.get("runner_name")
        market_name = self.request.query_params.get("market_name")
        event_name = self.request.query_params.get("event_name")

        year = self.request.query_params.get("year")
        month = self.request.query_params.get("month")
        day = self.request.query_params.get("day")
        week = self.request.query_params.get("week")

        phase = self.request.query_params.get("phase")
        snapshot = self.request.query_params.get("snapshot")
        search = self.request.query_params.get("search")

        start_date = self.request.query_params.get("start_date")
        end_date = self.request.query_params.get("end_date")

        if market_id:
            queryset = queryset.filter(market__market_id__icontains=market_id)

        if runner_id:
            queryset = queryset.filter(runner_id=runner_id)

        if selection_id:
            queryset = queryset.filter(runner__selection_id=selection_id)

        if runner_name:
            queryset = queryset.filter(runner__runner_name__icontains=runner_name)

        if market_name:
            queryset = queryset.filter(market__market_name__icontains=market_name)

        if event_name:
            queryset = queryset.filter(market__event_name__icontains=event_name)

        if year:
            queryset = queryset.filter(year=year)

        if month:
            queryset = queryset.filter(month=month)

        if day:
            queryset = queryset.filter(day=day)

        if week:
            queryset = queryset.filter(tick_time__week=week)

        if phase:
            queryset = queryset.filter(phase__iexact=phase)

        if snapshot:
            queryset = queryset.filter(snapshot__icontains=snapshot)

        if start_date:
            queryset = queryset.filter(tick_time__date__gte=start_date)

        if end_date:
            queryset = queryset.filter(tick_time__date__lte=end_date)

        if search:
            queryset = queryset.filter(
                Q(market__market_id__icontains=search) |
                Q(market__market_name__icontains=search) |
                Q(market__event_name__icontains=search) |
                Q(runner__runner_name__icontains=search) |
                Q(snapshot__icontains=search) |
                Q(phase__icontains=search)
            )

        return queryset

    @action(detail=False, methods=["get"], url_path="by-week")
    def by_week(self, request):
        queryset = self.get_queryset()

        year = request.query_params.get("year")
        week = request.query_params.get("week")

        if year:
            queryset = queryset.filter(year=year)

        if week:
            queryset = queryset.filter(tick_time__week=week)

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    @action(detail=False, methods=["get"], url_path="latest")
    def latest_ticks(self, request):
        queryset = PriceTick.objects.select_related("market", "runner").all().order_by("-tick_time")

        market_id = request.query_params.get("market_id")
        selection_id = request.query_params.get("selection_id")
        runner_name = request.query_params.get("runner_name")

        if market_id:
            queryset = queryset.filter(market__market_id__icontains=market_id)

        if selection_id:
            queryset = queryset.filter(runner__selection_id=selection_id)

        if runner_name:
            queryset = queryset.filter(runner__runner_name__icontains=runner_name)

        queryset = queryset[:100]

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)
    


class LiveMarketTickViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = LiveMarketTick.objects.all().order_by("-publish_time_utc")
    serializer_class = LiveMarketTickSerializer


    
@extend_schema_view(
    list=extend_schema(tags=["Patterns"]),
    retrieve=extend_schema(tags=["Patterns"]),
    create=extend_schema(tags=["Patterns"]),
    update=extend_schema(tags=["Patterns"]),
    partial_update=extend_schema(tags=["Patterns"]),
    destroy=extend_schema(tags=["Patterns"]),
)

class PatternViewSet(viewsets.ModelViewSet):
    queryset = Pattern.objects.select_related("market", "runner", "feature_vector").all().order_by("-id")
    serializer_class = PatternSerializer
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]

    filterset_fields = [
        "label",
        "pattern_type",
        "market",
        "runner",
        "runner_won",
    ]

    search_fields = [
        "runner_name",
        "event_name",
        "winner",
        "pattern_type",
        "label",
    ]

    ordering_fields = [
        "id",
        "market_time",
        "window_start",
        "window_end",
        "price_change_pct",
        "momentum",
        "volatility",
        "trend_slope",
        "max_drawdown",
        "tick_count",
        "duration_sec",
        "created_at",
    ]

class IngestTicksView(APIView):
    @extend_schema(
        tags=["Price Ticks"],
        summary="Ingest batch of price ticks",
        request=list,
        responses={200: dict, 400: dict},
    )
    def post(self, request):
        ticks = request.data

        if not isinstance(ticks, list):
            return Response({"error": "Expected list of ticks"}, status=400)

        task = insert_ticks.delay(ticks)

        return Response({
            "message": "Tick batch queued",
            "task_id": task.id,
            "count": len(ticks)
        })


@api_view(["POST"])
def ingest_ticks(request):
    view = IngestTicksView()
    return view.post(request)







def health_check(request):
    db_status = "ok"
    redis_status = "ok"
    celery_status = "ok"

    # DB check
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1;")
            cursor.fetchone()
    except Exception as e:
        db_status = f"error: {str(e)}"

    # Redis check
    try:
        r = redis.Redis(host="127.0.0.1", port=6379, db=0)
        r.ping()
    except Exception as e:
        redis_status = f"error: {str(e)}"

    # Celery broker check
    try:
        current_app.control.inspect(timeout=1).ping()
    except Exception as e:
        celery_status = f"error: {str(e)}"

    overall = "ok"
    if "error" in db_status or "error" in redis_status or "error" in celery_status:
        overall = "degraded"

    return JsonResponse({
        "status": overall,
        "database": db_status,
        "redis": redis_status,
        "celery": celery_status,
    })



@extend_schema_view(
    list=extend_schema(
        summary="List all players"
    ),
    retrieve=extend_schema(
        summary="Retrieve player by player_id",
        parameters=[
            OpenApiParameter(
                name="player_id",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.PATH,
                description="Player ID string. Example: 1c2a64cd or viratkohli_88733497",
            )
        ],
    ),
)
class PlayerViewSet(viewsets.ModelViewSet):
    queryset = Player.objects.all().order_by("player_name")
    serializer_class = PlayerSerializer
    lookup_field = "player_id"
    lookup_value_regex = "[^/]+"

    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ["role", "country", "debut_year", "last_season"]
    search_fields = ["player_name", "normalized_name", "cricbuzz_profile_id"]
    ordering_fields = ["player_name", "debut_year", "last_season", "created_at"]

    @extend_schema(
        tags=["Players"],
        parameters=[
            OpenApiParameter(
                name="player_id",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.PATH,
                required=True,
                description="Player id, for example 1c2a64cd",
            ),
        ],
    )
    @action(detail=False, methods=["get"], url_path=r"by-player/(?P<player_id>[^/.]+)")
    def by_player(self, request, player_id=None):
        obj = self.get_queryset().filter(player_id=player_id).first()
        if not obj:
            return Response({"detail": "Player not found"}, status=404)
        return Response(self.get_serializer(obj).data)

    @extend_schema(
        tags=["Players"],
        responses=IPL2026PlayerListSerializer(many=True),
    )
    @action(detail=False, methods=["get"], url_path="ipl-2026")
    def ipl_2026(self, request):
        season = 2026
        current_team_map = {
            row["player_id"]: row["team_name"]
            for row in PlayerIPLTeam.objects.filter(season=season, is_current=True).values("player_id", "team_name")
        }

        matches_map = {
            row["player_id"]: row["cnt"]
            for row in MatchPlayer.objects.filter(match__season=season)
            .values("player_id")
            .annotate(cnt=Count("match_id", distinct=True))
        }

        players = Player.objects.filter(ipl_teams__season=season).distinct().order_by("player_name")
        data = []
        for p in players:
            data.append({
                "player_id": p.player_id,
                "player_name": p.player_name,
                "role": p.role,
                "country": p.country,
                "current_team": current_team_map.get(p.player_id),
                "matches_played": matches_map.get(p.player_id, 0),
                "ipl_debut": p.ipl_debut,
                "last_season": p.last_season,
            })
        return Response(data)

 
    @extend_schema(
        tags=["Players"],
        parameters=[
            OpenApiParameter(
                name="player_id",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.PATH,
                required=True,
                description="Player id",
            ),
        ],
        responses=PlayerProfileSerializer,
    )
    @action(detail=False, methods=["get"], url_path=r"profile/(?P<player_id>[^/.]+)")
    def profile(self, request, player_id=None):
        player = Player.objects.filter(player_id=player_id).first()
        if not player:
            return Response({"detail": "Player not found"}, status=404)

        current_team = (
            PlayerIPLTeam.objects.filter(player=player, season=2026, is_current=True)
            .values_list("team_name", flat=True)
            .first()
        )

        teams_played_for = list(
            PlayerIPLTeam.objects.filter(player=player)
            .order_by("season", "team_name")
            .values_list("team_name", flat=True)
            .distinct()
        )

        total_matches = MatchPlayer.objects.filter(player=player).values("match").distinct().count()
        matches_in_2026 = MatchPlayer.objects.filter(player=player, match__season=2026).values("match").distinct().count()
        last_match_played = MatchPlayer.objects.filter(player=player).aggregate(dt=Max("match__match_date"))["dt"]

        batting_qs = PlayerMatchBatting.objects.filter(player=player)
        bowling_qs = PlayerMatchBowling.objects.filter(player=player)
        situation_qs = PlayerSituationStats.objects.filter(player=player).values(
            "phase", "innings_type", "matches_played", "runs", "balls",
            "strike_rate", "boundary_count", "boundary_pct", "wickets_lost", "dismissal_rate"
        )

        recent_matches = list(
            MatchPlayer.objects.filter(player=player)
            .select_related("match")
            .order_by("-match__match_date")[:10]
            .values(
                "match__match_id",
                "match__match_date",
                "match__team_home",
                "match__team_away",
                "team_name",
                "batting_position",
            )
        )

        batting_agg = batting_qs.aggregate(
            innings=Count("id"),
            total_runs=Sum("runs"),
            total_balls=Sum("balls_faced"),
            total_fours=Sum("fours"),
            total_sixes=Sum("sixes"),
            highest_score=Max("runs"),
        )

        not_outs = batting_qs.filter(is_not_out=True).count()
        batting_innings = batting_agg["innings"] or 0
        total_runs_value = batting_agg["total_runs"] or 0
        total_balls_value = batting_agg["total_balls"] or 0
        outs = batting_innings - not_outs if batting_innings >= not_outs else 0

        batting = {
            "innings": batting_innings,
            "total_runs": total_runs_value,
            "total_balls": total_balls_value,
            "total_fours": batting_agg["total_fours"] or 0,
            "total_sixes": batting_agg["total_sixes"] or 0,
            "highest_score": batting_agg["highest_score"] or 0,
            "not_outs": not_outs,
            "batting_average": round(total_runs_value / outs, 2) if outs > 0 else None,
            "strike_rate": round((total_runs_value / total_balls_value) * 100, 2) if total_balls_value > 0 else 0,
        }

        bowling_agg = bowling_qs.aggregate(
            innings=Count("id"),
            total_wickets=Sum("wickets"),
            total_runs_given=Sum("runs_given"),
            total_wides=Sum("wides"),
            total_noballs=Sum("noballs"),
            best_wickets=Max("wickets"),
        )

        total_wickets_value = bowling_agg["total_wickets"] or 0
        total_runs_given_value = bowling_agg["total_runs_given"] or 0

        bowling = {
            "innings": bowling_agg["innings"] or 0,
            "wickets": total_wickets_value,
            "runs_given": total_runs_given_value,
            "wides": bowling_agg["total_wides"] or 0,
            "noballs": bowling_agg["total_noballs"] or 0,
            "best_wickets": bowling_agg["best_wickets"] or 0,
            "bowling_average": round(total_runs_given_value / total_wickets_value, 2) if total_wickets_value > 0 else None,
        }

        data = {
            "player_id": player.player_id,
            "player_name": player.player_name,
            "country": player.country,
            "role": player.role,
            "ipl_debut": player.ipl_debut,
            "last_season": player.last_season,
            "current_team": current_team,
            "teams_played_for": teams_played_for,
            "total_matches": total_matches,
            "matches_in_2026": matches_in_2026,
            "last_match_played": last_match_played,
            "batting": batting,
            "bowling": bowling,
            "situation_stats": list(situation_qs),
            "recent_matches": recent_matches,
        }
        return Response(data)
@extend_schema_view(
    list=extend_schema(tags=["Players"]),
    retrieve=extend_schema(tags=["Players"]),
    create=extend_schema(tags=["Players"]),
    update=extend_schema(tags=["Players"]),
    partial_update=extend_schema(tags=["Players"]),
    destroy=extend_schema(tags=["Players"]),
)
class PlayerIPLTeamViewSet(viewsets.ModelViewSet):
    queryset = PlayerIPLTeam.objects.select_related("player").all().order_by("-season", "team_name")
    serializer_class = PlayerIPLTeamSerializer
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ["season", "team_name", "team_short", "is_current", "player"]
    search_fields = ["team_name", "team_short", "player__player_name", "player__player_id"]
    ordering_fields = ["season", "team_name"]


@extend_schema_view(
    list=extend_schema(tags=["Players"]),
    retrieve=extend_schema(tags=["Players"]),
    create=extend_schema(tags=["Players"]),
    update=extend_schema(tags=["Players"]),
    partial_update=extend_schema(tags=["Players"]),
    destroy=extend_schema(tags=["Players"]),
)
class IPLMatchViewSet(viewsets.ModelViewSet):
    queryset = IPLMatch.objects.all().order_by("-match_date", "match_number")
    serializer_class = IPLMatchSerializer
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]

    filterset_fields = [
        "season",
        "match_date",
        "team1",
        "team2",
        "toss_winner",
        "toss_decision",
        "venue",
        "status",
    ]

    search_fields = [
        "match_id",
        "team1",
        "team2",
        "toss_winner",
        "toss_decision",
        "venue",
        "status",
    ]

    ordering_fields = ["match_date", "season", "match_number"]


@extend_schema_view(
    list=extend_schema(tags=["Players"]),
    retrieve=extend_schema(tags=["Players"]),
    create=extend_schema(tags=["Players"]),
    update=extend_schema(tags=["Players"]),
    partial_update=extend_schema(tags=["Players"]),
    destroy=extend_schema(tags=["Players"]),
)
class MatchPlayerViewSet(viewsets.ModelViewSet):
    queryset = MatchPlayer.objects.select_related("match", "player").all().order_by("match__match_date")
    serializer_class = MatchPlayerSerializer
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]

    filterset_fields = [
        "match",
        "player",
    ]

    search_fields = [
        "player__player_name",
        "player__player_id",
        "match__match_id",
        "match__team1",
        "match__team2",
    ]

    ordering_fields = ["match__match_date"]


@extend_schema_view(
    list=extend_schema(tags=["Players"]),
    retrieve=extend_schema(tags=["Players"]),
    create=extend_schema(tags=["Players"]),
    update=extend_schema(tags=["Players"]),
    partial_update=extend_schema(tags=["Players"]),
    destroy=extend_schema(tags=["Players"]),
)
class DeliveryViewSet(viewsets.ModelViewSet):
    queryset = Delivery.objects.select_related(
        "match",
        "batter",
        "bowler",
        "non_striker",
        "player_out",
    ).all().order_by("match_id", "innings", "over_number", "ball_number", "id")

    serializer_class = DeliverySerializer
    filterset_fields = [
        "match",
        "match_id",
        "innings",
        "over_number",
        "ball_number",
        "batter",
        "bowler",
        "player_out",
        "is_wicket",
        "extra_type",
    ]
    search_fields = [
        "match__match_id",
        "batter__player_name",
        "bowler__player_name",
        "non_striker__player_name",
        "player_out__player_name",
        "wicket_kind",
    ]
    ordering_fields = ["innings", "over_number", "ball_number", "id"]

@extend_schema_view(
    list=extend_schema(tags=["Players"]),
    retrieve=extend_schema(tags=["Players"]),
    create=extend_schema(tags=["Players"]),
    update=extend_schema(tags=["Players"]),
    partial_update=extend_schema(tags=["Players"]),
    destroy=extend_schema(tags=["Players"]),
)
class PlayerMatchBattingViewSet(viewsets.ModelViewSet):
    queryset = PlayerMatchBatting.objects.select_related("match", "player").all().order_by("-runs")
    serializer_class = PlayerMatchBattingSerializer
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]

    filterset_fields = [
        "match",
        "player",
        "innings",
        "is_not_out",
        "dismissal_kind",
    ]

    search_fields = [
        "player__player_name",
        "player__player_id",
        "match__match_id",
        "match__team1",
        "match__team2",
    ]

    ordering_fields = ["runs", "balls_faced", "strike_rate"]


@extend_schema_view(
    list=extend_schema(tags=["Players"]),
    retrieve=extend_schema(tags=["Players"]),
    create=extend_schema(tags=["Players"]),
    update=extend_schema(tags=["Players"]),
    partial_update=extend_schema(tags=["Players"]),
    destroy=extend_schema(tags=["Players"]),
)
class PlayerMatchBowlingViewSet(viewsets.ModelViewSet):
    queryset = PlayerMatchBowling.objects.select_related("match", "player").all().order_by("-wickets")
    serializer_class = PlayerMatchBowlingSerializer
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = [
        "match",
        "player",
        "innings",
    ]
    search_fields = [
        "player__player_name",
        "player__player_id",
        "match__match_id",
        "match__team_home",
        "match__team_away",
    ]
    ordering_fields = ["wickets", "economy", "runs_given", "overs_bowled"]


@extend_schema_view(
    list=extend_schema(tags=["Players"]),
    retrieve=extend_schema(tags=["Players"]),
    create=extend_schema(tags=["Players"]),
    update=extend_schema(tags=["Players"]),
    partial_update=extend_schema(tags=["Players"]),
    destroy=extend_schema(tags=["Players"]),
)
class PlayerSituationStatsViewSet(viewsets.ModelViewSet):
    queryset = PlayerSituationStats.objects.select_related("player").all().order_by("player__player_name")
    serializer_class = PlayerSituationStatsSerializer
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ["player", "phase", "innings_type"]
    search_fields = ["player__player_name", "player__player_id"]
    ordering_fields = ["matches_played", "runs", "strike_rate", "boundary_pct", "dismissal_rate"]

from .models import LiveMatchState, LiveDelivery
from .serializers import LiveMatchStateSerializer, LiveDeliverySerializer


class LiveMatchStateViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = LiveMatchState.objects.select_related("match").all().order_by("-fetched_at")
    serializer_class = LiveMatchStateSerializer

    def get_queryset(self):
        qs = super().get_queryset()
        match_id = self.request.query_params.get("match_id")
        source_match_id = self.request.query_params.get("source_match_id")
        status_value = self.request.query_params.get("status")

        if match_id:
            qs = qs.filter(match__match_id=match_id)
        if source_match_id:
            qs = qs.filter(source_match_id=source_match_id)
        if status_value:
            qs = qs.filter(status__icontains=status_value)

        return qs

    @action(detail=False, methods=["get"], url_path="latest")
    def latest(self, request):
        match_id = request.query_params.get("match_id")
        qs = self.get_queryset()

        if match_id:
            qs = qs.filter(match__match_id=match_id)

        obj = qs.order_by("-fetched_at").first()
        if not obj:
            return Response({"detail": "No live match state found"}, status=404)

        return Response(self.get_serializer(obj).data)


class LiveDeliveryViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = LiveDelivery.objects.select_related("match", "batter", "bowler", "non_striker").all().order_by(
        "-updated_at", "-live_delivery_id"
    )
    serializer_class = LiveDeliverySerializer

    def get_queryset(self):
        qs = super().get_queryset()
        match_id = self.request.query_params.get("match_id")
        source_match_id = self.request.query_params.get("source_match_id")
        innings = self.request.query_params.get("innings")
        over_number = self.request.query_params.get("over_number")
        is_wicket = self.request.query_params.get("is_wicket")

        if match_id:
            qs = qs.filter(match__match_id=match_id)
        if source_match_id:
            qs = qs.filter(source_match_id=source_match_id)
        if innings:
            qs = qs.filter(innings=innings)
        if over_number:
            qs = qs.filter(over_number=over_number)
        if is_wicket in ("true", "false"):
            qs = qs.filter(is_wicket=(is_wicket == "true"))

        return qs

    @action(detail=False, methods=["get"], url_path="latest")
    def latest(self, request):
        match_id = request.query_params.get("match_id")
        qs = self.get_queryset()

        if match_id:
            qs = qs.filter(match__match_id=match_id)

        data = qs.order_by("-updated_at", "-live_delivery_id")[:20]
        return Response(self.get_serializer(data, many=True).data)

def safe_json(value):
    if not value:
        return {}

    try:
        return json.loads(value)
    except Exception:
        return {}


def safe_float(value):
    if value in ["", None]:
        return None

    try:
        return float(value)
    except Exception:
        return None


def safe_int(value):
    if value in ["", None]:
        return None

    try:
        return int(float(value))
    except Exception:
        return None


import csv
import json
from pathlib import Path
from collections import OrderedDict

from django.conf import settings
from rest_framework.decorators import api_view
from rest_framework.response import Response


def safe_json(value):
    if not value:
        return {}

    if isinstance(value, dict):
        return value

    try:
        return json.loads(value)
    except Exception:
        return {}


def safe_int(value):
    if value in [None, "", "null", "None"]:
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def safe_float(value):
    if value in [None, "", "null", "None"]:
        return None
    try:
        return float(value)
    except Exception:
        return None


def get_runner_name_from_price_json(price_json, runner_id):
    """Match runner_name from price_json.runners[] by selection_id"""
    for runner in price_json.get("runners", []):
        if str(runner.get("selection_id")) == str(runner_id):
            return runner.get("runner_name")
    return None

import csv
import json
from pathlib import Path
from collections import OrderedDict

import requests
from django.conf import settings
from rest_framework.decorators import api_view
from rest_framework.response import Response


def safe_json(value):
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception:
        return {}


def safe_int(value):
    if value in [None, "", "null", "None"]:
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def safe_float(value):
    if value in [None, "", "null", "None"]:
        return None
    try:
        return float(value)
    except Exception:
        return None


def fetch_market_runner_names(market_id):
    """
    Returns dict: { "22121561": "Delhi Capitals", "38528100": "Punjab Kings" }
    Reads from DB Runner table first (reliable), falls back to external API.
    """
    from betapp.models import Runner as RunnerModel

    runner_map = {}

    # Primary: DB lookup — always up to date after run_market_ws seeds the catalog
    try:
        qs = RunnerModel.objects.filter(market_id=str(market_id)).values("selection_id", "runner_name")
        for r in qs:
            rid = str(r["selection_id"])
            rname = (r["runner_name"] or "").strip()
            if rname and not rname.startswith("Runner "):
                runner_map[rid] = rname
    except Exception as e:
        print(f"[fetch_market_runner_names] DB lookup failed for market_id={market_id}: {e}")

    if runner_map:
        return runner_map

    # Fallback: external API
    try:
        response = requests.get(
            "https://staging.myzosh.com/api/get_exch_markets",
            params={"market_id": market_id},
            timeout=5,
        )
        data = response.json()
        markets = data if isinstance(data, list) else data.get("markets") or data.get("results") or [data]
        for market in markets:
            if str(market.get("market_id")) == str(market_id):
                for runner in market.get("runners", []):
                    selection_id = str(runner.get("selection_id"))
                    rname = runner.get("runner_name")
                    if selection_id and rname:
                        runner_map[selection_id] = rname
                break
    except Exception as e:
        print(f"[fetch_market_runner_names] API fallback failed for market_id={market_id}: {e}")

    return runner_map


@api_view(["GET"])
def match_csv_history(request):
    """
    API examples:

    /api/match-csv-history/?source_match_id=151878
    /api/match-csv-history/?market_id=1.257104784
    /api/match-csv-history/?runner_id=2954263

    Any one ID is enough.
    """

    source_match_id = request.GET.get("source_match_id")
    market_id = request.GET.get("market_id")
    runner_id = request.GET.get("runner_id")

    if not source_match_id and not market_id and not runner_id:
        return Response(
            {"error": "Pass any one: source_match_id OR market_id OR runner_id"},
            status=400,
        )

    csv_dir = Path(settings.BASE_DIR) / "live_csv_archive"

    if not csv_dir.exists():
        return Response(
            {"error": "CSV folder not found", "path": str(csv_dir)},
            status=404,
        )

    rows = []
    files_checked = []

    for csv_file in csv_dir.glob("*.csv"):
        files_checked.append(str(csv_file.name))
        try:
            with open(csv_file, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    matched = False
                    if source_match_id and str(row.get("source_match_id")) == str(source_match_id):
                        matched = True
                    if market_id and str(row.get("market_id")) == str(market_id):
                        matched = True
                    if runner_id and str(row.get("runner_id")) == str(runner_id):
                        matched = True
                    if matched:
                        rows.append(row)
        except Exception as e:
            print(f"[match_csv_history] CSV read error: {csv_file} -> {e}")

    rows.sort(
        key=lambda x: (
            safe_int(x.get("innings")) or 0,
            safe_float(x.get("overs_float")) or 0,
            x.get("ball_key") or "",
            x.get("runner_id") or "",
        )
    )

    # ── fetch runner names once per unique market_id ──────────────────────────
    unique_market_ids = set(row.get("market_id") for row in rows if row.get("market_id"))
    runner_name_cache = {}  # { market_id: { runner_id: runner_name } }
    for mid in unique_market_ids:
        runner_name_cache[mid] = fetch_market_runner_names(mid)
    # ──────────────────────────────────────────────────────────────────────────

    balls_map = OrderedDict()
    market = []
    patterns = []
    full_rows = []
    
    for row in rows:
        cricket_json = safe_json(row.get("cricket_json"))
        price_json = safe_json(row.get("price_json"))
        prediction_json = safe_json(row.get("prediction_json"))

        ball_key = row.get("ball_key")
        current_runner_id = row.get("runner_id")
        current_market_id = row.get("market_id")

        # get runner name from cache (API) first, then fallbacks
        runner_name = (
            row.get("runner_name")
            or runner_name_cache.get(current_market_id, {}).get(str(current_runner_id))
            or price_json.get("runner_name")
            or price_json.get("selection_name")
            or price_json.get("name")
            or price_json.get("runner")
        )

        # ── inject runner_name into price_json ────────────────────────────────
        price_json["runner_name"] = runner_name
        # ──────────────────────────────────────────────────────────────────────

        if ball_key not in balls_map:
            balls_map[ball_key] = {
                "source_match_id": row.get("source_match_id"),
                "market_id": current_market_id,
                "ball_key": ball_key,

                "innings": safe_int(row.get("innings")),
                "score": row.get("score"),
                "score_num": safe_int(row.get("score_num")),
                "wickets": safe_int(row.get("wickets")),
                "overs": row.get("overs"),
                "overs_float": safe_float(row.get("overs_float")),
                "crr": safe_float(row.get("crr")),
                "rrr": safe_float(row.get("rrr")),
                "status": row.get("status"),
                "state": row.get("state"),
                "toss": row.get("toss"),
                "target": safe_int(row.get("target")),
                "phase": row.get("phase"),
                "innings_type": row.get("innings_type"),
                "recent": row.get("recent"),

                "last5_runs": safe_float(row.get("last5_runs")),
                "last5_wkts": safe_float(row.get("last5_wkts")),
                "last3_runs": safe_float(row.get("last3_runs")),

                "latest_ball": row.get("latest_ball"),
                "commentary": row.get("latest_ball"),

                "b1_name": row.get("b1_name"),
                "b1_runs": safe_int(row.get("b1_runs")),
                "b1_balls": safe_int(row.get("b1_balls")),
                "b1_4s": safe_int(row.get("b1_4s")),
                "b1_6s": safe_int(row.get("b1_6s")),
                "b1_sr": safe_float(row.get("b1_sr")),

                "b2_name": row.get("b2_name"),
                "b2_runs": safe_int(row.get("b2_runs")),
                "b2_balls": safe_int(row.get("b2_balls")),
                "b2_4s": safe_int(row.get("b2_4s")),
                "b2_6s": safe_int(row.get("b2_6s")),
                "b2_sr": safe_float(row.get("b2_sr")),

                "bw1_name": row.get("bw1_name"),
                "bw1_overs": row.get("bw1_overs"),
                "bw1_runs": safe_int(row.get("bw1_runs")),
                "bw1_wkts": safe_int(row.get("bw1_wkts")),
                "bw1_eco": safe_float(row.get("bw1_eco")),

                "p_runs": safe_int(row.get("p_runs")),
                "p_balls": safe_int(row.get("p_balls")),

                "cricket_json": cricket_json,
                "runners": [],
            }

        runner_payload = {
            "runner_id": current_runner_id,
            "runner_name": runner_name,

            "ltp": safe_float(row.get("ltp")),
            "prev_ltp": safe_float(row.get("prev_ltp")),
            "tv": safe_float(row.get("tv")),
            "market_updated_at": row.get("market_updated_at"),

            "signal": row.get("signal"),
            "signal_source": row.get("signal_source"),
            "mode": row.get("mode"),
            "price_going": row.get("price_going"),
            "confidence": safe_float(row.get("confidence")),
            "p_back": safe_float(row.get("p_back")),
            "p_lay": safe_float(row.get("p_lay")),
            "reason": row.get("reason"),

            "pattern_name": row.get("pattern_name"),
            "pattern_category": row.get("pattern_category"),
            "pattern_category_label": row.get("pattern_category_label"),
            "pattern_detail": row.get("pattern_detail"),
            "pattern_description": row.get("pattern_description"),
            "pattern_price_direction": row.get("pattern_price_direction"),
            "pattern_avg_price_move": safe_float(row.get("pattern_avg_price_move")),
            "pattern_historical_accuracy": safe_float(row.get("pattern_historical_accuracy")),
            "pattern_color": row.get("pattern_color"),

            "price_json": price_json,  # now includes runner_name
            "prediction": prediction_json,
        }

        balls_map[ball_key]["runners"].append(runner_payload)

        market.append({
            "source_match_id": row.get("source_match_id"),
            "market_id": current_market_id,
            "runner_id": current_runner_id,
            "runner_name": runner_name,
            "ball_key": ball_key,
            "ltp": safe_float(row.get("ltp")),
            "prev_ltp": safe_float(row.get("prev_ltp")),
            "tv": safe_float(row.get("tv")),
            "market_updated_at": row.get("market_updated_at"),
            "price_json": price_json,  # now includes runner_name
        })

        patterns.append({
            "source_match_id": row.get("source_match_id"),
            "market_id": current_market_id,
            "runner_id": current_runner_id,
            "runner_name": runner_name,
            "ball_key": ball_key,
            "signal": row.get("signal"),
            "signal_source": row.get("signal_source"),
            "mode": row.get("mode"),
            "price_going": row.get("price_going"),
            "confidence": safe_float(row.get("confidence")),
            "p_back": safe_float(row.get("p_back")),
            "p_lay": safe_float(row.get("p_lay")),
            "reason": row.get("reason"),
            "pattern_name": row.get("pattern_name"),
            "pattern_category": row.get("pattern_category"),
            "pattern_category_label": row.get("pattern_category_label"),
            "pattern_detail": row.get("pattern_detail"),
            "pattern_description": row.get("pattern_description"),
            "pattern_price_direction": row.get("pattern_price_direction"),
            "pattern_avg_price_move": safe_float(row.get("pattern_avg_price_move")),
            "pattern_historical_accuracy": safe_float(row.get("pattern_historical_accuracy")),
            "pattern_color": row.get("pattern_color"),
            "prediction_json": prediction_json,
        })

        full_rows.append({
            "source_match_id": row.get("source_match_id"),
            "market_id": current_market_id,
            "runner_id": current_runner_id,
            "runner_name": runner_name,
            "ball_key": ball_key,
            "cricket": cricket_json,
            "price": price_json,  # now includes runner_name
            "prediction": prediction_json,
            "raw_row": row,
        })

    balls = list(balls_map.values())

    return Response({
        "type": "match_history",
        "filters": {
            "source_match_id": source_match_id,
            "market_id": market_id,
            "runner_id": runner_id,
        },
        "csv_folder": str(csv_dir),
        "files_checked": files_checked,
        "total_rows": len(rows),
        "total_balls": len(balls),
        "history": {
            "balls": balls,
            "market": market,
            "patterns": patterns,
            "full_rows": full_rows,
        },
    })


