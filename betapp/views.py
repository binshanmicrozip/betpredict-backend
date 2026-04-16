from datetime import datetime, timedelta
from django.db.models import Q
from django.utils.dateparse import parse_date
from rest_framework import viewsets
from rest_framework.decorators import action, api_view
from rest_framework.response import Response
from django.db.models import Sum, Max, Count, Q
from django.db.models.functions import Coalesce
from rest_framework.decorators import action

from .models import (Market, Runner, PriceTick, Player,
    PlayerIPLTeam,
    IPLMatch,
    MatchPlayer,
    Delivery,
    PlayerMatchBatting,
    PlayerMatchBowling,
    PlayerSituationStats,)
from .serializers import (MarketSerializer, RunnerSerializer, PriceTickSerializer, PlayerSerializer,
    PlayerIPLTeamSerializer,
    IPLMatchSerializer,
    MatchPlayerSerializer,
    DeliverySerializer,
    PlayerMatchBattingSerializer,
    PlayerMatchBowlingSerializer,
    PlayerSituationStatsSerializer, PlayerProfileSerializer, IPL2026PlayerListSerializer)
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


@api_view(["GET"])
def latest_prediction(request):
    raw = r.get("prediction:latest")
    if not raw:
        return Response({"message": "No prediction yet"}, status=404)
    return Response(json.loads(raw))


@api_view(["POST"])
def predict_signal(request):
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




@api_view(["POST"])
def ingest_ticks(request):
    ticks = request.data

    if not isinstance(ticks, list):
        return Response({"error": "Expected list of ticks"}, status=400)

    task = insert_ticks.delay(ticks)

    return Response({
        "message": "Tick batch queued",
        "task_id": task.id,
        "count": len(ticks)
    })







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
        tags=["Players"],
        parameters=[
            OpenApiParameter(
                name="player",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Filter by Cricsheet player id, for example 1c2a64cd",
            ),
            OpenApiParameter(
                name="match__season",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                description="Filter by season, for example 2026",
            ),
            OpenApiParameter(
                name="team_name",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Filter by team name",
            ),
            OpenApiParameter(
                name="match__match_date__gte",
                type=OpenApiTypes.DATE,
                location=OpenApiParameter.QUERY,
                description="Match date greater than or equal to",
            ),
            OpenApiParameter(
                name="match__match_date__lte",
                type=OpenApiTypes.DATE,
                location=OpenApiParameter.QUERY,
                description="Match date less than or equal to",
            ),
        ],
    ),
    retrieve=extend_schema(tags=["Players"]),
    create=extend_schema(tags=["Players"]),
    update=extend_schema(tags=["Players"]),
    partial_update=extend_schema(tags=["Players"]),
    destroy=extend_schema(tags=["Players"]),
)
class PlayerViewSet(viewsets.ModelViewSet):
    queryset = MatchPlayer.objects.select_related("match", "player").all().order_by("match__match_date")
    serializer_class = MatchPlayerSerializer
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]

    filterset_fields = {
        "match": ["exact"],
        "player": ["exact"],
        "team_name": ["exact", "icontains"],
        "batting_position": ["exact"],
        "match__season": ["exact"],
        "match__match_date": ["exact", "gte", "lte"],
    }

    search_fields = [
        "team_name",
        "player__player_name",
        "player__player_id",
        "match__match_id",
        "match__team_home",
        "match__team_away",
    ]
    ordering_fields = ["batting_position", "match__match_date"]

    @extend_schema(
        tags=["Players"],
        parameters=[
            OpenApiParameter(
                name="player_id",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.PATH,
                required=True,
                description="Cricsheet player id, for example 1c2a64cd",
            ),
        ],
    )
    @action(detail=False, methods=["get"], url_path=r"by-player/(?P<player_id>[^/.]+)")
    def by_player(self, request, player_id=None):
        qs = self.get_queryset().filter(player_id=player_id)
        serializer = self.get_serializer(qs, many=True)
        return Response(serializer.data)
    
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
        "team_home",
        "team_away",
        "winner",
        "venue",
        "city",
    ]
    search_fields = [
        "match_id",
        "team_home",
        "team_away",
        "winner",
        "venue",
        "city",
        "player_of_match",
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
    filterset_fields = ["match", "player", "team_name", "batting_position"]
    search_fields = [
        "team_name",
        "player__player_name",
        "player__player_id",
        "match__match_id",
        "match__team_home",
        "match__team_away",
    ]
    ordering_fields = ["batting_position", "match__match_date"]

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
        "match", "batter", "bowler", "non_striker", "player_out"
    ).all().order_by("match", "innings", "over_number", "ball_number", "delivery_id")
    serializer_class = DeliverySerializer
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = [
        "match",
        "innings",
        "over_number",
        "ball_number",
        "batter",
        "bowler",
        "non_striker",
        "player_out",
        "is_wicket",
        "extra_type",
        "wicket_kind",
    ]
    search_fields = [
        "match__match_id",
        "batter__player_name",
        "bowler__player_name",
        "non_striker__player_name",
        "player_out__player_name",
    ]
    ordering_fields = ["innings", "over_number", "ball_number", "delivery_id"]

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
        "batting_position",
    ]
    search_fields = [
        "player__player_name",
        "player__player_id",
        "match__match_id",
        "match__team_home",
        "match__team_away",
    ]
    ordering_fields = ["runs", "balls_faced", "strike_rate", "batting_position"]
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