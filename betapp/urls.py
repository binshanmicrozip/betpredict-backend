from django.urls import path, include
from rest_framework.routers import DefaultRouter
from rest_framework_simplejwt.views import TokenRefreshView

from .views import (
    MarketViewSet,
    RunnerViewSet,
    PriceTickViewSet,
    PatternViewSet,
    PlayerViewSet,
    PlayerIPLTeamViewSet,
    IPLMatchViewSet,
    MatchPlayerViewSet,
    DeliveryViewSet,
    PlayerMatchBattingViewSet,
    PlayerMatchBowlingViewSet,
    PlayerSituationStatsViewSet,
    health_check,
    IngestTicksView,
    PredictSignalView,
    LatestPredictionView,
    LoginAPIView,
    match_csv_history,
)
from .views import LiveMatchStateViewSet, LiveDeliveryViewSet, LiveMarketTickViewSet
from .views_match_history import match_history_detail_view
from .views_player_stats import PlayerStatsView
from .views_patterns import PatternListView, PatternDetailView
from .views_signal import latest_signal_view



router = DefaultRouter()

router.register(r"markets", MarketViewSet, basename="market")
router.register(r"runners", RunnerViewSet, basename="runner")
router.register(r"price-ticks", PriceTickViewSet, basename="price-tick")
router.register(r"live-market-ticks", LiveMarketTickViewSet, basename="live-market-ticks")
router.register(r"patterns", PatternViewSet, basename="patterns")
router.register(r"players", PlayerViewSet, basename="players")
router.register(r"player-ipl-teams", PlayerIPLTeamViewSet, basename="player-ipl-teams")
router.register(r"ipl-matches", IPLMatchViewSet, basename="ipl-matches")
router.register(r"match-players", MatchPlayerViewSet, basename="match-players")
router.register(r"deliveries", DeliveryViewSet, basename="deliveries")
router.register(r"player-match-batting", PlayerMatchBattingViewSet, basename="player-match-batting")
router.register(r"player-match-bowling", PlayerMatchBowlingViewSet, basename="player-match-bowling")
router.register(r"player-situation-stats", PlayerSituationStatsViewSet, basename="player-situation-stats")
router.register(r"live-match-states", LiveMatchStateViewSet, basename="live-match-states")
router.register(r"live-deliveries", LiveDeliveryViewSet, basename="live-deliveries")

urlpatterns = [
    path("api/health/", health_check, name="health_check"),
    path("api/price-ticks/ingest/", IngestTicksView.as_view(), name="ingest_ticks"),
    path("api/predict/", PredictSignalView.as_view(), name="predict-signal"),
    path("api/latest-prediction/", LatestPredictionView.as_view(), name="latest-prediction"),
    path("api/login/", LoginAPIView.as_view(), name="api-login"),
    path("api/token/refresh/", TokenRefreshView.as_view(), name="token-refresh"),

    path("api/signal/", latest_signal_view, name="latest-signal"),
    

    path("api/ipl-matches/<str:match_id>/history/", match_history_detail_view, name="match-history-detail"),
    path("api/match-csv-history/", match_csv_history, name="match_csv_history"),



  




    # custom player endpoints
    # path("api/player-search/", PlayerSearchAPIView.as_view(), name="player-search"),
    path("api/player-stats/", PlayerStatsView.as_view(), name="player-stats"),

    path("api/patterns/", PatternListView.as_view()),
    path("api/patterns/detail/", PatternDetailView.as_view()),

    # router endpoints
    path("api/", include(router.urls)),
]