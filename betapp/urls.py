from django.urls import path, include
from rest_framework.routers import DefaultRouter
from rest_framework_simplejwt.views import TokenRefreshView
from .views import (
    MarketViewSet,
    RunnerViewSet,
    PriceTickViewSet,
    PlayerViewSet,
    PlayerIPLTeamViewSet,
    IPLMatchViewSet,
    MatchPlayerViewSet,
    DeliveryViewSet,
    PlayerMatchBattingViewSet,
    PlayerMatchBowlingViewSet,
    PlayerSituationStatsViewSet,
    health_check,
    ingest_ticks,
    predict_signal,
    latest_prediction,
    LoginAPIView,
)
from .views import LiveMatchStateViewSet, LiveDeliveryViewSet

router = DefaultRouter()

router.register(r"markets", MarketViewSet, basename="market")
router.register(r"runners", RunnerViewSet, basename="runner")
router.register(r"price-ticks", PriceTickViewSet, basename="price-tick")
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
    path("api/price-ticks/ingest/", ingest_ticks, name="ingest_ticks"),
    path("api/predict/", predict_signal, name="predict-signal"),
    path("api/latest-prediction/", latest_prediction, name="latest-prediction"),
    path("api/login/", LoginAPIView.as_view(), name="api-login"),
    path("api/token/refresh/", TokenRefreshView.as_view(), name="token-refresh"),

    path("api/", include(router.urls)),
]