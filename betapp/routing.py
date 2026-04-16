from django.urls import re_path
from .consumers import BetSignalConsumer

websocket_urlpatterns = [
    re_path(r"ws/bet-signals/$", BetSignalConsumer.as_asgi()),
]