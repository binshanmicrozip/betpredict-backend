from django.apps import AppConfig
import os
import threading

class BetappConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "betapp"

    # def ready(self):
    #     # Prevent duplicate thread in Django autoreload
    #     if os.environ.get("RUN_MAIN") != "true":
    #         return

        # from .ws_consumer import start_websocket

        # thread = threading.Thread(target=start_websocket(), daemon=True)
        # thread.start()
        # print("Satradar WebSocket thread started")