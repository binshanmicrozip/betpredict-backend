import json
from channels.generic.websocket import AsyncWebsocketConsumer


class BetSignalConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        print("[WebSocket] Client connected")
        await self.channel_layer.group_add("bet_signals", self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        print("[WebSocket] Client disconnected")
        await self.channel_layer.group_discard("bet_signals", self.channel_name)

    async def send_signal(self, event):
        print("[WebSocket] Sending payload to frontend:", event["payload"])
        await self.send(text_data=json.dumps(event["payload"]))