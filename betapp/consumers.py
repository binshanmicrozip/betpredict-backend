import json
from channels.generic.websocket import AsyncWebsocketConsumer


class BetSignalConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        print("✅ WebSocket Connected")
        await self.channel_layer.group_add("bet_signals", self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("bet_signals", self.channel_name)

    async def send_message(self, event):
        await self.send(text_data=json.dumps(event["payload"]))

    async def send_signal(self, event):
        # compatibility with old sender type if any old code remains
        await self.send(text_data=json.dumps(event["payload"]))