from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer


def push_signal_to_frontend(payload: dict):
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "bet_signals",
        {
            "type": "send_signal",
            "payload": payload,
        },
    )