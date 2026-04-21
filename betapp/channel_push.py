from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer


def push_to_group(group_name: str, payload: dict):
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        group_name,
        {
            "type": "send_message",
            "payload": payload,
        },
    )


# optional compatibility if old code still imports this
def push_signal_to_frontend(payload: dict):
    push_to_group("bet_signals", payload)