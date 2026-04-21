from rest_framework.decorators import api_view
from rest_framework.response import Response

from betapp.models import Signal


@api_view(["GET"])
def latest_signal_view(request):
    market_id = request.GET.get("market_id")
    runner_id = request.GET.get("runner_id")
    match_id = request.GET.get("match_id")

    qs = Signal.objects.all().order_by("-created_at")

    if market_id:
        qs = qs.filter(market_id=str(market_id))

    if runner_id:
        qs = qs.filter(runner_id=str(runner_id))

    if match_id:
        qs = qs.filter(match_id=str(match_id))

    obj = qs.first()
    if not obj:
        return Response({
            "found": False,
            "data": None,
            "message": "No signal found"
        })

    return Response({
        "found": True,
        "data": {
            "id": obj.id,
            "match_id": obj.match_id if obj.match_id else None,
            "market_id": obj.market_id,
            "runner_id": obj.runner_id,
            "striker_name": obj.striker_name,
            "phase": obj.phase,
            "innings_type": obj.innings_type,
            "final_probability": float(obj.final_probability) if obj.final_probability is not None else None,
            "signal": obj.signal,
            "model_source": obj.model_source,
            "raw_features": obj.raw_features,
            "raw_output": obj.raw_output,
            "created_at": obj.created_at,
        }
    })