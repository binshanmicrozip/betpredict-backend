from rest_framework.decorators import api_view
from rest_framework.response import Response

from betapp.services.match_history_service import get_match_history_payload


@api_view(["GET"])
def match_history_detail_view(request, match_id):
    payload = get_match_history_payload(match_id)
    if not payload.get("found"):
        return Response(payload, status=404)
    return Response(payload)