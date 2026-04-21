from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes

from betapp.services.pattern_service import (
    get_all_patterns,
    get_patterns_by_category,
    get_pattern_by_name,
)


class PatternListView(APIView):
    @extend_schema(
        tags=["Patterns"],
        summary="Get all patterns or patterns by category",
        parameters=[
            OpenApiParameter(
                name="category",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                required=False,
                description="Filter patterns by category"
            ),
        ],
        responses={200: dict, 404: dict},
    )
    def get(self, request):
        category = request.GET.get("category")

        if category:
            data = get_patterns_by_category(category)
            if not data:
                return Response(
                    {"error": "category not found"},
                    status=status.HTTP_404_NOT_FOUND,
                )
            return Response(data)

        return Response(get_all_patterns())


class PatternDetailView(APIView):
    @extend_schema(
        tags=["Patterns"],
        summary="Get pattern details by name",
        parameters=[
            OpenApiParameter(
                name="pattern",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Pattern name"
            ),
        ],
        responses={200: dict, 400: dict, 404: dict},
    )
    def get(self, request):
        pattern = request.GET.get("pattern")

        if not pattern:
            return Response(
                {"error": "pattern is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        data = get_pattern_by_name(pattern)

        if not data:
            return Response(
                {"error": "pattern not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(data)