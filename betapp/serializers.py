from rest_framework import serializers
from typing import Any

from django.contrib.auth import authenticate

from .models import (Market, Runner, PriceTick, Pattern, Player,
    PlayerIPLTeam,
    IPLMatch,
    MatchPlayer,
    Delivery,
    PlayerMatchBatting,
    PlayerMatchBowling,
    PlayerSituationStats, LiveMarketTick,)



class LoginSerializer(serializers.Serializer):
    username = serializers.CharField()
    password = serializers.CharField(write_only=True)

    def validate(self, attrs):
        username = attrs.get("username")
        password = attrs.get("password")

        if not username or not password:
            raise serializers.ValidationError("Username and password are required.")

        user = authenticate(username=username, password=password)

        if not user:
            raise serializers.ValidationError("Invalid username or password.")

        if not user.is_active:
            raise serializers.ValidationError("User account is disabled.")

        attrs["user"] = user
        return attrs


class MarketSerializer(serializers.ModelSerializer):
    class Meta:
        model = Market
        fields = "__all__"


class RunnerSerializer(serializers.ModelSerializer):
    market_id = serializers.CharField(source="market.market_id", read_only=True)
    event_name = serializers.CharField(source="market.event_name", read_only=True)

    class Meta:
        model = Runner
        fields = "__all__"


class PriceTickSerializer(serializers.ModelSerializer):
    market_id = serializers.CharField(source="market.market_id", read_only=True)
    market_name = serializers.CharField(source="market.market_name", read_only=True)
    event_name = serializers.CharField(source="market.event_name", read_only=True)
    selection_id = serializers.IntegerField(source="runner.selection_id", read_only=True)
    runner_name = serializers.CharField(source="runner.runner_name", read_only=True)

    class Meta:
        model = PriceTick
        fields = [
            "id",
            "market",
            "market_id",
            "market_name",
            "event_name",
            "runner",
            "selection_id",
            "runner_name",
            "year",
            "month",
            "day",
            "snapshot",
            "tick_time",
            "ltp",
            "win_prob",
            "traded_volume",
            "phase",
        ]


class PatternSerializer(serializers.ModelSerializer):
    market_id = serializers.CharField(source="market.market_id", read_only=True)
    runner_id = serializers.CharField(source="runner.runner_id", read_only=True)

    class Meta:
        model = Pattern
        fields = [
            "id",
            "feature_vector",
            "market",
            "runner",
            "market_id",
            "runner_id",
            "runner_name",
            "event_name",
            "market_time",
            "winner",
            "runner_won",
            "window_start",
            "window_end",
            "window_start_ms",
            "window_end_ms",
            "window_start_utc",
            "price_at_start",
            "price_at_end",
            "price_high",
            "price_low",
            "price_change_pct",
            "momentum",
            "volatility",
            "trend_slope",
            "max_drawdown",
            "tick_count",
            "duration_sec",
            "pattern_type",
            "label",
            "created_at",
        ]



from rest_framework import serializers
from .models import LiveMarketTick


class LiveMarketTickSerializer(serializers.ModelSerializer):
    class Meta:
        model = LiveMarketTick
        fields = [
            "market_id",
            "event_id",
            "event_name",
            "market_type",
            "market_time",
            "runner_id",
            "runner_name",
            "publish_time_ms",
            "publish_time_utc",
            "ltp",
            "prev_ltp",
            "price_change",
            "price_change_pct",
            "price_direction",
            "market_status",
            "in_play",
            "bet_delay",
            "winner",
            "settled_time",
            "year",
            "month",
        ]
class PlayerSerializer(serializers.ModelSerializer):
    country = serializers.CharField(
        allow_null=True,
        required=False
    )

    class Meta:
        model = Player
        fields = [
            "player_id",
            "player_name",
            "normalized_name",
            "country",
            "role",
            "ipl_debut",
            "debut_year",
            "last_season",
            "cricbuzz_profile_id",
            "cricbuzz_profile_url",
            "created_at",
        ]
        read_only_fields = [
            "created_at",
            "normalized_name",
        ]
class PlayerIPLTeamSerializer(serializers.ModelSerializer):
    player_name = serializers.CharField(source="player.player_name", read_only=True)

    class Meta:
        model = PlayerIPLTeam
        fields = "__all__"


class IPLMatchSerializer(serializers.ModelSerializer):
    class Meta:
        model = IPLMatch
        fields = "__all__"


class MatchPlayerSerializer(serializers.ModelSerializer):
    player_name = serializers.CharField(source="player.player_name", read_only=True)
    match_date = serializers.DateField(source="match.match_date", read_only=True)
    match_name = serializers.SerializerMethodField()

    class Meta:
        model = MatchPlayer
        fields = "__all__"

    def get_match_name(self, obj) -> str:
        return f"{obj.match.team_home} vs {obj.match.team_away}"


class DeliverySerializer(serializers.ModelSerializer):
    match_date = serializers.DateField(source="match.match_date", read_only=True)
    batter_name = serializers.CharField(source="batter.player_name", read_only=True)
    bowler_name = serializers.CharField(source="bowler.player_name", read_only=True)
    non_striker_name = serializers.CharField(source="non_striker.player_name", read_only=True)
    player_out_name = serializers.CharField(source="player_out.player_name", read_only=True)

    class Meta:
        model = Delivery
        fields = "__all__"


class PlayerMatchBattingSerializer(serializers.ModelSerializer):
    player_name = serializers.CharField(source="player.player_name", read_only=True)
    match_date = serializers.DateField(source="match.match_date", read_only=True)
    match_name = serializers.SerializerMethodField()

    class Meta:
        model = PlayerMatchBatting
        fields = "__all__"

    def get_match_name(self, obj) -> str:
        return f"{obj.match.team_home} vs {obj.match.team_away}"


class PlayerMatchBowlingSerializer(serializers.ModelSerializer):
    player_name = serializers.CharField(source="player.player_name", read_only=True)
    match_date = serializers.DateField(source="match.match_date", read_only=True)
    match_name = serializers.SerializerMethodField()

    class Meta:
        model = PlayerMatchBowling
        fields = "__all__"

    def get_match_name(self, obj) -> str:
        return f"{obj.match.team_home} vs {obj.match.team_away}"


class PlayerSituationStatsSerializer(serializers.ModelSerializer):
    player_name = serializers.CharField(source="player.player_name", read_only=True)

    class Meta:
        model = PlayerSituationStats
        fields = "__all__"


class IPL2026PlayerListSerializer(serializers.Serializer):
    player_id = serializers.CharField()
    player_name = serializers.CharField()
    role = serializers.CharField(allow_null=True)
    country = serializers.CharField(allow_null=True)
    current_team = serializers.CharField(allow_null=True)
    matches_played = serializers.IntegerField()
    ipl_debut = serializers.DateField(allow_null=True)
    last_season = serializers.IntegerField(allow_null=True)


class PlayerProfileSerializer(serializers.Serializer):
    player_id = serializers.CharField()
    player_name = serializers.CharField()
    country = serializers.CharField(allow_null=True)
    role = serializers.CharField(allow_null=True)
    ipl_debut = serializers.DateField(allow_null=True)
    last_season = serializers.IntegerField(allow_null=True)

    current_team = serializers.CharField(allow_null=True)
    teams_played_for = serializers.ListField(child=serializers.CharField())
    total_matches = serializers.IntegerField()
    matches_in_2026 = serializers.IntegerField()
    last_match_played = serializers.DateField(allow_null=True)

    batting = serializers.DictField()
    bowling = serializers.DictField()
    situation_stats = serializers.ListField()
    recent_matches = serializers.ListField()
from .models import LiveMatchState, LiveDelivery


class LiveMatchStateSerializer(serializers.ModelSerializer):
    match_id = serializers.CharField(source="match.match_id", read_only=True)

    class Meta:
        model = LiveMatchState
        fields = "__all__"


class LiveDeliverySerializer(serializers.ModelSerializer):
    match_id = serializers.CharField(source="match.match_id", read_only=True)

    class Meta:
        model = LiveDelivery
        fields = "__all__"