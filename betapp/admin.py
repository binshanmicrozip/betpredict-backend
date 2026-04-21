from django.contrib import admin
from .models import (Market, Runner, PriceTick, FeatureVector, Pattern, Player,
    PlayerIPLTeam,
    IPLMatch,
    MatchPlayer,
    Delivery,
    PlayerMatchBatting,
    PlayerMatchBowling,
    PlayerSituationStats)


@admin.register(Market)
class MarketAdmin(admin.ModelAdmin):
    list_display = [field.name for field in Market._meta.fields]
    search_fields = ('market_id', 'event_name', 'event_id')
    list_filter = ('status', 'market_type', 'country_code')
    list_per_page = 50


@admin.register(Runner)
class RunnerAdmin(admin.ModelAdmin):
    list_display = [field.name for field in Runner._meta.fields]
    search_fields = ('runner_name', 'selection_id', 'market__market_id')
    list_filter = ('status', 'final_result')
    list_select_related = ('market',)
    list_per_page = 50


@admin.register(PriceTick)
class PriceTickAdmin(admin.ModelAdmin):
    # ✅ Show all fields dynamically
    list_display = [field.name for field in PriceTick._meta.fields]

    # ✅ CRITICAL: prevent heavy query
    show_full_result_count = False

    # ✅ Reduce load
    list_per_page = 20

    # ✅ Optimize joins
    list_select_related = ('market', 'runner')

    # ✅ Basic filters only (avoid heavy)
    list_filter = ('year', 'month', 'phase')

    search_fields = (
        'market__market_id',
        'runner__runner_name',
        'runner__selection_id'
    )

    ordering = ('-tick_time',)

    # ✅ Optimize query
    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related('market', 'runner').only(
            *[field.name for field in PriceTick._meta.fields],
            'market__market_id',
            'runner__runner_name'
        )


@admin.register(FeatureVector)
class FeatureVectorAdmin(admin.ModelAdmin):
    list_display = [field.name for field in FeatureVector._meta.fields]
    list_per_page = 50


@admin.register(Pattern)
class PatternAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "market",
        "runner",
        "runner_name",
        "pattern_type",
        "label",
        "tick_count",
        "market_time",
        "created_at",
    )
    search_fields = (
        "runner_name",
        "event_name",
        "winner",
        "pattern_type",
        "label",
    )
    list_filter = (
        "label",
        "pattern_type",
        "runner_won",
    )


from .models import (
    Player,
    IPLMatch,
    MatchPlayer,
    Delivery,
    PlayerMatchBatting,
    PlayerMatchBowling,
    PlayerSituationStats,
)


@admin.register(Player)
class PlayerAdmin(admin.ModelAdmin):
    list_display = (
        "player_id",
        "player_name",
        "normalized_name",
        "country",
        "role",
        "debut_year",
        "last_season",
    )
    search_fields = (
        "player_id",
        "player_name",
        "normalized_name",
        "country",
        "role",
        "cricbuzz_profile_id",
    )
    list_filter = ("role", "country", "debut_year", "last_season")
    ordering = ("player_name",)


@admin.register(IPLMatch)
class IPLMatchAdmin(admin.ModelAdmin):
    list_display = (
        "match_id",
        "season",
        "match_date",
    )
    search_fields = ("match_id",)
    list_filter = ("season", "match_date")
    ordering = ("-match_date", "-season")


@admin.register(MatchPlayer)
class MatchPlayerAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "match",
        "player",
    )
    search_fields = (
        "match__match_id",
        "player__player_name",
        "player__normalized_name",
    )
    list_filter = ("match__season",)
    ordering = ("match", "player")


@admin.register(Delivery)
class DeliveryAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "match",
        "innings",
        "over_number",
        "ball_number",
        "batter",
        "bowler",
        "runs_batter",
        "runs_extras",
        "runs_total",
        "is_wicket",
        "wicket_kind",
    )
    search_fields = (
        "match__match_id",
        "batter__player_name",
        "bowler__player_name",
        "non_striker__player_name",
        "player_out__player_name",
        "wicket_kind",
    )
    list_filter = (
        "innings",
        "is_wicket",
        "extra_type",
        "match__season",
    )
    ordering = ("match_id", "innings", "over_number", "ball_number", "id")


@admin.register(PlayerMatchBatting)
class PlayerMatchBattingAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "match",
        "player",
        "innings",
        "runs",
        "balls_faced",
        "fours",
        "sixes",
        "strike_rate",
        "dismissal_kind",
        "is_not_out",
    )
    search_fields = (
        "match__match_id",
        "player__player_name",
        "player__normalized_name",
    )
    list_filter = ("innings", "is_not_out", "match__season")
    ordering = ("match", "innings", "player")


@admin.register(PlayerMatchBowling)
class PlayerMatchBowlingAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "match",
        "player",
        "innings",
        "overs_bowled",
        "balls_bowled_calc",
        "runs_given",
        "wickets",
        "economy",
        "wides",
        "noballs",
    )
    search_fields = (
        "match__match_id",
        "player__player_name",
        "player__normalized_name",
    )
    list_filter = ("innings", "match__season")
    ordering = ("match", "innings", "player")


@admin.register(PlayerSituationStats)
class PlayerSituationStatsAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "player",
        "phase",
        "innings_type",
        "matches_played",
        "runs",
        "balls",
        "strike_rate",
        "boundary_count",
        "boundary_pct",
        "wickets_lost",
        "dismissal_rate",
    )
    search_fields = (
        "player__player_name",
        "player__normalized_name",
    )
    list_filter = ("phase", "innings_type")
    ordering = ("player", "phase", "innings_type")