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
    list_display = [field.name for field in Pattern._meta.fields]
    list_filter = ('label',)
    list_per_page = 50


@admin.register(Player)
class PlayerAdmin(admin.ModelAdmin):
    list_display = ("player_id", "player_name", "role", "nationality", "ipl_debut", "last_season")
    search_fields = ("player_id", "player_name")


@admin.register(PlayerIPLTeam)
class PlayerIPLTeamAdmin(admin.ModelAdmin):
    list_display = ("player", "team_name", "team_short", "season", "is_current")
    list_filter = ("season", "team_name", "is_current")
    search_fields = ("player__player_name", "team_name")


@admin.register(IPLMatch)
class IPLMatchAdmin(admin.ModelAdmin):
    list_display = ("match_id", "season", "match_number", "match_date", "team_home", "team_away", "winner")
    list_filter = ("season", "match_date")
    search_fields = ("match_id", "team_home", "team_away", "venue", "city")


@admin.register(MatchPlayer)
class MatchPlayerAdmin(admin.ModelAdmin):
    list_display = ("match", "player", "team_name", "batting_position")
    list_filter = ("team_name",)
    search_fields = ("player__player_name", "match__match_id")


@admin.register(Delivery)
class DeliveryAdmin(admin.ModelAdmin):
    list_display = (
        "delivery_id", "match", "innings", "over_number", "ball_number",
        "batter", "bowler", "runs_total", "is_wicket", "wicket_kind"
    )
    list_filter = ("innings", "is_wicket", "extra_type")
    search_fields = ("match__match_id", "batter__player_name", "bowler__player_name")


@admin.register(PlayerMatchBatting)
class PlayerMatchBattingAdmin(admin.ModelAdmin):
    list_display = (
        "match", "player", "innings", "runs", "balls_faced",
        "fours", "sixes", "strike_rate", "dismissal_kind", "is_not_out"
    )
    list_filter = ("innings", "is_not_out")
    search_fields = ("match__match_id", "player__player_name")


@admin.register(PlayerMatchBowling)
class PlayerMatchBowlingAdmin(admin.ModelAdmin):
    list_display = (
        "match", "player", "innings", "overs_bowled",
        "runs_given", "wickets", "economy", "wides", "noballs"
    )
    list_filter = ("innings",)
    search_fields = ("match__match_id", "player__player_name")


@admin.register(PlayerSituationStats)
class PlayerSituationStatsAdmin(admin.ModelAdmin):
    list_display = (
        "player", "phase", "innings_type", "matches_played", "runs",
        "balls", "strike_rate", "boundary_count", "boundary_pct",
        "wickets_lost", "dismissal_rate"
    )
    list_filter = ("phase", "innings_type")
    search_fields = ("player__player_name",)