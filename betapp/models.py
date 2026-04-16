from django.db import models


class Market(models.Model):
    market_id = models.CharField(max_length=20, primary_key=True)
    event_id = models.CharField(max_length=20)
    event_name = models.TextField()
    market_name = models.CharField(max_length=255, blank=True, null=True)
    market_type = models.CharField(max_length=50)
    event_type_id = models.CharField(max_length=10)
    country_code = models.CharField(max_length=5, blank=True, null=True)
    timezone = models.CharField(max_length=50, blank=True, null=True)
    market_open_date = models.DateTimeField(blank=True, null=True)
    market_start_time = models.DateTimeField()
    suspend_time = models.DateTimeField(blank=True, null=True)
    settled_time = models.DateTimeField(blank=True, null=True)
    number_of_winners = models.IntegerField(default=0)
    number_of_active_runners = models.IntegerField(default=0)
    bet_delay = models.IntegerField(default=0)
    market_base_rate = models.DecimalField(max_digits=8, decimal_places=2, blank=True, null=True)
    turn_in_play_enabled = models.BooleanField(default=False)
    persistence_enabled = models.BooleanField(default=False)
    bsp_market = models.BooleanField(default=False)
    bsp_reconciled = models.BooleanField(default=False)
    cross_matching = models.BooleanField(default=False)
    runners_voidable = models.BooleanField(default=False)
    complete = models.BooleanField(default=False)
    regulators = models.TextField(blank=True, null=True)
    opening_status = models.CharField(max_length=20, blank=True, null=True)
    status = models.CharField(max_length=20, default='OPEN')
    in_play_seen = models.BooleanField(default=False)
    in_play_start_time = models.DateTimeField(blank=True, null=True)
    total_tick_messages = models.BigIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'markets'
        managed = False
        indexes = [
            models.Index(fields=['event_id']),
            models.Index(fields=['event_type_id']),
            models.Index(fields=['market_start_time']),
            models.Index(fields=['status']),
        ]

    def __str__(self):
        return f"{self.market_id} - {self.event_name}"


class Runner(models.Model):
    market = models.ForeignKey(Market, on_delete=models.CASCADE, related_name='runners')
    selection_id = models.BigIntegerField()
    runner_name = models.CharField(max_length=255)
    sort_priority = models.SmallIntegerField(default=0)
    status = models.CharField(max_length=20, default='ACTIVE')
    final_result = models.CharField(max_length=20, blank=True, null=True)

    opening_price = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    opening_win_prob = models.DecimalField(max_digits=8, decimal_places=3, blank=True, null=True)
    closing_price = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    closing_win_prob = models.DecimalField(max_digits=8, decimal_places=3, blank=True, null=True)
    lowest_price = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    highest_price = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    price_range = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    price_change = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)

    total_ticks = models.IntegerField(default=0)
    pre_match_ticks = models.IntegerField(default=0)
    in_play_ticks = models.IntegerField(default=0)
    first_tick_time = models.DateTimeField(blank=True, null=True)
    last_tick_time = models.DateTimeField(blank=True, null=True)

    in_play_first_price = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    in_play_last_price = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    in_play_min_price = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    in_play_max_price = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)

    class Meta:
        db_table = 'runners'
        managed = False
        unique_together = ('market', 'selection_id')
        indexes = [
            models.Index(fields=['market']),
            models.Index(fields=['selection_id']),
            models.Index(fields=['status']),
        ]

    def __str__(self):
        return f"{self.runner_name} ({self.selection_id})"


class PriceTick(models.Model):
    market = models.ForeignKey(
        Market,
        on_delete=models.CASCADE,
        related_name='price_ticks'
    )
    runner = models.ForeignKey(
        Runner,
        on_delete=models.CASCADE,
        related_name='price_ticks'
    )

    year = models.IntegerField(blank=True, null=True)
    month = models.IntegerField(blank=True, null=True)
    day = models.IntegerField(blank=True, null=True)

    snapshot = models.CharField(max_length=100, blank=True, null=True)
    tick_time = models.DateTimeField()

    ltp = models.DecimalField(max_digits=10, decimal_places=3)
    win_prob = models.DecimalField(max_digits=8, decimal_places=3, blank=True, null=True)
    traded_volume = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    phase = models.CharField(max_length=20, blank=True, null=True)

    class Meta:
        db_table = 'price_ticks'
        managed = False
        indexes = [
            models.Index(fields=['tick_time']),
            models.Index(fields=['year', 'month', 'day']),
            models.Index(fields=['market', '-tick_time']),
            models.Index(fields=['runner', '-tick_time']),
            models.Index(fields=['phase']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['runner', 'tick_time', 'snapshot'],
                name='uq_price_ticks_runner_tick_time_snapshot'
            )
        ]

    def __str__(self):
        return f"{self.runner_id} - {self.ltp} @ {self.tick_time}"

class FeatureVector(models.Model):
    runner = models.ForeignKey(Runner, on_delete=models.CASCADE, related_name='feature_vectors')
    market = models.ForeignKey(Market, on_delete=models.CASCADE, related_name='feature_vectors')
    window_start = models.DateTimeField()
    window_end = models.DateTimeField()
    price_change_pct = models.DecimalField(max_digits=8, decimal_places=5)
    momentum = models.DecimalField(max_digits=8, decimal_places=5)
    volatility = models.DecimalField(max_digits=8, decimal_places=5)
    trend_slope = models.DecimalField(max_digits=10, decimal_places=7)
    max_drawdown = models.DecimalField(max_digits=8, decimal_places=5)
    tick_count = models.SmallIntegerField()
    duration_sec = models.DecimalField(max_digits=6, decimal_places=2)
    feature_version = models.CharField(max_length=10, default='v1.0')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'feature_vectors'
        indexes = [
            models.Index(fields=['runner', 'window_end']),
            models.Index(fields=['market', 'window_end']),
        ]

    def __str__(self):
        return f"FV {self.id} - {self.runner_id}"


class Pattern(models.Model):
    LABEL_CHOICES = [
        ('UP', 'UP'),
        ('DOWN', 'DOWN'),
        ('NEUTRAL', 'NEUTRAL'),
    ]

    feature_vector = models.ForeignKey(FeatureVector, on_delete=models.CASCADE, related_name='patterns')
    runner = models.ForeignKey(Runner, on_delete=models.CASCADE, related_name='patterns')
    market = models.ForeignKey(Market, on_delete=models.CASCADE, related_name='patterns')
    window_start = models.DateTimeField()
    window_end = models.DateTimeField()
    label = models.CharField(max_length=10, choices=LABEL_CHOICES)
    tick_count = models.SmallIntegerField()

    class Meta:
        db_table = 'patterns'
        indexes = [
            models.Index(fields=['runner', 'label']),
            models.Index(fields=['market', 'label']),
        ]

    def __str__(self):
        return f"Pattern {self.id} - {self.label}"


from decimal import Decimal
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator


# ============================================================
# NEW IPL / PLAYER TABLES
# ============================================================
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator


class Player(models.Model):
    ROLE_CHOICES = [
        ("Batsman", "Batsman"),
        ("Bowler", "Bowler"),
        ("All-rounder", "All-rounder"),
        ("Wicketkeeper", "Wicketkeeper"),
        ("Unknown", "Unknown"),
    ]

    player_id = models.CharField(max_length=20, primary_key=True)
    player_name = models.CharField(max_length=255, db_index=True)
    nationality = models.CharField(max_length=100, blank=True, null=True)
    role = models.CharField(max_length=20, choices=ROLE_CHOICES, default="Unknown")
    ipl_debut = models.DateField(blank=True, null=True)
    last_season = models.PositiveSmallIntegerField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "players"
        ordering = ["player_name"]

    def __str__(self):
        return f"{self.player_name} ({self.player_id})"


class PlayerIPLTeam(models.Model):
    player = models.ForeignKey(
        Player,
        on_delete=models.CASCADE,
        related_name="ipl_teams",
        db_column="player_id",
    )
    team_name = models.CharField(max_length=255)
    team_short = models.CharField(max_length=20, blank=True, null=True)
    season = models.PositiveSmallIntegerField(db_index=True)
    is_current = models.BooleanField(default=False)

    class Meta:
        db_table = "player_ipl_teams"
        unique_together = ("player", "team_name", "season")
        indexes = [
            models.Index(fields=["season"]),
            models.Index(fields=["team_name"]),
            models.Index(fields=["player", "season"]),
        ]

    def __str__(self):
        return f"{self.player.player_name} - {self.team_short or self.team_name} ({self.season})"


class IPLMatch(models.Model):
    match_id = models.CharField(max_length=100, primary_key=True)
    season = models.PositiveSmallIntegerField(db_index=True)
    match_number = models.PositiveIntegerField(blank=True, null=True)
    match_date = models.DateField(db_index=True)
    venue = models.CharField(max_length=255, blank=True, null=True)
    city = models.CharField(max_length=100, blank=True, null=True)
    team_home = models.CharField(max_length=255, db_index=True)
    team_away = models.CharField(max_length=255, db_index=True)
    toss_winner = models.CharField(max_length=255, blank=True, null=True)
    toss_decision = models.CharField(max_length=20, blank=True, null=True)
    winner = models.CharField(max_length=255, blank=True, null=True)
    win_by_runs = models.PositiveIntegerField(blank=True, null=True)
    win_by_wickets = models.PositiveIntegerField(blank=True, null=True)
    player_of_match = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        db_table = "ipl_matches"
        ordering = ["-match_date", "match_number"]
        indexes = [
            models.Index(fields=["season", "match_date"]),
            models.Index(fields=["team_home"]),
            models.Index(fields=["team_away"]),
        ]

    def __str__(self):
        return f"{self.match_id} - {self.team_home} vs {self.team_away}"


class MatchPlayer(models.Model):
    match = models.ForeignKey(
        IPLMatch,
        on_delete=models.CASCADE,
        related_name="match_players",
        db_column="match_id",
    )
    player = models.ForeignKey(
        Player,
        on_delete=models.CASCADE,
        related_name="match_appearances",
        db_column="player_id",
    )
    team_name = models.CharField(max_length=255)
    batting_position = models.PositiveSmallIntegerField(blank=True, null=True)

    class Meta:
        db_table = "match_players"
        unique_together = ("match", "player")
        indexes = [
            models.Index(fields=["match", "team_name"]),
            models.Index(fields=["player"]),
        ]

    def __str__(self):
        return f"{self.match.match_id} - {self.player.player_name}"


class Delivery(models.Model):
    EXTRA_TYPE_CHOICES = [
        ("wide", "wide"),
        ("noball", "noball"),
        ("legbye", "legbye"),
        ("bye", "bye"),
    ]

    delivery_id = models.BigAutoField(primary_key=True)
    match = models.ForeignKey(
        IPLMatch,
        on_delete=models.CASCADE,
        related_name="deliveries",
        db_column="match_id",
    )
    innings = models.PositiveSmallIntegerField(
        validators=[MinValueValidator(1), MaxValueValidator(4)]
    )
    over_number = models.PositiveSmallIntegerField()
    ball_number = models.PositiveSmallIntegerField()

    batter = models.ForeignKey(
        Player,
        on_delete=models.PROTECT,
        related_name="balls_faced_records",
        db_column="batter_id",
    )
    bowler = models.ForeignKey(
        Player,
        on_delete=models.PROTECT,
        related_name="balls_bowled_records",
        db_column="bowler_id",
    )
    non_striker = models.ForeignKey(
        Player,
        on_delete=models.PROTECT,
        related_name="non_striker_records",
        db_column="non_striker_id",
    )

    runs_batter = models.PositiveSmallIntegerField(default=0)
    runs_extras = models.PositiveSmallIntegerField(default=0)
    runs_total = models.PositiveSmallIntegerField(default=0)
    extra_type = models.CharField(max_length=20, choices=EXTRA_TYPE_CHOICES, blank=True, null=True)

    is_wicket = models.BooleanField(default=False)
    wicket_kind = models.CharField(max_length=50, blank=True, null=True)
    player_out = models.ForeignKey(
        Player,
        on_delete=models.PROTECT,
        related_name="dismissal_records",
        db_column="player_out_id",
        blank=True,
        null=True,
    )

    class Meta:
        db_table = "deliveries"
        ordering = ["match", "innings", "over_number", "ball_number", "delivery_id"]
        constraints = [
            models.UniqueConstraint(
                fields=["match", "innings", "over_number", "ball_number"],
                name="unique_delivery_per_ball",
            )
        ]
        indexes = [
            models.Index(fields=["match", "innings", "over_number", "ball_number"]),
            models.Index(fields=["batter"]),
            models.Index(fields=["bowler"]),
            models.Index(fields=["player_out"]),
            models.Index(fields=["is_wicket"]),
        ]

    def __str__(self):
        return f"{self.match.match_id} {self.innings}.{self.over_number}.{self.ball_number}"


class PlayerMatchBatting(models.Model):
    match = models.ForeignKey(
        IPLMatch,
        on_delete=models.CASCADE,
        related_name="batting_scorecards",
        db_column="match_id",
    )
    player = models.ForeignKey(
        Player,
        on_delete=models.CASCADE,
        related_name="match_batting",
        db_column="player_id",
    )
    innings = models.PositiveSmallIntegerField()
    runs = models.PositiveIntegerField(default=0)
    balls_faced = models.PositiveIntegerField(default=0)
    fours = models.PositiveIntegerField(default=0)
    sixes = models.PositiveIntegerField(default=0)
    strike_rate = models.DecimalField(max_digits=7, decimal_places=2, default=0)
    dismissal_kind = models.CharField(max_length=50, blank=True, null=True)
    is_not_out = models.BooleanField(default=False)
    batting_position = models.PositiveSmallIntegerField(blank=True, null=True)

    class Meta:
        db_table = "player_match_batting"
        unique_together = ("match", "player", "innings")
        indexes = [
            models.Index(fields=["player"]),
            models.Index(fields=["match"]),
            models.Index(fields=["innings"]),
        ]

    def __str__(self):
        return f"{self.match.match_id} - {self.player.player_name} batting"


class PlayerMatchBowling(models.Model):
    match = models.ForeignKey(
        IPLMatch,
        on_delete=models.CASCADE,
        related_name="bowling_scorecards",
        db_column="match_id",
    )
    player = models.ForeignKey(
        Player,
        on_delete=models.CASCADE,
        related_name="match_bowling",
        db_column="player_id",
    )
    innings = models.PositiveSmallIntegerField()
    overs_bowled = models.DecimalField(max_digits=4, decimal_places=1, default=0)
    runs_given = models.PositiveIntegerField(default=0)
    wickets = models.PositiveIntegerField(default=0)
    economy = models.DecimalField(max_digits=7, decimal_places=2, default=0)
    wides = models.PositiveIntegerField(default=0)
    noballs = models.PositiveIntegerField(default=0)

    class Meta:
        db_table = "player_match_bowling"
        unique_together = ("match", "player", "innings")
        indexes = [
            models.Index(fields=["player"]),
            models.Index(fields=["match"]),
            models.Index(fields=["innings"]),
        ]

    def __str__(self):
        return f"{self.match.match_id} - {self.player.player_name} bowling"


class PlayerSituationStats(models.Model):
    PHASE_CHOICES = [
        ("powerplay", "powerplay"),
        ("middle", "middle"),
        ("death", "death"),
    ]

    INNINGS_TYPE_CHOICES = [
        ("chasing", "chasing"),
        ("defending", "defending"),
    ]

    player = models.ForeignKey(
        Player,
        on_delete=models.CASCADE,
        related_name="situation_stats",
        db_column="player_id",
    )
    phase = models.CharField(max_length=20, choices=PHASE_CHOICES)
    innings_type = models.CharField(max_length=20, choices=INNINGS_TYPE_CHOICES)
    matches_played = models.PositiveIntegerField(default=0)
    runs = models.PositiveIntegerField(default=0)
    balls = models.PositiveIntegerField(default=0)
    strike_rate = models.DecimalField(max_digits=7, decimal_places=2, default=0)
    boundary_count = models.PositiveIntegerField(default=0)
    boundary_pct = models.DecimalField(max_digits=7, decimal_places=2, default=0)
    wickets_lost = models.PositiveIntegerField(default=0)
    dismissal_rate = models.DecimalField(max_digits=10, decimal_places=4, default=0)

    class Meta:
        db_table = "player_situation_stats"
        unique_together = ("player", "phase", "innings_type")
        indexes = [
            models.Index(fields=["player"]),
            models.Index(fields=["phase"]),
            models.Index(fields=["innings_type"]),
        ]

    def __str__(self):
        return f"{self.player.player_name} - {self.phase} - {self.innings_type}"


class LiveMatchState(models.Model):
    live_id = models.BigAutoField(primary_key=True)

    match = models.ForeignKey(
        IPLMatch,
        on_delete=models.CASCADE,
        related_name="live_states",
        db_column="match_id",
    )

    source = models.CharField(max_length=50, default="cricbuzz")
    source_match_id = models.CharField(max_length=50, db_index=True)

    status = models.CharField(max_length=255, blank=True, null=True)
    state = models.CharField(max_length=100, blank=True, null=True)

    innings = models.PositiveSmallIntegerField(blank=True, null=True)
    batting_team = models.CharField(max_length=255, blank=True, null=True)
    bowling_team = models.CharField(max_length=255, blank=True, null=True)

    score = models.PositiveIntegerField(default=0)
    wickets = models.PositiveIntegerField(default=0)
    overs = models.DecimalField(max_digits=5, decimal_places=1, default=0)

    current_run_rate = models.DecimalField(max_digits=6, decimal_places=2, blank=True, null=True)
    required_run_rate = models.DecimalField(max_digits=6, decimal_places=2, blank=True, null=True)
    target = models.PositiveIntegerField(blank=True, null=True)

    toss_winner = models.CharField(max_length=255, blank=True, null=True)
    toss_decision = models.CharField(max_length=20, blank=True, null=True)

    partnership_runs = models.PositiveIntegerField(default=0)
    partnership_balls = models.PositiveIntegerField(default=0)

    recent_overs = models.CharField(max_length=255, blank=True, null=True)
    last5_overs_runs = models.PositiveIntegerField(blank=True, null=True)
    last5_overs_wickets = models.PositiveIntegerField(blank=True, null=True)
    last3_overs_runs = models.PositiveIntegerField(blank=True, null=True)

    powerplay_runs = models.PositiveIntegerField(blank=True, null=True)
    powerplay_from = models.DecimalField(max_digits=4, decimal_places=1, blank=True, null=True)
    powerplay_to = models.DecimalField(max_digits=4, decimal_places=1, blank=True, null=True)

    latest_ball_text = models.TextField(blank=True, null=True)
    raw_json = models.JSONField(blank=True, null=True)

    fetched_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "live_match_states"
        indexes = [
            models.Index(fields=["match"]),
            models.Index(fields=["source_match_id"]),
            models.Index(fields=["fetched_at"]),
        ]

    def __str__(self):
        return f"{self.match.match_id} live {self.score}/{self.wickets} ({self.overs})"


class LiveDelivery(models.Model):
    EXTRA_TYPE_CHOICES = [
        ("wide", "wide"),
        ("noball", "noball"),
        ("legbye", "legbye"),
        ("bye", "bye"),
    ]

    live_delivery_id = models.BigAutoField(primary_key=True)

    match = models.ForeignKey(
        IPLMatch,
        on_delete=models.CASCADE,
        related_name="live_deliveries",
        db_column="match_id",
    )

    source = models.CharField(max_length=50, default="cricbuzz")
    source_match_id = models.CharField(max_length=50, db_index=True)

    innings = models.PositiveSmallIntegerField(blank=True, null=True)
    over_number = models.PositiveSmallIntegerField(blank=True, null=True)
    ball_number = models.PositiveSmallIntegerField(blank=True, null=True)

    batter = models.ForeignKey(
        Player,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="live_balls_faced",
        db_column="batter_id",
    )
    bowler = models.ForeignKey(
        Player,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="live_balls_bowled",
        db_column="bowler_id",
    )
    non_striker = models.ForeignKey(
        Player,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="live_non_striker_balls",
        db_column="non_striker_id",
    )

    batter_name = models.CharField(max_length=255, blank=True, null=True)
    bowler_name = models.CharField(max_length=255, blank=True, null=True)
    non_striker_name = models.CharField(max_length=255, blank=True, null=True)

    runs_batter = models.PositiveSmallIntegerField(default=0)
    runs_extras = models.PositiveSmallIntegerField(default=0)
    runs_total = models.PositiveSmallIntegerField(default=0)

    extra_type = models.CharField(max_length=20, choices=EXTRA_TYPE_CHOICES, blank=True, null=True)
    is_wicket = models.BooleanField(default=False)
    wicket_kind = models.CharField(max_length=50, blank=True, null=True)
    player_out = models.ForeignKey(
        Player,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="live_dismissal_records",
        db_column="player_out_id",
    )
    player_out_name = models.CharField(max_length=255, blank=True, null=True)

    commentary = models.TextField(blank=True, null=True)
    event_key = models.CharField(max_length=120, unique=True, db_index=True)

    is_confirmed = models.BooleanField(default=False)
    raw_json = models.JSONField(blank=True, null=True)

    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "live_deliveries"
        ordering = ["match", "innings", "over_number", "ball_number", "live_delivery_id"]
        indexes = [
            models.Index(fields=["match", "innings", "over_number", "ball_number"]),
            models.Index(fields=["source_match_id"]),
            models.Index(fields=["updated_at"]),
            models.Index(fields=["is_confirmed"]),
        ]

    def __str__(self):
        return f"{self.match.match_id} live {self.innings}.{self.over_number}.{self.ball_number}"