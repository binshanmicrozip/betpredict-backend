from django.db.models import Sum, Count, Max, Q

from betapp.models import PlayerMatchBatting, PlayerMatchBowling, PlayerSituationStats


def empty_batting_stats():
    return {
        "matches": 0,
        "innings": 0,
        "runs": 0,
        "balls": 0,
        "highest_score": 0,
        "average": None,
        "strike_rate": None,
        "fours": 0,
        "sixes": 0,
        "fifties": 0,
        "hundreds": 0,
        "not_outs": 0,
    }


def empty_bowling_stats():
    return {
        "matches": 0,
        "balls_bowled": 0,
        "wickets": 0,
        "runs_given": 0,
        "economy": None,
        "wides": 0,
        "noballs": 0,
    }


def get_career_batting_stats(player_id):
    data = (
        PlayerMatchBatting.objects.filter(player_id=player_id)
        .aggregate(
            total_matches=Count("match_id", distinct=True),
            total_innings=Count("id"),
            total_runs=Sum("runs"),
            total_balls=Sum("balls_faced"),
            max_score=Max("runs"),
            total_fours=Sum("fours"),
            total_sixes=Sum("sixes"),
            total_fifties=Count("id", filter=Q(runs__gte=50, runs__lt=100)),
            total_hundreds=Count("id", filter=Q(runs__gte=100)),
            total_not_outs=Count("id", filter=Q(is_not_out=True)),
        )
    )

    runs = data["total_runs"] or 0
    balls = data["total_balls"] or 0
    innings = data["total_innings"] or 0
    not_outs = data["total_not_outs"] or 0
    dismissals = innings - not_outs

    return {
        "matches": data["total_matches"] or 0,
        "innings": innings,
        "runs": runs,
        "balls": balls,
        "highest_score": data["max_score"] or 0,
        "average": round(runs / dismissals, 2) if dismissals > 0 else None,
        "strike_rate": round((runs / balls) * 100, 2) if balls else None,
        "fours": data["total_fours"] or 0,
        "sixes": data["total_sixes"] or 0,
        "fifties": data["total_fifties"] or 0,
        "hundreds": data["total_hundreds"] or 0,
        "not_outs": not_outs,
    }


def get_career_bowling_stats(player_id):
    data = (
        PlayerMatchBowling.objects.filter(player_id=player_id)
        .aggregate(
            total_matches=Count("match_id", distinct=True),
            total_balls_bowled=Sum("balls_bowled_calc"),
            total_wickets=Sum("wickets"),
            total_runs_given=Sum("runs_given"),
            total_wides=Sum("wides"),
            total_noballs=Sum("noballs"),
        )
    )

    balls = data["total_balls_bowled"] or 0
    runs = data["total_runs_given"] or 0

    return {
        "matches": data["total_matches"] or 0,
        "balls_bowled": balls,
        "wickets": data["total_wickets"] or 0,
        "runs_given": runs,
        "economy": round((runs / balls) * 6, 2) if balls else None,
        "wides": data["total_wides"] or 0,
        "noballs": data["total_noballs"] or 0,
    }


def get_season_batting_stats(player_id, season):
    data = (
        PlayerMatchBatting.objects.filter(player_id=player_id, match__season=season)
        .aggregate(
            total_matches=Count("match_id", distinct=True),
            total_innings=Count("id"),
            total_runs=Sum("runs"),
            total_balls=Sum("balls_faced"),
            max_score=Max("runs"),
            total_fours=Sum("fours"),
            total_sixes=Sum("sixes"),
            total_fifties=Count("id", filter=Q(runs__gte=50, runs__lt=100)),
            total_hundreds=Count("id", filter=Q(runs__gte=100)),
            total_not_outs=Count("id", filter=Q(is_not_out=True)),
        )
    )

    runs = data["total_runs"] or 0
    balls = data["total_balls"] or 0
    innings = data["total_innings"] or 0
    not_outs = data["total_not_outs"] or 0
    dismissals = innings - not_outs

    return {
        "matches": data["total_matches"] or 0,
        "innings": innings,
        "runs": runs,
        "balls": balls,
        "highest_score": data["max_score"] or 0,
        "average": round(runs / dismissals, 2) if dismissals > 0 else None,
        "strike_rate": round((runs / balls) * 100, 2) if balls else None,
        "fours": data["total_fours"] or 0,
        "sixes": data["total_sixes"] or 0,
        "fifties": data["total_fifties"] or 0,
        "hundreds": data["total_hundreds"] or 0,
        "not_outs": not_outs,
    }


def get_season_bowling_stats(player_id, season):
    data = (
        PlayerMatchBowling.objects.filter(player_id=player_id, match__season=season)
        .aggregate(
            total_matches=Count("match_id", distinct=True),
            total_balls_bowled=Sum("balls_bowled_calc"),
            total_wickets=Sum("wickets"),
            total_runs_given=Sum("runs_given"),
            total_wides=Sum("wides"),
            total_noballs=Sum("noballs"),
        )
    )

    balls = data["total_balls_bowled"] or 0
    runs = data["total_runs_given"] or 0

    return {
        "matches": data["total_matches"] or 0,
        "balls_bowled": balls,
        "wickets": data["total_wickets"] or 0,
        "runs_given": runs,
        "economy": round((runs / balls) * 6, 2) if balls else None,
        "wides": data["total_wides"] or 0,
        "noballs": data["total_noballs"] or 0,
    }


def get_situation_stats(player_id, phase, innings_type):
    obj = PlayerSituationStats.objects.filter(
        player_id=player_id,
        phase=phase,
        innings_type=innings_type,
    ).first()

    if not obj:
        return {
            "found": False,
            "matches_played": None,
            "strike_rate": None,
            "boundary_pct": None,
            "dismissal_rate": None,
            "runs": None,
            "balls": None,
            "wickets_lost": None,
        }

    return {
        "found": True,
        "matches_played": obj.matches_played,
        "strike_rate": float(obj.strike_rate) if obj.strike_rate is not None else None,
        "boundary_pct": float(obj.boundary_pct) if obj.boundary_pct is not None else None,
        "dismissal_rate": float(obj.dismissal_rate) if obj.dismissal_rate is not None else None,
        "runs": obj.runs,
        "balls": obj.balls,
        "wickets_lost": obj.wickets_lost,
    }


def get_role_based_stats(player, season):
    role = (player.role or "").strip().lower()

    career_batting = get_career_batting_stats(player.player_id)
    career_bowling = get_career_bowling_stats(player.player_id)
    current_batting = get_season_batting_stats(player.player_id, season)
    current_bowling = get_season_bowling_stats(player.player_id, season)

    if role == "batsman":
        return {
            "career_batting_stats": career_batting,
            "career_bowling_stats": empty_bowling_stats(),
            "current_ipl_batting_stats": current_batting,
            "current_ipl_bowling_stats": empty_bowling_stats(),
        }

    if role == "bowler":
        return {
            "career_batting_stats": empty_batting_stats(),
            "career_bowling_stats": career_bowling,
            "current_ipl_batting_stats": empty_batting_stats(),
            "current_ipl_bowling_stats": current_bowling,
        }

    if role == "all-rounder":
        return {
            "career_batting_stats": career_batting,
            "career_bowling_stats": career_bowling,
            "current_ipl_batting_stats": current_batting,
            "current_ipl_bowling_stats": current_bowling,
        }

    if role == "wicketkeeper":
        return {
            "career_batting_stats": career_batting,
            "career_bowling_stats": empty_bowling_stats(),
            "current_ipl_batting_stats": current_batting,
            "current_ipl_bowling_stats": empty_bowling_stats(),
        }

    return {
        "career_batting_stats": career_batting,
        "career_bowling_stats": career_bowling,
        "current_ipl_batting_stats": current_batting,
        "current_ipl_bowling_stats": current_bowling,
    }