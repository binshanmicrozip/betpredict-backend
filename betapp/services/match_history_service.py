from collections import defaultdict

from betapp.models import (
    IPLMatch,
    Delivery,
    MatchPlayer,
    PlayerMatchBatting,
    PlayerMatchBowling,Signal
 
)
from betapp.predictor import detect_pattern


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def serialize_match(match: IPLMatch) -> dict:
    return {
        "match_id": match.match_id,
        "season": match.season,
        "match_date": match.match_date,
        "match_number": match.match_number,
        "team1": match.team1,
        "team2": match.team2,
        "toss_winner": match.toss_winner,
        "toss_decision": match.toss_decision,
        "venue": match.venue,
        "status": match.status,
    }


def build_innings_summary(match: IPLMatch) -> list[dict]:
    innings_data = []

    for innings_no in [1, 2]:
        deliveries = Delivery.objects.filter(match=match, innings=innings_no).order_by("over_number", "ball_number")

        if not deliveries.exists():
            continue

        total_runs = sum(d.runs_total for d in deliveries)
        wickets = sum(1 for d in deliveries if d.is_wicket)
        last_ball = deliveries.last()
        overs = f"{last_ball.over_number}.{last_ball.ball_number}" if last_ball else "0.0"

        batting_team = None
        first_delivery = deliveries.first()
        if first_delivery and first_delivery.batter:
            batter_player_id = first_delivery.batter_id
            mp = MatchPlayer.objects.filter(match=match, player_id=batter_player_id).first()
            batting_team = getattr(mp, "team_name", None) if mp else None

        innings_data.append({
            "innings": innings_no,
            "batting_team": batting_team,
            "score": total_runs,
            "wickets": wickets,
            "overs": overs,
        })

    return innings_data


def serialize_batting_scorecard(match: IPLMatch) -> list[dict]:
    rows = (
        PlayerMatchBatting.objects
        .filter(match=match)
        .select_related("player")
        .order_by("innings", "-runs", "player__player_name")
    )

    result = []
    for row in rows:
        result.append({
            "innings": row.innings,
            "player_id": row.player_id,
            "player_name": row.player.player_name if row.player else None,
            "runs": row.runs,
            "balls_faced": row.balls_faced,
            "fours": row.fours,
            "sixes": row.sixes,
            "strike_rate": float(row.strike_rate) if row.strike_rate is not None else None,
            "dismissal_kind": row.dismissal_kind,
            "is_not_out": row.is_not_out,
        })
    return result


def serialize_bowling_scorecard(match: IPLMatch) -> list[dict]:
    rows = (
        PlayerMatchBowling.objects
        .filter(match=match)
        .select_related("player")
        .order_by("innings", "-wickets", "economy", "player__player_name")
    )

    result = []
    for row in rows:
        result.append({
            "innings": row.innings,
            "player_id": row.player_id,
            "player_name": row.player.player_name if row.player else None,
            "overs_bowled": float(row.overs_bowled) if row.overs_bowled is not None else None,
            "balls_bowled_calc": row.balls_bowled_calc,
            "runs_given": row.runs_given,
            "wickets": row.wickets,
            "economy": float(row.economy) if row.economy is not None else None,
            "wides": row.wides,
            "noballs": row.noballs,
        })
    return result


def serialize_deliveries(match: IPLMatch) -> list[dict]:
    rows = (
        Delivery.objects
        .filter(match=match)
        .select_related("batter", "bowler", "non_striker", "player_out")
        .order_by("innings", "over_number", "ball_number")
    )

    result = []
    for d in rows:
        result.append({
            "innings": d.innings,
            "over_number": d.over_number,
            "ball_number": d.ball_number,
            "batter": d.batter.player_name if d.batter else None,
            "bowler": d.bowler.player_name if d.bowler else None,
            "non_striker": d.non_striker.player_name if d.non_striker else None,
            "player_out": d.player_out.player_name if d.player_out else None,
            "runs_batter": d.runs_batter,
            "runs_extras": d.runs_extras,
            "runs_total": d.runs_total,
            "extra_type": d.extra_type,
            "is_wicket": d.is_wicket,
            "wicket_kind": d.wicket_kind,
        })
    return result


def build_over_summaries(match: IPLMatch) -> list[dict]:
    deliveries = (
        Delivery.objects
        .filter(match=match)
        .order_by("innings", "over_number", "ball_number")
        .select_related("batter", "bowler", "player_out")
    )

    grouped = defaultdict(list)
    for d in deliveries:
        grouped[(d.innings, d.over_number)].append(d)

    over_summaries = []

    innings_score = defaultdict(int)
    innings_wickets = defaultdict(int)

    previous_over_runs = defaultdict(int)
    previous_two_over_runs = defaultdict(int)
    previous_over_wickets = defaultdict(int)

    for (innings, over_number), balls in grouped.items():
        over_runs = sum(b.runs_total for b in balls)
        over_wickets = sum(1 for b in balls if b.is_wicket)
        boundaries = sum(1 for b in balls if b.runs_batter in (4, 6))
        dots = sum(1 for b in balls if b.runs_total == 0 and not b.extra_type)
        sixes = sum(1 for b in balls if b.runs_batter == 6)
        fours = sum(1 for b in balls if b.runs_batter == 4)

        last_ball = balls[-1]
        last_ball_wicket = 1 if last_ball.is_wicket else 0
        last_ball_six = 1 if last_ball.runs_batter == 6 else 0

        innings_score[innings] += over_runs
        innings_wickets[innings] += over_wickets

        completed_overs = over_number + 1
        current_run_rate = (innings_score[innings] / completed_overs) if completed_overs else 0.0

        rolling_3_runs = previous_two_over_runs[innings] + previous_over_runs[innings] + over_runs
        rolling_6_runs = rolling_3_runs
        rolling_3_wickets = previous_over_wickets[innings] + over_wickets
        rolling_6_dots = dots
        rolling_6_boundaries = boundaries

        wickets_before = innings_wickets[innings]
        wickets_in_hand = max(0, 10 - wickets_before)

        is_powerplay = 1 if over_number <= 5 else 0
        is_middle = 1 if 6 <= over_number <= 14 else 0
        is_death = 1 if over_number >= 15 else 0
        is_innings_2 = 1 if innings == 2 else 0

        batting_momentum = rolling_3_runs / 18.0 if 18 else 0.0

        # For historical completed matches, required rate is not exact from delivery-only here.
        # Keep 0 if not available.
        required_rate = 0.0

        features = {
            "over": over_number + 1,
            "is_powerplay": is_powerplay,
            "is_middle": is_middle,
            "is_death": is_death,
            "is_innings_2": is_innings_2,
            "last_ball_wicket": last_ball_wicket,
            "last_ball_six": last_ball_six,
            "rolling_3_wickets": rolling_3_wickets,
            "rolling_3_runs": rolling_3_runs,
            "rolling_6_runs": rolling_6_runs,
            "rolling_6_dots": rolling_6_dots,
            "rolling_6_boundaries": rolling_6_boundaries,
            "batting_momentum": round(batting_momentum, 4),
            "wickets_before": wickets_before,
            "wickets_in_hand": wickets_in_hand,
            "current_run_rate": round(current_run_rate, 4),
            "required_rate": required_rate,
            "price": 2.0,
            "price_trend_3": 0.0,
            "pp_runs": innings_score[innings] if is_powerplay else 0,
        }

        pattern = detect_pattern(features)

        over_summaries.append({
            "innings": innings,
            "over_number": over_number,
            "runs": over_runs,
            "wickets": over_wickets,
            "boundaries": boundaries,
            "dots": dots,
            "fours": fours,
            "sixes": sixes,
            "score_after_over": innings_score[innings],
            "wickets_after_over": innings_wickets[innings],
            "features_used": features,
            "pattern": pattern,
        })

        previous_two_over_runs[innings] = previous_over_runs[innings]
        previous_over_runs[innings] = over_runs
        previous_over_wickets[innings] = over_wickets

    return over_summaries


def serialize_related_signals(match: IPLMatch) -> list[dict]:
    rows = Signal.objects.filter(match=match).order_by("created_at")

    result = []
    for s in rows:
        result.append({
            "id": s.id,
            "market_id": s.market_id,
            "runner_id": s.runner_id,
            "striker_name": s.striker_name,
            "phase": s.phase,
            "innings_type": s.innings_type,
            "final_probability": float(s.final_probability) if s.final_probability is not None else None,
            "signal": s.signal,
            "model_source": s.model_source,
            "raw_output": s.raw_output,
            "created_at": s.created_at,
        })
    return result


def get_match_history_payload(match_id: str) -> dict:
    match = IPLMatch.objects.filter(match_id=str(match_id)).first()
    if not match:
        return {"found": False, "message": "Match not found"}

    over_summaries = build_over_summaries(match)

    # summary of patterns
    pattern_counter = defaultdict(int)
    for over_item in over_summaries:
        pattern_name = (over_item.get("pattern") or {}).get("pattern_name")
        if pattern_name:
            pattern_counter[pattern_name] += 1

    top_patterns = sorted(
        [{"pattern_name": k, "count": v} for k, v in pattern_counter.items()],
        key=lambda x: (-x["count"], x["pattern_name"])
    )

    return {
        "found": True,
        "match": serialize_match(match),
        "innings_summary": build_innings_summary(match),
        "batting_scorecard": serialize_batting_scorecard(match),
        "bowling_scorecard": serialize_bowling_scorecard(match),
        "deliveries": serialize_deliveries(match),
        "over_summaries": over_summaries,
        "top_patterns": top_patterns,
        "related_signals": serialize_related_signals(match),
    }