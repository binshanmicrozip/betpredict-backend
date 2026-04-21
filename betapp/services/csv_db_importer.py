import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict

from django.db import transaction

from betapp.models import (
    Player,
    IPLMatch,
    MatchPlayer,
    Delivery,
    PlayerMatchBatting,
    PlayerMatchBowling,
)
from betapp.utils.player_profile_utils import normalize_player_name


EXTRA_BALL_NOT_COUNT_FOR_BATTER = {"wide"}
EXTRA_BALL_NOT_COUNT_FOR_BOWLER = {"wide", "noball"}

WICKET_EXCLUDE_FOR_BOWLER = {
    "run out",
    "retired hurt",
    "retired out",
    "obstructing the field",
}

WICKET_EXCLUDE_FOR_BATTER_OUT = {
    "retired hurt",
}


def to_decimal(value, places="0.01"):
    return Decimal(str(value)).quantize(Decimal(places), rounding=ROUND_HALF_UP)


def balls_to_overs_decimal(legal_balls: int) -> Decimal:
    overs = legal_balls // 6
    balls = legal_balls % 6
    return Decimal(f"{overs}.{balls}")


def safe_int(value, default=None):
    if pd.isna(value) or value == "":
        return default
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def safe_str(value, default=""):
    if pd.isna(value):
        return default
    return str(value).strip()


def build_match_status(outcome_winner, outcome_runs, outcome_wickets):
    outcome_winner = safe_str(outcome_winner, "")
    outcome_runs = safe_int(outcome_runs, 0)
    outcome_wickets = safe_int(outcome_wickets, 0)

    if outcome_winner and outcome_runs > 0:
        return f"{outcome_winner} won by {outcome_runs} runs"
    if outcome_winner and outcome_wickets > 0:
        return f"{outcome_winner} won by {outcome_wickets} wickets"
    if outcome_winner:
        return f"{outcome_winner} won"
    return "completed"


def get_or_create_player(player_name: str):
    player_name = safe_str(player_name)
    if not player_name:
        return None

    normalized_name = normalize_player_name(player_name)

    player = Player.objects.filter(normalized_name=normalized_name).first()
    if player:
        changed_fields = []

        if not player.player_name:
            player.player_name = player_name
            changed_fields.append("player_name")

        if not player.normalized_name:
            player.normalized_name = normalized_name
            changed_fields.append("normalized_name")

        if changed_fields:
            player.save(update_fields=changed_fields)

        return player

    base_player_id = normalized_name.replace(" ", "")[:40] or player_name.lower().replace(" ", "")
    player_id = base_player_id
    suffix = 1

    while Player.objects.filter(player_id=player_id).exists():
        existing = Player.objects.filter(player_id=player_id).first()
        if existing and existing.normalized_name == normalized_name:
            return existing
        player_id = f"{base_player_id}_{suffix}"
        suffix += 1

    return Player.objects.create(
        player_id=player_id,
        player_name=player_name,
        normalized_name=normalized_name,
        country=None,
        role="Unknown",
    )


@transaction.atomic
def import_one_match_from_csv_rows(match_df: pd.DataFrame):
    if match_df.empty:
        return {"status": "skipped", "reason": "empty dataframe"}

    first = match_df.iloc[0]

    match_id = safe_str(first["match_id"])
    season = safe_int(first["season"], None)

    # If season is missing in CSV, derive from match_date
    match_date = first["match_date"] if not pd.isna(first["match_date"]) else None
    if season is None and match_date:
        try:
            season = int(str(match_date)[:4])
        except Exception:
            season = None

    if season is None:
        return {
            "status": "skipped",
            "match_id": match_id,
            "reason": "season missing",
        }

    venue = safe_str(first["venue"], None)
    match_number = safe_int(first["match_number"], None)
    toss_winner = safe_str(first["toss_winner"], None)
    toss_decision = safe_str(first["toss_decision"], None)

    teams = [t for t in match_df["batting_team"].dropna().astype(str).unique().tolist() if t.strip()]
    team1 = teams[0] if len(teams) > 0 else None
    team2 = teams[1] if len(teams) > 1 else None

    outcome_winner = safe_str(first.get("outcome_winner", ""), "")
    outcome_runs = safe_int(first.get("outcome_runs", 0), 0)
    outcome_wickets = safe_int(first.get("outcome_wickets", 0), 0)
    status = build_match_status(outcome_winner, outcome_runs, outcome_wickets)

    match, _ = IPLMatch.objects.update_or_create(
        match_id=match_id,
        defaults={
            "season": season,
            "match_date": match_date,
            "match_number": match_number,
            "team1": team1,
            "team2": team2,
            "toss_winner": toss_winner,
            "toss_decision": toss_decision,
            "venue": venue,
            "status": status,
        },
    )

    Delivery.objects.filter(match=match).delete()
    PlayerMatchBatting.objects.filter(match=match).delete()
    PlayerMatchBowling.objects.filter(match=match).delete()
    MatchPlayer.objects.filter(match=match).delete()

    players_in_match = {}
    batting_data = {}
    bowling_data = {}

    sorted_rows = match_df.sort_values(by=["innings", "over", "ball_in_over"]).reset_index(drop=True)

    for _, row in sorted_rows.iterrows():
        innings = safe_int(row["innings"], 1)
        over_number = safe_int(row["over"], 0)
        ball_number = safe_int(row["ball_in_over"], 0) + 1

        batter_name = safe_str(row["batter"], "")
        bowler_name = safe_str(row["bowler"], "")
        non_striker_name = safe_str(row["non_striker"], "")
        batting_team = safe_str(row["batting_team"], "")

        batter = get_or_create_player(batter_name) if batter_name else None
        bowler = get_or_create_player(bowler_name) if bowler_name else None
        non_striker = get_or_create_player(non_striker_name) if non_striker_name else None

        if batter:
            players_in_match[batter.player_id] = batter
        if bowler:
            players_in_match[bowler.player_id] = bowler
        if non_striker:
            players_in_match[non_striker.player_id] = non_striker

        is_wide = safe_int(row["is_wide"], 0) == 1
        is_noball = safe_int(row["is_noball"], 0) == 1

        extra_type = None
        if is_wide:
            extra_type = "wide"
        elif is_noball:
            extra_type = "noball"

        runs_batter = safe_int(row["runs_batter"], 0)
        runs_extras = safe_int(row["runs_extras"], 0)
        runs_total = safe_int(row["runs_total"], 0)

        wickets_this_ball = safe_int(row["wickets_this_ball"], 0)
        wicket_kind = safe_str(row["wicket_kind"], None)
        player_out_name = safe_str(row["player_out"], "")
        player_out = get_or_create_player(player_out_name) if player_out_name else None

        if player_out:
            players_in_match[player_out.player_id] = player_out

        Delivery.objects.create(
            match=match,
            innings=innings,
            over_number=over_number,
            ball_number=ball_number,
            batter=batter,
            bowler=bowler,
            non_striker=non_striker,
            player_out=player_out,
            runs_batter=runs_batter,
            runs_extras=runs_extras,
            runs_total=runs_total,
            extra_type=extra_type,
            is_wicket=wickets_this_ball > 0,
            wicket_kind=wicket_kind,
        )

        if batter:
            b_key = (innings, batter.player_id)
            if b_key not in batting_data:
                batting_data[b_key] = {
                    "player": batter,
                    "runs": 0,
                    "balls": 0,
                    "fours": 0,
                    "sixes": 0,
                    "dismissal": None,
                }

            b = batting_data[b_key]
            b["runs"] += runs_batter

            if extra_type not in EXTRA_BALL_NOT_COUNT_FOR_BATTER:
                b["balls"] += 1

            if runs_batter == 4:
                b["fours"] += 1
            elif runs_batter == 6:
                b["sixes"] += 1

            if player_out and player_out.player_id == batter.player_id:
                if (wicket_kind or "").lower() not in WICKET_EXCLUDE_FOR_BATTER_OUT:
                    b["dismissal"] = wicket_kind

        if bowler:
            bo_key = (innings, bowler.player_id)
            if bo_key not in bowling_data:
                bowling_data[bo_key] = {
                    "player": bowler,
                    "balls": 0,
                    "runs": 0,
                    "wickets": 0,
                    "wides": 0,
                    "noballs": 0,
                }

            bo = bowling_data[bo_key]
            bo["runs"] += runs_total

            if extra_type == "wide":
                bo["wides"] += runs_extras
            elif extra_type == "noball":
                bo["noballs"] += runs_extras

            if extra_type not in EXTRA_BALL_NOT_COUNT_FOR_BOWLER:
                bo["balls"] += 1

            if wickets_this_ball > 0 and wicket_kind:
                if wicket_kind.lower() not in WICKET_EXCLUDE_FOR_BOWLER:
                    bo["wickets"] += 1

    for player in players_in_match.values():
        MatchPlayer.objects.get_or_create(match=match, player=player)

    batting_rows = 0
    for (innings, _player_id), row in batting_data.items():
        balls = row["balls"]
        strike_rate = to_decimal((row["runs"] / balls) * 100) if balls else Decimal("0.00")

        PlayerMatchBatting.objects.create(
            match=match,
            player=row["player"],
            innings=innings,
            runs=row["runs"],
            balls_faced=balls,
            fours=row["fours"],
            sixes=row["sixes"],
            strike_rate=strike_rate,
            dismissal_kind=row["dismissal"],
            is_not_out=row["dismissal"] is None,
        )
        batting_rows += 1

    bowling_rows = 0
    for (innings, _player_id), row in bowling_data.items():
        balls = row["balls"]
        economy = to_decimal((row["runs"] / balls) * 6) if balls else Decimal("0.00")

        PlayerMatchBowling.objects.create(
            match=match,
            player=row["player"],
            innings=innings,
            overs_bowled=balls_to_overs_decimal(balls),
            balls_bowled_calc=balls,
            runs_given=row["runs"],
            wickets=row["wickets"],
            economy=economy,
            wides=row["wides"],
            noballs=row["noballs"],
        )
        bowling_rows += 1

    return {
        "status": "imported",
        "match_id": match_id,
        "season": season,
        "deliveries": len(sorted_rows),
        "players": len(players_in_match),
        "batting_rows": batting_rows,
        "bowling_rows": bowling_rows,
    }


def import_cricsheet_csv_to_db(csv_path: str):
    df = pd.read_csv(csv_path, low_memory=False)

    if df.empty:
        return {
            "status": "no_data",
            "matches_processed": 0,
            "imported": [],
        }

    required_columns = [
        "match_id",
        "match_date",
        "season",
        "venue",
        "match_number",
        "batting_team",
        "bowling_team",
        "toss_winner",
        "toss_decision",
        "outcome_winner",
        "outcome_runs",
        "outcome_wickets",
        "innings",
        "over",
        "ball_in_over",
        "batter",
        "bowler",
        "non_striker",
        "runs_batter",
        "runs_extras",
        "runs_total",
        "is_wide",
        "is_noball",
        "wickets_this_ball",
        "wicket_kind",
        "player_out",
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required CSV columns: {missing}")

    imported_results = []
    match_ids = df["match_id"].astype(str).unique().tolist()

    for match_id in match_ids:
        match_df = df[df["match_id"].astype(str) == str(match_id)].copy()
        result = import_one_match_from_csv_rows(match_df)
        imported_results.append(result)

    return {
        "status": "success",
        "matches_processed": len(imported_results),
        "imported": imported_results,
    }