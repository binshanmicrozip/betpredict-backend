import hashlib
import re
from decimal import Decimal, InvalidOperation

from django.db import transaction

from betapp.models import IPLMatch, Player, LiveMatchState, LiveDelivery


def safe_decimal(value, default="0"):
    try:
        if value in ("", None):
            return Decimal(default)
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def safe_int(value, default=0):
    try:
        if value in ("", None):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_score_text(score_text):
    if not score_text or "/" not in str(score_text):
        return 0, 0
    try:
        runs, wickets = str(score_text).split("/", 1)
        return int(runs), int(wickets)
    except Exception:
        return 0, 0


def extract_toss_values(toss_text):
    if not toss_text:
        return None, None

    toss_winner = None
    toss_decision = None
    text = str(toss_text).strip()

    if " won toss" in text:
        toss_winner = text.split(" won toss", 1)[0].strip()

    if "chose to" in text:
        toss_decision = text.split("chose to", 1)[1].strip().lower()

    return toss_winner, toss_decision


def normalize_player_id_from_name(name):
    if not name:
        return None
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", name.lower()).strip("_")
    cleaned = cleaned[:14] if cleaned else "unknown"
    return f"live_{cleaned}"[:20]


def get_or_create_player(name):
    if not name:
        return None

    player = Player.objects.filter(player_name__iexact=name.strip()).first()
    if player:
        return player

    player_id = normalize_player_id_from_name(name)
    player, _ = Player.objects.get_or_create(
        player_id=player_id,
        defaults={
            "player_name": name.strip(),
            "role": "Unknown",
        },
    )
    return player


def build_event_key(source_match_id, innings, over_number, ball_number, commentary):
    raw = f"{source_match_id}|{innings}|{over_number}|{ball_number}|{commentary or ''}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


@transaction.atomic
def save_live_snapshot(match_id, source_match_id, parsed_data, raw_json):
    match, _ = IPLMatch.objects.get_or_create(
        match_id=str(match_id),
        defaults={
            "season": 2026,
            "match_number": None,
            "match_date": "2026-04-14",
            "venue": "Unknown",
            "city": "Unknown",
            "team_home": parsed_data.get("batting_team") or "Unknown",
            "team_away": parsed_data.get("bowling_team") or "Unknown",
            "toss_winner": None,
            "toss_decision": None,
            "winner": None,
            "win_by_runs": None,
            "win_by_wickets": None,
            "player_of_match": None,
        },
    )

    total_runs, wickets = parse_score_text(parsed_data.get("score"))
    toss_winner, toss_decision = extract_toss_values(parsed_data.get("toss"))

    live_state, _ = LiveMatchState.objects.update_or_create(
        match=match,
        source_match_id=str(source_match_id),
        defaults={
            "source": "cricbuzz",
            "status": parsed_data.get("status"),
            "state": parsed_data.get("state"),
            "innings": safe_int(parsed_data.get("innings"), None) if parsed_data.get("innings") not in ("", None) else None,
            "batting_team": parsed_data.get("batting_team"),
            "bowling_team": parsed_data.get("bowling_team"),
            "score": total_runs,
            "wickets": wickets,
            "overs": safe_decimal(parsed_data.get("overs"), "0"),
            "current_run_rate": safe_decimal(parsed_data.get("crr"), "0") if parsed_data.get("crr") not in ("", None) else None,
            "required_run_rate": safe_decimal(parsed_data.get("rrr"), "0") if parsed_data.get("rrr") not in ("", None) else None,
            "target": safe_int(parsed_data.get("target"), None) if parsed_data.get("target") not in ("", None) else None,
            "toss_winner": toss_winner,
            "toss_decision": toss_decision,
            "partnership_runs": safe_int(parsed_data.get("p_runs"), 0),
            "partnership_balls": safe_int(parsed_data.get("p_balls"), 0),
            "recent_overs": parsed_data.get("recent"),
            "last5_overs_runs": safe_int(parsed_data.get("last5_runs"), None) if parsed_data.get("last5_runs") not in ("", None) else None,
            "last5_overs_wickets": safe_int(parsed_data.get("last5_wkts"), None) if parsed_data.get("last5_wkts") not in ("", None) else None,
            "last3_overs_runs": safe_int(parsed_data.get("last3_runs"), None) if parsed_data.get("last3_runs") not in ("", None) else None,
            "powerplay_runs": safe_int(parsed_data.get("pp_runs"), None) if parsed_data.get("pp_runs") not in ("", None) else None,
            "powerplay_from": safe_decimal(parsed_data.get("pp_from"), "0") if parsed_data.get("pp_from") not in ("", None) else None,
            "powerplay_to": safe_decimal(parsed_data.get("pp_to"), "0") if parsed_data.get("pp_to") not in ("", None) else None,
            "latest_ball_text": parsed_data.get("latest_ball"),
            "raw_json": raw_json,
        },
    )

    # update main IPLMatch table from live data
    update_ipl_match_from_live(match, parsed_data, toss_winner, toss_decision)

    return live_state


def update_ipl_match_from_live(match, parsed_data, toss_winner=None, toss_decision=None):
    changed = False

    if parsed_data.get("batting_team") and match.team_home == "Unknown":
        match.team_home = parsed_data.get("batting_team")
        changed = True

    if parsed_data.get("bowling_team") and match.team_away == "Unknown":
        match.team_away = parsed_data.get("bowling_team")
        changed = True

    if toss_winner and not match.toss_winner:
        match.toss_winner = toss_winner
        changed = True

    if toss_decision and not match.toss_decision:
        match.toss_decision = toss_decision
        changed = True

    status_text = (parsed_data.get("status") or "").lower()
    state_text = (parsed_data.get("state") or "").lower()

    # if match completed, update result
    if any(x in status_text for x in ["won by", "match tied", "no result", "abandoned"]) or state_text in ["complete", "completed"]:
        winner_team = parsed_data.get("winner_team")
        win_by_runs = parsed_data.get("win_by_runs")
        win_by_wickets = parsed_data.get("win_by_wickets")
        player_of_match = parsed_data.get("player_of_match")

        if winner_team:
            match.winner = winner_team
            changed = True

        if win_by_runs not in (None, ""):
            try:
                match.win_by_runs = int(win_by_runs)
                changed = True
            except Exception:
                pass

        if win_by_wickets not in (None, ""):
            try:
                match.win_by_wickets = int(win_by_wickets)
                changed = True
            except Exception:
                pass

        if player_of_match:
            match.player_of_match = player_of_match
            changed = True

    if changed:
        match.save()


def parse_commentary_item(raw_item):
    innings = raw_item.get("inningsId") or raw_item.get("innings")
    over_number = raw_item.get("overNumber") or raw_item.get("overNum")
    ball_number = raw_item.get("ballNbr") or raw_item.get("ballNumber")
    commentary = raw_item.get("commText") or raw_item.get("commentText") or ""

    batter_name = (
        raw_item.get("batsmanName")
        or raw_item.get("strikerName")
        or raw_item.get("batName")
    )
    bowler_name = raw_item.get("bowlerName") or raw_item.get("bowlName")
    non_striker_name = raw_item.get("nonStrikerName")

    runs_batter = safe_int(
        raw_item.get("runs")
        or raw_item.get("batRuns")
        or raw_item.get("batterRuns"),
        0
    )
    runs_extras = safe_int(raw_item.get("extras"), 0)
    runs_total = safe_int(
        raw_item.get("totalRuns")
        or raw_item.get("runsTotal"),
        runs_batter + runs_extras
    )

    is_wicket = bool(raw_item.get("isWicket", False))
    wicket_kind = raw_item.get("wicketType") or raw_item.get("wicketKind")
    player_out_name = raw_item.get("outPlayerName")
    extra_type = raw_item.get("extraType")

    return {
        "innings": safe_int(innings, None) if innings not in ("", None) else None,
        "over_number": safe_int(over_number, None) if over_number not in ("", None) else None,
        "ball_number": safe_int(ball_number, None) if ball_number not in ("", None) else None,
        "commentary": commentary,
        "batter_name": batter_name,
        "bowler_name": bowler_name,
        "non_striker_name": non_striker_name,
        "runs_batter": runs_batter,
        "runs_extras": runs_extras,
        "runs_total": runs_total,
        "extra_type": extra_type,
        "is_wicket": is_wicket,
        "wicket_kind": wicket_kind,
        "player_out_name": player_out_name,
    }


@transaction.atomic
def save_live_commentary(match_id, source_match_id, raw_json, parsed_data=None):
    match, _ = IPLMatch.objects.get_or_create(
        match_id=str(match_id),
        defaults={
            "season": 2026,
            "match_number": None,
            "match_date": "2026-04-14",
            "venue": "Unknown",
            "city": "Unknown",
            "team_home": parsed_data.get("batting_team") if parsed_data else "Unknown",
            "team_away": parsed_data.get("bowling_team") if parsed_data else "Unknown",
        },
    )

    commentary_list = raw_json.get("commentaryList", []) or []
    saved_count = 0

    for raw_item in commentary_list:
        item = parse_commentary_item(raw_item)

        batter_name = item["batter_name"] or (parsed_data.get("b1_name") if parsed_data else None)
        bowler_name = item["bowler_name"] or (parsed_data.get("bw1_name") if parsed_data else None)
        non_striker_name = item["non_striker_name"] or (parsed_data.get("b2_name") if parsed_data else None)

        batter = get_or_create_player(batter_name) if batter_name else None
        bowler = get_or_create_player(bowler_name) if bowler_name else None
        non_striker = get_or_create_player(non_striker_name) if non_striker_name else None
        player_out = get_or_create_player(item["player_out_name"]) if item["player_out_name"] else None

        event_key = build_event_key(
            source_match_id=source_match_id,
            innings=item["innings"],
            over_number=item["over_number"],
            ball_number=item["ball_number"],
            commentary=item["commentary"],
        )

        LiveDelivery.objects.update_or_create(
            event_key=event_key,
            defaults={
                "match": match,
                "source": "cricbuzz",
                "source_match_id": str(source_match_id),
                "innings": item["innings"],
                "over_number": item["over_number"],
                "ball_number": item["ball_number"],
                "batter": batter,
                "bowler": bowler,
                "non_striker": non_striker,
                "batter_name": batter_name,
                "bowler_name": bowler_name,
                "non_striker_name": non_striker_name,
                "runs_batter": item["runs_batter"] or 0,
                "runs_extras": item["runs_extras"] or 0,
                "runs_total": item["runs_total"] or 0,
                "extra_type": item["extra_type"],
                "is_wicket": item["is_wicket"],
                "wicket_kind": item["wicket_kind"],
                "player_out": player_out,
                "player_out_name": item["player_out_name"],
                "commentary": item["commentary"],
                "raw_json": raw_item,
            },
        )
        saved_count += 1

    return saved_count