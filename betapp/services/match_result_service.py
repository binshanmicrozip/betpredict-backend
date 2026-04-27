import logging
from django.utils import timezone
from betapp.models import IPLMatch

logger = logging.getLogger(__name__)

COMPLETED_STATES = {"complete", "finished", "result", "stumps"}


def is_match_complete(cricket: dict) -> bool:
    state = (cricket.get("state") or "").lower().strip()
    status = (cricket.get("status") or "").lower().strip()

    if state in COMPLETED_STATES:
        return True

    result_keywords = ["won by", "match tied", "no result", "abandoned"]
    if any(kw in status for kw in result_keywords):
        return True

    return False


def parse_winner_from_status(status: str) -> tuple:
    if not status:
        return ("", "")

    status_lower = status.lower()

    if "won by" in status_lower:
        parts = status.split(" won by ", 1)
        winner = parts[0].strip()
        margin = parts[1].strip() if len(parts) > 1 else ""
        return (winner, margin)

    if "match tied" in status_lower or "tied" in status_lower:
        return ("Tied", "")

    if "no result" in status_lower:
        return ("No Result", "")

    if "abandoned" in status_lower:
        return ("Abandoned", "")

    return ("", "")


def save_match_result(match_id: str, cricket: dict) -> bool:
    try:
        match = IPLMatch.objects.filter(match_id=str(match_id)).first()

        if not match:
            logger.warning(f"[MatchResult] match_id={match_id} not found in ipl_matches")
            return False

        if match.is_complete:
            logger.debug(f"[MatchResult] match_id={match_id} already complete, skipping")
            return False

        status_text = cricket.get("status") or ""
        winner, winning_margin = parse_winner_from_status(status_text)

        match.is_complete = True
        match.status = status_text
        match.result_summary = status_text
        match.winner = winner
        match.winning_margin = winning_margin
        match.result_updated_at = timezone.now()

        # Parse and save toss if not already set
        if not match.toss_winner and cricket.get("toss"):
            toss_raw = cricket.get("toss") or ""
            if " won the toss" in toss_raw:
                match.toss_winner = toss_raw.split(" won the toss")[0].strip()
            if "elected to bat" in toss_raw.lower():
                match.toss_decision = "bat"
            elif "elected to field" in toss_raw.lower() or "elected to bowl" in toss_raw.lower():
                match.toss_decision = "field"

        match.save()

        logger.info(
            f"[MatchResult] SAVED match_id={match_id} "
            f"winner='{winner}' margin='{winning_margin}'"
        )
        return True

    except Exception as e:
        logger.exception(f"[MatchResult] Error saving result for match_id={match_id}: {e}")
        return False