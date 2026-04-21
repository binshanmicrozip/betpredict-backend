import re
import unicodedata


PLAYER_ALIAS_MAP = {
    "v kohli": "virat kohli",
    "r sharma": "rohit sharma",
    "rg sharma": "rohit sharma",
    "ms dhoni": "ms dhoni",
    "sa yadav": "suryakumar yadav",
    "jj bumrah": "jasprit bumrah",
    "rk singh": "rinku singh",
    "hh pandya": "hardik pandya",
    "sp narine": "sunil narine",
    "ys chahal": "yuzvendra chahal",
    "r jadeja": "ravindra jadeja",
}


def normalize_text(value: str) -> str:
    if not value:
        return ""

    value = unicodedata.normalize("NFKD", str(value))
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_player_name(name: str) -> str:
    normalized = normalize_text(name)
    return PLAYER_ALIAS_MAP.get(normalized, normalized)


def map_role(raw_role: str) -> str:
    raw = normalize_text(raw_role)

    if not raw:
        return "Unknown"

    if any(x in raw for x in ["wk", "wicket keeper", "wicketkeeper", "keeper"]):
        return "Wicketkeeper"

    if any(x in raw for x in ["all rounder", "allrounder", "batting allrounder", "bowling allrounder"]):
        return "All-rounder"

    bowling_keywords = [
        "bowler", "fast", "medium", "pace", "spin",
        "offbreak", "legbreak", "googly", "orthodox",
        "left arm", "right arm", "seam"
    ]
    if any(x in raw for x in bowling_keywords):
        return "Bowler"

    if any(x in raw for x in ["batsman", "batter", "bat"]):
        return "Batsman"

    return "Unknown"


def extract_profile_id(url: str):
    match = re.search(r"/profiles/(\d+)", url or "")
    return match.group(1) if match else None