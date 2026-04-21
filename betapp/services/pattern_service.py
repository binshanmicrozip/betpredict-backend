import json
import os
from django.conf import settings

FILE_PATH = os.path.join(settings.BASE_DIR, "betapp", "patterns_library.json")


def load_patterns():
    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError(f"Pattern JSON file not found: {FILE_PATH}")

    with open(FILE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_all_patterns():
    data = load_patterns()
    return data.get("library", {})


def get_patterns_by_category(category):
    data = load_patterns()
    return data.get("library", {}).get(category, {})


def get_pattern_by_name(pattern_name):
    data = load_patterns()
    library = data.get("library", {})

    for category, category_data in library.items():
        patterns = category_data.get("patterns", {})
        if pattern_name in patterns:
            return {
                "category": category,
                "pattern_name": pattern_name,
                **patterns[pattern_name],
            }

    return None