import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote

from betapp.utils.player_profile_utils import (
    normalize_player_name,
    map_role,
    extract_profile_id,
)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
}

PROFILE_URL_MAP = {
    "virat kohli": "https://www.cricbuzz.com/profiles/1413/virat-kohli",
    "rohit sharma": "https://www.cricbuzz.com/profiles/576/rohit-sharma",
    "ms dhoni": "https://www.cricbuzz.com/profiles/265/ms-dhoni",
    "rinku singh": "https://www.cricbuzz.com/profiles/14628/rinku-singh",
    "hardik pandya": "https://www.cricbuzz.com/profiles/9647/hardik-pandya",
    "jasprit bumrah": "https://www.cricbuzz.com/profiles/9311/jasprit-bumrah",
    "suryakumar yadav": "https://www.cricbuzz.com/profiles/7915/suryakumar-yadav",
    "sunil narine": "https://www.cricbuzz.com/profiles/4810/sunil-narine",
    "yuzvendra chahal": "https://www.cricbuzz.com/profiles/7910/yuzvendra-chahal",
    "ravindra jadeja": "https://www.cricbuzz.com/profiles/587/ravindra-jadeja",
}


class CricbuzzProfileService:
    SEARCH_URL = "https://www.cricbuzz.com/search?q={query}"
    BASE_URL = "https://www.cricbuzz.com"

    def resolve_profile_url(self, player_name: str):
        player_name = (player_name or "").strip()
        if not player_name:
            return None

        search_url = self.search_profile_url(player_name)
        if search_url:
            return search_url

        normalized_name = normalize_player_name(player_name)
        return PROFILE_URL_MAP.get(normalized_name)

    def search_profile_url(self, player_name: str):
        try:
            url = self.SEARCH_URL.format(query=quote(player_name))
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
        except Exception as exc:
            print(f"[Cricbuzz] Search failed for {player_name}: {exc}")
            return None

        profile_urls = self.extract_profile_urls(response.text)
        if not profile_urls:
            return None

        return self.pick_best_profile(player_name, profile_urls)

    def extract_profile_urls(self, html: str):
        matches = re.findall(r'href="(/profiles/\d+/[^"]+)"', html)
        urls = []
        for match in matches:
            full_url = self.BASE_URL + match
            if full_url not in urls:
                urls.append(full_url)
        return urls

    def pick_best_profile(self, player_name: str, profile_urls):
        target = normalize_player_name(player_name)
        target_parts = set(target.split())
        scored = []

        for url in profile_urls:
            slug = url.rstrip("/").split("/")[-1]
            slug_name = normalize_player_name(slug.replace("-", " "))
            slug_parts = set(slug_name.split())

            score = 0
            if slug_name == target:
                score += 100

            common = len(target_parts & slug_parts)
            score += common * 10

            if target in slug_name:
                score += 15
            if slug_name in target:
                score += 10

            scored.append((score, url))

        scored.sort(reverse=True, key=lambda x: x[0])
        return scored[0][1] if scored and scored[0][0] > 0 else None

    def parse_profile_page(self, profile_url: str):
        try:
            response = requests.get(profile_url, headers=HEADERS, timeout=20)
            response.raise_for_status()
        except Exception as exc:
            print(f"[Cricbuzz] Profile fetch failed for {profile_url}: {exc}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text("\n", strip=True)

        role = self.extract_role(text)
        country = self.extract_country(text)

        return {
            "cricbuzz_profile_id": extract_profile_id(profile_url),
            "cricbuzz_profile_url": profile_url,
            "country": country,
            "role": role,
        }

    def extract_role(self, text: str):
        patterns = [
            r"Role\s*[:\-]?\s*([A-Za-z\-\s]+)",
            r"role\s*[:\-]?\s*([A-Za-z\-\s]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                mapped = map_role(value)
                if mapped != "Unknown":
                    return mapped

        for keyword in [
            "WK-Batsman",
            "Wicketkeeper",
            "Batting Allrounder",
            "Bowling Allrounder",
            "Allrounder",
            "All-rounder",
            "Bowler",
            "Batsman",
            "Batter",
        ]:
            if keyword.lower() in text.lower():
                mapped = map_role(keyword)
                if mapped != "Unknown":
                    return mapped

        return "Unknown"

    def extract_country(self, text: str):
        known_countries = [
            "India",
            "Australia",
            "England",
            "South Africa",
            "New Zealand",
            "Pakistan",
            "Sri Lanka",
            "Bangladesh",
            "Afghanistan",
            "West Indies",
            "Zimbabwe",
            "Ireland",
            "Netherlands",
            "Scotland",
            "Nepal",
            "UAE",
        ]

        lines = [line.strip() for line in text.split("\n") if line.strip()]

        for i, line in enumerate(lines[:80]):
            if line.lower() == "info" and i > 0:
                prev = lines[i - 1]
                if prev in known_countries:
                    return prev

        for country in known_countries:
            if re.search(rf"\b{re.escape(country)}\b", text):
                return country

        return None

    def parse_ipl_years_from_profile(self, profile_url: str):
        batting_url = profile_url.rstrip("/") + "/all-matches/batting"

        try:
            response = requests.get(batting_url, headers=HEADERS, timeout=20)
            response.raise_for_status()
        except Exception as exc:
            print(f"[Cricbuzz] All-matches fetch failed for {profile_url}: {exc}")
            return {
                "debut_year": None,
                "last_season": None,
                "ipl_debut": None,
            }

        text = response.text
        years = set()

        for match in re.finditer(r"Indian Premier League\s+(\d{4})", text, flags=re.IGNORECASE):
            years.add(int(match.group(1)))

        debut_year = min(years) if years else None
        last_season = max(years) if years else None

        return {
            "debut_year": debut_year,
            "last_season": last_season,
            "ipl_debut": None,
        }

    def fetch_full_profile(self, player_name: str, sleep_sec: float = 0.4):
        profile_url = self.resolve_profile_url(player_name)
        if not profile_url:
            return {
                "status": "not_found",
                "player_name": player_name,
                "message": "No Cricbuzz profile found",
            }

        profile_data = self.parse_profile_page(profile_url)
        if not profile_data:
            return {
                "status": "error",
                "player_name": player_name,
                "message": "Failed to parse profile page",
            }

        ipl_data = self.parse_ipl_years_from_profile(profile_url)
        time.sleep(sleep_sec)

        return {
            "status": "found",
            "player_name": player_name,
            "profile_url": profile_url,
            "profile_data": profile_data,
            "ipl_data": ipl_data,
        }