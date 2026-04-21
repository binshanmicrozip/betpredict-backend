import re
import time
import uuid
import unicodedata
from collections import defaultdict
from datetime import date

import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand
from django.db import transaction

from betapp.models import (
    Player,
    PlayerIPLTeam,
    MatchPlayer,
    PlayerMatchBatting,
    PlayerMatchBowling,
    PlayerSituationStats,
    Delivery,
)

BASE_URL = "https://www.cricbuzz.com"
SQUADS_URL = "https://www.cricbuzz.com/cricket-series/9241/indian-premier-league-2026/squads"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

TEAM_SHORT_MAP = {
    "Chennai Super Kings": "CSK",
    "Delhi Capitals": "DC",
    "Gujarat Titans": "GT",
    "Royal Challengers Bengaluru": "RCB",
    "Punjab Kings": "PBKS",
    "Kolkata Knight Riders": "KKR",
    "Sunrisers Hyderabad": "SRH",
    "Rajasthan Royals": "RR",
    "Lucknow Super Giants": "LSG",
    "Mumbai Indians": "MI",
}

# Updated fallback pages:
# Cricbuzz currently exposes working team roster pages under:
# /cricket-team/<slug>/<team_id>/players
TEAM_PAGE_FALLBACK = {
    "Chennai Super Kings": "https://www.cricbuzz.com/cricket-team/chennai-super-kings/58/players",
    "Delhi Capitals": "https://www.cricbuzz.com/cricket-team/delhi-capitals/61/players",
    "Gujarat Titans": "https://www.cricbuzz.com/cricket-team/gujarat-titans/971/players",
    "Royal Challengers Bengaluru": "https://www.cricbuzz.com/cricket-team/royal-challengers-bengaluru/59/players",
    "Punjab Kings": "https://www.cricbuzz.com/cricket-team/punjab-kings/65/players",
    "Kolkata Knight Riders": "https://www.cricbuzz.com/cricket-team/kolkata-knight-riders/63/players",
    "Sunrisers Hyderabad": "https://www.cricbuzz.com/cricket-team/sunrisers-hyderabad/255/players",
    "Rajasthan Royals": "https://www.cricbuzz.com/cricket-team/rajasthan-royals/64/players",
    "Lucknow Super Giants": "https://www.cricbuzz.com/cricket-team/lucknow-super-giants/966/players",
    "Mumbai Indians": "https://www.cricbuzz.com/cricket-team/mumbai-indians/62/players",
}

COUNTRY_LIST = {
    "India", "Australia", "England", "South Africa", "New Zealand",
    "Sri Lanka", "West Indies", "Pakistan", "Bangladesh", "Afghanistan",
    "Zimbabwe", "Ireland", "Scotland", "Netherlands", "Nepal", "Namibia",
}

ROLE_MAP = {
    "wk-batsman": "Wicketkeeper",
    "wk-batter": "Wicketkeeper",
    "wicket keeper": "Wicketkeeper",
    "wicketkeeper": "Wicketkeeper",
    "keeper": "Wicketkeeper",
    "batsman": "Batsman",
    "batter": "Batsman",
    "bowler": "Bowler",
    "batting allrounder": "All-rounder",
    "bowling allrounder": "All-rounder",
    "all-rounder": "All-rounder",
    "allrounder": "All-rounder",
}


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = name.lower().strip()
    name = name.replace(".", " ")
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def map_role(raw_role: str) -> str:
    if not raw_role:
        return "Unknown"

    role_text = raw_role.lower().strip()

    for key, value in ROLE_MAP.items():
        if key in role_text:
            return value

    if "all" in role_text and "round" in role_text:
        return "All-rounder"
    if "keeper" in role_text or "wk" in role_text:
        return "Wicketkeeper"
    if "bowl" in role_text:
        return "Bowler"
    if "bat" in role_text:
        return "Batsman"

    return "Unknown"


class CricbuzzClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def get_html(self, url: str) -> str:
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        return r.text

    def extract_team_links(self):
        # Keep the squads page request for debugging/visibility,
        # but use stable fallback team-pages for actual data import.
        html = self.get_html(SQUADS_URL)
        soup = BeautifulSoup(html, "html.parser")

        print("\n[DEBUG] Checking squads page...")
        heading = soup.get_text(" ", strip=True)
        print(f"[DEBUG] Squads page loaded: {'Indian Premier League 2026' in heading}")

        print("[DEBUG] Using fallback team player pages:")
        for team_name, url in TEAM_PAGE_FALLBACK.items():
            print(f"[DEBUG] {team_name} -> {url}")

        return TEAM_PAGE_FALLBACK.copy()

    def parse_team_squad(self, team_name: str, team_url: str):
        html = self.get_html(team_url)
        soup = BeautifulSoup(html, "html.parser")

        print(f"\n[DEBUG] Parsing team page: {team_name}")
        print(f"[DEBUG] URL: {team_url}")

        players = []
        seen = set()

        # role headings on team page like:
        # BATSMEN / ALL ROUNDER / WICKET KEEPER / BOWLER
        current_role_heading = None
        role_headings = {
            "BATSMEN": "Batsman",
            "BATTERS": "Batsman",
            "ALL ROUNDER": "All-rounder",
            "ALL ROUNDERS": "All-rounder",
            "ALL-ROUNDER": "All-rounder",
            "WICKET KEEPER": "Wicketkeeper",
            "WICKET KEEPERS": "Wicketkeeper",
            "BOWLER": "Bowler",
            "BOWLERS": "Bowler",
        }

        # First pass: use structured text hints from page
        body_text_lines = [x.strip() for x in soup.get_text("\n", strip=True).splitlines() if x.strip()]

        # Build a quick map from player name -> role using nearby headings in text
        text_role_map = {}
        active_role = None
        for line in body_text_lines:
            upper_line = line.upper()
            if upper_line in role_headings:
                active_role = role_headings[upper_line]
                continue

            # skip obviously generic lines
            if line.lower() in {"home", "schedule", "results", "news", "videos", "photos", "stats", "players"}:
                continue

            # only assign role to plausible player-name lines
            if active_role and re.match(r"^[A-Za-z][A-Za-z\s\.\-']+$", line) and len(line.split()) <= 5:
                text_role_map[normalize_name(line)] = active_role

        # Second pass: extract actual player profile links
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if "/profiles/" not in href:
                continue

            m = re.search(r"/profiles/(\d+)/", href)
            if not m:
                continue

            profile_id = m.group(1)
            profile_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            player_name = a.get_text(" ", strip=True)

            if not player_name:
                continue

            norm_name = normalize_name(player_name)
            key = (profile_id, norm_name)
            if key in seen:
                continue
            seen.add(key)

            role_from_squad = text_role_map.get(norm_name)

            # fallback from parent text if available
            if not role_from_squad:
                parent_text = a.parent.get_text(" ", strip=True) if a.parent else ""
                cleaned_parent = parent_text.replace(player_name, "").strip(" -:|")
                role_from_squad = map_role(cleaned_parent)
                if role_from_squad == "Unknown":
                    role_from_squad = None

            row = {
                "team_name": team_name,
                "team_short": TEAM_SHORT_MAP.get(team_name),
                "player_name": player_name,
                "normalized_name": norm_name,
                "cricbuzz_profile_id": profile_id,
                "cricbuzz_profile_url": profile_url,
                "role_from_squad": role_from_squad,
            }

            players.append(row)
            print(
                f"[DEBUG] Player found: {player_name} | "
                f"profile_id={profile_id} | role={role_from_squad}"
            )

        print(f"[DEBUG] Total players parsed for {team_name}: {len(players)}")
        return players

    def parse_profile(self, profile_url: str):
        html = self.get_html(profile_url)
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n", strip=True)

        country = None
        role = None
        debut_year = None

        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue

            if country is None and line in COUNTRY_LIST:
                country = line

            if role is None:
                raw_role_match = re.search(r"Role[:\s]+(.+)", line, flags=re.IGNORECASE)
                if raw_role_match:
                    role = raw_role_match.group(1).strip()
                else:
                    mapped = map_role(line)
                    if mapped != "Unknown":
                        role = mapped

            if debut_year is None:
                years = re.findall(r"\b(19\d{2}|20\d{2})\b", line)
                years = [int(y) for y in years if 2008 <= int(y) <= date.today().year]
                if years:
                    debut_year = min(years)

        return {
            "country": country,
            "role": role or "Unknown",
            "debut_year": debut_year,
        }


class Command(BaseCommand):
    help = "Import IPL 2026 player master data from Cricbuzz, remove duplicates, and update PlayerIPLTeam."

    def add_arguments(self, parser):
        parser.add_argument("--sleep", type=float, default=0.5, help="Sleep seconds between profile requests")
        parser.add_argument("--team", type=str, help="Optional team name filter")
        parser.add_argument("--skip-profiles", action="store_true", help="Skip profile enrichment")

    def handle(self, *args, **options):
        sleep_time = options["sleep"]
        only_team = options.get("team")
        skip_profiles = options.get("skip_profiles", False)

        self.stdout.write(self.style.SUCCESS("Starting IPL 2026 player import..."))

        client = CricbuzzClient()
        team_links = client.extract_team_links()

        self.stdout.write(f"[DEBUG] Total team links fetched: {len(team_links)}")

        if only_team:
            team_links = {k: v for k, v in team_links.items() if k.lower() == only_team.lower()}
            self.stdout.write(f"[DEBUG] Team filter applied. Remaining team links: {len(team_links)}")

        if not team_links:
            self.stdout.write(self.style.ERROR("No team links found."))
            return

        all_players = []
        for team_name, team_url in team_links.items():
            self.stdout.write(f"Fetching squad/team page: {team_name}")
            try:
                team_players = client.parse_team_squad(team_name, team_url)
                self.stdout.write(f"[DEBUG] {team_name} players fetched: {len(team_players)}")
                all_players.extend(team_players)
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"Failed for {team_name}: {e}"))

        self.stdout.write(f"[DEBUG] Raw player rows before dedupe: {len(all_players)}")

        deduped_players = {}
        for p in all_players:
            key = p["cricbuzz_profile_id"] or p["normalized_name"]
            if key not in deduped_players:
                deduped_players[key] = p

        players = list(deduped_players.values())
        self.stdout.write(f"Squad player records found after dedupe: {len(players)}")

        if not players:
            self.stdout.write(self.style.ERROR("No players parsed from team pages."))
            return

        if not skip_profiles:
            for idx, player in enumerate(players, start=1):
                try:
                    self.stdout.write(f"[{idx}/{len(players)}] Enriching {player['player_name']}")
                    meta = client.parse_profile(player["cricbuzz_profile_url"])
                    player["country"] = meta.get("country")
                    player["role"] = meta.get("role") or map_role(player.get("role_from_squad"))
                    player["debut_year"] = meta.get("debut_year")
                    time.sleep(sleep_time)
                except Exception as e:
                    self.stdout.write(self.style.WARNING(
                        f"Profile fetch failed for {player['player_name']}: {e}"
                    ))
                    player["country"] = None
                    player["role"] = map_role(player.get("role_from_squad"))
                    player["debut_year"] = None
        else:
            for player in players:
                player["country"] = None
                player["role"] = map_role(player.get("role_from_squad"))
                player["debut_year"] = None

        self.cleanup_duplicates()

        created_count = 0
        updated_count = 0
        team_rows = 0

        with transaction.atomic():
            PlayerIPLTeam.objects.filter(season=2026).update(is_current=False)

            for row in players:
                player = self.upsert_player(row)
                if getattr(player, "_was_created", False):
                    created_count += 1
                else:
                    updated_count += 1

                PlayerIPLTeam.objects.update_or_create(
                    player=player,
                    team_name=row["team_name"],
                    season=2026,
                    defaults={
                        "team_short": row.get("team_short"),
                        "is_current": True,
                    }
                )
                team_rows += 1

        self.cleanup_duplicates()

        self.stdout.write(self.style.SUCCESS(
            f"Done. created={created_count}, updated={updated_count}, team_rows={team_rows}"
        ))

    @transaction.atomic
    def cleanup_duplicates(self):
        self.stdout.write("Cleaning duplicate players...")

        groups = defaultdict(list)
        for p in Player.objects.all().order_by("created_at", "player_name"):
            key = p.cricbuzz_profile_id or p.normalized_name or normalize_name(p.player_name)
            groups[key].append(p)

        removed = 0

        for _, players in groups.items():
            if len(players) <= 1:
                continue

            keeper = self.select_best_player(players)
            duplicates = [p for p in players if p.pk != keeper.pk]

            for dup in duplicates:
                PlayerIPLTeam.objects.filter(player=dup).update(player=keeper)
                MatchPlayer.objects.filter(player=dup).update(player=keeper)
                PlayerMatchBatting.objects.filter(player=dup).update(player=keeper)
                PlayerMatchBowling.objects.filter(player=dup).update(player=keeper)
                PlayerSituationStats.objects.filter(player=dup).update(player=keeper)
                Delivery.objects.filter(batter=dup).update(batter=keeper)
                Delivery.objects.filter(bowler=dup).update(bowler=keeper)
                Delivery.objects.filter(non_striker=dup).update(non_striker=keeper)
                Delivery.objects.filter(player_out=dup).update(player_out=keeper)
                dup.delete()
                removed += 1

        self.stdout.write(self.style.SUCCESS(f"Duplicate players removed: {removed}"))

    def select_best_player(self, players):
        def score(p):
            return (
                1 if p.cricbuzz_profile_id else 0,
                1 if p.country else 0,
                1 if p.role and p.role != "Unknown" else 0,
                1 if p.debut_year else 0,
                1 if p.ipl_debut else 0,
                1 if p.last_season else 0,
            )
        return sorted(players, key=score, reverse=True)[0]

    def upsert_player(self, row):
        normalized_name = row["normalized_name"]
        role = row.get("role") or "Unknown"

        player = None

        if row.get("cricbuzz_profile_id"):
            player = Player.objects.filter(cricbuzz_profile_id=row["cricbuzz_profile_id"]).first()

        if not player:
            player = Player.objects.filter(normalized_name=normalized_name).first()

        defaults = {
            "player_name": row["player_name"],
            "normalized_name": normalized_name,
            "country": row.get("country"),
            "role": role,
            "debut_year": row.get("debut_year"),
            "last_season": 2026,
            "cricbuzz_profile_id": row.get("cricbuzz_profile_id"),
            "cricbuzz_profile_url": row.get("cricbuzz_profile_url"),
        }

        if row.get("debut_year"):
            defaults["ipl_debut"] = date(int(row["debut_year"]), 1, 1)

        if player:
            changed = False
            for key, value in defaults.items():
                old_value = getattr(player, key, None)
                if value is not None and old_value != value:
                    setattr(player, key, value)
                    changed = True
            if changed:
                player.save()
            player._was_created = False
            return player

        player_id = self.generate_player_id(normalized_name)
        player = Player.objects.create(player_id=player_id, **defaults)
        player._was_created = True
        return player

    def generate_player_id(self, normalized_name):
        existing = Player.objects.filter(normalized_name=normalized_name).first()
        if existing:
            return existing.player_id

        base = normalized_name.replace(" ", "")[:12] or "player"
        suffix = uuid.uuid4().hex[:8]
        return f"{base}_{suffix}"