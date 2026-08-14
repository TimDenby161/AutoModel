#!/usr/bin/env python3
"""
Export FotMob player profiles and combined season statistics to CSV.

Full-history import (run once):
    1. Put player IDs in player_ids.txt, one ID per line.
    2. Place this script in the same folder.
    3. Run: python fotmob_export.py --mode full

Fast ongoing refresh:
    python fotmob_export.py --mode refresh

You can also supply a TXT or CSV input:
    python fotmob_export.py my_player_ids.csv

Outputs are written beside this script:
    fotmob_players_full.csv       (full mode)
    fotmob_players_current.csv    (refresh mode)
    fotmob_errors.csv

Both modes resume safely: IDs already present in that mode's output CSV are
skipped on later runs.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_PLAYER_URL = "https://www.fotmob.com/api/data/playerData"
BASE_STATS_URL = "https://www.fotmob.com/api/data/playerStats"
MATCH_DETAILS_URL = "https://www.fotmob.com/api/data/matchDetails"
YEARS = list(range(2027, 1999, -1))
CURRENT_SEASON_YEAR = 2027  # season ending year that is actually in progress right now
LAST_FIVE_MATCH_WORKERS = 5  # concurrent matchDetails lookups when resolving "last 5" starts
FRIENDLY_LEAGUE_ID = 489  # FotMob's "Club Friendlies" competition, shared across every club

# FotMob's primaryTeam is wrong/stale for these IDs - override until it catches up.
CURRENT_CLUB_OVERRIDES: dict[str, tuple[str, int]] = {
    "1076698": ("Chesterfield", 9786),
    "1423636": ("Chesterfield", 9786),
}

PROFILE_HEADERS = [
    "Player ID",
    "Name",
    "Primary Position",
    "Other Positions",
    "DOB",
    "Age",
    "Nationality",
    "Nationality Code",
    "Current Club",
    "Current Club ID",
    "Currently On Loan",
    "Height",
    "Preferred Foot",
    "Shirt Number",
    "Contract End",
    "Market Value",
    "Market Value Display",
]

SEASON_FIELDS = [
    "Club",
    "Appearances",
    "Goals",
    "Assists",
    "Rating",
    "Minutes",
    "Starts",
    "xG",
    "Non-Penalty xG",
    "Shots",
    "xA",
    "Accurate Passes",
    "Chances Created",
    "Big Chances Created",
    "Successful Dribbles",
    "Duels Won",
    "Aerials Won",
    "Touches",
    "Defensive Actions",
    "Tackles",
    "Interceptions",
    "Recoveries",
    "Clearances",
    "Yellow Cards",
    "Red Cards",
]

EXTRA_HEADERS = [
    "Gender",
    "Captain",
    "Career Status",
    "Injury Status",
    "Injury Expected Return",
    "Injury Last Updated",
    "International Duty",
    "Trophies Won",
    "Peak Market Value",
    "Peak Market Value Date",
    "Traits vs Position Peers",
    "Recent Form Rating",
    "Last Match Date",
    "Last 5 Starts",
    "Last 5 Minutes",
]

HEADERS = (
    PROFILE_HEADERS
    + [f"{year} {field}" for year in YEARS for field in SEASON_FIELDS]
    + EXTRA_HEADERS
)

DETAIL_KEYS = {
    "minutes_played": "minutes",
    "player_started_matches": "starts",
    "expected_goals": "xG",
    "non_penalty_xg": "nonPenaltyXG",
    "shots": "shots",
    "expected_assists": "xA",
    "successful_passes": "accuratePasses",
    "chances_created": "chancesCreated",
    "big_chance_created_team_title": "bigChancesCreated",
    "dribbles_succeeded": "dribbles",
    "duel_won": "duelsWon",
    "aerials_won": "aerialsWon",
    "touches": "touches",
    "defensive_actions": "defensiveActions",
    "matchstats.headers.tackles": "tackles",
    "interceptions": "interceptions",
    "recoveries": "recoveries",
    "clearances": "clearances",
    "yellow_cards": "yellowCards",
    "red_cards": "redCards",
}

DETAIL_FIELD_ORDER = [
    "minutes",
    "starts",
    "xG",
    "nonPenaltyXG",
    "shots",
    "xA",
    "accuratePasses",
    "chancesCreated",
    "bigChancesCreated",
    "dribbles",
    "duelsWon",
    "aerialsWon",
    "touches",
    "defensiveActions",
    "tackles",
    "interceptions",
    "recoveries",
    "clearances",
    "yellowCards",
    "redCards",
]

PRINT_LOCK = threading.Lock()
RATE_LOCK = threading.Lock()
FILE_LOCK = threading.Lock()
LAST_REQUEST_AT = 0.0


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description=(
            "Export FotMob player profiles and combined statistics to a CSV "
            "beside this script."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("full", "refresh"),
        default="full",
        help=(
            "full = all detailed seasons; refresh = basic history plus detailed "
            "current season only (default: full)"
        ),
    )
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=script_dir / "player_ids.txt",
        help="TXT or CSV containing player IDs (default: player_ids.txt)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Output CSV (default: fotmob_players_full.csv in full mode or "
            "fotmob_players_current.csv in refresh mode)"
        ),
    )
    parser.add_argument(
        "--errors",
        type=Path,
        default=script_dir / "fotmob_errors.csv",
        help="Error CSV (default: fotmob_errors.csv beside the script)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Concurrent players (defaults: 4 full, 12 refresh)",
    )
    parser.add_argument(
        "--detail-workers",
        type=int,
        default=6,
        help=(
            "Parallel competition requests within each player in full mode "
            "(default: 6)"
        ),
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=None,
        help="Global delay between HTTP request starts (defaults: 0.04s full, 0.08s refresh)",
    )
    parser.add_argument(
        "--current-year",
        type=int,
        default=CURRENT_SEASON_YEAR,
        help=f"Season ending year used by refresh mode (default: {CURRENT_SEASON_YEAR})",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        help="Attempts per HTTP request (default: 4)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing output instead of resuming",
    )
    return parser.parse_args()


def safe_print(message: str) -> None:
    with PRINT_LOCK:
        print(message, flush=True)


def rate_limit(minimum_delay: float) -> None:
    global LAST_REQUEST_AT
    with RATE_LOCK:
        now = time.monotonic()
        wait_for = minimum_delay - (now - LAST_REQUEST_AT)
        if wait_for > 0:
            time.sleep(wait_for)
        LAST_REQUEST_AT = time.monotonic()


def fetch_json(
    url: str,
    *,
    retries: int,
    request_delay: float,
    timeout: float = 40.0,
) -> dict[str, Any]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; FotMobCSVExporter/1.0)",
        "Referer": "https://www.fotmob.com/",
    }
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        rate_limit(request_delay)
        request = Request(url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=timeout) as response:
                status = getattr(response, "status", 200)
                body = response.read()
            if status != 200:
                raise RuntimeError(f"HTTP {status}")
            return json.loads(body.decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep((2 ** (attempt - 1)) + random.uniform(0.0, 0.5))

    raise RuntimeError(f"request failed after {retries} attempts: {last_error}")


def load_player_ids(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(
            f"Input file not found: {path}\n"
            "Create player_ids.txt beside the script, with one ID per line."
        )

    ids: list[str] = []
    seen: set[str] = set()

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        if path.suffix.lower() == ".csv":
            rows = csv.reader(handle)
            values = (cell for row in rows for cell in row)
        else:
            values = (
                token
                for line in handle
                for token in re.split(r"[\s,;\t]+", line.strip())
            )

        for value in values:
            candidate = str(value).strip()
            if candidate.isdigit() and candidate not in seen:
                ids.append(candidate)
                seen.add(candidate)

    if not ids:
        raise ValueError(f"No numeric player IDs found in {path}")
    return ids


def load_completed_ids(output_path: Path) -> set[str]:
    if not output_path.exists() or output_path.stat().st_size == 0:
        return set()

    completed: set[str] = set()
    with output_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "Player ID" not in reader.fieldnames:
            raise ValueError(
                f"{output_path} does not have the expected 'Player ID' column. "
                "Rename it or run with --overwrite."
            )
        if list(reader.fieldnames) != HEADERS:
            raise ValueError(
                f"{output_path} has a different column layout than the current "
                "YEARS/SEASON_FIELDS configuration (it was likely written before "
                "a schema change). Move it aside or run with --overwrite to "
                "regenerate it with the current columns."
            )
        for row in reader:
            player_id = str(row.get("Player ID", "")).strip()
            if player_id.isdigit():
                completed.add(player_id)
    return completed


def find_player_info(player: dict[str, Any], translation_key: str) -> dict[str, Any] | None:
    for item in player.get("playerInformation") or []:
        if item.get("translationKey") == translation_key:
            return item
    return None


def get_player_info(player: dict[str, Any], translation_key: str) -> Any:
    item = find_player_info(player, translation_key)
    if not item:
        return ""
    value = item.get("value") or {}
    if value.get("numberValue") is not None:
        return value["numberValue"]
    fallback = value.get("fallback")
    if isinstance(fallback, (str, int, float)):
        return fallback
    return value.get("key") or ""


def get_nationality_code(player: dict[str, Any]) -> str:
    country = find_player_info(player, "country_sentencecase")
    if not country:
        return ""
    icon = country.get("icon") or {}
    return str(country.get("countryCode") or icon.get("id") or "")


def parse_iso_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, dict):
        value = value.get("utcTime") or value.get("dateValue")
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None


def date_text(value: date | None) -> str:
    return value.isoformat() if value else ""


def calculate_age(dob: date | None) -> int | str:
    if dob is None:
        return ""
    today = datetime.now(timezone.utc).date()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))


def get_contract_end(player: dict[str, Any]) -> date | None:
    contract = parse_iso_date(player.get("contractEnd"))
    if contract:
        return contract
    item = find_player_info(player, "contract_end")
    if not item:
        return None
    value = item.get("value") or {}
    return parse_iso_date(value.get("dateValue") or value.get("fallback"))


def primary_position(player: dict[str, Any]) -> str:
    description = player.get("positionDescription") or {}
    primary = description.get("primaryPosition") or {}
    return str(primary.get("label") or "")


def other_positions(player: dict[str, Any]) -> str:
    description = player.get("positionDescription") or {}
    positions = description.get("nonPrimaryPositions") or []
    return ", ".join(str(item.get("label")) for item in positions if item.get("label"))


def injury_fields(player: dict[str, Any]) -> tuple[str, str, str]:
    injury = player.get("injuryInformation")
    if not injury:
        return "", "", ""
    status = str(injury.get("name") or "")
    expected = injury.get("expectedReturn") or {}
    return_text = str(
        expected.get("expectedReturnDateParam")
        or expected.get("expectedReturnFallback")
        or ""
    )
    updated = parse_iso_date(injury.get("lastUpdated"))
    return status, return_text, date_text(updated)


def summarize_international_duty(player: dict[str, Any]) -> str:
    duty = player.get("internationalDuty")
    if not duty:
        return ""
    if isinstance(duty, str):
        return duty
    if not isinstance(duty, dict):
        return str(duty)
    parts = [
        str(duty[key])
        for key in ("teamName", "title", "name", "confederation", "stage", "reason")
        if duty.get(key)
    ]
    return " - ".join(parts) if parts else json.dumps(duty)


def summarize_trophies(player: dict[str, Any]) -> str:
    trophies = player.get("trophies") or {}
    entries = []
    for team in trophies.get("playerTrophies") or []:
        team_name = team.get("teamName") or ""
        for tournament in team.get("tournaments") or []:
            seasons_won = tournament.get("seasonsWon") or []
            if not seasons_won:
                continue
            league_name = tournament.get("leagueName") or ""
            entries.append(f"{team_name} - {league_name} ({', '.join(seasons_won)})")
    return "; ".join(entries)


def peak_market_value(player: dict[str, Any]) -> tuple[int | str, str]:
    values = (player.get("marketValues") or {}).get("values") or []
    numeric = [(entry.get("value"), entry.get("date")) for entry in values if entry.get("value")]
    if not numeric:
        return "", ""
    peak_value, peak_date = max(numeric, key=lambda pair: pair[0])
    return peak_value, date_text(parse_iso_date(peak_date))


def summarize_traits(player: dict[str, Any]) -> str:
    items = (player.get("traits") or {}).get("items") or []
    parts = []
    for item in items:
        title = item.get("title")
        value = item.get("value")
        if title is None or value is None:
            continue
        try:
            parts.append(f"{title}: {round(float(value) * 100)}%")
        except (TypeError, ValueError):
            continue
    return ", ".join(parts)


def recent_form(player: dict[str, Any]) -> tuple[float | str, str]:
    matches = player.get("recentMatches") or []
    if not matches:
        return "", ""
    last_match_date = date_text(parse_iso_date((matches[0].get("matchDate") or {})))

    ratings = []
    for match in matches[:5]:
        if not match.get("playedInMatch"):
            continue
        rating = (match.get("ratingProps") or {}).get("rating")
        try:
            rating = float(rating)
        except (TypeError, ValueError):
            continue
        if rating > 0:
            ratings.append(rating)

    average = round(sum(ratings) / len(ratings), 2) if ratings else ""
    return average, last_match_date


def ending_year(season_name: Any) -> int | None:
    text = str(season_name or "")
    european = re.fullmatch(r"(\d{4})/(\d{4})", text)
    if european:
        return int(european.group(2))
    if re.fullmatch(r"\d{4}", text):
        return int(text)
    return None


def choose_season(entries: list[dict[str, Any]], year: int) -> dict[str, Any] | None:
    # Prefer a European-style season ending in the requested year.
    for entry in entries:
        name = str(entry.get("seasonName") or "")
        match = re.fullmatch(r"(\d{4})/(\d{4})", name)
        if match and int(match.group(2)) == year:
            return entry
    # Fall back to a calendar-year competition.
    for entry in entries:
        if str(entry.get("seasonName") or "") == str(year):
            return entry
    return None


def season_league_ids(player: dict[str, Any], year: int) -> set[int]:
    """League IDs the player actually featured in during the season ending `year`,
    read from careerHistory (which FotMob already excludes friendlies from). Used to
    decide which entries in recentMatches belong to that season, since recentMatches
    itself carries no season label."""
    career = player.get("careerHistory") or {}
    items = career.get("careerItems") or {}
    senior = items.get("senior") or {}
    ids: set[int] = set()
    for entry in senior.get("seasonEntries") or []:
        if ending_year(entry.get("seasonName")) != year:
            continue
        for tournament in entry.get("tournamentStats") or []:
            league_id = tournament.get("leagueId")
            if league_id is None:
                continue
            try:
                ids.add(int(league_id))
            except (TypeError, ValueError):
                continue
    return ids


def match_was_started(
    player_id: str,
    match_id: Any,
    *,
    retries: int,
    request_delay: float,
) -> bool:
    """True if player_id appears in either side's starting lineup for match_id.
    Mirrors the starters/subs split getMatches.py already relies on — recentMatches'
    own onBench/lineupPositionId fields are NOT reliable for this (onBench is False
    for late substitutes who came off the bench, not just for starters, and
    lineupPositionId is frequently missing even for clear 90-minute appearances)."""
    if not match_id:
        return False
    query = urlencode({"matchId": match_id})
    data = fetch_json(
        f"{MATCH_DETAILS_URL}?{query}",
        retries=retries,
        request_delay=request_delay,
    )
    lineup = (data.get("content") or {}).get("lineup") or {}
    for side in ("homeTeam", "awayTeam"):
        starters = (lineup.get(side) or {}).get("starters") or []
        if any(str(entry.get("id")) == str(player_id) for entry in starters):
            return True
    return False


def last_five_club_games(
    player: dict[str, Any],
    player_id: str,
    current_club_id: Any,
    *,
    season_year: int,
    retries: int,
    request_delay: float,
) -> tuple[int | str, int | str]:
    """Starts and total minutes across the current club's last 5 games in the given
    season, per recentMatches, including pre-season/club friendlies (FotMob excludes
    those from careerHistory's season stats, so they'd otherwise never match
    season_league_ids - they're still real minutes for the current club, so they're
    added back in explicitly). NOTE: recentMatches only lists matches the player was
    at least part of the squad for - a match the player was entirely left out of
    (e.g. long-term injury, not yet registered) may not appear at all, so on rare
    occasions this can effectively be "last 5 available" rather than a strict last 5
    fixtures. Returns ("", "") when there's no current club or no season data at all
    to check against, and (0, 0) when the club simply hasn't played any qualifying
    games yet this season."""
    if not current_club_id:
        return "", ""

    matches = player.get("recentMatches") or []
    if not matches:
        return "", ""

    league_ids = season_league_ids(player, season_year)

    club_id_text = str(current_club_id)
    relevant = [
        match
        for match in matches
        if str(match.get("teamId") or "") == club_id_text
        and (
            match.get("leagueId") in league_ids
            or match.get("leagueId") == FRIENDLY_LEAGUE_ID
        )
    ][:5]

    if not relevant:
        return 0, 0

    total_minutes = 0
    for match in relevant:
        try:
            total_minutes += int(float(match.get("minutesPlayed") or 0))
        except (TypeError, ValueError):
            continue

    starts = 0
    with ThreadPoolExecutor(max_workers=min(LAST_FIVE_MATCH_WORKERS, len(relevant))) as executor:
        futures = [
            executor.submit(
                match_was_started,
                player_id,
                match.get("id"),
                retries=retries,
                request_delay=request_delay,
            )
            for match in relevant
        ]
        for future in as_completed(futures):
            try:
                if future.result():
                    starts += 1
            except Exception:
                continue  # a flaky single match-detail lookup shouldn't sink the whole row

    return starts, total_minutes


def number_or_blank(value: Any) -> int | float | str:
    if value is None or value == "":
        return ""
    try:
        number = float(value)
        return int(number) if number.is_integer() else number
    except (TypeError, ValueError):
        return str(value)


def basic_season_data(player: dict[str, Any], year: int) -> dict[str, Any]:
    career = player.get("careerHistory") or {}
    items = career.get("careerItems") or {}
    senior = items.get("senior") or {}
    season = choose_season(senior.get("seasonEntries") or [], year)
    if not season:
        return {
            "club": "",
            "appearances": "",
            "goals": "",
            "assists": "",
            "rating": "",
        }
    rating = season.get("rating") or {}
    return {
        "club": season.get("team") or "",
        "appearances": number_or_blank(season.get("appearances")),
        "goals": number_or_blank(season.get("goals")),
        "assists": number_or_blank(season.get("assists")),
        "rating": number_or_blank(rating.get("rating")),
    }


def empty_details(blank: bool = False) -> dict[str, Any]:
    value: int | str = "" if blank else 0
    return {field: value for field in DETAIL_FIELD_ORDER}


def extract_competition_details(data: dict[str, Any]) -> dict[str, Any]:
    values: dict[str, float] = {}

    for item in (data.get("topStatCard") or {}).get("items") or []:
        key = item.get("localizedTitleId")
        if key:
            try:
                values[key] = float(item.get("statValue") or 0)
            except (TypeError, ValueError):
                values[key] = 0.0

    for group in (data.get("statsSection") or {}).get("items") or []:
        for item in group.get("items") or []:
            key = item.get("localizedTitleId")
            if key:
                try:
                    values[key] = float(item.get("statValue") or 0)
                except (TypeError, ValueError):
                    values[key] = 0.0

    result = empty_details()
    for fotmob_key, output_key in DETAIL_KEYS.items():
        result[output_key] = values.get(fotmob_key, 0.0)
    return result


def combined_detailed_stats(
    player: dict[str, Any],
    player_id: str,
    year: int,
    *,
    retries: int,
    request_delay: float,
) -> dict[str, Any]:
    stat_season = choose_season(player.get("statSeasons") or [], year)
    if not stat_season:
        return empty_details(blank=True)

    combined = empty_details()
    fetched_any = False

    for tournament in stat_season.get("tournaments") or []:
        entry_id = tournament.get("entryId")
        if not entry_id or tournament.get("hasDeepStats") is False:
            continue

        query = urlencode(
            {
                "playerId": player_id,
                "seasonId": entry_id,
                "isFirstSeason": "false",
            }
        )
        data = fetch_json(
            f"{BASE_STATS_URL}?{query}",
            retries=retries,
            request_delay=request_delay,
        )
        details = extract_competition_details(data)
        fetched_any = True
        for field in DETAIL_FIELD_ORDER:
            combined[field] += details[field]

    if not fetched_any:
        return empty_details(blank=True)

    for field in ("xG", "nonPenaltyXG", "xA"):
        combined[field] = round(float(combined[field]), 2)
    for field in DETAIL_FIELD_ORDER:
        value = combined[field]
        if isinstance(value, float) and value.is_integer():
            combined[field] = int(value)
    return combined


def all_detailed_stats(
    player: dict[str, Any],
    player_id: str,
    years: list[int],
    *,
    retries: int,
    request_delay: float,
    detail_workers: int,
) -> dict[int, dict[str, Any]]:
    """Fetch every independent season/competition request concurrently."""
    results = {year: empty_details(blank=True) for year in years}
    jobs: list[tuple[int, str]] = []

    for year in years:
        stat_season = choose_season(player.get("statSeasons") or [], year)
        if not stat_season:
            continue
        for tournament in stat_season.get("tournaments") or []:
            entry_id = tournament.get("entryId")
            if entry_id and tournament.get("hasDeepStats") is not False:
                jobs.append((year, str(entry_id)))

    if not jobs:
        return results

    def fetch_competition(entry_id: str) -> dict[str, Any]:
        query = urlencode(
            {
                "playerId": player_id,
                "seasonId": entry_id,
                "isFirstSeason": "false",
            }
        )
        data = fetch_json(
            f"{BASE_STATS_URL}?{query}",
            retries=retries,
            request_delay=request_delay,
        )
        return extract_competition_details(data)

    totals = {year: empty_details() for year in years}
    fetched_years: set[int] = set()
    with ThreadPoolExecutor(max_workers=min(detail_workers, len(jobs))) as executor:
        futures = {
            executor.submit(fetch_competition, entry_id): year
            for year, entry_id in jobs
        }
        for future in as_completed(futures):
            year = futures[future]
            details = future.result()
            fetched_years.add(year)
            for field in DETAIL_FIELD_ORDER:
                totals[year][field] += details[field]

    for year in fetched_years:
        for field in ("xG", "nonPenaltyXG", "xA"):
            totals[year][field] = round(float(totals[year][field]), 2)
        for field, value in totals[year].items():
            if isinstance(value, float) and value.is_integer():
                totals[year][field] = int(value)
        results[year] = totals[year]
    return results


def build_player_row(
    player_id: str,
    *,
    mode: str,
    current_year: int,
    detail_workers: int,
    retries: int,
    request_delay: float,
) -> list[Any]:
    player_query = urlencode({"id": player_id})
    player = fetch_json(
        f"{BASE_PLAYER_URL}?{player_query}",
        retries=retries,
        request_delay=request_delay,
    )

    dob = parse_iso_date(player.get("birthDate"))
    primary_team = player.get("primaryTeam") or {}
    club_override = CURRENT_CLUB_OVERRIDES.get(player_id)
    market_item = find_player_info(player, "transfer_value") or {}
    market_value_data = market_item.get("value") or {}
    current_club_id = club_override[1] if club_override else primary_team.get("teamId") or ""

    row: list[Any] = [
        player.get("id") or player_id,
        player.get("name") or "",
        primary_position(player),
        other_positions(player),
        date_text(dob),
        calculate_age(dob),
        get_player_info(player, "country_sentencecase"),
        get_nationality_code(player),
        club_override[0] if club_override else primary_team.get("teamName") or "",
        current_club_id,
        bool(primary_team.get("onLoan", False)),
        get_player_info(player, "height_sentencecase"),
        get_player_info(player, "preferred_foot"),
        get_player_info(player, "shirt"),
        date_text(get_contract_end(player)),
        market_value_data.get("numberValue", ""),
        market_value_data.get("fallback", ""),
    ]

    detailed_years = YEARS if mode == "full" else [current_year]
    detailed_by_year = all_detailed_stats(
        player,
        player_id,
        detailed_years,
        retries=retries,
        request_delay=request_delay,
        detail_workers=detail_workers if mode == "full" else min(detail_workers, 3),
    )

    for year in YEARS:
        basic = basic_season_data(player, year)
        details = detailed_by_year.get(year, empty_details(blank=True))
        row.extend(
            [
                basic["club"],
                basic["appearances"],
                basic["goals"],
                basic["assists"],
                basic["rating"],
            ]
        )
        row.extend(details[field] for field in DETAIL_FIELD_ORDER)

    injury_status, injury_return, injury_updated = injury_fields(player)
    peak_value, peak_value_date = peak_market_value(player)
    form_rating, last_match_date = recent_form(player)
    last_five_starts, last_five_minutes = last_five_club_games(
        player,
        player_id,
        current_club_id,
        season_year=current_year,
        retries=retries,
        request_delay=request_delay,
    )

    row.extend(
        [
            player.get("gender") or "",
            bool(player.get("isCaptain", False)),
            player.get("status") or "",
            injury_status,
            injury_return,
            injury_updated,
            summarize_international_duty(player),
            summarize_trophies(player),
            peak_value,
            peak_value_date,
            summarize_traits(player),
            form_rating,
            last_match_date,
            last_five_starts,
            last_five_minutes,
        ]
    )

    return row


def append_csv_row(path: Path, headers: list[str], row: list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with FILE_LOCK:
        needs_header = not path.exists() or path.stat().st_size == 0
        with path.open("a", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            if needs_header:
                writer.writerow(headers)
            writer.writerow(row)
            handle.flush()


def append_error(path: Path, player_id: str, error: Exception) -> None:
    append_csv_row(
        path,
        ["Player ID", "Error", "Timestamp UTC"],
        [
            player_id,
            str(error),
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
        ],
    )


def prune_errors(path: Path, ids_to_remove: set[str]) -> None:
    """Drop existing error rows for IDs about to be retried this run, so a
    persistently failing ID doesn't accumulate a duplicate row per run."""
    if not ids_to_remove or not path.exists() or path.stat().st_size == 0:
        return

    with FILE_LOCK:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames
            if not fieldnames:
                return
            kept_rows = [
                row
                for row in reader
                if str(row.get("Player ID", "")).strip() not in ids_to_remove
            ]

        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(kept_rows)


def main() -> int:
    args = parse_args()
    args.input = args.input.resolve()
    script_dir = Path(__file__).resolve().parent
    if args.output is None:
        filename = (
            "fotmob_players_full.csv"
            if args.mode == "full"
            else "fotmob_players_current.csv"
        )
        args.output = script_dir / filename
    args.output = args.output.resolve()
    args.errors = args.errors.resolve()

    if args.workers is None:
        args.workers = 4 if args.mode == "full" else 12
    if args.request_delay is None:
        args.request_delay = 0.04 if args.mode == "full" else 0.08

    if args.workers < 1:
        raise ValueError("--workers must be at least 1")
    if args.detail_workers < 1:
        raise ValueError("--detail-workers must be at least 1")
    if args.request_delay < 0:
        raise ValueError("--request-delay cannot be negative")
    if args.retries < 1:
        raise ValueError("--retries must be at least 1")
    if args.current_year not in YEARS:
        raise ValueError(
            f"--current-year must be one of: {', '.join(map(str, YEARS))}"
        )

    player_ids = load_player_ids(args.input)

    if args.overwrite:
        if args.output.exists():
            args.output.unlink()
        if args.errors.exists():
            args.errors.unlink()
        completed: set[str] = set()
    else:
        completed = load_completed_ids(args.output)

    pending = [player_id for player_id in player_ids if player_id not in completed]

    if not args.overwrite:
        prune_errors(args.errors, set(pending))

    safe_print(f"Loaded {len(player_ids):,} unique player IDs.")
    safe_print(f"Mode: {args.mode}")
    if args.mode == "refresh":
        safe_print(f"Detailed season: {args.current_year}")
    safe_print(f"Already complete: {len(completed):,}")
    safe_print(f"Remaining: {len(pending):,}")
    safe_print(f"Output: {args.output}")

    if not pending:
        safe_print("Nothing to do.")
        return 0

    completed_now = 0
    failed_now = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_id = {
            executor.submit(
                build_player_row,
                player_id,
                mode=args.mode,
                current_year=args.current_year,
                detail_workers=args.detail_workers,
                retries=args.retries,
                request_delay=args.request_delay,
            ): player_id
            for player_id in pending
        }

        for future in as_completed(future_to_id):
            player_id = future_to_id[future]
            try:
                row = future.result()
                append_csv_row(args.output, HEADERS, row)
                completed_now += 1
                safe_print(
                    f"[OK {completed_now + failed_now}/{len(pending)}] "
                    f"{player_id} - {row[1]}"
                )
            except KeyboardInterrupt:
                safe_print("Stopped. Run the script again to resume.")
                return 130
            except Exception as exc:
                failed_now += 1
                append_error(args.errors, player_id, exc)
                safe_print(
                    f"[ERROR {completed_now + failed_now}/{len(pending)}] "
                    f"{player_id}: {exc}"
                )

    safe_print(
        f"Finished. Added {completed_now:,} players; "
        f"{failed_now:,} failed."
    )
    if failed_now:
        safe_print(f"Failures were written to {args.errors}")
    return 0 if failed_now == 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nStopped. Run the script again to resume.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
