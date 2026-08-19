#!/usr/bin/env python3
"""
Export AutoModel competition data to a CSV beside this script.

Setup:
    1. Create competition_ids.txt beside this script, one ID per line.
    2. Run: python getCompetitions.py

Outputs:
    automodel_competitions.csv
    automodel_competition_errors.csv

Re-running resumes automatically by skipping IDs already in the output.
Use --overwrite to rebuild the file. Use --season "2025/2026" to request a
specific season for every competition.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://www.fotmob.com/api/data/leagues"

HEADERS = [
    "Competition ID",
    "Competition Name",
    "Short Name",
    "Type",
    "Country Code",
    "Gender",
    "Selected Season",
    "Latest Season",
    "Tournament ID",
    "Data Provider",
    "League Colour",
    "Can Sync Calendar",
    "FotMob URL",
    "Logo URL",
    "Available Season Count",
    "Available Seasons",
    "Season Tournament IDs",
    "Previous Winner Club ID",
    "Previous Winner",
    "Previous Winner Season",
    "Previous Runner-Up Club ID",
    "Previous Runner-Up",
    "Winner History",
    "Team Count",
    "Team IDs",
    "Teams",
    "Round Count",
    "Active Round",
    "Group Count",
    "Groups",
    "Table Section Count",
    "Table Sections",
    "Standings",
    "Qualification Places",
    "Relegation Places",
    "Fixture Count",
    "Completed Fixtures",
    "Scheduled Fixtures",
    "Cancelled Fixtures",
    "First Fixture UTC",
    "Last Fixture UTC",
    "Next Match ID",
    "Next Match UTC",
    "Next Home Club ID",
    "Next Home Club",
    "Next Away Club ID",
    "Next Away Club",
    "Has Ongoing Match",
    "Has Team Of The Week",
    "Player Stats Available",
    "Team Stats Available",
    "Tabs",
    "Retrieved UTC",
]

PRINT_LOCK = threading.Lock()
RATE_LOCK = threading.Lock()
CSV_LOCK = threading.Lock()
LAST_REQUEST_AT = 0.0


def parse_args() -> argparse.Namespace:
    folder = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Export AutoModel competition data to a resumable CSV."
    )
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=folder / "competition_ids.txt",
        help="TXT or CSV containing competition IDs",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=folder / "automodel_competitions.csv",
    )
    parser.add_argument(
        "--errors",
        type=Path,
        default=folder / "automodel_competition_errors.csv",
    )
    parser.add_argument(
        "--season",
        default="",
        help='Optional season, for example "2025/2026"',
    )
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--request-delay", type=float, default=0.08)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def safe_print(message: str) -> None:
    with PRINT_LOCK:
        print(message, flush=True)


def rate_limit(delay: float) -> None:
    global LAST_REQUEST_AT
    with RATE_LOCK:
        now = time.monotonic()
        wait_for = delay - (now - LAST_REQUEST_AT)
        if wait_for > 0:
            time.sleep(wait_for)
        LAST_REQUEST_AT = time.monotonic()


def fetch_competition(
    competition_id: str,
    *,
    season: str,
    retries: int,
    request_delay: float,
) -> dict[str, Any]:
    params = {"id": competition_id, "ccode3": "GBR_MA"}
    if season:
        params["season"] = season
    url = f"{BASE_URL}?{urlencode(params)}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; AutoModelCompetitionCSVExporter/1.0)",
        "Referer": "https://www.fotmob.com/",
    }
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        rate_limit(request_delay)
        try:
            with urlopen(
                Request(url, headers=headers, method="GET"), timeout=45
            ) as response:
                data = json.loads(response.read().decode("utf-8"))
            details = data.get("details") or {}
            if not details.get("id"):
                raise ValueError("response contains no competition details")
            return data
        except (
            HTTPError,
            URLError,
            TimeoutError,
            json.JSONDecodeError,
            ValueError,
        ) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.5))
    raise RuntimeError(f"request failed after {retries} attempts: {last_error}")


def load_ids(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(
            f"Input file not found: {path}\n"
            "Create competition_ids.txt beside the script, one ID per line."
        )
    found: list[str] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        values: Iterable[str]
        if path.suffix.lower() == ".csv":
            values = (cell for row in csv.reader(handle) for cell in row)
        else:
            values = (
                token
                for line in handle
                for token in re.split(r"[\s,;\t]+", line.strip())
            )
        for value in values:
            item = str(value).strip()
            if item.isdigit() and item not in seen:
                found.append(item)
                seen.add(item)
    if not found:
        raise ValueError(f"No numeric competition IDs found in {path}")
    return found


def completed_ids(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "Competition ID" not in reader.fieldnames:
            raise ValueError(
                f"{path} has no 'Competition ID' column. "
                "Use --overwrite or another output."
            )
        return {
            str(row.get("Competition ID", "")).strip()
            for row in reader
            if str(row.get("Competition ID", "")).strip().isdigit()
        }


def pipe(values: Iterable[Any]) -> str:
    return " | ".join(
        str(value).strip()
        for value in values
        if value is not None and str(value).strip()
    )


def bool_text(value: Any) -> str:
    if value is None:
        return ""
    return "TRUE" if bool(value) else "FALSE"


def all_table_rows(data: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    sections: list[str] = []
    seen_ids: set[str] = set()
    for index, section in enumerate(data.get("table") or [], start=1):
        block = section.get("data") or {}
        section_name = (
            block.get("leagueName")
            or section.get("tableHeader")
            or f"Table {index}"
        )
        sections.append(str(section_name))
        table = block.get("table") or {}
        candidates = table.get("all") or []
        for row in candidates:
            team_id = str(row.get("id") or "")
            key = f"{index}:{team_id}"
            if team_id and key not in seen_ids:
                copy = dict(row)
                copy["_section"] = section_name
                rows.append(copy)
                seen_ids.add(key)
    return rows, sections


def team_list(
    data: dict[str, Any], standings: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    fixture_info = (data.get("fixtures") or {}).get("fixtureInfo") or {}
    teams = fixture_info.get("teams") or []
    if teams:
        return teams
    unique: dict[str, dict[str, Any]] = {}
    for row in standings:
        team_id = str(row.get("id") or "")
        if team_id:
            unique[team_id] = {
                "id": row.get("id"),
                "name": row.get("name"),
            }
    return list(unique.values())


def fixture_status(match: dict[str, Any]) -> str:
    status = match.get("status") or {}
    if status.get("cancelled"):
        return "cancelled"
    if status.get("finished"):
        return "finished"
    return "scheduled"


def competition_row(
    competition_id: str, data: dict[str, Any]
) -> dict[str, Any]:
    details = data.get("details") or {}
    overview = data.get("overview") or {}
    fixtures_block = data.get("fixtures") or {}
    fixture_info = fixtures_block.get("fixtureInfo") or {}
    fixtures = fixtures_block.get("allMatches") or []
    stats = data.get("stats") or {}
    stat_links = stats.get("seasonStatLinks") or []
    selected_season = (
        details.get("selectedSeason")
        or overview.get("selectedSeason")
        or overview.get("season")
        or ""
    )
    selected_link = next(
        (
            item
            for item in stat_links
            if str(item.get("Name") or "") == str(selected_season)
        ),
        stat_links[0] if stat_links else {},
    )

    standings, table_sections = all_table_rows(data)
    teams = team_list(data, standings)
    rounds = fixture_info.get("rounds") or []
    groups = fixture_info.get("groups") or []
    seasons = data.get("allAvailableSeasons") or []
    winners = data.get("seasons") or []
    previous = winners[0] if winners else {}
    previous_winner = previous.get("winner") or {}
    previous_loser = previous.get("loser") or {}

    fixture_times = sorted(
        str((match.get("status") or {}).get("utcTime"))
        for match in fixtures
        if (match.get("status") or {}).get("utcTime")
    )
    next_match = next(
        (match for match in fixtures if fixture_status(match) == "scheduled"),
        {},
    )
    next_status = next_match.get("status") or {}
    next_home = next_match.get("home") or {}
    next_away = next_match.get("away") or {}

    legends: list[dict[str, Any]] = []
    for section in data.get("table") or []:
        legends.extend(((section.get("data") or {}).get("legend") or []))
    qualification = 0
    relegation = 0
    for legend in legends:
        count = len(legend.get("indices") or [])
        title = str(legend.get("title") or "").casefold()
        if "relegat" in title:
            relegation += count
        else:
            qualification += count

    standings_text = pipe(
        (
            f"{row.get('_section')}: " if len(table_sections) > 1 else ""
        )
        + f"{row.get('idx', '')}. {row.get('name', '')} "
        + f"P{row.get('played', '')} W{row.get('wins', '')} "
        + f"D{row.get('draws', '')} L{row.get('losses', '')} "
        + f"GD{row.get('goalConDiff', '')} Pts{row.get('pts', '')}"
        for row in standings
    )
    winner_history = pipe(
        f"{item.get('seasonName', '')}: "
        f"{(item.get('winner') or {}).get('name', '')}"
        for item in winners
        if (item.get("winner") or {}).get("name")
    )
    season_tournaments = pipe(
        f"{item.get('Name', '')}:{item.get('TournamentId', '')}"
        for item in stat_links
    )

    seopath = details.get("seopath") or ""
    comp_id = details.get("id") or competition_id
    return {
        "Competition ID": comp_id,
        "Competition Name": details.get("name", ""),
        "Short Name": details.get("shortName", ""),
        "Type": details.get("type", ""),
        "Country Code": details.get("country", ""),
        "Gender": details.get("gender", ""),
        "Selected Season": selected_season,
        "Latest Season": details.get("latestSeason", ""),
        "Tournament ID": selected_link.get("TournamentId", ""),
        "Data Provider": details.get("dataProvider", ""),
        "League Colour": details.get("leagueColor", ""),
        "Can Sync Calendar": bool_text(details.get("canSyncCalendar")),
        "FotMob URL": (
            f"https://www.fotmob.com/leagues/{comp_id}/overview/{seopath}"
        ),
        "Logo URL": f"https://images.fotmob.com/image_resources/logo/leaguelogo/{comp_id}.png",
        "Available Season Count": len(seasons),
        "Available Seasons": pipe(seasons),
        "Season Tournament IDs": season_tournaments,
        "Previous Winner Club ID": previous_winner.get("id", ""),
        "Previous Winner": previous_winner.get("name", ""),
        "Previous Winner Season": previous.get("seasonName", ""),
        "Previous Runner-Up Club ID": previous_loser.get("id", ""),
        "Previous Runner-Up": previous_loser.get("name", ""),
        "Winner History": winner_history,
        "Team Count": len(teams),
        "Team IDs": pipe(team.get("id") for team in teams),
        "Teams": pipe(team.get("name") for team in teams),
        "Round Count": len(rounds),
        "Active Round": (fixture_info.get("activeRound") or {}).get(
            "roundId", ""
        ),
        "Group Count": len(groups),
        "Groups": pipe(
            (group.get("name") or group.get("groupName") or group.get("id"))
            if isinstance(group, dict) else group
            for group in groups
        ),
        "Table Section Count": len(data.get("table") or []),
        "Table Sections": pipe(table_sections),
        "Standings": standings_text,
        "Qualification Places": qualification,
        "Relegation Places": relegation,
        "Fixture Count": len(fixtures),
        "Completed Fixtures": sum(
            fixture_status(match) == "finished" for match in fixtures
        ),
        "Scheduled Fixtures": sum(
            fixture_status(match) == "scheduled" for match in fixtures
        ),
        "Cancelled Fixtures": sum(
            fixture_status(match) == "cancelled" for match in fixtures
        ),
        "First Fixture UTC": fixture_times[0] if fixture_times else "",
        "Last Fixture UTC": fixture_times[-1] if fixture_times else "",
        "Next Match ID": next_match.get("id", ""),
        "Next Match UTC": next_status.get("utcTime", ""),
        "Next Home Club ID": next_home.get("id", ""),
        "Next Home Club": next_home.get("name", ""),
        "Next Away Club ID": next_away.get("id", ""),
        "Next Away Club": next_away.get("name", ""),
        "Has Ongoing Match": bool_text(
            fixtures_block.get("hasOngoingMatch")
            if "hasOngoingMatch" in fixtures_block
            else overview.get("hasOngoingMatch")
        ),
        "Has Team Of The Week": bool_text(overview.get("hasTotw")),
        "Player Stats Available": bool_text(
            stats.get("players") is not None or bool(stat_links)
        ),
        "Team Stats Available": bool_text(stats.get("teams") is not None),
        "Tabs": pipe(data.get("tabs") or []),
        "Retrieved UTC": datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ),
    }


def append_row(path: Path, row: dict[str, Any]) -> None:
    with CSV_LOCK:
        new_file = not path.exists() or path.stat().st_size == 0
        with path.open("a", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=HEADERS)
            if new_file:
                writer.writeheader()
            writer.writerow({header: row.get(header, "") for header in HEADERS})


def append_error(path: Path, competition_id: str, error: Exception) -> None:
    with CSV_LOCK:
        new_file = not path.exists() or path.stat().st_size == 0
        with path.open("a", encoding="utf-8-sig", newline="") as handle:
            fields = ["Competition ID", "Error", "Failed UTC"]
            writer = csv.DictWriter(handle, fieldnames=fields)
            if new_file:
                writer.writeheader()
            writer.writerow(
                {
                    "Competition ID": competition_id,
                    "Error": str(error),
                    "Failed UTC": datetime.now(timezone.utc).isoformat(
                        timespec="seconds"
                    ),
                }
            )


def process_one(
    competition_id: str, args: argparse.Namespace
) -> tuple[str, dict[str, Any]]:
    data = fetch_competition(
        competition_id,
        season=args.season,
        retries=args.retries,
        request_delay=args.request_delay,
    )
    return competition_id, competition_row(competition_id, data)


def main() -> int:
    args = parse_args()
    args.input = args.input.resolve()
    args.output = args.output.resolve()
    args.errors = args.errors.resolve()
    if args.workers < 1 or args.retries < 1 or args.request_delay < 0:
        raise ValueError("workers/retries must be positive; delay cannot be negative")

    ids = load_ids(args.input)
    if args.overwrite:
        args.output.unlink(missing_ok=True)
        args.errors.unlink(missing_ok=True)
    done = completed_ids(args.output)
    pending = [item for item in ids if item not in done]
    safe_print(
        f"Found {len(ids)} IDs; {len(done)} complete; "
        f"{len(pending)} to process."
    )
    if not pending:
        safe_print(f"Nothing to do. Output: {args.output}")
        return 0

    successes = 0
    failures = 0
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_one, competition_id, args): competition_id
            for competition_id in pending
        }
        for future in as_completed(futures):
            competition_id = futures[future]
            try:
                _, row = future.result()
                append_row(args.output, row)
                successes += 1
                safe_print(
                    f"[{successes + failures}/{len(pending)}] "
                    f"OK {competition_id}: {row['Competition Name']}"
                )
            except Exception as exc:
                failures += 1
                append_error(args.errors, competition_id, exc)
                safe_print(
                    f"[{successes + failures}/{len(pending)}] "
                    f"ERROR {competition_id}: {exc}"
                )

    safe_print(
        f"Finished: {successes} succeeded, {failures} failed.\n"
        f"Output: {args.output}"
    )
    if failures:
        safe_print(f"Errors: {args.errors}")
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        safe_print("\nStopped. Run the same command to resume.")
        raise SystemExit(130)
    except Exception as exc:
        safe_print(f"ERROR: {exc}")
        raise SystemExit(1)
