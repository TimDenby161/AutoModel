#!/usr/bin/env python3
"""
Export useful AutoModel club data to a CSV in the same folder as this script.

Setup:
    1. Create club_ids.txt beside this script, one club ID per line.
    2. Run: python getClubs.py

The script creates:
    automodel_clubs.csv
    automodel_club_errors.csv

Successful club IDs are skipped when the script is run again, so failed or
interrupted imports can be retried safely without starting over.
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
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://www.fotmob.com/api/data/teams"

HEADERS = [
    "Club ID",
    "Club Name",
    "Short Name",
    "Country Code",
    "Gender",
    "Latest Season",
    "Primary League ID",
    "Primary League",
    "Current Tournament ID",
    "FotMob URL",
    "Crest URL",
    "Can Sync Calendar",
    "Stadium",
    "Stadium City",
    "Stadium Country",
    "Latitude",
    "Longitude",
    "Capacity",
    "Opened",
    "Surface",
    "Primary Colour",
    "Coach ID",
    "Coach Name",
    "Coach Nationality",
    "Coach DOB",
    "Squad Size",
    "Goalkeepers",
    "Defenders",
    "Midfielders",
    "Forwards",
    "Goalkeeper IDs",
    "Defender IDs",
    "Midfielder IDs",
    "Forward IDs",
    "Squad Player IDs",
    "Squad Player Names",
    "Competition Count",
    "Competition IDs",
    "Competitions",
    "Competition Seasons",
    "Next Match ID",
    "Next Match UTC",
    "Next Opponent ID",
    "Next Opponent",
    "Next Competition ID",
    "Next Competition",
    "Last Match ID",
    "Last Match UTC",
    "Last Opponent ID",
    "Last Opponent",
    "Last Competition ID",
    "Last Competition",
    "Last Score",
    "Last Result",
    "Retrieved UTC",
]

PRINT_LOCK = threading.Lock()
RATE_LOCK = threading.Lock()
CSV_LOCK = threading.Lock()
LAST_REQUEST_AT = 0.0


def parse_args() -> argparse.Namespace:
    folder = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Export AutoModel club information to a resumable CSV."
    )
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=folder / "club_ids.txt",
        help="TXT or CSV containing club IDs (default: club_ids.txt)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=folder / "automodel_clubs.csv",
        help="Output CSV (default: automodel_clubs.csv beside the script)",
    )
    parser.add_argument(
        "--errors",
        type=Path,
        default=folder / "automodel_club_errors.csv",
        help="Error CSV (default: automodel_club_errors.csv beside the script)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Concurrent clubs to process (default: 10)",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.08,
        help="Global delay between request starts (default: 0.08s)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        help="Attempts per club (default: 4)",
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


def rate_limit(delay: float) -> None:
    global LAST_REQUEST_AT
    with RATE_LOCK:
        now = time.monotonic()
        wait_for = delay - (now - LAST_REQUEST_AT)
        if wait_for > 0:
            time.sleep(wait_for)
        LAST_REQUEST_AT = time.monotonic()


def fetch_club(
    club_id: str, *, retries: int, request_delay: float
) -> dict[str, Any]:
    query = urlencode({"id": club_id})
    url = f"{BASE_URL}?{query}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; AutoModelClubCSVExporter/1.0)",
        "Referer": "https://www.fotmob.com/",
    }
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        rate_limit(request_delay)
        try:
            with urlopen(
                Request(url, headers=headers, method="GET"), timeout=40
            ) as response:
                body = response.read()
            return json.loads(body.decode("utf-8"))
        except (
            HTTPError,
            URLError,
            TimeoutError,
            json.JSONDecodeError,
        ) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.5))

    raise RuntimeError(f"request failed after {retries} attempts: {last_error}")


def load_ids(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(
            f"Input file not found: {path}\n"
            "Create club_ids.txt beside the script, one club ID per line."
        )

    found: list[str] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        if path.suffix.lower() == ".csv":
            values = (cell for row in csv.reader(handle) for cell in row)
        else:
            values = (
                token
                for line in handle
                for token in re.split(r"[\s,;\t]+", line.strip())
            )
        for value in values:
            club_id = str(value).strip()
            if club_id.isdigit() and club_id not in seen:
                found.append(club_id)
                seen.add(club_id)

    if not found:
        raise ValueError(f"No numeric club IDs found in {path}")
    return found


def completed_ids(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "Club ID" not in reader.fieldnames:
            raise ValueError(
                f"{path} has no 'Club ID' column. Use --overwrite or another output."
            )
        return {
            str(row.get("Club ID", "")).strip()
            for row in reader
            if str(row.get("Club ID", "")).strip().isdigit()
        }


def nested(data: Any, *keys: str, default: Any = "") -> Any:
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return default if current is None else current


def venue_stat(venue: dict[str, Any], label: str) -> Any:
    for pair in venue.get("statPairs") or []:
        if len(pair) >= 2 and str(pair[0]).casefold() == label.casefold():
            return pair[1]
    return ""


def squad_groups(data: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for group in nested(data, "squad", "squad", default=[]) or []:
        result[str(group.get("title") or "").casefold()] = group.get("members") or []
    return result


def members_for(
    groups: dict[str, list[dict[str, Any]]], *possible_titles: str
) -> list[dict[str, Any]]:
    for title in possible_titles:
        if title in groups:
            return groups[title]
    return []


def joined(items: list[Any]) -> str:
    return ", ".join(str(item) for item in items if item not in (None, ""))


def match_fields(match: dict[str, Any] | None, club_id: str) -> list[Any]:
    if not match:
        return [""] * 7
    opponent = match.get("opponent") or {}
    tournament = match.get("tournament") or {}
    status = match.get("status") or {}
    return [
        match.get("id") or "",
        status.get("utcTime") or "",
        opponent.get("id") or "",
        opponent.get("name") or "",
        tournament.get("leagueId") or "",
        tournament.get("name") or "",
        status.get("scoreStr") or "",
    ]


def result_text(match: dict[str, Any] | None) -> str:
    if not match or match.get("result") is None:
        return ""
    return {1: "Win", 0: "Draw", -1: "Loss"}.get(match.get("result"), "")


def build_row(
    club_id: str, *, retries: int, request_delay: float
) -> list[Any]:
    data = fetch_club(
        club_id, retries=retries, request_delay=request_delay
    )
    details = data.get("details") or {}
    overview = data.get("overview") or {}
    venue = overview.get("venue") or {}
    venue_widget = venue.get("widget") or {}
    location = venue_widget.get("location") or []
    schema = details.get("sportsTeamJSONLD") or {}
    schema_location = schema.get("location") or {}
    address = schema_location.get("address") or {}
    colours = overview.get("teamColors") or {}

    groups = squad_groups(data)
    coaches = members_for(groups, "coach", "coaches")
    coach = coaches[0] if coaches else {}
    keepers = members_for(groups, "keepers", "goalkeepers")
    defenders = members_for(groups, "defenders")
    midfielders = members_for(groups, "midfielders")
    attackers = members_for(groups, "attackers", "forwards")
    players = keepers + defenders + midfielders + attackers

    tournaments = nested(data, "stats", "tournamentSeasons", default=[]) or []
    competition_ids: list[Any] = []
    competition_names: list[str] = []
    competition_seasons: list[str] = []
    seen_competitions: set[str] = set()
    for tournament in tournaments:
        parent_id = tournament.get("parentLeagueId")
        name = str(tournament.get("leagueName") or "")
        key = str(parent_id or name)
        if key not in seen_competitions:
            seen_competitions.add(key)
            competition_ids.append(parent_id)
            competition_names.append(name)
        season_text = " — ".join(
            part
            for part in [
                name,
                str(tournament.get("season") or ""),
                str(tournament.get("tournamentId") or ""),
            ]
            if part
        )
        if season_text:
            competition_seasons.append(season_text)

    next_match = overview.get("nextMatch") or {}
    last_match = overview.get("lastMatch") or {}
    next_values = match_fields(next_match, club_id)
    last_values = match_fields(last_match, club_id)

    return [
        details.get("id") or club_id,
        details.get("name") or "",
        details.get("shortName") or "",
        details.get("country") or "",
        details.get("gender") or "",
        details.get("latestSeason") or overview.get("season") or "",
        details.get("primaryLeagueId") or "",
        details.get("primaryLeagueName") or "",
        nested(data, "stats", "primarySeasonId"),
        schema.get("url") or (
            f"https://www.fotmob.com/teams/{club_id}/overview/"
            f"{details.get('seopath') or ''}"
        ),
        schema.get("logo") or (
            f"https://images.fotmob.com/image_resources/logo/teamlogo/{club_id}.png"
        ),
        bool(details.get("canSyncCalendar", False)),
        venue_widget.get("name") or schema_location.get("name") or "",
        venue_widget.get("city") or address.get("addressLocality") or "",
        address.get("addressCountry") or "",
        location[0] if len(location) > 0 else nested(schema_location, "geo", "latitude"),
        location[1] if len(location) > 1 else nested(schema_location, "geo", "longitude"),
        venue_stat(venue, "Capacity"),
        venue_stat(venue, "Opened"),
        venue_stat(venue, "Surface"),
        colours.get("lightMode") or colours.get("darkMode") or "",
        coach.get("id") or "",
        coach.get("name") or "",
        coach.get("cname") or coach.get("ccode") or "",
        coach.get("dateOfBirth") or "",
        len(players),
        len(keepers),
        len(defenders),
        len(midfielders),
        len(attackers),
        joined([member.get("id") for member in keepers]),
        joined([member.get("id") for member in defenders]),
        joined([member.get("id") for member in midfielders]),
        joined([member.get("id") for member in attackers]),
        joined([member.get("id") for member in players]),
        joined([member.get("name") for member in players]),
        len(seen_competitions),
        joined(competition_ids),
        joined(competition_names),
        " | ".join(competition_seasons),
        *next_values[:6],
        *last_values,
        result_text(last_match),
        datetime.now(timezone.utc).isoformat(timespec="seconds"),
    ]


def append_row(path: Path, headers: list[str], row: list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with CSV_LOCK:
        needs_header = not path.exists() or path.stat().st_size == 0
        with path.open("a", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            if needs_header:
                writer.writerow(headers)
            writer.writerow(row)
            handle.flush()


def append_error(path: Path, club_id: str, error: Exception) -> None:
    append_row(
        path,
        ["Club ID", "Error", "Timestamp UTC"],
        [
            club_id,
            str(error),
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
        ],
    )


def main() -> int:
    args = parse_args()
    args.input = args.input.resolve()
    args.output = args.output.resolve()
    args.errors = args.errors.resolve()

    if args.workers < 1:
        raise ValueError("--workers must be at least 1")
    if args.request_delay < 0:
        raise ValueError("--request-delay cannot be negative")
    if args.retries < 1:
        raise ValueError("--retries must be at least 1")

    ids = load_ids(args.input)
    if args.overwrite:
        if args.output.exists():
            args.output.unlink()
        if args.errors.exists():
            args.errors.unlink()
        done: set[str] = set()
    else:
        done = completed_ids(args.output)

    pending = [club_id for club_id in ids if club_id not in done]
    safe_print(f"Loaded {len(ids):,} unique club IDs.")
    safe_print(f"Already complete: {len(done):,}")
    safe_print(f"Remaining: {len(pending):,}")
    safe_print(f"Output: {args.output}")

    if not pending:
        safe_print("Nothing to do.")
        return 0

    succeeded = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                build_row,
                club_id,
                retries=args.retries,
                request_delay=args.request_delay,
            ): club_id
            for club_id in pending
        }
        for future in as_completed(futures):
            club_id = futures[future]
            try:
                row = future.result()
                if len(row) != len(HEADERS):
                    raise RuntimeError(
                        f"internal column mismatch: {len(row)} values for "
                        f"{len(HEADERS)} headers"
                    )
                append_row(args.output, HEADERS, row)
                succeeded += 1
                safe_print(
                    f"[OK {succeeded + failed}/{len(pending)}] "
                    f"{club_id} - {row[1]}"
                )
            except Exception as exc:
                failed += 1
                append_error(args.errors, club_id, exc)
                safe_print(
                    f"[ERROR {succeeded + failed}/{len(pending)}] "
                    f"{club_id}: {exc}"
                )

    safe_print(f"Finished. Added {succeeded:,} clubs; {failed:,} failed.")
    if failed:
        safe_print(f"Failures were written to {args.errors}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nStopped. Run the script again to resume.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
