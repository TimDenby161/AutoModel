#!/usr/bin/env python3
"""
Export FotMob manager profiles, career records and trophies to CSV.

Setup:
    1. Create manager_ids.txt beside this script, one FotMob manager ID per line.
    2. Run: python fotmob_manager_export.py

Outputs:
    fotmob_managers.csv
    fotmob_manager_errors.csv

The export is resumable. Re-running it skips manager IDs already present in the
output and retries only missing/failed IDs.
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


BASE_URL = "https://www.fotmob.com/api/data/playerData"

HEADERS = [
    "Manager ID",
    "Name",
    "DOB",
    "Age",
    "Nationality",
    "Nationality Code",
    "Gender",
    "Status",
    "Height",
    "Current Club ID",
    "Current Club",
    "Current Club Start",
    "Current Club End",
    "Career Spells",
    "Career Club Count",
    "Career Club IDs",
    "Career Clubs",
    "Career History",
    "Total Matches",
    "Total Wins",
    "Total Draws",
    "Total Losses",
    "Career Win Percentage",
    "Career Points Per Game",
    "Latest Spell Matches",
    "Latest Spell Wins",
    "Latest Spell Draws",
    "Latest Spell Losses",
    "Latest Spell Win Percentage",
    "Latest Spell Points Per Game",
    "Trophies Won",
    "Runner-Up Finishes",
    "Trophy Club Count",
    "Trophy Club IDs",
    "Trophy Clubs",
    "Competition IDs Won",
    "Competitions Won",
    "Trophy History",
    "Recent Matches",
    "Recent Wins",
    "Recent Draws",
    "Recent Losses",
    "Recent Win Percentage",
    "Last Match ID",
    "Last Match UTC",
    "Last Opponent ID",
    "Last Opponent",
    "Last Competition ID",
    "Last Competition",
    "Last Score",
    "Last Result",
    "FotMob URL",
    "Image URL",
    "Retrieved UTC",
]

PRINT_LOCK = threading.Lock()
RATE_LOCK = threading.Lock()
CSV_LOCK = threading.Lock()
LAST_REQUEST_AT = 0.0


def parse_args() -> argparse.Namespace:
    folder = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Export FotMob manager data to a resumable CSV."
    )
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=folder / "manager_ids.txt",
        help="TXT or CSV containing manager IDs (default: manager_ids.txt)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=folder / "fotmob_managers.csv",
        help="Output CSV (default: fotmob_managers.csv beside the script)",
    )
    parser.add_argument(
        "--errors",
        type=Path,
        default=folder / "fotmob_manager_errors.csv",
        help="Error CSV (default: fotmob_manager_errors.csv beside the script)",
    )
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--request-delay", type=float, default=0.08)
    parser.add_argument("--retries", type=int, default=4)
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


def fetch_manager(
    manager_id: str, *, retries: int, request_delay: float
) -> dict[str, Any]:
    url = f"{BASE_URL}?{urlencode({'id': manager_id})}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; FotMobManagerCSVExporter/1.0)",
        "Referer": "https://www.fotmob.com/",
    }
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        rate_limit(request_delay)
        try:
            request = Request(url, headers=headers, method="GET")
            with urlopen(request, timeout=40) as response:
                data = json.loads(response.read().decode("utf-8"))
            if not data.get("isCoach"):
                raise ValueError("ID exists but is not marked as a coach")
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
            "Create manager_ids.txt beside the script, one manager ID per line."
        )
    found: list[str] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        values = (
            (cell for row in csv.reader(handle) for cell in row)
            if path.suffix.lower() == ".csv"
            else (
                token
                for line in handle
                for token in re.split(r"[\s,;\t]+", line.strip())
            )
        )
        for value in values:
            manager_id = str(value).strip()
            if manager_id.isdigit() and manager_id not in seen:
                found.append(manager_id)
                seen.add(manager_id)
    if not found:
        raise ValueError(f"No numeric manager IDs found in {path}")
    return found


def completed_ids(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "Manager ID" not in reader.fieldnames:
            raise ValueError(
                f"{path} has no 'Manager ID' column. Use --overwrite or another output."
            )
        return {
            str(row.get("Manager ID", "")).strip()
            for row in reader
            if str(row.get("Manager ID", "")).strip().isdigit()
        }


def parse_date_value(value: Any) -> date | None:
    if isinstance(value, dict):
        value = value.get("utcTime") or value.get("dateValue")
    if not value:
        return None
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def utc_value(value: Any) -> str:
    parsed = parse_date_value(value)
    return parsed.isoformat() if parsed else ""


def current_spell(spell_lists: list[list[dict[str, Any]]]) -> dict[str, Any]:
    """Pick the spell that's actually still open (or most recently started),
    across every source list, regardless of each list's own ordering."""
    best: dict[str, Any] = {}
    best_key: tuple[int, date] | None = None
    for entries in spell_lists:
        for spell in entries:
            started = parse_date_value(spell.get("startDate"))
            ended = parse_date_value(spell.get("endDate"))
            is_open = bool(spell.get("active")) or ended is None
            key = (1 if is_open else 0, started or date.min)
            if best_key is None or key > best_key:
                best, best_key = spell, key
    return best


def calculate_age(dob: str) -> int | str:
    try:
        born = date.fromisoformat(dob[:10])
    except (TypeError, ValueError):
        return ""
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


def player_info(data: dict[str, Any], key: str) -> tuple[Any, str]:
    for item in data.get("playerInformation") or []:
        if item.get("translationKey") == key or item.get("title", "").casefold() == key:
            value = item.get("value") or {}
            raw = value.get("numberValue", value.get("fallback", ""))
            return raw, str(item.get("countryCode") or "")
    return "", ""


def result_for_match(match: dict[str, Any]) -> str:
    home = int(match.get("homeScore") or 0)
    away = int(match.get("awayScore") or 0)
    own = home if match.get("isHomeTeam") else away
    opponent = away if match.get("isHomeTeam") else home
    return "W" if own > opponent else "D" if own == opponent else "L"


def unique_join(values: list[Any]) -> str:
    return ", ".join(dict.fromkeys(str(value) for value in values if value not in ("", None)))


def build_row(data: dict[str, Any], requested_id: str) -> dict[str, Any]:
    manager_id = str(data.get("id") or requested_id)
    dob = utc_value(data.get("birthDate"))
    nationality, nationality_code = player_info(data, "country_sentencecase")
    height, _ = player_info(data, "height_sentencecase")

    coach_stats = data.get("coachStats") or {}
    historical_spells = coach_stats.get("historicalCareerEntries") or []
    team_entries = (
        ((data.get("careerHistory") or {}).get("careerItems") or {})
        .get("coach", {})
        .get("teamEntries", [])
    ) or []

    spells = historical_spells or team_entries
    latest = current_spell([historical_spells, team_entries])

    def spell_number(spell: dict[str, Any], key: str) -> float:
        value = spell.get(key)
        return float(value) if isinstance(value, (int, float)) else 0.0

    totals = {
        key: sum(spell_number(spell, key) for spell in spells)
        for key in ("matches", "wins", "draws", "losses")
    }
    total_matches = totals["matches"]
    career_win_pct = totals["wins"] / total_matches * 100 if total_matches else ""
    career_ppg = (
        (totals["wins"] * 3 + totals["draws"]) / total_matches
        if total_matches
        else ""
    )

    career_history = " | ".join(
        f"{spell.get('teamName') or spell.get('team', '')} "
        f"({utc_value(spell.get('startDate'))} to "
        f"{utc_value(spell.get('endDate')) or 'present'}): "
        f"{spell.get('matches', '')} matches, {spell.get('wins', '')} wins"
        for spell in spells
    )

    coach_trophies = (data.get("trophies") or {}).get("coachTrophies") or []
    won = runner_up = 0
    trophy_club_ids: list[Any] = []
    trophy_clubs: list[Any] = []
    competition_ids: list[Any] = []
    competitions: list[Any] = []
    trophy_parts: list[str] = []
    for club in coach_trophies:
        trophy_club_ids.append(club.get("teamId"))
        trophy_clubs.append(club.get("teamName"))
        for tournament in club.get("tournaments") or []:
            seasons_won = tournament.get("seasonsWon") or []
            seasons_runner_up = tournament.get("seasonsRunnerUp") or []
            won += len(seasons_won)
            runner_up += len(seasons_runner_up)
            if seasons_won:
                competition_ids.append(tournament.get("leagueId"))
                competitions.append(tournament.get("leagueName"))
            trophy_parts.append(
                f"{club.get('teamName', '')} - {tournament.get('leagueName', '')}: "
                f"won [{', '.join(seasons_won)}]; "
                f"runner-up [{', '.join(seasons_runner_up)}]"
            )

    recent = data.get("recentMatches") or []
    recent_results = [result_for_match(match) for match in recent]
    last_match = recent[0] if recent else {}
    last_result = recent_results[0] if recent_results else ""
    last_score = (
        f"{last_match.get('homeScore', '')}-{last_match.get('awayScore', '')}"
        if last_match
        else ""
    )
    slug = ((data.get("meta") or {}).get("seopath") or "")

    row = {header: "" for header in HEADERS}
    row.update(
        {
            "Manager ID": manager_id,
            "Name": data.get("name", ""),
            "DOB": dob,
            "Age": calculate_age(dob),
            "Nationality": nationality,
            "Nationality Code": nationality_code,
            "Gender": data.get("gender", ""),
            "Status": data.get("status", ""),
            "Height": height,
            "Current Club ID": latest.get("teamId", ""),
            "Current Club": latest.get("teamName") or latest.get("team", ""),
            "Current Club Start": utc_value(latest.get("startDate")),
            "Current Club End": utc_value(latest.get("endDate")),
            "Career Spells": len(spells),
            "Career Club Count": len({s.get("teamId") for s in spells if s.get("teamId")}),
            "Career Club IDs": unique_join([s.get("teamId") for s in spells]),
            "Career Clubs": unique_join(
                [s.get("teamName") or s.get("team") for s in spells]
            ),
            "Career History": career_history,
            "Total Matches": int(totals["matches"]),
            "Total Wins": int(totals["wins"]),
            "Total Draws": int(totals["draws"]),
            "Total Losses": int(totals["losses"]),
            "Career Win Percentage": round(career_win_pct, 2) if career_win_pct != "" else "",
            "Career Points Per Game": round(career_ppg, 2) if career_ppg != "" else "",
            "Latest Spell Matches": latest.get("matches", ""),
            "Latest Spell Wins": latest.get("wins", ""),
            "Latest Spell Draws": latest.get("draws", ""),
            "Latest Spell Losses": latest.get("losses", ""),
            "Latest Spell Win Percentage": round(latest.get("winPercentage"), 2)
            if isinstance(latest.get("winPercentage"), (int, float))
            else "",
            "Latest Spell Points Per Game": round(latest.get("pointsPerGame"), 2)
            if isinstance(latest.get("pointsPerGame"), (int, float))
            else "",
            "Trophies Won": won,
            "Runner-Up Finishes": runner_up,
            "Trophy Club Count": len(set(trophy_club_ids)),
            "Trophy Club IDs": unique_join(trophy_club_ids),
            "Trophy Clubs": unique_join(trophy_clubs),
            "Competition IDs Won": unique_join(competition_ids),
            "Competitions Won": unique_join(competitions),
            "Trophy History": " | ".join(trophy_parts),
            "Recent Matches": len(recent),
            "Recent Wins": recent_results.count("W"),
            "Recent Draws": recent_results.count("D"),
            "Recent Losses": recent_results.count("L"),
            "Recent Win Percentage": round(
                recent_results.count("W") / len(recent_results) * 100, 2
            )
            if recent_results
            else "",
            "Last Match ID": last_match.get("id", ""),
            "Last Match UTC": utc_value(last_match.get("matchDate")),
            "Last Opponent ID": last_match.get("opponentTeamId", ""),
            "Last Opponent": last_match.get("opponentTeamName", ""),
            "Last Competition ID": last_match.get("leagueId", ""),
            "Last Competition": last_match.get("leagueName", ""),
            "Last Score": last_score,
            "Last Result": last_result,
            "FotMob URL": f"https://www.fotmob.com/players/{manager_id}/{slug}",
            "Image URL": f"https://images.fotmob.com/image_resources/playerimages/{manager_id}.png",
            "Retrieved UTC": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
    )
    return row


def append_row(path: Path, row: dict[str, Any]) -> None:
    with CSV_LOCK:
        new_file = not path.exists() or path.stat().st_size == 0
        with path.open("a", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=HEADERS, extrasaction="ignore")
            if new_file:
                writer.writeheader()
            writer.writerow(row)


def append_error(path: Path, manager_id: str, error: Exception) -> None:
    with CSV_LOCK:
        new_file = not path.exists() or path.stat().st_size == 0
        with path.open("a", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            if new_file:
                writer.writerow(["Manager ID", "Error", "Failed UTC"])
            writer.writerow(
                [
                    manager_id,
                    str(error),
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                ]
            )


def process(manager_id: str, args: argparse.Namespace) -> dict[str, Any]:
    data = fetch_manager(
        manager_id, retries=args.retries, request_delay=args.request_delay
    )
    return build_row(data, manager_id)


def main() -> int:
    args = parse_args()
    args.input = args.input.resolve()
    args.output = args.output.resolve()
    args.errors = args.errors.resolve()
    try:
        manager_ids = load_ids(args.input)
        if args.overwrite:
            args.output.unlink(missing_ok=True)
            args.errors.unlink(missing_ok=True)
        done = completed_ids(args.output)
        pending = [manager_id for manager_id in manager_ids if manager_id not in done]
        safe_print(
            f"{len(manager_ids)} IDs loaded; {len(done)} already complete; "
            f"{len(pending)} to process."
        )
        if not pending:
            return 0
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = {
                executor.submit(process, manager_id, args): manager_id
                for manager_id in pending
            }
            for number, future in enumerate(as_completed(futures), start=1):
                manager_id = futures[future]
                try:
                    row = future.result()
                    append_row(args.output, row)
                    safe_print(
                        f"[{number}/{len(pending)}] OK {manager_id} - {row['Name']}"
                    )
                except Exception as exc:
                    append_error(args.errors, manager_id, exc)
                    safe_print(f"[{number}/{len(pending)}] ERROR {manager_id}: {exc}")
        return 0
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
