#!/usr/bin/env python3
"""
Keep the "PlayerData" Google Sheet (and fotmob_players_full.csv) up to date
from player_ids.txt, on a schedule.

- Player IDs not yet in the sheet get the full multi-season pull (same as
  getPlayers.py --mode full).
- Player IDs already in the sheet get a lightweight refresh: profile info,
  the current season's stats, and the "extra" fields (injury, trophies,
  market value, recent form, etc.) are updated in place. Every other
  season's detailed stats are left exactly as they are in the sheet, since
  a finished season's numbers don't change and re-fetching them is wasted
  work.
- After PlayerData is updated, the "Players" tab's column A is rewritten
  to match PlayerData's full ID list, growing the sheet and copying the
  existing B:LX lookup formulas down into any new rows. Pass
  --skip-players-tab to leave that tab alone.
- The "ManagerData" tab is kept in sync from manager_ids.txt the same way,
  using getManagers.py's fetch/build logic. Managers don't have the
  season-by-season history players do, so every manager (new or already
  in the sheet) just gets a fresh full re-fetch each run - no partial
  refresh/merge needed. Pass --skip-managers to leave it alone.
- The "CompetitionData" tab is kept in sync from competition_ids.txt the
  same way, using getCompetitions.py's fetch/build logic. Same as
  managers, every competition just gets a fresh full re-fetch and
  overwrite each run. Pass --skip-competitions to leave it alone.
- After CompetitionData is updated, the "Leagues" tab's column A is
  rewritten to match its full ID list, the same way the Players tab
  mirrors PlayerData. Pass --skip-leagues-tab to leave that tab alone.
- The "MatchData" tab is kept in sync from club_ids.txt using
  getMatches.py's collect_matches(): an existing row only gets rewritten
  if it's not finished yet AND its scheduled date has already passed
  (i.e. it's actually due for a status/result update); a not-yet-due
  future fixture that's already round-filled is left alone until its
  date arrives. Any match not yet in the sheet - including every match
  for a newly added club - gets added regardless. Historical matches
  outside the current season aren't touched. Pass --skip-matches to
  leave it alone.
- The "ClubData" tab is kept in sync from club_ids.txt (the same list
  MatchData uses) using getClubs.py's fetch/build logic. Same as managers
  and competitions, every club gets a fresh full re-fetch and overwrite
  each run. Pass --skip-clubs to leave it alone.
- After ClubData is updated, the "Club" tab's column A is rewritten to
  match its full ID list, the same way as Leagues/Players. Pass
  --skip-club-tab to leave that tab alone.
- After MatchData is updated, the "Matches" tab's column A is rewritten
  to match its full ID list, the same way as Leagues/Club/Players. Pass
  --skip-matches-tab to leave that tab alone.

One-time setup:
    pip install gspread
    Create a Google Cloud service account, add a JSON key for it, save the
    key beside this script as service_account.json (or pass --credentials
    with another path), and share the target Google Sheet with the service
    account's email address (ends in @...iam.gserviceaccount.com) as an
    Editor. No browser login is needed - the key alone authenticates.

Usage:
    python syncPlayersToSheet.py
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import getPlayers as gp
import getManagers as gmgr
import getCompetitions as gcomp
import getMatches as gmatch
import getClubs as gclub

try:
    import gspread
except ImportError:
    print(
        "Missing dependency: run `pip install gspread` first.",
        file=sys.stderr,
    )
    raise SystemExit(1)


DEFAULT_SPREADSHEET_ID = "1QhBOsdqzvxxLlXD8iJYwqGRaJIswU_6K86nLY88q5us"
DEFAULT_WORKSHEET = "PlayerData"

ID_COL = gp.HEADERS.index("Player ID")
PROFILE_LEN = len(gp.PROFILE_HEADERS)
BLOCK_LEN = len(gp.SEASON_FIELDS)
BASIC_LEN = 5  # Club, Appearances, Goals, Assists, Rating - the non-"detailed" fields

PLAYERS_WORKSHEET = "Players"
PLAYERS_HEADER_ROW = 2  # row 1 is blank, row 2 holds the column labels
PLAYERS_DATA_START_ROW = 3
PLAYERS_LAST_FORMULA_COLUMN = "LX"  # every column B..LX looks up PlayerData by ID

LEAGUES_WORKSHEET = "Leagues"
LEAGUES_HEADER_ROW = 1
LEAGUES_DATA_START_ROW = 2
LEAGUES_LAST_FORMULA_COLUMN = "G"  # every column B..G looks up CompetitionData by ID

CLUB_WORKSHEET = "Club"
CLUB_HEADER_ROW = 1
CLUB_DATA_START_ROW = 2
CLUB_LAST_FORMULA_COLUMN = "AG"  # every column B..AG looks up ClubData by ID

MATCHES_WORKSHEET = "Matches"
MATCHES_HEADER_ROW = 1
MATCHES_DATA_START_ROW = 2
MATCHES_LAST_FORMULA_COLUMN = "Q"  # every column B..Q looks up MatchData by ID

INDIVIDUAL_RESULTS_SPREADSHEET_ID = "1y2L7pOfIHqBMQCYsMy3g1Cm1iHCl3aR6onIpMWzWa1A"
INDIVIDUAL_RESULTS_WORKSHEET = "Individual Results"

PRINT_LOCK = threading.Lock()


def safe_print(message: str) -> None:
    with PRINT_LOCK:
        try:
            print(message, flush=True)
        except UnicodeEncodeError:
            print(message.encode("ascii", "backslashreplace").decode(), flush=True)


def parse_args() -> argparse.Namespace:
    folder = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=folder / "player_ids.txt",
        help="TXT or CSV containing player IDs (default: player_ids.txt)",
    )
    parser.add_argument("--spreadsheet-id", default=DEFAULT_SPREADSHEET_ID)
    parser.add_argument("--worksheet", default=DEFAULT_WORKSHEET)
    parser.add_argument(
        "--credentials",
        type=Path,
        default=folder / "service_account.json",
        help="Service account JSON key (default: service_account.json beside this script)",
    )
    parser.add_argument(
        "--csv-output",
        type=Path,
        default=folder / "fotmob_players_full.csv",
        help="CSV mirror of the sheet, rewritten each run",
    )
    parser.add_argument(
        "--errors",
        type=Path,
        default=folder / "fotmob_sync_errors.csv",
        help="Error CSV, rewritten each run",
    )
    parser.add_argument(
        "--skip-players",
        action="store_true",
        help="Don't sync the PlayerData sheet in this run",
    )
    parser.add_argument(
        "--skip-players-tab",
        action="store_true",
        help="Don't mirror IDs/formulas into the Players tab after syncing PlayerData",
    )
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--detail-workers", type=int, default=6)
    parser.add_argument("--request-delay", type=float, default=0.06)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--current-year", type=int, default=gp.CURRENT_SEASON_YEAR)
    parser.add_argument(
        "--sheet-batch-size",
        type=int,
        default=200,
        help="Rows per Sheets API call (keeps requests small and within quota)",
    )
    parser.add_argument(
        "--manager-input",
        type=Path,
        default=folder / "manager_ids.txt",
        help="TXT or CSV containing manager IDs (default: manager_ids.txt)",
    )
    parser.add_argument("--manager-worksheet", default="ManagerData")
    parser.add_argument(
        "--manager-csv-output",
        type=Path,
        default=folder / "fotmob_managers.csv",
        help="CSV mirror of the ManagerData sheet, rewritten each run",
    )
    parser.add_argument(
        "--manager-errors",
        type=Path,
        default=folder / "fotmob_manager_sync_errors.csv",
        help="Manager error CSV, rewritten each run",
    )
    parser.add_argument("--manager-workers", type=int, default=10)
    parser.add_argument("--manager-request-delay", type=float, default=0.08)
    parser.add_argument("--manager-retries", type=int, default=4)
    parser.add_argument(
        "--skip-managers",
        action="store_true",
        help="Don't sync the ManagerData tab in this run",
    )
    parser.add_argument(
        "--competition-input",
        type=Path,
        default=folder / "competition_ids.txt",
        help="TXT or CSV containing competition IDs (default: competition_ids.txt)",
    )
    parser.add_argument("--competition-worksheet", default="CompetitionData")
    parser.add_argument(
        "--competition-csv-output",
        type=Path,
        default=folder / "fotmob_competitions.csv",
        help="CSV mirror of the CompetitionData sheet, rewritten each run",
    )
    parser.add_argument(
        "--competition-errors",
        type=Path,
        default=folder / "fotmob_competition_sync_errors.csv",
        help="Competition error CSV, rewritten each run",
    )
    parser.add_argument(
        "--competition-season",
        default="",
        help='Optional season override, for example "2026/2027"',
    )
    parser.add_argument("--competition-workers", type=int, default=10)
    parser.add_argument("--competition-request-delay", type=float, default=0.08)
    parser.add_argument("--competition-retries", type=int, default=4)
    parser.add_argument(
        "--skip-competitions",
        action="store_true",
        help="Don't sync the CompetitionData tab in this run",
    )
    parser.add_argument(
        "--skip-leagues-tab",
        action="store_true",
        help="Don't mirror IDs/formulas into the Leagues tab after syncing CompetitionData",
    )
    parser.add_argument(
        "--club-input",
        type=Path,
        default=folder / "club_ids.txt",
        help="TXT or CSV containing club IDs (default: club_ids.txt)",
    )
    parser.add_argument("--match-worksheet", default="MatchData")
    parser.add_argument(
        "--match-csv-output",
        type=Path,
        default=folder / "fotmob_matches.csv",
        help="CSV mirror of the MatchData sheet, rewritten each run",
    )
    parser.add_argument(
        "--match-errors",
        type=Path,
        default=folder / "fotmob_match_sync_errors.csv",
        help="Match error CSV, rewritten each run",
    )
    parser.add_argument("--match-mode", choices=("full", "fixtures"), default="full")
    parser.add_argument("--match-from-date", default="", help="Inclusive YYYY-MM-DD filter")
    parser.add_argument("--match-to-date", default="", help="Inclusive YYYY-MM-DD filter")
    parser.add_argument(
        "--match-all-seasons",
        action="store_true",
        help="Don't clip fixtures to each club's current season",
    )
    parser.add_argument("--match-club-workers", type=int, default=10)
    parser.add_argument("--match-detail-workers", type=int, default=12)
    parser.add_argument("--match-request-delay", type=float, default=0.06)
    parser.add_argument("--match-retries", type=int, default=4)
    parser.add_argument(
        "--skip-matches",
        action="store_true",
        help="Don't sync the MatchData tab in this run",
    )
    parser.add_argument(
        "--skip-matches-tab",
        action="store_true",
        help="Don't mirror IDs/formulas into the Matches tab after syncing MatchData",
    )
    parser.add_argument(
        "--individual-results-spreadsheet-id",
        default=INDIVIDUAL_RESULTS_SPREADSHEET_ID,
        help="Spreadsheet ID to receive finished matches for the Individual Results tab",
    )
    parser.add_argument(
        "--individual-results-worksheet",
        default=INDIVIDUAL_RESULTS_WORKSHEET,
        help='Destination tab for finished matches (default: "Individual Results")',
    )
    parser.add_argument(
        "--skip-individual-results",
        action="store_true",
        help="Don't copy finished matches into the Individual Results spreadsheet",
    )
    parser.add_argument(
        "--only-individual-results",
        action="store_true",
        help="Only copy finished matches into Individual Results, using current sheet values",
    )
    parser.add_argument("--club-worksheet", default="ClubData")
    parser.add_argument(
        "--club-csv-output",
        type=Path,
        default=folder / "fotmob_clubs.csv",
        help="CSV mirror of the ClubData sheet, rewritten each run",
    )
    parser.add_argument(
        "--club-errors",
        type=Path,
        default=folder / "fotmob_club_sync_errors.csv",
        help="Club error CSV, rewritten each run",
    )
    parser.add_argument("--club-workers", type=int, default=10)
    parser.add_argument("--club-request-delay", type=float, default=0.08)
    parser.add_argument("--club-retries", type=int, default=4)
    parser.add_argument(
        "--skip-clubs",
        action="store_true",
        help="Don't sync the ClubData tab in this run",
    )
    parser.add_argument(
        "--skip-club-tab",
        action="store_true",
        help="Don't mirror IDs/formulas into the Club tab after syncing ClubData",
    )
    return parser.parse_args()


def merge_existing_row(existing_row: list[str], fresh_row: list[Any], current_year: int) -> list[Any]:
    """Start from a freshly refreshed row, but keep the old detailed stats
    for every season except the current one."""
    merged = list(fresh_row)
    padded_existing = existing_row + [""] * (len(gp.HEADERS) - len(existing_row))
    for index, year in enumerate(gp.YEARS):
        if year == current_year:
            continue
        block_start = PROFILE_LEN + index * BLOCK_LEN
        detail_start = block_start + BASIC_LEN
        detail_end = block_start + BLOCK_LEN
        for col in range(detail_start, detail_end):
            old_value = padded_existing[col]
            if old_value not in ("", None):
                merged[col] = old_value
    return merged


def fetch_new(player_id: str, args: argparse.Namespace) -> list[Any]:
    return gp.build_player_row(
        player_id,
        mode="full",
        current_year=args.current_year,
        detail_workers=args.detail_workers,
        retries=args.retries,
        request_delay=args.request_delay,
    )


def fetch_existing(player_id: str, existing_row: list[str], args: argparse.Namespace) -> list[Any]:
    fresh = gp.build_player_row(
        player_id,
        mode="refresh",
        current_year=args.current_year,
        detail_workers=min(args.detail_workers, 3),
        retries=args.retries,
        request_delay=args.request_delay,
    )
    return merge_existing_row(existing_row, fresh, args.current_year)


def column_letters(count: int) -> str:
    letters = gspread.utils.rowcol_to_a1(1, count)
    return "".join(ch for ch in letters if ch.isalpha())


def sheet_call(fn, *, retries: int = 5, description: str = "Sheets API call"):
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except gspread.exceptions.APIError as exc:
            last_error = exc
            if attempt < retries:
                wait = min(2**attempt, 30)
                safe_print(f"{description} failed ({exc}); retrying in {wait}s...")
                time.sleep(wait)
    raise RuntimeError(f"{description} failed after {retries} attempts: {last_error}")


def read_existing_values(worksheet, description: str) -> list[list[Any]]:
    """Read a whole sheet with UNFORMATTED_VALUE. The default FORMATTED_VALUE
    returns the cell's *display* text, which for a large enough number can
    collapse to scientific notation (e.g. "4.92E+06") - a different number
    can display identically, and a freshly fetched real ID like "4920046"
    then never matches that string, so it looks new and gets re-appended
    as a duplicate instead of recognized as already present."""
    return sheet_call(
        lambda: worksheet.get_all_values(value_render_option="UNFORMATTED_VALUE"),
        description=description,
    )


def id_column_values(worksheet, description: str) -> list[str]:
    """Like read_existing_values, but for a single ID column read via
    col_values - same UNFORMATTED_VALUE requirement, same reasoning."""
    return sheet_call(
        lambda: worksheet.col_values(1, value_render_option="UNFORMATTED_VALUE"),
        description=description,
    )


def open_spreadsheet(spreadsheet_id: str, credentials_path: Path):
    if not credentials_path.exists():
        raise SystemExit(
            f"Service account key not found: {credentials_path}\n"
            "Create a service account in Google Cloud Console, download its "
            "JSON key to that path (or pass --credentials), and share the "
            "target sheet with the service account's email as an Editor."
        )
    service_account_email = "the service account email"
    try:
        with credentials_path.open(encoding="utf-8") as handle:
            service_account_email = json.load(handle).get("client_email") or service_account_email
    except (OSError, json.JSONDecodeError):
        pass

    gc = gspread.service_account(filename=str(credentials_path))
    try:
        return gc.open_by_key(spreadsheet_id)
    except PermissionError as exc:
        raise SystemExit(
            f"Permission denied opening spreadsheet {spreadsheet_id}. Share it with "
            f"{service_account_email} as an Editor and try again."
        ) from exc
    except gspread.exceptions.APIError as exc:
        if "PERMISSION_DENIED" in str(exc):
            raise SystemExit(
                f"Permission denied opening spreadsheet {spreadsheet_id}. Share it with "
                f"{service_account_email} as an Editor and try again."
            ) from exc
        raise


def get_or_create_worksheet(spreadsheet, worksheet_name: str, cols: int):
    try:
        return spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=worksheet_name, rows=1, cols=cols)


def sync_lookup_tab(
    spreadsheet,
    worksheet_name: str,
    ids: list[str],
    *,
    data_start_row: int,
    last_formula_column: str,
) -> None:
    """Mirror `ids` into column A of `worksheet_name`, growing the sheet and
    copying the existing B:<last_formula_column> lookup formulas down for
    any new rows. Row order doesn't matter - every formula looks its own
    row's ID up by value (MATCH/XLOOKUP/INDEX), not by position."""
    last_col_index = gspread.utils.a1_to_rowcol(f"{last_formula_column}1")[1]
    worksheet = get_or_create_worksheet(spreadsheet, worksheet_name, last_col_index)
    existing_count = len(worksheet.col_values(1)[data_start_row - 1 :])
    existing_last_row = data_start_row - 1 + existing_count
    new_last_row = data_start_row - 1 + len(ids)

    # Base the formula copy-down on how far column B's formulas actually
    # reach, not on column A's ID count - if a previous run was interrupted
    # between writing IDs and copying formulas, those two can disagree, and
    # trusting column A alone would silently leave the gap unfilled forever.
    formula_last_row = data_start_row - 1 + len(worksheet.col_values(2)[data_start_row - 1 :])

    if new_last_row > worksheet.row_count:
        sheet_call(
            lambda: worksheet.add_rows(new_last_row - worksheet.row_count),
            description=f"Grow {worksheet_name} sheet",
        )

    clear_through = max(existing_last_row, new_last_row)
    if clear_through >= data_start_row:
        sheet_call(
            lambda: worksheet.batch_clear([f"A{data_start_row}:A{clear_through}"]),
            description=f"Clear {worksheet_name} column A",
        )

    if ids:
        # The source sheet's ID column stores numbers; MATCH/XLOOKUP/INDEX
        # do exact-type matching, so these must go in as numbers too or
        # every lookup formula in this tab breaks.
        sheet_call(
            lambda: worksheet.update(
                f"A{data_start_row}:A{new_last_row}",
                [[int(item_id)] for item_id in ids],
                value_input_option="RAW",
            ),
            description=f"Write {worksheet_name} column A",
        )

    # If the ID list shrank, column A above already blanked the leftover
    # rows, but their B..last_formula_column cells still hold formulas
    # keyed off what's now a blank A - left alone they'd just show #N/A
    # forever instead of being removed.
    leftover_through = max(existing_last_row, formula_last_row)
    if leftover_through > new_last_row:
        sheet_call(
            lambda: worksheet.batch_clear(
                [f"B{new_last_row + 1}:{last_formula_column}{leftover_through}"]
            ),
            description=f"Clear {worksheet_name} leftover formulas",
        )

    added_rows = new_last_row - formula_last_row
    if added_rows > 0 and formula_last_row >= data_start_row:
        copy_request = {
            "requests": [
                {
                    "copyPaste": {
                        "source": {
                            "sheetId": worksheet.id,
                            "startRowIndex": formula_last_row - 1,
                            "endRowIndex": formula_last_row,
                            "startColumnIndex": 1,
                            "endColumnIndex": last_col_index,
                        },
                        "destination": {
                            "sheetId": worksheet.id,
                            "startRowIndex": formula_last_row,
                            "endRowIndex": new_last_row,
                            "startColumnIndex": 1,
                            "endColumnIndex": last_col_index,
                        },
                        "pasteType": "PASTE_NORMAL",
                    }
                }
            ]
        }
        sheet_call(
            lambda: spreadsheet.batch_update(copy_request),
            description=f"Copy {worksheet_name} formulas down for new rows",
        )

    safe_print(
        f"{worksheet_name} tab: wrote {len(ids):,} IDs to column A, "
        f"copied formulas into {max(0, added_rows):,} new row(s)."
    )


def sync_players_tab(spreadsheet, playerdata_ids: list[str]) -> None:
    sync_lookup_tab(
        spreadsheet,
        PLAYERS_WORKSHEET,
        playerdata_ids,
        data_start_row=PLAYERS_DATA_START_ROW,
        last_formula_column=PLAYERS_LAST_FORMULA_COLUMN,
    )


def sync_leagues_tab(spreadsheet, competition_ids: list[str]) -> None:
    sync_lookup_tab(
        spreadsheet,
        LEAGUES_WORKSHEET,
        competition_ids,
        data_start_row=LEAGUES_DATA_START_ROW,
        last_formula_column=LEAGUES_LAST_FORMULA_COLUMN,
    )


def sync_club_tab(spreadsheet, clubdata_ids: list[str]) -> None:
    sync_lookup_tab(
        spreadsheet,
        CLUB_WORKSHEET,
        clubdata_ids,
        data_start_row=CLUB_DATA_START_ROW,
        last_formula_column=CLUB_LAST_FORMULA_COLUMN,
    )


def sync_matches_tab(spreadsheet, matchdata_ids: list[str]) -> None:
    sync_lookup_tab(
        spreadsheet,
        MATCHES_WORKSHEET,
        matchdata_ids,
        data_start_row=MATCHES_DATA_START_ROW,
        last_formula_column=MATCHES_LAST_FORMULA_COLUMN,
    )


def value_at(row: list[Any], index: int) -> Any:
    return row[index] if len(row) > index else ""


def parse_sheet_date(value: Any) -> date | None:
    if value in ("", None):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        # Google Sheets stores dates as days since 1899-12-30.
        return date(1899, 12, 30) + timedelta(days=int(value))

    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    elif " " in text and text[:10].count("-") == 2:
        text = text.split(" ", 1)[0]

    for date_format in (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%d %b %Y",
        "%d %B %Y",
    ):
        try:
            return datetime.strptime(text, date_format).date()
        except ValueError:
            continue
    return None


def normalize_sheet_value(value: Any) -> str:
    text = str(value).strip() if value not in (None, "") else ""
    try:
        number = float(text)
    except ValueError:
        return " ".join(text.split()).casefold()
    if number.is_integer():
        return str(int(number))
    return str(number)


def individual_result_key(values: tuple[Any, Any, Any, Any, Any]) -> tuple[str, str, str, str, str] | None:
    match_date = parse_sheet_date(values[0])
    if not match_date:
        return None
    return (
        match_date.isoformat(),
        normalize_sheet_value(values[1]),
        normalize_sheet_value(values[2]),
        normalize_sheet_value(values[3]),
        normalize_sheet_value(values[4]),
    )


def row_has_formula(row: list[Any]) -> bool:
    return any(str(value).startswith("=") for value in row)


def ensure_individual_result_formula_rows(
    destination_spreadsheet,
    destination,
    start_row: int,
    end_row: int,
) -> int:
    if end_row > destination.row_count:
        sheet_call(
            lambda: destination.add_rows(end_row - destination.row_count),
            description="Grow Individual Results tab",
        )

    last_col = column_letters(destination.col_count)
    formula_values = sheet_call(
        lambda: destination.get(
            f"A1:{last_col}{destination.row_count}",
            value_render_option="FORMULA",
        ),
        description="Read Individual Results formulas",
    )

    formula_last_row = start_row - 1
    for offset in range(start_row, min(end_row, len(formula_values)) + 1):
        if row_has_formula(formula_values[offset - 1]):
            formula_last_row = offset
        else:
            break

    if formula_last_row >= end_row:
        return 0

    source_row = formula_last_row
    if source_row < 1:
        return 0

    copy_start_row = max(formula_last_row + 1, start_row)
    if copy_start_row > end_row:
        return 0

    copy_request = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": destination.id,
                        "startRowIndex": source_row - 1,
                        "endRowIndex": source_row,
                        "startColumnIndex": 0,
                        "endColumnIndex": destination.col_count,
                    },
                    "destination": {
                        "sheetId": destination.id,
                        "startRowIndex": copy_start_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": 0,
                        "endColumnIndex": destination.col_count,
                    },
                    "pasteType": "PASTE_NORMAL",
                }
            }
        ]
    }
    sheet_call(
        lambda: destination_spreadsheet.batch_update(copy_request),
        description="Copy Individual Results formulas down",
    )
    return end_row - copy_start_row + 1


def sync_individual_results(source_spreadsheet, args: argparse.Namespace) -> None:
    source_matches = source_spreadsheet.worksheet(MATCHES_WORKSHEET)
    source_matchdata = source_spreadsheet.worksheet(args.match_worksheet)

    matches_values = sheet_call(
        lambda: source_matches.get("A:Q", value_render_option="FORMATTED_VALUE"),
        description="Read Matches tab for Individual Results",
    )
    matchdata_values = read_existing_values(
        source_matchdata, "Read MatchData for Individual Results finished statuses"
    )
    if not matches_values or not matchdata_values:
        safe_print("Individual Results: source Matches/MatchData is empty; nothing to copy.")
        return

    matchdata_header = matchdata_values[0]
    match_id_col = matchdata_header.index("Match ID")
    finished_col = matchdata_header.index("Finished")
    finished_ids: set[str] = set()
    for row in matchdata_values[1:]:
        match_id = normalize_sheet_value(value_at(row, match_id_col))
        finished = normalize_sheet_value(value_at(row, finished_col))
        if match_id and finished in {"1", "true", "yes", "finished"}:
            finished_ids.add(match_id)

    destination_spreadsheet = open_spreadsheet(
        args.individual_results_spreadsheet_id, args.credentials
    )
    destination = get_or_create_worksheet(
        destination_spreadsheet, args.individual_results_worksheet, 10
    )
    destination_values = sheet_call(
        lambda: destination.get_all_values(value_render_option="FORMATTED_VALUE"),
        description="Read Individual Results tab",
    )

    dated_destination_rows: list[tuple[date, int, list[Any]]] = []
    for offset, row in enumerate(destination_values[1:], start=2):
        row_date = parse_sheet_date(value_at(row, 1))
        if row_date:
            dated_destination_rows.append((row_date, offset, row))
    max_date = max((row_date for row_date, _, _ in dated_destination_rows), default=None)

    existing_on_max_date: set[tuple[str, str, str, str, str]] = set()
    max_date_last_row = len(destination_values)
    if max_date:
        max_date_last_row = max(
            row_number for row_date, row_number, _ in dated_destination_rows if row_date == max_date
        )
        for row_date, _, row in dated_destination_rows:
            if row_date != max_date:
                continue
            key = individual_result_key(
                (
                    value_at(row, 1),  # B: Date
                    value_at(row, 2),  # C: Home
                    value_at(row, 5),  # F: Away
                    value_at(row, 8),  # I: Home score
                    value_at(row, 9),  # J: Away score
                )
            )
            if key:
                existing_on_max_date.add(key)

    today = datetime.now().date()
    skipped_future = 0
    rows_to_add: list[tuple[date, tuple[Any, Any, Any, Any, Any]]] = []
    for row in matches_values[MATCHES_DATA_START_ROW - 1 :]:
        match_id = normalize_sheet_value(value_at(row, 0))
        if match_id not in finished_ids:
            continue

        result_values = (
            value_at(row, 1),   # B: Date
            value_at(row, 11),  # L: HomeAlt
            value_at(row, 14),  # O: AwayAlt
            value_at(row, 15),  # P: HomeS
            value_at(row, 16),  # Q: AwayS
        )
        key = individual_result_key(result_values)
        if not key:
            continue

        match_date = parse_sheet_date(result_values[0])
        if match_date > today:
            skipped_future += 1
            continue
        if max_date is None or match_date > max_date:
            rows_to_add.append((match_date, result_values))
        elif match_date == max_date and key not in existing_on_max_date:
            rows_to_add.append((match_date, result_values))

    if not rows_to_add:
        max_date_text = max_date.isoformat() if max_date else "no existing date"
        future_text = (
            f" Skipped {skipped_future:,} future-dated finished match(es)."
            if skipped_future
            else ""
        )
        safe_print(f"Individual Results: no new finished matches after {max_date_text}.{future_text}")
        return

    rows_to_add.sort(key=lambda item: item[0])
    start_row = max(1, max_date_last_row + 1)
    end_row = start_row + len(rows_to_add) - 1
    copied_formula_rows = ensure_individual_result_formula_rows(
        destination_spreadsheet, destination, start_row, end_row
    )

    updates = []
    for offset, (match_date, values) in enumerate(rows_to_add):
        row_number = start_row + offset
        updates.extend(
            [
                {"range": f"B{row_number}", "values": [[match_date.strftime("%d/%m/%Y")]]},
                {"range": f"C{row_number}", "values": [[values[1]]]},
                {"range": f"F{row_number}", "values": [[values[2]]]},
                {"range": f"I{row_number}", "values": [[values[3]]]},
                {"range": f"J{row_number}", "values": [[values[4]]]},
            ]
        )

    for start in range(0, len(updates), args.sheet_batch_size * 5):
        chunk = updates[start : start + args.sheet_batch_size * 5]
        sheet_call(
            lambda chunk=chunk: destination.batch_update(
                [dict(item) for item in chunk], value_input_option="USER_ENTERED"
            ),
            description="Write Individual Results rows",
        )

    max_date_text = max_date.isoformat() if max_date else "no existing date"
    safe_print(
        f"Individual Results: wrote {len(rows_to_add):,} finished match(es) "
        f"starting under {max_date_text}; copied formulas into "
        f"{copied_formula_rows:,} row(s); skipped {skipped_future:,} future-dated "
        "finished match(es)."
    )


def fetch_manager_row(manager_id: str, args: argparse.Namespace) -> list[Any]:
    data = gmgr.fetch_manager(
        manager_id, retries=args.manager_retries, request_delay=args.manager_request_delay
    )
    row_dict = gmgr.build_row(data, manager_id)
    # The sheet's existing "Manager ID" column holds numbers; build_row
    # returns it as a string, and writing that back as text would break
    # any exact-match lookup against this column (same trap as Players!A).
    row_dict["Manager ID"] = int(row_dict.get("Manager ID") or manager_id)
    return [row_dict.get(header, "") for header in gmgr.HEADERS]


def sync_managers(spreadsheet, args: argparse.Namespace) -> list[tuple[str, str]]:
    manager_ids = gmgr.load_ids(args.manager_input)
    safe_print(f"Loaded {len(manager_ids):,} manager IDs from {args.manager_input}")

    worksheet = get_or_create_worksheet(spreadsheet, args.manager_worksheet, len(gmgr.HEADERS))
    existing_values = read_existing_values(worksheet, "Read ManagerData sheet")

    if not existing_values:
        sheet_call(lambda: worksheet.update([gmgr.HEADERS]), description="Write ManagerData header")
        existing_values = [gmgr.HEADERS]

    if existing_values[0] != gmgr.HEADERS:
        raise SystemExit(
            "ManagerData's header row doesn't match getManagers.py's current "
            "HEADERS layout. Clear the sheet or fix the header before syncing."
        )

    manager_id_col = gmgr.HEADERS.index("Manager ID")
    existing_by_id: dict[str, int] = {}
    for offset, row in enumerate(existing_values[1:]):
        manager_id = str(row[manager_id_col]).strip() if len(row) > manager_id_col else ""
        if manager_id:
            existing_by_id[manager_id] = offset + 2  # +2: header row + 1-based

    # Managers have no per-season history to protect, so every ID (new or
    # already in the sheet) just gets fetched fresh and fully overwritten.
    appended_rows: list[list[Any]] = []
    updates: list[tuple[int, list[Any]]] = []
    errors: list[tuple[str, str]] = []

    jobs = {}
    with ThreadPoolExecutor(max_workers=max(1, args.manager_workers)) as executor:
        for manager_id in manager_ids:
            jobs[executor.submit(fetch_manager_row, manager_id, args)] = manager_id

        total = len(jobs)
        done = 0
        for future in as_completed(jobs):
            manager_id = jobs[future]
            done += 1
            try:
                row = future.result()
            except Exception as exc:
                errors.append((manager_id, str(exc)))
                safe_print(f"[{done}/{total}] ERROR (manager) {manager_id}: {exc}")
                continue
            if manager_id in existing_by_id:
                updates.append((existing_by_id[manager_id], row))
            else:
                appended_rows.append(row)
            safe_print(f"[{done}/{total}] OK (manager) {manager_id} - {row[1]}")

    last_col = column_letters(len(gmgr.HEADERS))
    if updates:
        for start in range(0, len(updates), args.sheet_batch_size):
            chunk = updates[start : start + args.sheet_batch_size]
            body = [
                {"range": f"A{row_number}:{last_col}{row_number}", "values": [row]}
                for row_number, row in chunk
            ]
            sheet_call(
                # batch_update mutates its input (prefixes "range" with the
                # worksheet title) - pass fresh copies so a retry after a
                # transient failure doesn't re-prefix an already-prefixed range.
                lambda body=body: worksheet.batch_update(
                    [dict(item) for item in body], value_input_option="RAW"
                ),
                description=f"Update manager rows {start + 1}-{start + len(chunk)}",
            )
            safe_print(f"Updated {min(start + len(chunk), len(updates))}/{len(updates)} existing managers in the sheet")

    if appended_rows:
        for start in range(0, len(appended_rows), args.sheet_batch_size):
            chunk = appended_rows[start : start + args.sheet_batch_size]
            sheet_call(
                lambda chunk=chunk: worksheet.append_rows(chunk, value_input_option="RAW"),
                description=f"Append manager rows {start + 1}-{start + len(chunk)}",
            )
            safe_print(f"Appended {min(start + len(chunk), len(appended_rows))}/{len(appended_rows)} new managers to the sheet")

    final_by_id: dict[str, list[Any]] = {
        manager_id: list(existing_values[row_number - 1])
        for manager_id, row_number in existing_by_id.items()
    }
    for row_number, row in updates:
        final_by_id[str(row[manager_id_col])] = row
    for row in appended_rows:
        final_by_id[str(row[manager_id_col])] = row

    ordered_ids = [mid for mid in manager_ids if mid in final_by_id]
    tmp_path = args.manager_csv_output.with_suffix(args.manager_csv_output.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(gmgr.HEADERS)
        writer.writerows(final_by_id[mid] for mid in ordered_ids)
    tmp_path.replace(args.manager_csv_output)

    if errors:
        write_errors(args.manager_errors, errors)

    safe_print(
        f"ManagerData done. {len(appended_rows):,} new managers added, "
        f"{len(updates):,} existing managers refreshed, {len(errors):,} failed."
    )
    return errors


def fetch_competition_row(competition_id: str, args: argparse.Namespace) -> list[Any]:
    data = gcomp.fetch_competition(
        competition_id,
        season=args.competition_season,
        retries=args.competition_retries,
        request_delay=args.competition_request_delay,
    )
    row_dict = gcomp.competition_row(competition_id, data)
    # CompetitionData!A stores IDs as numbers; keep the type consistent
    # with the sheet (same trap as Players!A and ManagerData!A).
    row_dict["Competition ID"] = int(row_dict.get("Competition ID") or competition_id)
    return [row_dict.get(header, "") for header in gcomp.HEADERS]


def sync_competitions(spreadsheet, args: argparse.Namespace) -> list[tuple[str, str]]:
    competition_ids = gcomp.load_ids(args.competition_input)
    safe_print(f"Loaded {len(competition_ids):,} competition IDs from {args.competition_input}")

    worksheet = get_or_create_worksheet(spreadsheet, args.competition_worksheet, len(gcomp.HEADERS))
    existing_values = read_existing_values(worksheet, "Read CompetitionData sheet")

    if not existing_values:
        sheet_call(lambda: worksheet.update([gcomp.HEADERS]), description="Write CompetitionData header")
        existing_values = [gcomp.HEADERS]

    if existing_values[0] != gcomp.HEADERS:
        raise SystemExit(
            "CompetitionData's header row doesn't match getCompetitions.py's "
            "current HEADERS layout. Clear the sheet or fix the header before syncing."
        )

    competition_id_col = gcomp.HEADERS.index("Competition ID")
    existing_by_id: dict[str, int] = {}
    for offset, row in enumerate(existing_values[1:]):
        competition_id = str(row[competition_id_col]).strip() if len(row) > competition_id_col else ""
        if competition_id:
            existing_by_id[competition_id] = offset + 2  # +2: header row + 1-based

    # Competitions have no per-season history to protect either - every ID
    # (new or already in the sheet) gets fetched fresh and fully overwritten.
    appended_rows: list[list[Any]] = []
    updates: list[tuple[int, list[Any]]] = []
    errors: list[tuple[str, str]] = []

    jobs = {}
    with ThreadPoolExecutor(max_workers=max(1, args.competition_workers)) as executor:
        for competition_id in competition_ids:
            jobs[executor.submit(fetch_competition_row, competition_id, args)] = competition_id

        total = len(jobs)
        done = 0
        for future in as_completed(jobs):
            competition_id = jobs[future]
            done += 1
            try:
                row = future.result()
            except Exception as exc:
                errors.append((competition_id, str(exc)))
                safe_print(f"[{done}/{total}] ERROR (competition) {competition_id}: {exc}")
                continue
            if competition_id in existing_by_id:
                updates.append((existing_by_id[competition_id], row))
            else:
                appended_rows.append(row)
            safe_print(f"[{done}/{total}] OK (competition) {competition_id} - {row[1]}")

    last_col = column_letters(len(gcomp.HEADERS))
    if updates:
        for start in range(0, len(updates), args.sheet_batch_size):
            chunk = updates[start : start + args.sheet_batch_size]
            body = [
                {"range": f"A{row_number}:{last_col}{row_number}", "values": [row]}
                for row_number, row in chunk
            ]
            sheet_call(
                lambda body=body: worksheet.batch_update(
                    [dict(item) for item in body], value_input_option="RAW"
                ),
                description=f"Update competition rows {start + 1}-{start + len(chunk)}",
            )
            safe_print(f"Updated {min(start + len(chunk), len(updates))}/{len(updates)} existing competitions in the sheet")

    if appended_rows:
        for start in range(0, len(appended_rows), args.sheet_batch_size):
            chunk = appended_rows[start : start + args.sheet_batch_size]
            sheet_call(
                lambda chunk=chunk: worksheet.append_rows(chunk, value_input_option="RAW"),
                description=f"Append competition rows {start + 1}-{start + len(chunk)}",
            )
            safe_print(f"Appended {min(start + len(chunk), len(appended_rows))}/{len(appended_rows)} new competitions to the sheet")

    final_by_id: dict[str, list[Any]] = {
        competition_id: list(existing_values[row_number - 1])
        for competition_id, row_number in existing_by_id.items()
    }
    for row_number, row in updates:
        final_by_id[str(row[competition_id_col])] = row
    for row in appended_rows:
        final_by_id[str(row[competition_id_col])] = row

    ordered_ids = [cid for cid in competition_ids if cid in final_by_id]
    tmp_path = args.competition_csv_output.with_suffix(args.competition_csv_output.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(gcomp.HEADERS)
        writer.writerows(final_by_id[cid] for cid in ordered_ids)
    tmp_path.replace(args.competition_csv_output)

    if errors:
        write_errors(args.competition_errors, errors)

    safe_print(
        f"CompetitionData done. {len(appended_rows):,} new competitions added, "
        f"{len(updates):,} existing competitions refreshed, {len(errors):,} failed."
    )
    return errors


def write_errors(path: Path, errors: list[tuple[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["ID", "Error"])
        writer.writerows(errors)


def sync_matches(spreadsheet, args: argparse.Namespace) -> list[tuple[str, str, str]]:
    club_ids = gmatch.load_ids(args.club_input)
    safe_print(f"Loaded {len(club_ids):,} club IDs from {args.club_input}")

    worksheet = get_or_create_worksheet(spreadsheet, args.match_worksheet, len(gmatch.HEADERS))
    existing_values = read_existing_values(worksheet, "Read MatchData sheet")

    if not existing_values:
        sheet_call(lambda: worksheet.update([gmatch.HEADERS]), description="Write MatchData header")
        existing_values = [gmatch.HEADERS]

    if existing_values[0] != gmatch.HEADERS:
        raise SystemExit(
            "MatchData's header row doesn't match getMatches.py's current "
            "HEADERS layout. Clear the sheet or fix the header before syncing."
        )

    header_len = len(gmatch.HEADERS)
    match_id_col = gmatch.HEADERS.index("Match ID")
    match_utc_col = gmatch.HEADERS.index("Match UTC")

    existing_by_id: dict[str, int] = {}
    cached: dict[str, dict[str, Any]] = {}
    cached_all: dict[str, dict[str, Any]] = {}
    for offset, row in enumerate(existing_values[1:]):
        padded = row + [""] * (header_len - len(row))
        match_id = str(padded[match_id_col]).strip()
        if not match_id:
            continue
        existing_by_id[match_id] = offset + 2  # +2: header row + 1-based
        row_dict = dict(zip(gmatch.HEADERS, padded))
        cached_all[match_id] = row_dict
        if str(row_dict.get("Detailed Data", "")) == "1":
            cached[match_id] = row_dict

    safe_print(
        f"MatchData: {len(existing_by_id):,} matches already in the sheet, "
        f"{len(cached):,} already fully detailed."
    )

    matches, errors = gmatch.collect_matches(
        club_ids,
        mode=args.match_mode,
        from_date=args.match_from_date,
        to_date=args.match_to_date,
        all_seasons=args.match_all_seasons,
        club_workers=args.match_club_workers,
        detail_workers=args.match_detail_workers,
        request_delay=args.match_request_delay,
        retries=args.match_retries,
        cached=cached,
        cached_all=cached_all,
    )

    today = datetime.now(timezone.utc).date().isoformat()

    updates: list[tuple[int, list[Any]]] = []
    appended_rows: list[list[Any]] = []
    skipped_not_due = 0
    for match_id, row_dict in matches.items():
        row = [row_dict.get(header, "") for header in gmatch.HEADERS]
        if match_id in existing_by_id:
            # An existing row is only worth rewriting if it's not finished
            # yet AND its date has already passed - i.e. it's actually due
            # for a status/result update. A future fixture that's already
            # round-filled has nothing left to change until its date
            # arrives, and a finished match never changes again.
            existing_row = cached_all.get(match_id, {})
            already_finished = str(existing_row.get("Finished", "")) == "1"
            match_date = str(existing_row.get("Match UTC", ""))[:10]
            not_due_yet = bool(match_date) and match_date > today
            if already_finished or not_due_yet:
                skipped_not_due += 1
                continue
            updates.append((existing_by_id[match_id], row))
        else:
            appended_rows.append(row)

    if skipped_not_due:
        safe_print(f"Skipped {skipped_not_due:,} matches that are finished or not due yet (nothing to update).")

    last_col = column_letters(header_len)
    if updates:
        for start in range(0, len(updates), args.sheet_batch_size):
            chunk = updates[start : start + args.sheet_batch_size]
            body = [
                {"range": f"A{row_number}:{last_col}{row_number}", "values": [row]}
                for row_number, row in chunk
            ]
            sheet_call(
                lambda body=body: worksheet.batch_update(
                    [dict(item) for item in body], value_input_option="RAW"
                ),
                description=f"Update match rows {start + 1}-{start + len(chunk)}",
            )
            safe_print(f"Updated {min(start + len(chunk), len(updates))}/{len(updates)} existing matches in the sheet")

    if appended_rows:
        for start in range(0, len(appended_rows), args.sheet_batch_size):
            chunk = appended_rows[start : start + args.sheet_batch_size]
            sheet_call(
                lambda chunk=chunk: worksheet.append_rows(chunk, value_input_option="RAW"),
                description=f"Append match rows {start + 1}-{start + len(chunk)}",
            )
            safe_print(f"Appended {min(start + len(chunk), len(appended_rows))}/{len(appended_rows)} new matches to the sheet")

    # Rebuild the CSV mirror from the full dataset - untouched existing rows
    # (anything outside this run's current-season scope) plus this run's
    # updates/appends - sorted the same way getMatches.py does.
    final_by_id: dict[str, list[Any]] = {
        match_id: [row_dict.get(header, "") for header in gmatch.HEADERS]
        for match_id, row_dict in cached_all.items()
    }
    for row_number, row in updates:
        final_by_id[str(row[match_id_col])] = row
    for row in appended_rows:
        final_by_id[str(row[match_id_col])] = row

    all_rows = list(final_by_id.values())
    all_rows.sort(key=lambda r: (str(r[match_utc_col] or ""), int(r[match_id_col] or 0)))
    tmp_path = args.match_csv_output.with_suffix(args.match_csv_output.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(gmatch.HEADERS)
        writer.writerows(all_rows)
    tmp_path.replace(args.match_csv_output)

    if errors:
        gmatch.write_errors(args.match_errors, errors)

    safe_print(
        f"MatchData done. {len(appended_rows):,} new matches added, "
        f"{len(updates):,} existing matches refreshed, {len(errors):,} failed."
    )
    return errors


def fetch_club_row(club_id: str, args: argparse.Namespace) -> list[Any]:
    row = gclub.build_row(club_id, retries=args.club_retries, request_delay=args.club_request_delay)
    club_id_col = gclub.HEADERS.index("Club ID")
    # ClubData!A stores IDs as numbers (same trap as the other tabs).
    row[club_id_col] = int(row[club_id_col] or club_id)
    return row


def sync_clubs(spreadsheet, args: argparse.Namespace) -> list[tuple[str, str]]:
    club_ids = gclub.load_ids(args.club_input)
    safe_print(f"Loaded {len(club_ids):,} club IDs from {args.club_input}")

    worksheet = get_or_create_worksheet(spreadsheet, args.club_worksheet, len(gclub.HEADERS))
    existing_values = read_existing_values(worksheet, "Read ClubData sheet")

    if not existing_values:
        sheet_call(lambda: worksheet.update([gclub.HEADERS]), description="Write ClubData header")
        existing_values = [gclub.HEADERS]

    if existing_values[0] != gclub.HEADERS:
        raise SystemExit(
            "ClubData's header row doesn't match getClubs.py's current "
            "HEADERS layout. Clear the sheet or fix the header before syncing."
        )

    club_id_col = gclub.HEADERS.index("Club ID")
    existing_by_id: dict[str, int] = {}
    for offset, row in enumerate(existing_values[1:]):
        club_id = str(row[club_id_col]).strip() if len(row) > club_id_col else ""
        if club_id:
            existing_by_id[club_id] = offset + 2  # +2: header row + 1-based

    # Clubs have no per-season history to protect either - every ID (new or
    # already in the sheet) gets fetched fresh and fully overwritten.
    appended_rows: list[list[Any]] = []
    updates: list[tuple[int, list[Any]]] = []
    errors: list[tuple[str, str]] = []

    jobs = {}
    with ThreadPoolExecutor(max_workers=max(1, args.club_workers)) as executor:
        for club_id in club_ids:
            jobs[executor.submit(fetch_club_row, club_id, args)] = club_id

        total = len(jobs)
        done = 0
        for future in as_completed(jobs):
            club_id = jobs[future]
            done += 1
            try:
                row = future.result()
            except Exception as exc:
                errors.append((club_id, str(exc)))
                safe_print(f"[{done}/{total}] ERROR (club) {club_id}: {exc}")
                continue
            if club_id in existing_by_id:
                updates.append((existing_by_id[club_id], row))
            else:
                appended_rows.append(row)
            safe_print(f"[{done}/{total}] OK (club) {club_id} - {row[1]}")

    last_col = column_letters(len(gclub.HEADERS))
    if updates:
        for start in range(0, len(updates), args.sheet_batch_size):
            chunk = updates[start : start + args.sheet_batch_size]
            body = [
                {"range": f"A{row_number}:{last_col}{row_number}", "values": [row]}
                for row_number, row in chunk
            ]
            sheet_call(
                lambda body=body: worksheet.batch_update(
                    [dict(item) for item in body], value_input_option="RAW"
                ),
                description=f"Update club rows {start + 1}-{start + len(chunk)}",
            )
            safe_print(f"Updated {min(start + len(chunk), len(updates))}/{len(updates)} existing clubs in the sheet")

    if appended_rows:
        for start in range(0, len(appended_rows), args.sheet_batch_size):
            chunk = appended_rows[start : start + args.sheet_batch_size]
            sheet_call(
                lambda chunk=chunk: worksheet.append_rows(chunk, value_input_option="RAW"),
                description=f"Append club rows {start + 1}-{start + len(chunk)}",
            )
            safe_print(f"Appended {min(start + len(chunk), len(appended_rows))}/{len(appended_rows)} new clubs to the sheet")

    final_by_id: dict[str, list[Any]] = {
        club_id: list(existing_values[row_number - 1])
        for club_id, row_number in existing_by_id.items()
    }
    for row_number, row in updates:
        final_by_id[str(row[club_id_col])] = row
    for row in appended_rows:
        final_by_id[str(row[club_id_col])] = row

    ordered_ids = [cid for cid in club_ids if cid in final_by_id]
    tmp_path = args.club_csv_output.with_suffix(args.club_csv_output.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(gclub.HEADERS)
        writer.writerows(final_by_id[cid] for cid in ordered_ids)
    tmp_path.replace(args.club_csv_output)

    if errors:
        write_errors(args.club_errors, errors)

    safe_print(
        f"ClubData done. {len(appended_rows):,} new clubs added, "
        f"{len(updates):,} existing clubs refreshed, {len(errors):,} failed."
    )
    return errors


def sync_player_data(spreadsheet, args: argparse.Namespace) -> list[tuple[str, str]]:
    player_ids = gp.load_player_ids(args.input)
    safe_print(f"Loaded {len(player_ids):,} player IDs from {args.input}")

    worksheet = get_or_create_worksheet(spreadsheet, args.worksheet, len(gp.HEADERS))
    existing_values = read_existing_values(worksheet, "Read sheet")

    if not existing_values:
        sheet_call(lambda: worksheet.update([gp.HEADERS]), description="Write header")
        existing_values = [gp.HEADERS]

    header_row = existing_values[0]
    if header_row != gp.HEADERS:
        raise SystemExit(
            "The sheet's header row doesn't match the current column layout "
            "(YEARS/SEASON_FIELDS/EXTRA_HEADERS may have changed since it was "
            "last written). Clear the sheet or fix the header before syncing."
        )

    existing_by_id: dict[str, tuple[int, list[str]]] = {}
    for offset, row in enumerate(existing_values[1:]):
        player_id = str(row[ID_COL]).strip() if len(row) > ID_COL else ""
        if player_id:
            existing_by_id[player_id] = (offset + 2, row)  # +2: header row + 1-based

    new_ids = [pid for pid in player_ids if pid not in existing_by_id]
    existing_ids = [pid for pid in player_ids if pid in existing_by_id]
    safe_print(f"New players: {len(new_ids):,}; existing players to refresh: {len(existing_ids):,}")

    appended_rows: list[list[Any]] = []
    updates: list[tuple[int, list[Any]]] = []
    errors: list[tuple[str, str]] = []

    jobs = {}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        for player_id in new_ids:
            jobs[executor.submit(fetch_new, player_id, args)] = ("new", player_id)
        for player_id in existing_ids:
            _, existing_row = existing_by_id[player_id]
            jobs[executor.submit(fetch_existing, player_id, existing_row, args)] = (
                "existing",
                player_id,
            )

        total = len(jobs)
        done = 0
        for future in as_completed(jobs):
            kind, player_id = jobs[future]
            done += 1
            try:
                row = future.result()
            except Exception as exc:
                errors.append((player_id, str(exc)))
                safe_print(f"[{done}/{total}] ERROR ({kind}) {player_id}: {exc}")
                continue
            if kind == "new":
                appended_rows.append(row)
            else:
                row_number, _ = existing_by_id[player_id]
                updates.append((row_number, row))
            safe_print(f"[{done}/{total}] OK ({kind}) {player_id} - {row[1]}")

    # Push updates to existing rows first, then append brand-new rows.
    last_col = column_letters(len(gp.HEADERS))
    if updates:
        for start in range(0, len(updates), args.sheet_batch_size):
            chunk = updates[start : start + args.sheet_batch_size]
            body = [
                {"range": f"A{row_number}:{last_col}{row_number}", "values": [row]}
                for row_number, row in chunk
            ]
            sheet_call(
                lambda body=body: worksheet.batch_update(
                    [dict(item) for item in body], value_input_option="RAW"
                ),
                description=f"Update rows {start + 1}-{start + len(chunk)}",
            )
            safe_print(f"Updated {min(start + len(chunk), len(updates))}/{len(updates)} existing rows in the sheet")

    if appended_rows:
        for start in range(0, len(appended_rows), args.sheet_batch_size):
            chunk = appended_rows[start : start + args.sheet_batch_size]
            sheet_call(
                lambda chunk=chunk: worksheet.append_rows(chunk, value_input_option="RAW"),
                description=f"Append rows {start + 1}-{start + len(chunk)}",
            )
            safe_print(f"Appended {min(start + len(chunk), len(appended_rows))}/{len(appended_rows)} new rows to the sheet")

    # Rebuild the CSV mirror from the same in-memory dataset (existing rows +
    # this run's updates/appends) so it always matches the sheet.
    final_by_id: dict[str, list[Any]] = {
        player_id: list(row) for player_id, (_, row) in existing_by_id.items()
    }
    for row_number, row in updates:
        final_by_id[str(row[ID_COL])] = row
    for row in appended_rows:
        final_by_id[str(row[ID_COL])] = row

    ordered_ids = [pid for pid in player_ids if pid in final_by_id]
    tmp_path = args.csv_output.with_suffix(args.csv_output.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(gp.HEADERS)
        writer.writerows(final_by_id[pid] for pid in ordered_ids)
    tmp_path.replace(args.csv_output)

    if errors:
        write_errors(args.errors, errors)

    safe_print(
        f"Done. {len(appended_rows):,} new players added, "
        f"{len(updates):,} existing players refreshed, {len(errors):,} failed."
    )
    safe_print(f"Sheet: https://docs.google.com/spreadsheets/d/{args.spreadsheet_id}")
    safe_print(f"CSV: {args.csv_output}")
    return errors


def main() -> int:
    args = parse_args()
    args.input = args.input.resolve()
    args.csv_output = args.csv_output.resolve()
    args.errors = args.errors.resolve()
    args.credentials = args.credentials.resolve()
    args.manager_input = args.manager_input.resolve()
    args.manager_csv_output = args.manager_csv_output.resolve()
    args.manager_errors = args.manager_errors.resolve()
    args.competition_input = args.competition_input.resolve()
    args.competition_csv_output = args.competition_csv_output.resolve()
    args.competition_errors = args.competition_errors.resolve()
    args.club_input = args.club_input.resolve()
    args.match_csv_output = args.match_csv_output.resolve()
    args.match_errors = args.match_errors.resolve()
    args.club_csv_output = args.club_csv_output.resolve()
    args.club_errors = args.club_errors.resolve()

    if args.only_individual_results:
        spreadsheet = open_spreadsheet(args.spreadsheet_id, args.credentials)
        sync_individual_results(spreadsheet, args)
        return 0

    spreadsheet = open_spreadsheet(args.spreadsheet_id, args.credentials)
    errors: list[tuple[str, str]] = []

    if args.skip_players:
        safe_print("Skipping PlayerData sync.")
    else:
        errors = sync_player_data(spreadsheet, args)
        if not args.skip_players_tab:
            player_worksheet = spreadsheet.worksheet(args.worksheet)
            playerdata_ids = id_column_values(player_worksheet, "Read final PlayerData IDs")[1:]
            sync_players_tab(spreadsheet, playerdata_ids)
    if errors:
        safe_print(f"Errors: {args.errors}")

    manager_errors: list[tuple[str, str]] = []
    if not args.skip_managers:
        manager_errors = sync_managers(spreadsheet, args)

    competition_errors: list[tuple[str, str]] = []
    if not args.skip_competitions:
        competition_errors = sync_competitions(spreadsheet, args)
        if not args.skip_leagues_tab:
            competition_worksheet = spreadsheet.worksheet(args.competition_worksheet)
            competition_ids = id_column_values(
                competition_worksheet, "Read final CompetitionData IDs"
            )[1:]
            sync_leagues_tab(spreadsheet, competition_ids)

    match_errors: list[tuple[str, str, str]] = []
    if not args.skip_matches:
        match_errors = sync_matches(spreadsheet, args)
        if not args.skip_matches_tab:
            match_worksheet = spreadsheet.worksheet(args.match_worksheet)
            matchdata_ids = id_column_values(match_worksheet, "Read final MatchData IDs")[1:]
            sync_matches_tab(spreadsheet, matchdata_ids)

    if not args.skip_individual_results:
        if args.skip_matches:
            safe_print(
                "Individual Results: MatchData sync was skipped; copying from current sheet values."
            )
        elif args.skip_matches_tab:
            safe_print(
                "Individual Results: Matches tab sync was skipped; copying from current Matches tab values."
            )
        sync_individual_results(spreadsheet, args)

    club_errors: list[tuple[str, str]] = []
    if not args.skip_clubs:
        club_errors = sync_clubs(spreadsheet, args)
        if not args.skip_club_tab:
            club_worksheet = spreadsheet.worksheet(args.club_worksheet)
            clubdata_ids = id_column_values(club_worksheet, "Read final ClubData IDs")[1:]
            sync_club_tab(spreadsheet, clubdata_ids)

    return (
        0
        if not errors
        and not manager_errors
        and not competition_errors
        and not match_errors
        and not club_errors
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
