#!/usr/bin/env python3
"""
Keep the "PlayerData" Google Sheet (and automodel_players_full.csv) up to date
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
import re
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
CLUB_LAST_FORMULA_COLUMN = "CP"  # every column B..CP looks up ClubData by ID

MATCHES_WORKSHEET = "Matches"
MATCHES_HEADER_ROW = 1
MATCHES_DATA_START_ROW = 2
MATCHES_LAST_FORMULA_COLUMN = "AZ"  # every column B..AZ looks up/derives from MatchData by ID

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
        default=folder / "automodel_players_full.csv",
        help="CSV mirror of the sheet, rewritten each run",
    )
    parser.add_argument(
        "--errors",
        type=Path,
        default=folder / "automodel_sync_errors.csv",
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
        default=folder / "automodel_managers.csv",
        help="CSV mirror of the ManagerData sheet, rewritten each run",
    )
    parser.add_argument(
        "--manager-errors",
        type=Path,
        default=folder / "automodel_manager_sync_errors.csv",
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
        default=folder / "automodel_competitions.csv",
        help="CSV mirror of the CompetitionData sheet, rewritten each run",
    )
    parser.add_argument(
        "--competition-errors",
        type=Path,
        default=folder / "automodel_competition_sync_errors.csv",
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
    parser.add_argument(
        "--matchdata-competition-input",
        type=Path,
        default=folder / "matchdata_competition_ids.txt",
        help=(
            "TXT or CSV of competition IDs to keep in MatchData/Matches - a fetched or "
            "existing match is kept if its Competition ID OR Parent Competition ID is in "
            "this list. If the file is missing, no filtering is applied. Default: "
            "matchdata_competition_ids.txt"
        ),
    )
    parser.add_argument("--match-worksheet", default="MatchData")
    parser.add_argument(
        "--match-csv-output",
        type=Path,
        default=folder / "automodel_matches.csv",
        help="CSV mirror of the MatchData sheet, rewritten each run",
    )
    parser.add_argument(
        "--match-errors",
        type=Path,
        default=folder / "automodel_match_sync_errors.csv",
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
        "--skip-projection-snapshot",
        action="store_true",
        help="Don't freeze not-yet-started matches' ProjH/ProjA/HomeScr/AwayScr and win/draw/away win%% into MatchData's Snap* columns",
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
        default=folder / "automodel_clubs.csv",
        help="CSV mirror of the ClubData sheet, rewritten each run",
    )
    parser.add_argument(
        "--club-errors",
        type=Path,
        default=folder / "automodel_club_sync_errors.csv",
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


def force_ids_text(worksheet, cells: dict[int, dict[int, Any]]) -> None:
    """Force specific (row, column) cells to Plain Text via a raw updateCells
    request. A normal values-API write (RAW or USER_ENTERED) isn't reliable
    for this - Sheets can still auto-detect a numeric-looking string as a
    NUMBER, which silently breaks any exact-type VLOOKUP/MATCH/INDEX lookup
    keyed on that ID later. `cells` maps 1-based row number -> {0-based
    column index: value}."""
    requests = []
    for row_number, col_values in cells.items():
        for col_index, value in col_values.items():
            if value in (None, ""):
                continue
            requests.append(
                {
                    "updateCells": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startRowIndex": row_number - 1,
                            "endRowIndex": row_number,
                            "startColumnIndex": col_index,
                            "endColumnIndex": col_index + 1,
                        },
                        "rows": [{"values": [{"userEnteredValue": {"stringValue": str(value)}}]}],
                        "fields": "userEnteredValue",
                    }
                }
            )
    for start in range(0, len(requests), 500):
        chunk = requests[start : start + 500]
        # Best-effort hardening, not critical path - the actual data write
        # already succeeded before this runs. A persistently-unavailable API
        # shouldn't be allowed to take down an otherwise-successful sync, so
        # this fails fast (few retries, short backoff cap) and just warns
        # rather than raising.
        try:
            sheet_call(
                lambda chunk=chunk: worksheet.spreadsheet.batch_update({"requests": chunk}),
                description=f"Force ID columns to text ({start + 1}-{start + len(chunk)})",
                retries=3,
                backoff_cap=8,
            )
        except RuntimeError as exc:
            safe_print(f"Warning: couldn't force ID columns to text ({start + 1}-{start + len(chunk)}): {exc}")


def set_plain_text_columns(worksheet, col_indices: list[int], *, start_row: int = 1) -> None:
    """Format whole ID columns as Plain Text.

    This is separate from force_ids_text: formatting the column prevents
    future manual edits or formula copy-downs from re-interpreting an ID as a
    number, while force_ids_text fixes the actual typed value in populated
    cells.
    """
    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": start_row - 1,
                    "startColumnIndex": col_index,
                    "endColumnIndex": col_index + 1,
                },
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}},
                "fields": "userEnteredFormat.numberFormat",
            }
        }
        for col_index in col_indices
    ]
    if requests:
        sheet_call(
            lambda: worksheet.spreadsheet.batch_update({"requests": requests}),
            description=f"Set {worksheet.title} ID columns to Plain Text",
        )


def set_plain_text_columns_by_header(
    worksheet, headers: list[str], id_headers: list[str], *, start_row: int = 1
) -> None:
    set_plain_text_columns(
        worksheet,
        [headers.index(header) for header in id_headers if header in headers],
        start_row=start_row,
    )


def force_id_columns_text(
    worksheet, headers: list[str], id_headers: list[str], row_items: list[tuple[int, list[Any]]]
) -> None:
    """Force the given ID columns (by header name) to Plain Text for the
    given (row_number, row) pairs - see force_ids_text for why."""
    col_indices = [headers.index(h) for h in id_headers]
    cells: dict[int, dict[int, Any]] = {}
    for row_number, row in row_items:
        col_values = {col: row[col] for col in col_indices if col < len(row)}
        if col_values:
            cells[row_number] = col_values
    if cells:
        force_ids_text(worksheet, cells)


def sheet_call(fn, *, retries: int = 5, description: str = "Sheets API call", backoff_cap: int = 30):
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except gspread.exceptions.APIError as exc:
            last_error = exc
            if attempt < retries:
                wait = min(2**attempt, backoff_cap)
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
    set_plain_text_columns(worksheet, [0], start_row=data_start_row)
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
        sheet_call(
            lambda: worksheet.update(
                f"A{data_start_row}:A{new_last_row}",
                [[item_id] for item_id in ids],
                value_input_option="RAW",
            ),
            description=f"Write {worksheet_name} column A",
        )
        # MATCH/XLOOKUP/INDEX in this tab's formula columns do exact-type
        # matching against the source *Data tab's ID column, which is kept
        # as Plain Text too (see force_ids_text) - both sides must agree.
        force_ids_text(
            worksheet,
            {data_start_row + i: {0: item_id} for i, item_id in enumerate(ids)},
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

    # If the ID list shrank enough that the sheet's actual row count is now
    # bigger than it needs to be, shrink it back down - the clears above only
    # blank cell content, they don't free up the workbook's 10M-cell budget.
    if worksheet.row_count > new_last_row:
        sheet_call(
            lambda: worksheet.resize(rows=new_last_row),
            description=f"Shrink {worksheet_name} sheet",
        )

    added_rows = new_last_row - formula_last_row
    if added_rows > 0 and formula_last_row >= data_start_row:
        # copyPaste refuses to run at all if a basic filter (Data > Create a
        # filter) is hiding any row in range - clear it first. Best-effort:
        # this fails harmlessly if there's no filter to remove.
        try:
            spreadsheet.batch_update({"requests": [{"clearBasicFilter": {"sheetId": worksheet.id}}]})
        except gspread.exceptions.APIError:
            pass

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


def find_column(header: list[Any], name: str) -> int:
    """0-based index of `name` in `header` - raises instead of silently
    writing into the wrong cell if Individual Results' column layout has
    changed since this was last updated (it already has, more than once)."""
    for i, value in enumerate(header):
        if str(value).strip() == name:
            return i
    raise SystemExit(
        f'Individual Results is missing a "{name}" column - its layout has '
        "changed; update sync_individual_results to match."
    )


# values: (Date, Home Club ID, Home name, Away Club ID, Away name, Home
# score, Away score). Keyed on name rather than ID for dedup, since name is
# always populated but ID may still be blank for a club not yet ID-mapped.
def individual_result_key(values: tuple[Any, ...]) -> tuple[str, str, str, str, str] | None:
    match_date = parse_sheet_date(values[0])
    if not match_date:
        return None
    return (
        match_date.isoformat(),
        normalize_sheet_value(values[2]),
        normalize_sheet_value(values[4]),
        normalize_sheet_value(values[5]),
        normalize_sheet_value(values[6]),
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


SCORE_TEXT_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")


def regulation_score(score_text: Any, home_score: Any, away_score: Any) -> tuple[Any, Any]:
    """For a match decided on penalties, FotMob's Home/Away Score fields
    report goals-plus-shootout-penalties combined (e.g. a 1-1 draw River
    Plate lost 7-8 on pens comes back as 8-9), not a real scoreline - the
    Score/scoreStr text ("1 - 1") is the genuine regulation-time result.
    Falls back to Home/Away Score only when Score isn't a parseable "H - A"."""
    match = SCORE_TEXT_RE.match(str(score_text or ""))
    if match:
        return int(match.group(1)), int(match.group(2))
    return home_score, away_score


def individual_results_rows_from_matches(
    spreadsheet, all_matches: dict[str, dict[str, Any]]
) -> list[tuple[Any, Any, Any, Any, Any, Any, Any]]:
    """Builds Individual Results source rows straight from this run's full,
    unfiltered match fetch - independent of whatever competitions MatchData/
    Matches themselves are currently scoped to, so Individual Results (the
    Club Ranking spreadsheet) keeps receiving every competition's finished
    matches even after MatchData/Matches are narrowed down.

    Returns (Date, Home Club ID, Home name, Away Club ID, Away name, Home
    score, Away score) - both the real FotMob Club ID (getMatches.py already
    resolves it, the same ID used everywhere else in this project, e.g.
    Club!A "Club ID") and the name. The Club Ranking spreadsheet's own
    Ranking Breakdown/Ranking tabs now match by ID first, falling back to
    name only for clubs it hasn't ID-mapped yet - keeping both here lets
    that fallback keep working."""
    rows: list[tuple[Any, Any, Any, Any, Any, Any, Any]] = []
    for row in all_matches.values():
        if not row.get("Finished") or row.get("Cancelled"):
            continue
        home_score, away_score = regulation_score(
            row.get("Score", ""), row.get("Home Score", ""), row.get("Away Score", "")
        )
        rows.append((
            row.get("Match UTC", ""),
            row.get("Home Club ID", ""),
            row.get("Home Club", ""),
            row.get("Away Club ID", ""),
            row.get("Away Club", ""),
            home_score,
            away_score,
        ))
    return rows


def individual_results_rows_from_sheet(
    spreadsheet, args: argparse.Namespace
) -> list[tuple[Any, Any, Any, Any, Any, Any, Any]]:
    """Fallback used only when this run has no fresh in-memory match data
    (--only-individual-results, or --skip-matches) - reads whatever's
    currently in the Matches/MatchData tabs, same as before this function was
    split up."""
    source_matches = spreadsheet.worksheet(MATCHES_WORKSHEET)
    source_matchdata = spreadsheet.worksheet(args.match_worksheet)

    matches_values = sheet_call(
        # A:AZ (not just A:Q) so column AY (Score) is available for
        # regulation_score() to prefer over the penalty-inflated HomeS/AwayS.
        lambda: source_matches.get("A:AZ", value_render_option="FORMATTED_VALUE"),
        description="Read Matches tab for Individual Results",
    )
    matchdata_values = read_existing_values(
        source_matchdata, "Read MatchData for Individual Results finished statuses"
    )
    if not matches_values or not matchdata_values:
        return []

    matchdata_header = matchdata_values[0]
    match_id_col = matchdata_header.index("Match ID")
    finished_col = matchdata_header.index("Finished")
    finished_ids: set[str] = set()
    for row in matchdata_values[1:]:
        match_id = normalize_sheet_value(value_at(row, match_id_col))
        finished = normalize_sheet_value(value_at(row, finished_col))
        if match_id and finished in {"1", "true", "yes", "finished"}:
            finished_ids.add(match_id)

    rows: list[tuple[Any, Any, Any, Any, Any, Any, Any]] = []
    for row in matches_values[MATCHES_DATA_START_ROW - 1 :]:
        match_id = normalize_sheet_value(value_at(row, 0))
        if match_id not in finished_ids:
            continue
        home_score, away_score = regulation_score(
            value_at(row, 50),  # AY: Score
            value_at(row, 15),  # P: HomeS
            value_at(row, 16),  # Q: AwayS
        )
        rows.append((
            value_at(row, 1),   # B: Date
            value_at(row, 9),   # J: Home Club ID
            value_at(row, 10),  # K: Home
            value_at(row, 12),  # M: Away Club ID
            value_at(row, 13),  # N: Away
            home_score,
            away_score,
        ))
    return rows


def sync_individual_results(
    source_spreadsheet,
    args: argparse.Namespace,
    all_matches: dict[str, dict[str, Any]] | None = None,
) -> None:
    finished_rows = (
        individual_results_rows_from_matches(source_spreadsheet, all_matches)
        if all_matches is not None
        else individual_results_rows_from_sheet(source_spreadsheet, args)
    )
    if not finished_rows:
        safe_print("Individual Results: no finished match data available; nothing to copy.")
        return

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
    if not destination_values:
        raise SystemExit("Individual Results tab is empty - expected a header row.")

    # Column positions are resolved by header name, not hardcoded letters -
    # this sheet's layout has already changed shape more than once (new ID/
    # audit columns added directly in Sheets), and a stale hardcoded letter
    # would silently write a score into the wrong cell instead of failing.
    header = destination_values[0]
    date_col = find_column(header, "Date")
    home_id_col = find_column(header, "Home ID")
    home_name_col = find_column(header, "HomeTeam")
    away_id_col = find_column(header, "Away ID")
    away_name_col = find_column(header, "AwayTeam")
    home_score_col = find_column(header, "FTHG")
    away_score_col = find_column(header, "FTAG")

    dated_destination_rows: list[tuple[date, int, list[Any]]] = []
    for offset, row in enumerate(destination_values[1:], start=2):
        row_date = parse_sheet_date(value_at(row, date_col))
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
                    value_at(row, date_col),
                    "",
                    value_at(row, home_name_col),
                    "",
                    value_at(row, away_name_col),
                    value_at(row, home_score_col),
                    value_at(row, away_score_col),
                )
            )
            if key:
                existing_on_max_date.add(key)

    today = datetime.now().date()
    skipped_future = 0
    rows_to_add: list[tuple[date, tuple[Any, ...]]] = []
    for result_values in finished_rows:
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

    date_letter = column_letters(date_col + 1)
    home_id_letter = column_letters(home_id_col + 1)
    home_name_letter = column_letters(home_name_col + 1)
    away_id_letter = column_letters(away_id_col + 1)
    away_name_letter = column_letters(away_name_col + 1)
    home_score_letter = column_letters(home_score_col + 1)
    away_score_letter = column_letters(away_score_col + 1)

    updates = []
    for offset, (match_date, values) in enumerate(rows_to_add):
        row_number = start_row + offset
        _, home_id, home_name, away_id, away_name, home_score, away_score = values
        updates.extend(
            [
                {"range": f"{date_letter}{row_number}", "values": [[match_date.strftime("%d/%m/%Y")]]},
                # A leading apostrophe forces text even under USER_ENTERED -
                # without it Sheets auto-detects a numeric-looking ID string
                # as a number, which silently breaks every ID-keyed VLOOKUP/
                # CONCAT match against Individual Results' own ID columns
                # (also text) the moment the two sides' types disagree.
                {"range": f"{home_id_letter}{row_number}", "values": [[f"'{home_id}" if home_id else ""]]},
                {"range": f"{home_name_letter}{row_number}", "values": [[home_name]]},
                {"range": f"{away_id_letter}{row_number}", "values": [[f"'{away_id}" if away_id else ""]]},
                {"range": f"{away_name_letter}{row_number}", "values": [[away_name]]},
                {"range": f"{home_score_letter}{row_number}", "values": [[home_score]]},
                {"range": f"{away_score_letter}{row_number}", "values": [[away_score]]},
            ]
        )

    for start in range(0, len(updates), args.sheet_batch_size * 7):
        chunk = updates[start : start + args.sheet_batch_size * 7]
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
    row_dict["Manager ID"] = row_dict.get("Manager ID") or manager_id
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
    manager_id_headers = [
        "Manager ID",
        "Current Club ID",
        "Career Club IDs",
        "Trophy Club IDs",
        "Competition IDs Won",
        "Last Match ID",
        "Last Opponent ID",
        "Last Competition ID",
    ]
    set_plain_text_columns_by_header(worksheet, gmgr.HEADERS, manager_id_headers, start_row=2)

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

    base_row = len(existing_values)
    # Reassert ID cells as string values after data writes so exact-match
    # lookups don't drift into text-vs-number mismatches.
    id_row_items = updates + [(base_row + 1 + i, row) for i, row in enumerate(appended_rows)]
    if id_row_items:
        force_id_columns_text(
            worksheet,
            gmgr.HEADERS,
            manager_id_headers,
            id_row_items,
        )

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
    # Keep IDs as text so exact-match formulas agree with the lookup tabs.
    row_dict["Competition ID"] = str(row_dict.get("Competition ID") or competition_id)
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
    competition_id_headers = [
        "Competition ID",
        "Tournament ID",
        "Season Tournament IDs",
        "Previous Winner Club ID",
        "Previous Runner-Up Club ID",
        "Team IDs",
        "Next Match ID",
        "Next Home Club ID",
        "Next Away Club ID",
    ]
    set_plain_text_columns_by_header(worksheet, gcomp.HEADERS, competition_id_headers, start_row=2)

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

    base_row = len(existing_values)
    id_row_items = updates + [(base_row + 1 + i, row) for i, row in enumerate(appended_rows)]
    if id_row_items:
        force_id_columns_text(
            worksheet,
            gcomp.HEADERS,
            competition_id_headers,
            id_row_items,
        )

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


SNAPSHOT_HEADERS = [
    "SnapProjH", "SnapProjA", "SnapHomeScr", "SnapAwayScr",
    "SnapHomeWinPct", "SnapDrawPct", "SnapAwayWinPct",
]
# Column indices (0-based) into the Matches tab's A:AQ layout for the live
# values these snapshot columns freeze. ProjH/ProjA and the win/draw/away
# probabilities both derive from HomeScr/AwayScr, which depend on TODAY() -
# see snapshot_pre_match_projections for why that means they drift forever
# unless captured before kickoff.
MATCHES_STARTED_COL = 2
MATCHES_SNAPSHOT_SOURCE_COLS = [19, 20, 21, 24, 40, 41, 42]  # ProjH, ProjA, HomeScr, AwayScr, HW%, D%, AW%


def snapshot_pre_match_projections(spreadsheet, args: argparse.Namespace) -> None:
    """Freezes each not-yet-started match's current ProjH/ProjA/HomeScr/
    AwayScr and Home/Draw/Away win% into extra MatchData columns, sitting
    past gmatch.HEADERS's own columns so sync_matches's normal row
    updates/appends (which only ever touch A:{len(gmatch.HEADERS)}) never
    overwrite them. Captured fresh every run while Started=0 (so it's always
    the latest pre-match prediction, informed by late team news), then left
    untouched forever the moment a match kicks off - without this, a
    finished match's "projection" is just whatever today's club ratings
    would predict, not what was actually predicted beforehand, since
    HomeScr/AwayScr (and everything derived from them) are literally
    formulas built on TODAY().
    """
    match_worksheet = spreadsheet.worksheet(args.match_worksheet)
    matches_worksheet = spreadsheet.worksheet(MATCHES_WORKSHEET)

    header_len = len(gmatch.HEADERS)
    snap_start_col = header_len + 1
    snap_end_col = header_len + len(SNAPSHOT_HEADERS)
    snap_start_letter = column_letters(snap_start_col)
    snap_end_letter = column_letters(snap_end_col)

    # MatchData's column count has always exactly matched gmatch.HEADERS
    # (it's created with that as its column count, and nothing else has ever
    # needed more) - grow it once to make room for these extra columns.
    if match_worksheet.col_count < snap_end_col:
        sheet_call(
            lambda: match_worksheet.resize(cols=snap_end_col),
            description="Grow MatchData for snapshot columns",
        )

    existing_header = sheet_call(
        lambda: match_worksheet.get(f"{snap_start_letter}1:{snap_end_letter}1"),
        description="Read MatchData snapshot header",
    )
    if not existing_header or existing_header[0] != SNAPSHOT_HEADERS:
        sheet_call(
            lambda: match_worksheet.update(
                f"{snap_start_letter}1:{snap_end_letter}1", [SNAPSHOT_HEADERS], value_input_option="RAW"
            ),
            description="Write MatchData snapshot header",
        )

    matchdata_values = read_existing_values(match_worksheet, "Read MatchData for snapshot")
    if len(matchdata_values) < 2:
        safe_print("Snapshot: MatchData is empty; nothing to do.")
        return
    match_id_col = gmatch.HEADERS.index("Match ID")
    row_by_match_id: dict[str, int] = {}
    for offset, row in enumerate(matchdata_values[1:]):
        mid = normalize_sheet_value(value_at(row, match_id_col))
        if mid:
            row_by_match_id[mid] = offset + 2  # +2: header row + 1-based

    matches_last_col = column_letters(43)  # A:AQ - through the win/draw/away-win probability columns
    matches_values = sheet_call(
        lambda: matches_worksheet.get(f"A2:{matches_last_col}", value_render_option="UNFORMATTED_VALUE"),
        description="Read Matches tab for snapshot",
    )

    updates = []
    for row in matches_values:
        match_id = normalize_sheet_value(value_at(row, 0))
        if not match_id or match_id not in row_by_match_id:
            continue
        if normalize_sheet_value(value_at(row, MATCHES_STARTED_COL)) in ("1", "true"):
            continue  # already kicked off - whatever was last captured stays frozen

        values = [value_at(row, c) for c in MATCHES_SNAPSHOT_SOURCE_COLS]
        if values[0] == "" and values[1] == "":
            continue  # no projection available yet (e.g. not enough matches played for a rating)

        updates.append({
            "range": f"{snap_start_letter}{row_by_match_id[match_id]}:{snap_end_letter}{row_by_match_id[match_id]}",
            "values": [values],
        })

    if updates:
        for start in range(0, len(updates), args.sheet_batch_size):
            chunk = updates[start : start + args.sheet_batch_size]
            sheet_call(
                lambda chunk=chunk: match_worksheet.batch_update(chunk, value_input_option="RAW"),
                description=f"Write pre-match snapshots {start + 1}-{start + len(chunk)}",
            )
    safe_print(f"Snapshot: captured pre-match projections for {len(updates):,} not-yet-started match(es).")


def load_matchdata_competition_ids(args: argparse.Namespace) -> set[str] | None:
    """The competition allow-list for MatchData/Matches - a match is kept if its
    Competition ID OR Parent Competition ID is in this set (group-stage
    competitions like the EFL Trophy or Champions League tag individual
    matches with a per-group Competition ID, but share one Parent Competition
    ID across the whole tournament). Returns None (no filtering) if the file
    doesn't exist, so this stays optional/backward compatible."""
    path = args.matchdata_competition_input
    if not path.exists():
        return None
    ids = set(gmatch.load_ids(path))
    return ids or None


def match_in_scope(row: dict[str, Any], keep_competition_ids: set[str] | None) -> bool:
    if not keep_competition_ids:
        return True
    return (
        str(row.get("Competition ID", "")) in keep_competition_ids
        or str(row.get("Parent Competition ID", "")) in keep_competition_ids
    )


def delete_sheet_rows(spreadsheet, worksheet, row_numbers: list[int], *, description: str) -> None:
    """Delete whole rows (1-based) from a worksheet, freeing their cells from
    the workbook's 10M-cell budget - unlike clearing content, this actually
    shrinks the sheet's row count. Adjacent row numbers are merged into
    ranges, and ranges are applied highest-row-first so a deletion never
    invalidates the row numbers still queued for later ranges."""
    if not row_numbers:
        return
    ordered = sorted(set(row_numbers))
    ranges: list[tuple[int, int]] = []
    for row_number in ordered:
        if ranges and ranges[-1][1] == row_number - 1:
            ranges[-1] = (ranges[-1][0], row_number)
        else:
            ranges.append((row_number, row_number))
    ranges.sort(key=lambda r: r[0], reverse=True)

    chunk_size = 200
    for start in range(0, len(ranges), chunk_size):
        chunk = ranges[start : start + chunk_size]
        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": worksheet.id,
                        "dimension": "ROWS",
                        "startIndex": lo - 1,
                        "endIndex": hi,
                    }
                }
            }
            for lo, hi in chunk
        ]
        sheet_call(
            lambda requests=requests: spreadsheet.batch_update({"requests": requests}),
            description=f"{description} (range {start + 1}-{start + len(chunk)} of {len(ranges)})",
        )

    # Mirror gspread's own delete_dimension() bookkeeping so worksheet.row_count
    # reflects reality for any caller that keeps using this worksheet object
    # afterward, instead of silently going stale.
    deleted = sum(hi - lo + 1 for lo, hi in ranges)
    worksheet._properties["gridProperties"]["rowCount"] -= deleted


def sync_matches(
    spreadsheet, args: argparse.Namespace
) -> tuple[list[tuple[str, str, str]], dict[str, dict[str, Any]]]:
    club_ids = gmatch.load_ids(args.club_input)
    safe_print(f"Loaded {len(club_ids):,} club IDs from {args.club_input}")

    keep_competition_ids = load_matchdata_competition_ids(args)
    if keep_competition_ids:
        safe_print(
            f"MatchData/Matches are scoped to {len(keep_competition_ids):,} competition "
            f"ID(s) from {args.matchdata_competition_input}."
        )

    worksheet = get_or_create_worksheet(spreadsheet, args.match_worksheet, len(gmatch.HEADERS))
    existing_values = read_existing_values(worksheet, "Read MatchData sheet")

    if not existing_values:
        sheet_call(lambda: worksheet.update([gmatch.HEADERS]), description="Write MatchData header")
        existing_values = [gmatch.HEADERS]

    header_len = len(gmatch.HEADERS)
    # Only the first header_len columns are getMatches.py's own layout - the
    # snapshot columns (SnapProjH etc., written by snapshot_pre_match_projections)
    # live past that and are allowed to trail without failing this check.
    if existing_values[0][:header_len] != gmatch.HEADERS:
        raise SystemExit(
            "MatchData's header row doesn't match getMatches.py's current "
            "HEADERS layout. Clear the sheet or fix the header before syncing."
        )
    match_id_headers = [
        "Match ID",
        "Competition ID",
        "Parent Competition ID",
        "Home Club ID",
        "Away Club ID",
        "Winner Club ID",
        "Player Of The Match ID",
        "Player Of The Match Club ID",
    ]
    set_plain_text_columns_by_header(worksheet, gmatch.HEADERS, match_id_headers, start_row=2)

    match_id_col = gmatch.HEADERS.index("Match ID")
    match_utc_col = gmatch.HEADERS.index("Match UTC")

    existing_by_id: dict[str, int] = {}
    cached: dict[str, dict[str, Any]] = {}
    cached_all: dict[str, dict[str, Any]] = {}
    out_of_scope_rows: list[int] = []
    for offset, row in enumerate(existing_values[1:]):
        padded = row + [""] * (header_len - len(row))
        match_id = str(padded[match_id_col]).strip()
        if not match_id:
            continue
        row_number = offset + 2  # +2: header row + 1-based
        row_dict = dict(zip(gmatch.HEADERS, padded))
        if not match_in_scope(row_dict, keep_competition_ids):
            out_of_scope_rows.append(row_number)
            continue
        existing_by_id[match_id] = row_number
        cached_all[match_id] = row_dict
        if str(row_dict.get("Detailed Data", "")) == "1":
            cached[match_id] = row_dict

    safe_print(
        f"MatchData: {len(existing_by_id):,} matches already in the sheet, "
        f"{len(cached):,} already fully detailed."
    )
    if out_of_scope_rows:
        safe_print(
            f"MatchData: {len(out_of_scope_rows):,} existing row(s) are outside the kept "
            "competitions and will be deleted after this run's updates/appends."
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

    today_date = datetime.now(timezone.utc).date()
    today = today_date.isoformat()
    # Matches within this many days are re-fetched even though they haven't
    # kicked off yet, to catch things a stored row can't self-correct from
    # otherwise: a rescheduled date/time, or a cup tie recorded as
    # "Wimbledon/Fulham" before the replay that should now read "Fulham".
    # Anything further out than this stays skipped, same as before, so the
    # bulk of the far-future fixture list isn't re-queried every run.
    NEAR_TERM_REFRESH_DAYS = 7
    near_term_horizon = (today_date + timedelta(days=NEAR_TERM_REFRESH_DAYS)).isoformat()

    updates: list[tuple[int, list[Any]]] = []
    appended_rows: list[list[Any]] = []
    skipped_not_due = 0
    skipped_out_of_scope = 0
    for match_id, row_dict in matches.items():
        if not match_in_scope(row_dict, keep_competition_ids):
            skipped_out_of_scope += 1
            continue
        row = [row_dict.get(header, "") for header in gmatch.HEADERS]
        if match_id in existing_by_id:
            # An existing row is only worth rewriting if it's not finished
            # yet AND (its date has already passed, so it's actually due for
            # a status/result update, OR it kicks off soon enough that its
            # own details could still change - see NEAR_TERM_REFRESH_DAYS
            # above). A finished match never changes again, and a fixture
            # both far out and already round-filled has nothing left to
            # change until one of those becomes true.
            existing_row = cached_all.get(match_id, {})
            already_finished = str(existing_row.get("Finished", "")) == "1"
            match_date = str(existing_row.get("Match UTC", ""))[:10]
            not_due_yet = bool(match_date) and match_date > near_term_horizon
            if already_finished or not_due_yet:
                skipped_not_due += 1
                continue
            updates.append((existing_by_id[match_id], row))
        else:
            appended_rows.append(row)

    if skipped_not_due:
        safe_print(f"Skipped {skipped_not_due:,} matches that are finished or not due yet (nothing to update).")
    if skipped_out_of_scope:
        safe_print(f"Skipped {skipped_out_of_scope:,} fetched match(es) outside the kept competitions.")

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

    base_row = len(existing_values)
    # Reassert ID cells as string values after data writes so exact-match
    # lookups don't drift into text-vs-number mismatches.
    existing_id_row_items = [
        (offset + 2, (row + [""] * (header_len - len(row)))[:header_len])
        for offset, row in enumerate(existing_values[1:])
    ]
    id_row_items = (
        existing_id_row_items
        + updates
        + [(base_row + 1 + i, row) for i, row in enumerate(appended_rows)]
    )
    if id_row_items:
        force_id_columns_text(
            worksheet,
            gmatch.HEADERS,
            match_id_headers,
            id_row_items,
        )

    if out_of_scope_rows:
        # Deleted last, using the row numbers captured before this run's
        # updates/appends: in-place updates don't move rows and appends only
        # add beyond the current last row, so those original row numbers are
        # still accurate right up until this point.
        delete_sheet_rows(
            spreadsheet, worksheet, out_of_scope_rows,
            description="Delete out-of-scope MatchData rows",
        )
        safe_print(f"Removed {len(out_of_scope_rows):,} out-of-scope row(s) from MatchData.")

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
    return errors, matches


def fetch_club_row(club_id: str, args: argparse.Namespace) -> list[Any]:
    row = gclub.build_row(club_id, retries=args.club_retries, request_delay=args.club_request_delay)
    club_id_col = gclub.HEADERS.index("Club ID")
    row[club_id_col] = row[club_id_col] or club_id
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
    club_id_headers = [
        "Club ID",
        "Primary League ID",
        "Current Tournament ID",
        "Coach ID",
        "Goalkeeper IDs",
        "Defender IDs",
        "Midfielder IDs",
        "Forward IDs",
        "Squad Player IDs",
        "Competition IDs",
        "Next Match ID",
        "Next Opponent ID",
        "Next Competition ID",
        "Last Match ID",
        "Last Opponent ID",
        "Last Competition ID",
    ]
    set_plain_text_columns_by_header(worksheet, gclub.HEADERS, club_id_headers, start_row=2)

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
    fatal_errors: list[tuple[str, str]] = []

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
                # A deleted/merged club ID is permanent, not worth retrying,
                # and not a real problem with the sync itself - don't let it
                # fail the whole run the way an unexpected error should.
                if not isinstance(exc, gclub.ClubNotFoundError):
                    fatal_errors.append((club_id, str(exc)))
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

    base_row = len(existing_values)
    # Reassert ID cells as string values after data writes so exact-match
    # lookups don't drift into text-vs-number mismatches.
    id_row_items = updates + [(base_row + 1 + i, row) for i, row in enumerate(appended_rows)]
    if id_row_items:
        force_id_columns_text(
            worksheet,
            gclub.HEADERS,
            club_id_headers,
            id_row_items,
        )

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

    not_found_count = len(errors) - len(fatal_errors)
    safe_print(
        f"ClubData done. {len(appended_rows):,} new clubs added, "
        f"{len(updates):,} existing clubs refreshed, {len(errors):,} failed "
        f"({not_found_count:,} permanently gone, {len(fatal_errors):,} unexpected)."
    )
    return fatal_errors


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
    player_id_headers = ["Player ID", "Current Club ID"]
    set_plain_text_columns_by_header(worksheet, gp.HEADERS, player_id_headers, start_row=2)

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

    base_row = len(existing_values)
    # Reassert ID cells as string values after data writes so exact-match
    # lookups don't drift into text-vs-number mismatches.
    id_row_items = updates + [(base_row + 1 + i, row) for i, row in enumerate(appended_rows)]
    if id_row_items:
        force_id_columns_text(worksheet, gp.HEADERS, player_id_headers, id_row_items)

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
    args.matchdata_competition_input = args.matchdata_competition_input.resolve()
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
    all_matches: dict[str, dict[str, Any]] | None = None
    if not args.skip_matches:
        match_errors, all_matches = sync_matches(spreadsheet, args)
        if not args.skip_matches_tab:
            match_worksheet = spreadsheet.worksheet(args.match_worksheet)
            matchdata_ids = id_column_values(match_worksheet, "Read final MatchData IDs")[1:]
            sync_matches_tab(spreadsheet, matchdata_ids)
        if not args.skip_projection_snapshot:
            snapshot_pre_match_projections(spreadsheet, args)

    if not args.skip_individual_results:
        if all_matches is None:
            safe_print(
                "Individual Results: MatchData sync was skipped; copying from current sheet values."
            )
        sync_individual_results(spreadsheet, args, all_matches=all_matches)

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
