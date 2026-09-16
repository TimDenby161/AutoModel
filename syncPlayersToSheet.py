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
from typing import Any, Callable, Iterable, TypeVar

import requests

import getPlayers as gp
import getManagers as gmgr
import getCompetitions as gcomp
import getMatches as gmatch
import getClubs as gclub
import getOdds as godds

try:
    import gspread
except ImportError:
    print(
        "Missing dependency: run `pip install gspread` first.",
        file=sys.stderr,
    )
    raise SystemExit(1)


SCRIPT_DIR = Path(__file__).resolve().parent
_T = TypeVar("_T")

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
PLAYERS_LOOKUP_MIRROR = SCRIPT_DIR / "lookup_mirror_players.csv"

LEAGUES_WORKSHEET = "Leagues"
LEAGUES_HEADER_ROW = 1
LEAGUES_DATA_START_ROW = 2
LEAGUES_LAST_FORMULA_COLUMN = "G"  # every column B..G looks up CompetitionData by ID
LEAGUES_LOOKUP_MIRROR = SCRIPT_DIR / "lookup_mirror_leagues.csv"

CLUB_WORKSHEET = "Club"
CLUB_HEADER_ROW = 1
CLUB_DATA_START_ROW = 2
CLUB_LAST_FORMULA_COLUMN = "CP"  # every column B..CP looks up ClubData by ID
CLUB_LOOKUP_MIRROR = SCRIPT_DIR / "lookup_mirror_club.csv"

MATCHES_WORKSHEET = "Matches"
MATCHES_HEADER_ROW = 1
MATCHES_DATA_START_ROW = 2
MATCHES_LAST_FORMULA_COLUMN = "AZ"  # every column B..AZ looks up/derives from MatchData by ID
MATCHES_LOOKUP_MIRROR = SCRIPT_DIR / "lookup_mirror_matches.csv"

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
    parser.add_argument(
        "--only-players-tab",
        action="store_true",
        help="Only mirror IDs/formulas into the Players tab from the local player CSV",
    )
    parser.add_argument(
        "--full-player-refresh",
        action="store_true",
        help=(
            "Refresh every existing player every run (the old default). Without this, "
            "only players whose club played a match in the last "
            "--player-refresh-window-hours get refreshed - new players are always "
            "fetched in full regardless."
        ),
    )
    parser.add_argument(
        "--player-refresh-window-hours",
        type=float,
        default=36,
        help="How far back a finished match still counts as 'recent enough' to refresh its players (default: 36)",
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
        "--sheets-request-delay",
        type=float,
        default=1.0,
        help=(
            "Minimum seconds between successive Sheets API calls, spread across every "
            "read/write this script makes - keeps request rate well under Sheets API "
            "quotas on a large, heavily-synced spreadsheet (default: 1.0)"
        ),
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
        "--only-leagues-tab",
        action="store_true",
        help="Only mirror IDs/formulas into the Leagues tab from the local competition CSV",
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
        "--match-local-cache",
        type=Path,
        default=folder / "automodel_matches_local_cache.csv",
        help=(
            "Local mirror of every match seen this run, in scope or not (unlike "
            "--match-csv-output, which only mirrors what's kept in the sheet) - lets "
            "an out-of-scope match's Round/Competition info be reused instead of "
            "re-fetched every run just because it'll be filtered out afterward. "
            "Default: automodel_matches_local_cache.csv"
        ),
    )
    parser.add_argument(
        "--competition-parents-cache",
        type=Path,
        default=folder / "competition_parent_ids.json",
        help=(
            "Learned Competition ID -> Parent Competition ID map, used to skip "
            "re-enriching finished matches whose competition is already known to be "
            "outside --matchdata-competition-input. Default: competition_parent_ids.json"
        ),
    )
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
        "--only-matches-tab",
        action="store_true",
        help="Only mirror IDs/formulas into the Matches tab from the local match CSV",
    )
    parser.add_argument(
        "--skip-projection-snapshot",
        action="store_true",
        help="Don't freeze not-yet-started matches' ProjH/ProjA/HomeScr/AwayScr and win/draw/away win%% into MatchData's Snap* columns",
    )
    parser.add_argument(
        "--skip-bets",
        action="store_true",
        help="Don't settle bets or fetch new odds/picks into the BetData tab",
    )
    parser.add_argument(
        "--odds-api-key",
        default="",
        help="The Odds API key (theoddsapi.com) - required to fetch new picks; settlement of existing picks still runs without it",
    )
    parser.add_argument("--odds-worksheet", default="BetData")
    parser.add_argument(
        "--odds-competition-map",
        type=Path,
        default=folder / "odds_competition_map.csv",
        help="CSV mapping AutoModel Competition ID -> The Odds API sport_key (default: odds_competition_map.csv)",
    )
    parser.add_argument(
        "--bet-csv-output",
        type=Path,
        default=folder / "automodel_bets.csv",
        help="CSV mirror of the BetData sheet, rewritten each run",
    )
    parser.add_argument("--odds-region", default="uk")
    parser.add_argument(
        "--odds-upcoming-window-days", type=float, default=10,
        help="Only fetch odds for competitions with a not-yet-started match within this many days",
    )
    parser.add_argument("--odds-retries", type=int, default=4)
    parser.add_argument("--odds-request-delay", type=float, default=1.0)
    parser.add_argument(
        "--bet-edge-threshold", type=float, default=0.10,
        help="Minimum model-vs-implied-odds edge (as a fraction, e.g. 0.10 = 10%%) to count as a recommended pick",
    )
    parser.add_argument(
        "--starting-bankroll", type=float, default=970.63,
        help=(
            "Starting bankroll for quarter-Kelly stake sizing (same currency as your odds region, "
            "e.g. GBP for --odds-region uk). Compounds with settled results over time - this only "
            "records/tracks picks, it never places a real bet or touches a bookmaker account."
        ),
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
    parser.add_argument(
        "--only-club-tab",
        action="store_true",
        help="Only mirror IDs/formulas into the Club tab from the local club CSV",
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


def apply_number_ids(row: list[Any], headers: list[str], id_headers: list[str]) -> list[Any]:
    """Coerce each of id_headers to a plain int in row (matched by header
    name/position), so a plain RAW write lands as NUMBER on every ID field -
    the same type the *Data/lookup tabs' own INDEX/MATCH/VLOOKUP formulas
    expect on both sides, with no extra formatting/API calls needed.
    Multi-value ID-list fields (e.g. "Squad Player IDs", pipe-joined) are
    left untouched - int() fails on them and the original value is kept."""
    for header in id_headers:
        idx = headers.index(header)
        if idx < len(row) and row[idx] not in (None, ""):
            try:
                row[idx] = int(row[idx])
            except (TypeError, ValueError):
                pass
    return row


_ACTIVE_SESSIONS: list[requests.Session] = []


def _reset_active_sessions() -> None:
    """Close every tracked HTTP session's pooled connections. A repeated
    timeout on otherwise-unremarkable calls (including tiny 1-2 cell reads,
    which have no size-related reason to be slow) looks less like Google
    being generally overloaded and more like one specific pooled TCP
    connection having gone bad - closing it forces the next request to open
    a fresh connection instead of retrying on the same stuck one. Safe to
    call anytime: a requests.Session stays fully usable after close(), it
    just opens new connections as needed."""
    for session in _ACTIVE_SESSIONS:
        try:
            session.close()
        except Exception:
            pass


_SHEETS_RATE_LOCK = threading.Lock()
_SHEETS_LAST_REQUEST = 0.0
SHEETS_MIN_REQUEST_INTERVAL = 1.0  # seconds between successive Sheets API calls


def _sheets_rate_limit() -> None:
    """Space out every Sheets API call by at least SHEETS_MIN_REQUEST_INTERVAL.
    Today's failures span every kind of call this file makes - tiny 1-2 cell
    reads, huge column reads, medium writes, metadata fetches - with
    outright APIError [404]/[500]/[502] responses mixed in with timeouts.
    That pattern doesn't fit any one request being too large; it fits
    Google throttling/degrading this spreadsheet once too many requests
    land on it in too short a window. This is the same rate_limit() pattern
    getMatches.py already uses for FotMob, applied here since sheet_call is
    the one chokepoint every Sheets API call in this file passes through."""
    global _SHEETS_LAST_REQUEST
    with _SHEETS_RATE_LOCK:
        wait = SHEETS_MIN_REQUEST_INTERVAL - (time.monotonic() - _SHEETS_LAST_REQUEST)
        if wait > 0:
            time.sleep(wait)
        _SHEETS_LAST_REQUEST = time.monotonic()


def _describe_exception(exc: Exception) -> str:
    """A richer failure description than str(exc) alone. gspread's own
    APIError.__str__ drops the "status" field Google actually returns
    (e.g. "UNAVAILABLE", "RESOURCE_EXHAUSTED", "INTERNAL", "NOT_FOUND") -
    that's the single clearest signal for telling a quota problem apart
    from a malformed request apart from a plain server hiccup, so surface
    it explicitly instead of just the numeric code and message."""
    if isinstance(exc, gspread.exceptions.APIError):
        status = exc.error.get("status", "?")
        message = exc.error.get("message", "")
        return f"APIError [{exc.code} {status}]: {message}"
    return f"{type(exc).__name__}: {exc}"


def sheet_call(fn, *, retries: int = 5, description: str = "Sheets API call", backoff_cap: int = 30):
    last_error: Exception | None = None
    started = time.monotonic()
    for attempt in range(1, retries + 1):
        _sheets_rate_limit()
        try:
            return fn()
        except (gspread.exceptions.APIError, requests.exceptions.RequestException) as exc:
            last_error = exc
            detail = _describe_exception(exc)
            if attempt < retries:
                wait = min(2**attempt, backoff_cap)
                safe_print(f"{description} failed (attempt {attempt}/{retries}): {detail}; retrying in {wait}s...")
                _reset_active_sessions()
                time.sleep(wait)
    elapsed = time.monotonic() - started
    raise RuntimeError(
        f"{description} failed after {retries} attempts over {elapsed:.0f}s: {_describe_exception(last_error)}"
    )


# Batch writes get fewer retries than the file's general default: a batch
# stuck badly enough to need every one of sheet_call's normal retries
# burns real minutes before giving up - and until the fix below, that
# RuntimeError also aborted every later batch in the same loop, so one bad
# batch could cost an entire tab's worth of updates. write_row_batches/
# append_row_batches both fail a stuck batch faster AND skip past it
# instead of aborting the rest.
WRITE_BATCH_RETRIES = 3
# Once a batch has already failed once at full size, further attempts are
# just checking "does a smaller slice succeed" - a handful of retries
# already ruled out simple transience at the full size, so each smaller
# slice gets only one attempt before deciding to split again (no backoff
# wait at that point either - it either works or it doesn't).
SPLIT_BATCH_RETRIES = 1


# Hard ceiling on how long ANY single top-level batch is allowed to spend
# splitting and retrying, no matter how deep it goes or how uniformly
# broken it turns out to be. Without this, a batch where every single row
# independently fails could theoretically cascade all the way down to
# one-row pieces and take hours (each of up to ~27 individual rows paying
# its own ~120s timeout). With it, once the clock runs out, everything
# still unresolved in this batch is skipped immediately - a hard
# guarantee, not just "usually fast."
MAX_BATCH_SECONDS = 600


def _write_with_splitting(
    items: list[Any],
    write_fn: Callable[[list[Any], int], None],
    *,
    label: str,
    retries: int,
    deadline: float,
) -> list[Any]:
    """Try write_fn(items, retries) as one request. If it still fails and
    there's more than one item, split in half and retry each half
    independently (with fewer retries each, since the full-size attempt
    already ruled out simple transience) - a batch that's borderline too
    large or slow to write as a whole may still succeed in smaller pieces,
    and a single item that still fails alone is a specific, useful signal
    (something about that one row's data) rather than being lumped in with
    its innocent neighbors. `deadline` (a time.monotonic() timestamp) caps
    the total time this can spend recursing - once it's passed, whatever's
    left is skipped immediately rather than splitting further. Returns
    whichever items were never successfully written."""
    if time.monotonic() > deadline:
        safe_print(
            f"*** {label}: giving up on the remaining {len(items)} item(s) in this batch - "
            f"{MAX_BATCH_SECONDS}s time budget for it is used up, skipping (will retry next run)"
        )
        return list(items)
    try:
        write_fn(items, retries)
        return []
    except RuntimeError as exc:
        if len(items) == 1:
            safe_print(
                f"*** {label}: single item still failed after {retries} attempts even alone, "
                f"skipping it (will retry next run): {exc}"
            )
            return items
        mid = len(items) // 2
        safe_print(
            f"*** {label}: a batch of {len(items)} failed after {retries} attempts - splitting "
            f"into {mid} + {len(items) - mid} and retrying each half separately..."
        )
        failed = _write_with_splitting(items[:mid], write_fn, label=label, retries=SPLIT_BATCH_RETRIES, deadline=deadline)
        failed += _write_with_splitting(items[mid:], write_fn, label=label, retries=SPLIT_BATCH_RETRIES, deadline=deadline)
        return failed


def diff_row_ranges(row_number: int, old_row: list[Any], new_row: list[Any]) -> list[dict[str, Any]]:
    """Compare old_row (what's currently on the sheet) against new_row
    (freshly fetched) column by column, and return only the cells that
    actually changed, grouped into maximal contiguous column runs - one
    {range, values} entry per run - instead of the whole row. A refresh
    that only really changes a handful of fields (e.g. current-season
    stats plus a couple of profile fields, PlayerData's normal "existing
    player" case) then writes just those columns rather than all ~730.
    Values are compared with normalize_sheet_value so a number stored as
    the sheet's own string "5" isn't treated as different from a freshly
    fetched 5. Returns [] if nothing actually changed - nothing to write
    at all for that row."""
    ranges: list[dict[str, Any]] = []
    run_start: int | None = None
    run_values: list[Any] = []

    def flush(end_idx: int) -> None:
        if run_start is None:
            return
        start_letter = column_letters(run_start + 1)
        end_letter = column_letters(end_idx)
        ranges.append({"range": f"{start_letter}{row_number}:{end_letter}{row_number}", "values": [run_values]})

    for col_idx in range(max(len(old_row), len(new_row))):
        old_val = old_row[col_idx] if col_idx < len(old_row) else ""
        new_val = new_row[col_idx] if col_idx < len(new_row) else ""
        if normalize_sheet_value(old_val) == normalize_sheet_value(new_val):
            flush(col_idx)
            run_start, run_values = None, []
            continue
        if run_start is None:
            run_start = col_idx
            run_values = []
        run_values.append(new_val)
    flush(len(new_row))
    return ranges


def write_row_batches(
    updates: list[tuple[int, list[Any]]],
    batch_size: int,
    *,
    worksheet,
    last_col: str,
    label: str,
    old_rows: dict[int, list[Any]] | None = None,
) -> set[int]:
    """Write (row_number, row) updates in chunks of `batch_size` via
    batch_update, splitting a chunk that fails into progressively smaller
    pieces (see _write_with_splitting) rather than writing off the whole
    thing - so one bad row costs only itself, not 26 healthy neighbors.
    Never allowed to abort later chunks in the same loop either way. Any
    row that still can't be written even alone keeps its current sheet
    value and gets tried again next run. The NEXT top-level chunk always
    starts back at the full `batch_size` regardless of whether this one
    needed splitting. Returns the row numbers that failed to write, so
    callers can exclude them from any downstream CSV mirror meant to match
    the sheet.

    `old_rows` (row_number -> the sheet's current row), when given, lets
    each row write only the columns that actually changed (diff_row_ranges)
    instead of the whole row - a row with no real changes at all costs no
    API call whatsoever. Without it (or for a row_number missing from
    old_rows), the full row is written, same as before."""
    if old_rows is not None:
        to_write: list[tuple[int, list[dict[str, Any]]]] = []
        unchanged = 0
        for row_number, row in updates:
            old_row = old_rows.get(row_number)
            ranges = (
                diff_row_ranges(row_number, old_row, row) if old_row is not None
                else [{"range": f"A{row_number}:{last_col}{row_number}", "values": [row]}]
            )
            if ranges:
                to_write.append((row_number, ranges))
            else:
                unchanged += 1
        if unchanged:
            safe_print(f"{label}: {unchanged:,}/{len(updates):,} row(s) unchanged, nothing to write for them")
    else:
        to_write = [
            (row_number, [{"range": f"A{row_number}:{last_col}{row_number}", "values": [row]}])
            for row_number, row in updates
        ]

    total = len(to_write)
    failed_rows: set[int] = set()
    written = 0
    for start in range(0, total, batch_size):
        chunk = to_write[start : start + batch_size]

        def do_write(items: list[tuple[int, list[dict[str, Any]]]], retries: int) -> None:
            body = [entry for _, ranges in items for entry in ranges]
            sheet_call(
                lambda body=body: worksheet.batch_update(
                    [dict(item) for item in body], value_input_option="RAW"
                ),
                description=(
                    f"{label} row {items[0][0]}" if len(items) == 1
                    else f"{label} rows {items[0][0]}-{items[-1][0]} ({len(items)})"
                ),
                retries=retries,
            )

        failed_chunk = _write_with_splitting(
            chunk, do_write, label=label, retries=WRITE_BATCH_RETRIES,
            deadline=time.monotonic() + MAX_BATCH_SECONDS,
        )
        failed_rows.update(row_number for row_number, _ in failed_chunk)
        written += len(chunk) - len(failed_chunk)
        safe_print(f"{label}: wrote {written:,}/{total:,} changed row(s) in the sheet")
    return failed_rows


def append_row_batches(
    rows: list[list[Any]],
    batch_size: int,
    *,
    worksheet,
    label: str,
) -> list[list[Any]]:
    """Append `rows` in chunks of `batch_size`, splitting a chunk that
    fails into progressively smaller pieces (see _write_with_splitting)
    rather than leaving the whole thing un-appended. Any row that still
    can't be appended even alone stays un-appended - it'll look new again
    next run and get retried then. The NEXT top-level chunk always starts
    back at the full `batch_size`. Returns only the rows that were
    actually appended, so callers can keep any downstream CSV mirror in
    sync with what's really on the sheet."""
    total = len(rows)
    appended: list[list[Any]] = []
    written = 0
    for start in range(0, total, batch_size):
        chunk = rows[start : start + batch_size]

        def do_append(items: list[list[Any]], retries: int) -> None:
            sheet_call(
                lambda items=items: worksheet.append_rows(items, value_input_option="RAW"),
                description=f"{label} append {len(items)} row(s)",
                retries=retries,
            )

        failed_chunk = _write_with_splitting(
            chunk, do_append, label=label, retries=WRITE_BATCH_RETRIES,
            deadline=time.monotonic() + MAX_BATCH_SECONDS,
        )
        failed_ids = {id(item) for item in failed_chunk}
        appended.extend(row for row in chunk if id(row) not in failed_ids)
        written += len(chunk) - len(failed_chunk)
        safe_print(f"{label}: appended {written:,}/{total:,} new rows to the sheet")
    return appended


def scaled_batch_size(base_batch_size: int, num_columns: int, *, reference_columns: int = 100) -> int:
    """Keep each Sheets API batch roughly the same total cell count no
    matter how wide a tab's rows are. --sheet-batch-size (200 rows) was
    tuned around tabs like ClubData/MatchData (50-110 columns); PlayerData
    is ~730 columns wide (17 profile fields plus a 25-field block per
    season), so the same row count sent several times as many cells per
    request and was timing out under load."""
    return max(10, base_batch_size * reference_columns // max(num_columns, 1))


def read_existing_values(worksheet, description: str) -> list[list[Any]]:
    """Read a whole sheet with UNFORMATTED_VALUE. The default FORMATTED_VALUE
    returns the cell's *display* text, which for a large enough number can
    collapse to scientific notation (e.g. "4.92E+06") - a different number
    can display identically, and a freshly fetched real ID like "4920046"
    then never matches that string, so it looks new and gets re-appended
    as a duplicate instead of recognized as already present.

    A brand-new, genuinely blank sheet (e.g. one just created by
    get_or_create_worksheet's add_worksheet fallback) comes back from
    gspread as [[]] - one empty row - not [], since it really does have
    exactly one (blank) row per its own row_count. Every "if not
    existing_values:" check in this file assumes a truly empty sheet reads
    back as [], so that case is normalized here rather than in each of the
    six call sites that rely on it."""
    values = sheet_call(
        lambda: worksheet.get_all_values(value_render_option="UNFORMATTED_VALUE"),
        description=description,
    )
    if values == [[]]:
        return []
    return values


def id_column_values(worksheet, description: str) -> list[str]:
    """Like read_existing_values, but for a single ID column read via
    col_values - same UNFORMATTED_VALUE requirement, same reasoning."""
    return sheet_call(
        lambda: worksheet.col_values(1, value_render_option="UNFORMATTED_VALUE"),
        description=description,
    )


def read_column_a_chunked(worksheet, description: str, chunk_rows: int = 2000) -> list[str]:
    """Read column A in fixed-size row chunks instead of one `col_values()`
    request for the whole column. A single very large read has been this
    workbook's most fragile, timeout-prone call (it kept failing even at a
    120s read timeout on the "Matches" tab) - several smaller, independently
    retried requests are each far more likely to succeed quickly even under
    real Google API strain, and a transient failure only costs a retry of
    that one chunk rather than the whole column."""
    total_rows = max(worksheet.row_count, 0)
    values: list[str] = []
    row = 1
    while row <= total_rows:
        end_row = min(row + chunk_rows - 1, total_rows)
        chunk = sheet_call(
            lambda row=row, end_row=end_row: worksheet.get(
                f"A{row}:A{end_row}", value_render_option="UNFORMATTED_VALUE"
            ),
            description=f"{description} (rows {row}-{end_row})",
        )
        for i in range(end_row - row + 1):
            values.append(str(chunk[i][0]).strip() if i < len(chunk) and chunk[i] else "")
        row = end_row + 1
    return values


def csv_id_values(path: Path, description: str) -> list[str]:
    """Read the first column from a just-written local CSV.

    The heavy data sync functions already write final ordered CSV mirrors
    before the lightweight lookup tabs are refreshed. Using those local IDs
    avoids a large immediate read-back from Sheets, which is both slower and
    prone to Google API 500/timeout responses on busy workbooks.
    """
    ids: list[str] = []
    try:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            next(reader, None)  # header
            for row in reader:
                item_id = normalize_sheet_value(value_at(row, 0))
                if item_id:
                    ids.append(item_id)
    except OSError as exc:
        raise RuntimeError(f"{description} failed: {exc}") from exc
    return ids


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
    # Without this, a stalled connection blocks forever - sheet_call's
    # retry/backoff logic only ever runs after a request actually raises,
    # so a request that just hangs waiting for a response (no error, no
    # timeout) can't be retried and the whole script sits there indefinitely.
    # 120s (not 60s): a full-column read of a several-thousand-row lookup
    # tab (e.g. sync_lookup_tab's column A read) has been observed to need
    # more than 60s on a busy workbook, which was tripping this timeout and
    # burning retries on reads that would have succeeded given more time.
    gc.set_timeout((10, 120))
    _ACTIVE_SESSIONS.append(gc.http_client.session)
    try:
        spreadsheet = sheet_call(
            lambda: gc.open_by_key(spreadsheet_id),
            description=f"Open spreadsheet {spreadsheet_id}",
        )
        memoize_worksheet_lookups(spreadsheet)
        return spreadsheet
    except PermissionError as exc:
        raise SystemExit(
            f"Permission denied opening spreadsheet {spreadsheet_id}. Share it with "
            f"{service_account_email} as an Editor and try again."
        ) from exc
    except RuntimeError as exc:
        if "PERMISSION_DENIED" in str(exc):
            raise SystemExit(
                f"Permission denied opening spreadsheet {spreadsheet_id}. Share it with "
                f"{service_account_email} as an Editor and try again."
            ) from exc
        raise


def memoize_worksheet_lookups(spreadsheet) -> None:
    """spreadsheet.worksheet(title) calls fetch_sheet_metadata() - a full
    metadata dump covering every tab in the spreadsheet (properties,
    conditional formats, filters, protected ranges, etc. for all of them,
    though not cell data) - EVERY time it's called, with no caching of its
    own. This workbook has 230+ tabs, so that dump is large, and this
    script calls spreadsheet.worksheet() well over a dozen times in a
    single run (once per tab it manages) - each one paying for a fresh
    fetch of EVERY tab's metadata just to find the one it wants. That's a
    major, entirely avoidable source of the repeated timeouts seen even on
    calls that look "simple" (opening a worksheet, a 1-2 cell spot-check) -
    they were never actually simple against this specific workbook.

    Patches this spreadsheet instance so the first .worksheet() call (for
    any title) fetches every tab's metadata ONCE and caches all of them;
    every later call this run, for any title, is served from memory with
    no further API call. get_or_create_worksheet also feeds a newly
    created worksheet straight into this same cache, so creating a tab
    never forces a second full re-fetch just to "discover" it."""
    cache: dict[str, Any] = {}
    spreadsheet._worksheet_cache = cache

    def populate() -> None:
        metadata = sheet_call(
            lambda: spreadsheet.fetch_sheet_metadata(),
            description="Fetch spreadsheet metadata",
        )
        for item in metadata.get("sheets", []):
            title = item["properties"]["title"]
            # setdefault, not overwrite: a repopulate (triggered by looking
            # up a title not yet cached, e.g. a genuinely new tab) must
            # never replace an already-cached Worksheet object - it may
            # have grown (add_rows/resize) since it was cached, and that
            # growth only lives on that object, not on the sheet's server
            # side metadata being re-fetched here.
            cache.setdefault(title, gspread.Worksheet(spreadsheet, item["properties"], spreadsheet.id, spreadsheet.client))

    def cached_worksheet(title: str):
        if not cache:
            populate()
        if title not in cache:
            populate()  # could be a tab created after the first snapshot
        if title not in cache:
            raise gspread.WorksheetNotFound(title)
        return cache[title]

    spreadsheet.worksheet = cached_worksheet


def get_or_create_worksheet(spreadsheet, worksheet_name: str, cols: int):
    try:
        # See memoize_worksheet_lookups - after the first call this run,
        # .worksheet() is served from an in-memory cache and this is no
        # longer a full-spreadsheet metadata fetch. gspread.WorksheetNotFound
        # isn't caught by sheet_call's except clause, so it still propagates
        # straight through to the fallback below, unretried, as intended.
        return sheet_call(
            lambda: spreadsheet.worksheet(worksheet_name),
            description=f"Open {worksheet_name} worksheet",
        )
    except gspread.WorksheetNotFound:
        try:
            worksheet = sheet_call(
                lambda: spreadsheet.add_worksheet(title=worksheet_name, rows=1, cols=cols),
                description=f"Create {worksheet_name} worksheet",
            )
        except RuntimeError as exc:
            # add_worksheet isn't idempotent, unlike everything else
            # sheet_call retries: if attempt 1's REQUEST actually created the
            # sheet server-side but its RESPONSE was lost to a client-side
            # timeout, every retry after that correctly reports "already
            # exists" - a permanent error from then on, not a transient one -
            # and burns the entire retry budget getting nowhere. Recognize
            # that specific case and just fetch the sheet a prior attempt
            # already created, instead of treating it as failure.
            if "already exists" not in str(exc):
                raise
            worksheet = sheet_call(
                lambda: spreadsheet.worksheet(worksheet_name),
                description=f"Open {worksheet_name} worksheet (already created by a prior attempt)",
            )
        cache = getattr(spreadsheet, "_worksheet_cache", None)
        if cache is not None:
            cache[worksheet_name] = worksheet
        return worksheet


def load_lookup_mirror(path: Path) -> list[str] | None:
    """A local, row-position-indexed mirror of a lookup tab's column A (one
    line per row starting at that tab's data_start_row, blank = a cleared
    gap) - lets sync_lookup_tab trust this instead of re-reading the whole
    column from the sheet every run. Returns None if there's no mirror yet
    (first run), so the caller knows it must bootstrap with a real read."""
    if not path.exists():
        return None
    with path.open(encoding="utf-8-sig", newline="") as f:
        return [(row[0].strip() if row else "") for row in csv.reader(f)]


def save_lookup_mirror(path: Path, ids_by_row: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerows([[value] for value in ids_by_row])
    temp.replace(path)


def sync_lookup_tab(
    spreadsheet,
    worksheet_name: str,
    ids: list[str],
    *,
    data_start_row: int,
    last_formula_column: str,
    mirror_path: Path | None = None,
) -> None:
    """Mirror `ids` into column A of `worksheet_name` - existing rows are
    NEVER rewritten or reordered, even if `ids` itself comes in a different
    order than last time (e.g. if the source *Data tab's own row order ever
    changed). Only genuinely new IDs get appended after the current last
    row; IDs no longer present get their row cleared in place, leaving a
    gap rather than shifting anything up. Every formula in this tab looks
    its own row's ID up by value (MATCH/XLOOKUP/INDEX), not by position, so
    a gap or a some-other-order-than-the-source costs nothing functionally
    - only row *position* stability for existing entries matters here.

    `mirror_path`, when given, is a local file remembering this tab's
    column A from the end of the last run. Since this function guarantees
    existing rows never move, that mirror should still describe the sheet
    exactly - so it's trusted in place of a full column read (this
    workbook's most common transient-timeout point) after one cheap
    single-cell spot-check confirms nothing's drifted (e.g. a manual edit,
    or a previous run that wrote to the sheet but crashed before saving the
    mirror). Any mismatch there falls back to a real read for this run."""
    last_col_index = gspread.utils.a1_to_rowcol(f"{last_formula_column}1")[1]
    worksheet = get_or_create_worksheet(spreadsheet, worksheet_name, last_col_index)

    existing_ids_raw = load_lookup_mirror(mirror_path) if mirror_path else None
    if existing_ids_raw is not None:
        # Two things could have drifted since the mirror was last saved: the
        # sheet's own last row might no longer match it (e.g. a manual edit),
        # or the sheet might have MORE rows than the mirror knows about (e.g.
        # a run that appended to the sheet but crashed before saving the
        # mirror, or the mirror file itself got overwritten by a stale
        # commit from a git merge between the local machine and CI). One
        # small range read checks both at once: the mirror's own last row,
        # plus the row right after it, which must be blank.
        try:
            last_row = data_start_row - 1 + len(existing_ids_raw)
            if existing_ids_raw:
                rows = sheet_call(
                    lambda: worksheet.get(f"A{last_row}:A{last_row + 1}", value_render_option="UNFORMATTED_VALUE"),
                    description=f"Spot-check {worksheet_name} column A",
                )
                actual_last = str(rows[0][0]).strip() if rows and rows[0] else ""
                actual_next = str(rows[1][0]).strip() if len(rows) > 1 and rows[1] else ""
                mismatch = actual_last != existing_ids_raw[-1] or bool(actual_next)
            else:
                rows = sheet_call(
                    lambda: worksheet.get(f"A{data_start_row}:A{data_start_row}", value_render_option="UNFORMATTED_VALUE"),
                    description=f"Spot-check {worksheet_name} column A",
                )
                mismatch = bool(rows and rows[0] and str(rows[0][0]).strip())
        except Exception:
            mismatch = True  # any spot-check trouble - just resync below
        if mismatch:
            safe_print(
                f"{worksheet_name} tab: local mirror doesn't match the sheet "
                "(spot-check failed) - doing a full column read to resync."
            )
            existing_ids_raw = None

    if existing_ids_raw is None:
        # Either there's no mirror yet, or the spot-check above didn't
        # confirm it - this read is what lets existing rows stay untouched
        # no matter what order `ids` arrives in (see docstring), and (once
        # the mirror is saved below) shouldn't be needed again next run.
        # Chunked (see read_column_a_chunked) rather than one big col_values()
        # call, since a single request for the whole column is exactly what's
        # been timing out even at a 120s read timeout.
        existing_ids_raw = read_column_a_chunked(
            worksheet, description=f"Read {worksheet_name} column A"
        )[data_start_row - 1 :]

    existing_row_by_id: dict[str, int] = {}
    for offset, raw in enumerate(existing_ids_raw):
        text = str(raw).strip()
        if text:
            existing_row_by_id[text] = data_start_row + offset

    wanted_ids = {str(item_id) for item_id in ids}
    new_ids = [item_id for item_id in ids if str(item_id) not in existing_row_by_id]
    removed_ids = [eid for eid in existing_row_by_id if eid not in wanted_ids]

    # Clear rows for IDs no longer present, in place - a gap, not a
    # reshuffle. Existing rows must never move.
    if removed_ids:
        ranges = [
            f"A{existing_row_by_id[rid]}:{last_formula_column}{existing_row_by_id[rid]}"
            for rid in removed_ids
        ]
        sheet_call(
            lambda ranges=ranges: worksheet.batch_clear(ranges),
            description=f"Clear {worksheet_name} rows for removed IDs",
        )
        for rid in removed_ids:
            idx = existing_row_by_id[rid] - data_start_row
            existing_ids_raw[idx] = ""

    if not new_ids:
        if mirror_path:
            save_lookup_mirror(mirror_path, existing_ids_raw)
        safe_print(
            f"{worksheet_name} tab: no new IDs to add "
            f"({len(removed_ids):,} removed, existing rows untouched)."
        )
        return

    last_row_before = data_start_row - 1 + len(existing_ids_raw)
    formula_last_row = last_row_before
    new_last_row = last_row_before + len(new_ids)

    if new_last_row > worksheet.row_count:
        sheet_call(
            lambda: worksheet.add_rows(new_last_row - worksheet.row_count),
            description=f"Grow {worksheet_name} sheet",
        )

    # MATCH/XLOOKUP/INDEX in this tab's formula columns do exact-type
    # matching against the source *Data tab's ID column - both are kept
    # as plain ints so a RAW write lands as NUMBER on both sides with no
    # extra formatting pass needed.
    sheet_call(
        lambda: worksheet.update(
            values=[[int(item_id)] for item_id in new_ids],
            range_name=f"A{last_row_before + 1}:A{new_last_row}",
            value_input_option="RAW",
        ),
        description=f"Append new IDs to {worksheet_name} column A",
    )

    added_rows = new_last_row - formula_last_row
    if added_rows > 0 and formula_last_row >= data_start_row:
        # copyPaste refuses to run at all if a basic filter (Data > Create a
        # filter) is hiding any row in range - clear it first. Best-effort:
        # this fails harmlessly if there's no filter to remove.
        try:
            sheet_call(
                lambda: spreadsheet.batch_update(
                    {"requests": [{"clearBasicFilter": {"sheetId": worksheet.id}}]}
                ),
                description=f"Clear {worksheet_name} basic filter",
            )
        except (gspread.exceptions.APIError, RuntimeError):
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

    if mirror_path:
        save_lookup_mirror(mirror_path, existing_ids_raw + [str(item_id) for item_id in new_ids])

    safe_print(
        f"{worksheet_name} tab: appended {len(new_ids):,} new ID(s), "
        f"cleared {len(removed_ids):,} removed row(s), "
        f"copied formulas into {max(0, added_rows):,} new row(s)."
    )


def sync_players_tab(spreadsheet, playerdata_ids: list[str]) -> None:
    sync_lookup_tab(
        spreadsheet,
        PLAYERS_WORKSHEET,
        playerdata_ids,
        data_start_row=PLAYERS_DATA_START_ROW,
        last_formula_column=PLAYERS_LAST_FORMULA_COLUMN,
        mirror_path=PLAYERS_LOOKUP_MIRROR,
    )


def sync_leagues_tab(spreadsheet, competition_ids: list[str]) -> None:
    sync_lookup_tab(
        spreadsheet,
        LEAGUES_WORKSHEET,
        competition_ids,
        data_start_row=LEAGUES_DATA_START_ROW,
        last_formula_column=LEAGUES_LAST_FORMULA_COLUMN,
        mirror_path=LEAGUES_LOOKUP_MIRROR,
    )


def sync_club_tab(spreadsheet, clubdata_ids: list[str]) -> None:
    sync_lookup_tab(
        spreadsheet,
        CLUB_WORKSHEET,
        clubdata_ids,
        data_start_row=CLUB_DATA_START_ROW,
        last_formula_column=CLUB_LAST_FORMULA_COLUMN,
        mirror_path=CLUB_LOOKUP_MIRROR,
    )


def sync_matches_tab(spreadsheet, matchdata_ids: list[str]) -> None:
    sync_lookup_tab(
        spreadsheet,
        MATCHES_WORKSHEET,
        matchdata_ids,
        data_start_row=MATCHES_DATA_START_ROW,
        last_formula_column=MATCHES_LAST_FORMULA_COLUMN,
        mirror_path=MATCHES_LOOKUP_MIRROR,
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


def ensure_individual_result_formula_rows(
    destination_spreadsheet,
    destination,
    start_row: int,
    end_row: int,
) -> int:
    """Copy the formula columns from the row just above start_row down
    through end_row, unconditionally overwriting whatever's already there
    rather than trusting it. This sheet is pre-filled with formulas
    thousands of rows ahead of real data (created in bulk at some past
    point, using whatever formula pattern was current then); the previous
    version of this function skipped copying whenever a destination row
    "already had a formula" - which every one of those pre-filled rows
    always does, correct or not. That silently let a stale, since-fixed
    formula pattern persist in every pre-filled row forever: each night's
    new matches landed in the next pre-filled row and inherited whatever
    old formula was already sitting there, un-refreshed. Since these rows
    get real match data written into their data columns immediately after
    this call anyway, unconditionally refreshing the formula columns from
    the last known-good row costs nothing extra and removes that fragile
    assumption entirely."""
    if end_row > destination.row_count:
        sheet_call(
            lambda: destination.add_rows(end_row - destination.row_count),
            description="Grow Individual Results tab",
        )

    source_row = start_row - 1
    if source_row < 1:
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
                        "startRowIndex": start_row - 1,
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
    return end_row - start_row + 1


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
    source_matches = sheet_call(lambda: spreadsheet.worksheet(MATCHES_WORKSHEET), description=f"Open {MATCHES_WORKSHEET} worksheet")
    source_matchdata = sheet_call(lambda: spreadsheet.worksheet(args.match_worksheet), description=f"Open {args.match_worksheet} worksheet")

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
                row = apply_number_ids(future.result(), gmgr.HEADERS, manager_id_headers)
            except Exception as exc:
                errors.append((manager_id, str(exc)))
                safe_print(f"ERROR (manager) {manager_id}: {exc}")
                gmatch.progress("Fetching managers", done, total)
                continue
            if manager_id in existing_by_id:
                updates.append((existing_by_id[manager_id], row))
            else:
                appended_rows.append(row)
            gmatch.progress("Fetching managers", done, total)

    last_col = column_letters(len(gmgr.HEADERS))
    failed_rows = (
        write_row_batches(updates, args.sheet_batch_size, worksheet=worksheet, last_col=last_col, label="ManagerData")
        if updates
        else set()
    )
    appended_ok = (
        append_row_batches(appended_rows, args.sheet_batch_size, worksheet=worksheet, label="ManagerData")
        if appended_rows
        else []
    )

    final_by_id: dict[str, list[Any]] = {
        manager_id: list(existing_values[row_number - 1])
        for manager_id, row_number in existing_by_id.items()
    }
    for row_number, row in updates:
        if row_number not in failed_rows:
            final_by_id[str(row[manager_id_col])] = row
    for row in appended_ok:
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
        f"ManagerData done. {len(appended_ok):,}/{len(appended_rows):,} new managers added, "
        f"{len(updates) - len(failed_rows):,}/{len(updates):,} existing managers refreshed, "
        f"{len(errors):,} fetch failure(s)."
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
                row = apply_number_ids(future.result(), gcomp.HEADERS, competition_id_headers)
            except Exception as exc:
                errors.append((competition_id, str(exc)))
                safe_print(f"ERROR (competition) {competition_id}: {exc}")
                gmatch.progress("Fetching competitions", done, total)
                continue
            if competition_id in existing_by_id:
                updates.append((existing_by_id[competition_id], row))
            else:
                appended_rows.append(row)
            gmatch.progress("Fetching competitions", done, total)

    last_col = column_letters(len(gcomp.HEADERS))
    failed_rows = (
        write_row_batches(updates, args.sheet_batch_size, worksheet=worksheet, last_col=last_col, label="CompetitionData")
        if updates
        else set()
    )
    appended_ok = (
        append_row_batches(appended_rows, args.sheet_batch_size, worksheet=worksheet, label="CompetitionData")
        if appended_rows
        else []
    )

    final_by_id: dict[str, list[Any]] = {
        competition_id: list(existing_values[row_number - 1])
        for competition_id, row_number in existing_by_id.items()
    }
    for row_number, row in updates:
        if row_number not in failed_rows:
            final_by_id[str(row[competition_id_col])] = row
    for row in appended_ok:
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
        f"CompetitionData done. {len(appended_ok):,}/{len(appended_rows):,} new competitions added, "
        f"{len(updates) - len(failed_rows):,}/{len(updates):,} existing competitions refreshed, "
        f"{len(errors):,} fetch failure(s)."
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


def snapshot_pre_match_projections(spreadsheet, args: argparse.Namespace) -> dict[str, dict[str, Any]]:
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

    Returns Match ID -> {SNAPSHOT_HEADERS field: value} for every
    not-yet-started match with a projection available this run (the
    win/draw/away fields are fractions 0-1, not whole-number percentages -
    callers comparing against a whole-number source, e.g. odds-implied
    probabilities, need to multiply by 100). sync_bets reuses this
    in-memory instead of re-reading MatchData, since it needs exactly the
    same "what did the model believe right now" snapshot.
    """
    match_worksheet = sheet_call(lambda: spreadsheet.worksheet(args.match_worksheet), description=f"Open {args.match_worksheet} worksheet")
    matches_worksheet = sheet_call(lambda: spreadsheet.worksheet(MATCHES_WORKSHEET), description=f"Open {MATCHES_WORKSHEET} worksheet")

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
                values=[SNAPSHOT_HEADERS],
                range_name=f"{snap_start_letter}1:{snap_end_letter}1",
                value_input_option="RAW",
            ),
            description="Write MatchData snapshot header",
        )

    matchdata_values = read_existing_values(match_worksheet, "Read MatchData for snapshot")
    if len(matchdata_values) < 2:
        safe_print("Snapshot: MatchData is empty; nothing to do.")
        return {}
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
    snapshots: dict[str, dict[str, Any]] = {}
    for row in matches_values:
        match_id = normalize_sheet_value(value_at(row, 0))
        if not match_id or match_id not in row_by_match_id:
            continue
        if normalize_sheet_value(value_at(row, MATCHES_STARTED_COL)) in ("1", "true"):
            continue  # already kicked off - whatever was last captured stays frozen

        values = [value_at(row, c) for c in MATCHES_SNAPSHOT_SOURCE_COLS]
        if values[0] == "" and values[1] == "":
            continue  # no projection available yet (e.g. not enough matches played for a rating)

        snapshots[match_id] = dict(zip(SNAPSHOT_HEADERS, values))
        updates.append({
            "range": f"{snap_start_letter}{row_by_match_id[match_id]}:{snap_end_letter}{row_by_match_id[match_id]}",
            "values": [values],
        })

    if updates:
        for start in range(0, len(updates), args.sheet_batch_size):
            chunk = updates[start : start + args.sheet_batch_size]
            sheet_call(
                # batch_update mutates its input (prefixes "range" with the
                # worksheet title) - pass fresh copies so a retry after a
                # transient failure doesn't re-prefix an already-prefixed
                # range (this is what turned into the "'MatchData'!'MatchData'!
                # ..." 400 errors: each retry re-wrapped the previous
                # attempt's already-qualified range).
                lambda chunk=chunk: match_worksheet.batch_update(
                    [dict(item) for item in chunk], value_input_option="RAW"
                ),
                description=f"Write pre-match snapshots {start + 1}-{start + len(chunk)}",
            )
    safe_print(f"Snapshot: captured pre-match projections for {len(updates):,} not-yet-started match(es).")
    return snapshots


BET_HEADERS = [
    "Match ID", "Competition ID", "Date", "Home Team", "Away Team",
    "Sky Bet Home Odds", "Sky Bet Draw Odds", "Sky Bet Away Odds",
    "Implied Home %", "Implied Draw %", "Implied Away %",
    "Model Home %", "Model Draw %", "Model Away %",
    "Edge Home", "Edge Draw", "Edge Away",
    "Recommended Pick", "Recommended Edge",
    "Stake", "Result", "Profit/Loss", "Odds Retrieved UTC",
    # Appended at the end, not inserted in the middle, so a sheet already
    # populated under the pre-Kelly layout keeps every existing column's
    # position - old rows just get a blank cell here rather than every
    # later column silently shifting and corrupting already-written data.
    "Bankroll At Pick",
]
BET_OUTCOME_ODDS_COLUMN = {
    "Home Win": "Sky Bet Home Odds", "Draw": "Sky Bet Draw Odds", "Away Win": "Sky Bet Away Odds",
}
BET_OUTCOME_MODEL_PCT_COLUMN = {
    "Home Win": "Model Home %", "Draw": "Model Draw %", "Away Win": "Model Away %",
}
# getOdds.collect_odds() rows use its own HEADERS ("Home Odds"/"Draw Odds"/
# "Away Odds"), not BetData's sheet column names - a separate mapping from
# BET_OUTCOME_ODDS_COLUMN, which is keyed for BetData rows instead.
BET_OUTCOME_ODDS_ROW_KEY = {
    "Home Win": "Home Odds", "Draw": "Draw Odds", "Away Win": "Away Odds",
}
# Quarter Kelly: full Kelly is the mathematically "optimal" bankroll
# fraction for long-run growth IF the model's probability is exactly
# right, but real bettors almost never use it undiluted - it's brutally
# punishing when a probability estimate is even slightly off, and this
# system already has known data/name-matching quirks that can occasionally
# inflate an "edge". A quarter of full Kelly trades some growth rate for a
# much smoother, more forgiving bankroll curve.
KELLY_FRACTION = 0.25
# Hard ceiling regardless of what Kelly suggests - protects against a
# single wrong edge estimate (bad odds match, data glitch) suggesting a
# dangerously large stake. 5% of bankroll on one bet is already a lot;
# this is a backstop, not a target.
MAX_STAKE_FRACTION = 0.05


def grade_bet(recommended_pick: str, actual_result: str) -> str:
    if recommended_pick == "No Bet":
        return "No Bet"
    if not actual_result:
        return "Pending"
    return "Won" if recommended_pick == actual_result else "Lost"


def bet_profit_loss(result: str, stake: float, odds: float) -> float:
    """stake * (odds - 1) if the bet won, -stake if lost, 0 otherwise
    (pending/void/no-bet). This system recommends and tracks picks against
    a tracked bankroll - it never places a real bet or touches a
    bookmaker account."""
    if result == "Won":
        return round(stake * (odds - 1), 2)
    if result == "Lost":
        return round(-stake, 2)
    return 0.0


def kelly_stake(model_probability: float, odds: float, bankroll: float) -> float:
    """Quarter-Kelly stake in the same currency as `bankroll`. Full Kelly's
    fraction is f* = (p*odds - 1) / (odds - 1) - the fraction of bankroll
    that maximizes long-run geometric growth if `model_probability` is
    exactly correct. Clamped to >= 0 (a non-positive f* means no real edge,
    which shouldn't happen once a pick has already cleared
    --bet-edge-threshold, but this is a defensive floor, not an
    assumption) and capped at MAX_STAKE_FRACTION of bankroll regardless of
    what the raw formula suggests."""
    if odds <= 1 or bankroll <= 0:
        return 0.0
    full_kelly_fraction = max(0.0, (model_probability * odds - 1) / (odds - 1))
    fraction = min(full_kelly_fraction * KELLY_FRACTION, MAX_STAKE_FRACTION)
    return round(fraction * bankroll, 2)


def current_bankroll(starting_bankroll: float, rows: Iterable[list[Any]], col: dict[str, int]) -> float:
    """Starting bankroll plus every graded (Won/Lost) bet's Profit/Loss so
    far - Kelly stakes compound against this, the same way a real bettor's
    stakes would grow or shrink with their actual results, not against a
    number fixed at the start forever."""
    total = starting_bankroll
    for row in rows:
        if row[col["Result"]] in ("Won", "Lost"):
            try:
                total += float(row[col["Profit/Loss"]] or 0)
            except (TypeError, ValueError):
                pass
    return total


def sync_bets(
    spreadsheet,
    args: argparse.Namespace,
    all_matches: dict[str, dict[str, Any]] | None,
    snapshots: dict[str, dict[str, Any]],
) -> list[tuple[str, str]]:
    """Settle previously-recommended bets whose match has since finished,
    then fetch fresh Sky Bet odds (getOdds.collect_odds) for not-yet-started
    matches and record a value-bet recommendation for each one where the
    model's edge over the odds' implied probability clears
    --bet-edge-threshold. Structured like every other sync_* function
    (existing_by_id, updates/appended_rows, write_row_batches/
    append_row_batches, CSV rebuild) for the same reasons they are.

    Settlement runs whenever match results are available, independent of
    whether an odds API key is configured - so bets already on the board
    still get graded even on a run where fresh odds aren't fetched.

    A real recommendation, once made, is locked in - later runs don't
    recompute or overwrite an existing non-"No Bet" pick just because the
    odds or model moved before kickoff, the same way a real bet can't be
    silently swapped for a different outcome after it's placed. Only a
    brand-new match, or one still sitting at "No Bet", gets its pick
    (re)computed on a later run."""
    worksheet = get_or_create_worksheet(spreadsheet, args.odds_worksheet, len(BET_HEADERS))
    existing_values = read_existing_values(worksheet, "Read BetData sheet")

    if not existing_values:
        sheet_call(lambda: worksheet.update([BET_HEADERS]), description="Write BetData header")
        existing_values = [BET_HEADERS]

    if existing_values[0] != BET_HEADERS:
        raise SystemExit(
            "BetData's header row doesn't match this script's current "
            "BET_HEADERS layout. Clear the sheet or fix the header before syncing."
        )

    col = {name: i for i, name in enumerate(BET_HEADERS)}
    existing_by_id: dict[str, tuple[int, list[Any]]] = {}
    for offset, row in enumerate(existing_values[1:]):
        padded = row + [""] * (len(BET_HEADERS) - len(row))
        match_id = str(padded[col["Match ID"]]).strip()
        if match_id:
            existing_by_id[match_id] = (offset + 2, padded)  # +2: header row + 1-based

    errors: list[tuple[str, str]] = []
    updates: list[tuple[int, list[Any]]] = []
    settled_count = 0

    # 1. Settle pending bets whose match has since finished - reuses the
    # same all_matches dict and "Finished"=="1" gate as
    # players_who_played_recently, since it's the same "did this match
    # just conclude" question.
    if all_matches:
        for match_id, (row_number, row) in existing_by_id.items():
            if row[col["Result"]] != "Pending":
                continue
            match = all_matches.get(match_id)
            if not match or str(match.get("Finished", "")) != "1":
                continue
            if str(match.get("Cancelled", "")) == "1":
                row[col["Result"]] = "Void"
                row[col["Profit/Loss"]] = 0.0
                updates.append((row_number, row))
                settled_count += 1
                continue
            actual_result = str(match.get("Result", ""))  # "Home Win"/"Away Win"/"Draw"
            recommended = str(row[col["Recommended Pick"]])
            result = grade_bet(recommended, actual_result)
            odds_col = BET_OUTCOME_ODDS_COLUMN.get(recommended)
            odds = float(row[col[odds_col]]) if odds_col and row[col[odds_col]] not in ("", None) else 0.0
            # The stake actually recorded on this row when the pick was
            # made (Kelly-sized against the bankroll at that time) - not
            # today's --starting-bankroll or today's bankroll, since a
            # placed bet's size doesn't change after the fact.
            stake = float(row[col["Stake"]] or 0)
            row[col["Result"]] = result
            row[col["Profit/Loss"]] = bet_profit_loss(result, stake, odds)
            updates.append((row_number, row))
            settled_count += 1

    # Starting bankroll plus every settlement above (and every prior run's) -
    # new picks this run are Kelly-sized against this, so the stakes
    # actually compound with real results instead of staying fixed forever.
    bankroll = current_bankroll(
        args.starting_bankroll, (row for _, row in existing_by_id.values()), col
    )

    # 2. Fetch fresh odds + compute new picks for not-yet-started matches.
    appended_rows: list[list[Any]] = []
    if all_matches and args.odds_api_key:
        competition_map = godds.load_competition_map(args.odds_competition_map)
        odds_rows, odds_errors = godds.collect_odds(
            all_matches, competition_map, args.odds_api_key,
            region=args.odds_region, window_days=args.odds_upcoming_window_days,
            retries=args.odds_retries, request_delay=args.odds_request_delay,
        )
        errors.extend((item, error) for _kind, item, error in odds_errors)

        for match_id, odds_row in odds_rows.items():
            # A real pick, once made, is locked in - a genuine bet can't be
            # silently swapped for a different outcome because odds moved
            # later in the week. Only a match with no row yet, or one still
            # sitting at "No Bet" (never actually a commitment), gets its
            # recommendation (re)computed here.
            existing_row = existing_by_id.get(match_id)
            if existing_row and existing_row[1][col["Recommended Pick"]] not in ("", "No Bet"):
                continue

            snapshot = snapshots.get(match_id)
            if not snapshot:
                continue  # no model projection available for this match yet

            model_h = float(snapshot.get("SnapHomeWinPct") or 0) * 100
            model_d = float(snapshot.get("SnapDrawPct") or 0) * 100
            model_a = float(snapshot.get("SnapAwayWinPct") or 0) * 100
            implied_h = float(odds_row["Implied Home %"])
            implied_d = float(odds_row["Implied Draw %"])
            implied_a = float(odds_row["Implied Away %"])
            edge_h, edge_d, edge_a = model_h - implied_h, model_d - implied_d, model_a - implied_a

            best_pick, best_edge = max(
                [("Home Win", edge_h), ("Draw", edge_d), ("Away Win", edge_a)], key=lambda pair: pair[1]
            )
            if best_edge < args.bet_edge_threshold * 100:
                best_pick, best_edge = "No Bet", best_edge

            match = all_matches.get(match_id, {})
            row_values: list[Any] = [""] * len(BET_HEADERS)
            row_values[col["Match ID"]] = int(match_id)
            row_values[col["Competition ID"]] = (
                int(odds_row["Competition ID"]) if odds_row.get("Competition ID") else ""
            )
            row_values[col["Date"]] = str(match.get("Match UTC", ""))[:10]
            row_values[col["Home Team"]] = odds_row["Home Team"]
            row_values[col["Away Team"]] = odds_row["Away Team"]
            row_values[col["Sky Bet Home Odds"]] = odds_row["Home Odds"]
            row_values[col["Sky Bet Draw Odds"]] = odds_row["Draw Odds"]
            row_values[col["Sky Bet Away Odds"]] = odds_row["Away Odds"]
            row_values[col["Implied Home %"]] = implied_h
            row_values[col["Implied Draw %"]] = implied_d
            row_values[col["Implied Away %"]] = implied_a
            row_values[col["Model Home %"]] = round(model_h, 2)
            row_values[col["Model Draw %"]] = round(model_d, 2)
            row_values[col["Model Away %"]] = round(model_a, 2)
            row_values[col["Edge Home"]] = round(edge_h, 2)
            row_values[col["Edge Draw"]] = round(edge_d, 2)
            row_values[col["Edge Away"]] = round(edge_a, 2)
            row_values[col["Recommended Pick"]] = best_pick
            row_values[col["Recommended Edge"]] = round(best_edge, 2)
            if best_pick == "No Bet":
                stake = 0.0
            else:
                pick_model_pct = {"Home Win": model_h, "Draw": model_d, "Away Win": model_a}[best_pick]
                pick_odds = float(odds_row[BET_OUTCOME_ODDS_ROW_KEY[best_pick]])
                stake = kelly_stake(pick_model_pct / 100, pick_odds, bankroll)
            row_values[col["Stake"]] = stake
            row_values[col["Bankroll At Pick"]] = round(bankroll, 2)
            row_values[col["Result"]] = "No Bet" if best_pick == "No Bet" else "Pending"
            row_values[col["Profit/Loss"]] = 0.0
            row_values[col["Odds Retrieved UTC"]] = odds_row["Odds Retrieved UTC"]

            if match_id in existing_by_id:
                row_number, _ = existing_by_id[match_id]
                updates.append((row_number, row_values))
            else:
                appended_rows.append(row_values)

    last_col = column_letters(len(BET_HEADERS))
    failed_rows = (
        write_row_batches(updates, args.sheet_batch_size, worksheet=worksheet, last_col=last_col, label="BetData")
        if updates else set()
    )
    appended_ok = (
        append_row_batches(appended_rows, args.sheet_batch_size, worksheet=worksheet, label="BetData")
        if appended_rows else []
    )

    final_by_id: dict[str, list[Any]] = {
        match_id: row for match_id, (_, row) in existing_by_id.items()
    }
    for row_number, row in updates:
        if row_number not in failed_rows:
            final_by_id[str(row[col["Match ID"]])] = row
    for row in appended_ok:
        final_by_id[str(row[col["Match ID"]])] = row

    tmp_path = args.bet_csv_output.with_suffix(args.bet_csv_output.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(BET_HEADERS)
        writer.writerows(final_by_id.values())
    tmp_path.replace(args.bet_csv_output)

    safe_print(
        f"BetData done. {settled_count:,} bet(s) settled, {len(appended_ok):,}/{len(appended_rows):,} new pick(s) added, "
        f"{len(updates) - len(failed_rows):,}/{len(updates):,} row(s) written, {len(errors):,} odds error(s)."
    )
    return errors


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
        "Round",
        "Home Club ID",
        "Away Club ID",
        "Winner Club ID",
        "Player Of The Match ID",
        "Player Of The Match Club ID",
    ]

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

    # The sheet only ever holds in-scope matches (out-of-scope rows get
    # deleted below), so an out-of-scope match's Round/Competition info can
    # never be reused via existing_values above - it would always look
    # "never seen" and get re-fetched every run purely to be discarded again.
    # This local cache remembers every match seen last run regardless of
    # scope, closing that gap.
    local_cache = gmatch.load_all_rows(args.match_local_cache)
    reused_from_local_cache = 0
    for match_id, row in local_cache.items():
        if match_id not in cached_all:
            cached_all[match_id] = row
            reused_from_local_cache += 1
        if match_id not in cached and str(row.get("Detailed Data", "")) == "1":
            cached[match_id] = row
    if reused_from_local_cache:
        safe_print(
            f"MatchData: {reused_from_local_cache:,} additional out-of-scope match(es) "
            f"reused from the local cache ({args.match_local_cache.name})."
        )

    parent_cache = gmatch.load_parent_cache(args.competition_parents_cache)
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
        keep_competition_ids=keep_competition_ids,
        parent_cache=parent_cache,
    )
    gmatch.save_parent_cache(args.competition_parents_cache, parent_cache)

    # Coalesce this run's results onto the previous local cache (non-blank
    # new values win, otherwise keep what was already known) so a lighter
    # --match-mode fixtures run can't blank out previously-learned Round/
    # detail info for matches it didn't touch. Rebuilt from `matches` only,
    # so entries for clubs/seasons no longer in scope drop off naturally.
    merged_local_cache: dict[str, dict[str, Any]] = {}
    for match_id, row in matches.items():
        old_row = local_cache.get(match_id, {})
        merged_local_cache[match_id] = {**old_row, **{k: v for k, v in row.items() if v not in (None, "")}}
    gmatch.save_all_rows(args.match_local_cache, merged_local_cache)

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
        row = apply_number_ids([row_dict.get(header, "") for header in gmatch.HEADERS], gmatch.HEADERS, match_id_headers)
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
    failed_rows = (
        write_row_batches(updates, args.sheet_batch_size, worksheet=worksheet, last_col=last_col, label="MatchData")
        if updates
        else set()
    )
    appended_ok = (
        append_row_batches(appended_rows, args.sheet_batch_size, worksheet=worksheet, label="MatchData")
        if appended_rows
        else []
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
        if row_number not in failed_rows:
            final_by_id[str(row[match_id_col])] = row
    for row in appended_ok:
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
        f"MatchData done. {len(appended_ok):,}/{len(appended_rows):,} new matches added, "
        f"{len(updates) - len(failed_rows):,}/{len(updates):,} existing matches refreshed, "
        f"{len(errors):,} fetch failure(s)."
    )
    return errors, matches


def fetch_club_row(club_id: str, args: argparse.Namespace) -> list[Any]:
    row = gclub.build_row(club_id, retries=args.club_retries, request_delay=args.club_request_delay)
    club_id_col = gclub.HEADERS.index("Club ID")
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
                row = apply_number_ids(future.result(), gclub.HEADERS, club_id_headers)
            except Exception as exc:
                errors.append((club_id, str(exc)))
                # A deleted/merged club ID is permanent, not worth retrying,
                # and not a real problem with the sync itself - don't let it
                # fail the whole run the way an unexpected error should.
                if not isinstance(exc, gclub.ClubNotFoundError):
                    fatal_errors.append((club_id, str(exc)))
                safe_print(f"ERROR (club) {club_id}: {exc}")
                gmatch.progress("Fetching clubs", done, total)
                continue
            if club_id in existing_by_id:
                updates.append((existing_by_id[club_id], row))
            else:
                appended_rows.append(row)
            gmatch.progress("Fetching clubs", done, total)

    last_col = column_letters(len(gclub.HEADERS))
    failed_rows = (
        write_row_batches(updates, args.sheet_batch_size, worksheet=worksheet, last_col=last_col, label="ClubData")
        if updates
        else set()
    )
    appended_ok = (
        append_row_batches(appended_rows, args.sheet_batch_size, worksheet=worksheet, label="ClubData")
        if appended_rows
        else []
    )

    final_by_id: dict[str, list[Any]] = {
        club_id: list(existing_values[row_number - 1])
        for club_id, row_number in existing_by_id.items()
    }
    for row_number, row in updates:
        if row_number not in failed_rows:
            final_by_id[str(row[club_id_col])] = row
    for row in appended_ok:
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
        f"ClubData done. {len(appended_ok):,}/{len(appended_rows):,} new clubs added, "
        f"{len(updates) - len(failed_rows):,}/{len(updates):,} existing clubs refreshed, "
        f"{len(errors):,} failed ({not_found_count:,} permanently gone, {len(fatal_errors):,} unexpected)."
    )
    return fatal_errors


def players_who_played_recently(
    all_matches: dict[str, dict[str, Any]] | None, window_hours: float
) -> set[str] | None:
    """Player IDs who appeared (started or were an available substitute) in
    a finished match within the last `window_hours`. Returns None if there's
    no fresh match data to work from (e.g. --skip-matches this run), so
    callers can fall back to refreshing everyone rather than silently
    refreshing nobody."""
    if not all_matches:
        return None
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    players: set[str] = set()
    for row in all_matches.values():
        if str(row.get("Finished", "")) != "1":
            continue
        match_utc = str(row.get("Match UTC", ""))
        try:
            match_dt = datetime.fromisoformat(match_utc.replace("Z", "+00:00"))
        except ValueError:
            continue
        if match_dt < cutoff:
            continue
        for field in (
            "Home Starter IDs",
            "Away Starter IDs",
            "Home Substitute IDs",
            "Away Substitute IDs",
        ):
            ids_str = str(row.get(field, "") or "")
            players.update(pid for pid in ids_str.split("|") if pid)
    return players


def sync_player_data(
    spreadsheet, args: argparse.Namespace, players_to_refresh: set[str] | None = None
) -> list[tuple[str, str]]:
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

    existing_by_id: dict[str, tuple[int, list[str]]] = {}
    for offset, row in enumerate(existing_values[1:]):
        player_id = str(row[ID_COL]).strip() if len(row) > ID_COL else ""
        if player_id:
            existing_by_id[player_id] = (offset + 2, row)  # +2: header row + 1-based

    new_ids = [pid for pid in player_ids if pid not in existing_by_id]
    existing_ids = [pid for pid in player_ids if pid in existing_by_id]
    if players_to_refresh is not None:
        skipped = [pid for pid in existing_ids if pid not in players_to_refresh]
        existing_ids = [pid for pid in existing_ids if pid in players_to_refresh]
        safe_print(
            f"New players: {len(new_ids):,}; existing players refreshed (played "
            f"recently): {len(existing_ids):,}; existing players left untouched: "
            f"{len(skipped):,}."
        )
    else:
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
                row = apply_number_ids(future.result(), gp.HEADERS, player_id_headers)
            except Exception as exc:
                errors.append((player_id, str(exc)))
                safe_print(f"ERROR ({kind}) {player_id}: {exc}")
                gmatch.progress("Fetching players", done, total)
                continue
            if kind == "new":
                appended_rows.append(row)
            else:
                row_number, _ = existing_by_id[player_id]
                updates.append((row_number, row))
            gmatch.progress("Fetching players", done, total)

    # Push updates to existing rows first, then append brand-new rows.
    # PlayerData's ~730 columns are far wider than the other tabs
    # --sheet-batch-size was tuned for, so its own batches are scaled down
    # to keep roughly the same total cell count per request (see
    # scaled_batch_size).
    last_col = column_letters(len(gp.HEADERS))
    player_batch_size = scaled_batch_size(args.sheet_batch_size, len(gp.HEADERS))
    # PlayerData rows are read back as strings from the sheet - diffed
    # against the freshly fetched row (ints/floats/etc.) via
    # normalize_sheet_value inside diff_row_ranges, so a value that hasn't
    # actually changed doesn't get miscounted as different just because of
    # its Python type.
    old_rows_by_row_number = {row_number: old_row for row_number, old_row in existing_by_id.values()}
    failed_rows = (
        write_row_batches(
            updates, player_batch_size, worksheet=worksheet, last_col=last_col, label="PlayerData",
            old_rows=old_rows_by_row_number,
        )
        if updates
        else set()
    )
    appended_ok = (
        append_row_batches(appended_rows, player_batch_size, worksheet=worksheet, label="PlayerData")
        if appended_rows
        else []
    )

    # Rebuild the CSV mirror from the same in-memory dataset (existing rows +
    # this run's updates/appends) so it always matches the sheet - a row
    # whose write_row_batches call failed keeps its OLD existing_by_id data
    # here rather than the freshly-fetched-but-never-written value, and a
    # row from a failed append batch is left out entirely, since neither
    # ever actually made it onto the sheet.
    final_by_id: dict[str, list[Any]] = {
        player_id: list(row) for player_id, (_, row) in existing_by_id.items()
    }
    for row_number, row in updates:
        if row_number not in failed_rows:
            final_by_id[str(row[ID_COL])] = row
    for row in appended_ok:
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
        f"Done. {len(appended_ok):,}/{len(appended_rows):,} new players added, "
        f"{len(updates) - len(failed_rows):,}/{len(updates):,} existing players refreshed, "
        f"{len(errors):,} fetch failure(s)."
    )
    safe_print(f"Sheet: https://docs.google.com/spreadsheets/d/{args.spreadsheet_id}")
    safe_print(f"CSV: {args.csv_output}")
    return errors


def main() -> int:
    args = parse_args()
    global SHEETS_MIN_REQUEST_INTERVAL
    SHEETS_MIN_REQUEST_INTERVAL = args.sheets_request_delay
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
    args.competition_parents_cache = args.competition_parents_cache.resolve()
    args.match_local_cache = args.match_local_cache.resolve()
    args.odds_competition_map = args.odds_competition_map.resolve()
    args.bet_csv_output = args.bet_csv_output.resolve()
    args.match_csv_output = args.match_csv_output.resolve()
    args.match_errors = args.match_errors.resolve()
    args.club_csv_output = args.club_csv_output.resolve()
    args.club_errors = args.club_errors.resolve()

    if args.only_individual_results:
        spreadsheet = open_spreadsheet(args.spreadsheet_id, args.credentials)
        sync_individual_results(spreadsheet, args)
        return 0

    if args.only_players_tab:
        spreadsheet = open_spreadsheet(args.spreadsheet_id, args.credentials)
        playerdata_ids = csv_id_values(args.csv_output, "Read final PlayerData IDs from CSV")
        sync_players_tab(spreadsheet, playerdata_ids)
        return 0

    if args.only_leagues_tab:
        spreadsheet = open_spreadsheet(args.spreadsheet_id, args.credentials)
        competition_ids = csv_id_values(
            args.competition_csv_output, "Read final CompetitionData IDs from CSV"
        )
        sync_leagues_tab(spreadsheet, competition_ids)
        return 0

    if args.only_matches_tab:
        spreadsheet = open_spreadsheet(args.spreadsheet_id, args.credentials)
        matchdata_ids = csv_id_values(args.match_csv_output, "Read final MatchData IDs from CSV")
        sync_matches_tab(spreadsheet, matchdata_ids)
        return 0

    if args.only_club_tab:
        spreadsheet = open_spreadsheet(args.spreadsheet_id, args.credentials)
        clubdata_ids = csv_id_values(args.club_csv_output, "Read final ClubData IDs from CSV")
        sync_club_tab(spreadsheet, clubdata_ids)
        return 0

    spreadsheet = open_spreadsheet(args.spreadsheet_id, args.credentials)

    phase_failures: list[str] = []

    def run_phase(name: str, fn: Callable[[], _T]) -> _T | None:
        """Run one independent sync phase. A failure here - even one that
        already exhausted sheet_call's own retries - is logged and this
        phase is skipped rather than crashing the whole script: one
        stubborn transient API failure in a single, often low-stakes step
        (e.g. a lookup tab mirror) used to take down every phase after it
        too, discarding a whole night's worth of otherwise-successful work."""
        try:
            return fn()
        except Exception as exc:
            safe_print(f"*** {name} FAILED this run, skipping it: {exc}")
            phase_failures.append(name)
            return None

    # Matches now syncs before PlayerData (it used to run after) so that,
    # unless --full-player-refresh is passed, sync_player_data can use this
    # run's own fresh results to refresh only players whose club actually
    # played recently, instead of refetching every player every night.
    match_errors: list[tuple[str, str, str]] = []
    all_matches: dict[str, dict[str, Any]] | None = None
    if not args.skip_matches:
        match_result = run_phase("MatchData sync", lambda: sync_matches(spreadsheet, args))
        if match_result is not None:
            match_errors, all_matches = match_result
        if not args.skip_matches_tab:
            run_phase(
                "Matches tab sync",
                lambda: sync_matches_tab(
                    spreadsheet,
                    csv_id_values(args.match_csv_output, "Read final MatchData IDs from CSV"),
                ),
            )
        snapshots: dict[str, dict[str, Any]] = {}
        if not args.skip_projection_snapshot:
            snapshot_result = run_phase(
                "Pre-match projection snapshot",
                lambda: snapshot_pre_match_projections(spreadsheet, args),
            )
            if snapshot_result is not None:
                snapshots = snapshot_result
        if not args.skip_bets:
            bet_errors = run_phase(
                "Bet sync",
                lambda: sync_bets(spreadsheet, args, all_matches, snapshots),
            )
            if bet_errors:
                safe_print(f"Bet sync had {len(bet_errors):,} odds error(s) - see log above.")

    errors: list[tuple[str, str]] = []
    if args.skip_players:
        safe_print("Skipping PlayerData sync.")
    else:
        players_to_refresh = (
            None
            if args.full_player_refresh
            else players_who_played_recently(all_matches, args.player_refresh_window_hours)
        )
        if players_to_refresh is None and not args.full_player_refresh:
            safe_print(
                "No fresh match data this run (matches skipped or none found) - "
                "refreshing every existing player instead of only recent players."
            )
        errors = run_phase(
            "PlayerData sync",
            lambda: sync_player_data(spreadsheet, args, players_to_refresh=players_to_refresh),
        ) or []
        if not args.skip_players_tab:
            run_phase(
                "Players tab sync",
                lambda: sync_players_tab(
                    spreadsheet, csv_id_values(args.csv_output, "Read final PlayerData IDs from CSV")
                ),
            )
    if errors:
        safe_print(f"Errors: {args.errors}")

    manager_errors: list[tuple[str, str]] = []
    if not args.skip_managers:
        manager_errors = run_phase("ManagerData sync", lambda: sync_managers(spreadsheet, args)) or []

    competition_errors: list[tuple[str, str]] = []
    if not args.skip_competitions:
        competition_errors = run_phase(
            "CompetitionData sync", lambda: sync_competitions(spreadsheet, args)
        ) or []
        if not args.skip_leagues_tab:
            run_phase(
                "Leagues tab sync",
                lambda: sync_leagues_tab(
                    spreadsheet,
                    csv_id_values(args.competition_csv_output, "Read final CompetitionData IDs from CSV"),
                ),
            )

    if not args.skip_individual_results:
        if all_matches is None:
            safe_print(
                "Individual Results: MatchData sync was skipped; copying from current sheet values."
            )
        run_phase(
            "Individual Results sync",
            lambda: sync_individual_results(spreadsheet, args, all_matches=all_matches),
        )

    club_errors: list[tuple[str, str]] = []
    if not args.skip_clubs:
        club_errors = run_phase("ClubData sync", lambda: sync_clubs(spreadsheet, args)) or []
        if not args.skip_club_tab:
            run_phase(
                "Club tab sync",
                lambda: sync_club_tab(
                    spreadsheet, csv_id_values(args.club_csv_output, "Read final ClubData IDs from CSV")
                ),
            )

    if phase_failures:
        safe_print(
            f"*** {len(phase_failures)} phase(s) failed this run and were skipped: "
            f"{', '.join(phase_failures)}. Everything else still completed and was saved."
        )

    return (
        0
        if not errors
        and not manager_errors
        and not competition_errors
        and not match_errors
        and not club_errors
        and not phase_failures
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
