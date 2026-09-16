#!/usr/bin/env python3
"""
Fetch Sky Bet match-winner (1X2) odds for AutoModel's tracked competitions
via The Odds API (the-odds-api.com), and compute implied win/draw/away
probabilities for comparison against AutoModel's own model projections.

Bet365 does not license its odds to third-party aggregators (confirmed by
direct API check - it's absent from every competition tried, not just
smaller ones), so Sky Bet is used instead - both are major, widely-used UK
bookmakers.

The Odds API bills per call as (markets requested) x (regions requested),
flat, regardless of how many fixtures are in the response - so this only
calls the endpoint once per competition that actually has an upcoming
(not-yet-started) AutoModel fixture, never blanket-querying every tracked
competition regardless of whether it has anything on.

Examples:
    python getOdds.py --api-key YOUR_KEY
    python getOdds.py --api-key YOUR_KEY --competition-map odds_competition_map.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import threading
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ODDS_BASE_URL = "https://api.the-odds-api.com/v4"
# Bet365 isn't available via this API for any competition (confirmed) - Sky
# Bet is the stand-in bookmaker, chosen as another major, widely-used UK book.
BOOKMAKER_KEY = "skybet"
RATE_LOCK = threading.Lock()
LAST_REQUEST = 0.0

HEADERS = [
    "Match ID", "Competition ID", "Home Team", "Away Team", "Commence UTC",
    "Home Odds", "Draw Odds", "Away Odds",
    "Implied Home %", "Implied Draw %", "Implied Away %",
    "Odds Retrieved UTC",
]


def args() -> argparse.Namespace:
    folder = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description="Fetch Sky Bet 1X2 odds for AutoModel's tracked competitions.")
    p.add_argument("--matches-input", type=Path, default=folder / "automodel_matches_local_cache.csv",
                   help="CSV of AutoModel matches to fetch odds for (default: automodel_matches_local_cache.csv)")
    p.add_argument("--competition-map", type=Path, default=folder / "odds_competition_map.csv",
                   help="CSV mapping AutoModel Competition ID -> The Odds API sport_key")
    p.add_argument("--output", type=Path, default=folder / "automodel_odds.csv")
    p.add_argument("--errors", type=Path, default=folder / "automodel_odds_errors.csv")
    p.add_argument("--api-key", default="", help="The Odds API key (required)")
    p.add_argument("--region", default="uk")
    p.add_argument("--request-delay", type=float, default=1.0)
    p.add_argument("--retries", type=int, default=4)
    p.add_argument("--upcoming-window-days", type=float, default=10,
                   help="Only fetch odds for competitions with a not-yet-started match in this many days")
    return p.parse_args()


def load_competition_map(path: Path) -> dict[str, str]:
    """Competition ID -> The Odds API sport_key, e.g. "47" -> "soccer_epl".
    A competition missing from this file is silently skipped when scanning
    for what to fetch - not every competition AutoModel tracks for match
    results has betting markets (development/regional/non-league
    competitions mostly don't), so an unmapped ID isn't an error."""
    if not path.exists():
        return {}
    mapping: dict[str, str] = {}
    with path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            cid = str(row.get("Competition ID", "")).strip()
            sport_key = str(row.get("Sport Key", "")).strip()
            if cid and sport_key:
                mapping[cid] = sport_key
    return mapping


def rate_limit(delay: float) -> None:
    global LAST_REQUEST
    with RATE_LOCK:
        wait = delay - (time.monotonic() - LAST_REQUEST)
        if wait > 0:
            time.sleep(wait)
        LAST_REQUEST = time.monotonic()


def fetch(url: str, retries: int, delay: float) -> Any:
    last: Exception | None = None
    for attempt in range(retries):
        rate_limit(delay)
        try:
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=45) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            last = exc
            if attempt + 1 < retries:
                time.sleep(2 ** attempt + random.random() / 2)
    raise RuntimeError(f"request failed after {retries} attempts: {last}")


def fetch_odds_for_sport(
    sport_key: str, api_key: str, *, region: str, retries: int, delay: float
) -> list[dict[str, Any]]:
    url = (
        f"{ODDS_BASE_URL}/sports/{sport_key}/odds/?"
        + urlencode({"apiKey": api_key, "regions": region, "markets": "h2h", "oddsFormat": "decimal"})
    )
    return fetch(url, retries, delay)


# FotMob (AutoModel's source) systematically uses short/common club names
# ("Tottenham", "Man City", "Nottm Forest") while The Odds API uses fuller
# official ones ("Tottenham Hotspur", "Manchester City", "Nottingham
# Forest") - confirmed by directly diffing real EPL fixtures from both
# sources, not a one-off naming quirk. Word-subset containment (below)
# handles most of these on its own (e.g. {"tottenham"} is already a subset
# of {"tottenham","hotspur"}); this table only needs the handful of cases
# where the short form is an actual abbreviation rather than a dropped
# suffix word.
COMMON_ABBREVIATIONS = {
    "man": "manchester",
    "nottm": "nottingham",
    "utd": "united",
    "wolves": "wolverhampton",
    "spurs": "tottenham",
}


def normalize_team_name(name: str) -> set[str]:
    """Loose team-name word-set for matching The Odds API's fixtures
    (identified by team name, not a shared ID) against AutoModel's own club
    names. Strips accents/punctuation, drops filler words ("FC"/"AFC"), and
    expands known abbreviations (see COMMON_ABBREVIATIONS) - returns a set
    of words rather than a single string so callers can test containment
    (one side dropping suffix words like "Hotspur"/"United"/"City") rather
    than requiring an exact match."""
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode()
    text = text.casefold()
    text = re.sub(r"[^\w\s]", " ", text)
    words = {COMMON_ABBREVIATIONS.get(w, w) for w in text.split() if w not in ("fc", "afc", "cf")}
    return words


def team_names_match(a: str, b: str) -> bool:
    """True if the two team names plausibly refer to the same club: either
    normalized word-set is equal, or the smaller is a subset of the
    larger (covers a source dropping suffix words like "Hotspur" while the
    other keeps them, e.g. "Tottenham" vs "Tottenham Hotspur")."""
    words_a, words_b = normalize_team_name(a), normalize_team_name(b)
    if not words_a or not words_b:
        return False
    return words_a <= words_b or words_b <= words_a


def implied_probabilities(home_odds: float, draw_odds: float, away_odds: float) -> tuple[float, float, float]:
    """Overround-normalized 1/odds. Raw 1/odds always sums to > 100% (the
    bookmaker's built-in margin) - scaling each down by that sum gives a
    fair comparison against AutoModel's own probabilities, which do sum to
    100%."""
    raw = [1 / o if o else 0.0 for o in (home_odds, draw_odds, away_odds)]
    total = sum(raw)
    if not total:
        return (0.0, 0.0, 0.0)
    return tuple(r / total for r in raw)


def upcoming_matches_by_competition(
    matches: dict[str, dict[str, Any]], competition_map: dict[str, str], window_days: float
) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """Group not-yet-started matches (same "Started" gate
    snapshot_pre_match_projections uses) within `window_days`, by Sport
    Key - so the caller only fetches odds for competitions that actually
    have something upcoming, keeping well under the API's free-tier quota
    instead of blanket-querying every tracked competition nightly."""
    cutoff = datetime.now(timezone.utc) + timedelta(days=window_days)
    by_sport: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for match_id, row in matches.items():
        if str(row.get("Started", "")) == "1" or str(row.get("Cancelled", "")) == "1":
            continue
        sport_key = competition_map.get(str(row.get("Competition ID", "")))
        if not sport_key:
            continue
        try:
            match_dt = datetime.fromisoformat(str(row.get("Match UTC", "")).replace("Z", "+00:00"))
        except ValueError:
            continue
        if match_dt > cutoff:
            continue
        by_sport.setdefault(sport_key, []).append((match_id, row))
    return by_sport


def match_odds_to_fixtures(
    odds_events: list[dict[str, Any]],
    upcoming: list[tuple[str, dict[str, Any]]],
    *,
    time_tolerance_minutes: int = 10,
) -> tuple[list[dict[str, Any]], list[tuple[str, str, str]]]:
    """Match The Odds API's events (team name + kickoff time) to
    AutoModel's own Match ID. Returns (rows ready to write, (Type, ID,
    Error) tuples for the error CSV) - an unmatched fixture is logged, not
    silently dropped, since name variants are expected and get fixed by
    extending normalize_team_name as real misses turn up."""
    matched: list[dict[str, Any]] = []
    errors: list[tuple[str, str, str]] = []

    for event in odds_events:
        try:
            event_dt = datetime.fromisoformat(str(event.get("commence_time", "")).replace("Z", "+00:00"))
        except ValueError:
            errors.append(("Odds", str(event.get("id", "")), "unparseable commence_time"))
            continue

        home_name = event.get("home_team", "")
        away_name = event.get("away_team", "")

        found: tuple[str, dict[str, Any]] | None = None
        for match_id, row in upcoming:
            try:
                match_dt = datetime.fromisoformat(str(row.get("Match UTC", "")).replace("Z", "+00:00"))
            except ValueError:
                continue
            if abs((match_dt - event_dt).total_seconds()) > time_tolerance_minutes * 60:
                continue
            if team_names_match(str(row.get("Home Club", "")), home_name) and team_names_match(
                str(row.get("Away Club", "")), away_name
            ):
                found = (match_id, row)
                break

        if not found:
            errors.append((
                "Odds", str(event.get("id", "")),
                f"no AutoModel fixture matched for {event.get('home_team')} v "
                f"{event.get('away_team')} at {event.get('commence_time')}",
            ))
            continue

        match_id, row = found
        bookmaker = next((b for b in event.get("bookmakers", []) if b.get("key") == BOOKMAKER_KEY), None)
        if not bookmaker or not bookmaker.get("markets"):
            continue  # Sky Bet not offering this specific match yet - not an error, just skip

        outcomes = {o["name"]: o["price"] for o in bookmaker["markets"][0].get("outcomes", [])}
        home_odds = outcomes.get(event.get("home_team"))
        away_odds = outcomes.get(event.get("away_team"))
        draw_odds = outcomes.get("Draw")
        if not (home_odds and draw_odds and away_odds):
            continue

        implied_h, implied_d, implied_a = implied_probabilities(home_odds, draw_odds, away_odds)
        matched.append({
            "Match ID": match_id,
            "Competition ID": str(row.get("Competition ID", "")),
            "Home Team": event.get("home_team", ""),
            "Away Team": event.get("away_team", ""),
            "Commence UTC": event.get("commence_time", ""),
            "Home Odds": home_odds,
            "Draw Odds": draw_odds,
            "Away Odds": away_odds,
            "Implied Home %": round(implied_h * 100, 2),
            "Implied Draw %": round(implied_d * 100, 2),
            "Implied Away %": round(implied_a * 100, 2),
            "Odds Retrieved UTC": datetime.now(timezone.utc).isoformat(),
        })

    return matched, errors


def collect_odds(
    matches: dict[str, dict[str, Any]],
    competition_map: dict[str, str],
    api_key: str,
    *,
    region: str = "uk",
    window_days: float = 10,
    retries: int = 4,
    request_delay: float = 1.0,
) -> tuple[dict[str, dict[str, Any]], list[tuple[str, str, str]]]:
    """Fetch Sky Bet odds for every tracked competition with an upcoming
    fixture, matched to AutoModel Match IDs. Returns (Match ID -> row,
    errors) - the shape syncPlayersToSheet.py's sync_bets expects."""
    grouped = upcoming_matches_by_competition(matches, competition_map, window_days)
    all_rows: dict[str, dict[str, Any]] = {}
    all_errors: list[tuple[str, str, str]] = []

    for sport_key, upcoming in grouped.items():
        try:
            events = fetch_odds_for_sport(sport_key, api_key, region=region, retries=retries, delay=request_delay)
        except Exception as exc:
            all_errors.append(("Sport", sport_key, str(exc)))
            continue
        if isinstance(events, dict):  # API error payload, e.g. bad key/quota exceeded
            all_errors.append(("Sport", sport_key, str(events)))
            continue
        rows, errors = match_odds_to_fixtures(events, upcoming)
        for row in rows:
            all_rows[row["Match ID"]] = row
        all_errors.extend(errors)

    return all_rows, all_errors


def write_errors(path: Path, errors: list[tuple[str, str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Type", "ID", "Error", "Retrieved UTC"])
        now = datetime.now(timezone.utc).isoformat()
        for kind, item, error in errors:
            w.writerow([kind, item, error, now])


def load_matches_csv(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8-sig", newline="") as f:
        return {
            str(row.get("Match ID", "")): row
            for row in csv.DictReader(f)
            if row.get("Match ID")
        }


def main() -> int:
    a = args()
    if not a.api_key:
        print("Error: --api-key is required")
        return 1

    matches = load_matches_csv(a.matches_input)
    if not matches:
        print(f"Error: no matches found in {a.matches_input}")
        return 1
    competition_map = load_competition_map(a.competition_map)
    if not competition_map:
        print(f"Error: no competition mappings found in {a.competition_map}")
        return 1

    rows, errors = collect_odds(
        matches, competition_map, a.api_key,
        region=a.region, window_days=a.upcoming_window_days,
        retries=a.retries, request_delay=a.request_delay,
    )

    a.output.parent.mkdir(parents=True, exist_ok=True)
    temp = a.output.with_suffix(a.output.suffix + ".tmp")
    with temp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows.values())
    temp.replace(a.output)
    write_errors(a.errors, errors)

    print(f"Done: {len(rows)} match(es) with Sky Bet odds, {len(errors)} unmatched/error(s).")
    print(f"Saved: {a.output}")
    print(f"Errors: {len(errors)} ({a.errors})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
