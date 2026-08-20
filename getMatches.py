#!/usr/bin/env python3
"""
Export past and future AutoModel matches for every club in club_ids.txt.

Examples:
    python getMatches.py
    python getMatches.py --mode fixtures
    python getMatches.py --from-date 2025-07-01 --to-date 2027-06-30

Default ``full`` mode enriches completed matches with match facts, team stats,
line-ups and events. ``fixtures`` mode uses one request per club and is much
faster. Matches shared by two input clubs are written only once.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

TEAM_URL = "https://www.fotmob.com/api/data/teams"
MATCH_URL = "https://www.fotmob.com/api/data/matchDetails"
LOCK = threading.Lock()
RATE_LOCK = threading.Lock()
LAST_REQUEST = 0.0

STAT_FIELDS = [
    ("Possession", "BallPossesion"),
    ("xG", "expected_goals"),
    ("Non-Penalty xG", "expected_goals_non_penalty"),
    ("xGOT", "expected_goals_on_target"),
    ("Shots", "total_shots"),
    ("Shots On Target", "ShotsOnTarget"),
    ("Shots Off Target", "ShotsOffTarget"),
    ("Blocked Shots", "blocked_shots"),
    ("Big Chances", "big_chance"),
    ("Big Chances Missed", "big_chance_missed_title"),
    ("Touches In Opposition Box", "touches_opp_box"),
    ("Passes", "passes"),
    ("Accurate Passes", "accurate_passes"),
    ("Corners", "corners"),
    ("Offsides", "Offsides"),
    ("Fouls", "fouls"),
    ("Yellow Cards", "yellow_cards"),
    ("Red Cards", "red_cards"),
    ("Tackles", "won_tackles"),
    ("Interceptions", "interceptions"),
    ("Clearances", "clearances"),
    ("Saves", "saves"),
]

HEADERS = [
    "Match ID", "Match UTC", "Status", "Status Reason", "Started", "Finished",
    "Cancelled", "Awarded", "Competition ID", "Competition", "Parent Competition ID",
    "Round", "Stage", "Country Code", "Season", "Gender", "Home Club ID",
    "Home Club", "Away Club ID", "Away Club", "Home Score", "Away Score",
    "Score", "Result", "Winner Club ID", "Page URL", "FotMob URL",
    "Coverage Level", "Detailed Data",
] + [
    f"{side} {label}" for label, _ in STAT_FIELDS for side in ("Home", "Away")
] + [
    "Home Formation", "Away Formation", "Home Team Rating", "Away Team Rating",
    "Home Starter IDs", "Home Starters", "Away Starter IDs", "Away Starters",
    "Home Substitute IDs", "Home Substitutes", "Away Substitute IDs", "Away Substitutes",
    "Goal Count", "Goals", "Card Count", "Cards", "Substitution Count", "Substitutions",
    "Shot Count", "Home Shot Count", "Away Shot Count", "Referee", "Referee Country",
    "Stadium", "Stadium City", "Attendance", "Player Of The Match ID",
    "Player Of The Match", "Player Of The Match Club ID", "Player Of The Match Rating",
    "Temperature C", "Weather", "Wind Speed", "Humidity", "Highlights URL",
    "Retrieved UTC",
]


def args() -> argparse.Namespace:
    folder = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description="Export deduplicated AutoModel matches by club.")
    p.add_argument("input", nargs="?", type=Path, default=folder / "club_ids.txt")
    p.add_argument("--output", type=Path, default=folder / "automodel_matches.csv")
    p.add_argument("--errors", type=Path, default=folder / "automodel_match_errors.csv")
    p.add_argument("--mode", choices=("full", "fixtures"), default="full")
    p.add_argument("--from-date", default="", help="Inclusive YYYY-MM-DD filter")
    p.add_argument("--to-date", default="", help="Inclusive YYYY-MM-DD filter")
    p.add_argument("--all-seasons", action="store_true",
                   help="Don't clip fixtures to each club's current season")
    p.add_argument("--club-workers", type=int, default=10)
    p.add_argument("--detail-workers", type=int, default=12)
    p.add_argument("--request-delay", type=float, default=0.06)
    p.add_argument("--retries", type=int, default=4)
    p.add_argument("--overwrite", action="store_true",
                   help="Ignore reusable detailed rows in the existing output")
    return p.parse_args()


def load_ids(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input not found: {path}")
    with path.open(encoding="utf-8-sig", newline="") as f:
        values: Iterable[str] = (
            (cell for row in csv.reader(f) for cell in row)
            if path.suffix.lower() == ".csv"
            else (x for line in f for x in re.split(r"[,;\\s]+", line.strip()))
        )
        return list(dict.fromkeys(x.strip() for x in values if x.strip().isdigit()))


def rate_limit(delay: float) -> None:
    global LAST_REQUEST
    with RATE_LOCK:
        wait = delay - (time.monotonic() - LAST_REQUEST)
        if wait > 0:
            time.sleep(wait)
        LAST_REQUEST = time.monotonic()


def fetch(url: str, retries: int, delay: float) -> dict[str, Any]:
    last: Exception | None = None
    for attempt in range(retries):
        rate_limit(delay)
        try:
            req = Request(url, headers={
                "Accept": "application/json", "Referer": "https://www.fotmob.com/",
                "User-Agent": "Mozilla/5.0 (compatible; AutoModelMatchCSVExporter/1.0)",
            })
            with urlopen(req, timeout=45) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            last = exc
            if attempt + 1 < retries:
                time.sleep(2 ** attempt + random.random() / 2)
    raise RuntimeError(f"request failed after {retries} attempts: {last}")


def fixture_row(item: dict[str, Any]) -> dict[str, Any]:
    status = item.get("status") or {}
    home, away = item.get("home") or {}, item.get("away") or {}
    tournament = item.get("tournament") or {}
    finished, cancelled = bool(status.get("finished")), bool(status.get("cancelled"))
    hs, aas = home.get("score", ""), away.get("score", "")
    winner = ""
    result = ""
    if finished and not cancelled and isinstance(hs, (int, float)) and isinstance(aas, (int, float)):
        result = "Home Win" if hs > aas else "Away Win" if aas > hs else "Draw"
        winner = home.get("id", "") if hs > aas else away.get("id", "") if aas > hs else ""
    page = item.get("pageUrl") or ""
    return {
        "Match ID": item.get("id", ""), "Match UTC": status.get("utcTime", ""),
        "Status": "Cancelled" if cancelled else "Finished" if finished else "Live" if status.get("started") else "Scheduled",
        "Status Reason": (status.get("reason") or {}).get("long", ""),
        "Started": int(bool(status.get("started"))), "Finished": int(finished),
        "Cancelled": int(cancelled), "Awarded": int(bool(status.get("awarded"))),
        "Competition ID": tournament.get("leagueId", ""), "Competition": tournament.get("name", ""),
        "Round": tournament.get("round", ""), "Stage": tournament.get("stage", ""),
        "Home Club ID": home.get("id", ""), "Home Club": home.get("name", ""),
        "Away Club ID": away.get("id", ""), "Away Club": away.get("name", ""),
        "Home Score": hs, "Away Score": aas, "Score": status.get("scoreStr", ""),
        "Result": result, "Winner Club ID": winner, "Page URL": page,
        "FotMob URL": f"https://www.fotmob.com{page}" if page else "",
        "Detailed Data": 0,
    }


def season_bounds(season_label: str) -> tuple[str, str] | None:
    """Date range (inclusive, YYYY-MM-DD) covering a club's current season."""
    text = str(season_label or "")
    european = re.fullmatch(r"(\d{4})/(\d{4})", text)
    if european:
        start_year, end_year = european.groups()
        return f"{start_year}-07-01", f"{end_year}-06-30"
    if re.fullmatch(r"\d{4}", text):
        return f"{text}-01-01", f"{text}-12-31"
    return None


def nested(data: Any, *keys: str, default: Any = "") -> Any:
    for key in keys:
        if not isinstance(data, dict):
            return default
        data = data.get(key)
    return default if data is None else data


def info_value(facts: dict[str, Any], label: str) -> Any:
    value = (facts.get("infoBox") or {}).get(label, "")
    if isinstance(value, dict):
        return value.get("value") or value.get("name") or value.get("text") or ""
    return value


def stat_pairs(detail: dict[str, Any]) -> dict[str, list[Any]]:
    result: dict[str, list[Any]] = {}
    groups = nested(detail, "content", "stats", "Periods", "All", "stats", default=[]) or []
    for group in groups:
        for stat in group.get("stats") or []:
            key, values = stat.get("key"), stat.get("stats")
            if key and isinstance(values, list) and len(values) >= 2 and key not in result:
                result[key] = values
    return result


def people(team: dict[str, Any], group: str) -> tuple[str, str]:
    entries = team.get(group) or []
    return (
        "|".join(str(x.get("id")) for x in entries if x.get("id")),
        "|".join(str(x.get("name") or "") for x in entries if x.get("name")),
    )


def event_text(event: dict[str, Any]) -> str:
    minute = str(event.get("time", ""))
    if event.get("overloadTime"):
        minute += f"+{event['overloadTime']}"
    player = nested(event, "player", "name") or event.get("nameStr") or event.get("playerName") or ""
    return f"{minute}' {event.get('type', '')} {player}".strip()


def enrich(base: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    row = dict(base)
    general, header = detail.get("general") or {}, detail.get("header") or {}
    content = detail.get("content") or {}
    facts, lineup = content.get("matchFacts") or {}, content.get("lineup") or {}
    weather, shotmap = content.get("weather") or {}, content.get("shotmap") or {}
    row.update({
        "Competition ID": general.get("leagueId", row.get("Competition ID", "")),
        "Competition": general.get("leagueName", row.get("Competition", "")),
        "Parent Competition ID": general.get("parentLeagueId", ""),
        "Round": general.get("matchRound", row.get("Round", "")),
        "Country Code": general.get("countryCode", ""), "Gender": general.get("gender", ""),
        "Coverage Level": general.get("coverageLevel", ""), "Detailed Data": 1,
    })
    stats = stat_pairs(detail)
    for label, key in STAT_FIELDS:
        values = stats.get(key, ["", ""])
        row[f"Home {label}"], row[f"Away {label}"] = values[0], values[1]
    home, away = lineup.get("homeTeam") or {}, lineup.get("awayTeam") or {}
    row.update({
        "Home Formation": home.get("formation", ""), "Away Formation": away.get("formation", ""),
        "Home Team Rating": home.get("rating", ""), "Away Team Rating": away.get("rating", ""),
    })
    for prefix, team in (("Home", home), ("Away", away)):
        ids, names = people(team, "starters")
        row[f"{prefix} Starter IDs"], row[f"{prefix} Starters"] = ids, names
        ids, names = people(team, "subs")
        row[f"{prefix} Substitute IDs"], row[f"{prefix} Substitutes"] = ids, names
    events = (facts.get("events") or {}).get("events") or []
    goals = [x for x in events if str(x.get("type", "")).casefold() in ("goal", "own goal", "penalty")]
    cards = [x for x in events if "card" in str(x.get("type", "")).casefold()]
    subs = [x for x in events if "sub" in str(x.get("type", "")).casefold()]
    shots = shotmap.get("shots") or []
    home_id, away_id = str(row.get("Home Club ID", "")), str(row.get("Away Club ID", ""))
    potm = facts.get("playerOfTheMatch") or {}
    referee, stadium = info_value(facts, "Referee"), info_value(facts, "Stadium")
    row.update({
        "Goal Count": len(goals), "Goals": " | ".join(map(event_text, goals)),
        "Card Count": len(cards), "Cards": " | ".join(map(event_text, cards)),
        "Substitution Count": len(subs), "Substitutions": " | ".join(map(event_text, subs)),
        "Shot Count": len(shots),
        "Home Shot Count": sum(str(x.get("teamId", "")) == home_id for x in shots),
        "Away Shot Count": sum(str(x.get("teamId", "")) == away_id for x in shots),
        "Referee": referee, "Stadium": stadium, "Attendance": info_value(facts, "Attendance"),
        "Player Of The Match ID": potm.get("id", ""),
        "Player Of The Match": nested(potm, "name", "fullName") or potm.get("name", ""),
        "Player Of The Match Club ID": potm.get("teamId", ""),
        "Player Of The Match Rating": nested(potm, "rating", "num"),
        "Temperature C": weather.get("temperature", ""), "Weather": weather.get("description", ""),
        "Wind Speed": weather.get("windSpeed", ""), "Humidity": weather.get("relativeHumidity", ""),
        "Highlights URL": nested(facts, "highlights", "url"),
    })
    return row


def reusable_rows(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    with path.open(encoding="utf-8-sig", newline="") as f:
        return {
            str(row.get("Match ID", "")): row for row in csv.DictReader(f)
            if row.get("Match ID") and str(row.get("Detailed Data", "")) == "1"
        }


def load_all_rows(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    with path.open(encoding="utf-8-sig", newline="") as f:
        return {
            str(row.get("Match ID", "")): row for row in csv.DictReader(f)
            if row.get("Match ID")
        }


def write_errors(path: Path, errors: list[tuple[str, str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Type", "ID", "Error", "Retrieved UTC"])
        now = datetime.now(timezone.utc).isoformat()
        for kind, item, error in errors:
            w.writerow([kind, item, error, now])


def collect_matches(
    club_ids: list[str],
    *,
    mode: str,
    from_date: str,
    to_date: str,
    all_seasons: bool,
    club_workers: int,
    detail_workers: int,
    request_delay: float,
    retries: int,
    cached: dict[str, dict[str, Any]],
    cached_all: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], list[tuple[str, str, str]]]:
    """Fetch and merge fixtures for every club, enrich completed matches,
    and fill in round info for everything else.

    `cached` (Match ID -> row) lets already fully-enriched (finished,
    "Detailed Data"=1) matches skip a re-fetch - their result can't change.
    `cached_all` (every previously-seen Match ID -> row) lets not-yet-final
    matches reuse a previously-fetched round number instead of re-fetching
    just for that. Pass {} for both on a from-scratch run.
    """
    errors: list[tuple[str, str, str]] = []
    matches: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max(1, club_workers)) as pool:
        jobs = {pool.submit(fetch, f"{TEAM_URL}?{urlencode({'id': club_id})}",
                            retries, request_delay): club_id for club_id in club_ids}
        for future in as_completed(jobs):
            club_id = jobs[future]
            try:
                data = future.result()
                fixtures = nested(data, "fixtures", "allFixtures", "fixtures", default=[]) or []
                bounds = None if all_seasons else season_bounds(nested(data, "details", "latestSeason"))
                for item in fixtures:
                    row = fixture_row(item)
                    match_id, date = str(row["Match ID"]), str(row["Match UTC"])[:10]
                    if not match_id:
                        continue
                    if bounds and not (bounds[0] <= date <= bounds[1]):
                        continue
                    if from_date and date < from_date:
                        continue
                    if to_date and date > to_date:
                        continue
                    matches[match_id] = row
            except Exception as exc:
                errors.append(("Club", club_id, str(exc)))

    if mode == "full":
        targets = [m for m in matches.values() if m["Finished"] and not m["Cancelled"]]
        print(f"Enriching {len(targets)} completed matches; {len(cached)} reusable rows found...")
        with ThreadPoolExecutor(max_workers=max(1, detail_workers)) as pool:
            jobs = {}
            for row in targets:
                key = str(row["Match ID"])
                if key in cached:
                    # cached[key] was read back from the sheet via
                    # get_all_values(), which returns everything as a
                    # string - including Match ID, which needs to stay a
                    # number or every MATCH/INDEX/XLOOKUP formula keyed off
                    # it (e.g. the Matches tab's lookup formulas) breaks the
                    # instant this cached row gets rewritten.
                    reused = dict(cached[key])
                    reused["Match ID"] = int(key)
                    matches[key] = reused
                else:
                    url = f"{MATCH_URL}?{urlencode({'matchId': key})}"
                    jobs[pool.submit(fetch, url, retries, request_delay)] = (key, row)
            for future in as_completed(jobs):
                key, row = jobs[future]
                try:
                    matches[key] = enrich(row, future.result())
                except Exception as exc:
                    errors.append(("Match", key, str(exc)))

        # Round numbers aren't in the lightweight fixtures feed, only in
        # matchDetails - so scheduled/live matches (skipped above) never get
        # one from enrich(). Fetch just that for anything still missing it,
        # without marking it "Detailed Data" (its result still needs a real
        # fetch once it's actually played).
        round_targets = [
            row for row in matches.values()
            if not row.get("Round") and not row.get("Cancelled")
        ]
        reused_rounds = 0
        with ThreadPoolExecutor(max_workers=max(1, detail_workers)) as pool:
            jobs = {}
            for row in round_targets:
                key = str(row["Match ID"])
                cached_row = cached_all.get(key)
                if cached_row and cached_row.get("Round"):
                    row["Round"] = cached_row["Round"]
                    row["Parent Competition ID"] = row.get("Parent Competition ID") or cached_row.get("Parent Competition ID", "")
                    row["Country Code"] = row.get("Country Code") or cached_row.get("Country Code", "")
                    row["Gender"] = row.get("Gender") or cached_row.get("Gender", "")
                    row["Coverage Level"] = row.get("Coverage Level") or cached_row.get("Coverage Level", "")
                    reused_rounds += 1
                    continue
                url = f"{MATCH_URL}?{urlencode({'matchId': key})}"
                jobs[pool.submit(fetch, url, retries, request_delay)] = row
            if round_targets:
                print(f"Filling round info for {len(round_targets)} other fixtures "
                      f"({reused_rounds} reused, {len(jobs)} to fetch)...")
            for future in as_completed(jobs):
                row = jobs[future]
                try:
                    general = future.result().get("general") or {}
                    row["Round"] = general.get("matchRound", "")
                    row["Parent Competition ID"] = row.get("Parent Competition ID") or general.get("parentLeagueId", "")
                    row["Country Code"] = row.get("Country Code") or general.get("countryCode", "")
                    row["Gender"] = row.get("Gender") or general.get("gender", "")
                    row["Coverage Level"] = row.get("Coverage Level") or general.get("coverageLevel", "")
                except Exception as exc:
                    errors.append(("Match", str(row["Match ID"]), str(exc)))

    now = datetime.now(timezone.utc).isoformat()
    for row in matches.values():
        row["Retrieved UTC"] = now
    return matches, errors


def main() -> int:
    a = args()
    try:
        ids = load_ids(a.input)
        if not ids:
            raise ValueError(f"No numeric club IDs found in {a.input}")
        for value in (a.from_date, a.to_date):
            if value:
                datetime.strptime(value, "%Y-%m-%d")
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    cached = {} if a.overwrite else reusable_rows(a.output)
    cached_all = {} if a.overwrite else load_all_rows(a.output)
    print(f"Fetching fixtures for {len(ids)} clubs...")
    matches, errors = collect_matches(
        ids,
        mode=a.mode,
        from_date=a.from_date,
        to_date=a.to_date,
        all_seasons=a.all_seasons,
        club_workers=a.club_workers,
        detail_workers=a.detail_workers,
        request_delay=a.request_delay,
        retries=a.retries,
        cached=cached,
        cached_all=cached_all,
    )

    rows = list(matches.values())
    rows.sort(key=lambda x: (str(x.get("Match UTC", "")), int(x.get("Match ID") or 0)))
    a.output.parent.mkdir(parents=True, exist_ok=True)
    temp = a.output.with_suffix(a.output.suffix + ".tmp")
    with temp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    temp.replace(a.output)
    write_errors(a.errors, errors)
    finished = sum(bool(x.get("Finished")) for x in rows)
    future = sum(not bool(x.get("Started")) and not bool(x.get("Cancelled")) for x in rows)
    print(f"Done: {len(rows)} unique matches ({finished} finished, {future} future).")
    print(f"Saved: {a.output}")
    print(f"Errors: {len(errors)} ({a.errors})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
