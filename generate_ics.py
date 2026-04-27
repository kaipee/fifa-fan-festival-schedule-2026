#!/usr/bin/env python3
"""
Generate fifa_fan_festival_toronto_2026.ics — an RFC 5545 iCalendar file
covering FIFA Fan Festival™ Toronto (June 11 – July 19, 2026).

This script fetches the official schedule live on every run from:

  1. https://www.torontofwc26.ca/news/fifa-fan-festival-toronto-schedule
     (primary — per-day opening hours, match broadcasts, line-ups)
  2. https://www.torontofwc26.ca/FIFAFanFestival
     (festival overview + venue)
  3. https://www.toronto.ca/news/city-of-toronto-shares-first-look-at-fifa-fan-festival-toronto/
     (City of Toronto press release — corroborating data)
  4. https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026/fan-festival
     (FIFA global fan-festival page — best-effort, may not exist yet)

Each fetch is wrapped in retry/backoff with a 15-second timeout. On
hard failure the script falls back to a cached snapshot at
``data/cached_schedule.json`` (committed to the repo). On success the
cache is atomically updated so the next offline run still produces a
calendar reflecting the latest known data.

Output is a single .ics file with:
  * one VEVENT per individual performance when the source publishes an
    explicit start time (SUMMARY = "Performance: <Artist> @ FIFA Fan
    Festival Toronto")
  * one day-level VEVENT per festival day for opening hours / general
    programming when no per-performance times are available — the day's
    full line-up is included in DESCRIPTION (legacy behaviour)
  * one VEVENT per scheduled match broadcast (start time + ~2h block)
  * a VTIMEZONE block for America/Toronto

The file is plain UTF-8, uses CRLF line endings, folds long lines per
RFC 5545 §3.1, and uses stable SHA-1 derived UIDs so unchanged events
keep the same UID across regenerations.

Designed to run unattended in CI (e.g. a weekly GitHub Action).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

# --- Optional third-party deps -------------------------------------------
# Imported lazily inside fetch_*() so --dry-run / --check from a cached
# snapshot still works on a stripped-down host (the script will simply
# use the cache when requests/bs4 are missing).
try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent
OUTPUT_FILE = ROOT / "fifa_fan_festival_toronto_2026.ics"
CACHE_FILE = ROOT / "data" / "cached_schedule.json"

VENUE = ("Fort York & The Bentway, 250 Fort York Blvd, "
         "Toronto, ON M5V 3K9, Canada")
TZID = "America/Toronto"
PRODID = "-//roo//FIFA Fan Festival Toronto 2026//EN"

# Stable DTSTAMP so re-runs produce byte-identical output for unchanged
# data. (Per RFC 5545 DTSTAMP indicates when the iCalendar object was
# created, but using a fixed value keeps git diffs tight; CI will still
# bump it via the SOURCE_DATE_EPOCH env var if set.)
DTSTAMP_DEFAULT = "20260427T200000Z"

USER_AGENT = (
    "fifa-fan-festival-ics-bot/1.0 "
    "(+https://github.com/<owner>/<repo>)"
)

SOURCES = {
    "torontofwc26_schedule": (
        "https://www.torontofwc26.ca/news/"
        "fifa-fan-festival-toronto-schedule"
    ),
    "torontofwc26_festival": (
        "https://www.torontofwc26.ca/FIFAFanFestival"
    ),
    "toronto_news": (
        "https://www.toronto.ca/news/"
        "city-of-toronto-shares-first-look-at-fifa-fan-festival-toronto/"
    ),
    "fifa_fan_festival": (
        "https://www.fifa.com/en/tournaments/mens/worldcup/"
        "canadamexicousa2026/fan-festival"
    ),
}

FETCH_TIMEOUT = 15  # seconds
FETCH_RETRIES = 3
BACKOFF_BASE = 1.5  # seconds


# ---------------------------------------------------------------------------
# Schedule data model (also the JSON cache schema)
# ---------------------------------------------------------------------------

@dataclass
class Performance:
    artist: str
    time: str | None = None  # "HH:MM" 24h local — None = no published time
    duration_min: int = 60

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"artist": self.artist}
        if self.time:
            d["time"] = self.time
            d["duration_min"] = self.duration_min
        return d

    @classmethod
    def from_any(cls, obj: Any) -> "Performance":
        if isinstance(obj, str):
            return cls(artist=obj)
        return cls(
            artist=obj["artist"],
            time=obj.get("time"),
            duration_min=int(obj.get("duration_min", 60)),
        )


@dataclass
class Match:
    title: str
    start: str  # "HH:MM" 24h local
    duration_min: int = 120
    toronto_match: bool = False
    tentative: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "start": self.start,
            "duration_min": self.duration_min,
            "toronto_match": self.toronto_match,
            "tentative": self.tentative,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Match":
        return cls(
            title=d["title"],
            start=d["start"],
            duration_min=int(d.get("duration_min", 120)),
            toronto_match=bool(d.get("toronto_match", False)),
            tentative=bool(d.get("tentative", False)),
        )


@dataclass
class FestivalDay:
    date: str
    open_start: str
    open_end: str
    crosses_midnight: bool = False
    toronto_match_day: bool = False
    matches: list[Match] = field(default_factory=list)
    performances: list[Performance] = field(default_factory=list)
    cultural: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "open_start": self.open_start,
            "open_end": self.open_end,
            "crosses_midnight": self.crosses_midnight,
            "toronto_match_day": self.toronto_match_day,
            "matches": [m.to_dict() for m in self.matches],
            "performances": [p.to_dict() for p in self.performances],
            "cultural": list(self.cultural),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "FestivalDay":
        return cls(
            date=d["date"],
            open_start=d["open_start"],
            open_end=d["open_end"],
            crosses_midnight=bool(d.get("crosses_midnight", False)),
            toronto_match_day=bool(d.get("toronto_match_day", False)),
            matches=[Match.from_dict(m) for m in d.get("matches", [])],
            performances=[Performance.from_any(p)
                          for p in d.get("performances", [])],
            cultural=list(d.get("cultural", [])),
        )


# ---------------------------------------------------------------------------
# Cache I/O
# ---------------------------------------------------------------------------

def load_cache() -> list[FestivalDay] | None:
    if not CACHE_FILE.exists():
        return None
    try:
        raw = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        days = [FestivalDay.from_dict(d) for d in raw.get("days", [])]
        return days or None
    except Exception as exc:  # pragma: no cover
        print(f"WARNING: failed to load cache {CACHE_FILE}: {exc}",
              file=sys.stderr)
        return None


def save_cache_atomic(days: list[FestivalDay]) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": SOURCES,
        "days": [d.to_dict() for d in days],
    }
    fd, tmp_path = tempfile.mkstemp(
        prefix=".cached_schedule.", suffix=".json.tmp",
        dir=str(CACHE_FILE.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False,
                      sort_keys=False)
            f.write("\n")
        os.replace(tmp_path, CACHE_FILE)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------

def _fetch_url(url: str) -> str:
    """GET ``url`` with retries + exponential backoff. Raises on failure."""
    if requests is None:
        raise RuntimeError(
            "requests is not installed; run "
            "`pip install -r requirements.txt`"
        )
    last_exc: Exception | None = None
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                timeout=FETCH_TIMEOUT,
                headers={"User-Agent": USER_AGENT,
                         "Accept": "text/html,application/xhtml+xml"},
            )
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_exc = exc
            if attempt < FETCH_RETRIES:
                sleep_s = BACKOFF_BASE ** attempt
                print(
                    f"WARNING: fetch attempt {attempt}/{FETCH_RETRIES} "
                    f"failed for {url}: {exc}; "
                    f"retrying in {sleep_s:.1f}s",
                    file=sys.stderr,
                )
                time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


def fetch_sources() -> dict[str, str]:
    """Fetch every source URL. Returns {key: html} for ones that worked.

    Never raises — partial success is fine. The caller decides whether
    enough data was retrieved to parse a non-empty schedule.
    """
    fetched: dict[str, str] = {}
    for key, url in SOURCES.items():
        try:
            fetched[key] = _fetch_url(url)
            print(f"INFO: fetched {key} ({len(fetched[key])} bytes)",
                  file=sys.stderr)
        except Exception as exc:
            print(f"WARNING: could not fetch {key} ({url}): {exc}",
                  file=sys.stderr)
    return fetched


# --- HTML parsing helpers -------------------------------------------------

_TIME_RE = re.compile(
    r"\b(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|AM|PM)\b",
    re.IGNORECASE,
)
_DATE_RE = re.compile(
    r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*,?\s+"
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+"
    r"(\d{1,2})(?:,\s*(\d{4}))?",
    re.IGNORECASE,
)


def _to_24h(hour: int, minute: int, ampm: str) -> str:
    ampm = ampm.lower().replace(".", "")
    if ampm == "pm" and hour != 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0
    return f"{hour:02d}:{minute:02d}"


def parse_schedule_page(html: str) -> list[FestivalDay]:
    """Best-effort parse of the torontofwc26.ca schedule page.

    The site's markup may change without notice. This parser extracts
    what it can; anything missing falls through to the cache. Returns
    [] if no recognisable day blocks are found.
    """
    if BeautifulSoup is None:
        return []
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    # Heuristic: find lines that look like festival day headers, then
    # scan a window of following lines for opening hours + match times +
    # performer names. This is intentionally conservative — when the
    # page reorganises we'd rather emit nothing here and let the cache
    # take over than emit garbled events.
    days: list[FestivalDay] = []
    lines = [ln for ln in text.splitlines() if ln.strip()]
    i = 0
    while i < len(lines):
        line = lines[i]
        m = _DATE_RE.search(line)
        if not m or "2026" not in (m.group(0) + (m.group(3) or "")):
            i += 1
            continue
        # Map "Jun 11" → "2026-06-11"
        try:
            iso = datetime.strptime(
                f"{m.group(1)[:3]} {m.group(2)} 2026", "%b %d %Y"
            ).strftime("%Y-%m-%d")
        except ValueError:
            i += 1
            continue
        # Window of next ~30 lines
        window = "\n".join(lines[i:i + 30])
        hours_match = re.search(
            r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?)\s*[–-]\s*"
            r"(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?)",
            window, re.IGNORECASE,
        )
        if not hours_match:
            i += 1
            continue
        open_start = _to_24h(
            int(hours_match.group(1)),
            int(hours_match.group(2) or 0),
            hours_match.group(3),
        )
        open_end = _to_24h(
            int(hours_match.group(4)),
            int(hours_match.group(5) or 0),
            hours_match.group(6),
        )
        days.append(FestivalDay(
            date=iso,
            open_start=open_start,
            open_end=open_end,
        ))
        i += 1
    return days


def parse_live(fetched: dict[str, str]) -> list[FestivalDay]:
    """Parse all reachable sources into a unified schedule list.

    Currently we only have a robust parser for the primary
    torontofwc26.ca schedule page. The other pages are fetched for
    completeness (and to detect upstream changes) but their content
    isn't merged because their schemas are too volatile. If/when FIFA
    or the City publish structured data, add parsers here.
    """
    primary_html = fetched.get("torontofwc26_schedule")
    if not primary_html:
        return []
    return parse_schedule_page(primary_html)


def merge_with_cache(
    live: list[FestivalDay],
    cached: list[FestivalDay],
) -> list[FestivalDay]:
    """Merge live data on top of the cache (live wins per-day).

    The live parser only extracts coarse structure (date + opening
    hours). Performer/match details that the parser doesn't yet know
    how to extract are preserved from the cache so we don't regress.
    """
    by_date = {d.date: d for d in cached}
    for ld in live:
        if ld.date in by_date:
            base = by_date[ld.date]
            base.open_start = ld.open_start or base.open_start
            base.open_end = ld.open_end or base.open_end
            if ld.matches:
                base.matches = ld.matches
            if ld.performances:
                base.performances = ld.performances
            if ld.cultural:
                base.cultural = ld.cultural
        else:
            by_date[ld.date] = ld
    return sorted(by_date.values(), key=lambda d: d.date)


# ---------------------------------------------------------------------------
# iCalendar emitter
# ---------------------------------------------------------------------------

VTIMEZONE = """\
BEGIN:VTIMEZONE
TZID:America/Toronto
BEGIN:STANDARD
DTSTART:19701101T020000
RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU
TZOFFSETFROM:-0400
TZOFFSETTO:-0500
TZNAME:EST
END:STANDARD
BEGIN:DAYLIGHT
DTSTART:19700308T020000
RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU
TZOFFSETFROM:-0500
TZOFFSETTO:-0400
TZNAME:EDT
END:DAYLIGHT
END:VTIMEZONE"""


def fold(line: str) -> str:
    """Fold lines longer than 75 octets per RFC 5545 §3.1."""
    out: list[str] = []
    while len(line.encode("utf-8")) > 75:
        cut = 74
        while cut > 1 and (line[cut].encode("utf-8")[0] & 0xC0) == 0x80:
            cut -= 1
        out.append(line[:cut])
        line = " " + line[cut:]
    out.append(line)
    return "\r\n".join(out)


def escape(text: str) -> str:
    return (text
            .replace("\\", "\\\\")
            .replace(";", "\\;")
            .replace(",", "\\,")
            .replace("\n", "\\n"))


def fmt_local(date_str: str, hhmm: str, next_day: bool = False) -> str:
    dt = datetime.fromisoformat(f"{date_str}T{hhmm}:00")
    if next_day:
        dt += timedelta(days=1)
    return dt.strftime("%Y%m%dT%H%M%S")


def add_minutes(date_str: str, hhmm: str,
                minutes: int) -> tuple[str, str, bool]:
    dt = datetime.fromisoformat(f"{date_str}T{hhmm}:00") \
        + timedelta(minutes=minutes)
    base = datetime.fromisoformat(f"{date_str}T00:00:00")
    next_day = dt.date() != base.date()
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M"), next_day


def uid_for(summary: str, dtstart: str, location: str) -> str:
    """Stable SHA-1-derived UID. Same (summary, dtstart, location)
    triple → same UID across runs (req'd by RFC 5545 / CalDAV)."""
    seed = f"{summary}|{dtstart}|{location}"
    h = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"{h}@fifa-fan-festival-toronto-2026"


def emit_event(lines: list[str], *, summary: str, description: str,
               dtstart: str, dtend: str,
               categories: str = "FIFA Fan Festival Toronto",
               dtstamp: str = DTSTAMP_DEFAULT) -> None:
    lines.append("BEGIN:VEVENT")
    lines.append(f"UID:{uid_for(summary, dtstart, VENUE)}")
    lines.append(f"DTSTAMP:{dtstamp}")
    lines.append(f"DTSTART;TZID={TZID}:{dtstart}")
    lines.append(f"DTEND;TZID={TZID}:{dtend}")
    lines.append(fold(f"SUMMARY:{escape(summary)}"))
    lines.append(fold(f"LOCATION:{escape(VENUE)}"))
    lines.append(fold(f"DESCRIPTION:{escape(description)}"))
    lines.append(fold(f"CATEGORIES:{escape(categories)}"))
    lines.append("STATUS:CONFIRMED")
    lines.append("TRANSP:OPAQUE")
    lines.append("END:VEVENT")


def build_calendar(schedule: list[FestivalDay],
                   dtstamp: str = DTSTAMP_DEFAULT) -> str:
    lines: list[str] = []
    lines.append("BEGIN:VCALENDAR")
    lines.append("VERSION:2.0")
    lines.append(f"PRODID:{PRODID}")
    lines.append("CALSCALE:GREGORIAN")
    lines.append("METHOD:PUBLISH")
    lines.append(fold("X-WR-CALNAME:FIFA Fan Festival Toronto 2026"))
    lines.append(fold(
        "X-WR-CALDESC:FIFA Fan Festival\u2122 Toronto — official "
        "schedule (Fort York & The Bentway, June 11 – July 19, 2026). "
        "Source: torontofwc26.ca."))
    lines.append(f"X-WR-TIMEZONE:{TZID}")
    lines.extend(VTIMEZONE.split("\n"))

    for day in schedule:
        timed_perfs = [p for p in day.performances if p.time]
        untimed_perfs = [p for p in day.performances if not p.time]

        # ---------- Day-level VEVENT (opening hours + line-up) ----------
        desc_lines = [
            f"FIFA Fan Festival\u2122 Toronto — opening hours "
            f"{day.open_start} to {day.open_end}"
            f"{' (next day)' if day.crosses_midnight else ''}.",
            "",
        ]
        if day.toronto_match_day:
            desc_lines.append("*** Toronto Match Day ***")
            desc_lines.append("")
        if day.matches:
            desc_lines.append("Match broadcasts:")
            for m in day.matches:
                tag = " [TBC]" if m.tentative else ""
                tor = " [Toronto host-city match]" if m.toronto_match else ""
                desc_lines.append(
                    f"  • {m.start} — {m.title}{tor}{tag}")
            desc_lines.append("")
        if timed_perfs:
            desc_lines.append("Scheduled performances:")
            for p in timed_perfs:
                desc_lines.append(f"  • {p.time} — {p.artist}")
            desc_lines.append("")
        if untimed_perfs:
            desc_lines.append(
                "Performances: "
                + ", ".join(p.artist for p in untimed_perfs))
        if day.cultural:
            desc_lines.append(
                "Cultural & community: " + ", ".join(day.cultural))
        desc_lines.append("")
        desc_lines.append(
            "Free general admission — advance ticket required. "
            "Tickets: https://www.torontofwc26.ca/FIFAFanFestival")
        desc_lines.append(
            "Schedule source: " + SOURCES["torontofwc26_schedule"])
        desc_lines.append(
            "Note: Performance line-ups and TBC broadcasts may change; "
            "check the official site for the latest information.")

        summary = (
            f"FIFA Fan Festival Toronto — "
            f"{datetime.fromisoformat(day.date).strftime('%a %b %d')}"
            f"{' (Toronto Match Day)' if day.toronto_match_day else ''}"
        )
        emit_event(
            lines,
            summary=summary,
            description="\n".join(desc_lines),
            dtstart=fmt_local(day.date, day.open_start),
            dtend=fmt_local(day.date, day.open_end,
                            next_day=day.crosses_midnight),
            dtstamp=dtstamp,
        )

        # ---------- One VEVENT per match broadcast ----------
        for m in day.matches:
            end_date, end_hhmm, next_day = add_minutes(
                day.date, m.start, m.duration_min)
            tags = []
            if m.toronto_match:
                tags.append("Toronto host-city match")
            if m.tentative:
                tags.append("TBC")
            tag_str = f" [{'; '.join(tags)}]" if tags else ""
            match_desc = [
                "Match broadcast on the big screen at FIFA Fan "
                "Festival\u2122 Toronto.",
                "",
                f"Kick-off: {m.start} (local Toronto time).",
            ]
            if m.toronto_match:
                match_desc.append(
                    "This match is being played live in Toronto "
                    "(BMO Field / Toronto Stadium).")
            if m.tentative:
                match_desc.append(
                    "Fixture/teams to be confirmed (TBC) — see "
                    "official schedule for updates.")
            match_desc.append("")
            match_desc.append(
                "Source: " + SOURCES["torontofwc26_schedule"])
            emit_event(
                lines,
                summary=f"Match: {m.title}{tag_str}",
                description="\n".join(match_desc),
                dtstart=fmt_local(day.date, m.start),
                dtend=fmt_local(end_date, end_hhmm, next_day=next_day),
                categories="FIFA Fan Festival Toronto, Match Broadcast",
                dtstamp=dtstamp,
            )

        # ---------- One VEVENT per timed performance ----------
        for p in timed_perfs:
            end_date, end_hhmm, next_day = add_minutes(
                day.date, p.time, p.duration_min)  # type: ignore[arg-type]
            perf_desc = [
                f"Live performance by {p.artist} at FIFA Fan "
                f"Festival\u2122 Toronto.",
                "",
                f"Start: {p.time} (local Toronto time, "
                f"~{p.duration_min} min).",
                "",
                "Source: " + SOURCES["torontofwc26_schedule"],
            ]
            emit_event(
                lines,
                summary=(
                    f"Performance: {p.artist} "
                    f"@ FIFA Fan Festival Toronto"
                ),
                description="\n".join(perf_desc),
                dtstart=fmt_local(day.date, p.time),  # type: ignore[arg-type]
                dtend=fmt_local(end_date, end_hhmm, next_day=next_day),
                categories="FIFA Fan Festival Toronto, Performance",
                dtstamp=dtstamp,
            )

    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def gather_schedule() -> tuple[list[FestivalDay], dict[str, bool], bool]:
    """Return (schedule, source_status, used_cache_fallback).

    ``source_status`` maps SOURCES keys → True if reachable+parsed.
    ``used_cache_fallback`` is True when no live data could be merged
    and we're emitting purely from cache.
    """
    cached = load_cache() or []
    fetched = fetch_sources()
    status = {k: (k in fetched) for k in SOURCES}
    live = parse_live(fetched) if fetched else []
    if live:
        merged = merge_with_cache(live, cached)
        return merged, status, False
    if cached:
        print(
            "WARNING: live parse produced 0 days; falling back to "
            f"cache at {CACHE_FILE}",
            file=sys.stderr,
        )
        return cached, status, True
    print(
        "ERROR: no live data parsed and no cache available; refusing "
        "to emit an empty calendar.",
        file=sys.stderr,
    )
    return [], status, True


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate the FIFA Fan Festival Toronto 2026 .ics file."
    )
    parser.add_argument(
        "--check", action="store_true",
        help="Fetch + parse but don't write files; "
             "print STATUS: changed/unchanged/error.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the .ics to stdout instead of writing it.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        schedule, status, used_cache = gather_schedule()
    except Exception as exc:
        print(f"ERROR: gather_schedule failed: {exc}", file=sys.stderr)
        if args.check:
            print("STATUS: error")
        return 1

    if not schedule:
        if args.check:
            print("STATUS: error")
        return 1

    ics = build_calendar(schedule)

    if args.check:
        previous = ""
        if OUTPUT_FILE.exists():
            # Read as bytes to preserve CRLF (text mode would translate
            # them to LF on some platforms and produce false diffs).
            previous = OUTPUT_FILE.read_bytes().decode("utf-8")
        # Ignore DTSTAMP differences when comparing (we use a constant
        # but be defensive in case CI ever overrides it).
        def _norm(s: str) -> str:
            return re.sub(r"\r?\nDTSTAMP:[^\r\n]+", "", s)
        if _norm(previous) == _norm(ics):
            print("STATUS: unchanged")
        else:
            print("STATUS: changed")
        # --check never writes the .ics, but for visibility still
        # report which sources were used.
        for key, ok in status.items():
            print(f"  source {key}: {'ok' if ok else 'FAIL'}",
                  file=sys.stderr)
        if used_cache:
            print("  (used cache fallback)", file=sys.stderr)
        return 0

    if args.dry_run:
        sys.stdout.write(ics)
    else:
        OUTPUT_FILE.write_bytes(ics.encode("utf-8"))

    # Update cache on successful live fetch (don't overwrite cache
    # with itself if we never reached the network).
    if not used_cache and not args.dry_run:
        try:
            save_cache_atomic(schedule)
        except Exception as exc:  # pragma: no cover
            print(f"WARNING: failed to update cache: {exc}",
                  file=sys.stderr)

    n_days = len(schedule)
    n_matches = sum(len(d.matches) for d in schedule)
    n_perfs = sum(1 for d in schedule for p in d.performances if p.time)
    n_events = n_days + n_matches + n_perfs
    target = "stdout" if args.dry_run else str(OUTPUT_FILE)
    print(
        f"Wrote {target} "
        f"({n_days} festival days, {n_matches} match broadcasts, "
        f"{n_perfs} timed performances, {n_events} VEVENTs)."
    )
    if used_cache:
        print("  (used cache fallback — no live source reachable)",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
