#!/usr/bin/env python3
"""
Generate fifa_fan_festival_toronto_2026.ics — an RFC 5545 iCalendar file
covering FIFA Fan Festival™ Toronto (June 11 – July 19, 2026).

Source of schedule (retrieved 2026-04-27):
  https://www.torontofwc26.ca/news/fifa-fan-festival-toronto-schedule
  https://www.torontofwc26.ca/FIFAFanFestival
  https://www.toronto.ca/news/city-of-toronto-shares-first-look-at-fifa-fan-festival-toronto/

Venue:
  Fort York National Historic Site & The Bentway
  250 Fort York Blvd, Toronto, ON M5V 3K9

Output is a single .ics file with:
  * one VEVENT per festival day (opening hours, with full programming
    in DESCRIPTION)
  * one VEVENT per scheduled match broadcast (start time + ~2h block)
  * a VTIMEZONE block for America/Toronto (EDT, UTC-4 throughout the
    festival)

The file is plain ASCII, uses CRLF line endings, and folds long lines
per RFC 5545 §3.1.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_FILE = Path(__file__).parent / "fifa_fan_festival_toronto_2026.ics"

VENUE = ("Fort York & The Bentway, 250 Fort York Blvd, "
         "Toronto, ON M5V 3K9, Canada")
TZID = "America/Toronto"
PRODID = "-//roo//FIFA Fan Festival Toronto 2026//EN"

# Stable DTSTAMP so re-runs produce byte-identical output.
DTSTAMP = "20260427T200000Z"

# ---------------------------------------------------------------------------
# Schedule data — transcribed verbatim from torontofwc26.ca on 2026-04-27.
# ---------------------------------------------------------------------------

@dataclass
class Match:
    title: str           # e.g. "Mexico vs South Africa"
    start: str           # "HH:MM" 24h local Toronto time
    duration_min: int = 120
    toronto_match: bool = False  # match physically played in Toronto
    tentative: bool = False      # broadcast still TBC

@dataclass
class FestivalDay:
    date: str            # "YYYY-MM-DD"
    open_start: str      # "HH:MM"
    open_end: str        # "HH:MM" (may be next-day, see crosses_midnight)
    crosses_midnight: bool = False
    toronto_match_day: bool = False
    matches: list[Match] = field(default_factory=list)
    performances: list[str] = field(default_factory=list)
    cultural: list[str] = field(default_factory=list)


# All times are local Toronto (America/Toronto, EDT = UTC-4 in June/July).
SCHEDULE: list[FestivalDay] = [
    FestivalDay("2026-06-11", "12:30", "19:30",
        matches=[Match("Mexico vs South Africa", "15:00")],
        performances=[
            "AHI", "Skratch Bastid", "Walk off the Earth",
            "Kardinal Offishall Presents: Soundclash Society",
        ],
        cultural=[
            "Ballet Folklórico Puro México",
            "Mariachi Band Vientos del Norte",
        ]),
    FestivalDay("2026-06-12", "12:30", "23:30", toronto_match_day=True,
        matches=[
            Match("Canada vs Bosnia & Herzegovina", "15:00",
                  toronto_match=True),
            Match("USA vs Paraguay", "21:00"),
        ],
        performances=[
            "Big Wreck", "Choir! Choir! Choir!",
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-06-13", "13:00", "23:30",
        matches=[
            Match("Qatar vs Switzerland", "15:00"),
            Match("Brazil vs Morocco", "18:00"),
            Match("Haiti vs Scotland", "21:00"),
        ],
        performances=[
            "Anna Sofia", "The Strumbellas",
            "Kardinal Offishall Presents: Soundclash Society",
        ],
        cultural=["Springcreek Dancers"]),
    FestivalDay("2026-06-14", "13:30", "21:30",
        matches=[
            Match("Netherlands vs Japan", "16:00"),
            Match("Côte d'Ivoire vs Ecuador", "19:00"),
        ],
        performances=[
            "Murda Beatz",
            "Kardinal Offishall Presents: Soundclash Society",
        ],
        cultural=["Nagata Shachu"]),
    FestivalDay("2026-06-17", "14:30", "22:30", toronto_match_day=True,
        matches=[
            Match("England vs Croatia", "16:00", toronto_match=True),
            Match("Ghana vs Panama", "19:00"),
        ],
        performances=[
            "Kardinal Offishall Presents: Soundclash Society",
        ],
        cultural=["Black Stars Collective"]),
    FestivalDay("2026-06-18", "13:30", "23:30",
        matches=[
            Match("Switzerland vs Bosnia & Herzegovina", "15:00"),
            Match("Canada vs Qatar", "18:00"),
            Match("Mexico vs Korea", "21:00"),
        ],
        performances=[
            "Dwayne Gretzky", "The Brokes",
            "Kardinal Offishall Presents: Soundclash Society",
        ],
        cultural=["HanBeat Nanta"]),
    FestivalDay("2026-06-19", "13:30", "23:30",
        matches=[
            Match("USA vs Australia", "15:00"),
            Match("Scotland vs Morocco", "18:00"),
            Match("Brazil vs Haiti", "21:00"),
        ],
        performances=[
            "Dwayne Gretzky",
            "Kardinal Offishall Presents: Soundclash Society",
        ],
        cultural=["SuperDogs"]),
    FestivalDay("2026-06-20", "13:00", "22:30", toronto_match_day=True,
        matches=[
            Match("Germany vs Côte d'Ivoire", "16:00", toronto_match=True),
            Match("Ecuador vs Curaçao", "20:00"),
        ],
        performances=[
            "k-os", "Skratch Bastid",
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-06-21", "10:30", "21:00",
        matches=[
            Match("Spain vs Saudi Arabia", "12:00"),
            Match("Belgium vs IR Iran", "15:00"),
            Match("Uruguay vs Cape Verde", "18:00"),
        ],
        performances=[
            "Allied Nations", "Classic Roots", "Nimkii and the Niniis",
            "Kardinal Offishall Presents: Soundclash Society",
        ],
        cultural=["The Sky Dancers"]),
    FestivalDay("2026-06-23", "11:30", "22:30", toronto_match_day=True,
        matches=[
            Match("Portugal vs Uzbekistan", "13:00", toronto_match=True),
            Match("England vs Ghana", "16:00"),
            Match("Panama vs Croatia", "19:00"),
        ],
        performances=[
            "Shawn Desman",
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-06-24", "13:30", "23:30",
        matches=[
            Match("Switzerland vs Canada", "15:00"),
            Match("To be confirmed", "18:00", tentative=True),
            Match("To be confirmed", "21:00", tentative=True),
        ],
        performances=[
            "Snotty Nose Rez Kids",
            "Kardinal Offishall Presents: Soundclash Society",
        ],
        cultural=["SHOUT! the band"]),
    FestivalDay("2026-06-26", "13:30", "22:30", toronto_match_day=True,
        matches=[
            Match("To be confirmed (Toronto match)", "15:00",
                  toronto_match=True, tentative=True),
            Match("Uruguay vs Spain", "20:00"),
        ],
        performances=[
            "Ikky",
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-06-27", "14:30", "22:00",
        matches=[
            Match("Panama vs England", "17:00"),
            Match("Colombia vs Portugal", "19:30"),
        ],
        performances=[
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-07-02", "13:30", "22:30", toronto_match_day=True,
        matches=[
            Match("Round of 32", "15:00"),
            Match("Round of 32", "19:00"),
        ],
        performances=[
            "Deborah Cox", "TOBi",
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-07-03", "12:30", "00:15", crosses_midnight=True,
        matches=[
            Match("Round of 32", "14:00"),
            Match("Round of 32", "18:00"),
            Match("Round of 32", "21:30"),
        ],
        performances=[
            "Bedouin Soundclash", "Tyler Shaw",
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-07-04", "11:30", "20:30",
        matches=[
            Match("Round of 16", "13:00"),
            Match("Round of 16", "17:00"),
        ],
        performances=[
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-07-05", "13:30", "22:30",
        matches=[
            Match("Round of 16", "16:00"),
            Match("Round of 16", "20:00"),
        ],
        performances=[
            "MICO",
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-07-11", "14:30", "00:00", crosses_midnight=True,
        matches=[
            Match("Quarter-final", "17:00"),
            Match("Quarter-final", "21:00"),
        ],
        performances=[
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-07-14", "12:30", "18:30",
        matches=[Match("Semi-final", "15:00")],
        performances=[
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-07-15", "12:30", "18:30",
        matches=[Match("Semi-final", "15:00")],
        performances=[
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-07-18", "14:00", "21:00",
        matches=[Match("Bronze Final (Third-place play-off)", "17:00")],
        performances=[
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
    FestivalDay("2026-07-19", "12:00", "20:30",
        matches=[Match("Championship Final", "15:00")],
        performances=[
            "Kardinal Offishall Presents: Soundclash Society",
        ]),
]

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
    """Fold lines longer than 75 octets per RFC 5545 §3.1 (CRLF + space)."""
    out = []
    while len(line.encode("utf-8")) > 75:
        # Cut at 74 chars (octets) so the leading space on the next
        # line keeps total under 75.
        cut = 74
        # Avoid splitting inside a multi-byte UTF-8 sequence.
        while cut > 1 and (line[cut].encode("utf-8")[0] & 0xC0) == 0x80:
            cut -= 1
        out.append(line[:cut])
        line = " " + line[cut:]
    out.append(line)
    return "\r\n".join(out)


def escape(text: str) -> str:
    """Escape TEXT values per RFC 5545 §3.3.11."""
    return (text
            .replace("\\", "\\\\")
            .replace(";", "\\;")
            .replace(",", "\\,")
            .replace("\n", "\\n"))


def fmt_local(date_str: str, hhmm: str, next_day: bool = False) -> str:
    """Return YYYYMMDDTHHMMSS for a TZID=America/Toronto value."""
    dt = datetime.fromisoformat(f"{date_str}T{hhmm}:00")
    if next_day:
        dt += timedelta(days=1)
    return dt.strftime("%Y%m%dT%H%M%S")


def add_minutes(date_str: str, hhmm: str, minutes: int) -> tuple[str, str, bool]:
    dt = datetime.fromisoformat(f"{date_str}T{hhmm}:00") + timedelta(minutes=minutes)
    base = datetime.fromisoformat(f"{date_str}T00:00:00")
    next_day = dt.date() != base.date()
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M"), next_day


def uid(seed: str) -> str:
    h = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"{h}@fifa-fan-festival-toronto-2026"


def emit_event(lines: list[str], *, summary: str, description: str,
               dtstart: str, dtend: str, uid_seed: str,
               categories: str = "FIFA Fan Festival Toronto") -> None:
    lines.append("BEGIN:VEVENT")
    lines.append(f"UID:{uid(uid_seed)}")
    lines.append(f"DTSTAMP:{DTSTAMP}")
    lines.append(f"DTSTART;TZID={TZID}:{dtstart}")
    lines.append(f"DTEND;TZID={TZID}:{dtend}")
    lines.append(fold(f"SUMMARY:{escape(summary)}"))
    lines.append(fold(f"LOCATION:{escape(VENUE)}"))
    lines.append(fold(f"DESCRIPTION:{escape(description)}"))
    lines.append(fold(f"CATEGORIES:{escape(categories)}"))
    lines.append("STATUS:CONFIRMED")
    lines.append("TRANSP:OPAQUE")
    lines.append("END:VEVENT")


def build_calendar() -> str:
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

    for day in SCHEDULE:
        # ---------- Day-level VEVENT (opening hours) ----------
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
                desc_lines.append(f"  • {m.start} — {m.title}{tor}{tag}")
            desc_lines.append("")
        if day.performances:
            desc_lines.append("Performances: " + ", ".join(day.performances))
        if day.cultural:
            desc_lines.append(
                "Cultural & community: " + ", ".join(day.cultural))
        desc_lines.append("")
        desc_lines.append(
            "Free general admission — advance ticket required. "
            "Tickets: https://www.torontofwc26.ca/FIFAFanFestival")
        desc_lines.append(
            "Schedule source: "
            "https://www.torontofwc26.ca/news/fifa-fan-festival-toronto-schedule")
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
            uid_seed=f"day-{day.date}",
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
                f"Match broadcast on the big screen at FIFA Fan "
                f"Festival\u2122 Toronto.",
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
                "Source: "
                "https://www.torontofwc26.ca/news/fifa-fan-festival-toronto-schedule")
            emit_event(
                lines,
                summary=f"Match: {m.title}{tag_str}",
                description="\n".join(match_desc),
                dtstart=fmt_local(day.date, m.start),
                dtend=fmt_local(end_date, end_hhmm, next_day=next_day),
                uid_seed=f"match-{day.date}-{m.start}-{m.title}",
                categories="FIFA Fan Festival Toronto, Match Broadcast",
            )

    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"


def main() -> None:
    ics = build_calendar()
    OUTPUT_FILE.write_bytes(ics.encode("utf-8"))
    n_days = len(SCHEDULE)
    n_matches = sum(len(d.matches) for d in SCHEDULE)
    print(f"Wrote {OUTPUT_FILE} "
          f"({n_days} festival days, {n_matches} match broadcasts, "
          f"{n_days + n_matches} VEVENTs).")


if __name__ == "__main__":
    main()
