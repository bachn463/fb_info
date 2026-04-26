"""Parse PFR's /years/YYYY/ season-summary page for division standings.

The page has two tables — ``id="AFC"`` and ``id="NFC"`` — each grouping
team rows by division. Division names appear as ``thead onecell`` rows
that interleave with the team rows (e.g. "AFC East" header followed by
the four AFC East team rows).

The parser walks each table row-by-row, tracks the current division as
it goes, and emits one dict per team-season:

  ``franchise``: stable franchise key from the team's PFR URL slug
                 (lookup via ``ffpts.normalize.PFR_FRANCHISE``).
  ``team_display_name``: full team name (e.g. "Miami Dolphins").
  ``season``, ``conference``, ``division``, ``wins``, ``losses``,
  ``points``, ``points_against``.

The franchise key is the join target against the era-table-derived
``team_seasons`` rows (which key on ``(team, season)`` but carry
``franchise``), so the pipeline can attach W/L without having to
match free-form team display names.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from ffpts.parsers._base import unwrap_pfr_comments

_TEAM_HREF_RE = re.compile(r"/teams/([a-z]{3})/\d{4}\.htm")


def _coerce_int(s: str | None) -> int | None:
    if s is None or s == "":
        return None
    try:
        return int(s.replace(",", ""))
    except ValueError:
        return None


def _strip_seed_marker(name: str | None) -> str | None:
    """PFR appends '*' (division winner) and '+' (wildcard berth) markers
    to the team name in standings rows. Strip them for the display name."""
    if not name:
        return name
    return name.rstrip("*+ ").strip()


def _parse_one_table(table, conference: str, season: int) -> list[dict]:
    from ffpts.normalize import PFR_FRANCHISE

    out: list[dict] = []
    tbody = table.find("tbody") or table
    current_division: str | None = None
    for tr in tbody.find_all("tr"):
        classes = tr.get("class") or []
        # Division header rows (e.g. "AFC East") — update the cursor and
        # move on without emitting a row.
        if "thead" in classes:
            cell = tr.find(["th", "td"])
            if cell is not None and cell.get("data-stat") == "onecell":
                current_division = cell.get_text(strip=True) or current_division
            continue

        if current_division is None:
            # Defensive: shouldn't see a team row before a division header.
            continue

        team_cell = tr.find(["th", "td"], attrs={"data-stat": "team"})
        if team_cell is None:
            continue
        link = team_cell.find("a", href=True)
        slug = None
        if link:
            m = _TEAM_HREF_RE.search(link["href"])
            if m:
                slug = m.group(1)
        franchise = None
        if slug is not None:
            entry = PFR_FRANCHISE.get(slug.lower())
            franchise = entry[0] if entry else None

        def _stat(name: str) -> str | None:
            c = tr.find(["th", "td"], attrs={"data-stat": name})
            return c.get_text(strip=True) if c is not None else None

        out.append(
            {
                "franchise":         franchise,
                "team_display_name": _strip_seed_marker(team_cell.get_text(strip=True)),
                "team_slug":         slug,
                "season":            season,
                "conference":        conference,
                "division":          current_division,
                "wins":              _coerce_int(_stat("wins")),
                "losses":            _coerce_int(_stat("losses")),
                "points":            _coerce_int(_stat("points")),
                "points_against":    _coerce_int(_stat("points_opp")),
            }
        )
    return out


def parse_standings(html: str, season: int) -> list[dict]:
    """One dict per team-season. Combines the AFC and NFC tables."""
    soup = BeautifulSoup(unwrap_pfr_comments(html), "lxml")
    rows: list[dict] = []
    for conference, table_id in (("AFC", "AFC"), ("NFC", "NFC")):
        table = soup.find("table", id=table_id)
        if table is None:
            continue
        rows.extend(_parse_one_table(table, conference, season))
    return rows
