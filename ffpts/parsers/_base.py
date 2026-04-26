"""Shared parsing helpers for PFR HTML pages.

Two PFR quirks each parser has to handle:

1. **Stat tables are wrapped in HTML comments** to deter bots. The
   real markup looks like::

       <div class="placeholder"></div>
       <!--
         <div ...><table id="passing">...</table></div>
       -->

   We strip the surrounding ``<!--`` / ``-->`` markers around every
   ``<table>`` block before BeautifulSoup so the parser can find the
   table by id.

2. **Repeated header rows** appear interspersed in the tbody to keep
   columns labeled when scrolling — a row with ``class="thead"``
   every ~20 data rows. ``extract_table_rows`` skips them.

Each row is returned as a dict keyed by the cell's ``data-stat``
attribute, plus synthetic ``_player_slug`` / ``_team_slug`` when the
row has a player/team link. The dict values are the cell's stripped
text; numeric coercion happens in the page-specific parsers, not here.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, Tag

# Match the `<!--` and corresponding `-->` markers that PFR wraps
# directly around <table> blocks. We unwrap *only* comments that contain
# a `<table>` element — there are unrelated comments in PFR HTML we
# shouldn't touch (analytics tags, build markers, etc).
_COMMENT_AROUND_TABLE_RE = re.compile(
    r"<!--\s*(.*?<table[^>]*>.*?</table>.*?)\s*-->",
    re.DOTALL,
)

# PFR player link href pattern: /players/X/Xxxx00.htm
_PLAYER_HREF_RE = re.compile(r"/players/([A-Z])/([A-Za-z0-9]+)\.htm")

# Team link href pattern: /teams/abc/2023.htm
_TEAM_HREF_RE = re.compile(r"/teams/([a-z]{3})/\d{4}\.htm")


def unwrap_pfr_comments(html: str) -> str:
    """Strip ``<!-- ... -->`` markers around any block containing a
    ``<table>`` element, leaving the inner HTML in place so BS4 can
    parse it normally. Idempotent; non-matching comments untouched.
    """
    return _COMMENT_AROUND_TABLE_RE.sub(r"\1", html)


def extract_player_slug(cell: Tag) -> str | None:
    """Return the PFR slug from a player-name `<td>`'s `<a href>`, if any.

    Slug example: ``McCaCh01`` (extracted from ``/players/M/McCaCh01.htm``).
    Returns ``None`` if the cell has no PFR player link (e.g. league-
    leader summary rows).
    """
    a = cell.find("a", href=True)
    if a is None:
        return None
    m = _PLAYER_HREF_RE.search(a["href"])
    return m.group(2) if m else None


def extract_team_slug(cell: Tag) -> str | None:
    """Return the PFR franchise URL slug (e.g. ``crd``, ``rai``) from
    a team `<td>`'s `<a href>`, if any. Returns the *franchise-stable*
    slug — the same `rai` for an OAK 2010 row and an LV 2020 row.
    """
    a = cell.find("a", href=True)
    if a is None:
        return None
    m = _TEAM_HREF_RE.search(a["href"])
    return m.group(1) if m else None


def extract_table_rows(html: str, table_id: str) -> list[dict[str, str | None]]:
    """Return one dict per data row in the named PFR table.

    Each dict's keys are the cell's ``data-stat`` attribute names; the
    values are the cells' stripped text. Two synthetic keys are added
    when the row carries the matching link:

    - ``_player_slug``: from the `data-stat="player"` cell's link.
    - ``_team_slug``: from the `data-stat="team"` (or `team_name`) cell.

    Rows are filtered to skip:
    - Repeated header rows (``class="thead"``).
    - Rows with no `data-stat` cells (defensive).

    The HTML is unwrapped of PFR comment markers up front, so passing
    the raw page works.
    """
    # PFR uses several data-stat names for the player-name and team-name
    # cells across page types and eras: `player`, `name_display` for the
    # player cell; `team`, `team_name`, `team_name_abbr` for the team
    # cell. Treat all of them as the link-bearing cell so synthetic
    # slugs come out regardless of which table we're parsing.
    PLAYER_STATS = {"player", "name_display"}
    TEAM_STATS = {"team", "team_name", "team_name_abbr"}

    soup = BeautifulSoup(unwrap_pfr_comments(html), "lxml")
    table = soup.find("table", id=table_id)
    if table is None:
        return []
    tbody = table.find("tbody") or table
    out: list[dict[str, str | None]] = []
    for tr in tbody.find_all("tr"):
        classes = tr.get("class") or []
        if "thead" in classes:
            continue
        row: dict[str, str | None] = {}
        # Both <th data-stat=...> and <td data-stat=...> appear; PFR
        # uses <th> for the player-name cell in some tables.
        for cell in tr.find_all(["th", "td"]):
            stat = cell.get("data-stat")
            if not stat:
                continue
            text = cell.get_text(strip=True) or None
            row[stat] = text
            if stat in PLAYER_STATS:
                slug = extract_player_slug(cell)
                if slug is not None:
                    row["_player_slug"] = slug
            elif stat in TEAM_STATS:
                slug = extract_team_slug(cell)
                if slug is not None:
                    row["_team_slug"] = slug
        if row:
            out.append(row)
    return out
