"""Pre-1999 PFR ingest: stitches the 8 page parsers into our schema rows.

Mirrors the shape of ``ffpts.ingest`` but pulls from PFR HTML instead
of nflverse parquet. Three top-level functions:

- ``load_player_seasons(seasons, scraper)`` — fetches and merges
  passing / rushing / receiving / defense / kicking / returns into one
  row per ``(player_id, season, team)``. Computes fpts for skill
  positions inline.

- ``load_draft_picks(seasons, scraper)`` — fetches each year's draft
  page and concatenates.

- ``load_team_seasons_pfr(seasons, scraper)`` — fetches each year's
  standings and returns rows that can update the era-table-derived
  ``team_seasons`` rows with W/L.

The ``scraper`` parameter is duck-typed: anything with a ``get(path)``
method returning HTML works. Tests pass in a fixture-backed shim
that maps path -> tests/fixtures file, so the integration test runs
with zero network.
"""

from __future__ import annotations

from typing import Callable, Iterable, Protocol

import polars as pl

from ffpts.parsers import (
    parse_defense,
    parse_draft,
    parse_kicking,
    parse_passing,
    parse_receiving,
    parse_returns,
    parse_rushing,
    parse_standings,
)


class _ScraperLike(Protocol):
    def get(self, path: str) -> str: ...


# ---------------------------------------------------------------------------
# Player seasons
# ---------------------------------------------------------------------------

# Page paths and the parser to apply. Each parser returns a list of
# partial player-season dicts that share the same key shape (player_id,
# season, team) — we merge across pages by that key.
_PAGE_PARSERS: list[tuple[str, Callable[[str, int], list[dict]]]] = [
    ("/years/{year}/passing.htm",   parse_passing),
    ("/years/{year}/rushing.htm",   parse_rushing),
    ("/years/{year}/receiving.htm", parse_receiving),
    ("/years/{year}/defense.htm",   parse_defense),
    ("/years/{year}/kicking.htm",   parse_kicking),
    ("/years/{year}/returns.htm",   parse_returns),
]

# Fields used as identity keys; never overwritten once set.
_KEY_FIELDS = {"player_id", "season", "team"}
# Fields where the first non-None value wins. (Player metadata that
# every page reports.)
_FIRST_WINS = {"name", "position", "team_slug", "age", "games", "games_started"}

# Skill positions get fpts computed; everyone else gets NULL.
_SKILL_POSITIONS = {"QB", "RB", "WR", "TE", "FB", "HB"}


def _merge_partial(target: dict, partial: dict) -> None:
    """Update ``target`` in place with non-None fields from ``partial``."""
    for k, v in partial.items():
        if k in _KEY_FIELDS:
            continue
        if k in _FIRST_WINS:
            if target.get(k) is None and v is not None:
                target[k] = v
        else:
            if v is not None:
                # For numeric stats the per-page parser owns its category
                # exclusively, so no two pages should write the same key
                # — but if they do, last write wins (later parsers in the
                # list have priority by ordering convention).
                target[k] = v


def _compute_fpts(row: dict) -> None:
    """Populate fpts_std/half/ppr for skill-position rows."""
    if row.get("position") not in _SKILL_POSITIONS:
        row.setdefault("fpts_std", None)
        row.setdefault("fpts_half", None)
        row.setdefault("fpts_ppr", None)
        return

    def n(field: str) -> float:
        v = row.get(field)
        return float(v) if v is not None else 0.0

    base = (
        n("pass_yds") / 25.0
        + 4.0 * n("pass_td")
        - 2.0 * n("pass_int")
        + n("rush_yds") / 10.0
        + 6.0 * n("rush_td")
        + n("rec_yds") / 10.0
        + 6.0 * n("rec_td")
        - 2.0 * n("fumbles_lost")
        + 2.0 * (n("two_pt_pass") + n("two_pt_rush") + n("two_pt_rec"))
    )
    rec_count = n("rec")
    row["fpts_std"] = base
    row["fpts_half"] = base + 0.5 * rec_count
    row["fpts_ppr"] = base + 1.0 * rec_count


def load_player_seasons(
    seasons: Iterable[int],
    *,
    scraper: _ScraperLike,
) -> pl.DataFrame:
    """Merge all stat pages for each season into one row per player-team."""
    merged: dict[tuple[str, int, str], dict] = {}
    for season in seasons:
        for path_tmpl, parser in _PAGE_PARSERS:
            html = scraper.get(path_tmpl.format(year=season))
            for partial in parser(html, season):
                key = (partial["player_id"], partial["season"], partial["team"])
                if key not in merged:
                    merged[key] = {
                        "player_id": partial["player_id"],
                        "season":    partial["season"],
                        "team":      partial["team"],
                    }
                _merge_partial(merged[key], partial)

    for row in merged.values():
        _compute_fpts(row)
        row["sources"] = "pfr"
        # PFR pre-1994 lacks fumbles_lost; we don't get that column from
        # any of our parsers anyway, so the flag here is the season-aware
        # truth: True iff the season is 1994+.
        row["has_fumbles_lost"] = row["season"] >= 1994

    return pl.DataFrame(list(merged.values()))


# ---------------------------------------------------------------------------
# Draft picks
# ---------------------------------------------------------------------------

def load_draft_picks(
    seasons: Iterable[int],
    *,
    scraper: _ScraperLike,
) -> pl.DataFrame:
    """One row per draft pick across the requested seasons.

    Output schema columns: player_id, name, year, round, overall_pick,
    team. ``name`` and ``position`` flow through for the players-table
    upsert downstream.
    """
    rows: list[dict] = []
    for season in seasons:
        html = scraper.get(f"/years/{season}/draft.htm")
        rows.extend(parse_draft(html, season))
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Team seasons (standings) — produces W/L overrides for the era table
# ---------------------------------------------------------------------------

def load_team_season_records(
    seasons: Iterable[int],
    *,
    scraper: _ScraperLike,
) -> pl.DataFrame:
    """One row per (franchise, season) with W/L/PF/PA from PFR standings.

    The pipeline can join this against the era-table-derived
    team_seasons rows on (franchise, season) to populate the
    wins/losses/points columns.
    """
    rows: list[dict] = []
    for season in seasons:
        html = scraper.get(f"/years/{season}/")
        for r in parse_standings(html, season):
            if r.get("franchise") is None:
                # Defensive: a row without a recognized franchise can't
                # join anywhere; skip rather than poison the merge.
                continue
            rows.append(r)
    return pl.DataFrame(rows)
