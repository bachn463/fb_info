"""End-to-end build: PFR -> DuckDB.

Every season goes through ``ffpts.ingest_pfr`` (the PFR scraper +
parsers). One uniform data source 1970-present means one player_id
namespace (``pfr:<slug>``), one set of stat columns, and one place
awards data lives. The nflverse loader (``ffpts.ingest``) is kept in
the tree for offline use / reference but is not called from this
pipeline.

Per-season transactions and ``DELETE WHERE season = ?`` followed by
``INSERT`` make the pipeline idempotent — re-running for the same year
replaces its rows.

The PFR scraper is dependency-injected: tests pass a fixture-backed
shim (see ``tests/test_ingest_pfr.py:_FixtureScraper``); production
calls ``Scraper.from_session_file()`` to read
``data/pfr_session.json``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import duckdb
import polars as pl

from ffpts import ingest_awards, ingest_pfr
from ffpts.db import DEFAULT_DB_PATH, init_db
from ffpts.ingest import build_team_seasons

# Columns in the ingest output that don't belong in player_season_stats.
# `awards` is the raw PFR cell string carried through for the awards
# ingest step; not stored on the wide stats fact table.
_STATS_EXTRA_COLUMNS = {"name", "team_slug", "awards"}
# Same idea for draft_picks.
_DRAFT_EXTRA_COLUMNS = {"name", "position"}


def _upsert_players_from(con: duckdb.DuckDBPyConnection, staging_view: str) -> None:
    """Upsert (player_id, name) into players, widening first/last_season."""
    con.execute(
        f"""
        INSERT INTO players (player_id, name, first_season, last_season)
        SELECT player_id,
               COALESCE(
                   MAX(name) FILTER (WHERE name IS NOT NULL),
                   player_id
               )                AS name,
               MIN(season)      AS first_season,
               MAX(season)      AS last_season
        FROM   {staging_view}
        WHERE  player_id IS NOT NULL
        GROUP BY player_id
        ON CONFLICT (player_id) DO UPDATE SET
            name         = COALESCE(excluded.name, players.name),
            first_season = LEAST(players.first_season,  excluded.first_season),
            last_season  = GREATEST(players.last_season, excluded.last_season);
        """
    )


def _replace_team_seasons(
    con: duckdb.DuckDBPyConnection, ts: pl.DataFrame, seasons: list[int]
) -> None:
    placeholders = ",".join(["?"] * len(seasons))
    con.execute(f"DELETE FROM team_seasons WHERE season IN ({placeholders})", seasons)
    con.register("staging_ts", ts)
    con.execute("INSERT INTO team_seasons BY NAME SELECT * FROM staging_ts")
    con.unregister("staging_ts")


def _replace_draft_picks_in_range(
    con: duckdb.DuckDBPyConnection,
    draft: pl.DataFrame,
    start: int,
    end: int,
) -> None:
    """Replace draft_picks rows for years in ``[start, end]``.

    Year-scoped DELETE + INSERT so re-running for a sub-range cleanly
    replaces just that window. All draft rows are in the
    ``pfr:<slug>`` namespace (a single source of truth) so there's no
    namespace partitioning to worry about anymore.
    """
    if draft.is_empty():
        con.execute(
            "DELETE FROM draft_picks WHERE year BETWEEN ? AND ?", [start, end]
        )
        return
    drop_cols = [c for c in draft.columns if c in _DRAFT_EXTRA_COLUMNS]
    insertable = draft.drop(drop_cols) if drop_cols else draft
    con.register("staging_draft_full", draft)
    _upsert_players_from(
        con,
        "(SELECT player_id, name, year AS season FROM staging_draft_full)",
    )
    con.unregister("staging_draft_full")
    con.execute(
        "DELETE FROM draft_picks WHERE year BETWEEN ? AND ?", [start, end]
    )
    con.register("staging_draft", insertable)
    con.execute("INSERT INTO draft_picks BY NAME SELECT * FROM staging_draft")
    con.unregister("staging_draft")


def _replace_player_season_stats(
    con: duckdb.DuckDBPyConnection, stats: pl.DataFrame, season: int
) -> None:
    # PK is (player_id, season, team) — drop rows where any PK column
    # is NULL (rare PFR parser-time anomalies).
    if stats.is_empty():
        con.execute("DELETE FROM player_season_stats WHERE season = ?", [season])
        con.execute("DELETE FROM player_awards WHERE season = ?", [season])
        return
    insertable = stats.filter(
        pl.col("player_id").is_not_null()
        & pl.col("season").is_not_null()
        & pl.col("team").is_not_null()
    )
    drop_cols = [c for c in insertable.columns if c in _STATS_EXTRA_COLUMNS]
    if drop_cols:
        insertable = insertable.drop(drop_cols)
    con.register("staging_stats_full", stats)
    _upsert_players_from(con, "staging_stats_full")
    con.unregister("staging_stats_full")
    con.execute("DELETE FROM player_season_stats WHERE season = ?", [season])
    con.register("staging_stats", insertable)
    con.execute(
        "INSERT INTO player_season_stats BY NAME SELECT * FROM staging_stats"
    )
    con.unregister("staging_stats")

    # Awards — derive from the raw `awards` cell carried through the
    # staging frame, then replace this season's rows in player_awards.
    awards_df = ingest_awards.derive_awards(stats)
    con.execute("DELETE FROM player_awards WHERE season = ?", [season])
    if not awards_df.is_empty():
        con.register("staging_awards", awards_df)
        con.execute(
            "INSERT INTO player_awards BY NAME SELECT * FROM staging_awards"
        )
        con.unregister("staging_awards")


def _insert_supplemental_drafts(con: duckdb.DuckDBPyConnection) -> int:
    """Insert hand-encoded supplemental-draft picks into draft_picks.

    For each entry in ``SUPPLEMENTAL_DRAFTS``, look up matching
    ``player_id``s by display name in the populated ``players`` table
    and insert a draft row keyed by each. Idempotent (uses ``ON
    CONFLICT DO UPDATE``). Returns count of rows inserted/updated.
    """
    from ffpts.supplemental_drafts import SUPP_PICK_SENTINEL, SUPPLEMENTAL_DRAFTS

    affected = 0
    for supp in SUPPLEMENTAL_DRAFTS:
        rows = con.execute(
            "SELECT player_id FROM players WHERE name = ?", [supp.name]
        ).fetchall()
        for (pid,) in rows:
            con.execute(
                """
                INSERT INTO draft_picks (player_id, year, round, overall_pick, team)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (player_id) DO UPDATE SET
                    year         = excluded.year,
                    round        = excluded.round,
                    overall_pick = excluded.overall_pick,
                    team         = excluded.team
                """,
                [pid, supp.year, supp.round, SUPP_PICK_SENTINEL, supp.team],
            )
            affected += 1
    return affected


def _populate_player_colleges(con: duckdb.DuckDBPyConnection) -> int:
    """Fill ``players.college`` from the auto-scraped draft data, then
    overlay hand-curated overrides.

    Two-phase:
      1. UPDATE players SET college = (drafted-from school) for every
         player that has a draft_picks row with a non-NULL college.
         Source-of-truth for drafted players who never transferred.
      2. Apply each entry in ``KNOWN_COLLEGE_OVERRIDES`` — full
         college history (transfers) for the famous ones, plus
         missing-data fills for HOF UDFAs and supplemental-draft
         picks where step 1 left college NULL.

    Returns the count of override rows applied (for the build summary).
    """
    from ffpts.supplemental_drafts import KNOWN_COLLEGE_OVERRIDES

    # Phase 1: copy from draft_picks for every player where the draft
    # page recorded a college. Doesn't widen NULLs from supplemental
    # picks (their draft_picks.college is NULL) so the override step
    # below is the only place those players get a college.
    con.execute(
        """
        UPDATE players
        SET    college = d.college
        FROM   draft_picks d
        WHERE  d.player_id = players.player_id
          AND  d.college IS NOT NULL
        """
    )

    # Phase 2: hand-curated overrides. Each override fully replaces
    # the players.college value (the override is the more accurate
    # truth). Match by player_id when provided; otherwise by name.
    affected = 0
    for ov in KNOWN_COLLEGE_OVERRIDES:
        joined = ", ".join(ov.colleges)
        if ov.player_id is not None:
            cur = con.execute(
                "UPDATE players SET college = ? WHERE player_id = ?",
                [joined, ov.player_id],
            )
        else:
            cur = con.execute(
                "UPDATE players SET college = ? WHERE name = ?",
                [joined, ov.name],
            )
        # DuckDB's UPDATE returns rowcount via .fetchone() of an
        # implicit RETURNING; safer to look it up via a separate count.
        if ov.player_id is not None:
            n = con.execute(
                "SELECT COUNT(*) FROM players WHERE player_id = ? AND college = ?",
                [ov.player_id, joined],
            ).fetchone()[0]
        else:
            n = con.execute(
                "SELECT COUNT(*) FROM players WHERE name = ? AND college = ?",
                [ov.name, joined],
            ).fetchone()[0]
        affected += n
    return affected


def _insert_year_summary_awards(
    con: duckdb.DuckDBPyConnection, awards: pl.DataFrame
) -> None:
    """Insert year-summary awards (WPMOY etc) into player_awards.

    Also upserts the matching ``players`` rows since many winners
    (offensive linemen, special teamers) don't have stats on any of
    our parsed pages — without an explicit upsert here the INNER JOIN
    in ``v_award_winners`` would drop them.

    Idempotent — uses ON CONFLICT DO UPDATE so re-running build for
    the same year refreshes rather than duplicates.
    """
    if awards.is_empty():
        return
    con.register("staging_summary_awards", awards)
    # Upsert the players row first so v_award_winners' INNER JOIN
    # finds it. season is reused as both first_season and last_season
    # for an upsert that doesn't widen.
    _upsert_players_from(
        con,
        "(SELECT player_id, name, season FROM staging_summary_awards)",
    )
    con.execute(
        """
        INSERT INTO player_awards (player_id, season, award_type, vote_finish)
        SELECT player_id, season, award_type, vote_finish
        FROM   staging_summary_awards
        ON CONFLICT (player_id, season, award_type) DO UPDATE SET
            vote_finish = excluded.vote_finish
        """
    )
    con.unregister("staging_summary_awards")


def _attach_team_records(
    con: duckdb.DuckDBPyConnection,
    records: pl.DataFrame,
    seasons: list[int],
) -> None:
    """Update team_seasons.wins/losses from PFR standings rows.

    Joins by (franchise, season) — the era-table-derived team_seasons
    already has the right (team, season, franchise) tuple, and the
    standings parser provides the franchise key alongside W/L.
    """
    if records.is_empty():
        return
    con.register("staging_records", records)
    con.execute(
        """
        UPDATE team_seasons AS t
        SET    wins   = r.wins,
               losses = r.losses
        FROM   staging_records r
        WHERE  t.franchise = r.franchise
          AND  t.season    = r.season
        """
    )
    con.unregister("staging_records")


def build(
    seasons: Iterable[int],
    *,
    db_path: str | Path | None = DEFAULT_DB_PATH,
    pfr_scraper: Optional[object] = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    """Build the DB for the given seasons. Every year routes through
    ``ingest_pfr`` (PFR HTML via the browser-cookie scraper).

    ``pfr_scraper`` is duck-typed (``.get(path) -> str``). Tests pass
    a fixture-backed shim; production constructs one via
    ``Scraper.from_session_file()`` reading ``data/pfr_session.json``.
    """
    seasons_list = sorted(set(seasons))
    if not seasons_list:
        raise ValueError("build(seasons=...) requires at least one season")

    if pfr_scraper is None:
        from ffpts.scraper import Scraper
        pfr_scraper = Scraper.from_session_file()

    owns_con = con is None
    if owns_con:
        con = init_db(db_path)

    try:
        # Team-season metadata for the requested years (era table).
        ts = build_team_seasons(seasons_list)
        _replace_team_seasons(con, ts, seasons_list)

        # Draft picks for all requested years.
        drafts = ingest_pfr.load_draft_picks(seasons_list, scraper=pfr_scraper)
        _replace_draft_picks_in_range(
            con, drafts, seasons_list[0], seasons_list[-1]
        )

        # Standings W/L overrides.
        records = ingest_pfr.load_team_season_records(
            seasons_list, scraper=pfr_scraper
        )
        _attach_team_records(con, records, seasons_list)

        # Player-season stats + inline awards per year. Inline awards
        # come from the per-row `awards` cell on stat tables (MVP, OPOY,
        # PB, AP_FIRST, etc.); they get derived + inserted inside
        # _replace_player_season_stats.
        stats_count_by_season: dict[int, int] = {}
        for season in seasons_list:
            stats = ingest_pfr.load_player_seasons(
                [season], scraper=pfr_scraper
            )
            _replace_player_season_stats(con, stats, season)
            stats_count_by_season[season] = stats.height

        # Year-summary awards (WPMOY etc) live on the /years/YYYY/
        # page only. Insert AFTER the per-season stats loop so the
        # season-level DELETE inside _replace_player_season_stats
        # doesn't clobber them.
        summary_awards = ingest_pfr.load_year_summary_awards(
            seasons_list, scraper=pfr_scraper
        )
        _insert_year_summary_awards(con, summary_awards)

        # Supplemental drafts: name-lookup insertion after stats land
        # so the players-table lookup finds the right IDs.
        supp_count = _insert_supplemental_drafts(con)

        # College population: copy draft_picks.college -> players.college
        # then overlay hand-curated overrides (transfers, UDFAs, supp
        # picks). Runs LAST so the players table is fully populated
        # before we set college values on it.
        college_overrides = _populate_player_colleges(con)

        awards_total = con.execute(
            "SELECT COUNT(*) FROM player_awards"
        ).fetchone()[0]

        summary = {
            "seasons": seasons_list,
            "team_seasons_rows": ts.height,
            "draft_picks_rows": drafts.height,
            "supplemental_draft_rows": supp_count,
            "college_override_rows": college_overrides,
            "player_season_stats_rows": stats_count_by_season,
            "player_awards_rows": awards_total,
        }
    finally:
        if owns_con:
            con.close()

    return summary
