"""End-to-end build: nflverse -> DuckDB.

The orchestrator stitches together the schema (``ffpts.db``), the
ingest functions (``ffpts.ingest``), and the upsert SQL into one
``build(seasons, ...)`` call. Per-season transactions and ``DELETE
WHERE season = ?`` followed by ``INSERT`` make the pipeline
idempotent — re-running for the same year replaces its rows.

Loader callables are dependency-injected so the integration test
runs entirely from synthetic in-process data with zero network.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

from ffpts.db import DEFAULT_DB_PATH, init_db
from ffpts.ingest import (
    DraftLoader,
    PlayerStatsLoader,
    build_team_seasons,
    default_draft_loader,
    default_player_stats_loader,
    load_draft_picks,
    load_player_seasons,
)


# Columns in the ingest output that don't belong in player_season_stats.
_STATS_EXTRA_COLUMNS = {"name"}
# Same idea for draft_picks.
_DRAFT_EXTRA_COLUMNS = {"name"}


def _upsert_players_from(con: duckdb.DuckDBPyConnection, staging_view: str) -> None:
    """Upsert (player_id, name) into players, widening first/last_season.

    nflverse occasionally has NULL ``player_display_name`` (some
    preseason-only or ST-only roles, players who never registered a
    regular-season game) — and ``pfr_player_name`` is sometimes NULL on
    older draft rows. The players.name column is NOT NULL, so we pick
    the first non-null name across the group, falling back to the
    player_id itself when no name exists anywhere in the source. That
    keeps the row addressable for joins instead of crashing the build.
    """
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


def _replace_draft_picks(con: duckdb.DuckDBPyConnection, draft: pl.DataFrame) -> None:
    """Full table replace — draft is one-shot, not per-season."""
    drop_cols = [c for c in draft.columns if c in _DRAFT_EXTRA_COLUMNS]
    insertable = draft.drop(drop_cols) if drop_cols else draft
    con.register("staging_draft_full", draft)
    _upsert_players_from(
        con,
        "(SELECT player_id, name, year AS season FROM staging_draft_full)",
    )
    con.unregister("staging_draft_full")
    con.execute("DELETE FROM draft_picks")
    con.register("staging_draft", insertable)
    con.execute("INSERT INTO draft_picks BY NAME SELECT * FROM staging_draft")
    con.unregister("staging_draft")


def _replace_player_season_stats(
    con: duckdb.DuckDBPyConnection, stats: pl.DataFrame, season: int
) -> None:
    # PK is (player_id, season, team) — drop rows where any PK column
    # is NULL. nflverse occasionally has NULL recent_team for rows that
    # never registered a regular-season snap with a real team
    # (preseason-only / ST-only / mid-camp roster moves). Those rows
    # can't satisfy the PK anyway and shouldn't crash the build.
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


def build(
    seasons: Iterable[int],
    *,
    db_path: str | Path | None = DEFAULT_DB_PATH,
    player_loader: PlayerStatsLoader = default_player_stats_loader,
    draft_loader: DraftLoader = default_draft_loader,
    con: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    """Build the DB for the given seasons. Returns a small summary dict.

    The seasons argument is materialized to a sorted list. If ``con`` is
    given, the pipeline writes to it directly (used by tests with an
    in-memory connection); otherwise it opens (or creates) the DB at
    ``db_path``.
    """
    seasons_list = sorted(set(seasons))
    if not seasons_list:
        raise ValueError("build(seasons=...) requires at least one season")

    owns_con = con is None
    if owns_con:
        con = init_db(db_path)

    try:
        # Team-season metadata for the requested years.
        ts = build_team_seasons(seasons_list)
        _replace_team_seasons(con, ts, seasons_list)

        # Draft picks: one-shot, all years up to max(seasons).
        draft = load_draft_picks(
            loader=draft_loader, through_season=seasons_list[-1]
        )
        _replace_draft_picks(con, draft)

        # Player season stats: per-year fetch + replace.
        stats_count_by_season: dict[int, int] = {}
        for season in seasons_list:
            stats = load_player_seasons([season], loader=player_loader)
            _replace_player_season_stats(con, stats, season)
            stats_count_by_season[season] = stats.height

        summary = {
            "seasons": seasons_list,
            "team_seasons_rows": ts.height,
            "draft_picks_rows": draft.height,
            "player_season_stats_rows": stats_count_by_season,
        }
    finally:
        if owns_con:
            con.close()

    return summary
