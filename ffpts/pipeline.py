"""End-to-end build: nflverse + PFR -> DuckDB.

The orchestrator stitches together the schema (``ffpts.db``), the
ingest functions (``ffpts.ingest`` for 1999+ via nflverse,
``ffpts.ingest_pfr`` for 1970-1998 via the PFR scraper), and the
upsert SQL into one ``build(seasons, ...)`` call.

Per-season transactions and ``DELETE WHERE season = ?`` followed by
``INSERT`` make the pipeline idempotent — re-running for the same year
replaces its rows. Draft picks are replaced *by year range* per source,
so the PFR pre-1999 draft data and the nflverse 1999+ draft data
coexist cleanly.

Loader callables are dependency-injected so the integration test
runs entirely from synthetic in-process data with zero network.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import duckdb
import polars as pl

from ffpts import ingest_pfr
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

# Boundary between the two data sources. Years strictly less than this
# come from PFR via ingest_pfr; this year and later come from nflverse.
NFLVERSE_MIN_YEAR = 1999

# Columns in the ingest output that don't belong in player_season_stats.
_STATS_EXTRA_COLUMNS = {"name", "team_slug"}
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


def _replace_draft_picks_namespaced(
    con: duckdb.DuckDBPyConnection,
    draft: pl.DataFrame,
    *,
    scope_sql: str,
    scope_params: list,
) -> None:
    """Replace draft_picks rows that match a namespace+range scope.

    PFR rows live in the ``pfr:<slug>`` player_id namespace; nflverse
    rows live in the GSIS namespace ("00-0033280"). They never
    collide on the PK, so each source replaces only its own
    namespace and they coexist. ``scope_sql`` and ``scope_params``
    define the DELETE clause (e.g. ``player_id LIKE 'pfr:%' AND year
    BETWEEN ? AND ?`` or ``player_id NOT LIKE 'pfr:%'``).
    """
    if draft.is_empty():
        # Nothing to insert; still scrub the scope so a re-run with no
        # data clears prior rows in that namespace.
        con.execute(f"DELETE FROM draft_picks WHERE {scope_sql}", scope_params)
        return
    drop_cols = [c for c in draft.columns if c in _DRAFT_EXTRA_COLUMNS]
    insertable = draft.drop(drop_cols) if drop_cols else draft
    con.register("staging_draft_full", draft)
    _upsert_players_from(
        con,
        "(SELECT player_id, name, year AS season FROM staging_draft_full)",
    )
    con.unregister("staging_draft_full")
    con.execute(f"DELETE FROM draft_picks WHERE {scope_sql}", scope_params)
    con.register("staging_draft", insertable)
    con.execute("INSERT INTO draft_picks BY NAME SELECT * FROM staging_draft")
    con.unregister("staging_draft")


def _replace_player_season_stats(
    con: duckdb.DuckDBPyConnection, stats: pl.DataFrame, season: int
) -> None:
    # PK is (player_id, season, team) — drop rows where any PK column
    # is NULL. Both data sources occasionally produce such rows
    # (nflverse: NULL recent_team for ST-only players; PFR: rare
    # parser-time anomalies).
    if stats.is_empty():
        con.execute("DELETE FROM player_season_stats WHERE season = ?", [season])
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


def _insert_supplemental_drafts(con: duckdb.DuckDBPyConnection) -> int:
    """Insert hand-encoded supplemental-draft picks into draft_picks.

    For each entry in ``SUPPLEMENTAL_DRAFTS``, look up matching
    ``player_id``s by display name in the populated ``players`` table
    and insert a draft row keyed by each. A player who spans the
    1999 boundary has two ``players`` rows (one ``pfr:<slug>``-keyed,
    one GSIS-keyed) so they get one supp-draft entry per namespace —
    each side of their stats finds its draft entry.

    Idempotent (uses ``ON CONFLICT DO UPDATE``). Returns count of rows
    inserted/updated.
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


def _attach_team_records(
    con: duckdb.DuckDBPyConnection,
    records: pl.DataFrame,
    seasons: list[int],
) -> None:
    """Update team_seasons.wins/losses/etc from PFR standings rows.

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
    player_loader: PlayerStatsLoader = default_player_stats_loader,
    draft_loader: DraftLoader = default_draft_loader,
    pfr_scraper: Optional[object] = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    """Build the DB for the given seasons. Routes per-year:

    - ``season < 1999``  → PFR via ``ingest_pfr`` (needs ``pfr_scraper``).
    - ``season >= 1999`` → nflverse via ``ingest`` (uses the loaders).

    The ``pfr_scraper`` argument is optional: if pre-1999 years are in
    the range and no scraper is passed, one is constructed via
    ``Scraper.from_session_file()`` (reads ``data/pfr_session.json``).
    Tests inject a fixture-backed shim instead.
    """
    seasons_list = sorted(set(seasons))
    if not seasons_list:
        raise ValueError("build(seasons=...) requires at least one season")

    pfr_years = [s for s in seasons_list if s < NFLVERSE_MIN_YEAR]
    nfl_years = [s for s in seasons_list if s >= NFLVERSE_MIN_YEAR]

    if pfr_years and pfr_scraper is None:
        # Lazy import so a pure-nflverse build doesn't even touch the
        # scraper module's session-file requirements.
        from ffpts.scraper import Scraper
        pfr_scraper = Scraper.from_session_file()

    owns_con = con is None
    if owns_con:
        con = init_db(db_path)

    try:
        # Team-season metadata for the requested years (era table).
        ts = build_team_seasons(seasons_list)
        _replace_team_seasons(con, ts, seasons_list)

        stats_count_by_season: dict[int, int] = {}

        # ---- PFR side (pre-1999) -----------------------------------------
        pfr_draft_count = 0
        if pfr_years:
            pfr_drafts = ingest_pfr.load_draft_picks(
                pfr_years, scraper=pfr_scraper
            )
            _replace_draft_picks_namespaced(
                con, pfr_drafts,
                scope_sql="player_id LIKE 'pfr:%' AND year BETWEEN ? AND ?",
                scope_params=[pfr_years[0], pfr_years[-1]],
            )
            pfr_draft_count = pfr_drafts.height

            # Standings W/L overrides.
            records = ingest_pfr.load_team_season_records(
                pfr_years, scraper=pfr_scraper
            )
            _attach_team_records(con, records, pfr_years)

            for season in pfr_years:
                stats = ingest_pfr.load_player_seasons(
                    [season], scraper=pfr_scraper
                )
                _replace_player_season_stats(con, stats, season)
                stats_count_by_season[season] = stats.height

        # ---- nflverse side (1999+) ---------------------------------------
        nfl_draft_count = 0
        if nfl_years:
            # nflverse's draft data goes back to 1936; load it all,
            # not just the requested year range. A 1999+ stats row for
            # Peyton Manning needs his 1998 draft entry to join, even
            # though 1998 is pre-NFLVERSE_MIN_YEAR. PFR-source drafts
            # use the pfr:<slug> namespace and don't collide with the
            # GSIS-keyed nflverse rows on the PK.
            nfl_drafts = load_draft_picks(
                loader=draft_loader, through_season=nfl_years[-1]
            )
            _replace_draft_picks_namespaced(
                con, nfl_drafts,
                scope_sql="player_id NOT LIKE 'pfr:%'",
                scope_params=[],
            )
            nfl_draft_count = nfl_drafts.height

            for season in nfl_years:
                stats = load_player_seasons([season], loader=player_loader)
                _replace_player_season_stats(con, stats, season)
                stats_count_by_season[season] = stats.height

        # ---- Supplemental drafts (cross-cutting) -------------------------
        # Has to run AFTER the namespace replacements and AFTER stats
        # are loaded so the players-table lookup finds whichever IDs
        # the supp picks ended up with in this build.
        supp_count = _insert_supplemental_drafts(con)

        summary = {
            "seasons": seasons_list,
            "pfr_seasons": pfr_years,
            "nfl_seasons": nfl_years,
            "team_seasons_rows": ts.height,
            "draft_picks_rows": pfr_draft_count + nfl_draft_count,
            "supplemental_draft_rows": supp_count,
            "player_season_stats_rows": stats_count_by_season,
        }
    finally:
        if owns_con:
            con.close()

    return summary
