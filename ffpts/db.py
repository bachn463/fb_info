"""DuckDB schema, connection helper, and convenience views.

Single-file embedded DB. The fact table ``player_season_stats`` is a
wide, sparse layout (one row per (player, season, team) — most defenders
have NULL passing/receiving columns and most QBs have NULL defensive
columns). DuckDB stores this columnar so NULLs are cheap and aggregation
queries stay fast.

Two reasons not to normalize into per-category tables:
- The query surface ("most INTs by NFC North 1990-2005") joins one fact
  table to one team_seasons row — no five-way joins.
- Adding a new stat is a column add, not a new table.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

DEFAULT_DB_PATH = Path("data/ff.duckdb")


SCHEMA_DDL: list[str] = [
    # ---------- reference tables ----------
    """
    -- ``players`` is the canonical home for player metadata that
    -- isn't tied to a specific season. ``college`` is populated from
    -- two sources: PFR draft pages (drafted-from school, copied via
    -- pipeline UPDATE) and curated overrides for transfers / UDFAs /
    -- supplemental-draft picks whose college isn't on the draft page.
    CREATE TABLE IF NOT EXISTS players (
        player_id     TEXT PRIMARY KEY,   -- PFR slug, e.g. "McCaCh01"
        name          TEXT NOT NULL,
        first_season  INTEGER,
        last_season   INTEGER,
        college       TEXT                -- comma-list, e.g. "Alabama, Oklahoma"
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS draft_picks (
        player_id     TEXT PRIMARY KEY,
        year          INTEGER NOT NULL,
        round         INTEGER NOT NULL,
        overall_pick  INTEGER NOT NULL,
        team          TEXT    NOT NULL,   -- drafting team, PFR display code that year
        college       TEXT               -- "Alabama", "Ohio St.", etc.; NULL for older / unknown
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS team_seasons (
        team        TEXT    NOT NULL,     -- PFR display code as used that year
        season      INTEGER NOT NULL,
        franchise   TEXT    NOT NULL,     -- stable across relocations ("raiders")
        conference  TEXT    NOT NULL,     -- "AFC" | "NFC"
        division    TEXT    NOT NULL,     -- "NFC North", "NFC Central", ...
        wins        INTEGER,
        losses      INTEGER,
        ties        INTEGER,
        PRIMARY KEY (team, season)
    );
    """,
    # ---------- wide season-totals fact table ----------
    """
    CREATE TABLE IF NOT EXISTS player_season_stats (
        player_id        TEXT    NOT NULL,
        season           INTEGER NOT NULL,
        team             TEXT    NOT NULL,    -- "2TM"/"3TM" for multi-team summary rows
        position         TEXT,
        age              INTEGER,
        games            INTEGER,
        games_started    INTEGER,

        -- Passing (offense)
        pass_cmp         INTEGER,
        pass_att         INTEGER,
        pass_yds         INTEGER,
        pass_td          INTEGER,
        pass_int         INTEGER,            -- INTs THROWN (offense)
        pass_sacks_taken INTEGER,
        pass_sack_yds    INTEGER,
        pass_long        INTEGER,
        pass_rating      DOUBLE,

        -- Rushing
        rush_att         INTEGER,
        rush_yds         INTEGER,
        rush_td          INTEGER,
        rush_long        INTEGER,

        -- Receiving
        targets          INTEGER,
        rec              INTEGER,
        rec_yds          INTEGER,
        rec_td           INTEGER,
        rec_long         INTEGER,

        -- Defense (def_ prefix to disambiguate from offense)
        def_tackles_solo     INTEGER,
        def_tackles_assist   INTEGER,
        def_tackles_combined INTEGER,
        def_sacks            DOUBLE,
        def_int              INTEGER,        -- INTs CAUGHT (defense)
        def_int_yds          INTEGER,
        def_int_td           INTEGER,
        def_pass_def         INTEGER,
        def_fumbles_forced   INTEGER,
        def_fumbles_rec      INTEGER,
        def_fumbles_rec_yds  INTEGER,
        def_fumbles_rec_td   INTEGER,
        def_safeties         INTEGER,

        -- Kicking
        fg_made          INTEGER,
        fg_att           INTEGER,
        fg_long          INTEGER,
        xp_made          INTEGER,
        xp_att           INTEGER,

        -- Punting
        punts            INTEGER,
        punt_yds         INTEGER,
        punt_long        INTEGER,

        -- Returns
        kr               INTEGER,
        kr_yds           INTEGER,
        kr_td            INTEGER,
        pr               INTEGER,
        pr_yds           INTEGER,
        pr_td            INTEGER,

        -- Offensive misc
        fumbles          INTEGER,
        fumbles_lost     INTEGER,
        two_pt_pass      INTEGER,
        two_pt_rush      INTEGER,
        two_pt_rec       INTEGER,

        -- Computed fantasy (skill positions only; NULL otherwise)
        fpts_std         DOUBLE,
        fpts_half        DOUBLE,
        fpts_ppr         DOUBLE,

        -- Provenance / quality flags
        sources          TEXT,                -- comma-separated PFR pages contributing
        has_fumbles_lost BOOLEAN,             -- false for pre-1994 (column absent)

        PRIMARY KEY (player_id, season, team)
    );
    """,
    # ---------- awards (per player-season, multiple per row) ----------
    """
    CREATE TABLE IF NOT EXISTS player_awards (
        player_id    TEXT    NOT NULL,
        season       INTEGER NOT NULL,
        award_type   TEXT    NOT NULL,        -- 'MVP', 'OPOY', 'DPOY',
                                              -- 'OROY', 'DROY', 'CPOY',
                                              -- 'WPMOY', 'PB',
                                              -- 'AP_FIRST', 'AP_SECOND'
        vote_finish  INTEGER,                 -- 1 = won, 2+ = placing.
                                              -- NULL for binary awards
                                              -- (PB, AP_FIRST,
                                              -- AP_SECOND, WPMOY).
        PRIMARY KEY (player_id, season, award_type)
    );
    """,
]


VIEWS_DDL: list[str] = [
    """
    CREATE OR REPLACE VIEW v_player_season_full AS
    SELECT  s.*,
            -- Per-season ratio stats. NULLIF guards every denominator
            -- so 0-attempt / 0-target rows yield NULL (filtered out by
            -- pos_topN's IS NOT NULL clause) rather than crashing.
            CAST(s.pass_cmp AS DOUBLE)
                / NULLIF(s.pass_att, 0)            AS pass_cmp_pct,
            CAST(s.rec AS DOUBLE)
                / NULLIF(s.targets, 0)             AS catch_rate,
            p.name,
            d.year         AS draft_year,
            d.round        AS draft_round,
            d.overall_pick AS draft_overall_pick,
            d.team         AS draft_team,
            -- College sourced from players.college (canonical, includes
            -- curated transfer/UDFA overrides). draft_picks.college is
            -- the underlying scrape; the pipeline copies it forward and
            -- then applies overrides on top.
            p.college      AS college,
            t.conference,
            t.division,
            t.franchise
    FROM    player_season_stats s
    JOIN    players p USING (player_id)
    LEFT JOIN draft_picks d USING (player_id)
    LEFT JOIN team_seasons t ON t.team = s.team AND t.season = s.season;
    """,
    """
    CREATE OR REPLACE VIEW v_flex_seasons AS
    SELECT * FROM v_player_season_full
    WHERE  position IN ('RB', 'WR', 'TE');
    """,
    """
    CREATE OR REPLACE VIEW v_award_winners AS
    SELECT  pa.player_id,
            pa.season,
            pa.award_type,
            pa.vote_finish,
            p.name
    FROM    player_awards pa
    JOIN    players p USING (player_id);
    """,
]


def connect(path: str | Path | None = None) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection. ``path=None`` -> in-memory (used by tests)."""
    if path is None:
        return duckdb.connect(":memory:")
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


# Lightweight forward migrations for columns added after a DB was
# first populated. CREATE TABLE IF NOT EXISTS is a no-op against an
# existing table, so additive columns need an explicit ALTER. Each
# entry is (table, column, type) — DuckDB's ADD COLUMN IF NOT EXISTS
# makes this idempotent.
_COLUMN_MIGRATIONS: list[tuple[str, str, str]] = [
    ("draft_picks", "college", "TEXT"),
    ("players",     "college", "TEXT"),
]


def _migrate_columns(con: duckdb.DuckDBPyConnection) -> None:
    for table, column, sql_type in _COLUMN_MIGRATIONS:
        con.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {sql_type}"
        )


def apply_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create all tables and views. Safe to run on an existing DB —
    additive column migrations run before views are (re)created so
    a view referencing a newly-added column doesn't fail at bind time
    on a previously-populated DB."""
    for stmt in SCHEMA_DDL:
        con.execute(stmt)
    _migrate_columns(con)
    for stmt in VIEWS_DDL:
        con.execute(stmt)


def init_db(path: str | Path | None = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    """Connect + apply schema. Convenience for the pipeline and CLI."""
    con = connect(path)
    apply_schema(con)
    return con
