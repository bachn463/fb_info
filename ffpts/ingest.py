"""nflverse → player_season_stats schema mapping.

The ``transform_player_seasons`` function takes a polars DataFrame in
nflverse's ``load_player_stats`` format and returns one in our
``player_season_stats`` schema (plus a ``name`` column carrying the
player display name for the parent ``players`` upsert).

Fantasy points are computed inline as polars expressions for QB / RB /
WR / TE / FB rows; everything else gets NULL fpts. The formula must
match ``ffpts.scoring.fantasy_points`` — there is a test that re-runs
the scoring module on a sample row and asserts equality.

The ``loader`` callable is injectable so tests run network-free
against a synthetic DataFrame; the default loader hits nflverse via
nflreadpy, which fetches a parquet file from GitHub Releases (no
Cloudflare).
"""

from __future__ import annotations

from typing import Callable, Iterable

import polars as pl

PlayerStatsLoader = Callable[[Iterable[int]], pl.DataFrame]


def default_player_stats_loader(seasons: Iterable[int]) -> pl.DataFrame:
    import nflreadpy as nfl

    return nfl.load_player_stats(
        seasons=list(seasons),
        summary_level="reg",
    )


# Direct rename: nflverse seasonal column -> player_season_stats column.
# Anything in our schema not listed here is computed below or left NULL.
_DIRECT_RENAMES: dict[str, str] = {
    "player_id":               "player_id",
    "player_display_name":     "name",
    "position":                "position",
    "season":                  "season",
    "recent_team":             "team",
    "games":                   "games",
    # passing
    "completions":             "pass_cmp",
    "attempts":                "pass_att",
    "passing_yards":           "pass_yds",
    "passing_tds":             "pass_td",
    "passing_interceptions":   "pass_int",
    "sacks_suffered":          "pass_sacks_taken",
    "sack_yards_lost":         "pass_sack_yds",
    "passing_2pt_conversions": "two_pt_pass",
    # rushing
    "carries":                 "rush_att",
    "rushing_yards":           "rush_yds",
    "rushing_tds":             "rush_td",
    "rushing_2pt_conversions": "two_pt_rush",
    # receiving
    "targets":                 "targets",
    "receptions":              "rec",
    "receiving_yards":         "rec_yds",
    "receiving_tds":           "rec_td",
    "receiving_2pt_conversions": "two_pt_rec",
    # defense
    "def_tackles_solo":        "def_tackles_solo",
    "def_tackle_assists":      "def_tackles_assist",
    "def_sacks":               "def_sacks",
    "def_interceptions":       "def_int",
    "def_interception_yards":  "def_int_yds",
    "def_pass_defended":       "def_pass_def",
    "def_fumbles_forced":      "def_fumbles_forced",
    "def_safeties":            "def_safeties",
    "fumble_recovery_opp":     "def_fumbles_rec",
    "fumble_recovery_yards_opp": "def_fumbles_rec_yds",
    # kicking
    "fg_made":                 "fg_made",
    "fg_att":                  "fg_att",
    "fg_long":                 "fg_long",
    "pat_made":                "xp_made",
    "pat_att":                 "xp_att",
    # returns (return TDs are buried in special_teams_tds — left NULL)
    "punt_returns":            "pr",
    "punt_return_yards":       "pr_yds",
    "kickoff_returns":         "kr",
    "kickoff_return_yards":    "kr_yds",
}

_FUMBLE_PREFIXES = ("sack_fumbles", "rushing_fumbles", "receiving_fumbles")

# Positions that get fantasy points computed. Defensive players, kickers,
# and special-teams-only players get NULL fpts (we don't define a fantasy
# formula for them).
_SKILL_POSITIONS = ("QB", "RB", "WR", "TE", "FB", "HB")


def _present(raw: pl.DataFrame, col: str) -> pl.Expr:
    """``col`` from raw if present, else a NULL literal of unknown dtype."""
    if col in raw.columns:
        return pl.col(col)
    return pl.lit(None)


def _sum_present(raw: pl.DataFrame, cols: list[str]) -> pl.Series:
    """Sum the named columns from ``raw``, treating absent ones as 0.

    Returns a polars Series (one value per row in ``raw``). If none of
    the columns are present, returns a Series of zeros so downstream
    code can still ``.alias(...)`` it.
    """
    present = [c for c in cols if c in raw.columns]
    if not present:
        return pl.Series(values=[0] * raw.height, dtype=pl.Int64)
    total = raw[present[0]].fill_null(0).cast(pl.Int64)
    for c in present[1:]:
        total = total + raw[c].fill_null(0).cast(pl.Int64)
    return total


def transform_player_seasons(raw: pl.DataFrame) -> pl.DataFrame:
    """Map nflverse seasonal rows to our schema, with fantasy points."""
    # 1) Direct renames (with NULL fallback for absent source columns).
    select_exprs: list[pl.Expr] = [
        _present(raw, src).alias(dst) for src, dst in _DIRECT_RENAMES.items()
    ]
    df = raw.select(select_exprs)

    # 2) Aggregate fumble columns: nflverse splits fumbles by category
    #    (sack/rush/rec); our schema stores totals.
    fumbles = _sum_present(raw, [p for p in _FUMBLE_PREFIXES])
    fumbles_lost = _sum_present(raw, [f"{p}_lost" for p in _FUMBLE_PREFIXES])
    df = df.with_columns(
        fumbles.alias("fumbles"),
        fumbles_lost.alias("fumbles_lost"),
    )

    # 3) Combined defensive tackles.
    df = df.with_columns(
        (
            pl.col("def_tackles_solo").fill_null(0).cast(pl.Int64)
            + pl.col("def_tackles_assist").fill_null(0).cast(pl.Int64)
        ).alias("def_tackles_combined")
    )

    # 4) Fantasy points for skill positions.
    base = (
        pl.col("pass_yds").fill_null(0).cast(pl.Float64) / 25.0
        + 4 * pl.col("pass_td").fill_null(0).cast(pl.Float64)
        - 2 * pl.col("pass_int").fill_null(0).cast(pl.Float64)
        + pl.col("rush_yds").fill_null(0).cast(pl.Float64) / 10.0
        + 6 * pl.col("rush_td").fill_null(0).cast(pl.Float64)
        + pl.col("rec_yds").fill_null(0).cast(pl.Float64) / 10.0
        + 6 * pl.col("rec_td").fill_null(0).cast(pl.Float64)
        - 2 * pl.col("fumbles_lost").fill_null(0).cast(pl.Float64)
        + 2 * (
            pl.col("two_pt_pass").fill_null(0).cast(pl.Float64)
            + pl.col("two_pt_rush").fill_null(0).cast(pl.Float64)
            + pl.col("two_pt_rec").fill_null(0).cast(pl.Float64)
        )
    )
    rec_count = pl.col("rec").fill_null(0).cast(pl.Float64)
    is_skill = pl.col("position").is_in(list(_SKILL_POSITIONS))
    df = df.with_columns(
        pl.when(is_skill).then(base).otherwise(None).alias("fpts_std"),
        pl.when(is_skill).then(base + 0.5 * rec_count).otherwise(None).alias("fpts_half"),
        pl.when(is_skill).then(base + 1.0 * rec_count).otherwise(None).alias("fpts_ppr"),
    )

    # 5) Provenance / quality flags.
    df = df.with_columns(
        pl.lit("nflverse").alias("sources"),
        pl.lit(True).alias("has_fumbles_lost"),
    )

    return df


def load_player_seasons(
    seasons: Iterable[int],
    *,
    loader: PlayerStatsLoader = default_player_stats_loader,
) -> pl.DataFrame:
    """Top-level: fetch + transform. The default loader hits nflverse."""
    raw = loader(seasons)
    return transform_player_seasons(raw)


# ---------------------------------------------------------------------------
# Draft picks
# ---------------------------------------------------------------------------

DraftLoader = Callable[[], pl.DataFrame]


def default_draft_loader() -> pl.DataFrame:
    import nflreadpy as nfl

    return nfl.load_draft_picks()


def transform_draft_picks(raw: pl.DataFrame) -> pl.DataFrame:
    """Map nflverse draft rows to our draft_picks schema.

    Drops rows without a gsis_id (players who never registered an NFL
    appearance — they can't join to player_season_stats anyway).
    """
    return (
        raw
        .filter(pl.col("gsis_id").is_not_null())
        .select(
            pl.col("gsis_id").alias("player_id"),
            pl.col("season").cast(pl.Int64).alias("year"),
            pl.col("round").cast(pl.Int64).alias("round"),
            pl.col("pick").cast(pl.Int64).alias("overall_pick"),
            pl.col("team"),
            # Keep player_name for the players-table upsert in pipeline.
            pl.col("pfr_player_name").alias("name"),
        )
        # Defensive: same gsis_id appearing twice would break the PK. Take
        # the earliest (lowest season, then lowest pick) as the canonical
        # draft entry.
        .sort(["player_id", "year", "overall_pick"])
        .unique(subset=["player_id"], keep="first")
    )


def load_draft_picks(
    *,
    loader: DraftLoader = default_draft_loader,
    through_season: int | None = None,
) -> pl.DataFrame:
    """Fetch + transform. ``through_season`` filters to picks in or before
    the given year, useful for keeping the table aligned with the seasons
    range the pipeline is loading."""
    raw = loader()
    df = transform_draft_picks(raw)
    if through_season is not None:
        df = df.filter(pl.col("year") <= through_season)
    return df
