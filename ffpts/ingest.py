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


# ---------------------------------------------------------------------------
# Team-seasons (division / conference / franchise per (team, season))
# ---------------------------------------------------------------------------
#
# nflverse exposes team metadata only for the *current* configuration —
# load_teams() returns one row per franchise with its current division.
# We need the per-season division because realignment matters for
# queries like "NFC North 1990-2005" (which spans the 2002 realignment).
#
# Each era's team list is a list of ``(team_code, franchise_key)`` tuples
# rather than bare team codes. Reason: some team codes mean different
# franchises in different eras —
#
#   * "STL" = St. Louis Cardinals (1970-1987)  AND  St. Louis Rams (1995-2015)
#   * "BAL" = Baltimore Colts    (1970-1983)   AND  Baltimore Ravens (1996+)
#   * "HOU" = Houston Oilers     (1970-1996)   AND  Houston Texans (2002+)
#
# A flat team-code -> franchise lookup can't represent that. Embedding
# the franchise alongside each team in the era band makes the right
# answer fall out of "which era did this season belong to?".
#
# Eras encode every realignment 1970-present:
#
#   1970        Boston Patriots, Baltimore Colts, original 26-team setup.
#   1971-1975   Boston -> New England Patriots rename.
#   1976        TB joins AFC West, SEA joins NFC West (one-year swap).
#   1977-1981   SEA permanently AFC West, TB permanently NFC Central.
#   1982-1983   LA Raiders move (OAK -> RAI).
#   1984-1987   Colts move BAL -> IND.
#   1988-1993   Cardinals move STL -> PHO.
#   1994        Cardinals rename PHO -> ARI.
#   1995        Carolina + Jacksonville expansion, Rams LA -> STL,
#               Raiders LA -> OAK.
#   1996        CLE Browns suspended, Ravens new in BAL.
#   1997-1998   Oilers HOU -> OTI (Tennessee).
#   1999-2001   CLE Browns return, Tennessee Titans (TEN).
#   2002-2015   8-division realignment + Houston Texans expansion;
#               Seattle moves to NFC West.
#   2016        Rams move STL -> LAR.
#   2017-2019   Chargers move SD -> LAC.
#   2020+       Raiders move OAK -> LV.

# Stable ``(team_code, franchise)`` constants for the modern era. Used in
# multiple eras unchanged.
_BUF = ("BUF", "bills")
_MIA = ("MIA", "dolphins")
_NWE_M = ("NE", "patriots")        # nflverse 2-letter
_NYJ = ("NYJ", "jets")
_BAL_RAVENS = ("BAL", "ravens")
_CIN = ("CIN", "bengals")
_CLE = ("CLE", "browns")
_PIT = ("PIT", "steelers")
_HOU_TEXANS = ("HOU", "texans")
_IND = ("IND", "colts")
_JAX = ("JAX", "jaguars")
_TEN = ("TEN", "titans")
_DEN = ("DEN", "broncos")
_KC_M = ("KC", "chiefs")           # nflverse 2-letter
_DAL = ("DAL", "cowboys")
_NYG = ("NYG", "giants")
_PHI = ("PHI", "eagles")
_WAS = ("WAS", "commanders")
_CHI = ("CHI", "bears")
_DET = ("DET", "lions")
_GB_M = ("GB", "packers")          # nflverse 2-letter
_MIN = ("MIN", "vikings")
_ATL = ("ATL", "falcons")
_CAR = ("CAR", "panthers")
_NO_M = ("NO", "saints")           # nflverse 2-letter
_TB_M = ("TB", "buccaneers")       # nflverse 2-letter
_SEA = ("SEA", "seahawks")
_SF_M = ("SF", "49ers")            # nflverse 2-letter
_ARI = ("ARI", "cardinals")

# Pre-1999 PFR display codes that differ from nflverse's. PFR uses
# 3-letter codes (NWE, KAN, GNB, NOR, SFO, TAM, SDG) where nflverse
# uses 2-letter (NE, KC, GB, NO, SF, TB, SD).
_NWE_P = ("NWE", "patriots")
_KAN = ("KAN", "chiefs")
_GNB = ("GNB", "packers")
_NOR = ("NOR", "saints")
_SFO = ("SFO", "49ers")
_TAM = ("TAM", "buccaneers")
_SDG = ("SDG", "chargers")

# Time-ambiguous codes — same letters, different franchise per era.
_BAL_COLTS = ("BAL", "colts")            # 1970-1983
_BOS_PATRIOTS = ("BOS", "patriots")      # 1970 only
_HOU_OILERS = ("HOU", "titans")          # 1970-1996, same franchise as TEN
_OTI = ("OTI", "titans")                 # 1997-1998 Tennessee Oilers
_PHO = ("PHO", "cardinals")              # 1988-1993
_STL_CARDS = ("STL", "cardinals")        # 1970-1987
_STL_RAMS = ("STL", "rams")              # 1995-2015
_RAM = ("RAM", "rams")                   # 1970-1981 LA Rams (PFR display)
_RAI = ("RAI", "raiders")                # 1982-1994 LA Raiders (PFR display)
_OAK = ("OAK", "raiders")                # 1970-1981 + 1995-2019
_LV = ("LV", "raiders")
_LAR = ("LAR", "rams")
_LAC = ("LAC", "chargers")
_SD_M = ("SD", "chargers")               # 1999-2016 (nflverse)


_ERAS: list[tuple[int, int, dict[tuple[str, str], list[tuple[str, str]]]]] = [
    # 1970 only — 26 teams, BOS Patriots (renamed 1971), no TB/SEA.
    (1970, 1970, {
        ("AFC", "AFC East"):    [_BAL_COLTS, _BOS_PATRIOTS, _BUF, _MIA, _NYJ],
        ("AFC", "AFC Central"): [_CIN, _CLE, _HOU_OILERS, _PIT],
        ("AFC", "AFC West"):    [_DEN, _KAN, _OAK, _SDG],
        ("NFC", "NFC East"):    [_DAL, _NYG, _PHI, _STL_CARDS, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GNB, _MIN],
        ("NFC", "NFC West"):    [_ATL, _NOR, _RAM, _SFO],
    }),
    # 1971-1975 — Patriots renamed BOS->NWE.
    (1971, 1975, {
        ("AFC", "AFC East"):    [_BAL_COLTS, _BUF, _MIA, _NWE_P, _NYJ],
        ("AFC", "AFC Central"): [_CIN, _CLE, _HOU_OILERS, _PIT],
        ("AFC", "AFC West"):    [_DEN, _KAN, _OAK, _SDG],
        ("NFC", "NFC East"):    [_DAL, _NYG, _PHI, _STL_CARDS, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GNB, _MIN],
        ("NFC", "NFC West"):    [_ATL, _NOR, _RAM, _SFO],
    }),
    # 1976 only — TB joins AFC West, SEA joins NFC West (rotates next year).
    (1976, 1976, {
        ("AFC", "AFC East"):    [_BAL_COLTS, _BUF, _MIA, _NWE_P, _NYJ],
        ("AFC", "AFC Central"): [_CIN, _CLE, _HOU_OILERS, _PIT],
        ("AFC", "AFC West"):    [_DEN, _KAN, _OAK, _SDG, _TAM],
        ("NFC", "NFC East"):    [_DAL, _NYG, _PHI, _STL_CARDS, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GNB, _MIN],
        ("NFC", "NFC West"):    [_ATL, _NOR, _RAM, _SEA, _SFO],
    }),
    # 1977-1981 — SEA AFC West, TB NFC Central, OAK Raiders still in OAK.
    (1977, 1981, {
        ("AFC", "AFC East"):    [_BAL_COLTS, _BUF, _MIA, _NWE_P, _NYJ],
        ("AFC", "AFC Central"): [_CIN, _CLE, _HOU_OILERS, _PIT],
        ("AFC", "AFC West"):    [_DEN, _KAN, _OAK, _SDG, _SEA],
        ("NFC", "NFC East"):    [_DAL, _NYG, _PHI, _STL_CARDS, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GNB, _MIN, _TAM],
        ("NFC", "NFC West"):    [_ATL, _NOR, _RAM, _SFO],
    }),
    # 1982-1983 — Raiders move to LA (RAI). Colts still in BAL.
    (1982, 1983, {
        ("AFC", "AFC East"):    [_BAL_COLTS, _BUF, _MIA, _NWE_P, _NYJ],
        ("AFC", "AFC Central"): [_CIN, _CLE, _HOU_OILERS, _PIT],
        ("AFC", "AFC West"):    [_DEN, _KAN, _RAI, _SDG, _SEA],
        ("NFC", "NFC East"):    [_DAL, _NYG, _PHI, _STL_CARDS, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GNB, _MIN, _TAM],
        ("NFC", "NFC West"):    [_ATL, _NOR, _RAM, _SFO],
    }),
    # 1984-1987 — Colts move BAL->IND. Cardinals still in STL.
    (1984, 1987, {
        ("AFC", "AFC East"):    [_BUF, _IND, _MIA, _NWE_P, _NYJ],
        ("AFC", "AFC Central"): [_CIN, _CLE, _HOU_OILERS, _PIT],
        ("AFC", "AFC West"):    [_DEN, _KAN, _RAI, _SDG, _SEA],
        ("NFC", "NFC East"):    [_DAL, _NYG, _PHI, _STL_CARDS, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GNB, _MIN, _TAM],
        ("NFC", "NFC West"):    [_ATL, _NOR, _RAM, _SFO],
    }),
    # 1988-1993 — Cardinals move STL->PHO.
    (1988, 1993, {
        ("AFC", "AFC East"):    [_BUF, _IND, _MIA, _NWE_P, _NYJ],
        ("AFC", "AFC Central"): [_CIN, _CLE, _HOU_OILERS, _PIT],
        ("AFC", "AFC West"):    [_DEN, _KAN, _RAI, _SDG, _SEA],
        ("NFC", "NFC East"):    [_DAL, _NYG, _PHI, _PHO, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GNB, _MIN, _TAM],
        ("NFC", "NFC West"):    [_ATL, _NOR, _RAM, _SFO],
    }),
    # 1994 — Cardinals rename PHO->ARI. Last year of LA Rams + LA Raiders.
    (1994, 1994, {
        ("AFC", "AFC East"):    [_BUF, _IND, _MIA, _NWE_P, _NYJ],
        ("AFC", "AFC Central"): [_CIN, _CLE, _HOU_OILERS, _PIT],
        ("AFC", "AFC West"):    [_DEN, _KAN, _RAI, _SDG, _SEA],
        ("NFC", "NFC East"):    [_ARI, _DAL, _NYG, _PHI, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GNB, _MIN, _TAM],
        ("NFC", "NFC West"):    [_ATL, _NOR, _RAM, _SFO],
    }),
    # 1995 — CAR + JAX expansion. Rams LA->STL. Raiders LA->OAK. CLE still
    # in Cleveland (last year before suspension).
    (1995, 1995, {
        ("AFC", "AFC East"):    [_BUF, _IND, _MIA, _NWE_P, _NYJ],
        ("AFC", "AFC Central"): [_CIN, _CLE, _HOU_OILERS, ("JAX", "jaguars"), _PIT],
        ("AFC", "AFC West"):    [_DEN, _KAN, _OAK, _SDG, _SEA],
        ("NFC", "NFC East"):    [_ARI, _DAL, _NYG, _PHI, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GNB, _MIN, _TAM],
        ("NFC", "NFC West"):    [_ATL, ("CAR", "panthers"), _NOR, _SFO, _STL_RAMS],
    }),
    # 1996 — CLE Browns suspended, Ravens new in BAL.
    (1996, 1996, {
        ("AFC", "AFC East"):    [_BUF, _IND, _MIA, _NWE_P, _NYJ],
        ("AFC", "AFC Central"): [_BAL_RAVENS, _CIN, _HOU_OILERS, ("JAX", "jaguars"), _PIT],
        ("AFC", "AFC West"):    [_DEN, _KAN, _OAK, _SDG, _SEA],
        ("NFC", "NFC East"):    [_ARI, _DAL, _NYG, _PHI, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GNB, _MIN, _TAM],
        ("NFC", "NFC West"):    [_ATL, ("CAR", "panthers"), _NOR, _SFO, _STL_RAMS],
    }),
    # 1997-1998 — Oilers HOU->OTI (Tennessee Oilers).
    (1997, 1998, {
        ("AFC", "AFC East"):    [_BUF, _IND, _MIA, _NWE_P, _NYJ],
        ("AFC", "AFC Central"): [_BAL_RAVENS, _CIN, ("JAX", "jaguars"), _OTI, _PIT],
        ("AFC", "AFC West"):    [_DEN, _KAN, _OAK, _SDG, _SEA],
        ("NFC", "NFC East"):    [_ARI, _DAL, _NYG, _PHI, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GNB, _MIN, _TAM],
        ("NFC", "NFC West"):    [_ATL, ("CAR", "panthers"), _NOR, _SFO, _STL_RAMS],
    }),
    # 1999-2001 — CLE Browns return, Tennessee Titans (TEN). Switches to
    # nflverse-style 2-letter codes (KC, GB, NO, SF, TB, NE) since
    # nflverse is the data source from here on.
    (1999, 2001, {
        ("AFC", "AFC East"):    [_BUF, _IND, _MIA, _NWE_M, _NYJ],
        ("AFC", "AFC Central"): [_BAL_RAVENS, _CIN, _CLE, _JAX, _PIT, _TEN],
        ("AFC", "AFC West"):    [_DEN, _KC_M, _OAK, _SD_M, _SEA],
        ("NFC", "NFC East"):    [_ARI, _DAL, _NYG, _PHI, _WAS],
        ("NFC", "NFC Central"): [_CHI, _DET, _GB_M, _MIN, _TB_M],
        ("NFC", "NFC West"):    [_ATL, _CAR, _NO_M, _SF_M, _STL_RAMS],
    }),
    # 2002-2015 — 8 divisions, HOU Texans expansion, SEA -> NFC West.
    (2002, 2015, {
        ("AFC", "AFC East"):  [_BUF, _MIA, _NWE_M, _NYJ],
        ("AFC", "AFC North"): [_BAL_RAVENS, _CIN, _CLE, _PIT],
        ("AFC", "AFC South"): [_HOU_TEXANS, _IND, _JAX, _TEN],
        ("AFC", "AFC West"):  [_DEN, _KC_M, _OAK, _SD_M],
        ("NFC", "NFC East"):  [_DAL, _NYG, _PHI, _WAS],
        ("NFC", "NFC North"): [_CHI, _DET, _GB_M, _MIN],
        ("NFC", "NFC South"): [_ATL, _CAR, _NO_M, _TB_M],
        ("NFC", "NFC West"):  [_ARI, _SEA, _SF_M, _STL_RAMS],
    }),
    # 2016 — Rams STL -> LAR.
    (2016, 2016, {
        ("AFC", "AFC East"):  [_BUF, _MIA, _NWE_M, _NYJ],
        ("AFC", "AFC North"): [_BAL_RAVENS, _CIN, _CLE, _PIT],
        ("AFC", "AFC South"): [_HOU_TEXANS, _IND, _JAX, _TEN],
        ("AFC", "AFC West"):  [_DEN, _KC_M, _OAK, _SD_M],
        ("NFC", "NFC East"):  [_DAL, _NYG, _PHI, _WAS],
        ("NFC", "NFC North"): [_CHI, _DET, _GB_M, _MIN],
        ("NFC", "NFC South"): [_ATL, _CAR, _NO_M, _TB_M],
        ("NFC", "NFC West"):  [_ARI, _LAR, _SEA, _SF_M],
    }),
    # 2017-2019 — Chargers SD -> LAC.
    (2017, 2019, {
        ("AFC", "AFC East"):  [_BUF, _MIA, _NWE_M, _NYJ],
        ("AFC", "AFC North"): [_BAL_RAVENS, _CIN, _CLE, _PIT],
        ("AFC", "AFC South"): [_HOU_TEXANS, _IND, _JAX, _TEN],
        ("AFC", "AFC West"):  [_DEN, _KC_M, _LAC, _OAK],
        ("NFC", "NFC East"):  [_DAL, _NYG, _PHI, _WAS],
        ("NFC", "NFC North"): [_CHI, _DET, _GB_M, _MIN],
        ("NFC", "NFC South"): [_ATL, _CAR, _NO_M, _TB_M],
        ("NFC", "NFC West"):  [_ARI, _LAR, _SEA, _SF_M],
    }),
    # 2020+ — Raiders OAK -> LV.
    (2020, 2099, {
        ("AFC", "AFC East"):  [_BUF, _MIA, _NWE_M, _NYJ],
        ("AFC", "AFC North"): [_BAL_RAVENS, _CIN, _CLE, _PIT],
        ("AFC", "AFC South"): [_HOU_TEXANS, _IND, _JAX, _TEN],
        ("AFC", "AFC West"):  [_DEN, _KC_M, _LAC, _LV],
        ("NFC", "NFC East"):  [_DAL, _NYG, _PHI, _WAS],
        ("NFC", "NFC North"): [_CHI, _DET, _GB_M, _MIN],
        ("NFC", "NFC South"): [_ATL, _CAR, _NO_M, _TB_M],
        ("NFC", "NFC West"):  [_ARI, _LAR, _SEA, _SF_M],
    }),
]

# Earliest year covered by the era table.
TEAM_SEASONS_MIN_YEAR = 1970


def _divisions_for_season(
    season: int,
) -> dict[tuple[str, str], list[tuple[str, str]]]:
    for start, end, divisions in _ERAS:
        if start <= season <= end:
            return divisions
    raise ValueError(
        f"no era defined for season {season}; team_seasons covers "
        f"{TEAM_SEASONS_MIN_YEAR}+"
    )


def build_team_seasons(seasons: Iterable[int]) -> pl.DataFrame:
    """Return one row per (team, season) with conference/division/franchise.

    Pure function — no I/O. Each era directly carries the franchise key
    alongside the team code, so codes that mean different franchises in
    different eras (STL = Cardinals 1970-1987 / Rams 1995-2015, BAL =
    Colts 1970-1983 / Ravens 1996+, HOU = Oilers 1970-1996 / Texans
    2002+) resolve correctly via the era's start/end window.
    """
    rows: list[dict] = []
    for season in seasons:
        for (conf, div), team_specs in _divisions_for_season(season).items():
            for team, franchise in team_specs:
                rows.append(
                    {
                        "team": team,
                        "season": season,
                        "conference": conf,
                        "division": div,
                        "franchise": franchise,
                    }
                )
    return pl.DataFrame(
        rows,
        schema={
            "team": pl.Utf8,
            "season": pl.Int64,
            "conference": pl.Utf8,
            "division": pl.Utf8,
            "franchise": pl.Utf8,
        },
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
