"""Named SQL helpers for common questions.

Each helper returns a ``(sql, params)`` tuple ready for use with
``con.execute(sql, params)``. The CLI and ad-hoc callers can pick a
helper rather than write the SQL each time.

**Project default unit of analysis: the player-season.** Every helper
here ranks player-seasons (one row per (player, season, team)) — *not*
career totals or team aggregates. The same player appears multiple
times if multiple of their seasons qualify. Career and team-aggregate
rollups will be added as separate, explicitly-named helpers if and
when needed; they are never the default.
"""

from __future__ import annotations

from typing import Literal

ScoringMode = Literal["std", "half", "ppr"]
DivisionMode = Literal["historical", "franchise"]

_FPTS_COLUMN_FOR_SCORING: dict[str, str] = {
    "std":  "fpts_std",
    "half": "fpts_half",
    "ppr":  "fpts_ppr",
}


def flex_topN_by_draft_round(
    round_: int,
    n: int = 10,
    scoring: ScoringMode = "ppr",
) -> tuple[str, list]:
    """Top-N FLEX (RB/WR/TE) player-seasons drafted in a given round.

    Returns rows of (name, team, season, fpts, draft_round, draft_year,
    draft_overall_pick) ordered by fpts descending. The fpts column
    used corresponds to the ``scoring`` mode.
    """
    if scoring not in _FPTS_COLUMN_FOR_SCORING:
        raise ValueError(
            f"unknown scoring mode {scoring!r}; expected one of "
            f"{list(_FPTS_COLUMN_FOR_SCORING)}"
        )
    fpts_col = _FPTS_COLUMN_FOR_SCORING[scoring]
    sql = f"""
        SELECT name,
               team,
               season,
               {fpts_col} AS fpts,
               draft_round,
               draft_year,
               draft_overall_pick
        FROM   v_flex_seasons
        WHERE  draft_round = ?
          AND  {fpts_col} IS NOT NULL
        ORDER BY {fpts_col} DESC
        LIMIT  ?
    """
    return sql, [round_, n]


def most_def_int_by_division(
    division: str,
    *,
    start: int,
    end: int,
    n: int = 25,
    division_mode: DivisionMode = "historical",
) -> tuple[str, list]:
    """Top-N player-seasons by defensive interceptions, scoped to a division.

    Two interpretations of "division", picked by ``division_mode``:

    - ``"historical"``: filter by the per-season division as it existed
      that year. "NFC North" 1999-2001 returns no rows because the NFC
      North didn't exist before 2002 — those seasons were "NFC Central".
      Use this when you want strict period accuracy.

    - ``"franchise"``: filter by the *current* franchises that make up
      that division today. "NFC North" with this mode always means
      CHI / DET / GB / MIN, regardless of what the division was called
      that year. The query resolves the franchise set by selecting all
      franchises that ever lived in the named division.

    Returns rows of (name, team, season, def_int, conference, division,
    franchise) ordered by def_int desc. Same player can appear multiple
    times for different qualifying seasons (player-season default).
    """
    if division_mode == "historical":
        where = "v.division = ?"
        params: list = [division, start, end, n]
    elif division_mode == "franchise":
        where = """v.franchise IN (
            SELECT DISTINCT franchise
            FROM   v_player_season_full
            WHERE  division = ?
        )"""
        params = [division, start, end, n]
    else:
        raise ValueError(
            f"unknown division_mode {division_mode!r}; "
            f"expected 'historical' or 'franchise'"
        )

    sql = f"""
        SELECT v.name,
               v.team,
               v.season,
               v.def_int,
               v.conference,
               v.division,
               v.franchise
        FROM   v_player_season_full v
        WHERE  {where}
          AND  v.season BETWEEN ? AND ?
          AND  v.def_int IS NOT NULL
        ORDER BY v.def_int DESC, v.season ASC
        LIMIT  ?
    """
    return sql, params


# Position-specific top-N: pick a position (or "FLEX" / "ALL"), pick a
# stat to rank by, optionally filter by year range and/or draft round(s).
# rank_by is interpolated into SQL, so it MUST be validated against this
# allowlist of ranking-eligible columns from player_season_stats.
RANK_BY_ALLOWED: frozenset[str] = frozenset({
    "games", "games_started",
    # passing
    "pass_cmp", "pass_att", "pass_yds", "pass_td", "pass_int",
    "pass_sacks_taken", "pass_sack_yds", "pass_long", "pass_rating",
    # rushing
    "rush_att", "rush_yds", "rush_td", "rush_long",
    # receiving
    "targets", "rec", "rec_yds", "rec_td", "rec_long",
    # defense
    "def_tackles_solo", "def_tackles_assist", "def_tackles_combined",
    "def_sacks", "def_int", "def_int_yds", "def_int_td",
    "def_pass_def", "def_fumbles_forced", "def_fumbles_rec",
    "def_fumbles_rec_yds", "def_fumbles_rec_td", "def_safeties",
    # kicking / punting
    "fg_made", "fg_att", "fg_long", "xp_made", "xp_att",
    "punts", "punt_yds", "punt_long",
    # returns
    "kr", "kr_yds", "kr_td", "pr", "pr_yds", "pr_td",
    # fumbles + 2pt
    "fumbles", "fumbles_lost",
    "two_pt_pass", "two_pt_rush", "two_pt_rec",
    # fantasy
    "fpts_std", "fpts_half", "fpts_ppr",
})

# Position aliases: caller-friendly names that expand to a set.
# "ALL" means "no position filter" (handled specially below).
POSITION_ALIASES: dict[str, list[str] | None] = {
    "FLEX": ["RB", "WR", "TE"],
    "ALL":  None,
}


def pos_topN(
    position: str,
    *,
    n: int = 10,
    rank_by: str = "fpts_ppr",
    start: int | None = None,
    end: int | None = None,
    draft_rounds: list[int] | None = None,
) -> tuple[str, list]:
    """Top-N player-seasons at a given position, ranked by ``rank_by``.

    ``position``: a single position label ("QB", "RB", "WR", "TE",
    "CB", "LB", ...) or one of the aliases ``FLEX`` (RB+WR+TE) or
    ``ALL`` (no position filter). Case-insensitive.

    ``rank_by``: the column to rank on. Validated against an allowlist
    of ranking-eligible numeric columns from ``player_season_stats``;
    unknown columns raise ``ValueError`` to keep SQL injection off the
    table.

    Optional ``start`` / ``end`` filter to a year range (inclusive on
    both ends). Optional ``draft_rounds`` filters to player-seasons
    whose draft pick was in any of the given rounds.

    Returns rows of (name, team, season, position, rank_value,
    draft_round, draft_year, draft_overall_pick) ordered by rank_by
    desc then season asc. Player-season default — same player can
    appear multiple times for different qualifying years.
    """
    if rank_by not in RANK_BY_ALLOWED:
        raise ValueError(
            f"unknown rank-by column {rank_by!r}; allowed: "
            f"{sorted(RANK_BY_ALLOWED)}"
        )
    pos_upper = position.upper()
    if pos_upper in POSITION_ALIASES:
        positions = POSITION_ALIASES[pos_upper]
    else:
        positions = [pos_upper]

    where_clauses: list[str] = [f"{rank_by} IS NOT NULL"]
    params: list = []

    if positions is not None:
        placeholders = ",".join(["?"] * len(positions))
        where_clauses.append(f"position IN ({placeholders})")
        params.extend(positions)

    if start is not None:
        where_clauses.append("season >= ?")
        params.append(start)
    if end is not None:
        where_clauses.append("season <= ?")
        params.append(end)

    if draft_rounds:
        placeholders = ",".join(["?"] * len(draft_rounds))
        where_clauses.append(f"draft_round IN ({placeholders})")
        params.extend(draft_rounds)

    where_sql = " AND ".join(where_clauses)
    sql = f"""
        SELECT name,
               team,
               season,
               position,
               {rank_by} AS rank_value,
               draft_round,
               draft_year,
               draft_overall_pick
        FROM   v_player_season_full
        WHERE  {where_sql}
        ORDER BY {rank_by} DESC, season ASC
        LIMIT  ?
    """
    params.append(n)
    return sql, params
