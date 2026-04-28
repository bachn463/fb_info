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


# Position-specific top-N: pick a position (or "FLEX" / "ALL"), pick a
# stat to rank by, optionally filter by year range and/or draft round(s).
# rank_by is interpolated into SQL, so it MUST be validated against this
# allowlist of ranking-eligible columns. All numeric so sorting DESC is
# meaningful.
RANK_BY_ALLOWED: frozenset[str] = frozenset({
    "games", "games_started", "age",
    # passing — pass_cmp_pct is computed in v_player_season_full as
    # pass_cmp / NULLIF(pass_att, 0); rankable but NOT summable for
    # career totals (see _CAREER_RATIO_RANK_BY).
    "pass_cmp", "pass_att", "pass_yds", "pass_td", "pass_int",
    "pass_sacks_taken", "pass_sack_yds", "pass_long", "pass_rating",
    "pass_cmp_pct",
    # rushing
    "rush_att", "rush_yds", "rush_td", "rush_long",
    # receiving — catch_rate is computed in v_player_season_full as
    # rec / NULLIF(targets, 0); rankable like any other column.
    "targets", "rec", "rec_yds", "rec_td", "rec_long",
    "catch_rate",
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
    # Draft columns — drafted players only (rank_by adds an `IS NOT
    # NULL` clause that excludes undrafted).
    "draft_year", "draft_round", "draft_overall_pick",
})

# Position aliases: caller-friendly names that expand to a set.
# "ALL" means "no position filter" (handled specially below).
# Defensive groupings (SAFETY / DB / LB / DL) cover PFR's fine-grained
# variants — PFR records safeties as 'S', 'SS', or 'FS' across eras,
# corners as 'CB', linebackers as 'LB' / 'OLB' / 'MLB' / 'ILB' /
# 'RLB' / 'LLB', and DL as 'DE' / 'DT' / 'NT' (incl. side-suffixed
# forms). The aliases group them so a single --position SAFETY filter
# catches every flavor.
POSITION_ALIASES: dict[str, list[str] | None] = {
    "FLEX":   ["RB", "WR", "TE"],
    "ALL":    None,
    "SAFETY": ["S", "SS", "FS"],
    "DB":     ["CB", "S", "SS", "FS", "DB", "RCB", "LCB"],
    "LB":     ["LB", "OLB", "MLB", "ILB", "RLB", "LLB"],
    "DL":     ["DE", "DT", "NT", "LDE", "RDE", "LDT", "RDT"],
}

# Award types accepted by the has_award filter. Validated up-front so
# unknown labels raise a clear ValueError instead of silently matching
# nothing.
AWARD_TYPES_ALLOWED: frozenset[str] = frozenset({
    "MVP", "OPOY", "DPOY", "OROY", "DROY", "CPOY",
    "WPMOY",
    "PB", "AP_FIRST", "AP_SECOND",
    # HOF — Hall of Fame induction. Stored as one row per inductee
    # with season = the player's last NFL season. Treated as a binary
    # award (vote_finish IS NULL) so --has-award HOF matches every
    # HOFer's final season, --ever-won HOF matches every season of a
    # HOFer's career, and `ask career --award HOF` lists HOFers (all
    # tied at award_count=1). Sources: auto-detected from PFR's "HOF"
    # name suffix on draft pages + curated KNOWN_HOFERS list for
    # UDFAs / pre-1970 inductees.
    "HOF",
})

# Columns allowed as ORDER BY tiebreakers. Superset of RANK_BY_ALLOWED
# plus draft and identity columns that make sense for stable secondary
# sorts. All applied ASC; if you want DESC, use the column as the
# primary rank_by instead.
TIEBREAK_BY_ALLOWED: frozenset[str] = RANK_BY_ALLOWED | frozenset({
    "draft_year", "draft_round", "draft_overall_pick",
    "position", "season", "age", "name", "team",
})

# Subset of RANK_BY_ALLOWED that's meaningful for trivia. Trivia asks
# "guess the player who led the league in X" — for that frame, ranking
# by `age` (oldest player to record a stat) or by draft metadata
# (`draft_year` / `draft_round` / `draft_overall_pick`) makes a poor
# question. Trivia paths (CLI `trivia play` / `trivia random` and the
# web trivia forms) restrict to this set; the general `ask pos-top`
# helper still accepts the full RANK_BY_ALLOWED.
TRIVIA_RANK_BY_ALLOWED: frozenset[str] = RANK_BY_ALLOWED - frozenset({
    "age", "draft_year", "draft_round", "draft_overall_pick",
})


def pos_topN(
    position: str,
    *,
    n: int = 10,
    rank_by: str = "fpts_ppr",
    start: int | None = None,
    end: int | None = None,
    draft_rounds: list[int | str] | None = None,
    team: str | None = None,
    division: str | None = None,
    conference: str | None = None,
    first_name_contains: str | None = None,
    last_name_contains: str | None = None,
    unique: bool = False,
    # Backward-compat additive filters — each defaults to a no-op.
    has_award: list[str] | None = None,
    rookie_only: bool = False,
    min_stats: dict[str, float] | None = None,
    max_stats: dict[str, float] | None = None,
    draft_start: int | None = None,
    draft_end: int | None = None,
    ever_won_award: list[str] | None = None,
    drafted_by: str | None = None,
    tiebreak_by: list[str] | None = None,
    college: str | None = None,
    min_career_stats: dict[str, float] | None = None,
    max_career_stats: dict[str, float] | None = None,
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
    whose draft pick was in any of the given rounds; the special token
    ``"undrafted"`` (case-insensitive) matches players with no draft
    entry (draft_round IS NULL). Mixing rounds and ``"undrafted"``
    composes — e.g. ``[4, 5, "undrafted"]`` means "round 4 OR round 5
    OR undrafted".

    Scope filters (all optional, all combinable):
    - ``team``: filter to a single team code, e.g. "SF", "DAL".
      Compared as uppercase against the historical team code on the
      player-season row.
    - ``division``: exact match against the per-season division name
      ("NFC North", "AFC West", "NFC Central" pre-2002, etc.).
    - ``conference``: "AFC" or "NFC".
    - ``first_name_contains`` / ``last_name_contains``: case-insensitive
      substring match on the first / last name (split on the first
      whitespace in the player's display name). Both can be combined
      with each other and with everything else.

    ``unique``: when True, collapse to one row per player — their best
    season as ranked by ``rank_by`` (within the active filters). Ties
    on the rank value resolve to the earlier season. The default
    (False) preserves the player-season behavior.

    Award & rookie filters (additive, default no-op):
    - ``has_award``: list of award_type labels. Filters to player-
      seasons where the player won (or was binary-awarded) at least
      one of the listed awards that year. "Won" means
      ``vote_finish=1`` for voted awards (MVP/OPOY/...) or any entry
      for binary ones (PB/AP_FIRST/AP_SECOND/WPMOY). Validated against
      ``AWARD_TYPES_ALLOWED``; unknown labels raise ``ValueError``.
    - ``rookie_only``: when True, restricts to each player's first
      season as recorded in our DB. Computed as the row's ``season``
      equalling ``MIN(season) FROM player_season_stats`` for that
      player. Caveat: "first season we have data for", which is the
      rookie year for almost everyone in scope.
    - ``min_stats`` / ``max_stats``: dicts of ``{stat_column: threshold}``.
      Adds ``col >= threshold`` / ``col <= threshold`` clauses. Column
      names are validated against ``RANK_BY_ALLOWED`` (same allowlist
      as ``rank_by``) so they're safe to interpolate. Useful for
      "top X with at least N rec_yds" or "high-volume RBs with low
      rush_yds" style queries.
    - ``draft_start`` / ``draft_end``: filter to player-seasons where
      the player's draft year falls in the given inclusive range.
      Either bound can be omitted. Excludes undrafted players (their
      draft_year is NULL).
    - ``ever_won_award``: list of award_type labels. Filters to player-
      seasons of players who **at any point in their career** won
      one of the listed awards — independent of which season we're
      ranking. Composes with ``has_award`` (year-of-win filter).
      Same allowlist + win-only semantics as ``has_award``.
    - ``drafted_by``: filter to player-seasons where the player was
      drafted by this team. Compared as uppercase against
      ``draft_team`` on the joined draft_picks row. Excludes
      undrafted players.
    - ``tiebreak_by``: list of column names used as secondary ASC
      sort criteria when multiple rows share the primary ``rank_by``
      value. Validated against ``TIEBREAK_BY_ALLOWED``. Default tie
      resolution is just ``season ASC`` (deterministic, earliest
      year first); ``tiebreak_by`` inserts your columns *before*
      that fallback. Useful for "rank by fpts_ppr, break ties by
      draft_year then position".

    Returns rows of (name, team, season, position, rank_value,
    draft_round, draft_year, draft_overall_pick) — column set is
    fixed and unchanged regardless of which optional filters are
    enabled. Player-season default — same player can appear multiple
    times for different qualifying years (unless ``unique=True``).
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
        int_rounds: list[int] = []
        include_undrafted = False
        for entry in draft_rounds:
            if isinstance(entry, str) and entry.lower() == "undrafted":
                include_undrafted = True
            elif isinstance(entry, int) and not isinstance(entry, bool):
                int_rounds.append(entry)
            else:
                raise ValueError(
                    f"draft_rounds entries must be ints or 'undrafted', got: {entry!r}"
                )
        clauses: list[str] = []
        if int_rounds:
            placeholders = ",".join(["?"] * len(int_rounds))
            clauses.append(f"draft_round IN ({placeholders})")
            params.extend(int_rounds)
        if include_undrafted:
            clauses.append("draft_round IS NULL")
        if clauses:
            where_clauses.append("(" + " OR ".join(clauses) + ")")

    if team:
        where_clauses.append("team = ?")
        params.append(team.upper())
    if division:
        where_clauses.append("division = ?")
        params.append(division)
    if conference:
        where_clauses.append("conference = ?")
        params.append(conference.upper())

    # Name greps: split the display name on the first whitespace.
    # First name = split_part(name, ' ', 1); last name = the rest,
    # which collapses suffixes like "Jr." into the last-name match.
    if first_name_contains:
        where_clauses.append("split_part(name, ' ', 1) ILIKE ?")
        params.append(f"%{first_name_contains}%")
    if last_name_contains:
        where_clauses.append(
            "trim(substr(name, length(split_part(name, ' ', 1)) + 1)) ILIKE ?"
        )
        params.append(f"%{last_name_contains}%")

    # Awards filter: EXISTS subquery against player_awards. award_type
    # is interpolated against the validated allowlist so it's safe.
    if has_award:
        normalized: list[str] = []
        for a in has_award:
            label = str(a).upper()
            if label not in AWARD_TYPES_ALLOWED:
                raise ValueError(
                    f"unknown award_type {a!r}; allowed: "
                    f"{sorted(AWARD_TYPES_ALLOWED)}"
                )
            normalized.append(label)
        placeholders = ",".join(["?"] * len(normalized))
        where_clauses.append(
            f"EXISTS ("
            f"SELECT 1 FROM player_awards pa "
            f"WHERE  pa.player_id = v_player_season_full.player_id "
            f"  AND  pa.season    = v_player_season_full.season "
            f"  AND  pa.award_type IN ({placeholders}) "
            f"  AND  (pa.vote_finish IS NULL OR pa.vote_finish = 1)"
            f")"
        )
        params.extend(normalized)

    # Rookie-only: season equals first season we have stats for.
    if rookie_only:
        where_clauses.append(
            "season = (SELECT MIN(season) FROM player_season_stats "
            "WHERE player_id = v_player_season_full.player_id)"
        )

    # Draft-year range: drafted players only (drops undrafted).
    if draft_start is not None:
        where_clauses.append("draft_year >= ?")
        params.append(draft_start)
    if draft_end is not None:
        where_clauses.append("draft_year <= ?")
        params.append(draft_end)

    # Drafted-by-team filter — looks at the team that drafted the
    # player (draft_team), not the team they played for.
    if drafted_by:
        where_clauses.append("draft_team = ?")
        params.append(drafted_by.upper())

    # Career-award filter: player won this award at any season.
    # Same EXISTS pattern as has_award but without the season match.
    if ever_won_award:
        normalized: list[str] = []
        for a in ever_won_award:
            label = str(a).upper()
            if label not in AWARD_TYPES_ALLOWED:
                raise ValueError(
                    f"unknown award_type {a!r}; allowed: "
                    f"{sorted(AWARD_TYPES_ALLOWED)}"
                )
            normalized.append(label)
        placeholders = ",".join(["?"] * len(normalized))
        where_clauses.append(
            f"EXISTS ("
            f"SELECT 1 FROM player_awards pa "
            f"WHERE  pa.player_id = v_player_season_full.player_id "
            f"  AND  pa.award_type IN ({placeholders}) "
            f"  AND  (pa.vote_finish IS NULL OR pa.vote_finish = 1)"
            f")"
        )
        params.extend(normalized)

    # Min/max stat thresholds. Column names validated against the
    # same allowlist as rank_by (no SQL injection).
    if min_stats:
        for col, val in min_stats.items():
            if col not in RANK_BY_ALLOWED:
                raise ValueError(
                    f"unknown min_stats column {col!r}; allowed: "
                    f"{sorted(RANK_BY_ALLOWED)}"
                )
            where_clauses.append(f"{col} >= ?")
            params.append(val)
    if max_stats:
        for col, val in max_stats.items():
            if col not in RANK_BY_ALLOWED:
                raise ValueError(
                    f"unknown max_stats column {col!r}; allowed: "
                    f"{sorted(RANK_BY_ALLOWED)}"
                )
            where_clauses.append(f"{col} <= ?")
            params.append(val)

    # College filter — substring match (ILIKE) against draft_picks.college
    # via the v_player_season_full join. Players with no draft row have
    # college IS NULL and are excluded by ILIKE.
    if college:
        where_clauses.append("college ILIKE ?")
        params.append(f"%{college}%")

    # Career-total min/max thresholds: HAVING-style filter implemented
    # as a player_id IN (...) subquery on player_season_stats. Ratio
    # stats (pass_cmp_pct, catch_rate) recompute as SUM(num) /
    # NULLIF(SUM(den), 0) so divide-by-zero is impossible.
    career_min_max_clauses, career_params = _career_min_max_subquery_clauses(
        min_career_stats, max_career_stats
    )
    if career_min_max_clauses:
        where_clauses.append(
            "player_id IN (SELECT player_id FROM player_season_stats "
            "GROUP BY player_id HAVING " + " AND ".join(career_min_max_clauses) + ")"
        )
        params.extend(career_params)

    # Build ORDER BY: primary rank_by DESC, then user-supplied
    # tiebreaks ASC (validated against allowlist), then season ASC
    # as the deterministic fallback.
    tiebreak_cols: list[str] = []
    if tiebreak_by:
        for col in tiebreak_by:
            if col not in TIEBREAK_BY_ALLOWED:
                raise ValueError(
                    f"unknown tiebreak_by column {col!r}; allowed: "
                    f"{sorted(TIEBREAK_BY_ALLOWED)}"
                )
            tiebreak_cols.append(col)
    order_clauses = [f"{rank_by} DESC"]
    order_clauses.extend(f"{c} ASC" for c in tiebreak_cols)
    order_clauses.append("season ASC")
    order_sql = ", ".join(order_clauses)
    # In the outer SELECT (post-CTE), `rank_value` is the alias for
    # the rank_by column, so we re-use that for clarity.
    outer_order_clauses = ["rank_value DESC"]
    outer_order_clauses.extend(f"{c} ASC" for c in tiebreak_cols)
    outer_order_clauses.append("season ASC")
    outer_order_sql = ", ".join(outer_order_clauses)

    where_sql = " AND ".join(where_clauses)
    if unique:
        # Pick the best season per player_id (inside the same filter
        # set), then rank that one row per player against the others.
        sql = f"""
            WITH ranked AS (
                SELECT name,
                       team,
                       season,
                       position,
                       {rank_by} AS rank_value,
                       draft_round,
                       draft_year,
                       draft_overall_pick,
                       college,
                       ROW_NUMBER() OVER (
                           PARTITION BY player_id
                           ORDER BY {order_sql}
                       ) AS __rn
                FROM   v_player_season_full
                WHERE  {where_sql}
            )
            SELECT name, team, season, position, rank_value,
                   draft_round, draft_year, draft_overall_pick, college
            FROM   ranked
            WHERE  __rn = 1
            ORDER BY {outer_order_sql}
            LIMIT  ?
        """
    else:
        sql = f"""
            SELECT name,
                   team,
                   season,
                   position,
                   {rank_by} AS rank_value,
                   draft_round,
                   draft_year,
                   draft_overall_pick,
                   college
            FROM   v_player_season_full
            WHERE  {where_sql}
            ORDER BY {order_sql}
            LIMIT  ?
        """
    params.append(n)
    return sql, params


# ---------------------------------------------------------------------------
# Career totals — sums across all qualifying seasons for a player
# ---------------------------------------------------------------------------

# Per-season ratio stats. Summing percentages across seasons would be
# nonsense, so career_topN rebuilds these from the underlying
# (numerator, denominator) columns: SUM(num) / NULLIF(SUM(den), 0).
# NULLIF guards every denominator so a player with no qualifying
# attempts yields NULL rather than a divide-by-zero error.
_CAREER_RATIO_RANK_BY: dict[str, tuple[str, str]] = {
    "pass_cmp_pct": ("pass_cmp", "pass_att"),
    "catch_rate":   ("rec",      "targets"),
}


def _career_stat_expr(stat: str) -> str:
    """SQL fragment for the player's career value of `stat`. Plain
    SUM for raw columns; recomputed SUM(num)/NULLIF(SUM(den), 0) for
    per-season ratio stats."""
    if stat in _CAREER_RATIO_RANK_BY:
        num, den = _CAREER_RATIO_RANK_BY[stat]
        return f"CAST(SUM({num}) AS DOUBLE) / NULLIF(SUM({den}), 0)"
    return f"SUM({stat})"


def _career_min_max_subquery_clauses(
    min_career_stats: dict[str, float] | None,
    max_career_stats: dict[str, float] | None,
) -> tuple[list[str], list]:
    """Build HAVING-style clauses for a career-total filter. Used by
    pos_topN, career_topN, and award_topN as the body of a
    ``player_id IN (SELECT player_id FROM player_season_stats GROUP BY
    player_id HAVING ...)`` subquery.

    Each entry validates the stat against ``RANK_BY_ALLOWED`` (no SQL
    injection) and emits ``<career_expr> >= ?`` or ``<= ?``. Returns
    (clauses, params)."""
    clauses: list[str] = []
    params: list = []
    for src, op, label in (
        (min_career_stats, ">=", "min_career_stats"),
        (max_career_stats, "<=", "max_career_stats"),
    ):
        if not src:
            continue
        for stat, val in src.items():
            if stat not in RANK_BY_ALLOWED:
                raise ValueError(
                    f"unknown {label} column {stat!r}; allowed: "
                    f"{sorted(RANK_BY_ALLOWED)}"
                )
            clauses.append(f"{_career_stat_expr(stat)} {op} ?")
            params.append(val)
    return clauses, params


def career_topN(
    rank_by: str,
    *,
    n: int = 10,
    position: str | None = None,
    start: int | None = None,
    end: int | None = None,
    ever_won_award: list[str] | None = None,
    min_seasons: int | None = None,
    college: str | None = None,
    min_career_stats: dict[str, float] | None = None,
    max_career_stats: dict[str, float] | None = None,
    # Player-attribute filters (independent of single-season state) —
    # all defaulted to no-op for backward compat.
    draft_rounds: list[int | str] | None = None,
    drafted_by: str | None = None,
    first_name_contains: str | None = None,
    last_name_contains: str | None = None,
) -> tuple[str, list]:
    """Top-N players by *career-total* of ``rank_by`` summed across the
    seasons matching the (optional) filters.

    The position filter scopes the SUM to seasons where the player
    played that position — so career-as-QB totals exclude any
    one-off RB seasons. Default (no position filter) sums all seasons.

    Output columns: ``name, career_total, seasons, first_season,
    last_season``.
    """
    if rank_by not in RANK_BY_ALLOWED:
        raise ValueError(
            f"unknown rank_by column {rank_by!r}; allowed: "
            f"{sorted(RANK_BY_ALLOWED)}"
        )

    # Ratio stats: rebuild from underlying num/denom across seasons.
    # career_total = SUM(num) / NULLIF(SUM(den), 0) — never divides by
    # zero. The WHERE filter requires the denominator to be present so
    # a player with zero qualifying attempts is excluded from the
    # leaderboard rather than producing a NULL career_total row.
    if rank_by in _CAREER_RATIO_RANK_BY:
        num, den = _CAREER_RATIO_RANK_BY[rank_by]
        rank_expr = (
            f"CAST(SUM(s.{num}) AS DOUBLE) / NULLIF(SUM(s.{den}), 0)"
        )
        where: list[str] = [f"s.{den} > 0"]
    else:
        rank_expr = f"SUM(s.{rank_by})"
        where = [f"s.{rank_by} IS NOT NULL"]
    params: list = []
    if start is not None:
        where.append("s.season >= ?")
        params.append(start)
    if end is not None:
        where.append("s.season <= ?")
        params.append(end)

    if position and position.upper() != "ALL":
        if position.upper() in POSITION_ALIASES:
            alias_set = POSITION_ALIASES[position.upper()]
            if alias_set is not None:
                ph = ",".join(["?"] * len(alias_set))
                where.append(f"s.position IN ({ph})")
                params.extend(alias_set)
        else:
            where.append("s.position = ?")
            params.append(position)

    if ever_won_award:
        for a in ever_won_award:
            if a not in AWARD_TYPES_ALLOWED:
                raise ValueError(
                    f"unknown ever_won_award {a!r}; allowed: "
                    f"{sorted(AWARD_TYPES_ALLOWED)}"
                )
        ph = ",".join(["?"] * len(ever_won_award))
        # Outright winners only — matches the pos_topN behavior so
        # `--ever-won MVP` doesn't smuggle in 2nd-place vote-getters.
        # vote_finish IS NULL covers the binary awards (PB, AP_FIRST,
        # AP_SECOND, WPMOY) which never carry a placing.
        where.append(
            f"EXISTS (SELECT 1 FROM player_awards pa WHERE "
            f"pa.player_id = s.player_id AND pa.award_type IN ({ph}) "
            f"AND (pa.vote_finish = 1 OR pa.vote_finish IS NULL))"
        )
        params.extend(ever_won_award)

    # College filter via LEFT JOIN on draft_picks. Players without a
    # draft row have d.college NULL and are excluded by ILIKE.
    if college:
        where.append("d.college ILIKE ?")
        params.append(f"%{college}%")

    # Draft-round bucket — matches pos_topN's semantics. The special
    # token "undrafted" means d.draft_round IS NULL (no draft row at
    # all, so the LEFT JOIN yielded all-NULL).
    if draft_rounds:
        int_rounds: list[int] = []
        include_undrafted = False
        for entry in draft_rounds:
            if isinstance(entry, str) and entry.lower() == "undrafted":
                include_undrafted = True
            elif isinstance(entry, int) and not isinstance(entry, bool):
                int_rounds.append(entry)
            else:
                raise ValueError(
                    f"draft_rounds entries must be ints or 'undrafted', got: {entry!r}"
                )
        clauses: list[str] = []
        if int_rounds:
            ph = ",".join(["?"] * len(int_rounds))
            clauses.append(f"d.round IN ({ph})")
            params.extend(int_rounds)
        if include_undrafted:
            clauses.append("d.round IS NULL")
        if clauses:
            where.append("(" + " OR ".join(clauses) + ")")

    # Drafted-by — filters to players whose draft team matches.
    # Excludes undrafted players by definition (NULL d.team).
    if drafted_by:
        where.append("d.team = ?")
        params.append(drafted_by.upper())

    # Name greps — same split-on-first-space convention as pos_topN.
    if first_name_contains:
        where.append("split_part(p.name, ' ', 1) ILIKE ?")
        params.append(f"%{first_name_contains}%")
    if last_name_contains:
        where.append(
            "trim(substr(p.name, length(split_part(p.name, ' ', 1)) + 1)) ILIKE ?"
        )
        params.append(f"%{last_name_contains}%")

    # Career stat min/max thresholds — same HAVING-style subquery as
    # pos_topN. Goes through the same helper so ratio stats recompute
    # via SUM(num)/NULLIF(SUM(den), 0).
    career_clauses, career_params = _career_min_max_subquery_clauses(
        min_career_stats, max_career_stats
    )
    if career_clauses:
        where.append(
            "s.player_id IN (SELECT player_id FROM player_season_stats "
            "GROUP BY player_id HAVING " + " AND ".join(career_clauses) + ")"
        )
        params.extend(career_params)

    where_sql = " AND ".join(where)
    having_clauses: list[str] = []
    if min_seasons is not None:
        having_clauses.append("COUNT(DISTINCT s.season) >= ?")
        params.append(min_seasons)
    having_sql = ("HAVING " + " AND ".join(having_clauses)) if having_clauses else ""

    # ``positions`` is the slash-joined set of positions the player
    # held across the qualifying seasons (alpha-sorted for stable
    # output). ``teams`` is the comma-joined set ordered by the
    # *first* season the player appeared with that team — the natural
    # career chronology rather than alphabetical.
    sql = f"""
        SELECT p.name                                                  AS name,
               STRING_AGG(DISTINCT s.position, '/' ORDER BY s.position) AS positions,
               (SELECT STRING_AGG(team, ',' ORDER BY first_season)
                FROM (
                    SELECT s2.team, MIN(s2.season) AS first_season
                    FROM   player_season_stats s2
                    WHERE  s2.player_id = p.player_id
                    GROUP BY s2.team
                ))                                                     AS teams,
               {rank_expr}                                             AS career_total,
               COUNT(DISTINCT s.season)                                AS seasons,
               MIN(s.season)                                           AS first_season,
               MAX(s.season)                                           AS last_season
        FROM   player_season_stats s
        JOIN   players p USING (player_id)
        LEFT JOIN draft_picks d USING (player_id)
        WHERE  {where_sql}
        GROUP BY p.player_id, p.name
        {having_sql}
        ORDER BY career_total DESC NULLS LAST
        LIMIT  ?
    """
    params.append(n)
    return sql, params


# ---------------------------------------------------------------------------
# Awards listing
# ---------------------------------------------------------------------------

def awards_list(
    *,
    award_type: str | None = None,
    season: int | None = None,
    winners_only: bool = True,
) -> tuple[str, list]:
    """List rows from ``v_award_winners`` filtered by award type and/or
    season.

    ``winners_only=True`` (default) restricts to outright winners —
    rows with ``vote_finish = 1`` for vote-ranked awards (MVP, OPOY,
    DPOY, OROY, DROY, CPOY) plus all rows for binary awards (PB,
    AP_FIRST, AP_SECOND, WPMOY) which carry NULL ``vote_finish``.
    """
    if award_type is not None and award_type not in AWARD_TYPES_ALLOWED:
        raise ValueError(
            f"unknown award_type {award_type!r}; allowed: "
            f"{sorted(AWARD_TYPES_ALLOWED)}"
        )
    where: list[str] = []
    params: list = []
    if award_type is not None:
        where.append("vw.award_type = ?")
        params.append(award_type)
    if season is not None:
        where.append("vw.season = ?")
        params.append(season)
    if winners_only:
        where.append("(vw.vote_finish = 1 OR vw.vote_finish IS NULL)")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    # Position and team for each award row are derived per (player_id,
    # season) from player_season_stats. Players with no stats row
    # for that season (linemen, specialists who don't appear on the
    # six parsed pages) get NULL — STRING_AGG on an empty subquery
    # returns NULL, displayed as empty.
    sql = f"""
        SELECT vw.season,
               vw.award_type,
               vw.name,
               (SELECT STRING_AGG(DISTINCT s.position, '/' ORDER BY s.position)
                FROM   player_season_stats s
                WHERE  s.player_id = vw.player_id
                  AND  s.season    = vw.season)             AS position,
               (SELECT STRING_AGG(s.team, '/' ORDER BY s.team)
                FROM   player_season_stats s
                WHERE  s.player_id = vw.player_id
                  AND  s.season    = vw.season)             AS team,
               vw.vote_finish
        FROM   v_award_winners vw
        {where_sql}
        ORDER BY vw.season DESC, vw.award_type ASC,
                 COALESCE(vw.vote_finish, 1) ASC, vw.name ASC
    """
    return sql, params


# ---------------------------------------------------------------------------
# Award totals — count of an award_type won across a player's career
# ---------------------------------------------------------------------------

def award_topN(
    award_type: str,
    *,
    n: int = 10,
    position: str | None = None,
    college: str | None = None,
    min_career_stats: dict[str, float] | None = None,
    max_career_stats: dict[str, float] | None = None,
) -> tuple[str, list]:
    """Top-N players by career *count* of a single award type.

    Outright winners only — vote_finish=1 for vote-ranked awards
    (MVP/OPOY/...) plus all rows for binary awards (PB, AP_FIRST,
    AP_SECOND, WPMOY) which carry NULL vote_finish. Finalist credit
    is intentionally excluded.

    Filters compose:
      - ``position`` scopes to players whose stats include any season
        at that position (or alias group: SAFETY, DB, LB, DL, FLEX).
      - ``college`` substring-matches draft_picks.college.
      - ``min_career_stats`` / ``max_career_stats`` apply HAVING-style
        thresholds on career SUMs. Ratio stats (pass_cmp_pct,
        catch_rate) recompute as SUM(num)/NULLIF(SUM(den), 0) so
        divide-by-zero is impossible.

    Output columns: ``name, positions, teams, college, award_count,
    seasons``.
    """
    if award_type not in AWARD_TYPES_ALLOWED:
        raise ValueError(
            f"unknown award_type {award_type!r}; allowed: "
            f"{sorted(AWARD_TYPES_ALLOWED)}"
        )

    where: list[str] = [
        "pa.award_type = ?",
        "(pa.vote_finish = 1 OR pa.vote_finish IS NULL)",
    ]
    params: list = [award_type]

    # Position via EXISTS on player_season_stats. Award winners with
    # no stats row (linemen WPMOY etc.) are excluded by the EXISTS,
    # which is correct since we can't establish their position.
    if position and position.upper() != "ALL":
        if position.upper() in POSITION_ALIASES:
            alias_set = POSITION_ALIASES[position.upper()]
            if alias_set is not None:
                ph = ",".join(["?"] * len(alias_set))
                where.append(
                    f"EXISTS (SELECT 1 FROM player_season_stats ps "
                    f"WHERE ps.player_id = pa.player_id "
                    f"AND ps.position IN ({ph}))"
                )
                params.extend(alias_set)
        else:
            where.append(
                "EXISTS (SELECT 1 FROM player_season_stats ps "
                "WHERE ps.player_id = pa.player_id "
                "AND ps.position = ?)"
            )
            params.append(position)

    if college:
        where.append(
            "EXISTS (SELECT 1 FROM draft_picks d "
            "WHERE d.player_id = pa.player_id "
            "AND d.college ILIKE ?)"
        )
        params.append(f"%{college}%")

    career_clauses, career_params = _career_min_max_subquery_clauses(
        min_career_stats, max_career_stats
    )
    if career_clauses:
        where.append(
            "pa.player_id IN (SELECT player_id FROM player_season_stats "
            "GROUP BY player_id HAVING " + " AND ".join(career_clauses) + ")"
        )
        params.extend(career_params)

    where_sql = " AND ".join(where)
    # Correlated subqueries reference p.player_id (which is in GROUP BY)
    # rather than pa.player_id (which isn't), so DuckDB resolves the
    # outer reference cleanly. p.player_id == pa.player_id by virtue
    # of the JOIN ... USING (player_id).
    sql = f"""
        SELECT  p.name                                          AS name,
                (SELECT STRING_AGG(DISTINCT ps.position, '/' ORDER BY ps.position)
                 FROM   player_season_stats ps
                 WHERE  ps.player_id = p.player_id)             AS positions,
                (SELECT STRING_AGG(team, ',' ORDER BY first_season)
                 FROM (
                     SELECT ps.team, MIN(ps.season) AS first_season
                     FROM   player_season_stats ps
                     WHERE  ps.player_id = p.player_id
                     GROUP BY ps.team
                 ))                                             AS teams,
                (SELECT d.college FROM draft_picks d
                 WHERE  d.player_id = p.player_id)              AS college,
                COUNT(*)                                        AS award_count,
                STRING_AGG(CAST(pa.season AS TEXT), ','
                           ORDER BY pa.season)                  AS award_seasons
        FROM    player_awards pa
        JOIN    players p USING (player_id)
        WHERE   {where_sql}
        GROUP BY p.player_id, p.name
        ORDER BY award_count DESC, p.name ASC
        LIMIT  ?
    """
    params.append(n)
    return sql, params
