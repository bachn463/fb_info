"""Command-line entry point: ``fb_info``.

Three commands:

- ``fb_info build --start YEAR --end YEAR`` runs the PFR->DuckDB
  pipeline for the given seasons (default DB at data/ff.duckdb).
  Requires ``data/pfr_session.json`` set up with a browser-cookie
  session — see README "Pre-1999 PFR backfill" (now applies to all
  years, not just pre-1999).
- ``fb_info query "<SQL>"`` runs a raw SQL statement against the DB and
  prints the result as a tabulated table.
- ``fb_info ask <name> [opts...]`` runs a named helper from
  ``ffpts.queries``. Supported helpers and their flags map directly
  to the helper signatures.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import typer

from ffpts.db import DEFAULT_DB_PATH, init_db
from ffpts.pipeline import build as run_build
from ffpts.queries import (
    AWARD_TYPES_ALLOWED,
    RANK_BY_ALLOWED,
    TRIVIA_RANK_BY_ALLOWED,
    award_topN,
    career_topN,
    pos_topN,
)

app = typer.Typer(
    add_completion=False,
    help="FB Info — local DuckDB of NFL player and team season stats "
         "scraped from Pro Football Reference.",
)
ask_app = typer.Typer(
    add_completion=False,
    help="Run a named query helper (pos-top / career / awards / compare / records).",
)
app.add_typer(ask_app, name="ask")


def _history_dir_for_db(db_path: str | Path) -> Path:
    """Trivia history lives alongside the DB so test runs (which use
    a tmp_path DB) don't leak files into the repo's data/ directory."""
    return Path(db_path).resolve().parent / "trivia_history"


def _open_db(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    db_path = Path(db_path)
    if not db_path.exists():
        typer.echo(
            f"DB not found at {db_path}. Run `fb_info build --start YEAR --end YEAR` first.",
            err=True,
        )
        raise typer.Exit(code=1)
    return init_db(db_path)


def _print_rows(rows: list[tuple], columns: list[str]) -> None:
    """Pretty-print a result set as a fixed-width table."""
    if not rows:
        typer.echo("(no rows)")
        return
    widths = [len(c) for c in columns]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(_fmt_cell(cell)))
    sep = "  "
    typer.echo(sep.join(c.ljust(widths[i]) for i, c in enumerate(columns)))
    typer.echo(sep.join("-" * widths[i] for i in range(len(columns))))
    for row in rows:
        typer.echo(sep.join(_fmt_cell(c).ljust(widths[i]) for i, c in enumerate(row)))


def _fmt_cell(c) -> str:
    if c is None:
        return ""
    if isinstance(c, float):
        return f"{c:.2f}"
    return str(c)


@app.command("build")
def cmd_build(
    start: int = typer.Option(..., "--start", help="First season (inclusive)."),
    end: int = typer.Option(..., "--end", help="Last season (inclusive)."),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db", help="Path to the DuckDB file."),
) -> None:
    """Pull seasons [start..end] from PFR and load into the DB.

    Reads data/pfr_session.json for the browser-cookie session that
    gets us past Cloudflare. ~9 PFR pages per season (passing,
    rushing, receiving, defense, kicking, returns, draft, standings)
    at the polite 5s throttle ≈ 45s per uncached year. Cached
    forever after.
    """
    if start > end:
        typer.echo(f"--start ({start}) must be <= --end ({end})", err=True)
        raise typer.Exit(code=2)
    summary = run_build(seasons=range(start, end + 1), db_path=db)
    typer.echo(f"Built DB at {db}")
    typer.echo(f"  seasons: {summary['seasons'][0]}..{summary['seasons'][-1]}")
    typer.echo(f"  team_seasons: {summary['team_seasons_rows']} rows")
    typer.echo(f"  draft_picks:  {summary['draft_picks_rows']} rows")
    total = sum(summary["player_season_stats_rows"].values())
    typer.echo(f"  player_season_stats: {total} rows across {len(summary['seasons'])} seasons")


@app.command("web")
def cmd_web(
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8000, "--port"),
    db: Path = typer.Option(
        DEFAULT_DB_PATH, "--db",
        help="Path to the DuckDB file (the same file the CLI reads).",
    ),
) -> None:
    """Launch a tiny local web frontend over the same query helpers.

    Plain HTML, no JS framework. Routes:
      /            home
      /ask         pos-top / career / awards form + result table
      /trivia      daily / random / make-your-own
    """
    try:
        from ffpts.web import run as run_web
    except ImportError as e:
        typer.echo(
            f"Web extras aren't installed: {e}. "
            'Install with `pip install -e ".[web]"`.',
            err=True,
        )
        raise typer.Exit(code=2)
    typer.echo(f"FB Info web on http://{host}:{port} (Ctrl-C to stop)")
    run_web(host=host, port=port, db=db)


@app.command("query")
def cmd_query(
    sql: str = typer.Argument(..., help="A SQL statement to run."),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db", help="Path to the DuckDB file."),
) -> None:
    """Run a raw SQL statement against the DB and print the result."""
    con = _open_db(db)
    try:
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall() if cols else []
        _print_rows(rows, cols)
    finally:
        con.close()


@ask_app.command("pos-top")
def ask_pos_top(
    position: str = typer.Option(
        "ALL", "--position",
        help='Position label ("QB", "RB", "WR", "TE", "CB", ...) or '
             '"FLEX" (RB/WR/TE) or "ALL" (default — no position filter).',
    ),
    rank_by: str = typer.Option(
        "fpts_ppr", "--rank-by",
        help="Stat column to rank by. e.g. fpts_ppr, pass_yds, rec, "
             "def_int, def_sacks, fg_made.",
    ),
    n: int = typer.Option(10, "--n", help="Number of player-seasons."),
    start: int | None = typer.Option(None, "--start", help="First season (inclusive)."),
    end: int | None = typer.Option(None, "--end", help="Last season (inclusive)."),
    draft_rounds: str | None = typer.Option(
        None, "--draft-rounds",
        help='Comma-separated draft rounds and/or the literal "undrafted". '
             'Examples: "4,5"  |  "undrafted"  |  "4,5,undrafted". '
             "Filters to player-seasons whose draft pick was in any of "
             "these rounds, or who were undrafted, or both.",
    ),
    team: str | None = typer.Option(
        None, "--team", help='Team code, e.g. "SF", "DAL", "GB".'
    ),
    division: str | None = typer.Option(
        None, "--division",
        help='Per-season division name, e.g. "NFC North", "AFC West", "NFC Central".',
    ),
    conference: str | None = typer.Option(
        None, "--conference", help='"AFC" or "NFC".'
    ),
    first_name_contains: str | None = typer.Option(
        None, "--first-name-contains",
        help="Case-insensitive substring match on first name. "
             'Example: --first-name-contains z',
    ),
    last_name_contains: str | None = typer.Option(
        None, "--last-name-contains",
        help="Case-insensitive substring match on last name (everything "
             'after the first space in the display name).',
    ),
    unique: bool = typer.Option(
        False, "--unique/--no-unique",
        help="One row per player — their best season as ranked by "
             "--rank-by (within the active filters). Ties on the rank "
             "value resolve to the earlier season.",
    ),
    has_award: list[str] | None = typer.Option(
        None, "--has-award",
        help="Filter to player-seasons where the player won this "
             "award *that year*. Repeatable: --has-award MVP --has-award OROY = "
             "MVP OR OROY. Allowed: MVP, OPOY, DPOY, OROY, DROY, "
             "CPOY, WPMOY, PB, AP_FIRST, AP_SECOND.",
    ),
    ever_won: list[str] | None = typer.Option(
        None, "--ever-won",
        help="Filter to player-seasons of players who won this award "
             "*at any point in their career*. Repeatable. Composes "
             "with --has-award. Same allowlist.",
    ),
    rookie_only: bool = typer.Option(
        False, "--rookie-only/--no-rookie-only",
        help="Restrict to each player's first season we have data for "
             "(approximately their rookie year).",
    ),
    draft_start: int | None = typer.Option(
        None, "--draft-start",
        help="Filter to players drafted in or after this year "
             "(inclusive). Excludes undrafted players.",
    ),
    draft_end: int | None = typer.Option(
        None, "--draft-end",
        help="Filter to players drafted in or before this year "
             "(inclusive). Excludes undrafted players.",
    ),
    drafted_by: str | None = typer.Option(
        None, "--drafted-by",
        help="Filter to players drafted by this team code (e.g. "
             "'DAL', 'PIT'). Different from --team (the team they "
             "actually played for that season).",
    ),
    tiebreak_by: list[str] | None = typer.Option(
        None, "--tiebreak-by",
        help="Secondary ASC sort columns (when the primary --rank-by "
             "ties). Repeatable. Allowed: any rank-eligible stat plus "
             "draft_year, draft_round, draft_overall_pick, position, "
             "season, age, name, team. Example: --tiebreak-by "
             "draft_year --tiebreak-by draft_round.",
    ),
    min_stat: list[str] | None = typer.Option(
        None, "--min-stat",
        help="Stat threshold of the form col=value. Repeatable. "
             'Example: --min-stat rush_yds=1000 --min-stat rec=50',
    ),
    max_stat: list[str] | None = typer.Option(
        None, "--max-stat",
        help="Stat ceiling of the form col=value. Repeatable. "
             'Example: --max-stat rush_yds=999',
    ),
    college: str | None = typer.Option(
        None, "--college",
        help='College name (substring match). Example: --college Oregon. '
             'Only catches drafted players (PFR exposes college on draft '
             'pages); undrafted UDFAs are excluded.',
    ),
    min_career_stat: list[str] | None = typer.Option(
        None, "--min-career-stat",
        help="Career-total floor of the form col=value. Filters to "
             "players whose SUM(col) across all seasons is >= value. "
             "Repeatable. Ratio stats (pass_cmp_pct, catch_rate) "
             "recompute as SUM(num)/SUM(den). "
             "Example: --min-career-stat pass_yds=20000",
    ),
    max_career_stat: list[str] | None = typer.Option(
        None, "--max-career-stat",
        help="Career-total ceiling of the form col=value. Same shape "
             'as --min-career-stat. Example: '
             '--max-career-stat def_int=30 (under 30 career INTs).',
    ),
    show_awards: bool = typer.Option(
        False, "--show-awards/--no-show-awards",
        help="Append an `awards` column (comma-list of award_types "
             'won that season, e.g. "PB,AP_FIRST,MVP") to the printed '
             "table. Default off — column set unchanged.",
    ),
    show_context: bool = typer.Option(
        False, "--show-context/--no-show-context",
        help="Append `conference`, `division`, `franchise` columns "
             "to the printed table. Default off.",
    ),
    teammate_of: str | None = typer.Option(
        None, "--teammate-of",
        help='Restrict to players who shared any (team, season) with '
             "the named player at any point. Example: "
             '`--teammate-of "Justin Fields"` -> top WRs who were ever '
             "Fields' teammate, even in seasons he didn't play. Accepts "
             'either a name (substring match) or a `pfr:Slug` directly.',
    ),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db", help="Path to the DuckDB file."),
) -> None:
    """Top-N player-seasons at a position, ranked by a stat column.

    All filter flags combine — pass any subset to scope the result.
    Examples:

        fb_info ask pos-top --position QB --rank-by pass_yds --draft-rounds 4,5
        fb_info ask pos-top --position WR --rank-by rec_yds --team SF
        fb_info ask pos-top --position ALL --rank-by def_int --division "NFC North"
        fb_info ask pos-top --position ALL --first-name-contains z --rank-by fpts_ppr
        fb_info ask pos-top --position FLEX --has-award OROY --rookie-only
        fb_info ask pos-top --position RB --rank-by rush_att \\
            --team PIT --start 1990 --end 2020 --max-stat rush_yds=999
        fb_info ask pos-top --position QB --rank-by fpts_ppr \\
            --has-award MVP --show-awards --show-context
    """
    rounds_list: list[int | str] | None = None
    if draft_rounds:
        rounds_list = []
        for token in (t.strip() for t in draft_rounds.split(",")):
            if not token:
                continue
            if token.lower() == "undrafted":
                rounds_list.append("undrafted")
                continue
            try:
                rounds_list.append(int(token))
            except ValueError:
                typer.echo(
                    f"--draft-rounds entries must be ints or 'undrafted'; "
                    f"got {token!r} in {draft_rounds!r}",
                    err=True,
                )
                raise typer.Exit(code=2)

    min_stats_dict = _parse_stat_pairs(min_stat, "--min-stat")
    max_stats_dict = _parse_stat_pairs(max_stat, "--max-stat")
    min_career_dict = _parse_stat_pairs(min_career_stat, "--min-career-stat")
    max_career_dict = _parse_stat_pairs(max_career_stat, "--max-career-stat")

    con = _open_db(db)
    try:
        teammate_resolved = _resolve_teammate_of(con, teammate_of)
        teammate_id = teammate_resolved[0] if teammate_resolved else None
        teammate_name = teammate_resolved[1] if teammate_resolved else None
        sql, params = pos_topN(
            position, n=n, rank_by=rank_by,
            start=start, end=end, draft_rounds=rounds_list,
            team=team, division=division, conference=conference,
            first_name_contains=first_name_contains,
            last_name_contains=last_name_contains,
            unique=unique,
            has_award=has_award if has_award else None,
            ever_won_award=ever_won if ever_won else None,
            rookie_only=rookie_only,
            draft_start=draft_start, draft_end=draft_end,
            drafted_by=drafted_by,
            tiebreak_by=tiebreak_by if tiebreak_by else None,
            min_stats=min_stats_dict if min_stats_dict else None,
            max_stats=max_stats_dict if max_stats_dict else None,
            college=college,
            min_career_stats=min_career_dict if min_career_dict else None,
            max_career_stats=max_career_dict if max_career_dict else None,
            teammate_of_player_id=teammate_id,
        )
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

        if show_awards or show_context:
            rows, cols = _augment_display(con, rows, cols, show_awards, show_context)

        _print_rows(rows, cols)
    finally:
        con.close()


def _validate_trivia_rank_by(rank_by: str | None) -> None:
    """Reject rank_by columns that don't fit the trivia frame.

    `age` and the draft-metadata columns (draft_year / draft_round /
    draft_overall_pick) are valid `ask pos-top` rank-bys but make for
    poor trivia ("guess the player who led the league in age" — the
    answer set is just whoever was oldest, not really a stat
    leaderboard). The general allowlist still accepts them; trivia
    paths use TRIVIA_RANK_BY_ALLOWED."""
    if rank_by is None or rank_by in TRIVIA_RANK_BY_ALLOWED:
        return
    typer.echo(
        f"--rank-by {rank_by!r} isn't allowed in trivia. "
        f"`age` and the draft_* columns produce trivial answer sets. "
        f"Allowed: {', '.join(sorted(TRIVIA_RANK_BY_ALLOWED))}",
        err=True,
    )
    raise typer.Exit(code=2)


def _parse_stat_pairs(
    pairs: list[str] | None, flag_name: str
) -> dict[str, float]:
    """Parse repeated ``col=value`` flags into ``{col: value}``."""
    out: dict[str, float] = {}
    if not pairs:
        return out
    for pair in pairs:
        if "=" not in pair:
            typer.echo(
                f"{flag_name} must be of the form col=value, got {pair!r}",
                err=True,
            )
            raise typer.Exit(code=2)
        col, val = pair.split("=", 1)
        col = col.strip()
        try:
            out[col] = float(val.strip())
        except ValueError:
            typer.echo(
                f"{flag_name} value must be numeric, got {val!r} in {pair!r}",
                err=True,
            )
            raise typer.Exit(code=2)
    return out


def _fmt_award(award_type: str, vote_finish: int | None) -> str:
    """Format an award row for the awards-column display.

    Voted awards (MVP, OPOY, DPOY, OROY, DROY, CPOY) carry a numeric
    vote_finish — 1 = won, 2+ = placing. Render as ``MVP-1``,
    ``MVP-2`` etc. so the user sees who actually won vs. placed.

    Binary awards (PB, AP_FIRST, AP_SECOND, WPMOY) have NULL
    vote_finish and render bare.
    """
    if vote_finish is None:
        return award_type
    return f"{award_type}-{vote_finish}"


def _augment_display(
    con,
    rows: list[tuple],
    cols: list[str],
    show_awards: bool,
    show_context: bool,
) -> tuple[list[tuple], list[str]]:
    """Append ``awards`` and/or context columns to the result for display.

    Pure CLI-side enrichment — joins are made via secondary queries
    against player_awards and v_player_season_full keyed by
    (name, season, team). The underlying pos_topN query and its
    column set are unchanged.
    """
    new_cols = list(cols)
    new_rows = [list(r) for r in rows]

    name_idx = cols.index("name")
    team_idx = cols.index("team")
    season_idx = cols.index("season")

    # Build (name, season, team) -> (awards_str, conf, div, franchise).
    keys = [
        (r[name_idx], r[season_idx], r[team_idx]) for r in rows
    ]
    if show_awards:
        new_cols.append("awards")
    if show_context:
        new_cols.extend(["conference", "division", "franchise"])

    for i, (name, season, team) in enumerate(keys):
        extras: list = []
        if show_awards:
            # Sort: wins (vote_finish=1) first, then placings 2,3,...,
            # then binary awards (NULL vote_finish, alphabetical).
            award_rows = con.execute(
                "SELECT award_type, vote_finish "
                "FROM   player_awards pa "
                "JOIN   players p USING (player_id) "
                "WHERE  p.name = ? AND pa.season = ? "
                "ORDER BY (vote_finish IS NULL) ASC, "
                "         vote_finish ASC, "
                "         award_type ASC",
                [name, season],
            ).fetchall()
            extras.append(",".join(_fmt_award(a, f) for (a, f) in award_rows))
        if show_context:
            ctx = con.execute(
                "SELECT conference, division, franchise "
                "FROM   team_seasons "
                "WHERE  team = ? AND season = ?",
                [team, season],
            ).fetchone()
            extras.extend(ctx if ctx else (None, None, None))
        new_rows[i].extend(extras)

    return [tuple(r) for r in new_rows], new_cols


# ---------------------------------------------------------------------------
# Trivia
# ---------------------------------------------------------------------------

trivia_app = typer.Typer(
    add_completion=False,
    help="Trivia / fact-recall game built on top of the same query helpers.",
)
app.add_typer(trivia_app, name="trivia")


@trivia_app.command("play")
def trivia_play(
    rank_by: str = typer.Option(
        "fpts_ppr", "--rank-by", help="Stat column to rank the answer set."
    ),
    n: int = typer.Option(10, "--n", help="Number of answers (top-N)."),
    position: str = typer.Option(
        "ALL", "--position",
        help='Position filter. "FLEX" expands to RB/WR/TE; "ALL" disables.',
    ),
    start: int | None = typer.Option(None, "--start"),
    end: int | None = typer.Option(None, "--end"),
    draft_rounds: str | None = typer.Option(None, "--draft-rounds"),
    team: str | None = typer.Option(None, "--team"),
    division: str | None = typer.Option(None, "--division"),
    conference: str | None = typer.Option(None, "--conference"),
    first_name_contains: str | None = typer.Option(None, "--first-name-contains"),
    last_name_contains: str | None = typer.Option(None, "--last-name-contains"),
    has_award: list[str] | None = typer.Option(None, "--has-award"),
    ever_won: list[str] | None = typer.Option(None, "--ever-won"),
    rookie_only: bool = typer.Option(False, "--rookie-only/--no-rookie-only"),
    draft_start: int | None = typer.Option(None, "--draft-start"),
    draft_end: int | None = typer.Option(None, "--draft-end"),
    drafted_by: str | None = typer.Option(None, "--drafted-by"),
    tiebreak_by: list[str] | None = typer.Option(None, "--tiebreak-by"),
    min_stat: list[str] | None = typer.Option(None, "--min-stat"),
    max_stat: list[str] | None = typer.Option(None, "--max-stat"),
    college: str | None = typer.Option(
        None, "--college",
        help="College name (substring match). Drafted players only.",
    ),
    min_career_stat: list[str] | None = typer.Option(
        None, "--min-career-stat",
        help="Career-total floor of the form col=value. Repeatable.",
    ),
    max_career_stat: list[str] | None = typer.Option(
        None, "--max-career-stat",
        help="Career-total ceiling of the form col=value. Repeatable.",
    ),
    teammate_of: str | None = typer.Option(
        None, "--teammate-of",
        help='Restrict the answer set to players who were ever a '
             "teammate of the named player (shared any (team, "
             'season)). Example: --teammate-of "Justin Fields".',
    ),
    unique: bool = typer.Option(
        True, "--unique/--no-unique",
        help="Default ON for trivia: each player counts once. Use "
             "--no-unique to play with player-seasons (same player can "
             "appear multiple times for different years).",
    ),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
) -> None:
    """Top-N trivia game. Same filters as `fb_info ask pos-top` plus an
    interactive guessing loop.

    Type a player name (case-insensitive partial match). Special
    inputs: ``give up`` to print the full ranked list and exit;
    ``hint`` for a clue; ``quit`` to exit (also prints the ranked
    list — the user always leaves with the answers visible).
    """
    _validate_trivia_rank_by(rank_by)
    rounds_list: list[int | str] | None = None
    if draft_rounds:
        rounds_list = []
        for token in (t.strip() for t in draft_rounds.split(",")):
            if not token:
                continue
            if token.lower() == "undrafted":
                rounds_list.append("undrafted")
                continue
            try:
                rounds_list.append(int(token))
            except ValueError:
                typer.echo(
                    f"--draft-rounds entries must be ints or 'undrafted'; "
                    f"got {token!r}",
                    err=True,
                )
                raise typer.Exit(code=2)

    min_stats_dict = _parse_stat_pairs(min_stat, "--min-stat")
    max_stats_dict = _parse_stat_pairs(max_stat, "--max-stat")
    min_career_dict = _parse_stat_pairs(min_career_stat, "--min-career-stat")
    max_career_dict = _parse_stat_pairs(max_career_stat, "--max-career-stat")

    con = _open_db(db)
    try:
        teammate_resolved = _resolve_teammate_of(con, teammate_of)
        teammate_id = teammate_resolved[0] if teammate_resolved else None
        teammate_name = teammate_resolved[1] if teammate_resolved else None
        sql, params = pos_topN(
            position, n=n, rank_by=rank_by,
            start=start, end=end, draft_rounds=rounds_list,
            team=team, division=division, conference=conference,
            first_name_contains=first_name_contains,
            last_name_contains=last_name_contains,
            unique=unique,
            has_award=has_award if has_award else None,
            ever_won_award=ever_won if ever_won else None,
            rookie_only=rookie_only,
            draft_start=draft_start, draft_end=draft_end,
            drafted_by=drafted_by,
            tiebreak_by=tiebreak_by if tiebreak_by else None,
            min_stats=min_stats_dict if min_stats_dict else None,
            max_stats=max_stats_dict if max_stats_dict else None,
            college=college,
            min_career_stats=min_career_dict if min_career_dict else None,
            max_career_stats=max_career_dict if max_career_dict else None,
            teammate_of_player_id=teammate_id,
        )
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        answers = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        con.close()

    if not answers:
        typer.echo("No matching player-seasons for those filters; nothing to guess.")
        raise typer.Exit(code=0)

    title = _build_trivia_title(
        n=n, rank_by=rank_by, position=position,
        start=start, end=end,
        team=team, division=division, conference=conference,
        first_name_contains=first_name_contains,
        last_name_contains=last_name_contains,
        has_award=has_award, ever_won=ever_won,
        rookie_only=rookie_only,
        draft_start=draft_start, draft_end=draft_end,
        drafted_by=drafted_by, draft_rounds=rounds_list,
        min_stats=min_stats_dict, max_stats=max_stats_dict,
        unique=unique,
        college=college,
        min_career_stats=min_career_dict if min_career_dict else None,
        max_career_stats=max_career_dict if max_career_dict else None,
        teammate_of_name=teammate_name,
    )

    # Save a template-shaped spec so this game can be replayed later.
    # Mirrors what `_resolve_template` consumes — same shape as
    # daily/random templates, with ``mode = "season"`` since play is
    # always single-season.
    from ffpts.trivia_replay import save_spec
    play_spec: dict = {
        "rank_by":  rank_by,
        "n":        n,
        "position": position,
        "mode":     "season",
        "unique":   unique,
    }
    if start is not None:               play_spec["start"]               = start
    if end is not None:                 play_spec["end"]                 = end
    if team:                            play_spec["team"]                = team
    if division:                        play_spec["division"]            = division
    if conference:                      play_spec["conference"]          = conference
    if first_name_contains:             play_spec["first_name_contains"] = first_name_contains
    if last_name_contains:              play_spec["last_name_contains"]  = last_name_contains
    if has_award:                       play_spec["has_award"]           = has_award
    if ever_won:                        play_spec["ever_won_award"]      = ever_won
    if rookie_only:                     play_spec["rookie_only"]         = True
    if rounds_list:                     play_spec["draft_rounds"]        = rounds_list
    if draft_start is not None:         play_spec["draft_start"]         = draft_start
    if draft_end is not None:           play_spec["draft_end"]           = draft_end
    if drafted_by:                      play_spec["drafted_by"]          = drafted_by
    if tiebreak_by:                     play_spec["tiebreak_by"]         = tiebreak_by
    if college:                         play_spec["college"]             = college
    if min_career_dict:                 play_spec["min_career_stats"]    = min_career_dict
    if max_career_dict:                 play_spec["max_career_stats"]    = max_career_dict
    if min_stats_dict:                  play_spec["min_stats"]           = min_stats_dict
    if max_stats_dict:                  play_spec["max_stats"]           = max_stats_dict
    if teammate_id:                     play_spec["teammate_of_player_id"] = teammate_id
    if teammate_name:                   play_spec["teammate_of_name"] = teammate_name
    game_id = save_spec(
        play_spec, label="play",
        history_dir=_history_dir_for_db(db),
    )
    typer.echo(f"(game {game_id} — replay with `fb_info trivia replay {game_id}`)")

    _run_trivia_loop(answers, rank_by=rank_by, title=title)


def _build_trivia_title(
    *, n: int, rank_by: str, position: str,
    start: int | None, end: int | None,
    team: str | None, division: str | None, conference: str | None,
    first_name_contains: str | None, last_name_contains: str | None,
    has_award: list[str] | None, ever_won: list[str] | None,
    rookie_only: bool,
    draft_start: int | None, draft_end: int | None,
    drafted_by: str | None, draft_rounds: list | None,
    min_stats: dict | None, max_stats: dict | None,
    unique: bool,
    mode: str = "season",
    college: str | None = None,
    min_career_stats: dict | None = None,
    max_career_stats: dict | None = None,
    teammate_of_name: str | None = None,
) -> str:
    """Render a one-line description of the active trivia query so
    users know what they're guessing at game start and at exit."""
    pos = (position or "").upper()
    pos_part = "" if pos in ("", "ALL") else f"{pos} "

    if start and end:
        years = f" ({start}-{end})"
    elif start:
        years = f" ({start}+)"
    elif end:
        years = f" (through {end})"
    else:
        years = ""

    unit = "career-totals" if mode == "career" else "player-seasons"
    head = f"Top {n} {pos_part}{unit} by {rank_by}{years}"

    clauses: list[str] = []
    scope = []
    if team:
        scope.append(team.upper())
    if division:
        scope.append(division)
    if conference:
        scope.append(conference.upper())
    if scope:
        clauses.append("from " + " / ".join(scope))

    if has_award:
        clauses.append(f"with award {'/'.join(has_award)} that season")
    if ever_won:
        clauses.append(f"who ever won {'/'.join(ever_won)}")
    if rookie_only:
        clauses.append("rookie season only")

    if drafted_by:
        clauses.append(f"drafted by {drafted_by.upper()}")
    if draft_start and draft_end:
        clauses.append(f"drafted {draft_start}-{draft_end}")
    elif draft_start:
        clauses.append(f"drafted {draft_start}+")
    elif draft_end:
        clauses.append(f"drafted through {draft_end}")
    if draft_rounds:
        clauses.append(
            f"draft rounds {','.join(str(r) for r in draft_rounds)}"
        )

    if min_stats:
        for k, v in min_stats.items():
            clauses.append(f"{k} >= {_fmt_cell(v)}")
    if max_stats:
        for k, v in max_stats.items():
            clauses.append(f"{k} <= {_fmt_cell(v)}")
    if min_career_stats:
        for k, v in min_career_stats.items():
            clauses.append(f"career {k} >= {_fmt_cell(v)}")
    if max_career_stats:
        for k, v in max_career_stats.items():
            clauses.append(f"career {k} <= {_fmt_cell(v)}")

    if college:
        clauses.append(f"from {college}")

    if teammate_of_name:
        clauses.append(f"ever a teammate of {teammate_of_name}")

    if first_name_contains:
        clauses.append(f"first name contains '{first_name_contains}'")
    if last_name_contains:
        clauses.append(f"last name contains '{last_name_contains}'")

    # Career mode is always unique-by-player by definition; only flag
    # the multi-season tag in season mode.
    if not unique and mode == "season":
        clauses.append("multi-season per player allowed")

    if clauses:
        return head + ", " + ", ".join(clauses)
    return head


def _run_trivia_loop(
    answers: list[dict], *, rank_by: str, title: str
) -> None:
    """Interactive REPL. Each answer dict has keys: name, team,
    season, position, rank_value, draft_round, draft_year,
    draft_overall_pick.
    """
    n = len(answers)
    found: set[int] = set()
    guesses = 0
    hint_cursor = 0
    hint_levels: dict[int, int] = {}

    typer.echo(title)
    typer.echo(
        "Type a name (substring OK). "
        "Commands: `give up`, `hint`, `quit`."
    )

    while len(found) < n:
        try:
            raw = typer.prompt(">>>", prompt_suffix=" ")
        except (EOFError, KeyboardInterrupt):
            typer.echo()
            break
        guess = raw.strip()
        if not guess:
            continue
        cmd = guess.lower()
        if cmd == "quit":
            _print_final_ranked_list(
                answers, found, rank_by=rank_by, title=title
            )
            typer.echo(
                f"\nFinal score: {len(found)} / {n} in {guesses} guesses."
            )
            return
        if cmd == "give up":
            _print_final_ranked_list(
                answers, found, rank_by=rank_by, title=title
            )
            typer.echo(
                f"\nFinal score: {len(found)} / {n} in {guesses} guesses."
            )
            return
        if cmd == "hint":
            hint_cursor = _print_hint(
                answers, found, hint_cursor, hint_levels, rank_by=rank_by
            )
            continue

        guesses += 1
        matches = _match_guess(guess, answers, found)
        if not matches:
            typer.echo(f"  Not in the top {n}.")
        elif len(matches) > 1 and len({_player_identity(answers[i]) for i in matches}) > 1:
            # Genuinely ambiguous — matches refer to multiple distinct
            # players. Don't list candidate names (would spoil
            # answers).
            typer.echo(
                f"  Ambiguous — matches {len(matches)} answers across "
                "multiple players. Be more specific."
            )
        else:
            # Either a single match, or multiple matches that are all
            # the same player (same name + draft fingerprint). Credit
            # all of them on this one guess so a player who appears
            # multiple times in the list (e.g. with --no-unique) only
            # has to be guessed once.
            for i in matches:
                found.add(i)
            for i in sorted(matches):
                row = answers[i]
                rank = i + 1
                typer.echo(
                    f"  Correct! #{rank} {row['name']}, {row['season']} "
                    f"({row['team']}, {rank_by}={_fmt_cell(row['rank_value'])})."
                )

    # Loop exited (either all found, or stdin closed). Print the full
    # ranked list either way so the user always leaves with the answers.
    _print_final_ranked_list(answers, found, rank_by=rank_by, title=title)
    if len(found) == n:
        typer.echo(
            f"\nAll {n} found in {guesses} guesses. Nice."
        )
    else:
        typer.echo(
            f"\nFinal score: {len(found)} / {n} in {guesses} guesses."
        )


def _match_guess(guess: str, answers: list[dict], found: set[int]) -> list[int]:
    """Return indices of unfound answers whose name contains the
    guess (case-insensitive)."""
    needle = guess.lower()
    return [
        i for i, row in enumerate(answers)
        if i not in found and needle in (row["name"] or "").lower()
    ]


def _player_identity(row: dict) -> tuple:
    """A tuple that identifies the underlying player across multiple
    answer rows. Two answer dicts with the same identity are the same
    person — used to credit a single guess against all of their
    appearances when --no-unique is in effect.

    (name, draft_year, draft_overall_pick) — name plus the draft
    fingerprint, which is unique per player. Undrafted players land
    on (name, None, None) which is still unique within a top-N answer
    set in practice."""
    return (
        row.get("name"),
        row.get("draft_year"),
        row.get("draft_overall_pick"),
    )


def _hint_layers(row: dict, *, rank_by: str) -> list[str]:
    """Ordered list of hint reveals for a single answer row. Each
    successive call to ``hint`` on the same row reveals one more
    layer."""
    layers: list[str] = []
    layers.append(f"team {row.get('team') or '?'}")
    layers.append(f"year {row.get('season') or '?'}")
    layers.append(f"position {row.get('position') or '?'}")
    dy = row.get("draft_year")
    s = row.get("season")
    if dy is not None and s is not None:
        layers.append(f"season #{int(s) - int(dy) + 1} of career")
    else:
        layers.append("career year unknown")
    layers.append(f"{rank_by}={_fmt_cell(row.get('rank_value'))}")
    dr = row.get("draft_round")
    if dr is not None and dy is not None:
        layers.append(f"drafted round {dr} in {dy}")
    elif dr is not None:
        layers.append(f"drafted round {dr}")
    else:
        layers.append("undrafted")
    name = row.get("name") or ""
    last = name.split()[-1] if name else ""
    if last:
        layers.append(f"last name starts with '{last[0].upper()}'")
    return layers


def _print_hint(
    answers: list[dict],
    found: set[int],
    cursor: int,
    hint_levels: dict[int, int],
    *,
    rank_by: str,
) -> int:
    """Print a progressive hint about an unfound answer.

    Cursor cycles through unfound answers. Each time we re-land on the
    same player (after wrapping), the level for that player advances
    and one more layer is revealed (team, then year, then position,
    then career-year, then ranking-stat value, then draft round/year,
    then last-name initial).
    """
    unfound = [i for i in range(len(answers)) if i not in found]
    if not unfound:
        typer.echo("  No hints — you got them all.")
        return cursor
    idx = unfound[cursor % len(unfound)]
    level = hint_levels.get(idx, 0) + 1
    layers = _hint_layers(answers[idx], rank_by=rank_by)
    capped = min(level, len(layers))
    hint_levels[idx] = capped
    rank = idx + 1
    typer.echo(
        f"  Hint #{capped} for #{rank}: " + ", ".join(layers[:capped])
    )
    return cursor + 1


def _print_final_ranked_list(
    answers: list[dict], found: set[int], *, rank_by: str, title: str
) -> None:
    """Print the full ranked answer list with a marker per row showing
    whether the user found it (✓) or not (✗). Called on every trivia
    exit path so the user always leaves with the answers."""
    if not answers:
        return
    typer.echo(f"\nFinal ranked list — {title}:")
    for i, row in enumerate(answers):
        marker = "✓" if i in found else "✗"
        rank = i + 1
        typer.echo(
            f"  {marker} #{rank}: {row['name']} "
            f"({row['team']} {row['season']}, "
            f"{rank_by}={_fmt_cell(row['rank_value'])})"
        )


# ---------------------------------------------------------------------------
# `ask records` — single-season all-time records dashboard
# ---------------------------------------------------------------------------

# Curated stat columns per category. Tuned to the ones fans actually
# care about; not the full RANK_BY_ALLOWED set (which would make the
# output noisy with 50+ rows).
_RECORDS_OFFENSE = [
    "pass_yds", "pass_td", "pass_cmp", "pass_rating",
    "rush_yds", "rush_td", "rush_att",
    "rec", "rec_yds", "rec_td", "targets",
    "fpts_ppr", "fpts_half", "fpts_std",
]
_RECORDS_DEFENSE = [
    "def_sacks", "def_int", "def_int_td",
    "def_tackles_combined", "def_pass_def",
    "def_fumbles_forced", "def_fumbles_rec",
    "def_fumbles_rec_td", "def_safeties",
]
_RECORDS_SPECIAL = [
    "fg_made", "fg_long", "xp_made",
    "punts", "punt_yds", "punt_long",
    "kr_yds", "kr_td", "pr_yds", "pr_td",
]
_RECORDS_CATEGORIES: dict[str, list[str]] = {
    "offense": _RECORDS_OFFENSE,
    "defense": _RECORDS_DEFENSE,
    "special": _RECORDS_SPECIAL,
    "all":     _RECORDS_OFFENSE + _RECORDS_DEFENSE + _RECORDS_SPECIAL,
}


@ask_app.command("records")
def ask_records(
    category: str = typer.Option(
        "all", "--category",
        help="offense | defense | special | all (default).",
    ),
    n: int = typer.Option(
        1, "--n",
        help="Top-N per stat. Default 1 (just the record holder).",
    ),
    start: int | None = typer.Option(None, "--start"),
    end: int | None = typer.Option(None, "--end"),
    position: str = typer.Option(
        "ALL", "--position",
        help='Optional position scope. "ALL" (default) = no filter.',
    ),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
) -> None:
    """Single-season records: top-N for each major stat, side by side.

    Walks a curated list of stat columns, runs a top-N query for each,
    and prints all results as one labelled table. Defaults to top-1 per
    stat ("the record holder for X is..."); pass --n 3 to see runners-up.
    """
    if category not in _RECORDS_CATEGORIES:
        typer.echo(
            f"--category must be one of "
            f"{sorted(_RECORDS_CATEGORIES)}, got {category!r}",
            err=True,
        )
        raise typer.Exit(code=2)
    stats = _RECORDS_CATEGORIES[category]

    con = _open_db(db)
    try:
        rows: list[tuple] = []
        for stat in stats:
            sql, params = pos_topN(
                position, n=n, rank_by=stat,
                start=start, end=end, unique=False,
            )
            try:
                cur = con.execute(sql, params)
                results = cur.fetchall()
            except Exception:
                # A few stats have no data for some position scopes
                # (e.g. fpts_* with position=ALL still works, but if
                # the user passed --position QB we just skip empty).
                continue
            cols = [d[0] for d in cur.description]
            value_idx = cols.index("rank_value")
            name_idx = cols.index("name")
            team_idx = cols.index("team")
            season_idx = cols.index("season")
            pos_idx = cols.index("position")
            for r in results:
                rows.append((
                    stat,
                    r[name_idx],
                    r[pos_idx],
                    r[team_idx],
                    r[season_idx],
                    r[value_idx],
                ))
    finally:
        con.close()

    _print_rows(rows, ["stat", "name", "pos", "team", "season", "value"])


# ---------------------------------------------------------------------------
# `ask career` — career totals
# ---------------------------------------------------------------------------

@ask_app.command("career")
def ask_career(
    rank_by: str = typer.Option(
        "fpts_ppr", "--rank-by",
        help="Stat column to sum across seasons. Ignored when --award "
             "is set (which dispatches to award-count ranking).",
    ),
    award: str | None = typer.Option(
        None, "--award",
        help="Rank by career *count* of this award type instead of a "
             "stat sum. One of: "
             + ", ".join(sorted(AWARD_TYPES_ALLOWED)) + ". "
             "Composes with --position, --college, --min-career-stat, "
             "--max-career-stat. Other filters (start/end, ever-won, "
             "min-seasons, draft, name) are ignored when --award is set "
             "since the underlying award_topN helper doesn't accept them.",
    ),
    n: int = typer.Option(10, "--n"),
    position: str = typer.Option(
        "ALL", "--position",
        help='Position scope (sums only seasons at this position). '
             '"ALL" (default) = all seasons.',
    ),
    start: int | None = typer.Option(None, "--start"),
    end: int | None = typer.Option(None, "--end"),
    ever_won: list[str] | None = typer.Option(
        None, "--ever-won",
        help="Restrict to players who won this award at any point. "
             "Repeatable.",
    ),
    min_seasons: int | None = typer.Option(
        None, "--min-seasons",
        help="Require at least this many qualifying seasons "
             "(blocks one-year-wonders distorting the leaderboard).",
    ),
    college: str | None = typer.Option(
        None, "--college",
        help="College name (substring match). Drafted players only.",
    ),
    min_career_stat: list[str] | None = typer.Option(
        None, "--min-career-stat",
        help="Career-total floor of the form col=value. Repeatable. "
             "Composes with --rank-by — use to require, e.g., "
             "--min-career-stat games=100 alongside a low-volume "
             "ranking.",
    ),
    max_career_stat: list[str] | None = typer.Option(
        None, "--max-career-stat",
        help="Career-total ceiling of the form col=value. Example: "
             '"--max-career-stat def_int=30" for safeties with under '
             "30 career interceptions.",
    ),
    draft_rounds: str | None = typer.Option(
        None, "--draft-rounds",
        help='Comma-separated draft rounds and/or "undrafted". '
             "Filters to players whose draft pick fell in any of these "
             "rounds. Same shape as `ask pos-top --draft-rounds`.",
    ),
    drafted_by: str | None = typer.Option(
        None, "--drafted-by",
        help="Filter to players drafted by this team code (e.g. 'PIT').",
    ),
    first_name_contains: str | None = typer.Option(
        None, "--first-name-contains",
        help="Case-insensitive substring match on first name.",
    ),
    last_name_contains: str | None = typer.Option(
        None, "--last-name-contains",
        help="Case-insensitive substring match on last name.",
    ),
    draft_start: int | None = typer.Option(
        None, "--draft-start",
        help="Filter to players drafted in or after this year.",
    ),
    draft_end: int | None = typer.Option(
        None, "--draft-end",
        help="Filter to players drafted in or before this year.",
    ),
    teammate_of: str | None = typer.Option(
        None, "--teammate-of",
        help='Restrict to players who shared any (team, season) with '
             "the named player at any point. Composes with both "
             "--rank-by and --award modes.",
    ),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
) -> None:
    """Top-N players by career value of either a stat sum (--rank-by)
    or an award count (--award).

    --rank-by mode (default): SUM(stat) across qualifying seasons,
    ranked descending. Ratio stats (pass_cmp_pct / catch_rate)
    recompute from the underlying numerator / denominator.

    --award mode: COUNT(*) of player_awards rows of that type
    (outright winners only). Composes with position, college, career
    stat thresholds, year range, ever-won, draft filters, and name
    contains. e.g. `--award CPOY --ever-won MVP` lists CPOY winners
    who also won MVP at some point. min-seasons is silently dropped
    in --award mode (it's a career-stat concept).
    """
    rounds_list: list[int | str] | None = None
    if draft_rounds:
        rounds_list = []
        for token in (t.strip() for t in draft_rounds.split(",")):
            if not token:
                continue
            if token.lower() == "undrafted":
                rounds_list.append("undrafted")
            else:
                try:
                    rounds_list.append(int(token))
                except ValueError:
                    typer.echo(
                        f"--draft-rounds entries must be ints or 'undrafted'; "
                        f"got {token!r}",
                        err=True,
                    )
                    raise typer.Exit(code=2)
    min_career_dict = _parse_stat_pairs(min_career_stat, "--min-career-stat")
    max_career_dict = _parse_stat_pairs(max_career_stat, "--max-career-stat")

    con = _open_db(db)
    try:
        teammate_resolved = _resolve_teammate_of(con, teammate_of)
        teammate_id = teammate_resolved[0] if teammate_resolved else None
        teammate_name = teammate_resolved[1] if teammate_resolved else None
        if award:
            # Award-count ranking. award_topN now composes with the same
            # filters career_topN does (except min_seasons, which is a
            # career-stat concept and doesn't apply to award counts).
            sql, params = award_topN(
                award, n=n, position=position,
                college=college,
                min_career_stats=min_career_dict if min_career_dict else None,
                max_career_stats=max_career_dict if max_career_dict else None,
                start=start, end=end,
                ever_won_award=ever_won if ever_won else None,
                draft_rounds=rounds_list,
                draft_start=draft_start, draft_end=draft_end,
                drafted_by=drafted_by,
                first_name_contains=first_name_contains,
                last_name_contains=last_name_contains,
                teammate_of_player_id=teammate_id,
            )
        else:
            sql, params = career_topN(
                rank_by, n=n, position=position,
                start=start, end=end,
                ever_won_award=ever_won if ever_won else None,
                min_seasons=min_seasons,
                college=college,
                min_career_stats=min_career_dict if min_career_dict else None,
                max_career_stats=max_career_dict if max_career_dict else None,
                draft_rounds=rounds_list,
                drafted_by=drafted_by,
                first_name_contains=first_name_contains,
                last_name_contains=last_name_contains,
                draft_start=draft_start, draft_end=draft_end,
                teammate_of_player_id=teammate_id,
            )
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        _print_rows(cur.fetchall(), cols)
    finally:
        con.close()




# ---------------------------------------------------------------------------
# `ask compare` — two-player head-to-head career summary
# ---------------------------------------------------------------------------

# Career stats shown side-by-side for `ask compare`. Same curated list
# as `ask records` defense+offense+special so users see one row per stat.
_COMPARE_STATS = (
    _RECORDS_OFFENSE + _RECORDS_DEFENSE + _RECORDS_SPECIAL
)


def _player_career_blurb(con: duckdb.DuckDBPyConnection, player_id: str) -> str:
    """Return ``"(YYYY-YYYY, TM1/TM2/...)"`` describing the player's
    seasons + teams. Used for compare disambiguation so users can
    pick between same-name players. Empty string if no stats rows."""
    row = con.execute(
        """
        SELECT MIN(season),
               MAX(season),
               STRING_AGG(team, '/' ORDER BY first_season)
        FROM (
            SELECT season, team, MIN(season) OVER (PARTITION BY team) AS first_season
            FROM   player_season_stats
            WHERE  player_id = ?
        )
        """,
        [player_id],
    ).fetchone()
    if not row or row[0] is None:
        return ""
    first, last, teams = row
    return f"({first}-{last}, {teams})"


def _resolve_player(
    con: duckdb.DuckDBPyConnection, name: str
) -> tuple[str, str] | None:
    """Resolve a name fragment to (player_id, exact_name).

    Tries exact (case-insensitive) match first; falls back to substring
    if exactly one substring match exists. When multiple players match
    (same exact spelling, e.g. two "Adrian Peterson"s), prints a
    disambiguation list that includes each candidate's year range and
    teams so the caller can re-invoke with ``--p1-id`` / ``--p2-id``.
    """
    rows = con.execute(
        "SELECT player_id, name FROM players WHERE LOWER(name) = LOWER(?)",
        [name],
    ).fetchall()
    if len(rows) == 1:
        return rows[0]
    if len(rows) > 1:
        typer.echo(
            f"  {len(rows)} players match {name!r} exactly:",
            err=True,
        )
        for pid, exact in rows:
            blurb = _player_career_blurb(con, pid) or "(no stats rows)"
            typer.echo(f"    --p?-id {pid}  {exact} {blurb}", err=True)
        typer.echo(
            "  Re-run with --p1-id / --p2-id pfr:Slug to pick.",
            err=True,
        )
        return None

    rows = con.execute(
        "SELECT player_id, name FROM players WHERE LOWER(name) LIKE LOWER(?)",
        [f"%{name}%"],
    ).fetchall()
    if len(rows) == 1:
        return rows[0]
    if len(rows) == 0:
        typer.echo(f"  No player matches {name!r}.", err=True)
        return None
    typer.echo(
        f"  {len(rows)} players match {name!r}:",
        err=True,
    )
    for pid, exact in rows[:10]:
        blurb = _player_career_blurb(con, pid) or "(no stats rows)"
        typer.echo(f"    --p?-id {pid}  {exact} {blurb}", err=True)
    if len(rows) > 10:
        typer.echo(f"    ... and {len(rows) - 10} more", err=True)
    typer.echo(
        "  Be more specific or pass --p1-id / --p2-id pfr:Slug.",
        err=True,
    )
    return None


def _resolve_teammate_of(
    con: duckdb.DuckDBPyConnection, value: str | None
) -> tuple[str, str] | None:
    """Resolve a ``--teammate-of`` argument to (player_id, exact_name).

    Accepts either a ``pfr:Slug`` (used directly) or a display name
    (resolved via ``_resolve_player``). On ambiguous / missing match
    the underlying resolver prints a disambiguation list to stderr;
    we then exit with code 2 so the user can re-invoke with a more
    specific value.

    Returns None when ``value`` itself is None/empty (caller knows
    no teammate filter is in effect). Two-tuple lets callers feed
    the name to the title builder while threading the player_id to
    the SQL helper."""
    if not value:
        return None
    if value.startswith("pfr:"):
        row = _lookup_player_by_id(con, value)
        if row is None:
            raise typer.Exit(code=2)
        return row[0], row[1]
    row = _resolve_player(con, value)
    if row is None:
        raise typer.Exit(code=2)
    return row[0], row[1]


def _lookup_player_by_id(
    con: duckdb.DuckDBPyConnection, player_id: str
) -> tuple[str, str] | None:
    """Look up (player_id, name) by exact player_id. Returns None and
    prints an error if no row matches."""
    row = con.execute(
        "SELECT player_id, name FROM players WHERE player_id = ?",
        [player_id],
    ).fetchone()
    if row is None:
        typer.echo(f"  No player with player_id={player_id!r}.", err=True)
        return None
    return row


@ask_app.command("compare")
def ask_compare(
    player1: str = typer.Argument(
        "", help="First player name (substring OK). Optional if --p1-id is given.",
    ),
    player2: str = typer.Argument(
        "", help="Second player name (substring OK). Optional if --p2-id is given.",
    ),
    p1_id: str | None = typer.Option(
        None, "--p1-id",
        help="Disambiguation override: exact pfr:Slug for player 1. "
             "Use when two players share the same display name "
             "(printed alongside the candidate list when name "
             "resolution finds multiple matches).",
    ),
    p2_id: str | None = typer.Option(
        None, "--p2-id",
        help="Disambiguation override: exact pfr:Slug for player 2.",
    ),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
) -> None:
    """Career head-to-head between two players.

    Shows career totals for major offensive / defensive / special-teams
    stats side-by-side, plus seasons played and best-single-season for
    each player. Each player resolved via case-insensitive name match
    against the players table; ambiguous matches print a candidate
    list with year ranges + teams and exit non-zero — re-run with
    ``--p1-id`` / ``--p2-id pfr:Slug`` to pick.
    """
    con = _open_db(db)
    try:
        if p1_id is not None:
            r1 = _lookup_player_by_id(con, p1_id)
        elif player1:
            r1 = _resolve_player(con, player1)
        else:
            typer.echo("  Pass a player1 name or --p1-id.", err=True)
            r1 = None
        if p2_id is not None:
            r2 = _lookup_player_by_id(con, p2_id)
        elif player2:
            r2 = _resolve_player(con, player2)
        else:
            typer.echo("  Pass a player2 name or --p2-id.", err=True)
            r2 = None

        if r1 is None or r2 is None:
            raise typer.Exit(code=2)
        pid1, name1 = r1
        pid2, name2 = r2

        # When the display names collide, append the year/team blurb so
        # the printed table columns are distinguishable.
        if name1 == name2:
            b1 = _player_career_blurb(con, pid1)
            b2 = _player_career_blurb(con, pid2)
            if b1:
                name1 = f"{name1} {b1}"
            if b2:
                name2 = f"{name2} {b2}"

        agg_cols = ", ".join(f"SUM({c}) AS {c}" for c in _COMPARE_STATS)
        meta = (
            "COUNT(DISTINCT season)  AS seasons, "
            "MIN(season)             AS first_season, "
            "MAX(season)             AS last_season"
        )
        sql = f"SELECT {meta}, {agg_cols} FROM player_season_stats WHERE player_id = ?"
        row1 = con.execute(sql, [pid1]).fetchone()
        row2 = con.execute(sql, [pid2]).fetchone()

        cols = ["seasons", "first_season", "last_season"] + list(_COMPARE_STATS)
        d1 = dict(zip(cols, row1))
        d2 = dict(zip(cols, row2))

        rows = [(c, d1.get(c), d2.get(c)) for c in cols]
        _print_rows(rows, ["stat", name1, name2])
    finally:
        con.close()


# ---------------------------------------------------------------------------
# `trivia daily` / `trivia random` — template-driven trivia
# ---------------------------------------------------------------------------

# Vocabularies the random generator samples from. Curated to produce
# non-empty answer sets on the canonical 1970-2025 build, and to skip
# combinations that don't exist (e.g. no point ranking by `def_sacks`
# before 1982 since that's when sacks became an official stat).
#
# List-duplication is the weighting mechanism. Target distribution
# (lean toward the stats fans care about most):
#   offense (pass+rush+rec): ~48%
#   fantasy:                 ~38%
#   defense:                 ~10%
#   special teams (K + ret):  ~4%
# Total list size = 100 so each entry-count = direct percentage.
_RANDOM_RANK_BY: list[str] = (
    # Passing — 16 (incl. 3 for the new pass_cmp_pct ratio stat)
    ["pass_yds"] * 5 + ["pass_td"] * 4 + ["pass_cmp_pct"] * 3
    + ["pass_rating"] * 2 + ["pass_cmp"] * 2
    # Rushing — 14
    + ["rush_yds"] * 5 + ["rush_td"] * 5 + ["rush_att"] * 4
    # Receiving — 18 (catch_rate is the new ratio stat)
    + ["rec_yds"] * 5 + ["rec"] * 5 + ["rec_td"] * 4
    + ["targets"] * 2 + ["catch_rate"] * 2
    # Fantasy — 38 (PPR most popular, hence heaviest)
    + ["fpts_ppr"] * 16 + ["fpts_half"] * 12 + ["fpts_std"] * 10
    # Defense — 10
    + ["def_sacks"] * 3 + ["def_int"] * 3 + ["def_tackles_combined"] * 2
    + ["def_int_td"] + ["def_pass_def"]
    # Special teams — 4 (rarest; one per kicking + each return type)
    + ["fg_made"] + ["fg_long"] + ["kr_yds"] + ["pr_yds"]
)

# Used to apply richer filter combinations when the rank-by is on the
# offensive / fantasy side — offense trivia leans harder on team /
# award / rookie qualifiers since the answer space is broader and a
# bare "top 10 pass_yds" question is too easy.
_OFFENSE_AND_FANTASY_RANK_BY: frozenset[str] = frozenset({
    "pass_yds", "pass_td", "pass_cmp", "pass_rating",
    "rush_yds", "rush_td", "rush_att",
    "rec_yds", "rec", "rec_td", "targets",
    "fpts_ppr", "fpts_half", "fpts_std",
})
# Stats with restricted era support — random picks for these add a
# matching --start so we don't return empty answer sets.
_STAT_MIN_SEASON: dict[str, int] = {
    "def_sacks":            1982,
    "def_tackles_combined": 1994,
    "def_tackles_solo":     1994,
    "def_tackles_assist":   1994,
    "targets":              1992,
}

# Position pool per stat. Random generator picks rank_by first, then
# samples a position from the corresponding list — list duplication is
# how we weight (e.g. QB appears 5x for pass_yds so QBs are picked 5/6
# of the time, with ALL the rare crossover). The cross-position picks
# (RB throwing for yardage, QB making receptions) are kept on the menu
# at low frequency for trivia novelty.
_STAT_COMPATIBLE_POSITIONS: dict[str, list[str]] = {
    "pass_yds":              ["QB"] * 5 + ["ALL"],
    "pass_td":               ["QB"] * 5 + ["ALL"],
    "pass_cmp":              ["QB"] * 5 + ["ALL"],
    "pass_rating":           ["QB"],
    "pass_cmp_pct":          ["QB"],
    "rush_yds":              ["RB"] * 4 + ["FLEX", "QB", "ALL", "WR"],
    "rush_td":               ["RB"] * 4 + ["FLEX", "QB", "ALL", "WR"],
    "rush_att":              ["RB"] * 5 + ["FLEX", "QB", "ALL"],
    "rec_yds":               ["WR"] * 3 + ["TE", "RB", "FLEX", "ALL"],
    "rec":                   ["WR"] * 3 + ["TE", "RB", "FLEX", "ALL"],
    "rec_td":                ["WR"] * 2 + ["TE", "RB", "FLEX", "ALL"],
    "targets":               ["WR"] * 2 + ["TE", "RB", "FLEX", "ALL"],
    "catch_rate":            ["WR"] * 2 + ["TE", "RB", "FLEX", "ALL"],
    "fpts_ppr":              ["FLEX"] * 2 + ["ALL"] * 2 + ["QB", "RB", "WR", "TE"],
    "fpts_half":             ["FLEX"] * 2 + ["ALL"] * 2 + ["QB", "RB", "WR", "TE"],
    "fpts_std":              ["FLEX"] * 2 + ["ALL"] * 2 + ["QB", "RB", "WR", "TE"],
    # Defense — alias groups (DL / LB / DB / SAFETY) cover the
    # natural specialties for each stat: sacks come from DL + LB
    # primarily, INTs from DBs + LBs, tackles from everyone with LB
    # heaviest. ALL stays as the broadest pick.
    "def_sacks":             ["ALL"] * 4 + ["DL"] * 3 + ["LB"] * 2,
    "def_int":               ["ALL"] * 3 + ["DB"] * 3 + ["SAFETY"] * 2 + ["LB"],
    "def_int_td":            ["ALL"] * 3 + ["DB"] * 2 + ["SAFETY"] + ["LB"],
    "def_pass_def":          ["ALL"] * 3 + ["DB"] * 3 + ["SAFETY"] * 2 + ["LB"],
    "def_tackles_combined":  ["ALL"] * 3 + ["LB"] * 2 + ["DL", "DB", "SAFETY"],
    # Kicking — K (and P for punts, if/when added).
    "fg_made":               ["K"],
    "fg_long":               ["K"],
    "xp_made":               ["K"],
    # Returns — return jobs go to RBs/WRs/DBs alike, so ALL.
    "kr_yds":                ["ALL"],
    "kr_td":                 ["ALL"],
    "pr_yds":                ["ALL"],
    "pr_td":                 ["ALL"],
}

_RANDOM_TEAMS: list[str] = [
    "DAL", "PIT", "GB", "SF", "KAN", "NE", "DEN", "PHI",
    "NYG", "CHI", "MIN", "CLE", "BAL", "BUF", "MIA", "TEN",
    "IND", "JAX", "HOU", "CIN", "ATL", "CAR", "NO", "TB",
    "DET", "ARI", "LAR", "SEA", "WAS", "NYJ", "LAC", "LV",
]
_RANDOM_DIVISIONS: list[str] = [
    "AFC East", "AFC North", "AFC South", "AFC West",
    "NFC East", "NFC North", "NFC South", "NFC West",
    "NFC Central",  # historical (pre-2002)
]
_RANDOM_CONFERENCES: list[str] = ["AFC", "NFC"]
_RANDOM_AWARDS: list[str] = [
    "MVP", "OPOY", "DPOY", "OROY", "DROY", "CPOY",
    "PB", "AP_FIRST", "AP_SECOND", "HOF",
]
_RANDOM_N_CHOICES: list[int] = [5, 5, 10, 10, 10, 15]

# Draft-round buckets sampled by the random generator. Each entry is
# a list[int|str] in the format pos_topN expects.
_RANDOM_DRAFT_ROUNDS: list[list] = [
    [1],
    [2, 3],
    [4, 5, 6, 7],
    ["undrafted"],
]

# Companion-stat thresholds keyed by the rank-by stat. Each entry is a
# list of (stat, threshold_value) pairs to sample from when the random
# gen wants to add a min_stat or max_stat constraint. Thresholds are
# tuned so the resulting answer set is usually non-empty across the
# 1970-2025 build but the filter still meaningfully narrows the pool.
_COMPANION_MIN_STAT_FOR: dict[str, list[tuple[str, float]]] = {
    "pass_yds":     [("pass_td", 25), ("rush_yds", 200), ("pass_cmp", 300)],
    "pass_td":      [("pass_yds", 4000), ("rush_yds", 200)],
    "pass_cmp":     [("pass_yds", 3500), ("pass_td", 20)],
    "pass_cmp_pct": [("pass_att", 100), ("pass_td", 15)],
    "pass_rating":  [("pass_att", 200), ("pass_td", 15)],
    "rush_yds":     [("rush_td", 8), ("rec_yds", 200), ("rush_att", 200)],
    "rush_td":      [("rush_yds", 800), ("rush_att", 150)],
    "rush_att":     [("rush_yds", 800), ("rush_td", 5)],
    "rec_yds":      [("rec_td", 8), ("rec", 60), ("targets", 80)],
    "rec":          [("rec_yds", 800), ("rec_td", 5)],
    "rec_td":       [("rec_yds", 600), ("rec", 40)],
    "targets":      [("rec", 50), ("rec_yds", 600)],
    "catch_rate":   [("targets", 30), ("rec", 30), ("rec_yds", 400)],
    "fpts_ppr":     [("games", 12), ("rec", 40)],
    "fpts_half":    [("games", 12), ("rush_att", 100)],
    "fpts_std":     [("games", 12), ("rush_td", 5)],
    "def_sacks":    [("games", 12)],
    "def_int":      [("games", 12)],
    "def_tackles_combined": [("games", 12)],
    "fg_made":      [("fg_att", 20)],
    "kr_yds":       [("kr", 20)],
    "pr_yds":       [("pr", 20)],
}
_COMPANION_MAX_STAT_FOR: dict[str, list[tuple[str, float]]] = {
    "rush_att":     [("rush_yds", 999)],   # high-volume, low-yardage
    "pass_att":     [("pass_yds", 3000)],  # bad-but-busy QBs
    "targets":      [("rec_yds", 700)],    # lots of looks, few yards
    "fpts_ppr":     [("games", 13)],       # short-season top-N
    "fpts_half":    [("games", 13)],
    "fpts_std":     [("games", 13)],
}

# Initials sampled for the last-name letter filter (and now first
# names too — same list since both filters are substring matches).
_LAST_NAME_INITIALS: list[str] = list("ABCDEFGHIJKLMNOPRSTW")

# Broad position pool sampled when the random gen picks position
# without a user pin. ALL is weighted heaviest (broadest answer
# space), then the alias groups + skill positions, then niche
# concrete labels. The user described the desired default as "any"
# position — this list realizes that.
_RANDOM_POSITIONS_ANY: list[str] = [
    "ALL", "ALL", "ALL", "ALL",                     # ~30% weight on ALL
    "FLEX", "FLEX",                                 # ~14% RB+WR+TE
    "QB", "RB", "WR", "TE",                         # ~7% each
    "SAFETY", "DB", "LB", "DL",                     # defensive groups
    "K",                                            # kickers (low share)
]

# Colleges sampled when the random gen rolls a `--college` pin.
# Curated list of programs with deep NFL representation so a random
# pick almost always returns a non-empty answer set.
_RANDOM_COLLEGES: list[str] = [
    "Alabama", "Ohio State", "Michigan", "USC", "Oklahoma",
    "Texas", "LSU", "Florida", "Notre Dame", "Penn State",
    "Georgia", "Auburn", "Miami", "Florida State", "UCLA",
    "Tennessee", "Nebraska", "Wisconsin", "Iowa", "Pittsburgh",
    "Stanford", "Clemson", "Texas A&M", "Oregon", "Washington",
    "Arkansas", "Mississippi", "Mississippi State", "South Carolina",
]

# Career-scale companion thresholds. Used in BOTH season and career
# modes — a single-season ranking restricted by a career floor is
# fun trivia ("top 10 single-season pass_yds among QBs with 50000+
# career pass_yds"). Same shape as _COMPANION_MIN_STAT_FOR but with
# career-totals values.
_COMPANION_MIN_CAREER_STAT_FOR: dict[str, list[tuple[str, float]]] = {
    "pass_yds":     [("pass_yds", 30000), ("pass_td", 200), ("games", 100)],
    "pass_td":     [("pass_yds", 30000), ("pass_td", 200)],
    "pass_cmp":     [("pass_yds", 25000), ("games", 80)],
    "pass_rating":  [("pass_att", 1500)],
    "pass_cmp_pct": [("pass_att", 1500)],
    "rush_yds":     [("rush_yds", 5000), ("rush_td", 30), ("games", 80)],
    "rush_td":      [("rush_yds", 5000), ("rush_td", 30)],
    "rush_att":     [("rush_yds", 5000), ("games", 80)],
    "rec_yds":      [("rec_yds", 5000), ("rec_td", 30), ("games", 80)],
    "rec":          [("rec", 300), ("rec_yds", 4000)],
    "rec_td":       [("rec_yds", 4000), ("rec_td", 30)],
    "targets":      [("rec", 300), ("games", 80)],
    "catch_rate":   [("targets", 200)],
    "fpts_ppr":     [("games", 80)],
    "fpts_half":    [("games", 80)],
    "fpts_std":     [("games", 80)],
    "def_sacks":    [("def_sacks", 30), ("games", 100)],
    "def_int":      [("def_int", 20), ("games", 100)],
    "def_int_td":   [("def_int", 15)],
    "def_pass_def": [("games", 100)],
    "def_tackles_combined": [("games", 100)],
    "fg_made":      [("fg_att", 100)],
    "fg_long":      [("fg_made", 50)],
    "kr_yds":       [("kr", 50)],
    "pr_yds":       [("pr", 50)],
}
_COMPANION_MAX_CAREER_STAT_FOR: dict[str, list[tuple[str, float]]] = {
    # "Modest career, big single season" — contrast trivia.
    "rush_yds":     [("games", 100)],
    "pass_yds":     [("pass_int", 100)],
    "rec_yds":      [("rec_yds", 9999)],
    "fpts_ppr":     [("games", 100)],
    "def_sacks":    [("def_sacks", 50)],
    "def_int":      [("def_int", 30)],
}


def _random_trivia_template(
    rng,
    overrides: dict | None = None,
    *,
    teammate_pool: list[tuple[str, str]] | None = None,
) -> dict:
    """Build a fresh template by sampling each filter dimension.

    Picks at most one of {team, division, conference} so geo filters
    don't compound into empty sets, and at most one award filter
    (has_award OR ever_won). Year range, rookie_only, and uniqueness
    are independent randomized toggles.

    ``overrides`` (passed by ``trivia random`` when the user supplies
    flags) pins individual dimensions: any key present in overrides
    skips the corresponding random pick and uses the user value.
    Dimensions not in overrides stay random. Empty dict / None means
    fully random (the default behavior).

    ``teammate_pool`` is an optional list of (player_id, name) pairs
    eligible to anchor a random teammate-of pin. The caller (typically
    ``_pick_non_empty_template``) pre-fetches it from the DB so the
    retry loop doesn't re-query. None / empty disables the random
    teammate-of roll. The pool is meant to enforce a quality bar on
    the anchor (e.g. only players with at least one award row) so
    random teammate-of games don't pin a no-name nobody's heard of.
    """
    overrides = overrides or {}

    # rank_by — pinned or random. Era floor still applies to whatever
    # we end up with so def_sacks pre-1982 doesn't sneak through.
    rank_by = overrides.get("rank_by") or rng.choice(_RANDOM_RANK_BY)

    # Mode — 25% career-totals trivia, 75% single-season. Pinning via
    # ``--mode career`` (or season) skips the roll. Career mode is
    # implicitly unique-by-player so the unique flag is ignored when
    # mode == career.
    #
    # If the user pinned any single-season-only filter (team /
    # division / conference / has_award / rookie_only / min_stats /
    # max_stats / draft_start / draft_end / tiebreak_by) we force
    # season mode — career_topN doesn't accept those filters and
    # silently dropping a user pin would be confusing. Explicit
    # ``--mode career`` still wins over this auto-fallback.
    _SEASON_ONLY = (
        "team", "division", "conference", "has_award", "rookie_only",
        "min_stats", "max_stats", "draft_start", "draft_end",
        "tiebreak_by",
    )
    if "mode" in overrides:
        mode = overrides["mode"]
    elif any(overrides.get(k) for k in _SEASON_ONLY):
        mode = "season"
    else:
        mode = "career" if rng.random() < 0.25 else "season"

    # Position — when not pinned, pick from a broad pool that mixes
    # ALL, alias groups (FLEX / SAFETY / DB / LB / DL), and concrete
    # labels. ALL carries the most weight so unfiltered games still
    # come up most often, but specific positions appear regularly so
    # games vary. Stat-compat is intentionally NOT used here: a
    # random "top 10 RB by pass_yds" or "K by rec_yds" produces
    # near-empty answer sets that the quality gate re-rolls — that's
    # fine; the user explicitly asked for any-position random.
    if overrides.get("position"):
        position = overrides["position"]
    else:
        position = rng.choice(_RANDOM_POSITIONS_ANY)

    spec: dict = {
        "rank_by":  rank_by,
        "n":        overrides.get("n") or rng.choice(_RANDOM_N_CHOICES),
        "position": position,
        "mode":     mode,
        "unique": (
            overrides["unique"] if "unique" in overrides
            else (True if mode == "career"
                  else rng.choice([True, True, False]))
        ),
    }

    # Filter intensity: offense / fantasy get richer qualifiers (huge
    # candidate pools, bare top-N questions are too easy); defense /
    # special-teams keep modest probabilities so we don't pile filters
    # onto an already-narrow pool.
    is_off  = rank_by in _OFFENSE_AND_FANTASY_RANK_BY
    # Pin-count distribution is centered around 3-4 active filters.
    # High-impact rolls (year / geo / award) keep their high
    # probability — they pull the floor up to ~1 pin so every game
    # has *something*. Low-impact rolls (drafted_by / college /
    # max_career / first-name initial) are bumped above their old
    # vestigial 3-6% range so they show up enough to feel like real
    # dimensions of the random gen.
    p_year      = 0.80 if is_off else 0.65
    p_geo       = 0.55 if is_off else 0.45
    p_award     = 0.45 if is_off else 0.35
    p_rookie    = 0.20 if is_off else 0.15
    p_draft_rd  = 0.20 if is_off else 0.15
    p_min_stat  = 0.30 if is_off else 0.15
    p_max_stat  = 0.15 if is_off else 0.05
    p_initial   = 0.15 if is_off else 0.10
    p_drafted_by    = 0.13 if is_off else 0.10
    p_draft_year    = 0.15 if is_off else 0.10
    p_first_initial = 0.13 if is_off else 0.10
    p_college       = 0.13 if is_off else 0.08
    p_min_career    = 0.18 if is_off else 0.10
    p_max_career    = 0.10 if is_off else 0.05
    # Teammate-of: kept low because it's a strong, narrow filter (the
    # anchor's career arc dominates the eligible pool). Pool eligibility
    # is gated upstream — only players with at least one award row are
    # candidates — so when it does roll, the anchor is recognizable.
    p_teammate      = 0.06 if is_off else 0.04

    # Year range — pin if user gave start and/or end, else maybe random.
    min_floor = _STAT_MIN_SEASON.get(rank_by, 1970)
    pinned_start = overrides.get("start")
    pinned_end   = overrides.get("end")
    if pinned_start is not None or pinned_end is not None:
        if pinned_start is not None:
            spec["start"] = max(pinned_start, min_floor)
        elif min_floor > 1970:
            spec["start"] = min_floor
        if pinned_end is not None:
            spec["end"] = pinned_end
    elif rng.random() < p_year:
        start = rng.randint(min_floor, 2018)
        end   = rng.randint(start, 2024)
        spec["start"] = start
        spec["end"]   = end
    else:
        # No random year range — still respect the era floor.
        if min_floor > 1970:
            spec["start"] = min_floor

    if mode == "season":
        # ----- Season-mode (pos_topN) filter generation -----

        # Geo: pinned (any of team/division/conference) or random one.
        if overrides.get("team"):
            spec["team"] = overrides["team"]
        elif overrides.get("division"):
            spec["division"] = overrides["division"]
        elif overrides.get("conference"):
            spec["conference"] = overrides["conference"]
        else:
            geo = rng.random()
            if geo < p_geo / 3:
                spec["team"] = rng.choice(_RANDOM_TEAMS)
            elif geo < 2 * p_geo / 3:
                spec["division"] = rng.choice(_RANDOM_DIVISIONS)
            elif geo < p_geo:
                spec["conference"] = rng.choice(_RANDOM_CONFERENCES)

        # Award filter — has_award (single season) or ever_won.
        if overrides.get("has_award"):
            spec["has_award"] = list(overrides["has_award"])
        elif overrides.get("ever_won_award"):
            spec["ever_won_award"] = list(overrides["ever_won_award"])
        else:
            aw = rng.random()
            if aw < p_award * 0.4:
                spec["has_award"] = [rng.choice(_RANDOM_AWARDS)]
            elif aw < p_award:
                spec["ever_won_award"] = [rng.choice(_RANDOM_AWARDS)]

        if overrides.get("rookie_only"):
            spec["rookie_only"] = True
        elif rng.random() < p_rookie:
            spec["rookie_only"] = True

        if overrides.get("draft_rounds"):
            spec["draft_rounds"] = list(overrides["draft_rounds"])
        elif rng.random() < p_draft_rd:
            spec["draft_rounds"] = list(rng.choice(_RANDOM_DRAFT_ROUNDS))

        # min_stat / max_stat — ratio stats always get a denominator
        # floor on top of any pinned min_stats.
        pinned_min = dict(overrides.get("min_stats") or {})
        pinned_max = dict(overrides.get("max_stats") or {})
        if rank_by == "pass_cmp_pct" and "pass_att" not in pinned_min:
            pinned_min["pass_att"] = 100
        elif rank_by == "catch_rate" and "targets" not in pinned_min:
            pinned_min["targets"] = 30
        if pinned_min:
            spec["min_stats"] = pinned_min
        if pinned_max:
            spec["max_stats"] = pinned_max
        if not overrides.get("min_stats") and rng.random() < p_min_stat:
            candidates = _COMPANION_MIN_STAT_FOR.get(rank_by, [])
            if candidates:
                stat, value = rng.choice(candidates)
                spec.setdefault("min_stats", {})[stat] = value
        if not overrides.get("max_stats") and rng.random() < p_max_stat:
            candidates = _COMPANION_MAX_STAT_FOR.get(rank_by, [])
            if candidates:
                stat, value = rng.choice(candidates)
                spec.setdefault("max_stats", {})[stat] = value

        if overrides.get("last_name_contains"):
            spec["last_name_contains"] = overrides["last_name_contains"]
        elif rng.random() < p_initial:
            spec["last_name_contains"] = rng.choice(_LAST_NAME_INITIALS)

        # First-name initial — symmetrical to last-name. Independent
        # roll so both can apply (rare but possible).
        if overrides.get("first_name_contains"):
            spec["first_name_contains"] = overrides["first_name_contains"]
        elif rng.random() < p_first_initial:
            spec["first_name_contains"] = rng.choice(_LAST_NAME_INITIALS)

        # Career-stat thresholds — random rolls apply to season mode
        # too: "top single-season pass_yds for QBs with 50000+
        # career pass_yds" is reasonable trivia. Pinned overrides
        # win.
        if overrides.get("min_career_stats"):
            spec["min_career_stats"] = dict(overrides["min_career_stats"])
        elif rng.random() < p_min_career:
            cands = _COMPANION_MIN_CAREER_STAT_FOR.get(rank_by, [])
            if cands:
                stat, value = rng.choice(cands)
                spec.setdefault("min_career_stats", {})[stat] = value
        if overrides.get("max_career_stats"):
            spec["max_career_stats"] = dict(overrides["max_career_stats"])
        elif rng.random() < p_max_career:
            cands = _COMPANION_MAX_CAREER_STAT_FOR.get(rank_by, [])
            if cands:
                stat, value = rng.choice(cands)
                spec.setdefault("max_career_stats", {})[stat] = value

        # tiebreak_by — passthrough only (it's a sort order, not a
        # limiter, so rolling it randomly doesn't add trivia value).
        if overrides.get("tiebreak_by"):
            spec["tiebreak_by"] = overrides["tiebreak_by"]

    else:
        # ----- Career-mode (career_topN) filter generation -----
        #
        # career_topN takes a smaller set of filters than pos_topN.
        # Filters that don't apply (team / division / conference /
        # has_award (single season) / rookie_only / draft_rounds /
        # drafted_by / first_name_contains / last_name_contains /
        # min_stats / max_stats) are dropped; user pins for them are
        # ignored with a passthrough on `min_stats` only when it
        # matches an auto-floor for ratio stats.

        # ever_won_award — career-applicable. has_award user-pin is
        # converted (it's the closest analogue in career mode).
        if overrides.get("ever_won_award"):
            spec["ever_won_award"] = list(overrides["ever_won_award"])
        elif overrides.get("has_award"):
            spec["ever_won_award"] = list(overrides["has_award"])
        elif rng.random() < p_award:
            spec["ever_won_award"] = [rng.choice(_RANDOM_AWARDS)]

        # min_career_stats — ratio stats need a higher floor (career
        # totals across many seasons), pinned values win. The random
        # roll below adds a companion threshold on top of that.
        pinned_min_career = dict(overrides.get("min_career_stats") or {})
        if rank_by == "pass_cmp_pct" and "pass_att" not in pinned_min_career:
            pinned_min_career["pass_att"] = 200
        elif rank_by == "catch_rate" and "targets" not in pinned_min_career:
            pinned_min_career["targets"] = 100
        if pinned_min_career:
            spec["min_career_stats"] = pinned_min_career
        if not overrides.get("min_career_stats") and rng.random() < p_min_career:
            cands = _COMPANION_MIN_CAREER_STAT_FOR.get(rank_by, [])
            if cands:
                stat, value = rng.choice(cands)
                spec.setdefault("min_career_stats", {})[stat] = value

        if overrides.get("max_career_stats"):
            spec["max_career_stats"] = dict(overrides["max_career_stats"])
        elif rng.random() < p_max_career:
            cands = _COMPANION_MAX_CAREER_STAT_FOR.get(rank_by, [])
            if cands:
                stat, value = rng.choice(cands)
                spec.setdefault("max_career_stats", {})[stat] = value

        # min_seasons floor random toggle — keeps career leaderboards
        # from being dominated by 1-game wonders.
        if rng.random() < (0.30 if is_off else 0.20):
            spec["min_seasons"] = rng.choice([3, 5, 8])

        # Draft-round bucket. Player attribute, composes fine with
        # career SUMs.
        if overrides.get("draft_rounds"):
            spec["draft_rounds"] = list(overrides["draft_rounds"])
        elif rng.random() < p_draft_rd:
            spec["draft_rounds"] = list(rng.choice(_RANDOM_DRAFT_ROUNDS))

        # Last-name initial — same random toggle as season mode.
        if overrides.get("last_name_contains"):
            spec["last_name_contains"] = overrides["last_name_contains"]
        elif rng.random() < p_initial:
            spec["last_name_contains"] = rng.choice(_LAST_NAME_INITIALS)

        # First-name initial — random roll, symmetric to last-name.
        if overrides.get("first_name_contains"):
            spec["first_name_contains"] = overrides["first_name_contains"]
        elif rng.random() < p_first_initial:
            spec["first_name_contains"] = rng.choice(_LAST_NAME_INITIALS)

    # ----- Shared (both modes) random rolls -----
    # college / drafted_by / draft_start / draft_end apply to both
    # season and career queries — handled here once so we don't
    # branch-duplicate.

    if overrides.get("college"):
        spec["college"] = overrides["college"]
    elif rng.random() < p_college:
        spec["college"] = rng.choice(_RANDOM_COLLEGES)

    if overrides.get("draft_start") is not None or overrides.get("draft_end") is not None:
        if overrides.get("draft_start") is not None:
            spec["draft_start"] = overrides["draft_start"]
        if overrides.get("draft_end") is not None:
            spec["draft_end"] = overrides["draft_end"]
    elif rng.random() < p_draft_year:
        ds = rng.randint(1970, 2010)
        de = rng.randint(ds, 2024)
        spec["draft_start"] = ds
        spec["draft_end"]   = de

    if overrides.get("drafted_by"):
        spec["drafted_by"] = overrides["drafted_by"]
    elif rng.random() < p_drafted_by:
        spec["drafted_by"] = rng.choice(_RANDOM_TEAMS)

    # Teammate-of: pin survives if user supplied one; otherwise we
    # may roll a random anchor from ``teammate_pool``. The pool is
    # restricted to players with at least one award row (cheapest
    # "recognizable name" gate the user asked for), so random
    # teammate-of games never anchor on someone nobody's heard of.
    # ``teammate_of_name`` rides alongside for the title builder; the
    # player_id is what goes to the SQL filter.
    if overrides.get("teammate_of_player_id"):
        spec["teammate_of_player_id"] = overrides["teammate_of_player_id"]
        if overrides.get("teammate_of_name"):
            spec["teammate_of_name"] = overrides["teammate_of_name"]
    elif teammate_pool and rng.random() < p_teammate:
        pid, name = rng.choice(teammate_pool)
        spec["teammate_of_player_id"] = pid
        spec["teammate_of_name"] = name

    # Soft cap on pin count. With 14 independent rolls some games
    # accumulate 7+ filters and end up nearly impossible. Drop random
    # NON-PINNED dimensions until we're at the cap. User-pinned
    # filters from `overrides` are sacrosanct — never dropped.
    _trim_to_max_pins(spec, overrides, rng, max_pins=6)

    return spec


# Dimensions safe to drop when trimming over-stuffed templates.
# Excludes the always-set core (rank_by / n / position / mode /
# unique) and tiebreak (sort order, not a filter).
_TRIMMABLE_KEYS: tuple[str, ...] = (
    "team", "division", "conference",
    "has_award", "ever_won_award",
    "rookie_only", "draft_rounds",
    "min_stats", "max_stats",
    "min_career_stats", "max_career_stats",
    "first_name_contains", "last_name_contains",
    "college", "drafted_by",
    "draft_start", "draft_end",
    "min_seasons",
    "start", "end",
    "teammate_of_player_id",
)


def _trim_to_max_pins(
    spec: dict, overrides: dict, rng, *, max_pins: int,
) -> None:
    """Drop random non-user-pinned dimensions until the pin count
    is <= max_pins. Mutates ``spec`` in place."""
    pinned_keys = set(overrides or {})
    while True:
        active = [k for k in _TRIMMABLE_KEYS if k in spec]
        if len(active) <= max_pins:
            return
        droppable = [k for k in active if k not in pinned_keys]
        if not droppable:
            # Every active dimension was user-pinned. Respect the
            # user's pins and stop trimming.
            return
        # draft_start/draft_end roll together — drop both as a pair.
        # teammate_of_player_id rides with teammate_of_name (display
        # only) — drop the name when the id goes.
        victim = rng.choice(droppable)
        spec.pop(victim, None)
        if victim in ("draft_start", "draft_end"):
            spec.pop("draft_start", None)
            spec.pop("draft_end", None)
        elif victim == "teammate_of_player_id":
            spec.pop("teammate_of_name", None)


_CAREER_TOPN_KEYS = {
    "start", "end", "ever_won_award", "min_seasons", "college",
    "min_career_stats", "max_career_stats",
    # Player-attribute filters that compose with career queries.
    "draft_rounds", "drafted_by",
    "draft_start", "draft_end",
    "first_name_contains", "last_name_contains",
    "teammate_of_player_id",
}


def _resolve_template(con: duckdb.DuckDBPyConnection, template: dict):
    """Run the appropriate top-N helper for a template, return
    (answers, n, rank_by, position).

    Branches on ``template['mode']``:
      - ``"season"`` (default) → pos_topN, single-season rows.
      - ``"career"`` → career_topN, player-level rows. Returned dicts
        are normalized to the same shape as season rows (name, team,
        season, position, rank_value, ...) so the trivia loop's
        accessors are mode-agnostic.

    Returns ``answers=None`` on SQL failure (rare — happens when a
    user-pinned filter combo is genuinely impossible, e.g.
    rookie_only with a draft round that excludes everyone)."""
    args = dict(template)
    rank_by = args.pop("rank_by")
    n = args.pop("n")
    position = args.pop("position", "ALL")
    mode = args.pop("mode", "season")
    args.pop("unique", None) if mode == "career" else None
    try:
        if mode == "career":
            career_args = {
                k: v for k, v in args.items() if k in _CAREER_TOPN_KEYS
            }
            sql, params = career_topN(
                rank_by, n=n, position=position, **career_args
            )
            cur = con.execute(sql, params)
            cols = [d[0] for d in cur.description]
            raw_rows = cur.fetchall()
            answers = [_normalize_career_row(dict(zip(cols, r))) for r in raw_rows]
        else:
            sql, params = pos_topN(position, n=n, rank_by=rank_by, **args)
            cur = con.execute(sql, params)
            cols = [d[0] for d in cur.description]
            answers = [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception:
        return None, n, rank_by, position
    return answers, n, rank_by, position


def _normalize_career_row(row: dict) -> dict:
    """Map a career_topN row dict (name / positions / teams /
    career_total / seasons / first_season / last_season) onto the
    season-row shape the trivia loop expects (name / team / season /
    position / rank_value / draft_*). Career rows lose draft info
    (career_topN doesn't carry it) and gain a synthesized
    ``first-last`` season string."""
    fs, ls = row.get("first_season"), row.get("last_season")
    season = f"{fs}-{ls}" if fs is not None and ls is not None else "?"
    return {
        "name":               row.get("name"),
        "team":               row.get("teams") or "?",
        "season":             season,
        "position":           row.get("positions"),
        "rank_value":         row.get("career_total"),
        "draft_round":        None,
        "draft_year":         None,
        "draft_overall_pick": None,
        "college":            None,
    }


def _is_quality_answer_set(answers: list[dict] | None, n: int) -> bool:
    """A trivia answer set is "good" only if every spot is filled with
    a meaningful (non-zero) rank value:

      - len(answers) >= n  (we asked for top N; need at least N rows)
      - every row's rank_value > 0  (a 0-rush_yds RB shouldn't be on a
        rushing leaderboard; it means the filter set is broader than
        the actual eligible pool and we backfilled with zeros)

    Both checks short-circuit the random gen's retry loop so we
    re-roll until we find a satisfying template."""
    if not answers or len(answers) < n:
        return False
    for row in answers:
        v = row.get("rank_value")
        if v is None:
            return False
        try:
            if float(v) <= 0:
                return False
        except (TypeError, ValueError):
            return False
    return True


def _eligible_teammate_pool(
    con: duckdb.DuckDBPyConnection,
) -> list[tuple[str, str]]:
    """Players eligible to anchor a random teammate-of pin.

    The quality bar is "appears at least once in player_awards" — the
    cheapest "recognizable name" gate available (single SELECT
    DISTINCT against a small table, ~5-10k rows on a full build). Pro
    Bowls qualify, so the pool is wide enough that random anchors
    aren't repetitive but narrow enough that nobodies are excluded.
    Returns ``(player_id, name)`` tuples; empty if the awards table
    isn't populated yet (early build, fallback degrades gracefully)."""
    try:
        rows = con.execute(
            "SELECT DISTINCT p.player_id, p.name "
            "FROM player_awards pa JOIN players p USING (player_id) "
            "WHERE p.name IS NOT NULL"
        ).fetchall()
    except Exception:
        return []
    return [(pid, name) for pid, name in rows]


def _pick_non_empty_template(
    con: duckdb.DuckDBPyConnection,
    rng,
    *,
    max_attempts: int = 25,
    overrides: dict | None = None,
) -> tuple[dict, list[dict], int, str, str]:
    """Sample random templates until one yields a quality answer set
    (>= N rows, all with positive rank_value), up to ``max_attempts``.
    ``overrides`` (if given) pins user-supplied dimensions for every
    attempt; the rest stay random.

    Falls back to a minimum-filter template if every attempt failed
    the quality check — the fallback also respects user overrides so
    a too-restrictive pin surfaces the empty/short result rather than
    silently being ignored."""
    # Pre-fetch the teammate-of anchor pool once: the retry loop runs
    # up to 25x and the underlying query is constant within a session.
    teammate_pool = _eligible_teammate_pool(con)
    for _ in range(max_attempts):
        template = _random_trivia_template(
            rng, overrides, teammate_pool=teammate_pool,
        )
        answers, n, rank_by, position = _resolve_template(con, template)
        if _is_quality_answer_set(answers, n):
            return template, answers, n, rank_by, position
    # Fallback: minimum filters but keep user pins. If overrides
    # pinned a rank_by / position / n / unique those override the
    # defaults; otherwise the fallback is "top-10 fpts_ppr, ALL,
    # unique" — safe across any DB state.
    overrides = overrides or {}
    fallback = {
        "rank_by":  overrides.get("rank_by")  or "fpts_ppr",
        "n":        overrides.get("n")        or 10,
        "position": overrides.get("position") or "ALL",
        "mode":     overrides.get("mode")     or "season",
        "unique":   overrides["unique"] if "unique" in overrides else True,
    }
    for key in (
        "start", "end", "team", "division", "conference",
        "first_name_contains", "last_name_contains",
        "has_award", "ever_won_award", "rookie_only",
        "draft_rounds", "draft_start", "draft_end", "drafted_by",
        "min_stats", "max_stats", "college",
        "min_career_stats", "max_career_stats", "tiebreak_by",
        "teammate_of_player_id", "teammate_of_name",
    ):
        if overrides.get(key):
            fallback[key] = overrides[key]
    answers, n, rank_by, position = _resolve_template(con, fallback)
    return fallback, answers or [], n, rank_by, position


def _run_template(
    template: dict, answers: list[dict],
    n: int, rank_by: str, position: str,
    *, label: str, history_dir: Path | None = None, save: bool = True,
) -> None:
    """Common end-of-pipeline: print label, build title from the
    resolved template args, run the REPL.

    ``save=True`` (default) persists the resolved template to
    ``history_dir`` (defaulting to data/trivia_history/ when None) so
    the game can be replayed via ``trivia replay <id>``. Replay itself
    sets save=False so the history isn't doubled when re-running an
    old game."""
    if save:
        from ffpts.trivia_replay import save_spec, DEFAULT_HISTORY_DIR

        # Re-attach the runtime fields so the saved spec is a
        # complete, self-contained template (replay rebuilds the game
        # from this dict alone).
        full_template = dict(template)
        full_template.setdefault("rank_by",  rank_by)
        full_template.setdefault("n",        n)
        full_template.setdefault("position", position)
        game_id = save_spec(
            full_template,
            label=label,
            history_dir=history_dir or DEFAULT_HISTORY_DIR,
        )
        typer.echo(f"(game {game_id} — replay with `fb_info trivia replay {game_id}`)")
    args = dict(template)
    args.pop("rank_by", None)
    args.pop("n", None)
    args.pop("position", None)

    title = _build_trivia_title(
        n=n, rank_by=rank_by, position=position,
        start=args.get("start"), end=args.get("end"),
        team=args.get("team"), division=args.get("division"),
        conference=args.get("conference"),
        first_name_contains=args.get("first_name_contains"),
        last_name_contains=args.get("last_name_contains"),
        has_award=args.get("has_award"),
        ever_won=args.get("ever_won_award"),
        rookie_only=args.get("rookie_only", False),
        draft_start=args.get("draft_start"),
        draft_end=args.get("draft_end"),
        drafted_by=args.get("drafted_by"),
        draft_rounds=args.get("draft_rounds"),
        min_stats=args.get("min_stats"),
        max_stats=args.get("max_stats"),
        unique=args.get("unique", True),
        mode=args.get("mode", "season"),
        college=args.get("college"),
        min_career_stats=args.get("min_career_stats"),
        max_career_stats=args.get("max_career_stats"),
        teammate_of_name=args.get("teammate_of_name"),
    )
    typer.echo(f"({label})")
    _run_trivia_loop(answers, rank_by=rank_by, title=title)


@trivia_app.command("daily")
def trivia_daily(
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
) -> None:
    """Same trivia game for everyone today (deterministic by date).

    Routes through the exact same generator as ``trivia random`` —
    today's date becomes the RNG seed, the random template generation
    runs over the full distribution (career mode included, same
    weights, same filter pools). The only difference between this and
    ``trivia random`` is the seed.
    """
    import datetime
    import random

    seed = int(datetime.date.today().isoformat().replace("-", ""))
    rng = random.Random(seed)
    con = _open_db(db)
    try:
        template, answers, n, rank_by, position = _pick_non_empty_template(con, rng)
    finally:
        con.close()
    if not answers:
        typer.echo("No matching player-seasons for today's template.")
        raise typer.Exit(code=0)
    _run_template(
        template, answers, n, rank_by, position,
        label=f"daily for {datetime.date.today()}",
        history_dir=_history_dir_for_db(db),
    )


@trivia_app.command("random")
def trivia_random(
    # User-pin flags. Same shape as `trivia play`. Anything passed here
    # becomes a hard constraint; remaining dimensions stay random.
    rank_by: str | None = typer.Option(
        None, "--rank-by",
        help="Pin the rank-by stat. e.g. --rank-by rush_yds for a "
             "rushing-only random game.",
    ),
    n: int | None = typer.Option(None, "--n", help="Pin the top-N count."),
    position: str | None = typer.Option(
        None, "--position",
        help="Pin the position (or alias like FLEX / SAFETY / DB / LB / "
             "DL / ALL).",
    ),
    start: int | None = typer.Option(None, "--start", help="Pin season range start."),
    end:   int | None = typer.Option(None, "--end",   help="Pin season range end."),
    team: str | None = typer.Option(None, "--team", help="Pin a team."),
    division: str | None = typer.Option(None, "--division"),
    conference: str | None = typer.Option(None, "--conference"),
    first_name_contains: str | None = typer.Option(None, "--first-name-contains"),
    last_name_contains: str | None = typer.Option(None, "--last-name-contains"),
    has_award: list[str] | None = typer.Option(None, "--has-award"),
    ever_won: list[str] | None = typer.Option(None, "--ever-won"),
    rookie_only: bool = typer.Option(
        False, "--rookie-only/--no-rookie-only",
        help="Pin rookie-only on. (Off-by-default; the random gen may "
             "still flip it on independently if you don't pass this.)",
    ),
    draft_rounds: str | None = typer.Option(
        None, "--draft-rounds",
        help='Pin a draft-round bucket — e.g. "1" or "4,5" or '
             '"undrafted".',
    ),
    draft_start: int | None = typer.Option(None, "--draft-start"),
    draft_end:   int | None = typer.Option(None, "--draft-end"),
    drafted_by: str | None = typer.Option(None, "--drafted-by"),
    min_stat: list[str] | None = typer.Option(None, "--min-stat"),
    max_stat: list[str] | None = typer.Option(None, "--max-stat"),
    college: str | None = typer.Option(None, "--college"),
    min_career_stat: list[str] | None = typer.Option(None, "--min-career-stat"),
    max_career_stat: list[str] | None = typer.Option(None, "--max-career-stat"),
    teammate_of: str | None = typer.Option(
        None, "--teammate-of",
        help="Pin the answer set to ever-teammates of the named "
             'player. Example: --teammate-of "Justin Fields".',
    ),
    unique: bool | None = typer.Option(
        None, "--unique/--no-unique",
        help="Pin uniqueness. Default (omit the flag) leaves it as a "
             "2/3-true random toggle in season mode (career mode is "
             "always unique-by-player).",
    ),
    mode: str | None = typer.Option(
        None, "--mode",
        help='Pin the trivia mode: "season" (single-season top-N) or '
             '"career" (career-totals top-N). Default leaves it random '
             "(~25% career, 75% season).",
    ),
    tiebreak_by: list[str] | None = typer.Option(None, "--tiebreak-by"),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
    seed: int | None = typer.Option(
        None, "--seed",
        help="Optional RNG seed for reproducibility (e.g. tests, "
             "sharing a specific generated game).",
    ),
) -> None:
    """Random trivia — fresh sampled template every call.

    Any flags you pass become hard constraints; the rest stay random.
    e.g. ``trivia random --start 1970 --end 1990 --team PIT`` produces
    a random rank-by + filter combination scoped to 1970-1990 Steelers
    seasons.

    Retries up to 25 attempts to find a non-empty answer set; falls
    back to a minimum-filter template (with your pins still applied)
    if everything came up empty.
    """
    import random

    # Parse the few comma/= flags into the shapes the queries layer
    # expects.
    rounds_list: list[int | str] | None = None
    if draft_rounds:
        rounds_list = []
        for token in (t.strip() for t in draft_rounds.split(",")):
            if not token:
                continue
            if token.lower() == "undrafted":
                rounds_list.append("undrafted")
            else:
                try:
                    rounds_list.append(int(token))
                except ValueError:
                    typer.echo(
                        f"--draft-rounds entries must be ints or 'undrafted'; "
                        f"got {token!r}",
                        err=True,
                    )
                    raise typer.Exit(code=2)
    min_stats_dict = _parse_stat_pairs(min_stat, "--min-stat")
    max_stats_dict = _parse_stat_pairs(max_stat, "--max-stat")
    _validate_trivia_rank_by(rank_by)
    min_career_dict = _parse_stat_pairs(min_career_stat, "--min-career-stat")
    max_career_dict = _parse_stat_pairs(max_career_stat, "--max-career-stat")

    overrides: dict = {}
    if rank_by:               overrides["rank_by"]            = rank_by
    if n is not None:         overrides["n"]                  = n
    if position:              overrides["position"]           = position
    if start is not None:     overrides["start"]              = start
    if end is not None:       overrides["end"]                = end
    if team:                  overrides["team"]               = team
    if division:              overrides["division"]           = division
    if conference:            overrides["conference"]         = conference
    if first_name_contains:   overrides["first_name_contains"] = first_name_contains
    if last_name_contains:    overrides["last_name_contains"]  = last_name_contains
    if has_award:             overrides["has_award"]          = has_award
    if ever_won:              overrides["ever_won_award"]     = ever_won
    if rookie_only:           overrides["rookie_only"]        = True
    if rounds_list:           overrides["draft_rounds"]       = rounds_list
    if draft_start is not None: overrides["draft_start"]      = draft_start
    if draft_end is not None:   overrides["draft_end"]        = draft_end
    if drafted_by:            overrides["drafted_by"]         = drafted_by
    if min_stats_dict:        overrides["min_stats"]          = min_stats_dict
    if max_stats_dict:        overrides["max_stats"]          = max_stats_dict
    if college:               overrides["college"]            = college
    if min_career_dict:       overrides["min_career_stats"]   = min_career_dict
    if max_career_dict:       overrides["max_career_stats"]   = max_career_dict
    if unique is not None:    overrides["unique"]             = unique
    # teammate_of resolution needs an open DB — defer until after
    # we've opened the connection. Keep the raw name in the local
    # `teammate_of_name` for later assembly.
    teammate_of_name = teammate_of
    if mode:
        if mode not in ("season", "career"):
            typer.echo(
                f"--mode must be 'season' or 'career', got {mode!r}",
                err=True,
            )
            raise typer.Exit(code=2)
        overrides["mode"] = mode
    if tiebreak_by:           overrides["tiebreak_by"]        = tiebreak_by

    rng = random.Random(seed)
    con = _open_db(db)
    try:
        # Resolve --teammate-of inside the connection block so the
        # resolver can read the players table. Stash the resolved
        # display name on the spec for the title builder; the
        # player_id flows through to the SQL filter.
        if teammate_of_name:
            resolved = _resolve_teammate_of(con, teammate_of_name)
            if resolved:
                overrides["teammate_of_player_id"] = resolved[0]
                overrides["teammate_of_name"] = resolved[1]
        template, answers, n_, rank_by_, position_ = _pick_non_empty_template(
            con, rng, overrides=overrides if overrides else None,
        )
    finally:
        con.close()
    if not answers:
        typer.echo("No matching player-seasons for any random template.")
        raise typer.Exit(code=0)
    label = "random with pins" if overrides else "random"
    _run_template(
        template, answers, n_, rank_by_, position_,
        label=label, history_dir=_history_dir_for_db(db),
    )


@trivia_app.command("replay")
def trivia_replay(
    game_id: str = typer.Argument(
        ...,
        help='Game ID from `trivia history` (e.g. "42" or "000042"). '
             "The exact same template runs against the current DB — "
             "answer set may differ if the DB was rebuilt with newer "
             "data, but the question stays the same.",
    ),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
) -> None:
    """Replay a saved trivia game by ID."""
    from ffpts.trivia_replay import load_spec

    history_dir = _history_dir_for_db(db)
    try:
        spec = load_spec(game_id, history_dir=history_dir)
    except FileNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(code=1)

    template = spec["template"]
    label = f"replay of #{spec['id']} ({spec.get('label', 'unknown')})"
    typer.echo(
        f"Replaying game {spec['id']} (saved {spec.get('saved_at', 'unknown')})"
    )

    con = _open_db(db)
    try:
        answers, n_, rank_by_, position_ = _resolve_template(con, template)
    finally:
        con.close()
    if not answers:
        typer.echo("No matching player-seasons for this saved template.")
        raise typer.Exit(code=0)
    # save=False — replays don't get re-saved (would inflate history
    # and confuse "replay of #N" loops).
    _run_template(
        template, answers, n_, rank_by_, position_,
        label=label, save=False,
    )


@trivia_app.command("history")
def trivia_history(
    n: int = typer.Option(20, "--n", help="Show this many recent games."),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
) -> None:
    """List recent saved trivia games."""
    from ffpts.trivia_replay import list_recent

    games = list_recent(n=n, history_dir=_history_dir_for_db(db))
    if not games:
        typer.echo("No saved trivia games yet — play one to populate history.")
        return
    typer.echo(f"Recent trivia games (newest first, top {len(games)}):")
    for g in games:
        gid = g.get("id", "?")
        when = g.get("saved_at", "?")
        # Trim ISO timestamp microseconds for display.
        if isinstance(when, str) and "." in when:
            when = when.split(".", 1)[0]
        label = g.get("label", "")
        t = g.get("template", {})
        rank_by = t.get("rank_by", "?")
        pos = t.get("position", "ALL")
        mode = t.get("mode", "season")
        n_count = t.get("n", "?")
        # One-line digest — full template can be inspected via the
        # JSON file directly if needed.
        typer.echo(
            f"  #{gid}  {when}  [{label}]  "
            f"top {n_count} {pos} {mode}/{rank_by}"
        )


if __name__ == "__main__":
    app()
