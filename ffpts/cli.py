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
    awards_list,
    career_topN,
    flex_topN_by_draft_round,
    most_def_int_by_division,
    pos_topN,
)

app = typer.Typer(
    add_completion=False,
    help="NFL player & team season stats — local DuckDB, scraped/loaded from nflverse.",
)
ask_app = typer.Typer(
    add_completion=False,
    help="Run a named query helper from ffpts.queries.",
)
app.add_typer(ask_app, name="ask")


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


@ask_app.command("flex-top")
def ask_flex_top(
    round_: int = typer.Option(..., "--round", help="Draft round to filter on."),
    n: int = typer.Option(10, "--n", help="Number of player-seasons to return."),
    scoring: str = typer.Option("ppr", "--scoring", help="std | half | ppr"),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db", help="Path to the DuckDB file."),
) -> None:
    """Top-N FLEX (RB/WR/TE) player-seasons drafted in a given round."""
    sql, params = flex_topN_by_draft_round(round_, n=n, scoring=scoring)  # type: ignore[arg-type]
    con = _open_db(db)
    try:
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        _print_rows(cur.fetchall(), cols)
    finally:
        con.close()


@ask_app.command("div-int")
def ask_div_int(
    division: str = typer.Option(..., "--division", help='Division name, e.g. "NFC North".'),
    start: int = typer.Option(..., "--start", help="First season (inclusive)."),
    end: int = typer.Option(..., "--end", help="Last season (inclusive)."),
    n: int = typer.Option(25, "--n", help="Number of player-seasons to return."),
    mode: str = typer.Option(
        "historical", "--mode",
        help="historical | franchise — see queries.py for semantics.",
    ),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db", help="Path to the DuckDB file."),
) -> None:
    """Top-N player-seasons by defensive INTs scoped to a division."""
    sql, params = most_def_int_by_division(
        division, start=start, end=end, n=n, division_mode=mode,  # type: ignore[arg-type]
    )
    con = _open_db(db)
    try:
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        _print_rows(cur.fetchall(), cols)
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
    )
    con = _open_db(db)
    try:
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

        if show_awards or show_context:
            rows, cols = _augment_display(con, rows, cols, show_awards, show_context)

        _print_rows(rows, cols)
    finally:
        con.close()


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
    )
    con = _open_db(db)
    try:
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
    )
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

    head = f"Top {n} {pos_part}player-seasons by {rank_by}{years}"

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

    if first_name_contains:
        clauses.append(f"first name contains '{first_name_contains}'")
    if last_name_contains:
        clauses.append(f"last name contains '{last_name_contains}'")

    if not unique:
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
        help="Stat column to sum across seasons.",
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
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
) -> None:
    """Top-N players by *career-total* of --rank-by.

    Different unit-of-analysis from `ask` (which is player-season).
    Career totals are summed across the seasons that match the
    optional filters, then ranked.
    """
    sql, params = career_topN(
        rank_by, n=n, position=position,
        start=start, end=end,
        ever_won_award=ever_won if ever_won else None,
        min_seasons=min_seasons,
    )
    con = _open_db(db)
    try:
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        _print_rows(cur.fetchall(), cols)
    finally:
        con.close()


# ---------------------------------------------------------------------------
# `ask awards` — list winners by award type / season
# ---------------------------------------------------------------------------

@ask_app.command("awards")
def ask_awards(
    award: str | None = typer.Option(
        None, "--award",
        help="Award type to list. One of: " + ", ".join(sorted(AWARD_TYPES_ALLOWED)),
    ),
    season: int | None = typer.Option(
        None, "--season",
        help="Restrict to one season (e.g. 2023).",
    ),
    winners_only: bool = typer.Option(
        True, "--winners-only/--include-finalists",
        help="Default ON: outright winners only. Pass "
             "--include-finalists to see runner-ups for vote-ranked "
             "awards (MVP, OPOY, DPOY, OROY, DROY, CPOY).",
    ),
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
) -> None:
    """List award winners. Defaults to all awards across all seasons
    (winners only); pass --award and/or --season to scope down."""
    sql, params = awards_list(
        award_type=award, season=season, winners_only=winners_only,
    )
    con = _open_db(db)
    try:
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
# (chosen so trivia leans toward the stats fans care about most):
#   offense (pass+rush+rec): ~58%
#   fantasy:                 ~20%
#   defense:                 ~14%
#   special teams (K + ret):  ~8%
_RANDOM_RANK_BY: list[str] = (
    # Passing
    ["pass_yds"] * 3 + ["pass_td"] * 3 + ["pass_cmp"] * 2 + ["pass_rating"] * 2
    # Rushing
    + ["rush_yds"] * 3 + ["rush_td"] * 3 + ["rush_att"] * 2
    # Receiving
    + ["rec_yds"] * 3 + ["rec"] * 3 + ["rec_td"] * 3 + ["targets"] * 2
    # Fantasy
    + ["fpts_ppr"] * 4 + ["fpts_half"] * 3 + ["fpts_std"] * 3
    # Defense (down-weighted vs. the offense categories)
    + ["def_sacks"] * 2 + ["def_int"] * 2 + ["def_int_td"]
    + ["def_tackles_combined"] + ["def_pass_def"]
    # Special teams (rarest — kickers and return men are a niche game)
    + ["fg_made"] + ["fg_long"]
    + ["kr_yds"] + ["pr_yds"]
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
    "rush_yds":              ["RB"] * 4 + ["FLEX", "QB", "ALL", "WR"],
    "rush_td":               ["RB"] * 4 + ["FLEX", "QB", "ALL", "WR"],
    "rush_att":              ["RB"] * 5 + ["FLEX", "QB", "ALL"],
    "rec_yds":               ["WR"] * 3 + ["TE", "RB", "FLEX", "ALL"],
    "rec":                   ["WR"] * 3 + ["TE", "RB", "FLEX", "ALL"],
    "rec_td":                ["WR"] * 2 + ["TE", "RB", "FLEX", "ALL"],
    "targets":               ["WR"] * 2 + ["TE", "RB", "FLEX", "ALL"],
    "fpts_ppr":              ["FLEX"] * 2 + ["ALL"] * 2 + ["QB", "RB", "WR", "TE"],
    "fpts_half":             ["FLEX"] * 2 + ["ALL"] * 2 + ["QB", "RB", "WR", "TE"],
    "fpts_std":              ["FLEX"] * 2 + ["ALL"] * 2 + ["QB", "RB", "WR", "TE"],
    # Defense — position is fine-grained (LB/DB/DE/...) and we don't
    # bother distinguishing in the DB, so ALL is the only sensible pick.
    "def_sacks":             ["ALL"],
    "def_int":               ["ALL"],
    "def_int_td":            ["ALL"],
    "def_pass_def":          ["ALL"],
    "def_tackles_combined":  ["ALL"],
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
    "PB", "AP_FIRST",
]
_RANDOM_N_CHOICES: list[int] = [5, 5, 10, 10, 10, 15]


def _random_trivia_template(rng) -> dict:
    """Build a fresh template by sampling each filter dimension.

    Picks at most one of {team, division, conference} so geo filters
    don't compound into empty sets, and at most one award filter
    (has_award OR ever_won). Year range, rookie_only, and uniqueness
    are independent randomized toggles.
    """
    rank_by = rng.choice(_RANDOM_RANK_BY)
    # Position is sampled from a stat-specific pool so we don't end up
    # ranking running backs by passing yards or kickers by sacks. The
    # weighting (list duplication) keeps natural pairings dominant
    # while leaving room for occasional cross-position trivia.
    pos_pool = _STAT_COMPATIBLE_POSITIONS.get(rank_by, ["ALL"])
    spec: dict = {
        "rank_by":  rank_by,
        "n":        rng.choice(_RANDOM_N_CHOICES),
        "position": rng.choice(pos_pool),
        "unique":   rng.choice([True, True, False]),  # 2/3 unique
    }

    # Filter intensity: offense / fantasy templates get richer
    # qualifiers (year ranges, team/division/conference, award, rookie)
    # because the unfiltered "top 10 pass_yds" question is too easy and
    # the candidate pool is huge; defense and special-teams trivia
    # already has a small enough candidate pool that piling on filters
    # makes most attempts return empty.
    is_off = rank_by in _OFFENSE_AND_FANTASY_RANK_BY
    p_year   = 0.65 if is_off else 0.35
    p_geo    = 0.45 if is_off else 0.20  # cumulative across team/div/conf
    p_award  = 0.40 if is_off else 0.15  # cumulative across has/ever
    p_rookie = 0.15 if is_off else 0.05

    # Year range — start ≤ end always enforced.
    min_floor = _STAT_MIN_SEASON.get(rank_by, 1970)
    if rng.random() < p_year:
        start = rng.randint(min_floor, 2018)
        end   = rng.randint(start, 2024)
        spec["start"] = start
        spec["end"]   = end
    else:
        # Always respect era floors even when no random year range —
        # ranking by def_sacks before 1982 returns nothing useful.
        if min_floor > 1970:
            spec["start"] = min_floor

    # Geo: at most one of team / division / conference. Split the
    # cumulative budget into thirds so each variant has equal share.
    geo = rng.random()
    if geo < p_geo / 3:
        spec["team"] = rng.choice(_RANDOM_TEAMS)
    elif geo < 2 * p_geo / 3:
        spec["division"] = rng.choice(_RANDOM_DIVISIONS)
    elif geo < p_geo:
        spec["conference"] = rng.choice(_RANDOM_CONFERENCES)

    # Award filter: at most one of has_award / ever_won_award. Skew
    # toward ever_won (richer answer space than has_award-that-season).
    aw = rng.random()
    if aw < p_award * 0.4:
        spec["has_award"] = [rng.choice(_RANDOM_AWARDS)]
    elif aw < p_award:
        spec["ever_won_award"] = [rng.choice(_RANDOM_AWARDS)]

    if rng.random() < p_rookie:
        spec["rookie_only"] = True

    return spec


def _resolve_template(con: duckdb.DuckDBPyConnection, template: dict):
    """Run pos_topN for a template, return (answers, n, rank_by, position).

    Returns ``answers=None`` if the SQL call itself raises (rare —
    happens if a template asks for an impossible combination like
    rookie_only + a draft round filter that excludes everyone)."""
    args = dict(template)
    rank_by = args.pop("rank_by")
    n = args.pop("n")
    position = args.pop("position", "ALL")
    try:
        sql, params = pos_topN(position, n=n, rank_by=rank_by, **args)
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        answers = [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception:
        return None, n, rank_by, position
    return answers, n, rank_by, position


def _pick_non_empty_template(
    con: duckdb.DuckDBPyConnection, rng, *, max_attempts: int = 12
) -> tuple[dict, list[dict], int, str, str]:
    """Sample random templates until one yields a non-empty answer
    set, up to ``max_attempts``. Falls back to a known-good
    minimum-filter template if every attempt returned empty."""
    last = None
    for _ in range(max_attempts):
        template = _random_trivia_template(rng)
        answers, n, rank_by, position = _resolve_template(con, template)
        if answers:
            return template, answers, n, rank_by, position
        last = (template, answers, n, rank_by, position)
    # Fallback: rank by fpts_ppr across all positions, no other filters.
    fallback = {"rank_by": "fpts_ppr", "n": 10, "position": "ALL", "unique": True}
    answers, n, rank_by, position = _resolve_template(con, fallback)
    return fallback, answers or [], n, rank_by, position


def _run_template(
    template: dict, answers: list[dict],
    n: int, rank_by: str, position: str, *, label: str,
) -> None:
    """Common end-of-pipeline: print label, build title from the
    resolved template args, run the REPL."""
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
    )
    typer.echo(f"({label})")
    _run_trivia_loop(answers, rank_by=rank_by, title=title)


@trivia_app.command("daily")
def trivia_daily(
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
) -> None:
    """Same trivia game for everyone today (deterministic by date).

    Uses today's date as the RNG seed, so every player sees the same
    randomly-generated template until midnight local time.
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
    )


@trivia_app.command("random")
def trivia_random(
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
    seed: int | None = typer.Option(
        None, "--seed",
        help="Optional RNG seed for reproducibility (e.g. tests, "
             "sharing a specific generated game).",
    ),
) -> None:
    """Random trivia — fresh sampled template every call.

    Samples rank_by, position, year range, optional team/division/
    conference, optional award filter, rookie-only, and uniqueness
    independently. Retries up to 12 times to find a non-empty answer
    set; falls back to top-N by fpts_ppr if everything came up empty.
    """
    import random

    rng = random.Random(seed)
    con = _open_db(db)
    try:
        template, answers, n, rank_by, position = _pick_non_empty_template(con, rng)
    finally:
        con.close()
    if not answers:
        typer.echo("No matching player-seasons for any random template.")
        raise typer.Exit(code=0)
    _run_template(template, answers, n, rank_by, position, label="random")


if __name__ == "__main__":
    app()
