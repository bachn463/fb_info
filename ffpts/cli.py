"""Command-line entry point: ``ffpts``.

Three commands:

- ``ffpts build --start YEAR --end YEAR`` runs the nflverse->DuckDB
  pipeline for the given seasons (default DB at data/ff.duckdb).
- ``ffpts query "<SQL>"`` runs a raw SQL statement against the DB and
  prints the result as a tabulated table.
- ``ffpts ask <name> [opts...]`` runs a named helper from
  ``ffpts.queries``. Supported helpers and their flags map directly
  to the helper signatures.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import typer

from ffpts.db import DEFAULT_DB_PATH, init_db
from ffpts.pipeline import build as run_build
from ffpts.queries import flex_topN_by_draft_round, most_def_int_by_division, pos_topN

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
            f"DB not found at {db_path}. Run `ffpts build --start YEAR --end YEAR` first.",
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
    """Pull seasons [start..end] from nflverse and load into the DB."""
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
        ..., "--position",
        help='Position label ("QB", "RB", "WR", "TE", "CB", ...) or '
             '"FLEX" (RB/WR/TE) or "ALL".',
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
    db: Path = typer.Option(DEFAULT_DB_PATH, "--db", help="Path to the DuckDB file."),
) -> None:
    """Top-N player-seasons at a position, ranked by a stat column.

    All filter flags combine — pass any subset to scope the result.
    Examples:

        ffpts ask pos-top --position QB --rank-by pass_yds --draft-rounds 4,5
        ffpts ask pos-top --position WR --rank-by rec_yds --team SF
        ffpts ask pos-top --position ALL --rank-by def_int --division "NFC North"
        ffpts ask pos-top --position ALL --first-name-contains z --rank-by fpts_ppr
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
    sql, params = pos_topN(
        position, n=n, rank_by=rank_by,
        start=start, end=end, draft_rounds=rounds_list,
        team=team, division=division, conference=conference,
        first_name_contains=first_name_contains,
        last_name_contains=last_name_contains,
        unique=unique,
    )
    con = _open_db(db)
    try:
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        _print_rows(cur.fetchall(), cols)
    finally:
        con.close()


if __name__ == "__main__":
    app()
