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

    _run_trivia_loop(answers, rank_by=rank_by)


def _run_trivia_loop(answers: list[dict], *, rank_by: str) -> None:
    """Interactive REPL. Each answer dict has keys: name, team,
    season, position, rank_value, draft_round, draft_year,
    draft_overall_pick.
    """
    n = len(answers)
    found: set[int] = set()
    guesses = 0
    hint_cursor = 0

    typer.echo(
        f"Top {n} player-seasons by {rank_by}. "
        f"Type a name (substring OK). "
        f"Commands: `give up`, `hint`, `quit`."
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
            _print_final_ranked_list(answers, found, rank_by=rank_by)
            typer.echo(
                f"\nFinal score: {len(found)} / {n} in {guesses} guesses."
            )
            return
        if cmd == "give up":
            _print_final_ranked_list(answers, found, rank_by=rank_by)
            typer.echo(
                f"\nFinal score: {len(found)} / {n} in {guesses} guesses."
            )
            return
        if cmd == "hint":
            hint_cursor = _print_hint(answers, found, hint_cursor)
            continue

        guesses += 1
        matches = _match_guess(guess, answers, found)
        if not matches:
            typer.echo(f"  Not in the top {n}.")
        elif len(matches) > 1:
            names = ", ".join(answers[i]["name"] for i in matches)
            typer.echo(
                f"  Multiple matches ({names}) — be more specific."
            )
        else:
            i = matches[0]
            found.add(i)
            row = answers[i]
            rank = i + 1
            typer.echo(
                f"  Correct! #{rank} {row['name']}, {row['season']} "
                f"({row['team']}, {rank_by}={_fmt_cell(row['rank_value'])})."
            )

    # Loop exited (either all found, or stdin closed). Print the full
    # ranked list either way so the user always leaves with the answers.
    _print_final_ranked_list(answers, found, rank_by=rank_by)
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


def _print_hint(answers: list[dict], found: set[int], cursor: int) -> int:
    """Print a hint about an unfound answer; return advanced cursor."""
    unfound = [i for i in range(len(answers)) if i not in found]
    if not unfound:
        typer.echo("  No hints — you got them all.")
        return cursor
    idx = unfound[cursor % len(unfound)]
    row = answers[idx]
    rank = idx + 1
    drafted_in = row.get("draft_year")
    if drafted_in is not None and row.get("season") is not None:
        n_year = int(row["season"]) - int(drafted_in) + 1
        career_hint = f", season #{n_year} of their career"
    else:
        career_hint = ""
    typer.echo(
        f"  Hint: #{rank} played for {row['team']} in {row['season']} "
        f"({row.get('position') or 'pos?'}){career_hint}."
    )
    return cursor + 1


def _print_final_ranked_list(
    answers: list[dict], found: set[int], *, rank_by: str
) -> None:
    """Print the full ranked answer list with a marker per row showing
    whether the user found it (✓) or not (✗). Called on every trivia
    exit path so the user always leaves with the answers."""
    if not answers:
        return
    typer.echo("\nFinal ranked list:")
    for i, row in enumerate(answers):
        marker = "✓" if i in found else "✗"
        rank = i + 1
        typer.echo(
            f"  {marker} #{rank}: {row['name']} "
            f"({row['team']} {row['season']}, "
            f"{rank_by}={_fmt_cell(row['rank_value'])})"
        )


if __name__ == "__main__":
    app()
