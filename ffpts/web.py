"""Tiny FastAPI frontend over the same query helpers the CLI uses.

Plain HTML, no JS, no CSS frameworks — styled like
motherfuckingwebsite.com. The trivia game state lives in an
in-memory dict keyed by game_id; restarting the server resets it
(by design — this is a local single-user tool, not a service).

Launch with ``fb_info web`` (which calls into ``run()``).
"""

from __future__ import annotations

import html
import secrets
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from ffpts.cli import (
    _STAT_COMPATIBLE_POSITIONS,
    _build_trivia_title,
    _hint_layers,
    _normalize_career_row,
    _open_db,
    _player_identity,
    _resolve_template,
    _pick_non_empty_template,
)
from ffpts.db import DEFAULT_DB_PATH
from ffpts.queries import (
    AWARD_TYPES_ALLOWED,
    POSITION_ALIASES,
    RANK_BY_ALLOWED,
    award_topN,
    awards_list,
    career_topN,
    pos_topN,
)


# ---------------------------------------------------------------------------
# In-memory game store. Each entry holds the resolved template, the
# answer set, and the running player state (found indices, hint cursor,
# hint level per index, guesses made). Keyed by an opaque
# token-style ID so URLs aren't guessable.
# ---------------------------------------------------------------------------
_GAMES: dict[str, dict[str, Any]] = {}


def _new_game_id() -> str:
    """Random URL-safe token. Long enough that two games can't collide."""
    return secrets.token_urlsafe(8)


def _make_app(db_path: Path) -> FastAPI:
    """Construct the FastAPI app, closure-scoping the DB path so we
    don't have to pass it through every handler."""
    app = FastAPI(title="FB Info")

    # ----- Pages -----

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return _page("FB Info", _intro_body())

    @app.get("/ask", response_class=HTMLResponse)
    def ask_form() -> str:
        return _page("Ask", _ask_form_body())

    @app.post("/ask", response_class=HTMLResponse)
    def ask_submit(
        kind: str = Form("pos-top"),
        rank_by: str = Form("fpts_ppr"),
        n: int = Form(10),
        position: str = Form("ALL"),
        start: str = Form(""),
        end: str = Form(""),
        team: str = Form(""),
        division: str = Form(""),
        conference: str = Form(""),
        has_award: str = Form(""),
        ever_won: str = Form(""),
        rookie_only: str = Form(""),
        college: str = Form(""),
        first_name_contains: str = Form(""),
        last_name_contains: str = Form(""),
        award: str = Form(""),
        season: str = Form(""),
        unique: str = Form(""),
    ) -> str:
        try:
            cols, rows, label = _run_ask(
                db_path,
                kind=kind,
                rank_by=rank_by, n=n, position=position,
                start=_int_or_none(start), end=_int_or_none(end),
                team=team or None, division=division or None,
                conference=conference or None,
                has_award=[has_award] if has_award else None,
                ever_won=[ever_won] if ever_won else None,
                rookie_only=bool(rookie_only),
                college=college or None,
                first_name_contains=first_name_contains or None,
                last_name_contains=last_name_contains or None,
                award=award or None,
                season=_int_or_none(season),
                unique=bool(unique),
            )
        except (ValueError, RuntimeError) as e:
            return _page("Error", f"<p><b>Query failed:</b> {html.escape(str(e))}</p>"
                                  '<p><a href="/ask">Back</a></p>')
        body = (
            f"<h2>{html.escape(label)}</h2>\n"
            + _render_table(cols, rows)
            + '<p><a href="/ask">New query</a></p>'
        )
        return _page(label, body)

    @app.get("/trivia", response_class=HTMLResponse)
    def trivia_index() -> str:
        return _page("Trivia", _trivia_index_body())

    @app.get("/trivia/play", response_class=HTMLResponse)
    def trivia_play_form() -> str:
        return _page("Trivia · Make-Your-Own", _trivia_play_form_body())

    @app.post("/trivia/play")
    def trivia_play_start(
        rank_by: str = Form("fpts_ppr"),
        n: int = Form(10),
        position: str = Form("ALL"),
        start: str = Form(""),
        end: str = Form(""),
        team: str = Form(""),
        has_award: str = Form(""),
        ever_won: str = Form(""),
        rookie_only: str = Form(""),
        unique: str = Form("on"),
    ):
        template = {
            "rank_by":  rank_by,
            "n":        n,
            "position": position,
            "mode":     "season",
            "unique":   bool(unique),
        }
        if start:               template["start"]              = int(start)
        if end:                 template["end"]                = int(end)
        if team:                template["team"]               = team
        if has_award:           template["has_award"]          = [has_award]
        if ever_won:            template["ever_won_award"]     = [ever_won]
        if rookie_only:         template["rookie_only"]        = True
        return _start_game(db_path, template, label="play")

    @app.get("/trivia/random", response_class=HTMLResponse)
    def trivia_random_form() -> str:
        return _page("Trivia · Random", _trivia_random_form_body())

    @app.post("/trivia/random")
    def trivia_random_start(
        seed: str = Form(""),
        rank_by: str = Form(""),
        position: str = Form(""),
        start: str = Form(""),
        end: str = Form(""),
        team: str = Form(""),
        has_award: str = Form(""),
        mode: str = Form(""),
    ):
        import random

        overrides: dict = {}
        if rank_by:    overrides["rank_by"]   = rank_by
        if position:   overrides["position"]  = position
        if start:      overrides["start"]     = int(start)
        if end:        overrides["end"]       = int(end)
        if team:       overrides["team"]      = team
        if has_award:  overrides["has_award"] = [has_award]
        if mode:       overrides["mode"]      = mode

        rng = random.Random(int(seed) if seed else None)
        con = _open_db(db_path)
        try:
            template, answers, _, _, _ = _pick_non_empty_template(
                con, rng, overrides=overrides if overrides else None,
            )
        finally:
            con.close()
        if not answers:
            return _page("Trivia · Random",
                         "<p>No matching player-seasons. Try fewer pins.</p>"
                         '<p><a href="/trivia/random">Back</a></p>')
        label = "random with pins" if overrides else "random"
        return _start_game_with_answers(template, answers, label=label)

    @app.get("/trivia/daily")
    def trivia_daily_start():
        import datetime
        import random

        seed = int(datetime.date.today().isoformat().replace("-", ""))
        rng = random.Random(seed)
        con = _open_db(db_path)
        try:
            template, answers, _, _, _ = _pick_non_empty_template(con, rng)
        finally:
            con.close()
        if not answers:
            return _page("Trivia · Daily",
                         "<p>No template produced answers today.</p>"
                         '<p><a href="/trivia">Back</a></p>')
        return _start_game_with_answers(
            template, answers, label=f"daily for {datetime.date.today()}",
        )

    @app.get("/trivia/{game_id}", response_class=HTMLResponse)
    def trivia_view(game_id: str) -> str:
        game = _GAMES.get(game_id)
        if not game:
            raise HTTPException(404, "Game not found (server may have restarted).")
        return _page(game["title"], _render_game(game_id, game))

    @app.post("/trivia/{game_id}/guess", response_class=HTMLResponse)
    def trivia_guess(game_id: str, guess: str = Form(...)) -> str:
        game = _GAMES.get(game_id)
        if not game:
            raise HTTPException(404, "Game not found.")
        _apply_guess(game, guess)
        return _page(game["title"], _render_game(game_id, game))

    @app.post("/trivia/{game_id}/hint", response_class=HTMLResponse)
    def trivia_hint(game_id: str) -> str:
        game = _GAMES.get(game_id)
        if not game:
            raise HTTPException(404, "Game not found.")
        _apply_hint(game)
        return _page(game["title"], _render_game(game_id, game))

    @app.post("/trivia/{game_id}/give-up", response_class=HTMLResponse)
    def trivia_give_up(game_id: str) -> str:
        game = _GAMES.get(game_id)
        if not game:
            raise HTTPException(404, "Game not found.")
        game["over"] = True
        return _page(game["title"], _render_game(game_id, game))

    return app


# ---------------------------------------------------------------------------
# Plumbing: ask helpers, game state, HTML rendering. Plain string
# concatenation rather than template files to keep this all in one module.
# ---------------------------------------------------------------------------

def _int_or_none(s: str) -> int | None:
    return int(s) if s else None


def _run_ask(
    db_path: Path,
    *, kind: str, rank_by: str, n: int, position: str,
    start: int | None, end: int | None,
    team: str | None, division: str | None, conference: str | None,
    has_award: list[str] | None, ever_won: list[str] | None,
    rookie_only: bool,
    college: str | None,
    first_name_contains: str | None, last_name_contains: str | None,
    award: str | None, season: int | None,
    unique: bool,
) -> tuple[list[str], list[tuple], str]:
    """Run the named ask helper and return (columns, rows, page_label).

    ``kind`` selects which helper:
      pos-top  -> pos_topN
      career   -> career_topN (or award_topN when --award is set)
      awards   -> awards_list
    """
    con = _open_db(db_path)
    try:
        if kind == "pos-top":
            sql, params = pos_topN(
                position, n=n, rank_by=rank_by,
                start=start, end=end,
                team=team, division=division, conference=conference,
                first_name_contains=first_name_contains,
                last_name_contains=last_name_contains,
                has_award=has_award, ever_won_award=ever_won,
                rookie_only=rookie_only, college=college,
                unique=unique,
            )
            label = (
                f"pos-top: top {n} {position} by {rank_by}"
                + (f" ({start}-{end})" if start and end else "")
            )
        elif kind == "career":
            if award:
                sql, params = award_topN(award, n=n, position=position, college=college)
                label = f"career: top {n} {position} by {award} count"
            else:
                sql, params = career_topN(
                    rank_by, n=n, position=position,
                    start=start, end=end,
                    ever_won_award=ever_won, college=college,
                    first_name_contains=first_name_contains,
                    last_name_contains=last_name_contains,
                )
                label = f"career: top {n} {position} by SUM({rank_by})"
        elif kind == "awards":
            sql, params = awards_list(
                award_type=award or None, season=season, winners_only=True,
            )
            label = (
                "awards: "
                + (f"{award} winners" if award else "all winners")
                + (f" in {season}" if season else "")
            )
        else:
            raise ValueError(f"unknown kind {kind!r}")
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    finally:
        con.close()
    return cols, rows, label


def _start_game(db_path: Path, template: dict, *, label: str):
    """Resolve a play-style template into a game and redirect."""
    con = _open_db(db_path)
    try:
        answers, n, rank_by, position = _resolve_template(con, template)
    finally:
        con.close()
    if not answers:
        return _page("Trivia",
                     "<p>No matching player-seasons for those filters.</p>"
                     '<p><a href="/trivia/play">Back</a></p>')
    return _start_game_with_answers(template, answers, label=label)


def _start_game_with_answers(template: dict, answers: list[dict], *, label: str):
    """Create the in-memory game state and redirect to its view."""
    rank_by = template["rank_by"]
    title = _build_trivia_title(
        n=template.get("n", len(answers)),
        rank_by=rank_by,
        position=template.get("position", "ALL"),
        start=template.get("start"), end=template.get("end"),
        team=template.get("team"), division=template.get("division"),
        conference=template.get("conference"),
        first_name_contains=template.get("first_name_contains"),
        last_name_contains=template.get("last_name_contains"),
        has_award=template.get("has_award"),
        ever_won=template.get("ever_won_award"),
        rookie_only=template.get("rookie_only", False),
        draft_start=template.get("draft_start"),
        draft_end=template.get("draft_end"),
        drafted_by=template.get("drafted_by"),
        draft_rounds=template.get("draft_rounds"),
        min_stats=template.get("min_stats"),
        max_stats=template.get("max_stats"),
        unique=template.get("unique", True),
        mode=template.get("mode", "season"),
        college=template.get("college"),
        min_career_stats=template.get("min_career_stats"),
        max_career_stats=template.get("max_career_stats"),
    )

    game_id = _new_game_id()
    _GAMES[game_id] = {
        "title":        title,
        "label":        label,
        "rank_by":      rank_by,
        "answers":      answers,
        "found":        set(),
        "guesses":      0,
        "hint_cursor":  0,
        "hint_levels":  {},
        "log":          [],     # list of {"type": "guess"/"hint", "text": "..."}
        "over":         False,
    }
    return RedirectResponse(f"/trivia/{game_id}", status_code=303)


def _apply_guess(game: dict, guess: str) -> None:
    if game["over"]:
        return
    needle = (guess or "").strip().lower()
    if not needle:
        return
    answers = game["answers"]
    found = game["found"]
    matches = [
        i for i, row in enumerate(answers)
        if i not in found and needle in (row["name"] or "").lower()
    ]
    game["guesses"] += 1
    if not matches:
        game["log"].append({"type": "wrong", "text": f"Guess '{html.escape(guess)}' — not in the top {len(answers)}."})
        return
    if len(matches) > 1 and len({_player_identity(answers[i]) for i in matches}) > 1:
        game["log"].append({
            "type": "ambiguous",
            "text": f"Guess '{html.escape(guess)}' is ambiguous — matches {len(matches)} answers across multiple players. Be more specific.",
        })
        return
    # Single player or all-same-player matches — credit them all.
    for i in matches:
        found.add(i)
    rb = game["rank_by"]
    for i in sorted(matches):
        row = answers[i]
        v = row.get("rank_value")
        v_disp = _fmt_value(v)
        game["log"].append({
            "type": "correct",
            "text": (
                f"Correct! #{i + 1} {html.escape(row['name'])}, {row['season']} "
                f"({row['team']}, {rb}={v_disp})."
            ),
        })
    if len(found) == len(answers):
        game["over"] = True


def _apply_hint(game: dict) -> None:
    if game["over"]:
        return
    answers = game["answers"]
    found = game["found"]
    unfound = [i for i in range(len(answers)) if i not in found]
    if not unfound:
        game["log"].append({"type": "info", "text": "No hints — you got them all."})
        return
    cursor = game["hint_cursor"]
    idx = unfound[cursor % len(unfound)]
    level = game["hint_levels"].get(idx, 0) + 1
    layers = _hint_layers(answers[idx], rank_by=game["rank_by"])
    capped = min(level, len(layers))
    game["hint_levels"][idx] = capped
    game["hint_cursor"] = cursor + 1
    game["log"].append({
        "type": "hint",
        "text": f"Hint #{capped} for #{idx + 1}: " + ", ".join(layers[:capped]),
    })


def _fmt_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


# ---------------------------------------------------------------------------
# HTML rendering. No CSS, system fonts, default colors — black on white,
# blue links. motherfuckingwebsite.com aesthetic. The only deviation:
# tables get a 1-pixel border so columns are readable.
# ---------------------------------------------------------------------------

def _page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
</head>
<body>
<h1><a href="/">FB Info</a></h1>
{body}
<hr>
<p><small><a href="/">home</a> &middot; <a href="/ask">ask</a> &middot; <a href="/trivia">trivia</a></small></p>
</body>
</html>
"""


def _intro_body() -> str:
    return """
<p>Tiny local frontend over a DuckDB of NFL player and team season stats.
Same query layer as the <code>fb_info</code> CLI.</p>
<ul>
  <li><a href="/ask">Ask</a> &mdash; player-season top-N, career totals, award listings.</li>
  <li><a href="/trivia">Trivia</a> &mdash; daily, random, or make-your-own.</li>
</ul>
"""


def _ask_form_body() -> str:
    pos_opts = _opts(_position_choices())
    rank_opts = _opts(sorted(RANK_BY_ALLOWED))
    award_opts = _opts([""] + sorted(AWARD_TYPES_ALLOWED))
    return f"""
<p>Run a single query. <em>kind=pos-top</em> ranks player-seasons,
<em>kind=career</em> ranks players by career total (or career
<em>--award</em> count), <em>kind=awards</em> lists raw award rows.</p>
<form method="post" action="/ask">
  <p>kind:
    <select name="kind">
      <option value="pos-top">pos-top (player-season)</option>
      <option value="career">career (sum or award count)</option>
      <option value="awards">awards (list winners)</option>
    </select>
  </p>
  <p>rank-by: <select name="rank_by">{rank_opts}</select>
     n: <input type="number" name="n" value="10" min="1" max="200" size="4"></p>
  <p>position: <select name="position">{pos_opts}</select></p>
  <p>start: <input type="number" name="start" placeholder="1970" size="6">
     end:   <input type="number" name="end"   placeholder="2025" size="6"></p>
  <p>team: <input name="team" placeholder="PIT" size="6">
     division: <input name="division" placeholder="NFC North" size="14">
     conference: <input name="conference" placeholder="AFC" size="6"></p>
  <p>has-award (this season): <select name="has_award">{award_opts}</select>
     ever-won (any season):   <select name="ever_won">{award_opts}</select></p>
  <p>award (career mode count, or awards-list filter): <select name="award">{award_opts}</select>
     season (awards mode only): <input type="number" name="season" size="6"></p>
  <p>college: <input name="college" placeholder="Alabama" size="14">
     first-name has: <input name="first_name_contains" size="8">
     last-name has:  <input name="last_name_contains" size="8"></p>
  <p>
    <label><input type="checkbox" name="rookie_only" value="1"> rookie-only</label>
    &nbsp;
    <label><input type="checkbox" name="unique" value="1"> unique-by-player (best season)</label>
  </p>
  <p><input type="submit" value="run"></p>
</form>
"""


def _trivia_index_body() -> str:
    return """
<p>Three modes:</p>
<ul>
  <li><a href="/trivia/daily">Daily</a> &mdash; same game for everyone today (deterministic by date).</li>
  <li><a href="/trivia/random">Random</a> &mdash; fresh template every call. Optional pins.</li>
  <li><a href="/trivia/play">Play</a> &mdash; make-your-own with explicit filters.</li>
</ul>
"""


def _trivia_play_form_body() -> str:
    pos_opts = _opts(_position_choices())
    rank_opts = _opts(sorted(RANK_BY_ALLOWED))
    award_opts = _opts([""] + sorted(AWARD_TYPES_ALLOWED))
    return f"""
<p>Build a custom trivia. Same filters as the CLI's <code>trivia play</code>.</p>
<form method="post" action="/trivia/play">
  <p>rank-by: <select name="rank_by">{rank_opts}</select>
     position: <select name="position">{pos_opts}</select>
     n: <input type="number" name="n" value="10" min="1" max="100" size="4"></p>
  <p>start: <input type="number" name="start" size="6">
     end:   <input type="number" name="end"   size="6"></p>
  <p>team: <input name="team" size="6"></p>
  <p>has-award (this season): <select name="has_award">{award_opts}</select>
     ever-won (any season):   <select name="ever_won">{award_opts}</select></p>
  <p>
    <label><input type="checkbox" name="rookie_only" value="1"> rookie-only</label>
    &nbsp;
    <label><input type="checkbox" name="unique" value="on" checked> unique-by-player</label>
  </p>
  <p><input type="submit" value="start game"></p>
</form>
"""


def _trivia_random_form_body() -> str:
    pos_opts = _opts(_position_choices())
    # rank_by leaves blank so the random gen picks if not pinned
    rank_opts = _opts([""] + sorted(RANK_BY_ALLOWED))
    award_opts = _opts([""] + sorted(AWARD_TYPES_ALLOWED))
    return f"""
<p>Random trivia. All fields are optional &mdash; anything you fill becomes
a hard pin, the rest stays random.</p>
<form method="post" action="/trivia/random">
  <p>seed: <input type="number" name="seed" placeholder="(blank = fresh)" size="8"></p>
  <p>rank-by: <select name="rank_by">{rank_opts}</select>
     position: <select name="position">{pos_opts}</select></p>
  <p>start: <input type="number" name="start" size="6">
     end:   <input type="number" name="end"   size="6"></p>
  <p>team: <input name="team" size="6">
     has-award: <select name="has_award">{award_opts}</select></p>
  <p>mode:
    <select name="mode">
      <option value="">(random — ~25% career)</option>
      <option value="season">season</option>
      <option value="career">career</option>
    </select>
  </p>
  <p><input type="submit" value="start random"></p>
</form>
"""


def _render_game(game_id: str, game: dict) -> str:
    answers = game["answers"]
    found = game["found"]
    title = game["title"]
    log_html = "".join(
        f"<li><em>{e['type']}</em>: {e['text']}</li>" for e in game["log"][-30:]
    )
    if game["over"]:
        # Final ranked list after the game ends.
        rb = game["rank_by"]
        rows = []
        for i, row in enumerate(answers):
            mark = "&#10003;" if i in found else "&#10007;"
            v = _fmt_value(row.get("rank_value"))
            rows.append(
                f"<tr><td>{mark}</td><td>#{i+1}</td>"
                f"<td>{html.escape(str(row.get('name','')))}</td>"
                f"<td>{html.escape(str(row.get('team','')))} {row.get('season','')}</td>"
                f"<td>{rb}={v}</td></tr>"
            )
        final_table = (
            "<table border=1>"
            "<tr><th></th><th>rank</th><th>name</th><th>team / season</th><th>value</th></tr>"
            + "".join(rows)
            + "</table>"
        )
        score = f"Final score: {len(found)} / {len(answers)} in {game['guesses']} guesses."
        return f"""
<h2>{html.escape(title)}</h2>
<p>({html.escape(game['label'])})</p>
<h3>Final ranked list</h3>
{final_table}
<p>{score}</p>
<p><a href="/trivia">New game</a></p>
"""

    return f"""
<h2>{html.escape(title)}</h2>
<p>({html.escape(game['label'])})</p>
<p>{len(found)} / {len(answers)} found, {game['guesses']} guesses.</p>

<form method="post" action="/trivia/{game_id}/guess">
  Guess (substring of player name): <input name="guess" autofocus>
  <input type="submit" value="submit">
</form>

<form method="post" action="/trivia/{game_id}/hint" style="display:inline">
  <input type="submit" value="hint">
</form>
<form method="post" action="/trivia/{game_id}/give-up" style="display:inline">
  <input type="submit" value="give up">
</form>

<h3>Log</h3>
<ul>{log_html}</ul>
"""


def _render_table(cols: list[str], rows: list[tuple]) -> str:
    if not rows:
        return "<p>(no rows)</p>"
    head = "<tr>" + "".join(f"<th>{html.escape(c)}</th>" for c in cols) + "</tr>"
    body = ""
    for r in rows:
        body += "<tr>" + "".join(
            f"<td>{html.escape(_fmt_value(c))}</td>" for c in r
        ) + "</tr>"
    return f"<table border=1>{head}{body}</table>"


def _opts(values: list[str]) -> str:
    return "".join(
        f"<option value=\"{html.escape(v)}\">{html.escape(v) if v else '(any)'}</option>"
        for v in values
    )


def _position_choices() -> list[str]:
    """All position selectors expand the same set: ALL + FLEX + alias
    groups + concrete PFR labels we know about."""
    aliases = list(POSITION_ALIASES)  # ALL, FLEX, SAFETY, DB, LB, DL
    concrete = ["QB", "RB", "WR", "TE", "FB", "K", "P"]
    return aliases + concrete


# ---------------------------------------------------------------------------
# Entry point used by the CLI command.
# ---------------------------------------------------------------------------

def run(host: str = "127.0.0.1", port: int = 8000, db: Path = DEFAULT_DB_PATH) -> None:
    """Launch the dev server. Single-process, no reload — restart after
    schema changes if needed."""
    import uvicorn

    app = _make_app(Path(db))
    uvicorn.run(app, host=host, port=port, log_level="info")
