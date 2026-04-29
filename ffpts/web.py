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
    TRIVIA_RANK_BY_ALLOWED,
    award_topN,
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
        # Top-level kind toggle (radio).
        kind: str = Form("pos-top"),
        # Shared filters (apply to both kinds).
        n: int = Form(10),
        position: str = Form("ALL"),
        start: str = Form(""),
        end: str = Form(""),
        college: str = Form(""),
        first_name_contains: str = Form(""),
        last_name_contains: str = Form(""),
        draft_rounds: str = Form(""),
        drafted_by: str = Form(""),
        draft_start: str = Form(""),
        draft_end: str = Form(""),
        ever_won: str = Form(""),
        min_career_stat: str = Form(""),
        max_career_stat: str = Form(""),
        # pos-top only.
        rank_by: str = Form("fpts_ppr"),
        team: str = Form(""),
        division: str = Form(""),
        conference: str = Form(""),
        has_award: str = Form(""),
        rookie_only: str = Form(""),
        unique: str = Form(""),
        min_stat: str = Form(""),
        max_stat: str = Form(""),
        tiebreak_by: str = Form(""),
        show_awards: str = Form(""),
        show_context: str = Form(""),
        # career only.
        career_mode: str = Form("rank_by"),
        career_rank_by: str = Form("fpts_ppr"),
        career_award: str = Form(""),
        min_seasons: str = Form(""),
    ) -> str:
        try:
            cols, rows, label = _run_ask(
                db_path,
                kind=kind,
                # shared
                n=n, position=position,
                start=_int_or_none(start), end=_int_or_none(end),
                college=college or None,
                first_name_contains=first_name_contains or None,
                last_name_contains=last_name_contains or None,
                draft_rounds=_parse_draft_rounds_form(draft_rounds),
                drafted_by=drafted_by or None,
                draft_start=_int_or_none(draft_start),
                draft_end=_int_or_none(draft_end),
                ever_won=[ever_won] if ever_won else None,
                min_career_stats=_parse_stat_pair_form(min_career_stat) or None,
                max_career_stats=_parse_stat_pair_form(max_career_stat) or None,
                # pos-top only
                rank_by=rank_by,
                team=team or None,
                division=division or None,
                conference=conference or None,
                has_award=[has_award] if has_award else None,
                rookie_only=bool(rookie_only),
                unique=bool(unique),
                min_stats=_parse_stat_pair_form(min_stat) or None,
                max_stats=_parse_stat_pair_form(max_stat) or None,
                tiebreak_by=[
                    t.strip() for t in tiebreak_by.split(",") if t.strip()
                ] or None,
                show_awards=bool(show_awards),
                show_context=bool(show_context),
                # career only
                career_mode=career_mode,
                career_rank_by=career_rank_by,
                career_award=career_award or None,
                min_seasons=_int_or_none(min_seasons),
            )
        except Exception as e:
            # Catch broadly — a malformed filter (e.g. a min-stat value
            # that isn't a real column) shouldn't 500 the page. Any
            # genuine bug shows up in the message verbatim so the user
            # can paste it back.
            return _page(
                "Error",
                f"<p><b>Query failed:</b> {html.escape(type(e).__name__)}: "
                f"{html.escape(str(e))}</p>"
                '<p><a href="/ask">Back</a></p>',
            )
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
        min_stat: str = Form(""),
        max_stat: str = Form(""),
        min_career_stat: str = Form(""),
        max_career_stat: str = Form(""),
        college: str = Form(""),
        draft_rounds: str = Form(""),
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
        if college:             template["college"]            = college
        # Stat thresholds — col=value pairs from form. Skip empty /
        # malformed; downstream pos_topN validates the col name.
        ms = _parse_stat_pair_form(min_stat)
        if ms:                  template["min_stats"]          = ms
        xs = _parse_stat_pair_form(max_stat)
        if xs:                  template["max_stats"]          = xs
        mcs = _parse_stat_pair_form(min_career_stat)
        if mcs:                 template["min_career_stats"]   = mcs
        xcs = _parse_stat_pair_form(max_career_stat)
        if xcs:                 template["max_career_stats"]   = xcs
        rounds_list = _parse_draft_rounds_form(draft_rounds)
        if rounds_list:         template["draft_rounds"]       = rounds_list
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
        ever_won: str = Form(""),
        college: str = Form(""),
        mode: str = Form(""),
        min_stat: str = Form(""),
        max_stat: str = Form(""),
        min_career_stat: str = Form(""),
        max_career_stat: str = Form(""),
        draft_rounds: str = Form(""),
    ):
        import random

        overrides: dict = {}
        if rank_by:    overrides["rank_by"]        = rank_by
        if position:   overrides["position"]       = position
        if start:      overrides["start"]          = int(start)
        if end:        overrides["end"]            = int(end)
        if team:       overrides["team"]           = team
        if has_award:  overrides["has_award"]      = [has_award]
        if ever_won:   overrides["ever_won_award"] = [ever_won]
        if college:    overrides["college"]        = college
        if mode:       overrides["mode"]           = mode
        # Stat thresholds — pass both season + career pairs as
        # overrides. The random gen routes them per mode: season-mode
        # templates use min_stats / max_stats, career-mode templates
        # use min_career_stats / max_career_stats. The other pair is
        # silently dropped to avoid confusing partial-pin behavior.
        ms = _parse_stat_pair_form(min_stat)
        if ms:         overrides["min_stats"]         = ms
        xs = _parse_stat_pair_form(max_stat)
        if xs:         overrides["max_stats"]         = xs
        mcs = _parse_stat_pair_form(min_career_stat)
        if mcs:        overrides["min_career_stats"]  = mcs
        xcs = _parse_stat_pair_form(max_career_stat)
        if xcs:        overrides["max_career_stats"]  = xcs
        rounds_list = _parse_draft_rounds_form(draft_rounds)
        if rounds_list: overrides["draft_rounds"]     = rounds_list

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


def _parse_draft_rounds_form(s: str) -> list[int | str] | None:
    """Parse a comma-separated draft-rounds form input into the
    ``list[int | str]`` shape pos_topN/career_topN expect. Each token
    is either an int round number or the literal "undrafted". Empty
    input → None (no filter). Bad tokens are skipped silently."""
    s = (s or "").strip()
    if not s:
        return None
    out: list[int | str] = []
    for tok in (t.strip() for t in s.split(",")):
        if not tok:
            continue
        if tok.lower() == "undrafted":
            out.append("undrafted")
        else:
            try:
                out.append(int(tok))
            except ValueError:
                continue
    return out or None


def _parse_stat_pair_form(s: str) -> dict[str, float]:
    """Parse a single ``col=value`` string from a form input into
    ``{col: float(value)}``. Empty / malformed inputs return ``{}``
    so a typo silently drops the filter rather than 500'ing the page.
    The col name still gets validated downstream by the helpers
    against RANK_BY_ALLOWED, which surfaces a friendly error."""
    s = (s or "").strip()
    if not s or "=" not in s:
        return {}
    col, val = s.split("=", 1)
    col = col.strip()
    if not col:
        return {}
    try:
        return {col: float(val.strip())}
    except ValueError:
        return {}


def _run_ask(
    db_path: Path,
    *, kind: str,
    # shared
    n: int, position: str,
    start: int | None, end: int | None,
    college: str | None,
    first_name_contains: str | None, last_name_contains: str | None,
    draft_rounds: list[int | str] | None,
    drafted_by: str | None,
    draft_start: int | None, draft_end: int | None,
    ever_won: list[str] | None,
    min_career_stats: dict[str, float] | None = None,
    max_career_stats: dict[str, float] | None = None,
    # pos-top only
    rank_by: str = "fpts_ppr",
    team: str | None = None,
    division: str | None = None,
    conference: str | None = None,
    has_award: list[str] | None = None,
    rookie_only: bool = False,
    unique: bool = False,
    min_stats: dict[str, float] | None = None,
    max_stats: dict[str, float] | None = None,
    tiebreak_by: list[str] | None = None,
    show_awards: bool = False,
    show_context: bool = False,
    # career only
    career_mode: str = "rank_by",
    career_rank_by: str = "fpts_ppr",
    career_award: str | None = None,
    min_seasons: int | None = None,
) -> tuple[list[str], list[tuple], str]:
    """Run the chosen ask helper and return (columns, rows, page_label).

    ``kind`` selects which top-level helper:
      pos-top -> pos_topN — single-season top-N with all filters;
                 supports show_awards / show_context column augmentation.
      career  -> dispatched on career_mode:
                   "rank_by" (default) -> career_topN with SUM(stat).
                   "award"             -> award_topN with COUNT(*).
                 Career stat thresholds, year range, college, draft
                 filters, ever-won, and name contains compose with both
                 sub-modes; min_seasons applies only to rank_by mode.
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
                min_stats=min_stats, max_stats=max_stats,
                min_career_stats=min_career_stats,
                max_career_stats=max_career_stats,
                draft_rounds=draft_rounds,
                draft_start=draft_start, draft_end=draft_end,
                drafted_by=drafted_by,
                tiebreak_by=tiebreak_by,
            )
            label = (
                f"pos-top: top {n} {position} by {rank_by}"
                + (f" ({start}-{end})" if start and end else "")
            )
            cur = con.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            if show_awards or show_context:
                # Reuse the CLI's _augment_display so the same
                # award-aggregation + team-context join logic works
                # for the web. Keeps both surfaces aligned.
                from ffpts.cli import _augment_display
                rows, cols = _augment_display(con, rows, cols, show_awards, show_context)
        elif kind == "career":
            if career_mode == "award":
                if not career_award:
                    raise ValueError(
                        "career sub-mode 'award' requires an award type."
                    )
                sql, params = award_topN(
                    career_award, n=n, position=position,
                    college=college,
                    min_career_stats=min_career_stats,
                    max_career_stats=max_career_stats,
                    start=start, end=end,
                    ever_won_award=ever_won,
                    draft_rounds=draft_rounds,
                    drafted_by=drafted_by,
                    draft_start=draft_start, draft_end=draft_end,
                    first_name_contains=first_name_contains,
                    last_name_contains=last_name_contains,
                )
                label = f"career: top {n} {position} by {career_award} count"
            else:
                sql, params = career_topN(
                    career_rank_by, n=n, position=position,
                    start=start, end=end,
                    ever_won_award=ever_won, college=college,
                    first_name_contains=first_name_contains,
                    last_name_contains=last_name_contains,
                    min_career_stats=min_career_stats,
                    max_career_stats=max_career_stats,
                    draft_rounds=draft_rounds,
                    drafted_by=drafted_by,
                    draft_start=draft_start, draft_end=draft_end,
                    min_seasons=min_seasons,
                )
                label = f"career: top {n} {position} by SUM({career_rank_by})"
            cur = con.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        else:
            raise ValueError(f"unknown kind {kind!r}")
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
    # Two top-level kinds — pos-top and career. The radio toggle
    # drives a tiny inline JS that hides/shows the kind-specific
    # fieldset. Without JS the user just sees both and the handler
    # dispatches on `kind`.
    return f"""
<p>Run a single query. Pick <em>pos-top</em> to rank player-seasons,
or <em>career</em> to rank players by career total (or career award
count).</p>
<form method="post" action="/ask">
  <p style="font-size:1.2em">
    <label><input type="radio" name="kind" value="pos-top" checked
                  onchange="_kindToggle()"> <b>pos-top</b></label>
    &nbsp;&nbsp;&nbsp;
    <label><input type="radio" name="kind" value="career"
                  onchange="_kindToggle()"> <b>career</b></label>
  </p>

  <fieldset><legend>shared</legend>
    <p>n: <input type="number" name="n" value="10" min="1" max="200" size="4">
       position: <select name="position">{pos_opts}</select></p>
    <p>start: <input type="number" name="start" placeholder="1970" size="6">
       end:   <input type="number" name="end"   placeholder="2025" size="6"></p>
    <p>college: <input name="college" placeholder="Alabama" size="14">
       first-name has: <input name="first_name_contains" size="8">
       last-name has:  <input name="last_name_contains" size="8"></p>
    <p>draft-rounds: <input name="draft_rounds" placeholder="1 or 4,5 or undrafted" size="22">
       drafted-by: <input name="drafted_by" placeholder="PIT" size="6"></p>
    <p>draft-start: <input type="number" name="draft_start" size="6">
       draft-end:   <input type="number" name="draft_end" size="6"></p>
    <p>ever-won (any season): <select name="ever_won">{award_opts}</select>
       <small>(applies to both kinds; e.g. CPOY winners who also won MVP)</small></p>
    <p>min-career-stat: <input name="min_career_stat" placeholder="pass_yds=20000" size="16">
       max-career-stat: <input name="max_career_stat" placeholder="def_int=30" size="16">
       <small>(career SUM threshold; both kinds)</small></p>
  </fieldset>

  <fieldset id="kind-pos-top"><legend>pos-top only</legend>
    <p>rank-by: <select name="rank_by">{rank_opts}</select></p>
    <p>team: <input name="team" placeholder="PIT" size="6">
       division: <input name="division" placeholder="NFC North" size="14">
       conference: <input name="conference" placeholder="AFC" size="6"></p>
    <p>has-award (this season): <select name="has_award">{award_opts}</select></p>
    <p>min-stat: <input name="min_stat" placeholder="games=10" size="14">
       max-stat: <input name="max_stat" placeholder="rush_yds=999" size="14">
       <small>(per-season threshold)</small></p>
    <p>tiebreak-by (comma-separated): <input name="tiebreak_by" placeholder="draft_year,draft_round" size="22"></p>
    <p>
      <label><input type="checkbox" name="rookie_only" value="1"> rookie-only</label>
      &nbsp;
      <label><input type="checkbox" name="unique" value="1"> unique-by-player (best season)</label>
    </p>
    <p>
      <label><input type="checkbox" name="show_awards" value="1"> show awards column</label>
      &nbsp;
      <label><input type="checkbox" name="show_context" value="1"> show conf/div/franchise</label>
    </p>
  </fieldset>

  <fieldset id="kind-career"><legend>career only</legend>
    <p>sub-mode:
      <label><input type="radio" name="career_mode" value="rank_by" checked> rank by stat sum</label>
      &nbsp;
      <label><input type="radio" name="career_mode" value="award"> rank by award count</label>
    </p>
    <p>rank-by stat: <select name="career_rank_by">{rank_opts}</select>
       <small>(used when sub-mode = rank by stat sum)</small></p>
    <p>award: <select name="career_award">{award_opts}</select>
       <small>(used when sub-mode = rank by award count; e.g. AP_FIRST or HOF)</small></p>
    <p>min-seasons: <input type="number" name="min_seasons" size="4">
       <small>(blocks one-year-wonders; rank-by mode only)</small></p>
  </fieldset>

  <p><input type="submit" value="run" style="font-size:1.1em"></p>
</form>

<script>
// Tiny toggle so only the relevant fieldset shows. No build step,
// no framework — radio buttons drive the visibility directly.
function _kindToggle() {{
  var kind = document.querySelector('input[name="kind"]:checked').value;
  document.getElementById('kind-pos-top').style.display = (kind === 'pos-top') ? '' : 'none';
  document.getElementById('kind-career').style.display  = (kind === 'career')  ? '' : 'none';
}}
_kindToggle();
</script>
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
    # Trivia restricts rank_by — `age` and draft_* are valid for ask
    # queries but produce trivial trivia answer sets, so the dropdown
    # excludes them.
    rank_opts = _opts(sorted(TRIVIA_RANK_BY_ALLOWED))
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
     ever-won (any season):   <select name="ever_won">{award_opts}</select>
     <small>(HOF is in the list — covers Hall of Fame inductees.)</small></p>
  <p>college: <input name="college" placeholder="Alabama" size="14">
     <small>(substring match against players.college)</small></p>
  <p>draft-rounds: <input name="draft_rounds" placeholder="1 or 4,5 or undrafted" size="22"></p>
  <p>min-stat: <input name="min_stat" placeholder="games=10" size="14">
     max-stat: <input name="max_stat" placeholder="rush_yds=999" size="14">
     <small>(per-season; col=value)</small></p>
  <p>min-career-stat: <input name="min_career_stat" placeholder="pass_yds=20000" size="16">
     max-career-stat: <input name="max_career_stat" placeholder="def_int=30" size="16">
     <small>(career SUM; col=value)</small></p>
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
    # rank_by leaves blank so the random gen picks if not pinned.
    # Same trivia-rank-by restriction as the play form.
    rank_opts = _opts([""] + sorted(TRIVIA_RANK_BY_ALLOWED))
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
     has-award (this season): <select name="has_award">{award_opts}</select>
     ever-won (any season): <select name="ever_won">{award_opts}</select></p>
  <p>college: <input name="college" placeholder="Alabama" size="14">
     <small>(HOF is in the award lists; --ever-won HOF restricts to inductees.)</small></p>
  <p>draft-rounds: <input name="draft_rounds" placeholder="1 or 4,5 or undrafted" size="22"></p>
  <p>mode:
    <select name="mode">
      <option value="">(random — ~25% career)</option>
      <option value="season">season</option>
      <option value="career">career</option>
    </select>
  </p>
  <p>min-stat: <input name="min_stat" placeholder="games=10" size="14">
     max-stat: <input name="max_stat" placeholder="rush_yds=999" size="14">
     <small>(used when mode=season)</small></p>
  <p>min-career-stat: <input name="min_career_stat" placeholder="pass_yds=20000" size="16">
     max-career-stat: <input name="max_career_stat" placeholder="def_int=30" size="16">
     <small>(used when mode=career)</small></p>
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
