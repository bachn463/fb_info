"""Persistence for trivia game specs so a game can be replayed later.

Every ``fb_info trivia`` invocation writes a JSON file under
``data/trivia_history/`` recording the resolved template — enough info
to reproduce the exact same game on a subsequent ``trivia replay <id>``.
The persisted shape is the same template dict that
``_resolve_template`` consumes (rank_by, n, position, mode, plus all
optional filter keys), so replay is just "load + run".

IDs are zero-padded ascending integers (000001, 000002, ...) — short
enough to type, monotonically increasing so the most recent game is
the highest number.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_HISTORY_DIR = Path("data/trivia_history")


def _next_id(history_dir: Path) -> str:
    """Pick the next zero-padded integer ID by counting existing files.

    Counting beats max(...)+1 because it's robust to manually-deleted
    middle files: a 6-digit pad gives plenty of room (10^6 games)."""
    history_dir.mkdir(parents=True, exist_ok=True)
    existing = list(history_dir.glob("*.json"))
    return f"{len(existing) + 1:06d}"


def save_spec(
    template: dict,
    *,
    label: str,
    history_dir: Path = DEFAULT_HISTORY_DIR,
) -> str:
    """Persist a trivia template + label to the history directory.
    Returns the assigned game ID. ``label`` is the short human tag
    that was printed at game start (``play``, ``random``, ``daily for
    YYYY-MM-DD``)."""
    history_dir.mkdir(parents=True, exist_ok=True)
    game_id = _next_id(history_dir)
    spec: dict[str, Any] = {
        "id":       game_id,
        "label":    label,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "template": template,
    }
    (history_dir / f"{game_id}.json").write_text(json.dumps(spec, indent=2))
    return game_id


def load_spec(
    game_id: str, *, history_dir: Path = DEFAULT_HISTORY_DIR,
) -> dict:
    """Load a saved spec by ID. Accepts either the padded form
    (``"000042"``) or a bare integer-string (``"42"``). Raises
    ``FileNotFoundError`` if no match."""
    history_dir.mkdir(parents=True, exist_ok=True)
    direct = history_dir / f"{game_id}.json"
    if direct.exists():
        return json.loads(direct.read_text())
    try:
        n = int(game_id)
    except ValueError as e:
        raise FileNotFoundError(
            f"trivia game id {game_id!r} not in {history_dir}"
        ) from e
    padded = history_dir / f"{n:06d}.json"
    if padded.exists():
        return json.loads(padded.read_text())
    raise FileNotFoundError(
        f"trivia game id {game_id!r} (or {n:06d}) not in {history_dir}"
    )


def list_recent(
    *, n: int = 20, history_dir: Path = DEFAULT_HISTORY_DIR,
) -> list[dict]:
    """Return the N most-recent saved specs, newest first.

    Newest-first by filename — IDs are monotonically increasing so the
    sort is exact. Skips files that fail to parse rather than raising
    (a corrupt entry shouldn't break ``trivia history``)."""
    history_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(history_dir.glob("*.json"), reverse=True)[:n]
    out: list[dict] = []
    for f in files:
        try:
            out.append(json.loads(f.read_text()))
        except (json.JSONDecodeError, OSError):
            continue
    return out
