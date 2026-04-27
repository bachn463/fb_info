"""Parse PFR's inline `awards` cell into structured records.

PFR stat tables (passing.htm, rushing.htm, ...) have a per-row
``data-stat="awards"`` cell with a comma-separated list of tokens like::

    "PB,AP-1,AP MVP-1,AP OPoY-2"
    "PB,AP CPoY-5"
    "WPMOY"

This module turns that into a list of ``{award_type, vote_finish}``
dicts ready for insertion into the ``player_awards`` table.

Pure function, no I/O. Unknown tokens are ignored (logged via the
caller's discretion) so the parser doesn't crash on a yet-uncatalogued
PFR string.
"""

from __future__ import annotations

import re

# AP voted awards: ``AP <token>-<finish>`` -> our award_type.
_AP_VOTED_AWARDS: dict[str, str] = {
    "MVP":  "MVP",
    "OPoY": "OPOY",
    "DPoY": "DPOY",
    "OROY": "OROY",
    "DROY": "DROY",
    "CPoY": "CPOY",
}

# Binary tokens (no vote_finish).
_BINARY_TOKENS: dict[str, str] = {
    "PB":     "PB",
    "AP-1":   "AP_FIRST",
    "AP-2":   "AP_SECOND",
    "WPMOY":  "WPMOY",
}

# Match e.g. "AP MVP-1", "AP CPoY-5". Groups: token, finish.
_AP_VOTED_RE = re.compile(r"^AP\s+(MVP|OPoY|DPoY|OROY|DROY|CPoY)-(\d+)$")


def parse_awards_string(raw: str | None) -> list[dict]:
    """``"PB,AP MVP-1,AP CPoY-5"`` ->
    ``[{award_type:'PB', vote_finish:None},
       {award_type:'MVP', vote_finish:1},
       {award_type:'CPOY', vote_finish:5}]``

    Returns ``[]`` for ``None`` or empty input. Unknown tokens are
    silently skipped so the parser tolerates new PFR award strings
    we haven't catalogued yet.
    """
    if not raw:
        return []
    out: list[dict] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if token in _BINARY_TOKENS:
            out.append({"award_type": _BINARY_TOKENS[token], "vote_finish": None})
            continue
        m = _AP_VOTED_RE.match(token)
        if m:
            ap_label, finish = m.group(1), int(m.group(2))
            out.append(
                {
                    "award_type": _AP_VOTED_AWARDS[ap_label],
                    "vote_finish": finish,
                }
            )
            continue
        # Unknown token — silently skipped.
    return out
