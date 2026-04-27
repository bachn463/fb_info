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

# AP voted awards: lowercase label -> our canonical award_type. Match
# is case-insensitive because PFR's actual format mixes cases:
#   "AP MVP-1"   (all caps)
#   "AP OPoY-1"  ("o" lowercase between caps)
#   "AP DRoY-1"  ("o" lowercase) — *not* "DROY"
#   "AP ORoY-1"  ("o" lowercase) — *not* "OROY"
# The previous strict regex silently dropped DRoY/ORoY rows. Now we
# match `[A-Za-z]+` and look up the lowercased label, so any case
# variant (OROY / OROY / ORoY / oroy) lands on the same canonical
# award_type.
_AP_VOTED_LABELS: dict[str, str] = {
    "mvp":  "MVP",
    "opoy": "OPOY",
    "dpoy": "DPOY",
    "oroy": "OROY",
    "droy": "DROY",
    "cpoy": "CPOY",
}

# Binary tokens (no vote_finish). Matched case-insensitively at the
# call site.
_BINARY_TOKENS: dict[str, str] = {
    "pb":     "PB",
    "ap-1":   "AP_FIRST",
    "ap-2":   "AP_SECOND",
    "wpmoy":  "WPMOY",
}

# Match e.g. "AP MVP-1", "AP CPoY-5". Groups: token, finish.
# Case-insensitive label so PFR's mixed-case forms all parse.
_AP_VOTED_RE = re.compile(r"^AP\s+([A-Za-z]+)-(\d+)$")


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
        lower = token.lower()
        if lower in _BINARY_TOKENS:
            out.append({"award_type": _BINARY_TOKENS[lower], "vote_finish": None})
            continue
        m = _AP_VOTED_RE.match(token)
        if m:
            label = m.group(1).lower()
            finish = int(m.group(2))
            if label in _AP_VOTED_LABELS:
                out.append(
                    {
                        "award_type": _AP_VOTED_LABELS[label],
                        "vote_finish": finish,
                    }
                )
            continue
        # Unknown token — silently skipped.
    return out
