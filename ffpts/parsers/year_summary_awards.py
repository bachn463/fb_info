"""Parse PFR's per-year ``/years/YYYY/`` summary page for awards that
don't appear in the per-row ``awards`` cell on stat tables.

The Walter Payton Man of the Year (WPMOY) award is the main one — PFR
records it inline on the year-summary page only::

    <strong><a href="/awards/walter-payton-man-of-the-year.htm">
      Walter Payton Man of the Year
    </a></strong>: <a href="/players/H/HeywCa01.htm">Cameron Heyward</a>

The awards section sits inside an HTML comment block that does NOT
contain a ``<table>``, so the generic ``unwrap_pfr_comments`` helper
(which only unwraps comments wrapping tables) leaves it hidden from
BS4. Rather than make the unwrap helper less precise — and risk
unwrapping unrelated comments — we use a targeted regex against the
raw HTML for this one pattern.

Returns the same shape as ``ingest_awards.derive_awards``: a list of
dicts with keys ``player_id, season, award_type, vote_finish`` so the
pipeline can ``INSERT BY NAME`` directly.
"""

from __future__ import annotations

import re

# Heading end -> any markup -> the next /players/ link, capturing both
# the slug (group 1) and the player display name (group 2). Many WPMOY
# winners are offensive linemen / kickers / specialists who don't show
# up on our parsed stat pages, so capturing the name lets the pipeline
# insert a `players` row for them — otherwise the INNER JOIN in
# v_award_winners drops them.
_WPMOY_RE = re.compile(
    r"Walter Payton Man of the Year</a></strong>"
    r".*?"
    r"<a\s+href=\"/players/[A-Z]/([A-Za-z0-9.]+)\.htm\""
    r"[^>]*>"
    r"([^<]+)"
    r"</a>",
    re.DOTALL,
)


def parse_year_summary_awards(html: str, season: int) -> list[dict]:
    """One row per recognized award on the year-summary page.

    Each row has the four ``player_awards`` columns plus a ``name``
    field carrying the player display name — needed because some
    winners (Cs, Ks, Ps, etc.) don't have stats rows on any of our
    parsed pages, so the pipeline upserts the ``players`` row from
    this name to keep ``v_award_winners`` joinable.
    """
    out: list[dict] = []
    m = _WPMOY_RE.search(html)
    if m:
        out.append(
            {
                "player_id":   f"pfr:{m.group(1)}",
                "name":        m.group(2).strip(),
                "season":      season,
                "award_type":  "WPMOY",
                "vote_finish": None,
            }
        )
    return out
