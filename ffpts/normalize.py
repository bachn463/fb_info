"""Reference data for normalizing PFR rows.

Two pieces:

1. **Franchise mapping.** PFR uses a stable 3-letter URL slug per
   franchise (e.g. ``crd`` for the Cardinals across STL/PHO/ARI eras,
   ``rai`` for the Raiders across OAK/LA/LVR). Stat-table cells link
   the displayed team code to ``/teams/<slug>/<year>.htm`` — parsers
   resolve the slug from the link and we map it here to:
     - a stable ``franchise`` key ("cardinals", "rams", ...) for
       all-time / current-franchise queries, and
     - the franchise's *current* display code ("ARI", "LAR", ...).

   Per-season historical display codes (e.g. "STL" in 1990, "ARI" in
   2010) are kept verbatim from PFR in the stat rows themselves.

2. **Position aliases.** PFR mixes single-position values ("RB") with
   slash/hyphen combos ("FB-RB", "WR-KR") and old-school spellings
   ("HB"). ``normalize_position`` collapses these to the modern
   primary-position label.

Division/conference per ``(team, season)`` is *not* hand-encoded here;
it's loaded from PFR standings pages by ``parsers/teams.py`` so the
realignment history (1970, 1976, 1977, 1995, 1999, 2002, ...) stays
authoritative without a fragile constants table.
"""

from __future__ import annotations

# PFR franchise URL slug -> (stable franchise key, current display code).
# Slugs come from the team-page URL pattern /teams/<slug>/<year>.htm and
# are stable across relocations and renames.
PFR_FRANCHISE: dict[str, tuple[str, str]] = {
    # AFC East
    "buf": ("bills",      "BUF"),
    "mia": ("dolphins",   "MIA"),
    "nwe": ("patriots",   "NWE"),
    "nyj": ("jets",       "NYJ"),
    # AFC North
    "rav": ("ravens",     "BAL"),
    "cin": ("bengals",    "CIN"),
    "cle": ("browns",     "CLE"),
    "pit": ("steelers",   "PIT"),
    # AFC South
    "htx": ("texans",     "HOU"),
    "clt": ("colts",      "IND"),  # BAL 1970-1983, IND 1984+
    "jax": ("jaguars",    "JAX"),
    "oti": ("titans",     "TEN"),  # HOU 1970-1996 Oilers, TEN 1997+
    # AFC West
    "den": ("broncos",    "DEN"),
    "kan": ("chiefs",     "KAN"),
    "rai": ("raiders",    "LVR"),  # OAK 1970-81, LARM 82-94, OAK 95-19, LVR 20+
    "sdg": ("chargers",   "LAC"),  # SDG 1970-2016, LAC 2017+
    # NFC East
    "dal": ("cowboys",    "DAL"),
    "nyg": ("giants",     "NYG"),
    "phi": ("eagles",     "PHI"),
    "was": ("commanders", "WAS"),
    # NFC North
    "chi": ("bears",      "CHI"),
    "det": ("lions",      "DET"),
    "gnb": ("packers",    "GNB"),
    "min": ("vikings",    "MIN"),
    # NFC South
    "atl": ("falcons",    "ATL"),
    "car": ("panthers",   "CAR"),
    "nor": ("saints",     "NOR"),
    "tam": ("buccaneers", "TAM"),
    # NFC West
    "ram": ("rams",       "LAR"),  # LAR 1970-94, STL 95-15, LAR 16+
    "sfo": ("49ers",      "SFO"),
    "sea": ("seahawks",   "SEA"),
    "crd": ("cardinals",  "ARI"),  # STL 1970-87, PHO 88-93, ARI 94+
}


def franchise_for_slug(slug: str) -> str | None:
    """Return the stable franchise key for a PFR URL slug, or None."""
    entry = PFR_FRANCHISE.get(slug.lower())
    return entry[0] if entry else None


def current_team_code_for_slug(slug: str) -> str | None:
    """Return the current display code for a PFR URL slug, or None."""
    entry = PFR_FRANCHISE.get(slug.lower())
    return entry[1] if entry else None


# Position aliases: PFR row values -> our canonical position label.
# Multi-position cells use the first listed position as primary.
_POSITION_ALIASES: dict[str, str] = {
    "HB": "RB",
    "TB": "RB",   # tailback
    "FB-RB": "FB",
    "RB-FB": "RB",
    "WR-KR": "WR",
    "KR-WR": "WR",
    "WR-PR": "WR",
    "PR-WR": "WR",
    "TE-FB": "TE",
    "FB-TE": "FB",
    "DB":  "DB",
    "DL":  "DL",
    "LB":  "LB",
    "OL":  "OL",
}


def normalize_position(raw: str | None) -> str | None:
    """Collapse PFR's mixed position labels to a single canonical token.

    Returns None for empty/None inputs. Unknown values are upper-cased
    and returned as-is so we don't silently drop a position we haven't
    seen yet — the call site can audit.
    """
    if raw is None:
        return None
    cleaned = raw.strip().upper()
    if not cleaned:
        return None
    if cleaned in _POSITION_ALIASES:
        return _POSITION_ALIASES[cleaned]
    # Combo positions not in the table: take the first slash/hyphen part.
    for sep in ("/", "-"):
        if sep in cleaned:
            return cleaned.split(sep, 1)[0]
    return cleaned
