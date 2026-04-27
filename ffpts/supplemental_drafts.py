"""Hand-encoded NFL Supplemental Draft picks since 1977.

The supplemental draft is a separate annual event that neither nflverse's
``import_draft_picks`` nor PFR's ``/years/YYYY/draft.htm`` page covers.
Players selected through it (Cris Carter, Bernie Kosar, Brian Bosworth,
Rob Moore, Ahmad Brooks, Josh Gordon, Sam Beal, ...) end up with NULL
``draft_round`` in our ``v_player_season_full`` view via the LEFT JOIN
to ``draft_picks`` — i.e. they incorrectly appear as undrafted in
``ask pos-top --draft-rounds undrafted`` queries.

This module fills that gap with a hand-encoded list. ``overall_pick``
is set to the ``SUPP_PICK_SENTINEL`` (=0) since the supplemental draft
doesn't have a meaningful overall-pick number; year + round + team are
authoritative.

The list isn't exhaustive — it covers the well-known supp picks who
had real NFL careers. Add to ``SUPPLEMENTAL_DRAFTS`` to extend.

Lookup happens by player display name in ``players`` after stats are
loaded, so we automatically pick up whichever ``player_id`` the player
has in your DB (``pfr:<slug>`` for pre-1999-only careers, GSIS for
1999+ careers, or both for players who span the boundary — the supp
draft entry is inserted for each matching player_id).
"""

from __future__ import annotations

from dataclasses import dataclass

# Supplemental drafts have no meaningful "overall pick"; use 0 so a
# query like `ORDER BY overall_pick` clearly sorts them apart.
SUPP_PICK_SENTINEL = 0


@dataclass(frozen=True)
class SuppDraft:
    """One NFL Supplemental Draft pick.

    ``name`` matches the player's display name in ``players.name`` (the
    ``player_display_name`` column from nflverse, or the ``name`` from
    PFR's draft / stat tables — both produce the conventional spelling).
    """

    name: str
    year: int
    round: int
    team: str


SUPPLEMENTAL_DRAFTS: list[SuppDraft] = [
    # 1984 NFL Supplemental Draft of USFL/CFL Players (a one-time
    # event prompted by the USFL's existence; not a regular yearly
    # supp draft, but the resolution mechanism is the same). Players
    # didn't actually join the NFL team until their USFL contracts
    # expired (typically 1985).
    SuppDraft("Steve Young",      1984, 1, "TAM"),
    SuppDraft("Reggie White",     1984, 1, "PHI"),
    SuppDraft("Mike Rozier",      1984, 1, "HOU"),
    SuppDraft("Gary Zimmerman",   1984, 3, "NYG"),

    # 1985 NFL Supplemental Draft (also a special one-time event for
    # USFL alumni in the year of the league's collapse).
    SuppDraft("Bernie Kosar",     1985, 1, "CLE"),

    # 1987 NFL Supplemental Draft
    SuppDraft("Brian Bosworth",   1987, 1, "SEA"),
    SuppDraft("Cris Carter",      1987, 4, "PHI"),

    # 1989 NFL Supplemental Draft
    SuppDraft("Steve Walsh",      1989, 1, "DAL"),
    SuppDraft("Timm Rosenbach",   1989, 1, "PHX"),

    # 1990 NFL Supplemental Draft
    SuppDraft("Rob Moore",        1990, 1, "NYJ"),

    # 2006 NFL Supplemental Draft
    SuppDraft("Ahmad Brooks",     2006, 3, "CIN"),

    # 2007 NFL Supplemental Draft
    SuppDraft("Jared Gaither",    2007, 5, "BAL"),
    SuppDraft("Paul Oliver",      2007, 4, "SDG"),

    # 2009 NFL Supplemental Draft
    SuppDraft("Jeremy Jarmon",    2009, 3, "WAS"),

    # 2010 NFL Supplemental Draft
    SuppDraft("Harvey Unga",      2010, 7, "CHI"),

    # 2011 NFL Supplemental Draft
    SuppDraft("Terrelle Pryor",   2011, 3, "OAK"),

    # 2012 NFL Supplemental Draft
    SuppDraft("Josh Gordon",      2012, 2, "CLE"),

    # 2018 NFL Supplemental Draft
    SuppDraft("Sam Beal",         2018, 3, "NYG"),
    SuppDraft("Adonis Alexander", 2018, 6, "WAS"),

    # 2019 NFL Supplemental Draft
    SuppDraft("Jalen Thompson",   2019, 5, "ARI"),
]


@dataclass(frozen=True)
class CollegeOverride:
    """Hand-encoded college history for players whose ``college`` is
    missing or incomplete in the auto-scraped data.

    Three reasons a player needs an override:
      1. Transfers — PFR's draft page lists only the drafted-from
         school, so transfers like Jalen Hurts (Alabama -> Oklahoma)
         lose their pre-transfer history.
      2. Supplemental-draft picks — our SUPPLEMENTAL_DRAFTS rows
         insert into draft_picks without a college value, since the
         supp-draft data we encode is just (round, team, year).
      3. Undrafted free agents — no draft_picks row at all, so the
         college column is NULL for them.

    ``colleges`` is an ordered tuple from earliest school to most
    recent (transfer chronology). Stored joined with ", " into
    ``players.college`` so the existing ``--college`` ILIKE filter
    catches any of the listed schools.

    ``player_id`` is optional; if omitted the override matches by
    ``name`` alone. Provide it when two players share a display name.
    """

    name: str
    colleges: tuple[str, ...]
    player_id: str | None = None


# Hand-curated overrides. Add to this list to extend coverage.
KNOWN_COLLEGE_OVERRIDES: list[CollegeOverride] = [
    # --- Famous post-2010 transfers (drafted-from college first wasn't
    # the only school they attended) ---
    CollegeOverride("Cam Newton",        ("Florida", "Blinn College", "Auburn")),
    CollegeOverride("Russell Wilson",    ("NC State", "Wisconsin")),
    CollegeOverride("Baker Mayfield",    ("Texas Tech", "Oklahoma")),
    CollegeOverride("Kyler Murray",      ("Texas A&M", "Oklahoma")),
    CollegeOverride("Joe Burrow",        ("Ohio State", "LSU")),
    CollegeOverride("Jalen Hurts",       ("Alabama", "Oklahoma")),
    CollegeOverride("Justin Fields",     ("Georgia", "Ohio State")),
    CollegeOverride("Bo Nix",            ("Auburn", "Oregon")),
    CollegeOverride("Caleb Williams",    ("Oklahoma", "USC")),
    CollegeOverride("Jayden Daniels",    ("Arizona State", "LSU")),
    CollegeOverride("Michael Penix Jr.", ("Indiana", "Washington")),
    CollegeOverride("Geno Smith",        ("West Virginia",)),  # no transfer; defensive fill
    CollegeOverride("Jameis Winston",    ("Florida State",)),
    CollegeOverride("Kenny Pickett",     ("Pittsburgh",)),
    CollegeOverride("Jordan Love",       ("Utah State",)),

    # --- Missing-data fills for HOFers / pre-1994 players where the
    # college didn't carry through (supplemental-draft picks like
    # Reggie White, undrafted FAs like Cliff Harris) ---
    CollegeOverride("Reggie White",      ("Tennessee",)),
    CollegeOverride("Cliff Harris",      ("Ouachita Baptist",)),
    CollegeOverride("Steve Young",       ("BYU",)),
    CollegeOverride("Cris Carter",       ("Ohio State",)),
    CollegeOverride("Bernie Kosar",      ("Miami (FL)",)),
    CollegeOverride("Mike Rozier",       ("Nebraska",)),
    CollegeOverride("Gary Zimmerman",    ("Oregon",)),
    CollegeOverride("Brian Bosworth",    ("Oklahoma",)),
    CollegeOverride("Steve Walsh",       ("Miami (FL)",)),
    CollegeOverride("Rob Moore",         ("Syracuse",)),
    CollegeOverride("Josh Gordon",       ("Baylor", "Utah"),
                    player_id="pfr:GordJo02"),
    CollegeOverride("Terrelle Pryor",    ("Ohio State",)),
    CollegeOverride("Ahmad Brooks",      ("Virginia",)),
]
