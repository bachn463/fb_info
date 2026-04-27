"""Unit + integration tests for the awards-derivation pipeline."""

from __future__ import annotations

import polars as pl
import pytest

from ffpts.ingest_awards import derive_awards


# --- derive_awards (unit) -----------------------------------------------


def test_empty_input_returns_empty_frame():
    df = pl.DataFrame()
    result = derive_awards(df)
    assert result.is_empty()
    assert set(result.columns) == {"player_id", "season", "award_type", "vote_finish"}


def test_no_awards_column_returns_empty_frame():
    df = pl.DataFrame({"player_id": ["p1"], "season": [2023]})
    result = derive_awards(df)
    assert result.is_empty()


def test_all_null_awards_returns_empty_frame():
    df = pl.DataFrame({
        "player_id": ["p1", "p2"],
        "season": [2023, 2023],
        "awards": [None, None],
    })
    result = derive_awards(df)
    assert result.is_empty()


def test_simple_pro_bowl():
    df = pl.DataFrame({
        "player_id": ["pfr:RiceJe00"],
        "season": [1985],
        "awards": ["PB"],
    })
    result = derive_awards(df).to_dicts()
    assert result == [
        {"player_id": "pfr:RiceJe00", "season": 1985,
         "award_type": "PB", "vote_finish": None},
    ]


def test_complex_awards_string_splits_to_multiple_rows():
    df = pl.DataFrame({
        "player_id": ["pfr:MariDa00"],
        "season": [1985],
        "awards": ["PB,AP-1"],
    })
    rows = sorted(derive_awards(df).to_dicts(), key=lambda r: r["award_type"])
    assert rows == [
        {"player_id": "pfr:MariDa00", "season": 1985,
         "award_type": "AP_FIRST", "vote_finish": None},
        {"player_id": "pfr:MariDa00", "season": 1985,
         "award_type": "PB", "vote_finish": None},
    ]


def test_voted_awards_carry_finish():
    df = pl.DataFrame({
        "player_id": ["pfr:TagoTu00"],
        "season": [2023],
        "awards": ["PB,AP CPoY-5"],
    })
    rows = sorted(derive_awards(df).to_dicts(), key=lambda r: r["award_type"])
    cpoy = next(r for r in rows if r["award_type"] == "CPOY")
    assert cpoy["vote_finish"] == 5


def test_dedup_when_same_player_appears_on_multiple_pages():
    """A QB shows up on both passing.htm and rushing.htm rows with
    the same awards string. derive_awards dedups by
    (player_id, season, award_type)."""
    df = pl.DataFrame({
        "player_id": ["pfr:MahoPa00", "pfr:MahoPa00"],
        "season":    [2023, 2023],
        "team":      ["KAN", "KAN"],
        "awards":    ["PB,AP MVP-7", "PB,AP MVP-7"],
    })
    rows = derive_awards(df).to_dicts()
    # PB + MVP, deduped from 2 rows -> 2 unique award entries.
    assert len(rows) == 2


def test_drops_rows_with_null_player_or_season():
    df = pl.DataFrame({
        "player_id": ["pfr:p1", None,        "pfr:p2"],
        "season":    [2023,     2023,        None],
        "awards":    ["PB",     "AP MVP-1",  "AP-1"],
    })
    rows = derive_awards(df).to_dicts()
    assert rows == [
        {"player_id": "pfr:p1", "season": 2023,
         "award_type": "PB", "vote_finish": None},
    ]


# --- Pipeline integration -----------------------------------------------


def _scraper():
    from tests.test_ingest_pfr import _FixtureScraper
    return _FixtureScraper()


@pytest.fixture
def populated_db():
    from ffpts.db import apply_schema, connect
    from ffpts.pipeline import build

    con = connect(None)
    apply_schema(con)
    summary = build(seasons=[1985], con=con, pfr_scraper=_scraper())
    yield con, summary
    con.close()


def test_pipeline_populates_player_awards_for_1985(populated_db):
    con, summary = populated_db
    # 1985 has lots of Pro Bowl + AP picks.
    assert summary["player_awards_rows"] > 0
    n_pb = con.execute(
        "SELECT COUNT(*) FROM player_awards WHERE season = 1985 AND award_type = 'PB'"
    ).fetchone()[0]
    assert n_pb > 0


def test_pipeline_marino_1985_has_pb_and_ap_first(populated_db):
    """Marino 1985 awards = 'PB,AP-1' — both should land in player_awards."""
    con, _ = populated_db
    rows = con.execute(
        """
        SELECT pa.award_type, pa.vote_finish
        FROM   player_awards pa
        JOIN   players p USING (player_id)
        WHERE  p.name = 'Dan Marino' AND pa.season = 1985
        ORDER BY pa.award_type
        """
    ).fetchall()
    assert ("PB", None) in rows
    assert ("AP_FIRST", None) in rows


def test_pipeline_payton_1985_has_some_awards(populated_db):
    """Walter Payton 1985 made the Pro Bowl."""
    con, _ = populated_db
    rows = con.execute(
        """
        SELECT pa.award_type
        FROM   player_awards pa
        JOIN   players p USING (player_id)
        WHERE  p.name = 'Walter Payton' AND pa.season = 1985
        """
    ).fetchall()
    award_types = {r[0] for r in rows}
    assert "PB" in award_types


def test_pipeline_v_award_winners_view_works(populated_db):
    con, _ = populated_db
    # Sanity: any 1985 MVP candidates? Marino was 1984 MVP; 1985 MVP was
    # Marcus Allen (Raiders RB). Either way at least one MVP entry should
    # exist for 1985.
    rows = con.execute(
        """
        SELECT name, vote_finish
        FROM   v_award_winners
        WHERE  season = 1985 AND award_type = 'MVP'
        ORDER BY vote_finish
        """
    ).fetchall()
    assert len(rows) >= 1


def test_pipeline_wpmoy_landed_for_1985_dwight_stephenson(populated_db):
    """WPMOY (Walter Payton Man of the Year) is on PFR's per-year
    summary page, not in stat-table awards cells. The pipeline now
    parses it from /years/YYYY/ and inserts into player_awards
    alongside the inline awards."""
    con, _ = populated_db
    rows = con.execute(
        "SELECT name FROM v_award_winners "
        "WHERE  award_type = 'WPMOY' AND season = 1985"
    ).fetchall()
    assert rows == [("Dwight Stephenson",)]


def test_pipeline_player_awards_idempotent_on_rerun():
    """Re-running build for the same season replaces awards cleanly."""
    from ffpts.db import apply_schema, connect
    from ffpts.pipeline import build

    con = connect(None)
    apply_schema(con)
    try:
        build(seasons=[1985], con=con, pfr_scraper=_scraper())
        n_before = con.execute("SELECT COUNT(*) FROM player_awards").fetchone()[0]
        build(seasons=[1985], con=con, pfr_scraper=_scraper())
        n_after = con.execute("SELECT COUNT(*) FROM player_awards").fetchone()[0]
        assert n_before == n_after
    finally:
        con.close()
