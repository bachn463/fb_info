"""Tests for the PFR parser base helpers (synthetic HTML — no fixtures)."""

from ffpts.parsers import (
    extract_player_slug,
    extract_table_rows,
    extract_team_slug,
    unwrap_pfr_comments,
)


# --- unwrap_pfr_comments -------------------------------------------------


def test_unwrap_strips_comment_around_table():
    html = "<div>before</div><!-- <table id='x'><tr><td>1</td></tr></table> --><div>after</div>"
    out = unwrap_pfr_comments(html)
    # The <table> should now be in the markup directly, not commented.
    assert "<!--" not in out
    assert "<table id='x'>" in out


def test_unwrap_leaves_unrelated_comments_alone():
    html = "<!-- analytics tag --><table id='real'></table>"
    out = unwrap_pfr_comments(html)
    # The analytics comment has no <table>, so we don't touch it.
    assert "<!-- analytics tag -->" in out


def test_unwrap_handles_multiline_block():
    html = """
    <div>
    <!--
       <div class="table_wrapper">
         <table id="passing">
           <tr><th>Player</th></tr>
         </table>
       </div>
    -->
    </div>
    """
    out = unwrap_pfr_comments(html)
    assert "<table id=\"passing\">" in out
    # The <!-- and --> markers around the table are gone.
    assert "<!--" not in out


def test_unwrap_idempotent():
    html = "<table id='x'><tr><td>1</td></tr></table>"
    assert unwrap_pfr_comments(html) == html


# --- extract_player_slug / extract_team_slug -----------------------------


def test_extract_player_slug_from_link():
    from bs4 import BeautifulSoup
    cell = BeautifulSoup(
        '<td data-stat="player"><a href="/players/M/McCaCh01.htm">Christian McCaffrey</a></td>',
        "lxml",
    ).find("td")
    assert extract_player_slug(cell) == "McCaCh01"


def test_extract_player_slug_returns_none_when_no_link():
    from bs4 import BeautifulSoup
    cell = BeautifulSoup('<td data-stat="player">League average</td>', "lxml").find("td")
    assert extract_player_slug(cell) is None


def test_extract_team_slug_returns_franchise_url_slug():
    from bs4 import BeautifulSoup
    cell = BeautifulSoup(
        '<td data-stat="team"><a href="/teams/sfo/2023.htm">SFO</a></td>',
        "lxml",
    ).find("td")
    # franchise slug is the lower-case 3-letter team code from /teams/<slug>/
    assert extract_team_slug(cell) == "sfo"


def test_extract_team_slug_returns_none_for_2tm_summary():
    from bs4 import BeautifulSoup
    cell = BeautifulSoup('<td data-stat="team">2TM</td>', "lxml").find("td")
    assert extract_team_slug(cell) is None


# --- extract_table_rows --------------------------------------------------


def _make_passing_table():
    return """
    <div>
    <!--
      <table id="passing">
        <thead>
          <tr><th data-stat="player">Player</th>
              <th data-stat="team">Tm</th>
              <th data-stat="pass_yds">Yds</th>
              <th data-stat="pass_td">TD</th></tr>
        </thead>
        <tbody>
          <tr>
            <td data-stat="player"><a href="/players/M/MahoPa00.htm">Patrick Mahomes</a></td>
            <td data-stat="team"><a href="/teams/kan/2023.htm">KAN</a></td>
            <td data-stat="pass_yds">4183</td>
            <td data-stat="pass_td">27</td>
          </tr>
          <tr class="thead">
            <td data-stat="player">Player</td>
            <td data-stat="team">Tm</td>
            <td data-stat="pass_yds">Yds</td>
            <td data-stat="pass_td">TD</td>
          </tr>
          <tr>
            <td data-stat="player"><a href="/players/A/AlleJo02.htm">Josh Allen</a></td>
            <td data-stat="team"><a href="/teams/buf/2023.htm">BUF</a></td>
            <td data-stat="pass_yds">4306</td>
            <td data-stat="pass_td">29</td>
          </tr>
          <tr>
            <td data-stat="player">League Average</td>
            <td data-stat="team"></td>
            <td data-stat="pass_yds">3500</td>
            <td data-stat="pass_td">22</td>
          </tr>
        </tbody>
      </table>
    -->
    </div>
    """


def test_extract_table_returns_one_dict_per_data_row():
    rows = extract_table_rows(_make_passing_table(), "passing")
    # Two real player rows; the thead row was skipped, the
    # league-average row stays (no class="thead", just a non-link row).
    assert len(rows) == 3


def test_extract_table_keys_are_data_stat_attributes():
    rows = extract_table_rows(_make_passing_table(), "passing")
    mahomes = rows[0]
    assert mahomes["pass_yds"] == "4183"
    assert mahomes["pass_td"] == "27"
    # Player text + slug
    assert mahomes["player"] == "Patrick Mahomes"
    assert mahomes["_player_slug"] == "MahoPa00"
    # Team text + slug
    assert mahomes["team"] == "KAN"
    assert mahomes["_team_slug"] == "kan"


def test_extract_table_skips_repeated_thead_rows():
    rows = extract_table_rows(_make_passing_table(), "passing")
    # No row should equal "Player" (the thead repeat).
    for r in rows:
        assert r.get("player") != "Player"


def test_extract_table_handles_summary_rows_without_links():
    rows = extract_table_rows(_make_passing_table(), "passing")
    avg = next(r for r in rows if r.get("player") == "League Average")
    # No slug (no link), but the cell text is still captured.
    assert "_player_slug" not in avg
    assert avg["pass_yds"] == "3500"


def test_extract_table_unknown_id_returns_empty_list():
    rows = extract_table_rows(_make_passing_table(), "rushing")
    assert rows == []


def test_extract_table_handles_uncommented_html():
    """If a future PFR change drops the comment wrapping, parsers still work."""
    html = """
    <table id="x">
      <tbody>
        <tr><td data-stat="foo">v1</td></tr>
        <tr><td data-stat="foo">v2</td></tr>
      </tbody>
    </table>
    """
    rows = extract_table_rows(html, "x")
    assert [r["foo"] for r in rows] == ["v1", "v2"]


def test_extract_table_treats_th_with_data_stat_like_a_cell():
    """PFR uses <th data-stat="player"> in some tables."""
    html = """
    <!--
    <table id="t">
      <tbody>
        <tr>
          <th data-stat="player"><a href="/players/X/XxxxYy00.htm">Xx</a></th>
          <td data-stat="rush_yds">100</td>
        </tr>
      </tbody>
    </table>
    -->
    """
    rows = extract_table_rows(html, "t")
    assert rows[0]["_player_slug"] == "XxxxYy00"
    assert rows[0]["rush_yds"] == "100"
