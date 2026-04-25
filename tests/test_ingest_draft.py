import polars as pl

from ffpts.ingest import load_draft_picks, transform_draft_picks


# Three sample draft picks: a real player, a duplicate gsis_id (later
# row should be discarded), and a row with no gsis_id (should be dropped).
SAMPLE_DRAFT_ROWS = [
    {
        "season": 2017, "round": 1, "pick": 8, "team": "CAR",
        "gsis_id": "00-0033280", "pfr_player_id": "McCaCh01",
        "pfr_player_name": "Christian McCaffrey", "position": "RB",
    },
    {
        # Duplicate gsis_id — only the earlier season should survive.
        "season": 2018, "round": 7, "pick": 250, "team": "DET",
        "gsis_id": "00-0033280", "pfr_player_id": "McCaCh01",
        "pfr_player_name": "Christian McCaffrey", "position": "RB",
    },
    {
        "season": 1976, "round": 8, "pick": 211, "team": "MIN",
        "gsis_id": None, "pfr_player_id": None,
        "pfr_player_name": "Some Person", "position": "DB",
    },
    {
        "season": 2020, "round": 1, "pick": 1, "team": "CIN",
        "gsis_id": "00-0036442", "pfr_player_id": "BurrJo01",
        "pfr_player_name": "Joe Burrow", "position": "QB",
    },
]


def _df(rows):
    return pl.DataFrame(rows)


def test_transform_draft_renames_columns_and_drops_missing_ids():
    df = transform_draft_picks(_df(SAMPLE_DRAFT_ROWS))
    # 4 input rows -> 2 unique gsis_ids (CMC + Burrow); the missing-id
    # row is dropped, and CMC's duplicate is collapsed to one row.
    assert df.height == 2
    assert sorted(df["player_id"].to_list()) == ["00-0033280", "00-0036442"]
    assert "year" in df.columns
    assert "overall_pick" in df.columns
    assert "round" in df.columns
    # Player name carried through for the players-table upsert.
    assert "name" in df.columns


def test_transform_draft_keeps_first_seen_when_duplicates():
    df = transform_draft_picks(_df(SAMPLE_DRAFT_ROWS))
    cmc = df.filter(pl.col("player_id") == "00-0033280")
    # The 2017 R1 pick survives; the bogus 2018 R7 duplicate is gone.
    assert cmc["year"][0] == 2017
    assert cmc["round"][0] == 1
    assert cmc["overall_pick"][0] == 8
    assert cmc["team"][0] == "CAR"


def test_load_draft_picks_uses_injected_loader_and_through_season_filter():
    captured = {"called": 0}

    def fake_loader():
        captured["called"] += 1
        return _df(SAMPLE_DRAFT_ROWS)

    # No filter: 2 rows.
    df_all = load_draft_picks(loader=fake_loader)
    assert df_all.height == 2
    assert captured["called"] == 1

    # through_season=2017 should drop Burrow (2020).
    df_filtered = load_draft_picks(loader=fake_loader, through_season=2017)
    assert df_filtered.height == 1
    assert df_filtered["player_id"][0] == "00-0033280"
