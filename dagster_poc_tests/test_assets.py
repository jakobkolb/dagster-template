from dagster import materialize
import pandas as pd
from dagster_poc.assets import top_stories, top_stories_ids, topstories_word_cloud


def test_top_stories_ids_returns_list_of_ids():
    ids = top_stories_ids()
    assert isinstance(ids, list)
    assert len(ids) == 10


def test_hackernews_stories_assets_pipeline_works():
    assets = [top_stories_ids, top_stories, topstories_word_cloud]
    results = materialize(assets)
    assert results.success
    df = results.output_for_node("top_stories")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 10
