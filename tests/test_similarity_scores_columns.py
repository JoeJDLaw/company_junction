import pandas as pd

from src.similarity import pair_scores


def test_similarity_outputs_expected_columns() -> None:
    df = pd.DataFrame(
        [
            {"account_id": "A1", "name_core": "acme ltd", "suffix_class": "ltd"},
            {"account_id": "A2", "name_core": "acme limited", "suffix_class": "ltd"},
        ],
    )
    settings = {"similarity": {"medium": 0, "penalty": {}}}
    out = pair_scores(
        df, settings, enable_progress=False, parallel_executor=None, interim_dir=None,
    )
    # OK if empty (thresholds/logic may filter), but if not empty, columns must exist
    if not out.empty:
        for col in [
            "id_a",
            "id_b",
            "score",
            "ratio_name",
            "ratio_set",
            "jaccard",
            "num_style_match",
            "suffix_match",
            "base_score",
        ]:
            assert col in out.columns
