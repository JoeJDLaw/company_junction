import pandas as pd
import pytest

import src.similarity as sim


def test_header_list_raises_typeerror(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_parallel(*args, **kwargs):
        return ["id_a", "id_b", "score", "ratio_name", "ratio_set"]

    monkeypatch.setattr(sim, "_compute_similarity_scores_parallel", fake_parallel)
    df = pd.DataFrame(
        [
            {"account_id": "A1", "name_core": "acme ltd", "suffix_class": "ltd"},
            {"account_id": "A2", "name_core": "acme limited", "suffix_class": "ltd"},
        ],
    )
    with pytest.raises(TypeError):
        sim.pair_scores(df, {"similarity": {"medium": 0, "penalty": {}}})
