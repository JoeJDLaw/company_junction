import pytest
from src.similarity import _compute_pair_score_optimized


def test_list_extend_misuse_regression():
    """Test that intentionally reproduces the failure mode so we never ship it again."""
    a = {"id_a": "A", "id_b": "B", "score": 1}
    scores = []
    scores.extend(a)  # WRONG on purpose: turns into ['id_a', 'id_b', 'score']
    assert not all(isinstance(x, dict) for x in scores)
    assert len(scores) == 3  # 3 keys from the dict
    assert scores[0] == 'id_a'  # First key from the dict
