"""Type definitions for similarity scoring components.

This module provides structured type definitions for similarity scoring
to eliminate Any propagation and improve type safety.
"""

from typing import TypedDict


class ScoreComponents(TypedDict):
    """Structured type for similarity score components.

    This TypedDict provides a clear contract for similarity scoring results,
    eliminating Any propagation while maintaining runtime compatibility.
    """

    score: int
    ratio_name: int
    ratio_set: int
    jaccard: float
    num_style_match: bool
    suffix_match: bool
    punctuation_mismatch: bool
    base_score: float
