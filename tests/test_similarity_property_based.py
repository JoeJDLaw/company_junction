"""Property-based tests for similarity scoring using Hypothesis."""

import math
from typing import Any

import pandas as pd
import pytest
from hypothesis import given, settings, strategies as st

from src.similarity.scoring import compute_score_components


class TestSimilarityPropertyBased:
    """Property-based tests for similarity scoring invariants."""

    @pytest.mark.hypothesis
    @given(
        name1=st.text(min_size=1, max_size=100),
        name2=st.text(min_size=1, max_size=100),
        suffix1=st.sampled_from(["corp", "inc", "llc", "corporation", "company"]),
        suffix2=st.sampled_from(["corp", "inc", "llc", "corporation", "company"]),
    )
    @settings(max_examples=200, deadline=None)
    def test_score_symmetry(self, name1: str, name2: str, suffix1: str, suffix2: str):
        """Test that similarity scores are symmetric (score(a,b) = score(b,a))."""
        # Create test data
        df = pd.DataFrame({
            "name_core": [name1.lower(), name2.lower()],
            "suffix_class": [suffix1, suffix2],
        })
        
        # Get settings
        settings_dict = self._get_test_settings()
        
        # Compute scores both ways
        score_ab = compute_score_components(
            df.iloc[0]["name_core"],
            df.iloc[1]["name_core"],
            df.iloc[0]["suffix_class"],
            df.iloc[1]["suffix_class"],
            settings_dict
        )
        
        score_ba = compute_score_components(
            df.iloc[1]["name_core"],
            df.iloc[0]["name_core"],
            df.iloc[1]["suffix_class"],
            df.iloc[0]["suffix_class"],
            settings_dict
        )
        
        # Scores should be symmetric (within floating point precision)
        assert math.isclose(score_ab["score"], score_ba["score"], rel_tol=1e-6, abs_tol=1e-6)
        assert math.isclose(score_ab["ratio_set"], score_ba["ratio_set"], rel_tol=1e-6, abs_tol=1e-6)
        assert math.isclose(score_ab["ratio_name"], score_ba["ratio_name"], rel_tol=1e-6, abs_tol=1e-6)

    @pytest.mark.hypothesis
    @given(
        name=st.text(min_size=1, max_size=100),
        suffix=st.sampled_from(["corp", "inc", "llc", "corporation", "company"]),
    )
    @settings(max_examples=100, deadline=None)
    def test_score_identity(self, name: str, suffix: str):
        """Test that identical names have perfect similarity scores."""
        settings_dict = self._get_test_settings()
        
        score = compute_score_components(
            name.lower(),
            name.lower(),
            suffix,
            suffix,
            settings_dict
        )
        
        # Identical names should have perfect scores
        assert score["score"] == 100
        assert score["ratio_set"] == 100
        assert score["ratio_name"] == 100

    @pytest.mark.hypothesis
    @given(
        name1=st.text(min_size=1, max_size=50),
        name2=st.text(min_size=1, max_size=50),
        suffix1=st.sampled_from(["corp", "inc", "llc"]),
        suffix2=st.sampled_from(["corp", "inc", "llc"]),
    )
    @settings(max_examples=200, deadline=None)
    def test_score_bounds(self, name1: str, name2: str, suffix1: str, suffix2: str):
        """Test that similarity scores are within valid bounds [0, 100]."""
        settings_dict = self._get_test_settings()
        
        score = compute_score_components(
            name1.lower(),
            name2.lower(),
            suffix1,
            suffix2,
            settings_dict
        )
        
        # All scores should be within bounds
        assert 0 <= score["score"] <= 100
        assert 0 <= score["ratio_set"] <= 100
        assert 0 <= score["ratio_name"] <= 100

    @pytest.mark.hypothesis
    @given(
        base_name=st.text(min_size=1, max_size=30),
        suffix=st.sampled_from(["corp", "inc", "llc", "corporation", "company"]),
    )
    @settings(max_examples=100, deadline=None)
    def test_suffix_normalization_equivalence(self, base_name: str, suffix: str):
        """Test that different suffix forms of the same base name have high similarity."""
        # Create equivalent suffixes
        suffix_variants = {
            "corp": ["corp", "corporation"],
            "inc": ["inc", "incorporated"],
            "llc": ["llc", "limited liability company"],
            "corporation": ["corp", "corporation"],
            "company": ["co", "company"]
        }
        
        if suffix in suffix_variants:
            variants = suffix_variants[suffix]
            settings_dict = self._get_test_settings()
            
            # Test all pairs of variants
            for i, variant1 in enumerate(variants):
                for variant2 in variants[i+1:]:
                    score = compute_score_components(
                        base_name.lower(),
                        base_name.lower(),
                        variant1,
                        variant2,
                        settings_dict
                    )
                    
                    # Equivalent suffixes should have high similarity
                    assert score["score"] >= 80, f"Low similarity for equivalent suffixes: {variant1} vs {variant2}"

    @pytest.mark.hypothesis
    @given(
        name1=st.text(min_size=1, max_size=50),
        name2=st.text(min_size=1, max_size=50),
        suffix1=st.sampled_from(["corp", "inc", "llc"]),
        suffix2=st.sampled_from(["corp", "inc", "llc"]),
    )
    @settings(max_examples=200, deadline=None)
    def test_penalty_monotonicity(self, name1: str, name2: str, suffix1: str, suffix2: str):
        """Test that penalties decrease scores monotonically."""
        settings_dict = self._get_test_settings()
        
        # Test with penalties enabled
        score_with_penalties = compute_score_components(
            name1.lower(),
            name2.lower(),
            suffix1,
            suffix2,
            settings_dict
        )
        
        # Test with penalties disabled
        settings_no_penalties = settings_dict.copy()
        settings_no_penalties["similarity"]["penalties"] = {
            "suffix_mismatch": 0,
            "numeric_style_mismatch": 0,
            "punctuation_mismatch": 0
        }
        
        score_without_penalties = compute_score_components(
            name1.lower(),
            name2.lower(),
            suffix1,
            suffix2,
            settings_no_penalties
        )
        
        # Score with penalties should be <= score without penalties
        assert score_with_penalties["score"] <= score_without_penalties["score"]

    @pytest.mark.hypothesis
    @given(
        name=st.text(min_size=1, max_size=50),
        suffix=st.sampled_from(["corp", "inc", "llc"]),
        whitespace_variant=st.sampled_from(["  ", "\t", "\n", "   \t  \n  "]),
    )
    @settings(max_examples=100, deadline=None)
    def test_whitespace_robustness(self, name: str, suffix: str, whitespace_variant: str):
        """Test that whitespace variations don't significantly affect scores."""
        settings_dict = self._get_test_settings()
        
        # Original name
        original_score = compute_score_components(
            name.lower(),
            name.lower(),
            suffix,
            suffix,
            settings_dict
        )
        
        # Name with whitespace variations
        name_with_whitespace = f"{whitespace_variant}{name}{whitespace_variant}"
        whitespace_score = compute_score_components(
            name_with_whitespace.lower(),
            name.lower(),
            suffix,
            suffix,
            settings_dict
        )
        
        # Scores should be very close (whitespace should be normalized)
        assert math.isclose(original_score["score"], whitespace_score["score"], abs_tol=5.0)

    @pytest.mark.hypothesis
    @given(
        base_name=st.text(min_size=1, max_size=30),
        suffix=st.sampled_from(["corp", "inc", "llc"]),
        case_variant=st.sampled_from(["UPPER", "lower", "MiXeD", "Title Case"]),
    )
    @settings(max_examples=100, deadline=None)
    def test_case_insensitivity(self, base_name: str, suffix: str, case_variant: str):
        """Test that case variations don't affect similarity scores."""
        settings_dict = self._get_test_settings()
        
        # Original name
        original_score = compute_score_components(
            base_name.lower(),
            base_name.lower(),
            suffix,
            suffix,
            settings_dict
        )
        
        # Name with case variation
        case_varied_name = self._apply_case_variant(base_name, case_variant)
        case_score = compute_score_components(
            case_varied_name.lower(),
            base_name.lower(),
            suffix,
            suffix,
            settings_dict
        )
        
        # Scores should be identical (case should be normalized)
        assert math.isclose(original_score["score"], case_score["score"], rel_tol=1e-6, abs_tol=1e-6)

    @pytest.mark.hypothesis
    @given(
        name1=st.text(min_size=1, max_size=50),
        name2=st.text(min_size=1, max_size=50),
        suffix1=st.sampled_from(["corp", "inc", "llc"]),
        suffix2=st.sampled_from(["corp", "inc", "llc"]),
    )
    @settings(max_examples=200, deadline=None)
    def test_component_score_relationships(self, name1: str, name2: str, suffix1: str, suffix2: str):
        """Test relationships between different score components."""
        settings_dict = self._get_test_settings()
        
        score = compute_score_components(
            name1.lower(),
            name2.lower(),
            suffix1,
            suffix2,
            settings_dict
        )
        
        # Token set ratio should be >= token sort ratio
        assert score["ratio_set"] >= score["ratio_name"]
        
        # Score should be based on the higher of the two ratios
        expected_composite = max(score["ratio_set"], score["ratio_name"])
        
        # Apply penalties if any
        if "penalties" in settings_dict["similarity"]:
            penalties = settings_dict["similarity"]["penalties"]
            total_penalty = 0
            
            if score.get("suffix_mismatch_penalty", 0) > 0:
                total_penalty += penalties.get("suffix_mismatch", 0)
            if score.get("numeric_style_mismatch_penalty", 0) > 0:
                total_penalty += penalties.get("numeric_style_mismatch", 0)
            if score.get("punctuation_mismatch_penalty", 0) > 0:
                total_penalty += penalties.get("punctuation_mismatch", 0)
            
            expected_composite = max(0, expected_composite - total_penalty)
        
        # Score should match expected (within floating point precision)
        assert math.isclose(score["score"], expected_composite, rel_tol=1e-6, abs_tol=1e-6)

    def _get_test_settings(self) -> dict[str, Any]:
        """Get test settings for similarity scoring."""
        return {
            "similarity": {
                "high": 85,
                "medium": 70,
                "low": 50,
                "penalties": {
                    "suffix_mismatch": 5,
                    "numeric_style_mismatch": 3,
                    "punctuation_mismatch": 2
                }
            }
        }

    def _apply_case_variant(self, text: str, variant: str) -> str:
        """Apply case variant to text."""
        if variant == "UPPER":
            return text.upper()
        elif variant == "lower":
            return text.lower()
        elif variant == "MiXeD":
            return "".join(c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(text))
        elif variant == "Title Case":
            return text.title()
        else:
            return text
