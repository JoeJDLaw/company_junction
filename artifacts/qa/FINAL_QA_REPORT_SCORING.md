# Final QA Report — Similarity Scoring Suite (Final)

- Python: `Python 3.12.2`
- Timestamp (UTC): `2025-09-06T00:23:15Z`

## Pytest (selection: -k "scoring")
- Console: artifacts/qa/pytest_scoring.txt
- JUnit: artifacts/qa/junit_scoring.xml

## Coverage (scoring.py)
- XML: artifacts/qa/coverage_scoring.xml
- HTML: artifacts/qa/htmlcov_scoring/index.html

Notes:
- No changes to `src/similarity/scoring.py`.
- Deterministic: durations via `--durations=20`.
- Config-over-constants verified in tests.