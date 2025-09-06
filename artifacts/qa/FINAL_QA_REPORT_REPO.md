# Final QA Report — Repository (Stabilized Run)

- Black: artifacts/qa/black_final.txt
- Ruff: artifacts/qa/ruff_final.txt
- Mypy: artifacts/qa/mypy_final.txt
- Pytest: artifacts/qa/pytest_final.txt (JUnit: artifacts/qa/junit_final.xml)
- Coverage XML: artifacts/qa/coverage_final.xml
- Coverage HTML: artifacts/qa/htmlcov_final/index.html

Quarantined by default (kept, not deleted):
- optional: perf/bench/schema/contract tests
- legacy: phase-specific or deprecated E2E tests

Refer to file headers for `pytestmark = pytest.mark.optional|legacy`.