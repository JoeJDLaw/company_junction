# Cursor Rules — Compact, Enforceable Set

> Use this as the rule-of-law while implementing. If a rule must be broken, call it out in the PR and propose an alternative.

## 1) Centralize Sorting
- One mapping function for ORDER BY logic. No per-function maps.
- Unknown sort keys → log error + use `config.ui.sort.default`.
- Same mapping must work across backends (DuckDB, pandas).

## 2) Configuration over Constants
- No hardcoded defaults for thresholds, backends, or sort order.
- Read from `config/settings.yaml`. Fallbacks must also be config-driven.

## 3) Cache Key Hygiene
- Include **source** (stats vs review_ready) and **backend** in every cache key.
- Parquet fingerprints must be source-specific.

## 4) Logging Contract
- Each path logs: `prefix | sort_key='...' | order_by='...' | backend=...`.
- Fallbacks log the reason and the chosen path.

## 5) Determinism & Safety
- Same inputs + run_id → identical outputs. No nondeterministic ordering.
- Cleanup tools: deterministic discovery (run_index.json), protect latest & pinned.

## 6) Test Coverage
- Sort mapping tests (unknown keys, cross-backend parity).
- Cache key tests (source/backend in key; different when inputs differ).
- Logging contract tests (distinct prefixes; includes sort_key/order_by/backend).
- Shape-guard tests for similarity (1D survivors).

## 7) Small, Reversible Changes
- Single-change PRs with clear success metrics.
- Rollback plan for each optimization.
