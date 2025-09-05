High-Level Plan (vNext of Similarity)

1) Quality & Evaluation Framework (foundation)

Goal: Make “Is this better?” answerable in minutes.
	•	Gold sets: Curate labeled pairs (match/non-match) with hard negatives and near-duplicates. Store under data/benchmarks/ with a manifest (schema: id_a,id_b,name_a,name_b,label,notes,segment).
	•	Metrics: Precision/Recall/F1 at gate, PR curves, ROC, AUC, and segment slices (length buckets, presence of numerics, punctuation, industry). Add Top-k eval for candidate ranking.
	•	Acceptance gates:
	•	Bulk vs parallel parity: mean |Δ| ≤ 0.5, max |Δ| ≤ 1.0.
	•	Scoring bounds always in [0,100], rounding rules consistent.
	•	CLI: scripts/benchmark_comparison.py grows a --suite similarity mode producing an HTML report to data/interim/*/grouping_profile.html + JSON to data/processed/index/latest.json.
	•	Repro: Pin config snapshots inside each report; write the config hash into outputs.

2) Scoring Improvements (surgical, reversible)

Goal: Lift recall on “messy” real-world names without precision collapse.
	•	Hybrid signal: Keep current RapidFuzz-style token_* ratios + Jaccard, then pilot a character n-gram similarity (e.g., 3-gram cosine) behind a config toggle, contributing a small weighted bump for borderline cases.
	•	Numeric awareness: Distinguish style vs substance (“Studio 54” vs “Studio Fifty Four”) with a numeric equivalence map (e.g., roman↔arabic) gated via config.
	•	Acronym/expanders: Optional ACME Intl. ↔ Acme International boost via canonicalization (already hinted by your normalization). Keep it opt-in, weighted low.

Success: +2–3 pp Recall at fixed Precision on benchmark hard negatives; no parity regressions.

3) Penalty Calibration (safe & data-driven)

Goal: Make penalties predictable and tunable.
	•	Penalty grid search: Define ranges for suffix/punctuation/numeric penalties; sweep on gold sets and store the Pareto frontier.
	•	Config bundles: Introduce named presets (e.g., strict, balanced, recall_plus) in settings.yaml, each frozen with comments. Select via env/CLI.
	•	Explainers: When a penalty fires, diagnostics should record {penalty_type, magnitude, tokens_involved} to aid debugging (only in DEBUG/diagnostics path).

4) Normalization & Dictionaries (controlled expansion)

Goal: Strengthen inputs without over-normalizing.
	•	Plural→singular map & weak tokens: Move lists to explicit assets under config/ with checksums; add a “dry-run diff” command that shows score impact per rule change before enabling.
	•	Canonical retail terms: Keep additions behind a feature flag and log coverage deltas (how many pairs touched).
	•	Safety valve: If normalize import fails, keep graceful fallback (you already have this in tests J).

5) Blocking Refinements (recall without cost explosion)

Goal: Fewer missed candidates at similar runtime.
	•	Adaptive token keys: Add an optional q-gram blocking path for very short names and a numeric-aware block when numbers present.
	•	Learned thresholds (optional later): Use historical matches to pick block keys per segment (length/charset profile).

6) Performance & Scale

Goal: Keep latency predictable, unlock larger runs.
	•	Vectorization pass: Identify hot loops in scoring.py; pre-tokenize and reuse token bags across pairs.
	•	Parallel mode audit: Ensure stable chunking and deterministic order; pin RNG if any shuffling exists.
	•	Artifact sizes: Keep parquet outputs schema-stable; enforce with tests/contracts/test_parquet_contracts.py.

7) Diagnostics, Observability, SLOs

Goal: Make failures loud and interpretable.
	•	Run-level summary: Emit distribution of scores, pass-rate at gate, top penalty types, and “moved by normalization” counts to docs/observability/dashboard.json.
	•	Drift monitors: Track weekly means of base signals per segment; alert when drift >2σ relative to 30-day baseline.
	•	Sampling UI hooks: Wire a small sample of borderline pairs (gate±5) into app/components/group_details.py for quick eyeballing.

8) API/Contract Hardening

Goal: Zero churn to downstream callers.
	•	Stable function surface: Keep score_pair, score_bulk, config schema and output columns constant; any additions must be additive.
	•	Strict schema tests: Already planned in E/L; extend with column-level dtypes + nullability.

9) Rollout & Safety

Goal: Non-destructive changes with safe escape hatches.
	•	Shadow runs: Run new config in shadow against a recent dataset; compare parity & quality.
	•	Feature flags: All new behaviors behind config toggles; default to current behavior.
	•	Rollback recipe: One-liner to revert to previous preset; document in docs/observability/runbook.md.

⸻

Concrete Deliverables (2–3 weeks after tests pass)
	1.	Benchmark suite + HTML/JSON reports and CLI.
	2.	Penalty preset pack (strict/balanced/recall_plus) with locked values.
	3.	Normalization assets (plural map, weak tokens, canonical terms) versioned, with an impact report.
	4.	Parity guard (mean/max |Δ| checks) embedded in CI.
	5.	Diagnostics log schema and a minimal dashboard JSON.

⸻

Suggested Order of Operations
	1.	✅ Finish Categories A–C, then H, L (math/bounds, sort contracts) — these stabilize interfaces.
	2.	Build the benchmark/diagnostics harness (Section 1 & 7).
	3.	Do penalty calibration (Section 3) behind flags.
	4.	Pilot lightweight scoring hybrid (Section 2) behind a flag.
	5.	Revisit blocking tweaks (Section 5) only after scoring/penalties settle.

⸻

Quick check-in
	•	Does this align with how you want to balance precision vs recall right now (e.g., favor precision for production gate, explore recall in reports)?
	•	Any segments we should up-weight in evaluation (e.g., fintech, “LLC/Inc” heavy, non-ASCII brands, numeric-rich names)?

If this looks right, I can sketch the benchmark CLI interface and the diagnostics JSON schema next so it’s ready the moment tests are green.