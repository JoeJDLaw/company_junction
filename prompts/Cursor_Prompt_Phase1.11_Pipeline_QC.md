# Phase1.11 — Cursor Task Prompt (Company Junction Deduplication Pipeline)

## 0) Context Bootstrapping (do this before anything else)
1. **Open and read** (in this order):
   - `cursor_rules.md` (treat this as the normative, source‑of‑truth document)
   - `CHANGELOG.md` (or `changelog.md` if lowercase)
   - `README.md`

2. **Summarize** back the relevant constraints, conventions, repo structure, and non-negotiable rules you must obey (naming, code style, testing, deployment, CI, data contracts, UI patterns). Identify any **conflicts** between these docs.

3. **Confirm context loaded** by listing the exact file paths you read and (if applicable) their last modified dates, plus a bullet list of key rules you will follow during this task.

---

## 1) Task Goals (Phase 1 finish work — critical review required before changes)
Your job is to:
- **Critically review** the plan below and propose **pushback, alternatives, and risks** first.
- **Wait for my confirmation** before making any code changes.
- Once approved, implement with tests, perf instrumentation, and UI explainability; then update docs and verify rule consistency.

### Plan to review (push back if there’s a better pattern)
**A. Full‑dataset validation run readiness**
- Enforce **lean dtypes** across candidate pairs & groups (`int32`, `float32`, categoricals); avoid carrying unused object columns.
- Emit a compact **perf summary** (`perf_summary.json`) including: total pairs; pairs ≥ medium/high thresholds; block size histogram (top tokens); group size histogram; wall‑clock durations by stage; **peak memory** (from `tracemalloc`); disposition counts.
- Ensure logs & summaries include a **config hash** so results are reproducible and attributable to a specific ruleset.

**B. Safer grouping via “edge‑gating” to primary**
- During Union‑Find / group assembly, a member may join a group only if it has **(i)** at least one edge to the current **primary** with score ≥ `high_threshold`, **or** (ii) an edge ≥ `medium_threshold` **and** shares at least one **non‑stop token** with the primary.
- Record gating decisions in explain metadata (e.g., `group_join_reason`, `weakest_edge_to_primary`).

**C. Stable `group_id`s**
- Compute a deterministic ID per group, independent of processing order: `group_id = sha1( sorted(member_ids) + config_hash )[:10]`.
- Persist this in outputs, and display in the UI.

**D. “Explain” metadata & UI hooks**
- For each record in a group, materialize: similarity sub‑scores, applied penalties, shared tokens, block key(s), join reason, and survivorship rationale.
- In Streamlit, add an **Explain** expander/panel rendering this metadata per row.

**E. (Optional, behind feature flag) Replace JSON overrides/blacklist with SQLite**
- Keep JSON export paths for portability, but use SQLite (or DuckDB) as the **authoritative** store with **append‑only audit tables** to support concurrent reviewers.
- Include a **feature flag** in config (e.g., `manual_store: {"driver": "json"|"sqlite", ...}`) so we can toggle without code changes.
- Add simple DAO layer + migration script that imports existing JSON on first run.

> If your critical review finds a simpler, lower‑risk path to hit the **<10 min / <8 GB** target for 94k Accounts, propose it.

---

## 2) Critical Review Requirement (before implementation)
Provide a **written critique** that includes:
- **Feasibility**: Where are the biggest time/memory risks (pandas joins, pair generation, group aggregation)?
- **Alternatives**: (a) two‑stage scoring (token/Jaccard prefilter → RapidFuzz on survivors), (b) Polars for heavy joins/groupbys, (c) smarter blocking (caps by top tokens; optional minhash/LSH), (d) canopy bounds (cap group growth unless ≥ high edge to primary).
- **Trade‑offs**: precision vs recall impacts for edge‑gating, risk of over‑splitting legitimate clusters, and how to tune thresholds.
- **Scope**: what *not* to do now (e.g., no LLM in Phase 1; no Spark; keep alias parsing modest).
- **Acceptance criteria** for Phase 1 sign‑off (explicit metrics & artifacts).

**Stop here and wait for my confirmation.**

---

## 3) Implementation (only after approval)
When I say “Proceed”, implement as follows. Keep changes small and reviewable.

### 3.1 Types & Memory Hygiene
- Add a centralized **dtype map** (module or YAML → applied at load and before write).
- Convert large intermediate frames to compact types, and drop columns not needed downstream.
- Add a **validator** that asserts no unexpected object‑dtype columns sneak into pair/group tables.

### 3.2 Edge‑Gating in Grouping (reference implementation sketch)
_Pseudocode (adapt to your structure):_
```python
def can_join(primary, candidate, edges, tokens, cfg) -> tuple[bool, str, float]:
    score = edges.get((primary.id, candidate.id)) or edges.get((candidate.id, primary.id), 0.0)
    shared = tokens[candidate.id] & tokens[primary.id] - cfg.stop_tokens
    if score >= cfg.similarity.high:
        return True, "edge>=high", score
    if score >= cfg.similarity.medium and len(shared) > 0:
        return True, "edge>=medium+shared_token", score
    return False, "insufficient_edge", score

# During group assembly:
for member in candidates:
    ok, reason, s = can_join(primary, member, edge_scores, token_sets, cfg)
    if ok:
        uf.union(primary.id, member.id)
        explain[member.id]["group_join_reason"] = reason
        explain[member.id]["score_to_primary"] = s
```

### 3.3 Stable Group IDs
```python
import hashlib, json

def stable_group_id(member_ids: list[str|int], config_obj: dict, n=10) -> str:
    payload = {
        "members": sorted(map(str, member_ids)),
        "config_hash": config_hash(config_obj),
    }
    h = hashlib.sha1(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode())
    return h.hexdigest()[:n]

def config_hash(config_obj: dict) -> str:
    h = hashlib.sha1(json.dumps(config_obj, separators=(",", ":"), sort_keys=True).encode())
    return h.hexdigest()[:8]
```

### 3.4 Perf Summary & Logging
- Implement `perf_summary.json` emission with: counts, thresholds, histograms, timings, peak memory, config hash, git commit.
- Ensure **block-top-tokens** export runs on full dataset and write a **caps suggestion** section if any token’s block sizes exceed configured limits.

### 3.5 UI Explainability
- Extend `review_ready.[csv|parquet]` to include explain fields (`weakest_edge_to_primary`, `group_join_reason`, `shared_tokens_count`, `applied_penalties`, `survivorship_reason`, etc.)
- Add an **Explain** panel in Streamlit per row and a filter for “groups with weakest_edge < X” and “size ≥ N”.

### 3.6 (Optional) SQLite Manual Store
- Introduce a minimal DAO with tables: `overrides`, `blacklist`, `audit_log` (append‑only), with UTC timestamps and user/session ID if available.
- Provide CLI/Streamlit actions to import/export JSON for backward compatibility.

### 3.7 Tests
- Add/update unit tests:
  - Edge‑gating logic (positive/negative cases; stop‑token interactions).
  - Stable `group_id` determinism (same inputs → same ID; config change → different ID).
  - Dtype validator (fails on unexpected object cols).
  - Perf summary schema & presence.
  - UI explain fields present for representative groups (mocked small dataset).
- Consider **property‑based tests** for alias/normalization joins using Hypothesis, keeping scope small.

### 3.8 CI & Artifacts
- Ensure tests pass locally and in CI.
- CI should publish artifacts: `review_ready.*`, `perf_summary.json`, `block_top_tokens.csv`, and (if enabled) SQLite DB snapshot.

---

## 4) Deliverables Checklist
- [ ] Written **critical review** (with pushback/alternatives) produced and approved.
- [ ] Code changes implementing approved items.
- [ ] Tests added/updated and passing.
- [ ] `perf_summary.json` & `block_top_tokens.csv` from a sample or full run (document if full run not executed).
- [ ] Streamlit **Explain** panel working on `review_ready.*`.
- [ ] (Optional) SQLite store behind feature flag.
- [ ] **Docs updated** (see §5).
- [ ] Final **source-of-truth check** against `cursor_rules.md` (see §6).

---

## 5) Documentation & Changelog
After implementation, **update both**:
- `CHANGELOG.md`: summarize what changed in **Phase1.11**, with scope, rationale, and any config migrations.
- `README.md`: reflect new flags (e.g., `manual_store`), new fields in `review_ready.*`, how to run the full 94k job, and how to read `perf_summary.json`.
- If you created new modules or CLIs, add short usage examples.

---

## 6) Source-of-Truth Verification
- Re‑open `cursor_rules.md` and verify it **still** reflects actual conventions and constraints post‑work.
- If it no longer fully aligns, either:
  - Propose edits to realign the rules to current reality (preferred), or
  - Propose changes to the implementation to conform with existing rules.
- Report explicitly: **“`cursor_rules.md` remains the source of truth”** or **“Discrepancy found”** + details.

---

## 7) Communication Protocol
- Always prefix status messages with **[Phase1.11]**.
- Use concise, actionable updates.
- Ask me to confirm **before** implementing any non‑trivial change or if you deviate from this plan.

---

## 8) Acceptance Criteria (Phase 1 sign‑off)
- On the 94k dataset (or a representative slice), pipeline stays **< 10 minutes** wall time and **< 8 GB** peak RSS **on our standard runner** (document specs).
- Grouping respects edge‑gating rules; large “blob” groups are rare and explainable.
- Outputs include stable `group_id`, explain fields, and `perf_summary.json`.
- UI enables efficient reviewer triage (Explain panel + filters).
- Docs and changelog reflect the changes; `cursor_rules.md` alignment confirmed.

---

## 9) Out of Scope (Phase 2 will handle later)
- LLM adjudication or enrichment (Phase2.x).
- Heavy alias NLP, Spark migration, or major schema overhauls beyond what’s listed.

---

### Notes / Hints
- Prefer **small PRs** grouped by concern (types, grouping, explain UI, storage).
- Keep **feature flags** for risky changes and ensure safe defaults match current behavior.
- Where possible, make performance changes **opt‑in** until validated on real data.

