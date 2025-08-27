# Cursor Prompt — Phase 1.4 Enhanced Normalization & Splitting Rules (Please review critically)

> **Ask:** Extend Phase 1.3 improvements with **enhanced normalization, splitting detection, and punctuation handling**. Apply these updates critically: if you disagree or know better approaches, **push back with reasoning and propose alternatives**.

---

## Additional Goals

1. Strengthen **normalization** of Account Names (remove underscores, stray punctuation, normalize quotes/parentheses).  
2. Handle **potential splits** when multiple companies are embedded in a single field (parentheses, semicolons, numbering).  
   - Phase 1.4: mark as `Verify` with a clear reason (do not attempt actual splitting yet).  
   - Phase 2: implement actual splitting into separate rows.  
3. Treat punctuation consistently: semicolon, colon, comma, and period should reduce confidence but not create hard differences.  
4. Flag suspicious patterns like `"on Pay Stubs"`, `"PNC"`, `"Not Sure"`, `"Unsure"` as likely errors → `Delete` or `Verify`.  
5. Provide hooks for optional **LLM evaluation** of edge cases (`evaluate_name_llm`), disabled by default but ready for Phase 2.  

---i

## Patches

### Patch H — Normalization: underscores, punctuation, quotes, parentheses

**File:** `src/normalize.py`, `tests/test_normalize.py`

**Changes:**
- Remove leading/trailing/multiple underscores from names (normalize to space).  
- Remove stray quotes (`"`, `'`) during normalization.  
- Remove or collapse parentheses content for scoring (`Holz Rubber Co Inc (Express Staffing)` → `holz rubber co inc express staffing`).  
- Preserve original form in `name_raw` for UI, but store a `name_cleaned` field for comparison.  
- Strip semicolons, colons, commas, and periods for scoring purposes (normalize to space).  
- Add flag columns:  
  - `has_parentheses`, `has_semicolon`, `has_multiple_names` (heuristic: semicolon, or numbered `(1)`, `(2)` patterns).  
  - These flags feed into disposition logic.

**Testing:**  
- Add cases for underscores (`__Don_Roberto__` → `don roberto`).  
- Parentheses removed (`Diamond Foods (Express Staffing)` → core `diamond foods express staffing`).  
- Multi-name string `(1) Don Roberto; (2) BYD Auto` → flags `has_multiple_names=True`.  

---

### Patch I — Splitting detection (Phase 1.4 only mark as Verify)

**File:** `src/disposition.py`, `tests/test_disposition.py`

**Changes:**
- If `has_multiple_names=True`, disposition = `Verify`.  
- Add `disposition_reason="multi-name string; requires split"`.
- Ensure survivorship logic still runs but disposition overrides to `Verify`.  
- Keep Phase 1 read-only (no actual splitting).  

---

### Patch J — Punctuation sensitivity

**File:** `src/similarity.py`

**Changes:**
- Already normalized punctuation removed for scoring.  
- Add penalty if original names contain conflicting punctuation (e.g., one has `;`, the other does not).  
- Configurable penalty in `settings.yaml` (e.g., `punctuation_mismatch: 3`).  
- Ensure score reduction only nudges pairs toward `Verify`, not hard separation.

---

### Patch K — Suspect phrases

**File:** `src/disposition.py`

**Changes:**
- Extend blacklist/suspicious detection: if cleaned name contains tokens like `"on pay stubs"`, `"pnc"`, `"not sure"`, `"unsure"`, mark as `Delete` (or `Verify` if context ambiguous).  
- Add regex list `SUSPECT_PATTERNS = [r"on pay stubs", r"pnc", r"not sure", r"unsure"]`.  
- Add `disposition_reason` with `"suspect_phrase:<match>"`.  

---

### Patch L — Optional LLM evaluation hook

**File:** `src/disposition.py`, `src/utils.py`, `config/settings.yaml`

**Changes:**
- Add `llm.enabled: false` and `llm.provider: openai` placeholders in config.  
- Function stub `evaluate_name_llm(name: str) -> Optional[dict]`.  
- If enabled, pass names through LLM to classify as `real_company: true/false`, `suggested_clean_name`.  
- For Phase 1.4, leave disabled but add a test stub.

---

## Config Additions

**File:** `config/settings.yaml`

```yaml
similarity:
  high: 92
  medium: 84
  penalty:
    suffix_mismatch: 25
    num_style_mismatch: 5
+    punctuation_mismatch: 3

llm:
  enabled: false
  provider: openai
  delete_threshold: 85
```

---

## Streamlit Integration

**File:** `app/main.py`

- Show new columns `has_parentheses`, `has_multiple_names`, and `disposition_reason` in group views.  
- Add a filter “Potential splits only” (checkbox → show groups flagged for multiple names).  
- When `has_multiple_names=True`, emphasize with a red badge “Split candidate”.  
- Collapsible panel for “Normalization flags” per record.

---

## Acceptance Criteria

- Normalization removes underscores, quotes, parentheses, semicolons, colons, commas, and periods for scoring.  
- Multi-name entries are flagged and disposition = `Verify`.  
- Blacklist extended with `"on pay stubs"`, `"pnc"`, `"not sure"`, `"unsure"`.  
- Punctuation mismatch penalty applied.  
- LLM hook stub present but disabled.  
- Streamlit shows flags and allows filtering by split candidates.  
- Tests pass with new edge cases.  

---

## Please review critically

If the proposed normalization or multi-name detection could **over-normalize legitimate legal names**, push back and suggest a safer alternative. For example, some companies *do* include commas or periods legally. Cursor should weigh the tradeoff: better recall vs risk of false merges.  

