# 📊 Repo Audit — Results Template

> Use one section per file. Keep descriptions crisp. Add only public API (what other modules are expected to call).

## Module: `path/to/module.py`

| Field | Details |
|---|---|
| **Purpose** | One sentence on the module’s role. |
| **Public API** | - `fn_a(arg1, arg2) → out`: brief description\n- `ClassB`: brief description |
| **Imports** | `a, b, c` |
| **Imported By** | `x, y, z` (top 10 only) |
| **Config & Constants** | Any defaults that should be config‑driven; schema constants used. |
| **Overlaps / Duplicates** | e.g., similar logic in `other_module.py` (DRY candidate) |
| **Logging Contract** | Example log lines showing `sort_key | order_by | backend` or note gaps. |

---

## Top Hubs (import graph)
Add a Mermaid or DOT graph of the top 20 “hub” modules with highest in‑degree/out‑degree.
