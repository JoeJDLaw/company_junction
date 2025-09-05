#!/usr/bin/env python3
"""Deterministic synthetic dataset for Company Junction resume/interrupt tests.

It creates clusters of names crafted to land in HIGH (>=92), MEDIUM (>=84,<92),
and LOW (<84) buckets for rapidfuzz token_* ratios, plus penalty cases.

Schema: minimally what's needed after schema resolution:
  - Account ID (15-char Salesforce-like)
  - Account Name
  - Created Date
Optional but useful:
  - alias_candidates
  - alias_sources
"""

import csv
from datetime import date


# Helper to make 15-char distinct Salesforce-like IDs
def sfid(n: int) -> str:
    """Generate unique 15-char Salesforce IDs."""
    # Use a simple pattern that fits in 15 chars
    # Start with 001Hs000054 and add unique suffix
    base = "001Hs000054"
    # Create unique suffix by using different characters
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    suffix = chars[n % len(chars)] + chars[(n // len(chars)) % len(chars)]
    # Pad to exactly 15 characters
    return (base + suffix).ljust(15, "A")


TODAY = date.today().isoformat()
rows = []


def add_row(
    idx: int,
    name: str,
    aliases: list[str] | None = None,
    srcs: list[str] | None = None,
    created: str = TODAY,
) -> None:
    """Add a row with proper field names."""
    rows.append(
        {
            "Account ID": sfid(idx),
            "Account Name": name,
            "Created Date": created,
            "alias_candidates": ";".join(aliases or []),
            "alias_sources": ";".join(srcs or []),
        },
    )


# Cluster A: HIGH similarity (>=92) - punctuation/reorder only
A = [
    "Alpha Widgets Inc",
    "Alpha Widgets, Inc.",
    "Alpha Widgets Incorporated",
    "Alpha Widget Inc",
]
for i, name in enumerate(A, start=1):
    add_row(i, name, aliases=["A Widgets"], srcs=["parentheses"])

# Cluster B: MEDIUM similarity (84-91) - token-set close with mild edits
B = [
    "Beta Tech Solutions LLC",
    "Solutions Beta Tech LLC",
    "Beta Technology Solution LLC",
    "Beta Tech Solution L.L.C.",
]
for i, name in enumerate(B, start=101):
    add_row(i, name, aliases=["BTS"], srcs=["semicolon"])

# Cluster C: LOW similarity (<84) - blocked but filtered by threshold
C = [
    "Gamma Holdings Ltd",
    "Gamma Outdoor Gear Ltd",
    "Gamma River Cruises Ltd",
]
for i, name in enumerate(C, start=201):
    add_row(i, name)

# Cluster D: Penalty – suffix mismatch
D = [
    "Delta Logistics Inc",
    "Delta Logistics GmbH",
]
for i, name in enumerate(D, start=301):
    add_row(i, name)

# Cluster E: Penalty – numeric style mismatch
E = [
    "Echo Media 20 20",
    "Echo Media 2020",
]
for i, name in enumerate(E, start=401):
    add_row(i, name)

# Cluster F: Alias-only linking
F = [
    ("Foxtrot Creative Studio", ["FCS", "Foxtrot Studio"]),
    ("Studio of Foxtrot Creative", ["Foxtrot Creative"]),
]
for i, (name, aliases) in enumerate(F, start=501):
    add_row(i, name, aliases=aliases, srcs=["plus"] * len(aliases))

# Cluster G: Additional edge cases for comprehensive testing
G = [
    "Hotel International Resort",
    "Hotel International Resort & Spa",
    "Hotel International Resort and Spa",
]
for i, name in enumerate(G, start=601):
    add_row(i, name)

out_path = "data/raw/company_junction_synth_resume_small.csv"
with open(out_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(
        f,
        fieldnames=[
            "Account ID",
            "Account Name",
            "Created Date",
            "alias_candidates",
            "alias_sources",
        ],
    )
    w.writeheader()
    for r in rows:
        w.writerow(r)

print(f"Wrote {len(rows)} rows to {out_path}")
print(f"Unique IDs: {len(set(r['Account ID'] for r in rows))}")
print(f"Unique names: {len(set(r['Account Name'] for r in rows))}")
print("\nSample records:")
for i, row in enumerate(rows[:5]):
    print(f"{i+1}: {row['Account ID']} | {row['Account Name']}")
