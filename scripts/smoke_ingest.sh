#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
PY="${PYTHON:-python}"
CLEAN="${CLEAN_BIN:-$PY -m src.cleaning}"

OUTDIR="${OUTDIR:-$ROOT_DIR/.smoke_out}"
CFG="${CFG:-$(python - <<'PY'
from src.utils.path_utils import get_config_path
print(get_config_path())
PY
)}"

rm -rf "$OUTDIR"
mkdir -p "$OUTDIR/fixtures"

echo "==> Creating fixtures"
# CSV (name only)
cat > "$OUTDIR/fixtures/minimal_accounts.csv" <<'CSV'
Name,Notes
Acme Inc,"notes 1"
Beta Co,"notes 2"
CSV

# CSV (name + id incl junk)
cat > "$OUTDIR/fixtures/accounts_with_ids.csv" <<'CSV'
Account Name,Account ID,Other
Acme Incorporated,XYZ123,foo
ACME INCORPORATED,xyz123,bar
Beta Co,,baz
CSV

# JSON (array of records)
cat > "$OUTDIR/fixtures/accounts.json" <<'JSON'
{
  "records": [
    {"company": "Gamma LLC", "uuid": "a1"},
    {"company": "Delta Ltd", "uuid": "a2"}
  ]
}
JSON

# XML (simple)
cat > "$OUTDIR/fixtures/accounts.xml" <<'XML'
<root>
  <record>
    <company>Omega BV</company>
    <id>42</id>
  </record>
  <record>
    <company>Sigma GmbH</company>
    <id>43</id>
  </record>
</root>
XML

echo "==> DRY RUNS (no heavy compute)"
# CSV dry-run with only Name
$CLEAN \
  --input "$OUTDIR/fixtures/minimal_accounts.csv" \
  --outdir "$OUTDIR/csv_dry" \
  --config "$CFG" \
  --name-col "Name" \
  --dry-run-ingest \
  --log-preview

# CSV dry-run with IDs (including blank)
$CLEAN \
  --input "$OUTDIR/fixtures/accounts_with_ids.csv" \
  --outdir "$OUTDIR/csv_ids_dry" \
  --config "$CFG" \
  --name-col "Account Name" \
  --id-col "Account ID" \
  --dry-run-ingest

# JSON dry-run using JSONPath
$CLEAN \
  --input "$OUTDIR/fixtures/accounts.json" \
  --outdir "$OUTDIR/json_dry" \
  --config "$CFG" \
  --json-record-path '$.records[*]' \
  --name-col company \
  --id-col uuid \
  --dry-run-ingest

# XML dry-run using XPath
$CLEAN \
  --input "$OUTDIR/fixtures/accounts.xml" \
  --outdir "$OUTDIR/xml_dry" \
  --config "$CFG" \
  --xml-record-path '/root/record' \
  --name-col company \
  --id-col id \
  --dry-run-ingest

echo "==> FULL RUN (CSV minimal)"
$CLEAN \
  --input "$OUTDIR/fixtures/minimal_accounts.csv" \
  --outdir "$OUTDIR/csv_full" \
  --config "$CFG" \
  --name-col "Name"

echo "==> Sanity checks"
test -f "$OUTDIR/csv_full/processed/review_ready.csv" || { echo "review_ready.csv missing"; exit 1; }

echo "✅ Smoke tests passed."
