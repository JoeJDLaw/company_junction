
#!/bin/bash
set -euo pipefail

# 1) Timestamp (YYYYMMDD_HHMMSS)
TS=$(date +"%Y%m%d_%H%M%S")

# 2) Ensure output dir exists
mkdir -p tarballs

# 3) Build a list of files to include
#    - Always include ./config/settings.yaml (if it exists)
#    - Prune unwanted directories INCLUDING ANY HIDDEN ".*" DIRS
#    - Exclude hidden files anywhere (paths containing "/.") and other noisy files

TMPFILE=$(mktemp -t cj_filelist.XXXXXX)

# Always include settings.yaml if present
if [[ -f ./config/settings.yaml ]]; then
  echo "./config/settings.yaml" > "$TMPFILE"
else
  : > "$TMPFILE"
fi

# Find all other files that we care about
find . \
  \( \
    -path '*/.*' -o \
    -path ./artifacts -o \
    -path ./htmlcov -o \
    -path ./__pycache__ -o \
    -path ./.venv -o \
    -path ./.mypy_cache -o \
    -path ./.pytest_cache -o \
    -path ./.ruff_cache -o \
    -path ./company_junction.egg-info -o \
    -path ./data -o \
    -path ./tarballs -o \
    -path ./docs -o \
    -path ./prompts -o \
    -path ./tests -o \
    -path ./deprecated -o \
    -path ./logs -o \
    -path ./app -o \
    -path ./scripts -o \
    -path ./tools -o \
    -path ./config \
  \) -prune -false -o \
  -type f \
  ! -path '*/.*' \
  ! -name '*.pyc' \
  ! -name '.DS_Store' \
  ! -name '._*' \
  ! -name 'cache' \
  ! -name 'coverage.xml' \
  ! -name 'COVERAGE_INSTRUCTIONS.md' \
  ! -name 'pipeline.log' \
  ! -name 'interrupt_test.log' \
  ! -name '*.parquet' \
  ! -name 'requirements.txt' \
  ! -name 'Makefile' \
  ! -name 'pytest.ini' \
  ! -name 'tox.ini' \
  ! -name 'mypy.ini' \
  ! -name 'ruff.toml' \
  ! -name 'run_streamlit.py' \
  ! -name 'create_tarball.sh' \
  ! -name 'cursor_rules.md' \
  ! -name 'setup.py' \
  -print >> "$TMPFILE"

# 4) Create the tarball using the file list
OUT="tarballs/code_only_${TS}.tgz"

tar czf "$OUT" -T "$TMPFILE"
rm -f "$TMPFILE"

echo "Created: $OUT"