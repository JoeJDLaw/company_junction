#!/bin/bash
set -euo pipefail

# Rollback script for ui_helpers refactor
# This script safely restores the original ui_helpers.py from backup

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKUP_FILE="$PROJECT_ROOT/deprecated/ui_helpers.py.bak"
TARGET_FILE="$PROJECT_ROOT/src/utils/ui_helpers.py"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}[ROLLBACK]${NC} $message"
}

print_error() {
    local message=$1
    echo -e "${RED}[ERROR]${NC} $message"
}

print_warning() {
    local message=$1
    echo -e "${YELLOW}[WARNING]${NC} $message"
}

print_success() {
    local message=$1
    echo -e "${GREEN}[SUCCESS]${NC} $message"
}

# Check if running from project root
if [[ ! -f "$PROJECT_ROOT/requirements.txt" ]]; then
    print_error "Must run from project root directory"
    exit 1
fi

print_status "$GREEN" "Starting ui_helpers rollback process..."

# Verify backup file exists
if [[ ! -f "$BACKUP_FILE" ]]; then
    print_error "Backup file not found: $BACKUP_FILE"
    print_error "Cannot proceed with rollback"
    exit 1
fi

print_status "$GREEN" "Backup file found: $BACKUP_FILE"

# Check backup file permissions
if [[ -w "$BACKUP_FILE" ]]; then
    print_warning "Backup file is writable - this should be write-protected"
    print_warning "Consider running: chmod 444 $BACKUP_FILE"
else
    print_status "$GREEN" "Backup file is properly write-protected"
fi

# Verify backup file is not empty
backup_size=$(wc -c < "$BACKUP_FILE")
if [[ $backup_size -eq 0 ]]; then
    print_error "Backup file is empty - cannot proceed"
    exit 1
fi

print_status "$GREEN" "Backup file size: $backup_size bytes"

# Check if target file exists and create backup of current state
if [[ -f "$TARGET_FILE" ]]; then
    current_backup="$PROJECT_ROOT/deprecated/ui_helpers_current_$(date +%Y%m%d_%H%M%S).bak"
    print_status "$YELLOW" "Creating backup of current ui_helpers.py: $current_backup"
    cp "$TARGET_FILE" "$current_backup"
    print_status "$GREEN" "Current state backed up to: $current_backup"
else
    print_warning "Target file does not exist - will create new"
fi

# Perform rollback
print_status "$GREEN" "Restoring ui_helpers.py from backup..."
cp "$BACKUP_FILE" "$TARGET_FILE"

# Verify restoration
if [[ -f "$TARGET_FILE" ]]; then
    restored_size=$(wc -c < "$TARGET_FILE")
    if [[ $restored_size -eq $backup_size ]]; then
        print_success "File restored successfully"
        print_status "$GREEN" "Restored file size: $restored_size bytes"
    else
        print_error "File size mismatch after restoration"
        print_error "Expected: $backup_size bytes, Got: $restored_size bytes"
        exit 1
    fi
else
    print_error "Failed to restore file"
    exit 1
fi

# Verify file integrity with diff
print_status "$GREEN" "Verifying file integrity..."
if diff "$BACKUP_FILE" "$TARGET_FILE" > /dev/null; then
    print_success "File integrity verified - no differences found"
else
    print_error "File integrity check failed - differences detected"
    exit 1
fi

# Reset feature flags
print_status "$GREEN" "Resetting feature flags..."
export CJ_UI_HELPERS_DEPRECATE=
export CJ_UI_HELPERS_NO_WARN=

# Show current environment
print_status "$GREEN" "Current environment variables:"
echo "  CJ_UI_HELPERS_DEPRECATE: ${CJ_UI_HELPERS_DEPRECATE:-unset}"
echo "  CJ_UI_HELPERS_NO_WARN: ${CJ_UI_HELPERS_NO_WARN:-unset}"

# Provide next steps
print_success "Rollback completed successfully!"
echo
print_status "$YELLOW" "Next steps:"
echo "  1. Restart your application/Streamlit server"
echo "  2. Verify that the original functionality is restored"
echo "  3. Check logs for any errors"
echo "  4. Run tests to ensure everything works: pytest -q"
echo
print_status "$YELLOW" "If you need to re-enable the refactored version:"
echo "  export CJ_UI_HELPERS_DEPRECATE=true"
echo "  export CJ_UI_HELPERS_NO_WARN=true"
echo
print_status "$YELLOW" "To make backup file write-protected:"
echo "  chmod 444 $BACKUP_FILE"

# Exit successfully
exit 0
