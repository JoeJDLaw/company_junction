#!/bin/bash
# Codemod script to replace ui_helpers imports with new module imports
# This script is idempotent and can be run multiple times safely

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🔄 Starting ui_helpers import replacement...${NC}"

# Check if required tools are available
if ! command -v rg &> /dev/null; then
    echo -e "${RED}❌ ripgrep (rg) is required but not installed${NC}"
    echo "Install with: brew install ripgrep (macOS) or apt install ripgrep (Ubuntu)"
    exit 1
fi

# Determine sed command (GNU sed on Linux, gsed on macOS)
if command -v gsed &> /dev/null; then
    SED_CMD="gsed"
elif sed --version &> /dev/null; then
    SED_CMD="sed"
else
    # Fall back to regular sed (macOS sed)
    SED_CMD="sed"
fi

echo -e "${YELLOW}Using sed command: $SED_CMD${NC}"

# Function to apply replacements
apply_replacements() {
    echo -e "${GREEN}📝 Applying import replacements...${NC}"
    
    # Group pagination imports
    echo "  - Replacing get_groups_page imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+get_groups_page\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import get_groups_page/from src.utils.group_pagination import get_groups_page/g'
    
    echo "  - Replacing get_total_groups_count imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+get_total_groups_count\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import get_total_groups_count/from src.utils.group_pagination import get_total_groups_count/g'
    
    # Group details imports
    echo "  - Replacing get_group_details imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+get_group_details\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import get_group_details/from src.utils.group_details import get_group_details/g'
    
    # Filtering imports
    echo "  - Replacing get_order_by imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+get_order_by\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import get_order_by/from src.utils.filtering import get_order_by/g'
    
    echo "  - Replacing build_sort_expression imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+build_sort_expression\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import build_sort_expression/from src.utils.filtering import build_sort_expression/g'
    
    # Artifact management imports
    echo "  - Replacing get_artifact_paths imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+get_artifact_paths\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import get_artifact_paths/from src.utils.artifact_management import get_artifact_paths/g'
    
    # Session imports
    echo "  - Replacing session imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+session\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import session/from src.utils.ui_session import session/g'
    
    echo "  - Replacing get_backend_choice imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+get_backend_choice\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import get_backend_choice/from src.utils.ui_session import get_backend_choice/g'
    
    echo "  - Replacing set_backend_choice imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+set_backend_choice\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import set_backend_choice/from src.utils.ui_session import set_backend_choice/g'
    
    # Cache imports
    echo "  - Replacing build_cache_key imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+build_cache_key\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import build_cache_key/from src.utils.cache_keys import build_cache_key/g'
    
    echo "  - Replacing build_details_cache_key imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+build_details_cache_key\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import build_details_cache_key/from src.utils.cache_keys import build_details_cache_key/g'
    
    # Run management imports
    echo "  - Replacing list_runs imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+list_runs\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import list_runs/from src.utils.run_management import list_runs/g'
    
    echo "  - Replacing get_run_metadata imports..."
    rg -l "from\s+src\.utils\.ui_helpers\s+import\s+get_run_metadata\b" | xargs -r $SED_CMD -i '' 's/from src\.utils\.ui_helpers import get_run_metadata/from src.utils.run_management import get_run_metadata/g'
}

# Function to verify replacements
verify_replacements() {
    echo -e "${GREEN}🔍 Verifying replacements...${NC}"
    
    # Check for any remaining ui_helpers imports (excluding the façade file itself)
    remaining_imports=$(git grep -n "src\.utils\.ui_helpers" -- ':!src/utils/ui_helpers.py' || true)
    
    if [ -n "$remaining_imports" ]; then
        echo -e "${YELLOW}⚠️  Found remaining ui_helpers imports:${NC}"
        echo "$remaining_imports"
        echo -e "${YELLOW}These may need manual review or additional codemod rules.${NC}"
        return 1
    else
        echo -e "${GREEN}✅ No remaining ui_helpers imports found!${NC}"
        return 0
    fi
}

# Function to run tests
run_tests() {
    echo -e "${GREEN}🧪 Running tests to verify changes...${NC}"
    
    if command -v pytest &> /dev/null; then
        pytest -q --tb=short
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✅ All tests passed!${NC}"
        else
            echo -e "${RED}❌ Some tests failed. Please review the changes.${NC}"
            return 1
        fi
    else
        echo -e "${YELLOW}⚠️  pytest not available, skipping test run${NC}"
    fi
}

# Main execution
main() {
    echo -e "${GREEN}🚀 ui_helpers Import Replacement Tool${NC}"
    echo "This script will replace all imports from src.utils.ui_helpers with new module imports."
    echo ""
    
    # Apply replacements
    apply_replacements
    
    echo ""
    echo -e "${GREEN}📊 Summary of replacements:${NC}"
    echo "  - get_groups_page, get_total_groups_count → src.utils.group_pagination"
    echo "  - get_group_details → src.utils.group_details"
    echo "  - get_order_by, build_sort_expression → src.utils.filtering"
    echo "  - get_artifact_paths → src.utils.artifact_management"
    echo "  - session, get_backend_choice, set_backend_choice → src.utils.ui_session"
    echo "  - build_cache_key, build_details_cache_key → src.utils.cache_keys"
    echo "  - list_runs, get_run_metadata → src.utils.run_management"
    echo ""
    
    # Verify replacements
    if verify_replacements; then
        echo -e "${GREEN}🎉 All imports successfully replaced!${NC}"
        
        # Run tests if available
        run_tests
        
        echo ""
        echo -e "${GREEN}✅ Migration complete! You can now safely remove src/utils/ui_helpers.py${NC}"
    else
        echo -e "${YELLOW}⚠️  Some imports may need manual review${NC}"
        exit 1
    fi
}

# Run main function
main "$@"
