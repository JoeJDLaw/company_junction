#!/usr/bin/env python3
"""
Comprehensive import test for the company junction pipeline.
This script tests all imports to ensure the codebase is properly structured.
"""

import sys
import importlib
import traceback
from pathlib import Path

def test_import(module_name, description=""):
    """Test importing a specific module."""
    try:
        module = importlib.import_module(module_name)
        print(f"✅ {module_name} - {description}")
        return True
    except Exception as e:
        print(f"❌ {module_name} - {description}")
        print(f"   Error: {e}")
        print(f"   Traceback: {traceback.format_exc()}")
        return False

def test_src_imports():
    """Test all imports from the src directory."""
    print("\n=== Testing src/ imports ===")
    
    # Add src to path for testing
    src_path = Path(__file__).parent / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    # Test main modules
    modules_to_test = [
        ("src.cleaning", "Main pipeline orchestration"),
        ("src.normalize", "Data normalization"),
        ("src.similarity", "Similarity computation"),
        ("src.grouping", "Group creation logic"),
        ("src.survivorship", "Primary record selection"),
        ("src.disposition", "Disposition classification"),
        ("src.alias_matching", "Alias matching"),
        ("src.manual_io", "Manual data I/O"),
        ("src.salesforce", "Salesforce utilities"),
        ("src.performance", "Performance tracking"),
        ("src.dtypes_map", "Data type mappings"),
    ]
    
    success_count = 0
    for module_name, description in modules_to_test:
        if test_import(module_name, description):
            success_count += 1
    
    return success_count, len(modules_to_test)

def test_utils_imports():
    """Test all imports from the src/utils package."""
    print("\n=== Testing src/utils/ imports ===")
    
    utils_modules = [
        ("src.utils", "Main utils package"),
        ("src.utils.dtypes", "Data type utilities"),
        ("src.utils.logging_utils", "Logging utilities"),
        ("src.utils.path_utils", "Path utilities"),
        ("src.utils.validation_utils", "Validation utilities"),
        ("src.utils.io_utils", "I/O utilities"),
        ("src.utils.perf_utils", "Performance utilities"),
        ("src.utils.hash_utils", "Hash utilities"),
    ]
    
    success_count = 0
    for module_name, description in utils_modules:
        if test_import(module_name, description):
            success_count += 1
    
    return success_count, len(utils_modules)

def test_cross_module_imports():
    """Test that modules can import from each other correctly."""
    print("\n=== Testing cross-module imports ===")
    
    # Test specific import patterns that are used in the codebase
    import_tests = [
        ("src.cleaning imports src.utils", lambda: __import__("src.cleaning")),
        ("src.grouping imports src.utils", lambda: __import__("src.grouping")),
        ("src.survivorship imports src.normalize", lambda: __import__("src.survivorship")),
        ("src.performance imports src.utils", lambda: __import__("src.performance")),
        ("src.utils.dtypes imports src.dtypes_map", lambda: __import__("src.utils.dtypes")),
    ]
    
    success_count = 0
    for description, import_func in import_tests:
        try:
            import_func()
            print(f"✅ {description}")
            success_count += 1
        except Exception as e:
            print(f"❌ {description}")
            print(f"   Error: {e}")
    
    return success_count, len(import_tests)

def test_app_imports():
    """Test app module imports."""
    print("\n=== Testing app/ imports ===")
    
    app_modules = [
        ("app.main", "Streamlit app"),
        ("app.manual_data", "Manual data utilities"),
    ]
    
    success_count = 0
    for module_name, description in app_modules:
        if test_import(module_name, description):
            success_count += 1
    
    return success_count, len(app_modules)

def main():
    """Run all import tests."""
    print("🔍 Running comprehensive import tests...")
    
    total_success = 0
    total_tests = 0
    
    # Test src imports
    success, count = test_src_imports()
    total_success += success
    total_tests += count
    
    # Test utils imports
    success, count = test_utils_imports()
    total_success += success
    total_tests += count
    
    # Test cross-module imports
    success, count = test_cross_module_imports()
    total_success += success
    total_tests += count
    
    # Test app imports
    success, count = test_app_imports()
    total_success += success
    total_tests += count
    
    # Summary
    print(f"\n=== Import Test Summary ===")
    print(f"Passed: {total_success}/{total_tests} ({total_success/total_tests*100:.1f}%)")
    
    if total_success == total_tests:
        print("🎉 All imports working correctly!")
        return 0
    else:
        print("⚠️  Some imports failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
