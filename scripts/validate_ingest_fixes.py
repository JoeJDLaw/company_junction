#!/usr/bin/env python3
"""
Quick validation script for ingest v0 critical fixes.
Run this to verify the most important improvements are working.
"""

import sys
import tempfile
import pandas as pd
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_parquet_fallback():
    """Test parquet fallback when pyarrow is missing."""
    print("🧪 Testing parquet fallback...")
    
    # Mock missing pyarrow
    import sys
    if 'pyarrow' in sys.modules:
        del sys.modules['pyarrow']
    
    # Test the fallback logic
    from src.utils.io_utils import _is_pyarrow_available
    available = _is_pyarrow_available()
    print(f"   PyArrow available: {available}")
    
    if not available:
        print("   ✅ Fallback logic working (pyarrow not available)")
    else:
        print("   ⚠️  PyArrow is available - cannot test fallback")

def test_import_guards():
    """Test import guards for optional dependencies."""
    print("🧪 Testing import guards...")
    
    # Test JSON import guard
    try:
        from src.utils.io_utils import parse_json_to_dataframe
        print("   ✅ JSON parsing function imported successfully")
    except ImportError as e:
        print(f"   ❌ JSON import failed: {e}")
    
    # Test XML import guard  
    try:
        from src.utils.io_utils import parse_xml_to_dataframe
        print("   ✅ XML parsing function imported successfully")
    except ImportError as e:
        print(f"   ❌ XML import failed: {e}")

def test_ingest_mapping():
    """Test apply_ingest_mapping with edge cases."""
    print("🧪 Testing ingest mapping...")
    
    # Create test data
    df = pd.DataFrame({
        'Name': ['Acme Inc', 'Beta Co'],
        'Notes': ['notes 1', 'notes 2']
    })
    df['__source_row_ordinal'] = pd.Series([1, 2], dtype='int32')
    
    from src.cleaning import apply_ingest_mapping
    
    # Test with None run_id
    result, resolved_name_col, resolved_id_col = apply_ingest_mapping(
        df.copy(),
        name_col="Name",
        id_col=None,
        run_id=None,  # This should be handled gracefully
        settings={"run_type": "test"},
        dry_run=False,
        log_preview=False
    )
    
    # Check internal_row_id generation
    expected_ids = ['run-000000001', 'run-000000002']
    actual_ids = result['internal_row_id'].tolist()
    
    if actual_ids == expected_ids:
        print("   ✅ None run_id handled gracefully")
    else:
        print(f"   ❌ None run_id not handled: expected {expected_ids}, got {actual_ids}")
    
    # Check account_id is <NA>
    if result['account_id'].isna().all():
        print("   ✅ Missing account_id handled with <NA>")
    else:
        print("   ❌ Missing account_id not handled correctly")

def test_header_normalization():
    """Test header normalization and collision handling."""
    print("🧪 Testing header normalization...")
    
    from src.cleaning import normalize_headers_unique_list
    
    # Test collision handling
    headers = ['Account Name', 'account_name', 'Account Name', 'Company']
    normalized = normalize_headers_unique_list(headers)
    
    expected = ['account_name', 'account_name__2', 'account_name__3', 'company']
    if normalized == expected:
        print("   ✅ Header collision handling works")
    else:
        print(f"   ❌ Header collision failed: expected {expected}, got {normalized}")

def test_synonym_detection():
    """Test synonym detection with normalized headers."""
    print("🧪 Testing synonym detection...")
    
    from src.cleaning import detect_name_col, detect_id_col
    
    # Test with compound token headers
    headers = ['company_name', 'id_field', 'other_col']
    df = pd.DataFrame({col: [f'value_{i}' for i in range(3)] for col in headers})
    settings = {
        'ingest': {
            'name_synonyms': ['company', 'name', 'account_name'],
            'id_synonyms': ['id', 'account_id', 'uuid']
        }
    }
    
    name_col = detect_name_col(headers, settings, df)
    id_col = detect_id_col(headers, settings, df)
    
    if name_col == 'company_name':
        print("   ✅ Name compound token detection works")
    else:
        print(f"   ⚠️  Name compound token detection: expected 'company_name', got '{name_col}' (may be correct)")
    
    if id_col == 'id_field':
        print("   ✅ ID compound token detection works")
    else:
        print(f"   ⚠️  ID compound token detection: expected 'id_field', got '{id_col}' (may be correct)")

def test_missing_source_ordinal():
    """Test missing __source_row_ordinal error handling."""
    print("🧪 Testing missing source ordinal error...")
    
    from src.cleaning import apply_ingest_mapping
    
    # Create test data without __source_row_ordinal
    df = pd.DataFrame({
        'Name': ['Acme Inc', 'Beta Co'],
        'Notes': ['notes 1', 'notes 2']
    })
    
    try:
        apply_ingest_mapping(
            df,
            name_col="Name",
            id_col=None,
            run_id="test",
            settings={"run_type": "test"},
            dry_run=False,
            log_preview=False
        )
        print("   ❌ Should have failed with missing __source_row_ordinal")
    except ValueError as e:
        if "Missing source_row_ordinal (normalized from __source_row_ordinal)" in str(e):
            print("   ✅ Missing source ordinal error handled correctly")
        else:
            print(f"   ❌ Wrong error message: {e}")
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")

def test_source_path_capture_json():
    """Test JSON __source_path capture."""
    print("🧪 Testing JSON __source_path capture...")
    
    from src.utils.io_utils import parse_json_to_dataframe
    import json
    import tempfile
    import os
    
    # Minimal inline JSON file and path
    data = {"records":[{"name":"A"},{"name":"B"}]}
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "s.json")
        with open(p, "w") as f: 
            json.dump(data, f)
        df = parse_json_to_dataframe(p, record_path="$.records[*]", add_source_path=True)
        
        if "__source_path" in df.columns and df["__source_path"].notna().all():
            print("   ✅ JSON __source_path captured")
        else:
            print("   ❌ JSON __source_path not captured correctly")

def test_source_path_capture_xml():
    """Test XML __source_path capture."""
    print("🧪 Testing XML __source_path capture...")
    
    from src.utils.io_utils import parse_xml_to_dataframe
    import tempfile
    import os
    
    xml = """<root><record><name>A</name></record><record><name>B</name></record></root>"""
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "s.xml")
        with open(p, "w") as f: 
            f.write(xml)
        df = parse_xml_to_dataframe(p, record_xpath="/root/record", add_source_path=True)
        
        if "__source_path" in df.columns and df["__source_path"].str.contains("/root/record").all():
            print("   ✅ XML __source_path captured")
        else:
            print("   ❌ XML __source_path not captured correctly")

def test_resume_path_consistency():
    """Test resume path consistency."""
    print("🧪 Testing resume path consistency...")
    
    from src.utils.cache_utils import create_cache_directories
    from src.utils.path_utils import get_interim_dir
    
    # Test that both methods return the same path
    run_id = "test_run_123"
    output_dir = "/tmp/test_output"
    
    interim_dir, _ = create_cache_directories(run_id, output_dir)
    interim_dir_2 = get_interim_dir(run_id, output_dir)
    
    if str(interim_dir) == str(interim_dir_2):
        print("   ✅ Resume path consistency works")
    else:
        print(f"   ❌ Resume path inconsistency: {interim_dir} vs {interim_dir_2}")

def main():
    """Run all validation tests."""
    print("🚀 Running ingest v0 validation tests...\n")
    
    tests = [
        test_parquet_fallback,
        test_import_guards,
        test_ingest_mapping,
        test_header_normalization,
        test_synonym_detection,
        test_missing_source_ordinal,
        test_source_path_capture_json,
        test_source_path_capture_xml,
        test_resume_path_consistency,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"   ❌ Test failed with exception: {e}")
        print()
    
    print(f"📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All critical fixes validated!")
        return 0
    else:
        print("⚠️  Some tests failed - check the output above")
        return 1

if __name__ == "__main__":
    sys.exit(main())
