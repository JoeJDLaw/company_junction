#!/usr/bin/env python3
"""Comprehensive test runner for Phase 2.0.0 testing plan."""

import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], description: str) -> bool:
    """Run a command and return success status."""
    print(f"\n🚀 {description}")
    print(f"   Command: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=False)
    
    if result.returncode == 0:
        print(f"✅ {description} - PASSED")
        return True
    else:
        print(f"❌ {description} - FAILED (exit code: {result.returncode})")
        return False


def main():
    """Main function to run all Phase 2 tests."""
    print("🎯 Phase 2.0.0 Testing Plan - Comprehensive Test Runner")
    print("=" * 60)
    
    # Check if we're in the right directory
    if not Path("src").exists():
        print("❌ Error: src/ directory not found. Run this script from the project root.")
        sys.exit(1)
    
    # Test categories
    test_categories = [
        {
            "name": "Unit Tests",
            "cmd": ["pytest", "tests/", "-v", "--tb=short"],
            "description": "Running all unit tests"
        },
        {
            "name": "Property-Based Tests", 
            "cmd": ["pytest", "tests/test_similarity_property_based.py", "-v", "--tb=short"],
            "description": "Running property-based tests with Hypothesis"
        },
        {
            "name": "E2E Tests",
            "cmd": ["pytest", "tests/test_resume_e2e.py", "-v", "--tb=short"],
            "description": "Running end-to-end resume tests"
        },
        {
            "name": "File Format Tests",
            "cmd": ["pytest", "tests/test_file_formats.py", "-v", "--tb=short"],
            "description": "Running file format detection tests"
        },
        {
            "name": "CLI Integration Tests",
            "cmd": ["pytest", "tests/test_cli_integration.py", "-v", "--tb=short"],
            "description": "Running CLI integration tests"
        },
        {
            "name": "Coverage Analysis",
            "cmd": ["pytest", "--cov=src", "--cov-branch", "--cov-report=term-missing:skip-covered", "--cov-report=xml"],
            "description": "Running tests with coverage analysis"
        }
    ]
    
    # Run all test categories
    results = {}
    for category in test_categories:
        success = run_command(category["cmd"], category["description"])
        results[category["name"]] = success
    
    # Run coverage gates check
    if results.get("Coverage Analysis", False):
        print(f"\n🚀 Checking coverage gates...")
        coverage_success = run_command(
            ["python", "scripts/check_coverage_gates.py"],
            "Running coverage gates check"
        )
        results["Coverage Gates"] = coverage_success
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Phase 2.0.0 Testing Results Summary")
    print("=" * 60)
    
    all_passed = True
    for category, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {category:<25} {status}")
        if not success:
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 All Phase 2.0.0 tests passed!")
        print("✅ Ready for production deployment")
        sys.exit(0)
    else:
        print("💥 Some Phase 2.0.0 tests failed!")
        print("❌ Review failures before proceeding")
        sys.exit(1)


if __name__ == "__main__":
    main()
