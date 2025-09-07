#!/usr/bin/env python3
"""Script to check coverage gates locally."""

import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str]) -> tuple[int, str, str]:
    """Run a command and return exit code, stdout, stderr."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def check_overall_coverage():
    """Check overall coverage gate (75%)."""
    print("🔍 Checking overall coverage gate (75%)...")
    
    cmd = ["coverage", "report", "--fail-under=75", "--show-missing"]
    exit_code, stdout, stderr = run_command(cmd)
    
    if exit_code == 0:
        print("✅ Overall coverage gate passed (≥75%)")
        return True
    else:
        print("❌ Overall coverage gate failed (<75%)")
        print(stdout)
        print(stderr)
        return False


def check_critical_modules_coverage():
    """Check critical modules coverage gate (90%)."""
    print("\n🔍 Checking critical modules coverage gate (90%)...")
    
    critical_modules = [
        "src/cleaning.py",
        "src/disposition.py", 
        "src/edge_grouping.py",
        "src/survivorship.py"
    ]
    
    all_passed = True
    
    for module in critical_modules:
        print(f"  Checking {module}...")
        cmd = ["coverage", "report", f"--include={module}", "--fail-under=90"]
        exit_code, stdout, stderr = run_command(cmd)
        
        if exit_code == 0:
            print(f"  ✅ {module} coverage ≥90%")
        else:
            print(f"  ❌ {module} coverage <90%")
            print(f"    {stdout}")
            all_passed = False
    
    return all_passed


def main():
    """Main function to check all coverage gates."""
    print("🚀 Running coverage gates check...")
    
    # Check if we're in the right directory
    if not Path("src").exists():
        print("❌ Error: src/ directory not found. Run this script from the project root.")
        sys.exit(1)
    
    # Check if coverage data exists
    if not Path("coverage.xml").exists():
        print("❌ Error: coverage.xml not found. Run 'pytest --cov=src --cov-report=xml' first.")
        sys.exit(1)
    
    # Check overall coverage
    overall_passed = check_overall_coverage()
    
    # Check critical modules coverage
    critical_passed = check_critical_modules_coverage()
    
    # Summary
    print("\n📊 Coverage Gates Summary:")
    print(f"  Overall coverage (≥75%): {'✅ PASS' if overall_passed else '❌ FAIL'}")
    print(f"  Critical modules (≥90%): {'✅ PASS' if critical_passed else '❌ FAIL'}")
    
    if overall_passed and critical_passed:
        print("\n🎉 All coverage gates passed!")
        sys.exit(0)
    else:
        print("\n💥 Some coverage gates failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
