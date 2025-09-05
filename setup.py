#!/usr/bin/env python3
"""Setup script for company_junction package.
"""

from setuptools import find_packages, setup

setup(
    name="company_junction",
    version="1.13.7",
    description="Salesforce Account deduplication pipeline",
    author="Company Junction Team",
    packages=find_packages(include=["src*", "app*"]),
    python_requires=">=3.10",
    install_requires=[
        "pandas>=1.5.0",
        "streamlit>=1.25.0",
        "rapidfuzz>=3.0.0",
        "pyyaml>=6.0",
        "openpyxl>=3.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=23.0.0",
            "ruff>=0.1.0",
            "mypy>=1.5.0",
            "pandas-stubs>=2.0.0",
            "types-PyYAML>=6.0.0",
            "types-streamlit>=1.25.0",
        ],
    },
)
