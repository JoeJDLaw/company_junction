# Company Junction Makefile

.PHONY: regen-stress
regen-stress:
	python scripts/generate_stress_dataset.py --fixed-today 2024-01-01

.PHONY: test-stress-manifest
test-stress-manifest:
	python tests/test_stress_dataset_manifest.py

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  regen-stress        - Regenerate stress dataset with fixed date"
	@echo "  test-stress-manifest - Run stress dataset validation tests"
	@echo "  help               - Show this help message"
