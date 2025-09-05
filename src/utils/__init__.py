"""
Utility modules for the company junction pipeline.
"""

# Import from all utility modules
from .dtypes import (
    apply_dtypes,
    assert_no_unexpected_object_columns,
    drop_intermediate_columns,
    optimize_dataframe_memory,
    get_dtypes_for_schema,
)

from .logging_utils import setup_logging
from .path_utils import get_project_root, ensure_directory_exists, get_data_paths
# from .validation_utils import validate_dataframe  # Moved to deprecated/
from .io_utils import (
    get_file_info,
    list_data_files,
    load_settings,
    load_relationship_ranks,
)
from .perf_utils import (
    # to_arrow_strings,  # DEPRECATED: PyArrow backend removed
    narrow_sort,
    parse_name_core_tokens,
    build_vectorized_masks,
    apply_vectorized_disposition,
    optimize_dataframe_memory,
)
from .hash_utils import (
    stable_content_hash,
    stable_schema_hash,
    stable_file_hash,
    compute_file_hash,  # Backward compatibility
)
from .id_utils import sfid15_to_18, normalize_sfid_series, validate_sfid_format

__all__ = [
    # Dtype utilities
    "apply_dtypes",
    "assert_no_unexpected_object_columns",
    "drop_intermediate_columns",
    "optimize_dataframe_memory",
    "get_dtypes_for_schema",
    # Logging utilities
    "setup_logging",
    # Path utilities
    "get_project_root",
    "ensure_directory_exists",
    "get_data_paths",
    # Validation utilities
    # "validate_dataframe",  # Moved to deprecated/
    # I/O utilities
    "get_file_info",
    "list_data_files",
    "load_settings",
    "load_relationship_ranks",
    # Performance utilities
    # "to_arrow_strings",  # DEPRECATED: PyArrow backend removed
    "narrow_sort",
    "parse_name_core_tokens",
    "build_vectorized_masks",
    "apply_vectorized_disposition",
    "optimize_dataframe_memory",
    # Hash utilities
    "stable_content_hash",
    "stable_schema_hash",
    "stable_file_hash",
    "compute_file_hash",
    # ID utilities
    "sfid15_to_18",
    "normalize_sfid_series",
    "validate_sfid_format",
]
