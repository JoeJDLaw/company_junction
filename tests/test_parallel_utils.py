"""Tests for parallel execution utilities."""

import threading
from unittest.mock import MagicMock, patch

from src.utils.parallel_utils import (
    ParallelExecutor,
    create_parallel_executor,
    is_loky_available,
    select_backend,
)


class TestLokyAvailability:
    """Test loky backend availability detection."""

    def test_is_loky_available_returns_bool(self) -> None:
        """Test that is_loky_available returns a boolean value."""
        result = is_loky_available()
        assert isinstance(result, bool)

    @patch("src.utils.parallel_utils._try_import_joblib", return_value=False)
    def test_is_loky_available_when_joblib_unavailable(
        self,
        mock_try_import: MagicMock,
    ) -> None:
        """Test loky availability when joblib is not available."""
        # Reset cache to ensure test isolation
        import src.utils.parallel_utils

        src.utils.parallel_utils._LOKY_AVAILABLE = None

        result = is_loky_available()
        assert result is False

    @patch("src.utils.parallel_utils._try_import_joblib", return_value=True)
    @patch("src.utils.parallel_utils.Parallel")
    def test_is_loky_available_when_loky_works(
        self,
        mock_parallel: MagicMock,
        mock_try_import: MagicMock,
    ) -> None:
        """Test loky availability when loky backend works."""
        # Reset cache to ensure test isolation
        import src.utils.parallel_utils

        src.utils.parallel_utils._LOKY_AVAILABLE = None

        # Mock successful loky execution
        mock_parallel.return_value.return_value = [1]

        result = is_loky_available()
        assert result is True

    @patch("src.utils.parallel_utils._try_import_joblib", return_value=True)
    @patch("src.utils.parallel_utils.Parallel")
    def test_is_loky_available_when_loky_fails(
        self,
        mock_parallel: MagicMock,
        mock_try_import: MagicMock,
    ) -> None:
        """Test loky availability when loky backend fails."""
        # Reset cache to ensure test isolation
        import src.utils.parallel_utils

        src.utils.parallel_utils._LOKY_AVAILABLE = None

        # Mock failed loky execution
        mock_parallel.side_effect = Exception("loky failed")

        result = is_loky_available()
        assert result is False


class TestBackendSelection:
    """Test backend selection logic."""

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", False)
    def test_select_backend_when_joblib_unavailable(self) -> None:
        """Test backend selection when joblib is not available."""
        backend, reason = select_backend("loky")
        assert backend == "sequential"
        assert reason == "joblib_unavailable"

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True)
    def test_select_backend_requested_threading(self) -> None:
        """Test backend selection when threading is requested."""
        backend, reason = select_backend("threading")
        assert backend == "threading"
        assert reason == "requested"

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True)
    @patch("src.utils.parallel_utils.is_loky_available")
    def test_select_backend_requested_loky_but_unavailable_falls_back_to_threading(
        self,
        mock_loky_available: MagicMock,
    ) -> None:
        """Test backend selection when loky is requested but unavailable."""
        mock_loky_available.return_value = False

        backend, reason = select_backend("loky")
        assert backend == "threading"
        assert reason == "fallback_unsupported"

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True)
    @patch("src.utils.parallel_utils.is_loky_available")
    def test_select_backend_requested_loky_and_available(
        self,
        mock_loky_available: MagicMock,
    ) -> None:
        """Test backend selection when loky is requested and available."""
        mock_loky_available.return_value = True

        backend, reason = select_backend("loky")
        assert backend == "loky"
        assert reason == "requested"

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True)
    @patch("src.utils.parallel_utils.is_loky_available")
    def test_select_backend_default_when_loky_available(
        self,
        mock_loky_available: MagicMock,
    ) -> None:
        """Test default backend selection when loky is available."""
        mock_loky_available.return_value = True

        backend, reason = select_backend("unknown")
        assert backend == "loky"
        assert reason == "default"

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True)
    @patch("src.utils.parallel_utils.is_loky_available")
    def test_select_backend_default_when_loky_unavailable(
        self,
        mock_loky_available: MagicMock,
    ) -> None:
        """Test default backend selection when loky is unavailable."""
        mock_loky_available.return_value = False

        backend, reason = select_backend("unknown")
        assert backend == "threading"
        assert reason == "fallback_unsupported"


class TestParallelExecutor:
    """Test ParallelExecutor class."""

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True)
    @patch("src.utils.parallel_utils.is_loky_available")
    @patch("src.utils.parallel_utils.calculate_optimal_workers")
    def test_parallel_executor_initialization(
        self,
        mock_calculate_workers: MagicMock,
        mock_loky_available: MagicMock,
    ) -> None:
        """Test ParallelExecutor initialization."""
        mock_calculate_workers.return_value = 4
        mock_loky_available.return_value = True

        executor = ParallelExecutor(workers=4, backend="loky")

        assert executor.workers == 4
        assert executor.backend == "loky"
        assert executor.backend_reason == "requested"

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", False)
    def test_parallel_executor_when_joblib_unavailable(self) -> None:
        """Test ParallelExecutor when joblib is not available."""
        executor = ParallelExecutor(workers=4, backend="loky")

        assert executor.workers == 1
        assert executor.backend == "sequential"
        assert executor.backend_reason == "joblib_unavailable"

    def test_parallel_executor_with_stop_flag(self) -> None:
        """Test ParallelExecutor with stop flag support."""
        stop_flag = threading.Event()
        executor = ParallelExecutor(stop_flag=stop_flag)

        assert executor.stop_flag == stop_flag

    def test_parallel_executor_default_stop_flag(self) -> None:
        """Test ParallelExecutor creates default stop flag when none provided."""
        executor = ParallelExecutor()

        assert isinstance(executor.stop_flag, threading.Event)

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", False)
    def test_execute_with_stop_flag(self) -> None:
        """Test execute method respects stop flag."""
        stop_flag = threading.Event()
        executor = ParallelExecutor(stop_flag=stop_flag)

        # Set stop flag after first item
        def func_with_stop(item):
            if item == 1:
                stop_flag.set()
            return item * 2

        items = [1, 2, 3, 4, 5]
        results = executor.execute(func_with_stop, items, "test_operation")

        # Should only process first item before stop flag is set
        assert len(results) == 1
        assert results[0] == 2

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", False)
    def test_execute_chunked_with_stop_flag(self) -> None:
        """Test execute_chunked method respects stop flag."""
        stop_flag = threading.Event()
        executor = ParallelExecutor(stop_flag=stop_flag, chunk_size=2)

        # Set stop flag after first chunk
        def func_with_stop(chunk):
            if chunk[0] == 1:
                stop_flag.set()
            return [x * 2 for x in chunk]

        items = [1, 2, 3, 4, 5, 6]
        results = executor.execute_chunked(func_with_stop, items, operation_name="test")

        # Should only process first chunk before stop flag is set
        assert len(results) == 2
        assert results == [2, 4]


class TestParallelExecutorChunking:
    """Test improved chunking logic in ParallelExecutor."""

    def test_balanced_chunking_large_input(self):
        """Test that large inputs create balanced chunks."""
        # Mock joblib availability
        with patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True):
            executor = ParallelExecutor(
                workers=12,
                backend="loky",
                chunk_size=1000,
                small_input_threshold=10000,
            )

            # Test with large input (366,895 items as mentioned in requirements)
            large_items = list(range(366895))

            # Mock the function to just return chunk info
            def mock_func(chunk):
                return {"size": len(chunk), "first": chunk[0] if chunk else None}

            # Execute chunked operation
            results = executor.execute_chunked(
                mock_func,
                large_items,
                operation_name="test_chunking",
            )

            # Verify we get reasonable chunk sizes
            assert len(results) > 0, "Should return results"

            # Check that chunk sizes are reasonable (not 1-item chunks)
            # With 12 workers, we should target ~36 chunks (12 * 3)
            # Each chunk should be around 10,000 items
            expected_chunk_size = 366895 // 36  # ~10,000
            tolerance = 0.5  # Allow 50% variation

            for result in results:
                chunk_size = result["size"]
                assert chunk_size >= 100, f"Chunk size {chunk_size} should be >= 100"
                assert chunk_size >= expected_chunk_size * (
                    1 - tolerance
                ), f"Chunk size {chunk_size} too small"
                assert chunk_size <= expected_chunk_size * (
                    1 + tolerance
                ), f"Chunk size {chunk_size} too large"

    def test_sequential_fallback_small_input(self):
        """Test that small inputs fall back to sequential processing."""
        with patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True):
            executor = ParallelExecutor(
                workers=12,
                backend="loky",
                chunk_size=1000,
                small_input_threshold=10000,
            )

            # Test with small input (5,000 items)
            small_items = list(range(5000))

            def mock_func(chunk):
                return {"size": len(chunk), "first": chunk[0] if chunk else None}

            results = executor.execute_chunked(
                mock_func,
                small_items,
                operation_name="test_small_input",
            )

            # Should use sequential processing for small input
            assert len(results) > 0, "Should return results"

            # Verify chunk sizes are reasonable even for sequential
            for result in results:
                chunk_size = result["size"]
                assert chunk_size >= 100, f"Chunk size {chunk_size} should be >= 100"


class TestCreateParallelExecutor:
    """Test create_parallel_executor factory function."""

    @patch("src.utils.parallel_utils.ParallelExecutor")
    def test_create_parallel_executor(self, mock_executor_class: MagicMock) -> None:
        """Test create_parallel_executor function."""
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        stop_flag = threading.Event()
        result = create_parallel_executor(
            workers=4,
            backend="loky",
            chunk_size=1000,
            small_input_threshold=5000,
            disable_parallel=False,
            stop_flag=stop_flag,
        )

        mock_executor_class.assert_called_once_with(
            workers=4,
            backend="loky",
            chunk_size=1000,
            small_input_threshold=5000,
            disable_parallel=False,
            stop_flag=stop_flag,
        )
        assert result == mock_executor
