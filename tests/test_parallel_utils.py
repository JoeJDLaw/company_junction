"""Tests for parallel execution utilities."""

from unittest.mock import patch, MagicMock
import threading

from src.utils.parallel_utils import (
    is_loky_available,
    select_backend,
    ParallelExecutor,
    create_parallel_executor,
)


class TestLokyAvailability:
    """Test loky backend availability detection."""

    def test_is_loky_available_returns_bool(self) -> None:
        """Test that is_loky_available returns a boolean value."""
        result = is_loky_available()
        assert isinstance(result, bool)

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", False)
    def test_is_loky_available_when_joblib_unavailable(self) -> None:
        """Test loky availability when joblib is not available."""
        result = is_loky_available()
        assert result is False

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True)
    @patch("src.utils.parallel_utils.Parallel")
    def test_is_loky_available_when_loky_works(self, mock_parallel) -> None:
        """Test loky availability when loky backend works."""
        # Mock successful loky execution
        mock_parallel.return_value.return_value = [1]

        result = is_loky_available()
        assert result is True

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True)
    @patch("src.utils.parallel_utils.Parallel")
    def test_is_loky_available_when_loky_fails(self, mock_parallel) -> None:
        """Test loky availability when loky backend fails."""
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
        self, mock_loky_available
    ) -> None:
        """Test backend selection when loky is requested but unavailable."""
        mock_loky_available.return_value = False

        backend, reason = select_backend("loky")
        assert backend == "threading"
        assert reason == "fallback_unsupported"

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True)
    @patch("src.utils.parallel_utils.is_loky_available")
    def test_select_backend_requested_loky_and_available(
        self, mock_loky_available
    ) -> None:
        """Test backend selection when loky is requested and available."""
        mock_loky_available.return_value = True

        backend, reason = select_backend("loky")
        assert backend == "loky"
        assert reason == "requested"

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True)
    @patch("src.utils.parallel_utils.is_loky_available")
    def test_select_backend_default_when_loky_available(
        self, mock_loky_available
    ) -> None:
        """Test default backend selection when loky is available."""
        mock_loky_available.return_value = True

        backend, reason = select_backend("unknown")
        assert backend == "loky"
        assert reason == "default"

    @patch("src.utils.parallel_utils.JOBLIB_AVAILABLE", True)
    @patch("src.utils.parallel_utils.is_loky_available")
    def test_select_backend_default_when_loky_unavailable(
        self, mock_loky_available
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
        self, mock_calculate_workers, mock_loky_available
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


class TestCreateParallelExecutor:
    """Test create_parallel_executor factory function."""

    @patch("src.utils.parallel_utils.ParallelExecutor")
    def test_create_parallel_executor(self, mock_executor_class) -> None:
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
