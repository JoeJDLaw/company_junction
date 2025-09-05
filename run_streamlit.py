#!/usr/bin/env python3
"""Wrapper script to run Streamlit with better interrupt handling.
"""

import logging
import signal
import subprocess
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def signal_handler(signum: int, frame: object) -> None:
    """Handle interrupt signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)


def main() -> None:
    """Run Streamlit with enhanced error handling."""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Run Streamlit with the main app - use virtual environment Python if available
        python_executable = sys.executable
        if ".venv" in python_executable or "venv" in python_executable:
            # Already in virtual environment
            pass
        else:
            # Try to use virtual environment Python
            import os

            venv_python = os.path.join(os.getcwd(), ".venv", "bin", "python")
            if os.path.exists(venv_python):
                python_executable = venv_python
                logger.info(f"Using virtual environment Python: {python_executable}")

        cmd = [python_executable, "-m", "streamlit", "run", "app/main.py"]
        logger.info("Starting Streamlit app...")
        logger.info(f"Command: {' '.join(cmd)}")

        # Run the process
        process = subprocess.run(cmd, check=False)

        if process.returncode == 0:
            logger.info("Streamlit app exited successfully")
        else:
            logger.warning(f"Streamlit app exited with code {process.returncode}")

    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error running Streamlit: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
