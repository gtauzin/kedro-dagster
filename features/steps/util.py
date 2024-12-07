"""Common functions for e2e testing."""

import tempfile
import venv
from pathlib import Path


def create_new_venv() -> Path:
    """Create a new venv.

    Returns:
        path to created venv
    """
    # Create venv
    venv_dir = Path(tempfile.mkdtemp())
    venv.main([str(venv_dir)])
    return venv_dir
