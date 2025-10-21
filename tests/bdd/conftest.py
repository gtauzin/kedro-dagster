from __future__ import annotations

import os
import shutil
import stat
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from .utils.sh_run import run
from .utils.venv import create_new_venv

OK_EXIT_CODE = 0


class Ctx:
    """Simple context holder to mimic Behave's context in pytest-bdd steps."""

    def __init__(self) -> None:
        self.temp_dir: Path = Path()
        self.venv_dir: Path = Path()
        self.env: dict[str, str] = {}
        self.python: str = ""
        self.pip: str = ""
        self.kedro: str = ""
        self.config_file: Path = Path()
        self.project_name: str = ""
        self.package_name: str = ""
        self.root_project_dir: Path = Path()
        self.result = None
        self.process = None


def _setup_context_with_venv(ctx: Ctx, venv_dir: Path) -> Ctx:
    ctx.venv_dir = venv_dir
    if os.name == "posix":
        bin_dir = ctx.venv_dir / "bin"
        path_sep = ":"
    else:
        bin_dir = ctx.venv_dir / "Scripts"
        path_sep = ";"
    ctx.pip = str(bin_dir / "pip")
    ctx.python = str(bin_dir / "python")
    ctx.kedro = str(bin_dir / "kedro")

    env = os.environ.copy()
    # Remove existing virtualenvs/conda from PATH and prepend our venv
    path = env.get("PATH", "").split(path_sep)
    path = [p for p in path if not (Path(p).parent / "pyvenv.cfg").is_file()]
    path = [p for p in path if not (Path(p).parent / "conda-meta").is_dir()]
    path = [str(bin_dir)] + path
    env["PATH"] = path_sep.join(path)
    # Isolate pip config to prevent interference
    pip_conf_path = ctx.venv_dir / "pip.conf"
    pip_conf_path.touch()
    env["PIP_CONFIG_FILE"] = str(pip_conf_path)
    ctx.env = env
    return ctx


@pytest.fixture(scope="session")
def session_ctx() -> Generator[Ctx, None, None]:
    """Create a dedicated venv and install the current package once per test session."""
    ctx = Ctx()
    # Allow reusing an external venv for CI speedups
    venv_dir = Path(os.environ.get("E2E_VENV", "")) if os.environ.get("E2E_VENV") else create_new_venv()
    _setup_context_with_venv(ctx, venv_dir)

    # Ensure we install the project and plugin into this venv
    for cmd in [
        [ctx.python, "-m", "pip", "install", "-U", "pip>=21.2", "setuptools>=38.0", "wheel", "."],
        [ctx.python, "-m", "pip", "install", "."],
    ]:
        res = run(cmd, env=ctx.env)
        if res.returncode != OK_EXIT_CODE:
            pytest.fail(f"Failed to set up session environment:\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}")

    try:
        yield ctx
    finally:
        # Only delete venv if it was created by us
        if "E2E_VENV" not in os.environ:

            def rmtree(top: Path) -> None:
                if os.name != "posix":
                    for root, _, files in os.walk(str(top), topdown=False):
                        for name in files:
                            os.chmod(os.path.join(root, name), stat.S_IWUSR)
                shutil.rmtree(str(top))

            rmtree(ctx.venv_dir)


@pytest.fixture
def ctx(session_ctx: Ctx) -> Generator[Ctx, None, None]:
    """Per-scenario context with a fresh temp directory and no running process."""
    # Fresh copy to avoid accidental carry-over
    ctx = session_ctx
    ctx.temp_dir = Path(tempfile.mkdtemp()).resolve()
    # Clear per-scenario state
    ctx.result = None
    ctx.process = None
    try:
        yield ctx
    finally:
        # Clean temp dir
        shutil.rmtree(str(ctx.temp_dir))
        # Ensure background process is terminated
        if ctx.process is not None:
            ctx.process.terminate()
            ctx.process.wait()
            ctx.process = None
