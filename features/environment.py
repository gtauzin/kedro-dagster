"""Behave environment setup commands"""
# mypy: ignore-errors

import os
import shutil
import stat
import tempfile
import time
from pathlib import Path

from features.steps.sh_run import run
from features.steps.util import create_new_venv


def call(cmd, env, verbose=False):
    res = run(cmd, env=env)
    if res.returncode or verbose:
        print(">", " ".join(cmd))
        print(res.stdout)
        print(res.stderr)
    assert res.returncode == 0


def before_all(context):
    """Environment preparation before other cli tests are run.
    Installs kedro by running pip in the top level directory.
    """

    # make a venv
    if "E2E_VENV" in os.environ:
        context.venv_dir = Path(os.environ["E2E_VENV"])
    else:
        context.venv_dir = create_new_venv()

    context = _setup_context_with_venv(context, context.venv_dir)

    call(
        [
            context.python,
            "-m",
            "pip",
            "install",
            "-U",
            # Temporarily pin pip to fix https://github.com/jazzband/pip-tools/issues/1503
            # This can be removed when Kedro 0.17.6 is released, because pip-tools is upgraded
            # for that version.
            "pip>=21.2",
            "setuptools>=38.0",
            "wheel",
            ".",
        ],
        env=context.env,
    )

    # install the plugin
    call([context.python, "-m", "pip", "install", "."], env=context.env)


def _setup_context_with_venv(context, venv_dir):
    context.venv_dir = venv_dir
    # note the locations of some useful stuff
    # this is because exe resolution in subprocess doesn't respect a passed env
    if os.name == "posix":
        bin_dir = context.venv_dir / "bin"
        path_sep = ":"
    else:
        bin_dir = context.venv_dir / "Scripts"
        path_sep = ";"
    context.pip = str(bin_dir / "pip")
    context.python = str(bin_dir / "python")
    context.kedro = str(bin_dir / "kedro")

    # clone the environment, remove any condas and venvs and insert our venv
    context.env = os.environ.copy()
    path = context.env["PATH"].split(path_sep)
    path = [p for p in path if not (Path(p).parent / "pyvenv.cfg").is_file()]
    path = [p for p in path if not (Path(p).parent / "conda-meta").is_dir()]
    path = [str(bin_dir)] + path
    context.env["PATH"] = path_sep.join(path)

    # Create an empty pip.conf file and point pip to it
    pip_conf_path = context.venv_dir / "pip.conf"
    pip_conf_path.touch()
    context.env["PIP_CONFIG_FILE"] = str(pip_conf_path)

    return context


def after_all(context):
    if "E2E_VENV" not in os.environ:
        # Best-effort cleanup of the temporary virtualenv. On Windows, file locks
        # from recently-terminated subprocesses can linger briefly; rmtree below
        # implements a retry strategy to tolerate that. Don't fail the suite if
        # final cleanup can't remove some locked files: use non-strict mode.
        rmtree(context.venv_dir, strict=False)


def before_scenario(context, feature):
    context.temp_dir = Path(tempfile.mkdtemp()).resolve()


def after_scenario(context, feature):
    # Ensure any background processes started during a scenario are terminated
    # before attempting to delete temporary directories (Windows file locks).
    proc = getattr(context, "process", None)
    if proc is not None:
        try:
            # Best effort: terminate, wait and close pipes to release handles.
            proc.terminate()
            proc.wait(timeout=20)
            try:
                if getattr(proc, "stdout", None):
                    proc.stdout.close()  # type: ignore[call-arg]
            except Exception:
                pass
        except Exception:
            # Don't block cleanup if termination fails; continue to cleanup with retries.
            pass
        finally:
            # Remove reference to help GC any remaining resources promptly.
            try:
                delattr(context, "process")
            except Exception:
                pass

    # Small delay to allow the OS to release file handles (especially on Windows).
    if os.name == "nt":
        time.sleep(0.5)

    rmtree(context.temp_dir)


def rmtree(top: Path, retries: int = 8, delay: float = 0.25, strict: bool = True) -> None:
    """Robust rmtree that tolerates transient Windows file locks.

    - On Windows, processes may still hold handles on files (e.g. logs, DLLs/PYD)
      for a short time after exit. We retry with exponential backoff.
    - We also attempt to remove read-only attributes before unlinking.
    """

    path_str = str(top)

    def _chmod_writeable(path: str) -> None:
        try:
            mode = os.stat(path).st_mode
            os.chmod(path, mode | stat.S_IWUSR | stat.S_IREAD)
        except Exception:
            # Best effort only.
            pass

    def _onerror(func, path, exc_info):
        # Legacy callback signature: (func, path, exc_info)
        _chmod_writeable(path)
        try:
            func(path)
        except Exception:
            # Re-raise to let the outer retry handle it
            raise

    def _onexc(func, path, exc):
        # Newer callback signature (Python >= 3.12/3.13): (func, path, exc)
        _chmod_writeable(path)
        try:
            func(path)
        except Exception:
            # Re-raise to let the outer retry handle it
            raise

    # Try to atomically rename the directory out of the way first. This often
    # succeeds even if some files are locked, allowing callers to proceed
    # without races on the original path.
    if os.path.isdir(path_str):
        try:
            base = path_str.rstrip("\\/")
            renamed = f"{base}.del-{int(time.time() * 1000)}"
            os.replace(path_str, renamed)
            path_str = renamed
        except Exception:
            # If rename fails, we'll proceed to delete in place.
            pass

    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            # Make files writeable first on Windows to reduce failures.
            if os.name == "nt" and os.path.exists(path_str):
                for root, dirs, files in os.walk(path_str, topdown=False):
                    for name in files:
                        _chmod_writeable(os.path.join(root, name))
                    for name in dirs:
                        _chmod_writeable(os.path.join(root, name))

            # Prefer the newer onexc API when available, fall back to onerror.
            try:
                shutil.rmtree(path_str, onexc=_onexc)  # type: ignore[call-arg]
            except TypeError:
                shutil.rmtree(path_str, onerror=_onerror)  # type: ignore[call-arg]
            return
        except (PermissionError, OSError) as exc:
            last_exc = exc
            if attempt >= retries - 1:
                break
            # Exponential backoff on Windows, constant delay elsewhere.
            sleep_s = delay * (2**attempt) if os.name == "nt" else delay
            time.sleep(sleep_s)

    # If we get here, all attempts failed.
    if last_exc is not None and strict:
        raise last_exc
    # Non-strict mode: best-effort cleanup; print a warning and continue.
    if last_exc is not None and not strict:
        try:
            print(f"[cleanup-warning] Failed to delete {path_str}: {last_exc}")
        except Exception:
            pass
