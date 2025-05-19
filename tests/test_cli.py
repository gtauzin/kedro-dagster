# mypy: ignore-errors
import socket
import subprocess
import sys
import time
from pathlib import Path

from kedro_dagster.cli import commands


def test_dagster_init(cli_runner, kedro_project, metadata):
    """Test that 'kedro dagster init' creates the right files."""
    command = "dagster init --env local --force --silent"
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    # Check that dagster.yml and definitions.py are created
    dagster_yml = Path(kedro_project) / "conf" / "local" / "dagster.yml"
    definitions_py = Path(kedro_project) / "src" / "fake_project" / "definitions.py"
    assert dagster_yml.exists(), f"Missing: {dagster_yml}"
    assert definitions_py.exists(), f"Missing: {definitions_py}"


def test_dagster_dev(cli_runner, kedro_project, metadata):
    """Test that 'kedro dagster dev' launches the Dagster UI and occupies the port."""
    host = "127.0.0.1"
    port = 3000
    command = [
        sys.executable,
        "-m",
        "kedro",
        "dagster",
        "dev",
        "--env",
        "local",
        "--log-level",
        "info",
        "--log-format",
        "colored",
        "--port",
        str(port),
        "--host",
        host,
        "--live-data-poll-rate",
        "2000",
    ]
    # Start the process in a subprocess so we can terminate it
    proc = subprocess.Popen(
        command,
        cwd=kedro_project,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        # Wait for the server to start (increase timeout if needed)
        timeout = 10
        interval = 1
        for _ in range(timeout):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                result = sock.connect_ex((host, port))
                if result == 0:
                    break
            time.sleep(interval)
        else:
            out, err = proc.communicate(timeout=5)
            raise AssertionError(
                f"Dagster dev did not start or port {port} not occupied.\nstdout:\n{out}\nstderr:\n{err}"
            )
        # If we reach here, the port is occupied
        assert True
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()
