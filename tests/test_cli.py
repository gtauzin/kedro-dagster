# mypy: ignore-errors
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
    """When passing a custom port, verify Dagster UI launches on that port and never mentions 3030."""
    host = "127.0.0.1"
    custom_port = 4040
    default_port = 3030

    cmd = [
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
        "--host",
        host,
        "--port",
        str(custom_port),
        "--live-data-poll-rate",
        "2000",
    ]

    proc = subprocess.Popen(
        cmd,
        cwd=kedro_project,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        deadline = time.time() + 10
        url_line = None

        while time.time() < deadline:
            line = proc.stdout.readline()
            if not line:
                time.sleep(0.1)
                continue
            # Only match the explicit UI startup message
            if "Serving dagster-webserver on" in line:
                url_line = line.strip()
                break

        assert url_line is not None, "Did not receive Dagster UI startup message within 10 s"

        # Verify the custom port is used
        assert f"{host}:{custom_port}" in url_line, f"Expected UI on {host}:{custom_port}, but got: {url_line}"

        # Ensure the default port is not mentioned
        assert str(default_port) not in url_line, (
            f"Default port {default_port} must not appear in the UI URL: {url_line}"
        )

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
