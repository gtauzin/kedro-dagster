import re

import pytest
from click.testing import CliRunner
from kedro.framework.cli.cli import info
from kedro.framework.startup import bootstrap_project

from kedro_dagster.cli import dagster_commands as cli_dagster
from kedro_dagster.cli import dev as cli_dev
from kedro_dagster.cli import init as cli_init


def _extract_cmd_from_help(msg: str) -> list[str]:
    """Parse Click help text and extract the list of top-level commands."""
    # [\s\S] is used instead of "." to match any character including new lines
    match = re.search(r"(?<=Commands:)([\s\S]+)$", msg)
    if not match:
        return []
    cmd_txt = match.group(1)
    cmd_list_detailed = cmd_txt.split("\n")

    cmd_list: list[str] = []
    for cmd_detailed in cmd_list_detailed:
        cmd_match = re.search(r"\w+(?=  )", string=cmd_detailed)
        if cmd_match is not None:
            cmd_list.append(cmd_match.group(0))
    return cmd_list


def test_dagster_commands_discovered(monkeypatch, kedro_project_no_dagster_config):
    monkeypatch.chdir(kedro_project_no_dagster_config)
    runner = CliRunner()
    result = runner.invoke(cli_dagster)

    assert result.exit_code == 0
    cmds = set(_extract_cmd_from_help(result.output))
    assert {"init", "dev"} == cmds
    assert "You have not updated your template yet" not in result.output


@pytest.mark.parametrize("inside_subdirectory", (True, False))
def test_cli_init_creates_files(monkeypatch, kedro_project_no_dagster_config, inside_subdirectory):
    # Ensure project is bootstrapped so package_name is known for definitions.py path
    bootstrap_project(kedro_project_no_dagster_config)

    cwd = kedro_project_no_dagster_config / "src" if inside_subdirectory else kedro_project_no_dagster_config
    monkeypatch.chdir(cwd)

    cli_runner = CliRunner()
    result = cli_runner.invoke(cli_init)

    assert result.exit_code == 0

    # dagster.yml written to base env by default
    dagster_yml = kedro_project_no_dagster_config / "conf" / "base" / "dagster.yml"
    assert dagster_yml.is_file()
    assert (
        "'conf/base/dagster.yml' successfully updated." in result.output
        or "A 'dagster.yml' already exists" in result.output
    )

    # definitions.py written in the package folder
    # Resolve package name by scanning src/* directory (kedro CLI creates a single package)
    pkg_dirs = list((kedro_project_no_dagster_config / "src").glob("*/definitions.py"))
    assert pkg_dirs, "definitions.py not created under src/<package>/"
    assert (
        "definitions.py' successfully updated." in result.output or "A 'definitions.py' already exists" in result.output
    )


def test_cli_init_existing_config_shows_warning(monkeypatch, kedro_project_no_dagster_config):
    monkeypatch.chdir(kedro_project_no_dagster_config)
    runner = CliRunner()
    # First initialization
    first = runner.invoke(cli_init)
    assert first.exit_code == 0

    # Second run without --force should warn and not fail
    second = runner.invoke(cli_init)
    assert second.exit_code == 0
    assert "A 'dagster.yml' already exists" in second.output
    assert "A 'definitions.py' already exists" in second.output


def test_cli_init_force_overwrites(monkeypatch, kedro_project_no_dagster_config):
    monkeypatch.chdir(kedro_project_no_dagster_config)
    runner = CliRunner()
    # Ensure files exist
    runner.invoke(cli_init)

    # Now force overwrite
    result = runner.invoke(cli_init, ["--force"])
    assert result.exit_code == 0
    assert "successfully updated" in result.output


def test_cli_init_with_wrong_env_prints_message(monkeypatch, kedro_project_no_dagster_config):
    monkeypatch.chdir(kedro_project_no_dagster_config)
    runner = CliRunner()
    result = runner.invoke(cli_init, ["--env", "debug"])  # non-existing env in starter
    assert result.exit_code == 0
    assert "No env 'debug' found" in result.output


def test_cli_init_silent_suppresses_success_logs(monkeypatch, kedro_project_no_dagster_config):
    monkeypatch.chdir(kedro_project_no_dagster_config)
    runner = CliRunner()
    result = runner.invoke(cli_init, ["--force", "--silent"])  # ensure it writes but no logs
    assert result.exit_code == 0
    assert "successfully updated" not in result.output


@pytest.mark.parametrize("inside_subdirectory", (True, False))
def test_cli_dev_invokes_dagster_with_defaults(
    monkeypatch, mocker, kedro_project_no_dagster_config, inside_subdirectory
):
    bootstrap_project(kedro_project_no_dagster_config)
    cwd = kedro_project_no_dagster_config / "src" if inside_subdirectory else kedro_project_no_dagster_config
    monkeypatch.chdir(cwd)

    runner = CliRunner()

    sp_call = mocker.patch("subprocess.call")
    result = runner.invoke(cli_dev)
    assert result.exit_code == 0

    # Expected default arguments from DevOptions
    # python_file default points to src/<package>/definitions.py
    # Resolve it dynamically similarly to DevOptions
    # We don't strictly require the file to exist for the call to be made
    called_args = sp_call.call_args[0][0]
    # Structure: ["dagster", "dev", "--python-file", file, "--log-level", lvl, "--log-format", fmt, "--host", host, "--port", port, "--live-data-poll-rate", poll]
    assert called_args[:2] == ["dagster", "dev"]
    args_map = {called_args[i]: called_args[i + 1] for i in range(2, len(called_args), 2)}
    assert args_map["--log-level"] == "info"
    assert args_map["--log-format"] in {"colored", "color"}  # accept template/config variations
    assert args_map["--host"] == "127.0.0.1"
    assert args_map["--port"] == "3000"
    assert args_map["--live-data-poll-rate"] == "2000"


def test_cli_dev_overrides(monkeypatch, mocker, kedro_project_no_dagster_config):
    monkeypatch.chdir(kedro_project_no_dagster_config)
    bootstrap_project(kedro_project_no_dagster_config)

    runner = CliRunner()
    sp_call = mocker.patch("subprocess.call")
    result = runner.invoke(
        cli_dev,
        [
            "--log-level",
            "debug",
            "--log-format",
            "json",
            "--host",
            "0.0.0.0",
            "--port",
            "4000",
            "--live-data-poll-rate",
            "1500",
        ],
    )
    assert result.exit_code == 0

    called_args = sp_call.call_args[0][0]
    args_map = {called_args[i]: called_args[i + 1] for i in range(2, len(called_args), 2)}
    assert args_map["--log-level"] == "debug"
    assert args_map["--log-format"] == "json"
    assert args_map["--host"] == "0.0.0.0"
    assert args_map["--port"] == "4000"
    assert args_map["--live-data-poll-rate"] == "1500"


def test_cli_plugin_shows_in_info(monkeypatch, tmp_path):
    # Sanity check that the plugin is discoverable by Kedro
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(info)
    assert result.exit_code == 0
    assert "kedro_dagster" in result.output
