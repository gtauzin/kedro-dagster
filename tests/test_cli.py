import re

import pytest
from click.testing import CliRunner
from kedro.framework.cli.cli import info
from kedro.framework.startup import bootstrap_project

from kedro_dagster.cli import dagster_commands as cli_dagster
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


def test_dagster_commands_discovered(monkeypatch, kedro_project_no_dagster_config_base):
    """Discover 'dagster' plugin commands in the Kedro CLI entrypoint."""
    options = kedro_project_no_dagster_config_base
    project_path = options.project_path
    monkeypatch.chdir(project_path)
    runner = CliRunner()
    result = runner.invoke(cli_dagster)

    assert result.exit_code == 0
    cmds = set(_extract_cmd_from_help(result.output))
    assert {"init", "dev"}.issubset(cmds)
    assert "You have not updated your template yet" not in result.output


@pytest.mark.parametrize("inside_subdirectory", (True, False))
def test_cli_init_creates_files(monkeypatch, kedro_project_no_dagster_config_base, inside_subdirectory):
    """CLI 'init' writes dagster.yml and package definitions.py in the project."""
    # Ensure project is bootstrapped so package_name is known for definitions.py path
    options = kedro_project_no_dagster_config_base
    project_path = options.project_path
    bootstrap_project(project_path)

    cwd = project_path / "src" if inside_subdirectory else project_path
    monkeypatch.chdir(cwd)

    cli_runner = CliRunner()
    result = cli_runner.invoke(cli_init)

    assert result.exit_code == 0

    # dagster.yml written to base env by default
    dagster_yml = project_path / "conf" / options.env / "dagster.yml"
    assert dagster_yml.is_file()
    assert (
        "'conf/base/dagster.yml' successfully updated." in result.output
        or "A 'dagster.yml' already exists" in result.output
    )

    # definitions.py written in the package folder
    # Resolve package name by scanning src/* directory (kedro CLI creates a single package)
    pkg_dirs = list((project_path / "src").glob("*/definitions.py"))
    assert pkg_dirs, "definitions.py not created under src/<package>/"
    assert (
        "definitions.py' successfully updated." in result.output or "A 'definitions.py' already exists" in result.output
    )

    # dg.toml is written at project root
    dg_toml = project_path / "dg.toml"
    assert dg_toml.is_file()
    assert ("'dg.toml' successfully updated." in result.output) or ("A 'dg.toml' already exists" in result.output)


def test_cli_init_existing_config_shows_warning(monkeypatch, kedro_project_no_dagster_config_base):
    """A second 'init' without --force warns about existing config files."""
    project_path = kedro_project_no_dagster_config_base.project_path
    monkeypatch.chdir(project_path)
    runner = CliRunner()
    # First initialization
    first = runner.invoke(cli_init)
    assert first.exit_code == 0

    # Second run without --force should warn and not fail
    second = runner.invoke(cli_init)
    assert second.exit_code == 0
    assert "A 'dagster.yml' already exists" in second.output
    assert "A 'definitions.py' already exists" in second.output
    assert "A 'dg.toml' already exists" in second.output


def test_cli_init_force_overwrites(monkeypatch, kedro_project_no_dagster_config_base):
    """'init --force' overwrites existing configuration files successfully."""
    project_path = kedro_project_no_dagster_config_base.project_path
    monkeypatch.chdir(project_path)
    runner = CliRunner()
    # Ensure files exist
    runner.invoke(cli_init)

    # Now force overwrite
    result = runner.invoke(cli_init, ["--force"])
    assert result.exit_code == 0
    assert "successfully updated" in result.output
    # Ensure dg.toml exists after force overwrite
    assert (kedro_project_no_dagster_config_base.project_path / "dg.toml").is_file()


def test_cli_init_with_wrong_env_prints_message(monkeypatch, kedro_project_no_dagster_config_base):
    """Invalid --env prints a helpful message and exits cleanly."""
    project_path = kedro_project_no_dagster_config_base.project_path
    monkeypatch.chdir(project_path)
    runner = CliRunner()
    result = runner.invoke(cli_init, ["--env", "debug"])  # non-existing env in starter
    assert result.exit_code == 0
    assert "No env 'debug' found" in result.output


def test_cli_init_silent_suppresses_success_logs(monkeypatch, kedro_project_no_dagster_config_base):
    """'--silent' suppresses success logs while still performing updates."""
    project_path = kedro_project_no_dagster_config_base.project_path
    monkeypatch.chdir(project_path)
    runner = CliRunner()
    result = runner.invoke(cli_init, ["--force", "--silent"])  # ensure it writes but no logs
    assert result.exit_code == 0
    assert "successfully updated" not in result.output


@pytest.mark.parametrize("inside_subdirectory", (True, False))
def test_cli_dev_invokes_dg(monkeypatch, mocker, kedro_project_no_dagster_config_base, inside_subdirectory):
    """'kedro dagster dev' proxies to 'dg dev' with pass-through args."""
    project_path = kedro_project_no_dagster_config_base.project_path
    cwd = project_path / "src" if inside_subdirectory else project_path
    monkeypatch.chdir(cwd)

    runner = CliRunner()

    sp_call = mocker.patch("kedro_dagster.cli.subprocess.call")
    result = runner.invoke(cli_dagster, ["dev"])  # no explicit flags; wrapper proxies to dg
    assert result.exit_code == 0

    called_args = sp_call.call_args[0][0]
    # Structure begins with ["dg", "dev", ...]
    assert called_args[:2] == ["dg", "dev"]


def test_cli_dev_overrides_forwarded(monkeypatch, mocker, kedro_project_no_dagster_config_base):
    """Explicit flags are forwarded to 'dg dev' unmodified."""
    project_path = kedro_project_no_dagster_config_base.project_path
    monkeypatch.chdir(project_path)
    bootstrap_project(project_path)

    runner = CliRunner()
    sp_call = mocker.patch("kedro_dagster.cli.subprocess.call")
    # Pass flags after optional separator to avoid click parsing conflicts
    result = runner.invoke(
        cli_dagster,
        [
            "dev",
            "--",
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
    # Expect invocation of 'dg dev' and flags forwarded
    assert called_args[:2] == ["dg", "dev"]
    # Build a map for double-dash options that take values via next token
    args_map = {
        called_args[i]: called_args[i + 1]
        for i in range(2, len(called_args))
        if str(called_args[i]).startswith("--")
        and i + 1 < len(called_args)
        and not str(called_args[i + 1]).startswith("--")
    }
    assert args_map["--log-level"] == "debug"
    assert args_map["--log-format"] == "json"
    assert args_map["--host"] == "0.0.0.0"
    assert args_map["--port"] == "4000"
    assert args_map["--live-data-poll-rate"] == "1500"


def test_cli_plugin_shows_in_info(monkeypatch, tmp_path):
    """The 'kedro_dagster' plugin appears in 'kedro info' output."""
    # Sanity check that the plugin is discoverable by Kedro
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(info)
    assert result.exit_code == 0
    assert "kedro_dagster" in result.output
