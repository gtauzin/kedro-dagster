import importlib
import re
import sys

import click
import pytest
from click.testing import CliRunner
from kedro.framework.cli.cli import info
from kedro.framework.startup import bootstrap_project

from kedro_dagster import utils
from kedro_dagster.cli import dagster_commands as cli_dagster
from kedro_dagster.cli import init as cli_init
from kedro_dagster.utils import DAGSTER_VERSION

if DAGSTER_VERSION >= (1, 10, 6):
    from kedro_dagster.cli import DgProxyCommand


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

    # dg.toml is written at project root only for newer Dagster versions
    dg_toml = project_path / "dg.toml"
    if DAGSTER_VERSION >= (1, 10, 6):
        assert dg_toml.is_file()
        assert ("'dg.toml' successfully updated." in result.output) or ("A 'dg.toml' already exists" in result.output)
    else:
        assert not dg_toml.exists()


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
    if DAGSTER_VERSION >= (1, 10, 6):
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
    # Structure begins with ["dg", "dev", ...] on newer versions or ["dagster", "dev", ...] on older ones
    if DAGSTER_VERSION >= (1, 10, 6):
        assert called_args[:2] == ["dg", "dev"]
    else:
        assert called_args[:2] == ["dagster", "dev"]


def test_cli_dev_overrides_forwarded(monkeypatch, mocker, kedro_project_no_dagster_config_base):
    """Explicit flags are forwarded to 'dg dev' unmodified."""
    project_path = kedro_project_no_dagster_config_base.project_path
    monkeypatch.chdir(project_path)
    bootstrap_project(project_path)

    runner = CliRunner()
    sp_call = mocker.patch("kedro_dagster.cli.subprocess.call")
    if DAGSTER_VERSION >= (1, 10, 6):
        # Pass flags after optional separator to avoid click parsing conflicts in proxy mode
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
    else:
        # Older CLI parses options directly without proxy semantics
        result = runner.invoke(
            cli_dagster,
            [
                "dev",
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


def test_cli_dev_help_includes_underlying_options(monkeypatch, kedro_project_no_dagster_config_base):
    """Help for 'kedro dagster dev' includes both wrapper and underlying dg options and uses dg description."""
    project_path = kedro_project_no_dagster_config_base.project_path
    monkeypatch.chdir(project_path)

    runner = CliRunner()
    result = runner.invoke(cli_dagster, ["dev", "--help"])

    assert result.exit_code == 0
    out = result.output
    # Wrapper option should be present
    assert "-e, --env" in out
    # A few known options should also be present inline
    assert "--log-level" in out
    assert "--log-format" in out
    assert "--host" in out
    assert "--port" in out
    if DAGSTER_VERSION >= (1, 10, 6):
        # Underlying dg description should be present (from 'dg dev --help')
        assert "Start a local instance of Dagster" in out
        # No separate section header from our implementation
        assert "Options from 'dg dev'" not in out


def test_cli_dev_no_duplication_when_user_flags_provided(monkeypatch, mocker, kedro_project_no_dagster_config_base):
    """If user supplies flags, wrapper must not append defaults or duplicate them."""
    project_path = kedro_project_no_dagster_config_base.project_path
    monkeypatch.chdir(project_path)

    runner = CliRunner()
    sp_call = mocker.patch("kedro_dagster.cli.subprocess.call")
    result = runner.invoke(
        cli_dagster,
        [
            "dev",
            "--log-level",
            "debug",
            "--log-format",
            "json",
        ],
    )
    assert result.exit_code == 0

    called_args = sp_call.call_args[0][0]
    # Ensure values come from user
    args_map = {
        called_args[i]: called_args[i + 1]
        for i in range(2, len(called_args))
        if str(called_args[i]).startswith("--")
        and i + 1 < len(called_args)
        and not str(called_args[i + 1]).startswith("--")
    }
    assert args_map.get("--log-level") == "debug"
    assert args_map.get("--log-format") == "json"


def test_cli_plugin_shows_in_info(monkeypatch, tmp_path):
    """The 'kedro_dagster' plugin appears in 'kedro info' output."""
    # Sanity check that the plugin is discoverable by Kedro
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(info)
    assert result.exit_code == 0
    assert "kedro_dagster" in result.output


def test_dg_proxy_help_handles_missing_underlying_command(monkeypatch):
    """Help rendering should not fail when the proxy has no underlying click.Command."""
    if DAGSTER_VERSION < (1, 10, 6):
        pytest.skip("DgProxyCommand is only defined for Dagster >= 1.10.6")

    # Build a minimal proxy command with an invalid underlying_cmd (None)
    proxy_cmd = DgProxyCommand(
        name="dummy",
        params=[
            click.Option(["--env", "-e"], required=False, default="local", help="The Kedro environment to use"),
            click.Argument(["args"], nargs=-1, type=click.UNPROCESSED),
        ],
        callback=lambda env, args: None,
        help="Dummy proxy",
        context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
        underlying_cmd=None,  # triggers the isinstance(self._underlying_cmd, click.Command) == False branch
    )

    # Mount the command under a temporary group and request help
    grp = click.Group()
    grp.add_command(proxy_cmd)

    runner = CliRunner()
    result = runner.invoke(grp, ["dummy", "--help"])

    # Should succeed and include our wrapper option; importantly, it should not crash
    assert result.exit_code == 0
    assert "-e, --env" in result.output


def test_dg_proxy_help_skips_non_parameter_entries(monkeypatch):
    """Ensure format_options silently skips objects in underlying_cmd.params that aren't click.Parameter."""
    if DAGSTER_VERSION < (1, 10, 6):
        pytest.skip("DgProxyCommand only defined for Dagster >= 1.10.6")

    class DummyUnderlying(click.Command):
        def __init__(self) -> None:
            super().__init__(name="dummy_underlying")
            # Inject mixed params: one real option + one bogus object
            self.params = [
                click.Option(["--alpha"], help="Alpha option"),  # valid
                object(),  # invalid (will be skipped by isinstance check)
            ]

    underlying = DummyUnderlying()

    proxy_cmd = DgProxyCommand(
        name="dummy2",
        params=[
            click.Option(["--env", "-e"], required=False, default="local", help="The Kedro environment to use"),
            click.Argument(["args"], nargs=-1, type=click.UNPROCESSED),
        ],
        callback=lambda env, args: None,
        help="Dummy proxy 2",
        context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
        underlying_cmd=underlying,
    )

    grp = click.Group()
    grp.add_command(proxy_cmd)
    runner = CliRunner()
    result = runner.invoke(grp, ["dummy2", "--help"])

    assert result.exit_code == 0
    # Wrapper option present
    assert "-e, --env" in result.output
    # Real underlying option appears
    assert "--alpha" in result.output
    # Nothing from the bogus object (can't really assert absence of random strings, but ensure no traceback)
    assert "Traceback" not in result.output


def test_cli_version_gate_old_branch_with_reload(monkeypatch):
    """Monkeypatch DAGSTER_VERSION to older than 1.10.6 and reload cli to cover the else branch.

    This ensures the top-level version gate is exercised for the False outcome and that
    the legacy 'dev' command (direct dagster CLI) is available with its options.
    """
    original_version = utils.DAGSTER_VERSION
    try:
        # Force older version and reload CLI module
        monkeypatch.setattr(utils, "DAGSTER_VERSION", (1, 10, 5), raising=False)
        sys.modules.pop("kedro_dagster.cli", None)
        cli_mod = importlib.import_module("kedro_dagster.cli")
        importlib.reload(cli_mod)

        runner = CliRunner()
        result = runner.invoke(cli_mod.dagster_commands, ["dev", "--help"])  # legacy dev is defined in else-branch
        assert result.exit_code == 0
        out = result.output
        # Wrapper option
        assert "-e, --env" in out
        # Legacy options should be present
        assert "--log-level" in out
        assert "--log-format" in out
        assert "--host" in out
        assert "--port" in out
        assert "--live-data-poll-rate" in out
    finally:
        # Restore original version and reload CLI to avoid side effects on other tests
        try:
            setattr(utils, "DAGSTER_VERSION", original_version)
        except Exception:
            pass
        sys.modules.pop("kedro_dagster.cli", None)
        cli_mod = importlib.import_module("kedro_dagster.cli")
        importlib.reload(cli_mod)


def _assert_args_map(called_args: list[str]) -> dict[str, str]:
    return {
        called_args[i]: called_args[i + 1]
        for i in range(2, len(called_args))
        if str(called_args[i]).startswith("--")
        and i + 1 < len(called_args)
        and not str(called_args[i + 1]).startswith("--")
    }


def test_cli_dev_old_branch_invokes_dagster(monkeypatch, mocker, kedro_project_no_dagster_config_base):
    """Under old CLI branch, 'kedro dagster dev' should call 'dagster dev' with provided flags and env."""
    project_path = kedro_project_no_dagster_config_base.project_path
    monkeypatch.chdir(project_path)

    original_version = utils.DAGSTER_VERSION
    try:
        monkeypatch.setattr(utils, "DAGSTER_VERSION", (1, 10, 5), raising=False)
        importlib.import_module("kedro_dagster.cli")  # ensure present to pop
        sys.modules.pop("kedro_dagster.cli", None)
        cli_mod = importlib.import_module("kedro_dagster.cli")
        importlib.reload(cli_mod)

        sp_call = mocker.patch("kedro_dagster.cli.subprocess.call")
        runner = CliRunner()
        result = runner.invoke(
            cli_mod.dagster_commands,
            [
                "dev",
                "-e",
                kedro_project_no_dagster_config_base.env,
                "--log-level",
                "info",
                "--log-format",
                "json",
                "--host",
                "0.0.0.0",
                "--port",
                "1234",
                "--live-data-poll-rate",
                "10",
            ],
        )
        assert result.exit_code == 0

        # Verify subprocess call
        called_args = sp_call.call_args[0][0]
        assert called_args[:2] == ["dagster", "dev"]
        # Ensure python-file path points to the definitions file in project src
        assert "--python-file" in called_args
        py_idx = called_args.index("--python-file") + 1
        defs_path = called_args[py_idx]
        assert str(defs_path).startswith(str(project_path / "src"))
        assert str(defs_path).endswith("definitions.py")

        args_map = _assert_args_map(called_args)
        assert args_map["--log-level"] == "info"
        assert args_map["--log-format"] == "json"
        assert args_map["--host"] == "0.0.0.0"
        assert args_map["--port"] == "1234"
        assert args_map["--live-data-poll-rate"] == "10"

        # Verify env and cwd passed to subprocess
        kwargs = sp_call.call_args[1]
        assert kwargs["cwd"] == str(project_path)
        assert kwargs["env"]["KEDRO_ENV"] == kedro_project_no_dagster_config_base.env
    finally:
        try:
            setattr(utils, "DAGSTER_VERSION", original_version)
        except Exception:
            pass
        sys.modules.pop("kedro_dagster.cli", None)
        cli_mod = importlib.import_module("kedro_dagster.cli")
        importlib.reload(cli_mod)


@pytest.mark.parametrize("inside_subdirectory", (True, False))
def test_cli_dev_old_branch_from_subdir(monkeypatch, mocker, kedro_project_no_dagster_config_base, inside_subdirectory):
    """Old branch must work regardless of being run from project root or src/ subdir."""
    project_path = kedro_project_no_dagster_config_base.project_path
    cwd = project_path / "src" if inside_subdirectory else project_path
    monkeypatch.chdir(cwd)

    original_version = utils.DAGSTER_VERSION
    try:
        monkeypatch.setattr(utils, "DAGSTER_VERSION", (1, 10, 5), raising=False)
        sys.modules.pop("kedro_dagster.cli", None)
        cli_mod = importlib.import_module("kedro_dagster.cli")
        importlib.reload(cli_mod)

        sp_call = mocker.patch("kedro_dagster.cli.subprocess.call")
        runner = CliRunner()
        result = runner.invoke(
            cli_mod.dagster_commands,
            [
                "dev",
                "-e",
                kedro_project_no_dagster_config_base.env,
                "--log-level",
                "debug",
                "--log-format",
                "color",
                "--host",
                "127.0.0.1",
                "--port",
                "5678",
                "--live-data-poll-rate",
                "5",
            ],
        )
        assert result.exit_code == 0

        called_args = sp_call.call_args[0][0]
        assert called_args[:2] == ["dagster", "dev"]
        kwargs = sp_call.call_args[1]
        assert kwargs["cwd"] == str(project_path)
        assert kwargs["env"]["KEDRO_ENV"] == kedro_project_no_dagster_config_base.env
    finally:
        try:
            setattr(utils, "DAGSTER_VERSION", original_version)
        except Exception:
            pass
        sys.modules.pop("kedro_dagster.cli", None)
        cli_mod = importlib.import_module("kedro_dagster.cli")
        importlib.reload(cli_mod)
