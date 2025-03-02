"""Behave step definitions for the cli_scenarios feature."""

import re
import textwrap
from pathlib import Path

import behave
import yaml
from behave import given, then, when

from features.steps.sh_run import ChildTerminatingPopen, run

OK_EXIT_CODE = 0


@given("I have prepared a config file")
def create_configuration_file(context: behave.runner.Context):
    """Behave step to create a temporary config file
    (given the existing temp directory)
    and store it in the context.
    """
    context.config_file = context.temp_dir / "config"
    context.project_name = "project-dummy"
    context.package_name = context.project_name.replace("-", "_")

    root_project_dir = context.temp_dir / context.project_name
    context.root_project_dir = root_project_dir
    config = {
        "project_name": context.project_name,
        "repo_name": context.project_name,
        "output_dir": str(context.temp_dir),
        "python_package": context.package_name,
    }
    with context.config_file.open("w") as config_file:
        yaml.dump(config, config_file, default_flow_style=False)


@given("I run a non-interactive kedro new using {starter_name} starter")
def create_project_from_config_file(context: behave.runner.Context, starter_name: str):
    """Behave step to run kedro new
    given the config I previously created.
    """
    res = run(
        [
            context.kedro,
            "new",
            "-c",
            str(context.config_file),
            "--starter",
            starter_name,
        ],
        env=context.env,
        cwd=str(context.temp_dir),
    )

    # add a consent file to prevent telemetry from prompting for input during e2e test
    telemetry_file = context.root_project_dir / ".telemetry"
    telemetry_file.parent.mkdir(parents=True, exist_ok=True)
    telemetry_file.write_text("consent: false", encoding="utf-8")

    # override base logging configuration to simplify assertions
    logging_conf = context.root_project_dir / "conf" / "base" / "logging.yml"
    logging_conf.parent.mkdir(parents=True, exist_ok=True)
    logging_conf.write_text(
        textwrap.dedent(
            """
        version: 1

        disable_existing_loggers: False

        formatters:
          simple:
            format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        handlers:
          console:
            class: logging.StreamHandler
            level: INFO
            formatter: simple
            stream: ext://sys.stdout

        loggers:
          kedro:
            level: INFO

        root:
          handlers: [console]
        """
        )
    )

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@given('I have executed the kedro command "{command}"')
def exec_kedro_command(context: behave.runner.Context, command: str):
    """Execute Kedro command and check the status."""
    make_cmd = [context.kedro] + command.split()

    res = run(make_cmd, env=context.env, cwd=str(context.root_project_dir))

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@given("I have installed the project dependencies")
def pip_install_dependencies(context: behave.runner.Context):
    """Install project dependencies using pip."""
    reqs_path = Path("requirements.txt")
    res = run(
        [context.pip, "install", "-r", str(reqs_path)],
        env=context.env,
        cwd=str(context.root_project_dir),
    )

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@when('I execute the kedro command "{command}"')
def exec_kedro_target(context: behave.runner.Context, command: str):
    """Execute Kedro target"""
    make_cmd = [context.kedro] + command.split()

    context.result = run(make_cmd, env=context.env, cwd=str(context.root_project_dir))

    if context.result.returncode != OK_EXIT_CODE:
        print(context.result.stdout)
        print(context.result.stderr)
        assert False


@when('I occupy port "{port}"')
def occupy_port(context: behave.runner.Context, port):
    """Execute  target"""
    ChildTerminatingPopen(
        ["nc", "-l", "0.0.0.0", port],
        env=context.env,
        cwd=str(context.root_project_dir),
    )


@then("I should get a successful exit code")
def check_status_code(context: behave.runner.Context):
    if context.result.returncode != OK_EXIT_CODE:
        print(context.result.stdout)
        print(context.result.stderr)
        assert False, f"Expected exit code /= {OK_EXIT_CODE} but got {context.result.returncode}"


@then("I should get an error exit code")
def check_failed_status_code(context: behave.runner.Context):
    if context.result.returncode == OK_EXIT_CODE:
        print(context.result.stdout)
        print(context.result.stderr)
        assert False, f"Expected exit code {OK_EXIT_CODE} but got {context.result.returncode}"


@then("A {filename} file should exist")
def check_if_file_exists(context: behave.runner.Context, filename: str):
    """Checks if file is present and has content.

    Args:
        context: Behave context.
        filepath: A path to a file to check for existence.
    """
    if filename == "definitions.py":
        filepath = "src/" + context.package_name + "/definitions.py"

    if filename == "dagster.yml":
        filepath = "conf/base/dagster.yml"

    filepath: Path = context.root_project_dir / filepath
    assert filepath.exists(), f"Expected {filepath} to exists but .exists() returns {filepath.exists()}"
    assert filepath.stat().st_size > 0, f"Expected {filepath} to have size > 0 but has {filepath.stat().st_size}"


@then("A {filepath} file should contain {text} string")
def grep_file(context: behave.runner.Context, filepath: str, text: str):
    """Checks if given file contains passed string.

    Args:
        context: Behave context.
        filepath: A path to a file to grep.
        text: Text (or regex) to search for.
    """
    filepath: Path = context.root_project_dir / filepath
    with filepath.open("r") as file:
        found = any(line and re.search(text, line) for line in file)
    assert found, f"String {text} not found in {filepath}"
