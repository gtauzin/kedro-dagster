"""Behave step definitions for the cli_scenarios feature."""

import re
import textwrap
import time
from pathlib import Path

import behave
import yaml
from behave import given, then, when

from features.steps.sh_run import ChildTerminatingPopen, run

OK_EXIT_CODE = 0


@given("I have prepared a config file")
def create_configuration_file(context: behave.runner.Context) -> None:
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
def create_project_from_config_file(context: behave.runner.Context, starter_name: str) -> None:
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
def exec_kedro_command(context: behave.runner.Context, command: str) -> None:
    """Execute Kedro command and check the status."""
    make_cmd = [context.kedro] + command.split()

    res = run(make_cmd, env=context.env, cwd=str(context.root_project_dir))

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@given("I have installed the project dependencies")
def pip_install_dependencies(context: behave.runner.Context) -> None:
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
def exec_kedro_target(context: behave.runner.Context, command: str) -> None:
    """Execute Kedro target. For 'dagster dev', run as background process."""
    make_cmd = [context.kedro] + command.split()
    if command.startswith("dagster dev"):
        # Start dagster dev as a background process
        context.process = ChildTerminatingPopen(make_cmd, env=context.env, cwd=str(context.root_project_dir))
        # Give the server time to start
        time.sleep(10)
        # No exit code to check here; port check will follow
        context.result = None
    else:
        context.result = run(make_cmd, env=context.env, cwd=str(context.root_project_dir))
        if context.result.returncode != OK_EXIT_CODE:
            print(context.result.stdout)
            print(context.result.stderr)
            assert False


@then("I should get a successful exit code")
def check_status_code(context: behave.runner.Context) -> None:
    # Only check exit code if not running a background process
    if getattr(context, "result", None) is not None:
        if context.result.returncode != OK_EXIT_CODE:
            print(context.result.stdout)
            print(context.result.stderr)
            assert False, f"Expected exit code /= {OK_EXIT_CODE} but got {context.result.returncode}"


@then("I should get an error exit code")
def check_failed_status_code(context: behave.runner.Context) -> None:
    if context.result.returncode == OK_EXIT_CODE:
        print(context.result.stdout)
        print(context.result.stderr)
        assert False, f"Expected exit code {OK_EXIT_CODE} but got {context.result.returncode}"


@then("A {filename} file should exist")
def check_if_file_exists(context: behave.runner.Context, filename: str) -> None:
    """Checks if file is present and has content.

    Args:
        context: Behave context.
        filepath: A path to a file to check for existence.
    """
    if filename == "definitions.py":
        filepath = "src/" + context.package_name + "/definitions.py"

    elif filename == "dagster.yml":
        filepath = Path("conf/base/dagster.yml")

    else:
        raise ValueError("`filename` should be either `definitions.py` or `dagster.yml`.")

    absolute_filepath: Path = context.root_project_dir / filepath
    assert absolute_filepath.exists(), (
        f"Expected {absolute_filepath} to exists but .exists() returns {absolute_filepath.exists()}"
    )
    assert absolute_filepath.stat().st_size > 0, (
        f"Expected {absolute_filepath} to have size > 0 but has {absolute_filepath.stat().st_size}"
    )


@then("A {filepath} file should contain {text} string")
def grep_file(context: behave.runner.Context, filepath: str, text: str) -> None:
    """Checks if given file contains passed string.

    Args:
        context: Behave context.
        filepath: A path to a file to grep.
        text: Text (or regex) to search for.
    """
    absolute_filepath: Path = context.root_project_dir / filepath
    with absolute_filepath.open("r") as file:
        found = any(line and re.search(text, line) for line in file)
    assert found, f"String {text} not found in {absolute_filepath}"


@then('the dagster UI should be served on "{host}:{port}"')
def check_dagster_ui_url(context: behave.runner.Context, host: str, port: str) -> None:
    """Check the dagster dev process output for the correct UI URL and absence of the default port."""
    process = getattr(context, "process", None)
    assert process is not None, "Dagster dev process was not started."
    deadline = time.time() + 10
    url_line = None
    while time.time() < deadline:
        line = process.stdout.readline() if hasattr(process, "stdout") and process.stdout else None
        if line and isinstance(line, bytes):
            line = line.decode(errors="replace")
        if not line:
            time.sleep(0.1)
            continue
        if "Serving dagster-webserver on" in line:
            url_line = line.strip()
            break
    assert url_line is not None, "Did not receive Dagster UI startup message within 10 s"
    assert f"{host}:{port}" in url_line, f"Expected UI on {host}:{port}, but got: {url_line}"


def after_scenario(context: behave.runner.Context, scenario: behave.model.Scenario) -> None:
    # Terminate dagster dev process if it was started
    if hasattr(context, "process"):
        context.process.terminate()
        context.process.wait()
        del context.process
