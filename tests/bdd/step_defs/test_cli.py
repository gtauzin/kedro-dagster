from __future__ import annotations

import re
import textwrap
import time
from pathlib import Path

import pytest
import yaml
from pytest_bdd import given, parsers, scenarios, then, when

from ..conftest import Ctx  # for type hints
from ..utils.sh_run import ChildTerminatingPopen, run

OK_EXIT_CODE = 0

scenarios("../features/cli.feature")


@given("I have prepared a config file")
def prepared_config_file(ctx: Ctx) -> None:
    ctx.config_file = ctx.temp_dir / "config"
    ctx.project_name = "project-dummy"
    ctx.package_name = ctx.project_name.replace("-", "_")
    ctx.root_project_dir = ctx.temp_dir / ctx.project_name
    config = {
        "project_name": ctx.project_name,
        "repo_name": ctx.project_name,
        "output_dir": str(ctx.temp_dir),
        "python_package": ctx.package_name,
    }
    with ctx.config_file.open("w") as f:
        yaml.dump(config, f, default_flow_style=False)


@given(parsers.parse("I run a non-interactive kedro new using {starter_name} starter"))
def create_project_from_config_file(ctx: Ctx, starter_name: str) -> None:
    res = run(
        [
            ctx.kedro,
            "new",
            "-c",
            str(ctx.config_file),
            "--starter",
            starter_name,
        ],
        env=ctx.env,
        cwd=str(ctx.temp_dir),
    )

    telemetry_file = ctx.root_project_dir / ".telemetry"
    telemetry_file.parent.mkdir(parents=True, exist_ok=True)
    telemetry_file.write_text("consent: false", encoding="utf-8")

    logging_conf = ctx.root_project_dir / "conf" / "base" / "logging.yml"
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
        pytest.fail(f"kedro new failed:\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}")


@given("I have installed the project dependencies")
def pip_install_dependencies(ctx: Ctx) -> None:
    reqs_path = Path("requirements.txt")
    res = run(
        [ctx.pip, "install", "-r", str(reqs_path)],
        env=ctx.env,
        cwd=str(ctx.root_project_dir),
    )
    if res.returncode != OK_EXIT_CODE:
        pytest.fail(f"pip install -r requirements.txt failed:\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}")


@given(parsers.parse('I have executed the kedro command "{command}"'))
def exec_kedro_command(ctx: Ctx, command: str) -> None:
    make_cmd = [ctx.kedro] + command.split()
    res = run(make_cmd, env=ctx.env, cwd=str(ctx.root_project_dir))
    if res.returncode != OK_EXIT_CODE:
        pytest.fail(f"kedro command failed:\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}")


@when(parsers.parse('I execute the kedro command "{command}"'))
def exec_kedro_target(ctx: Ctx, command: str) -> None:
    make_cmd = [ctx.kedro] + command.split()
    if command.startswith("dagster dev"):
        ctx.process = ChildTerminatingPopen(make_cmd, env=ctx.env, cwd=str(ctx.root_project_dir))
        time.sleep(10)  # give server time to start
        ctx.result = None
    else:
        ctx.result = run(make_cmd, env=ctx.env, cwd=str(ctx.root_project_dir))
        if ctx.result.returncode != OK_EXIT_CODE:
            pytest.fail(f"kedro command failed:\nSTDOUT:\n{ctx.result.stdout}\nSTDERR:\n{ctx.result.stderr}")


@then("I should get a successful exit code")
def check_status_code(ctx: Ctx) -> None:
    if getattr(ctx, "result", None) is not None:
        assert ctx.result is not None
        if ctx.result.returncode != OK_EXIT_CODE:
            pytest.fail(
                f"Expected exit code /= {OK_EXIT_CODE} but got {ctx.result.returncode}:\n"
                f"STDOUT:\n{ctx.result.stdout}\nSTDERR:\n{ctx.result.stderr}"
            )


@then("I should get an error exit code")
def check_failed_status_code(ctx: Ctx) -> None:
    assert ctx.result is not None
    if ctx.result.returncode == OK_EXIT_CODE:
        pytest.fail(
            f"Expected non-zero exit code but got {ctx.result.returncode}:\n"
            f"STDOUT:\n{ctx.result.stdout}\nSTDERR:\n{ctx.result.stderr}"
        )


@then(parsers.parse("A {filename} file should exist"))
def check_if_file_exists(ctx: Ctx, filename: str) -> None:
    if filename == "definitions.py":
        filepath = Path("src") / ctx.package_name / "definitions.py"
    elif filename == "dagster.yml":
        filepath = Path("conf/base/dagster.yml")
    else:
        raise ValueError("`filename` should be either `definitions.py` or `dagster.yml`.")
    absolute_filepath: Path = ctx.root_project_dir / filepath
    assert absolute_filepath.exists(), f"Expected {absolute_filepath} to exist"
    assert absolute_filepath.stat().st_size > 0, f"Expected {absolute_filepath} to have size > 0"


@then(parsers.parse("A {filepath} file should contain {text} string"))
def grep_file(ctx: Ctx, filepath: str, text: str) -> None:
    absolute_filepath: Path = ctx.root_project_dir / filepath
    with absolute_filepath.open("r") as file:
        found = any(line and re.search(text, line) for line in file)
    assert found, f"String {text} not found in {absolute_filepath}"


@then(parsers.parse('the dagster UI should be served on "{host}:{port}"'))
def check_dagster_ui_url(ctx: Ctx, host: str, port: str) -> None:
    process = getattr(ctx, "process", None)
    assert process is not None, "Dagster dev process was not started."
    deadline = time.time() + 10
    url_line = None
    while time.time() < deadline:
        line = process.stdout.readline() if hasattr(process, "stdout") and process.stdout else None  # type: ignore[attr-defined]
        if line and isinstance(line, bytes):
            line = line.decode(errors="replace")
        if not line:
            time.sleep(0.1)
            continue
        if "Serving dagster-webserver on" in line:
            url_line = str(line).strip()
            break
    assert url_line is not None, "Did not receive Dagster UI startup message within 10 s"
    assert f"{host}:{port}" in url_line, f"Expected UI on {host}:{port}, but got: {url_line}"
