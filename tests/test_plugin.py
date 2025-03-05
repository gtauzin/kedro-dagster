# mypy: ignore-errors

from __future__ import annotations

from kedro_dagster.cli import commands


def test_dagster_init(cli_runner, metadata):
    """Check the generation and validity of a simple Airflow DAG."""
    command = "dagster init"
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
