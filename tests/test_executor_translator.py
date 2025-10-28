# mypy: ignore-errors

from __future__ import annotations

import dagster as dg
import pytest
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.config import get_dagster_config
from kedro_dagster.dagster import ExecutorCreator


@pytest.mark.parametrize("env", ["base", "local"])
def test_executor_translator_creates_multiple_executors(env, request):
    """Build multiple executor definitions from config and validate their names/types."""
    # Arrange: project with multiple executors and a default job
    options = request.getfixturevalue(f"kedro_project_multi_executors_{env}")
    project_path = options.project_path

    # Act: parse dagster config and build executors
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()
    dagster_config = get_dagster_config(context)
    executors = ExecutorCreator(dagster_config=dagster_config).create_executors()

    # Assert: both executors are registered
    assert "seq" in executors
    assert "multiproc" in executors

    assert len(executors) == 2  # noqa: PLR2004

    assert isinstance(executors["seq"], dg.ExecutorDefinition)
    assert executors["seq"].name == "in_process"
    assert isinstance(executors["multiproc"], dg.ExecutorDefinition)
    assert executors["multiproc"].name == "multiprocess"
