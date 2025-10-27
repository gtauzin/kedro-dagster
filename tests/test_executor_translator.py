# mypy: ignore-errors

from __future__ import annotations

import pytest
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.config import get_dagster_config
from kedro_dagster.dagster import ExecutorCreator


@pytest.mark.parametrize("env", ["base", "local"])  # use existing per-env fixtures
def test_executor_translator_creates_multiple_executors(env, request):
    # Arrange: project with multiple executors and a default job
    project_path, _ = request.getfixturevalue(f"kedro_project_multi_executors_{env}")

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
