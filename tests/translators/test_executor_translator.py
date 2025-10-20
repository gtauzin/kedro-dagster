# mypy: ignore-errors

from __future__ import annotations

import pytest
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.config import get_dagster_config
from kedro_dagster.dagster import ExecutorCreator

from ..helpers import dagster_executors_config, envs, make_jobs_config
from ..scenarios.project_factory import KedroProjectOptions


@pytest.mark.parametrize("env", envs())
def test_executor_translator_creates_multiple_executors(project_variant_factory, env):
    # Arrange: build a project variant with multiple executors and a default job
    dagster_cfg = {
        "executors": dagster_executors_config(),
        "jobs": make_jobs_config(pipeline_name="__default__", executor="multiproc"),
    }

    project_path = project_variant_factory(KedroProjectOptions(env=env, dagster=dagster_cfg))

    # Act: parse dagster config and build executors
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()
    dagster_config = get_dagster_config(context)
    executors = ExecutorCreator(dagster_config=dagster_config).create_executors()

    # Assert: both executors are registered
    assert "inproc" in executors
    assert "multiproc" in executors

    MIN_EXPECTED_EXECUTORS = 2
    assert len(executors) >= MIN_EXPECTED_EXECUTORS
