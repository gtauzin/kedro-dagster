from __future__ import annotations

import pytest
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.config import get_dagster_config
from kedro_dagster.config.dev import DevOptions
from kedro_dagster.config.execution import (
    InProcessExecutorOptions,
    MultiprocessExecutorOptions,
)
from kedro_dagster.config.job import JobOptions
from tests.helpers import (
    dagster_executors_config,
    dagster_schedules_config,
    make_jobs_config,
)
from tests.scenarios.kedro_projects import options_integration_full


@pytest.mark.parametrize("env", ["base", "local"])
def test_get_dagster_config_loads_and_parses(project_variant_factory, env):
    # Prepare a project variant with executors, schedules, jobs defined in conf/<env>/dagster.yml
    project_path = project_variant_factory(options_integration_full(env))

    # Bootstrap and load Kedro context for the selected env
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    # When: we fetch the Dagster config through the configured loader
    dagster_config = get_dagster_config(context)

    # Then: dev options are present with defaults
    assert isinstance(dagster_config.dev, DevOptions)
    assert dagster_config.dev.log_level == "info"

    # And: executor map is parsed to proper option classes
    expected_exec = dagster_executors_config()
    assert set(dagster_config.executors.keys()) == set(expected_exec.keys())
    assert isinstance(dagster_config.executors["inproc"], InProcessExecutorOptions)
    assert isinstance(dagster_config.executors["multiproc"], MultiprocessExecutorOptions)
    EXPECTED_MAX_CONCURRENT = 2
    assert dagster_config.executors["multiproc"].max_concurrent == EXPECTED_MAX_CONCURRENT  # type: ignore[union-attr]

    # And: schedules are represented as ScheduleOptions and match conf
    expected_sched = dagster_schedules_config()
    assert set(dagster_config.schedules.keys()) == set(expected_sched.keys())
    assert dagster_config.schedules["daily"].cron_schedule == expected_sched["daily"]["cron_schedule"]

    # And: jobs are parsed with pipeline and string references for executor/schedule
    job_cfg = make_jobs_config(pipeline_name="__default__", executor="inproc", schedule="daily")
    assert set(dagster_config.jobs.keys()) == set(job_cfg.keys())
    job: JobOptions = dagster_config.jobs["default"]
    assert job.pipeline.pipeline_name == "__default__"
    assert job.executor == "inproc"
    assert job.schedule == "daily"
