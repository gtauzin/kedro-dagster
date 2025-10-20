from pathlib import Path

import pytest
from pydantic import ValidationError

from kedro_dagster.config.automation import ScheduleOptions
from kedro_dagster.config.dev import DevOptions
from kedro_dagster.config.execution import (
    CeleryDockerExecutorOptions,
    CeleryExecutorOptions,
    CeleryK8sJobExecutorOptions,
    DockerExecutorOptions,
    InProcessExecutorOptions,
    K8sJobExecutorOptions,
    MultiprocessExecutorOptions,
)
from kedro_dagster.config.job import JobOptions, PipelineOptions
from kedro_dagster.config.kedro_dagster import KedroDagsterConfig


def test_dev_options_defaults_and_forbid_extra():
    dev = DevOptions()
    assert dev.log_level == "info"
    assert dev.log_format == "colored"
    assert dev.port == "3000"
    assert dev.host == "127.0.0.1"
    assert dev.live_data_poll_rate == "2000"

    with pytest.raises(ValidationError):
        DevOptions(extra_field=True)  # type: ignore[arg-type]


def test_dev_options_python_file_property(monkeypatch, tmp_path: Path):
    # Stub a kedro project layout and metadata so python_file resolves deterministically
    project_root = tmp_path / "myproj"
    (project_root / "src" / "my_pkg").mkdir(parents=True)
    (project_root / "src" / "my_pkg" / "definitions.py").write_text("# defs")

    class DummyMeta:
        package_name = "my_pkg"

    def fake_find_project(_cwd: Path):
        return project_root

    def fake_bootstrap(_project_path: Path):
        return DummyMeta

    # Patch kedro helpers used by DevOptions.python_file
    monkeypatch.setattr("kedro_dagster.config.dev._find_kedro_project", fake_find_project)
    monkeypatch.setattr("kedro_dagster.config.dev.bootstrap_project", fake_bootstrap)

    dev = DevOptions()
    assert dev.python_file == project_root / "src" / "my_pkg" / "definitions.py"


def test_schedule_options_happy_path():
    s = ScheduleOptions(cron_schedule="*/5 * * * *", description="every 5m")
    assert s.cron_schedule.startswith("*/5")
    assert s.execution_timezone is None
    assert s.metadata is None


def test_pipeline_options_forbid_extra_and_defaults():
    p = PipelineOptions()
    assert p.pipeline_name is None
    assert p.from_nodes is None
    assert p.to_nodes is None
    assert p.node_names is None
    assert p.from_inputs is None
    assert p.to_outputs is None
    assert p.node_namespace is None
    assert p.tags is None

    with pytest.raises(ValidationError):
        PipelineOptions(unknown="x")  # type: ignore[arg-type]


def test_job_options_requires_pipeline_and_forbid_extra():
    with pytest.raises(ValidationError):
        JobOptions()  # type: ignore[call-arg]

    job = JobOptions(pipeline=PipelineOptions())
    assert isinstance(job.pipeline, PipelineOptions)

    with pytest.raises(ValidationError):
        JobOptions(pipeline=PipelineOptions(), extra_field=1)  # type: ignore[arg-type]


def test_inprocess_and_multiprocess_executor_defaults():
    inproc = InProcessExecutorOptions()
    # default retries enabled structure
    assert hasattr(inproc.retries, "enabled") or hasattr(inproc.retries, "disabled")

    multi = MultiprocessExecutorOptions()
    assert multi.max_concurrent == 1


def test_docker_executor_defaults_and_mutability():
    d = DockerExecutorOptions()
    assert d.env_vars == []
    assert d.networks == []
    assert d.container_kwargs is None


def test_k8s_executor_defaults_subset():
    k = K8sJobExecutorOptions()
    assert k.job_namespace == "dagster"
    assert isinstance(k.step_k8s_config.job_metadata, dict)
    assert k.labels == {}
    assert k.volumes == []


def test_celery_and_combined_executors_construct():
    CeleryExecutorOptions()
    CeleryDockerExecutorOptions()
    CeleryK8sJobExecutorOptions()


def test_kedrodagster_config_parses_executors_map_happy_path():
    cfg = KedroDagsterConfig(
        executors={
            "local": {"in_process": {}},
            "multi": {"multiprocess": {"max_concurrent": 3}},
            "dock": {"docker_executor": {"image": "alpine"}},
            "k8s": {"k8s_job_executor": {"job_namespace": "prod"}},
        }
    )

    assert isinstance(cfg.executors, dict)
    assert isinstance(cfg.executors["local"], InProcessExecutorOptions)
    assert isinstance(cfg.executors["multi"], MultiprocessExecutorOptions)
    MAX_CONCURRENCY = 3
    assert cfg.executors["multi"].max_concurrent == MAX_CONCURRENCY  # type: ignore[union-attr]
    assert isinstance(cfg.executors["dock"], DockerExecutorOptions)
    assert isinstance(cfg.executors["k8s"], K8sJobExecutorOptions)


def test_kedrodagster_config_unknown_executor_raises():
    with pytest.raises(ValueError):
        KedroDagsterConfig(executors={"weird": {"unknown": {}}})
