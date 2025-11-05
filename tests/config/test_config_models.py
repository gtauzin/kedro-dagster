# mypy: ignore-errors

import pytest
from pydantic import ValidationError

from kedro_dagster.config.automation import ScheduleOptions
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
from kedro_dagster.utils import KEDRO_VERSION


def test_schedule_options_happy_path():
    """ScheduleOptions accepts minimal fields and optional metadata/timezone."""
    s = ScheduleOptions(cron_schedule="*/5 * * * *", description="every 5m")
    assert s.cron_schedule.startswith("*/5")
    assert s.execution_timezone is None
    assert s.metadata is None


def test_pipeline_options_forbid_extra_and_defaults():
    """PipelineOptions defaults to None for filters and forbids unknown fields."""
    p = PipelineOptions()
    assert p.pipeline_name is None
    assert p.from_nodes is None
    assert p.to_nodes is None
    assert p.node_names is None
    assert p.from_inputs is None
    assert p.to_outputs is None
    assert p.tags is None
    if KEDRO_VERSION[0] >= 1:
        assert p.node_namespaces is None
    else:
        assert p.node_namespace is None

    with pytest.raises(ValidationError):
        PipelineOptions(unknown="x")


def test_pipeline_options_node_namespaces_list_shape():
    """For Kedro >= 1.0, node_namespaces accepts and exposes list[str]; alias property matches."""
    if KEDRO_VERSION[0] < 1:
        pytest.skip("Only relevant for Kedro >= 1.0")

    p = PipelineOptions(node_namespaces=["ns1", "ns2"])
    assert p.node_namespaces == ["ns1", "ns2"]
    assert not hasattr(p, "node_namespace")


def test_job_options_requires_pipeline_and_forbid_extra():
    """JobOptions require a PipelineOptions instance and reject extra fields."""
    with pytest.raises(ValidationError):
        JobOptions()

    job = JobOptions(pipeline=PipelineOptions())
    assert isinstance(job.pipeline, PipelineOptions)

    with pytest.raises(ValidationError):
        JobOptions(pipeline=PipelineOptions(), extra_field=1)


def test_inprocess_and_multiprocess_executor_defaults():
    """Executor option defaults include retries for in_process and max_concurrent for multiprocess."""
    inproc = InProcessExecutorOptions()
    # default retries enabled structure
    assert hasattr(inproc.retries, "enabled") or hasattr(inproc.retries, "disabled")

    multi = MultiprocessExecutorOptions()
    assert multi.max_concurrent == 1


def test_docker_executor_defaults_and_mutability():
    """DockerExecutorOptions default to empty lists and optional container kwargs."""
    d = DockerExecutorOptions()
    assert d.env_vars == []
    assert d.networks == []
    assert d.container_kwargs is None


def test_k8s_executor_defaults_subset():
    """K8sJobExecutorOptions default namespace, labels, volumes, and metadata shape."""
    k = K8sJobExecutorOptions()
    assert k.job_namespace == "dagster"
    assert isinstance(k.step_k8s_config.job_metadata, dict)
    assert k.labels == {}
    assert k.volumes == []


def test_celery_and_combined_executors_construct():
    """Celery-based executor option classes can be instantiated without arguments."""
    CeleryExecutorOptions()
    CeleryDockerExecutorOptions()
    CeleryK8sJobExecutorOptions()


def test_kedrodagster_config_parses_executors_map_happy_path():
    """KedroDagsterConfig parses executors mapping into strongly-typed option classes."""
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
    assert cfg.executors["multi"].max_concurrent == MAX_CONCURRENCY
    assert isinstance(cfg.executors["dock"], DockerExecutorOptions)
    assert isinstance(cfg.executors["k8s"], K8sJobExecutorOptions)


def test_kedrodagster_config_unknown_executor_raises():
    """Unknown executor identifiers result in a ValueError during parsing."""
    with pytest.raises(ValueError):
        KedroDagsterConfig(executors={"weird": {"unknown": {}}})
