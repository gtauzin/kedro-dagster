"""Configuration definitions for Kedro-Dagster."""

from logging import getLogger

from pydantic import BaseModel, Field

LOGGER = getLogger(__name__)


class InProcessExecutorOptions(BaseModel):
    """Execute all steps in a single process."""

    class RetriesEnableOptions(BaseModel):
        enabled: dict = {}

    class RetriesDisableOptions(BaseModel):
        disabled: dict = {}

    retries: RetriesEnableOptions | RetriesDisableOptions = Field(
        default=RetriesEnableOptions(),
        description="Whether retries are enabled or not.",
    )


class MultiprocessExecutorOptions(InProcessExecutorOptions):
    """Execute each step in an individual process."""

    max_concurrent: int | None = Field(
        default=None,
        description=(
            "The number of processes that may run concurrently. "
            "By default, this is set to be the return value of `multiprocessing.cpu_count()`."
        ),
        is_required=False,
    )


# TODO: Map all dagster executors
class K8sJobExecutorOptions(MultiprocessExecutorOptions):
    """Execute each step in a separate k8s job."""

    # TODO: Allow https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-k8s/dagster_k8s/job.py#L276
    class K8sJobConfig(BaseModel):
        container_config: dict | None = None
        pod_spec_config: dict | None = None
        pod_templat_spec_metadata: dict | None = None
        job_spec_config: dict | None = None
        job_metadata: dict | None = None

    job_namespace: str | None = Field(default=None, is_required=False)
    load_incluster_config: bool | None = Field(
        default=None,
        is_required=False,
        description="""Whether or not the executor is running within a k8s cluster already. If
        the job is using the `K8sRunLauncher`, the default value of this parameter will be
        the same as the corresponding value on the run launcher.
        If ``True``, we assume the executor is running within the target cluster and load config
        using ``kubernetes.config.load_incluster_config``. Otherwise, we will use the k8s config
        specified in ``kubeconfig_file`` (using ``kubernetes.config.load_kube_config``) or fall
        back to the default kubeconfig.""",
    )
    kubeconfig_file: str | None = Field(
        default=None,
        description="""Path to a kubeconfig file to use, if not using default kubeconfig. If
        the job is using the `K8sRunLauncher`, the default value of this parameter will be
        the same as the corresponding value on the run launcher.""",
        is_required=False,
    )
    step_k8s_config: K8sJobConfig = Field(
        default=K8sJobConfig(),
        description="Raw Kubernetes configuration for each step launched by the executor.",
        is_required=False,
    )
    per_step_k8s_config: dict[str, K8sJobConfig] = Field(
        default={},
        description="Per op k8s configuration overrides.",
        is_required=False,
    )


ExecutorOptions = InProcessExecutorOptions | MultiprocessExecutorOptions | K8sJobExecutorOptions


EXECUTOR_MAP = {
    "in_process": InProcessExecutorOptions,
    "multiprocess": MultiprocessExecutorOptions,
    "k8s_job_executor": K8sJobExecutorOptions,
}
