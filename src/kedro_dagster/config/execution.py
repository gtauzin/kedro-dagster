"""Configuration definitions for Dagster executors.

These pydantic models define the parameters accepted by Dagster executors when
declared in `dagster.yml` under the `executors` section.
"""

from logging import getLogger
from typing import Any

from pydantic import BaseModel, Field

LOGGER = getLogger(__name__)


class InProcessExecutorOptions(BaseModel):
    """Options for the in-process executor.

    Attributes:
        retries (RetriesEnableOptions | RetriesDisableOptions): Retry configuration for the executor.
    """

    class RetriesEnableOptions(BaseModel):
        """Enable retries for the executor."""

        enabled: dict = {}  # type: ignore[type-arg]

    class RetriesDisableOptions(BaseModel):
        """Disable retries for the executor."""

        disabled: dict = {}  # type: ignore[type-arg]

    retries: RetriesEnableOptions | RetriesDisableOptions = Field(
        default=RetriesEnableOptions(),
        description="Whether retries are enabled or not.",
    )


class MultiprocessExecutorOptions(InProcessExecutorOptions):
    """Options for the multiprocess executor.

    Attributes:
        retries (RetriesEnableOptions | RetriesDisableOptions): Retry configuration for the executor.
        max_concurrent (int): Maximum number of concurrent processes.
    """

    max_concurrent: int = Field(
        default=1,
        description=(
            "The number of processes that may run concurrently. "
            "By default, this is set to be the return value of `multiprocessing.cpu_count()`."
        ),
    )


class DaskClusterConfig(BaseModel):
    """Configuration for the Dask cluster.

    Attributes:
        existing (dict[str, str] | None): Connect to an existing scheduler.
        local (dict[str, Any] | None): Local cluster configuration.
        yarn (dict[str, Any] | None): YARN cluster configuration.
        ssh (dict[str, Any] | None): SSH cluster configuration.
        pbs (dict[str, Any] | None): PBS cluster configuration.
        moab (dict[str, Any] | None): Moab cluster configuration.
        sge (dict[str, Any] | None): SGE cluster configuration.
        lsf (dict[str, Any] | None): LSF cluster configuration.
        slurm (dict[str, Any] | None): SLURM cluster configuration.
        oar (dict[str, Any] | None): OAR cluster configuration.
        kube (dict[str, Any] | None): Kubernetes cluster configuration.
    """

    existing: dict[str, str] | None = Field(default=None, description="Connect to an existing scheduler.")
    local: dict[str, Any] | None = Field(default=None, description="Local cluster configuration.")
    yarn: dict[str, Any] | None = Field(default=None, description="YARN cluster configuration.")
    ssh: dict[str, Any] | None = Field(default=None, description="SSH cluster configuration.")
    pbs: dict[str, Any] | None = Field(default=None, description="PBS cluster configuration.")
    moab: dict[str, Any] | None = Field(default=None, description="Moab cluster configuration.")
    sge: dict[str, Any] | None = Field(default=None, description="SGE cluster configuration.")
    lsf: dict[str, Any] | None = Field(default=None, description="LSF cluster configuration.")
    slurm: dict[str, Any] | None = Field(default=None, description="SLURM cluster configuration.")
    oar: dict[str, Any] | None = Field(default=None, description="OAR cluster configuration.")
    kube: dict[str, Any] | None = Field(default=None, description="Kubernetes cluster configuration.")


class DaskExecutorOptions(BaseModel):
    """Options for the Dask executor.

    Attributes:
        cluster (DaskClusterConfig): Configuration for the Dask cluster.
    """

    cluster: DaskClusterConfig = Field(default=DaskClusterConfig(), description="Configuration for the Dask cluster.")


class DockerExecutorOptions(MultiprocessExecutorOptions):
    """Options for the Docker-based executor.

    Attributes:
        retries (RetriesEnableOptions | RetriesDisableOptions): Retry configuration for the executor.
        max_concurrent (int | None): Maximum number of concurrent processes.
        image (str | None): Docker image to use.
        network (str | None): Name of the network to connect the container at creation time.
        registry (dict[str, str] | None): Information for using a non local/public docker registry.
        env_vars (list[str]): Environment variables for the container.
        container_kwargs (dict[str, Any] | None): Key-value pairs for containers.create.
        networks (list[str]): Names of the networks to connect the container at creation time.
    """

    image: str | None = Field(
        default=None, description="The docker image to be used if the repository does not specify one."
    )
    network: str | None = Field(
        default=None, description="Name of the network to which to connect the launched container at creation time."
    )
    registry: dict[str, str] | None = Field(
        default=None, description="Information for using a non local/public docker registry."
    )
    env_vars: list[str] = Field(
        default=[],
        description=(
            "The list of environment variables names to include in the docker container. "
            "Each can be of the form KEY=VALUE or just KEY (in which case the value will be pulled "
            "from the local environment)."
        ),
    )
    container_kwargs: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Key-value pairs that can be passed into containers.create. See "
            "https://docker-py.readthedocs.io/en/stable/containers.html for the full list "
            "of available options."
        ),
    )
    networks: list[str] = Field(
        default=[], description="Names of the networks to which to connect the launched container at creation time."
    )


class CeleryExecutorOptions(BaseModel):
    """Options for the Celery-based executor.

    Attributes:
        broker (str | None): Celery broker URL.
        backend (str | None): Celery backend URL.
        include (list[str]): List of modules every worker should import.
        config_source (dict[str, Any] | None): Additional settings for the Celery app.
        retries (int | None): Number of retries for the Celery tasks.
    """

    broker: str | None = Field(
        default=None,
        description=(
            "The URL of the Celery broker. Default: "
            "'pyamqp://guest@{os.getenv('DAGSTER_CELERY_BROKER_HOST',"
            "'localhost')}//'."
        ),
    )
    backend: str | None = Field(
        default="rpc://",
        description="The URL of the Celery results backend. Default: 'rpc://'.",
    )
    include: list[str] = Field(default=[], description="List of modules every worker should import.")
    config_source: dict[str, Any] | None = Field(default=None, description="Additional settings for the Celery app.")
    retries: int | None = Field(default=None, description="Number of retries for the Celery tasks.")


class CeleryDockerExecutorOptions(CeleryExecutorOptions, DockerExecutorOptions):
    """Options for the Celery-based executor which launches tasks as Docker containers.

    Uses fields from both CeleryExecutorOptions and DockerExecutorOptions to configure
    Celery workers running in Docker.

    Attributes:
        broker (str | None): Celery broker URL.
        backend (str | None): Celery backend URL.
        include (list[str]): List of modules every worker should import.
        config_source (dict[str, Any] | None): Additional settings for the Celery app.
        retries (int | None): Number of retries for the Celery tasks.
        image (str | None): Docker image to use.
        network (str | None): Name of the network to connect the container at creation time.
        registry (dict[str, str] | None): Information for using a non local/public docker registry.
        env_vars (list[str]): Environment variables for the container.
        container_kwargs (dict[str, Any] | None): Key-value pairs for containers.create.
        networks (list[str]): Names of the networks to connect the container at creation time.
        max_concurrent (int | None): Maximum number of concurrent processes.
        retries (RetriesEnableOptions | RetriesDisableOptions): Retry configuration for the executor.
    """

    pass


class K8sJobConfig(BaseModel):
    """Configuration for Kubernetes jobs.

    Attributes:
        container_config (dict[str, Any]): Configuration for the Kubernetes container.
        pod_spec_config (dict[str, Any]): Configuration for the Pod specification.
        pod_template_spec_metadata (dict[str, Any]): Metadata for the Pod template specification.
        job_spec_config (dict[str, Any]): Configuration for the Job specification.
        job_metadata (dict[str, Any]): Metadata for the Job.
    """

    container_config: dict[str, Any] = Field(default={}, description="Configuration for the Kubernetes container.")
    pod_spec_config: dict[str, Any] = Field(
        default={}, description="Configuration for the Kubernetes Pod specification."
    )
    pod_template_spec_metadata: dict[str, Any] = Field(
        default={}, description="Metadata for the Kubernetes Pod template specification."
    )
    job_spec_config: dict[str, Any] = Field(
        default={}, description="Configuration for the Kubernetes Job specification."
    )
    job_metadata: dict[str, Any] = Field(default={}, description="Metadata for the Kubernetes Job.")


class K8sJobExecutorOptions(MultiprocessExecutorOptions):
    """Options for the Kubernetes-based executor.

    Attributes:
        retries (RetriesEnableOptions | RetriesDisableOptions): Retry configuration for the executor.
        max_concurrent (int | None): Maximum number of concurrent processes.
        job_namespace (str): Kubernetes namespace for jobs.
        load_incluster_config (bool): Whether the executor is running within a k8s cluster.
        kubeconfig_file (str | None): Path to a kubeconfig file to use.
        step_k8s_config (K8sJobConfig): Raw Kubernetes configuration for each step.
        per_step_k8s_config (dict[str, K8sJobConfig]): Per op k8s configuration overrides.
        image_pull_policy (str | None): Image pull policy for Pods.
        image_pull_secrets (list[dict[str, str]] | None): Credentials for pulling images.
        service_account_name (str | None): Kubernetes service account name.
        env_config_maps (list[str] | None): ConfigMapEnvSource names for environment variables.
        env_secrets (list[str] | None): Secret names for environment variables.
        env_vars (list[str] | None): Environment variables for the job.
        volume_mounts (list[dict[str, str]]): Volume mounts for the container.
        volumes (list[dict[str, str]]): Volumes for the Pod.
        labels (dict[str, str]): Labels for created pods.
        resources (dict[str, dict[str, str]] | None): Compute resource requirements.
        scheduler_name (str | None): Custom Kubernetes scheduler for Pods.
        security_context (dict[str, str]): Security settings for the container.
    """

    job_namespace: str = Field(default="dagster")
    load_incluster_config: bool = Field(
        default=True,
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
    )
    step_k8s_config: K8sJobConfig = Field(
        default=K8sJobConfig(),
        description="Raw Kubernetes configuration for each step launched by the executor.",
    )
    per_step_k8s_config: dict[str, K8sJobConfig] = Field(
        default={},
        description="Per op k8s configuration overrides.",
    )
    image_pull_policy: str | None = Field(
        default=None,
        description="Image pull policy to set on launched Pods.",
    )
    image_pull_secrets: list[dict[str, str]] | None = Field(
        default=None,
        description="Specifies that Kubernetes should get the credentials from the Secrets named in this list.",
    )
    service_account_name: str | None = Field(
        default=None,
        description="The name of the Kubernetes service account under which to run.",
    )
    env_config_maps: list[str] | None = Field(
        default=None,
        description="A list of custom ConfigMapEnvSource names from which to draw environment variables (using ``envFrom``) for the Job. Default: ``[]``.",
    )
    env_secrets: list[str] | None = Field(
        default=None,
        description="A list of custom Secret names from which to draw environment variables (using ``envFrom``) for the Job. Default: ``[]``.",
    )
    env_vars: list[str] | None = Field(
        default=None,
        description="A list of environment variables to inject into the Job. Each can be of the form KEY=VALUE or just KEY (in which case the value will be pulled from the current process). Default: ``[]``.",
    )
    volume_mounts: list[dict[str, str]] = Field(
        default=[],
        description="A list of volume mounts to include in the job's container. Default: ``[]``.",
    )
    volumes: list[dict[str, str]] = Field(
        default=[],
        description="A list of volumes to include in the Job's Pod. Default: ``[]``.",
    )
    labels: dict[str, str] = Field(
        default={},
        description="Labels to apply to all created pods.",
    )
    resources: dict[str, dict[str, str]] | None = Field(
        default=None,
        description="Compute resource requirements for the container.",
    )
    scheduler_name: str | None = Field(
        default=None,
        description="Use a custom Kubernetes scheduler for launched Pods.",
    )
    security_context: dict[str, str] = Field(
        default={},
        description="Security settings for the container.",
    )


class CeleryK8sJobExecutorOptions(CeleryExecutorOptions, K8sJobExecutorOptions):
    """Options for the Celery-based executor which launches tasks as Kubernetes jobs.

    Attributes:
        broker (str | None): Celery broker URL.
        backend (str | None): Celery backend URL.
        include (list[str]): List of modules every worker should import.
        config_source (dict[str, Any] | None): Additional settings for the Celery app.
        retries (int | None): Number of retries for the Celery tasks.
        job_namespace (str | None): Kubernetes namespace for jobs.
        load_incluster_config (bool | None): Whether the executor is running within a k8s cluster.
        kubeconfig_file (str | None): Path to a kubeconfig file to use.
        step_k8s_config (K8sJobConfig): Raw Kubernetes configuration for each step.
        per_step_k8s_config (dict[str, K8sJobConfig]): Per op k8s configuration overrides.
        image_pull_policy (str | None): Image pull policy for Pods.
        image_pull_secrets (list[dict[str, str]] | None): Credentials for pulling images.
        service_account_name (str | None): Kubernetes service account name.
        env_config_maps (list[str] | None): ConfigMapEnvSource names for environment variables.
        env_secrets (list[str] | None): Secret names for environment variables.
        env_vars (list[str] | None): Environment variables for the job.
        volume_mounts (list[dict[str, str]]): Volume mounts for the container.
        volumes (list[dict[str, str]]): Volumes for the Pod.
        labels (dict[str, str]): Labels for created pods.
        resources (dict[str, dict[str, str]] | None): Compute resource requirements.
        scheduler_name (str | None): Custom Kubernetes scheduler for Pods.
        security_context (dict[str, str]): Security settings for the container.
        job_wait_timeout (float): Wait time in seconds for a job to complete before marking as failed.
    """

    job_wait_timeout: float = Field(
        default=86400.0,
        description=(
            "Wait this many seconds for a job to complete before marking the run as failed."
            f" Defaults to {86400.0} seconds."
        ),
    )


ExecutorOptions = (
    InProcessExecutorOptions
    | MultiprocessExecutorOptions
    | K8sJobExecutorOptions
    | DockerExecutorOptions
    | CeleryExecutorOptions
    | CeleryDockerExecutorOptions
    | CeleryK8sJobExecutorOptions
)


EXECUTOR_MAP = {
    "in_process": InProcessExecutorOptions,
    "multiprocess": MultiprocessExecutorOptions,
    "k8s_job_executor": K8sJobExecutorOptions,
    "docker_executor": DockerExecutorOptions,
    "celery_executor": CeleryExecutorOptions,
    "celery_docker_executor": CeleryDockerExecutorOptions,
    "celery_k8s_job_executor": CeleryK8sJobExecutorOptions,
}
