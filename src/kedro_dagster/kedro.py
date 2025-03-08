"""Translation of Kedro run params and on error pipeline hooks."""

from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro import __version__ as kedro_version
from kedro.framework.project import pipelines

if TYPE_CHECKING:
    from kedro.framework.context import KedroContext


class KedroRunTranslator:
    """Translator for Kedro run params.

    Args:
        context (KedroContext): Kedro context.
        project_path (str): Path to the Kedro project.
        env (str): Kedro environment.
        session_id (str): Kedro session ID.

    """

    def __init__(self, context: "KedroContext", project_path: str, env: str, session_id: str):
        self._context = context
        self._catalog = context.catalog
        self._hook_manager = context._hook_manager
        self._kedro_params = dict(
            project_path=project_path,
            env=env,
            session_id=session_id,
            kedro_version=kedro_version,
        )

    def to_dagster(
        self,
        pipeline_name: str,
        filter_params: dict[str, Any],
    ) -> dg.ConfigurableResource:
        """Create a Dagster resource for Kedro pipeline hooks.

        Args:
            pipeline_name (str): Name of the Kedro pipeline.
            filter_params (dict[str, Any]): Parameters used to filter the pipeline.

        Returns:
            ConfigurableResource: A Dagster resource for Kedro pipeline hooks.

        """

        context = self._context
        hook_manager = self._hook_manager

        class RunParamsModel(dg.Config):
            session_id: str
            project_path: str
            env: str
            kedro_version: str
            pipeline_name: str
            load_versions: list[str] | None = None
            extra_params: dict[str, Any] | None = None
            runner: str | None = None
            node_names: list[str] | None = None
            from_nodes: list[str] | None = None
            to_nodes: list[str] | None = None
            from_inputs: list[str] | None = None
            to_outputs: list[str] | None = None
            node_namespace: str | None = None
            tags: list[str] | None = None

            class Config:
                # force triggering type control when setting value instead of init
                validate_assignment = True
                # raise an error if an unknown key is passed to the constructor
                extra = "forbid"

        class KedroRunResource(RunParamsModel, dg.ConfigurableResource):
            """Resource for Kedro context."""

            @property
            def run_params(self) -> dict[str, Any]:
                return self.model_dump()  # type: ignore[no-any-return]

            @property
            def pipeline(self) -> dict[str, Any]:
                return pipelines.get(self.pipeline_name).filter(  # type: ignore[no-any-return]
                    tags=self.tags,
                    from_nodes=self.from_nodes,
                    to_nodes=self.to_nodes,
                    node_names=self.node_names,
                    from_inputs=self.from_inputs,
                    to_outputs=self.to_outputs,
                    node_namespace=self.node_namespace,
                )

            def after_context_created_hook(self) -> None:
                hook_manager.hook.after_context_created(context=context)

        run_params = (
            self._kedro_params
            | filter_params
            | dict(
                pipeline_name=pipeline_name,
                load_versions=None,
                extra_params=None,
                runner=None,
            )
        )

        return KedroRunResource(**run_params)

    def _translate_on_pipeline_error_hook(
        self, named_jobs: dict[str, dg.JobDefinition]
    ) -> dict[str, dg.SensorDefinition]:
        """Translate Kedro pipeline hooks to Dagster resource and sensor.

        Args:
            named_jobs (dict[str, JobDefinition]): Dictionary of named Dagster jobs.

        Returns:
            dict[str, dg.SensorDefinition]: A dictionary with the sensor definition
            for the `on_pipeline_error` hook.

        """

        @dg.run_failure_sensor(
            name="on_pipeline_error_sensor",
            description="Sensor for kedro `on_pipeline_error` hook.",
            monitored_jobs=list(named_jobs.values()),
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def on_pipeline_error_sensor(context: dg.RunFailureSensorContext) -> None:
            kedro_context_resource = context.resource_defs["kedro_run"]
            run_params = kedro_context_resource.run_params
            pipeline = kedro_context_resource.pipeline

            error_class_name = context.failure_event.event_specific_data.error.cls_name
            error_message = context.failure_event.event_specific_data.error.message

            context.log.error(f"{error_class_name}: {error_message}")

            self._hook_manager.hook.on_pipeline_error(
                error=error_class_name(error_message),
                run_params=run_params,
                pipeline=pipeline,
                catalog=self._catalog,
            )
            context.log.info("Pipeline hook sensor executed `on_pipeline_error` hook`.")

        return {"on_pipeline_error_sensor": on_pipeline_error_sensor}
