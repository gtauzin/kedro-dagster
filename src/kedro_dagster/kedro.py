"""Translation of Kedro run params and on error pipeline hooks."""

from logging import getLogger
from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro import __version__ as kedro_version

from kedro_dagster.utils import KEDRO_VERSION, PYDANTIC_VERSION, create_pydantic_config, get_filter_params_dict

LOGGER = getLogger(__name__)

if TYPE_CHECKING:
    from kedro.framework.context import KedroContext


class KedroRunTranslator:
    """Translator for Kedro run params.

    Args:
        context (KedroContext): Kedro context.
        project_path (str): Path to the Kedro project.
        env (str): Kedro environment.
        run_id (str): Kedro run ID. In Kedro < 1.0, this is called `session_id`.

    """

    def __init__(self, context: "KedroContext", project_path: str, env: str, run_id: str):
        self._context = context
        self._catalog = context.catalog
        self._hook_manager = context._hook_manager
        self._kedro_params = dict(
            project_path=project_path,
            env=env,
            kedro_version=kedro_version,
        )
        if KEDRO_VERSION[0] >= 1:
            self._kedro_params["run_id"] = run_id
        else:  # pragma: no cover
            self._kedro_params["session_id"] = run_id

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
            ConfigurableResource: Dagster resource for Kedro pipeline hooks.

        """
        LOGGER.info(f"Creating Kedro run resource for pipeline '{pipeline_name}'")

        context = self._context
        hook_manager = self._hook_manager

        class RunParamsModel(dg.Config):
            if KEDRO_VERSION[0] >= 1:
                run_id: str
            else:
                session_id: str
            project_path: str
            env: str
            kedro_version: str
            pipeline_name: str
            load_versions: list[str] | None = None
            if KEDRO_VERSION[0] >= 1:
                runtime_params: dict[str, Any] | None = None
            else:  # pragma: no cover
                extra_params: dict[str, Any] | None = None
            runner: str | None = None
            node_names: list[str] | None = None
            from_nodes: list[str] | None = None
            to_nodes: list[str] | None = None
            from_inputs: list[str] | None = None
            to_outputs: list[str] | None = None
            # Kedro 1.x renamed the namespace filter kwarg to `node_namespaces` (plural).
            # Expose the appropriate field name based on the installed Kedro version while
            # keeping the rest of the configuration stable.
            if KEDRO_VERSION[0] >= 1:
                node_namespaces: list[str] | None = None
            else:  # pragma: no cover
                node_namespace: str | None = None
            tags: list[str] | None = None

            # Version-aware Pydantic configuration
            if PYDANTIC_VERSION[0] >= 2:  # noqa: PLR2004
                model_config = create_pydantic_config(validate_assignment=True, extra="forbid")
            else:  # pragma: no cover
                Config = create_pydantic_config(validate_assignment=True, extra="forbid")

        class KedroRunResource(RunParamsModel, dg.ConfigurableResource):
            """Resource for Kedro context."""

            @property
            def run_params(self) -> dict[str, Any]:
                """Return all run parameters as a dictionary.

                Returns:
                    dict[str, Any]: Dictionary containing all Kedro run parameters
                        including run_id/session_id, project path, environment, pipeline
                        name, and filtering options.
                """
                return self.model_dump()  # type: ignore[no-any-return]

            @property
            def pipeline(self) -> dict[str, Any]:
                """Load and return the filtered Kedro pipeline.

                Applies filtering parameters (node names, tags, namespaces, from/to nodes)
                to the base pipeline to produce the final executable pipeline.

                Returns:
                    dict[str, Any]: The filtered Kedro pipeline object.
                """
                # Lazy import to avoid circular dependency
                from kedro.framework.project import pipelines

                node_namespace_key: str | None = None
                node_namespace_val: Any | None = None
                if KEDRO_VERSION[0] >= 1:
                    node_namespace_key, node_namespace_val = "node_namespaces", getattr(self, "node_namespaces")
                else:  # pragma: no cover
                    node_namespace_key, node_namespace_val = (
                        "node_namespace",
                        getattr(self, "node_namespace"),
                    )

                pipeline_config: dict[str, Any] = {
                    "tags": self.tags,
                    "from_nodes": self.from_nodes,
                    "to_nodes": self.to_nodes,
                    "node_names": self.node_names,
                    "from_inputs": self.from_inputs,
                    "to_outputs": self.to_outputs,
                    node_namespace_key: node_namespace_val,
                }

                filter_kwargs = get_filter_params_dict(pipeline_config)

                pipeline_obj = pipelines.get(self.pipeline_name)
                return pipeline_obj.filter(**filter_kwargs)  # type: ignore[no-any-return]

            def after_context_created_hook(self) -> None:
                """Invoke the Kedro `after_context_created` hook.

                This method is called at the beginning of Dagster job execution to
                trigger any registered Kedro hooks that should run after the Kedro
                context is initialized.
                """
                hook_manager.hook.after_context_created(context=context)

            def after_catalog_created_hook(self) -> None:
                """Invoke the Kedro `after_catalog_created` hook.

                This method is called at the beginning of Dagster job execution to
                trigger any registered Kedro hooks that should run after the Kedro
                catalog is created.
                """
                from kedro.framework.context.context import _convert_paths_to_absolute_posix

                conf_catalog = context.config_loader["catalog"]
                conf_catalog = _convert_paths_to_absolute_posix(
                    project_path=context.project_path, conf_dictionary=conf_catalog
                )
                conf_creds = context._get_config_credentials()

                if KEDRO_VERSION[0] >= 1:
                    save_version = self.run_id
                else:  # pragma: no cover
                    save_version = self.session_id

                hook_manager.hook.after_catalog_created(
                    context=context,
                    catalog=context.catalog,
                    parameters=context._get_parameters(),
                    conf_catalog=conf_catalog,
                    conf_creds=conf_creds,
                    save_version=save_version,
                    load_versions=self.load_versions,
                )

        run_params = (
            self._kedro_params
            | filter_params
            | dict(
                pipeline_name=pipeline_name,
                load_versions=None,
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
            dict[str, dg.SensorDefinition]: Dictionary with the sensor definition
            for the `on_pipeline_error` hook.

        """
        LOGGER.info("Creating Dagster run sensors...")

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
