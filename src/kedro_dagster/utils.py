"""Utility functions."""

from pathlib import Path
from typing import TYPE_CHECKING, Any

import dagster as dg
from jinja2 import Environment, FileSystemLoader
from kedro.framework.project import find_pipelines
from pydantic import ConfigDict, create_model

if TYPE_CHECKING:
    from kedro.pipeline import Pipeline
    from kedro.pipeline.node import Node
    from pydantic import BaseModel


def render_jinja_template(src: str | Path, is_cookiecutter=False, **kwargs) -> str:  # type: ignore[no-untyped-def]
    """Render a Jinja template from a file or string.

    Args:
        src (str | Path): Path to the template file or template string.
        is_cookiecutter (bool): Whether to use cookiecutter-style rendering.
        **kwargs: Variables to pass to the template.

    Returns:
        str: Rendered template as a string.
    """
    src = Path(src)

    template_loader = FileSystemLoader(searchpath=src.parent.as_posix())
    # the keep_trailing_new_line option is mandatory to
    # make sure that black formatting will be preserved
    template_env = Environment(loader=template_loader, keep_trailing_newline=True)
    template = template_env.get_template(src.name)
    if is_cookiecutter:
        # we need to match tags from a cookiecutter object
        # but cookiecutter only deals with folder, not file
        # thus we need to create an object with all necessary attributes
        class FalseCookieCutter:
            def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
                self.__dict__.update(kwargs)

        parsed_template = template.render(cookiecutter=FalseCookieCutter(**kwargs))  # type: ignore[no-untyped-call]
    else:
        parsed_template = template.render(**kwargs)

    return parsed_template  # type: ignore[no-any-return]


def write_jinja_template(src: str | Path, dst: str | Path, **kwargs) -> None:  # type: ignore[no-untyped-def]
    """Render and write a Jinja template to a destination file.

    Args:
        src (str | Path): Path to the template file.
        dst (str | Path): Path to the output file.
        **kwargs: Variables to pass to the template.
    """
    dst = Path(dst)
    parsed_template = render_jinja_template(src, **kwargs)
    with open(dst, "w") as file_handler:
        file_handler.write(parsed_template)


def get_asset_key_from_dataset_name(dataset_name: str, env: str) -> dg.AssetKey:
    """Get a Dagster AssetKey from a Kedro dataset name and environment.

    Args:
        dataset_name (str): The Kedro dataset name.
        env (str): The Kedro environment.

    Returns:
        AssetKey: The corresponding Dagster AssetKey.
    """
    return dg.AssetKey([env] + dataset_name.split("."))


def dagster_format(name: str) -> str:
    """Format a name to Dagster's asset naming convention.

    Args:
        name (str): The name to format.

    Returns:
        str: The formatted name.
    """
    return name.replace(".", "__")


def kedro_format(name: str) -> str:
    """Convert a Dagster-formatted name back to Kedro's naming convention.

    Args:
        name (str): The Dagster-formatted name.

    Returns:
        str: The original Kedro name.
    """
    return name.replace("__", ".")


def _create_pydantic_model_from_dict(  # type: ignore[no-untyped-def]
    name: str, params: dict[str, Any], __base__, __config__: ConfigDict | None = None
) -> "BaseModel":
    """Dynamically create a Pydantic model from a dictionary of parameters.

    Args:
        name (str): Name of the model.
        params (dict): Parameters for the model.
        __base__: Base class for the model.
        __config__ (ConfigDict | None): Optional Pydantic config.

    Returns:
        BaseModel: The created Pydantic model.
    """
    fields = {}
    for param_name, param_value in params.items():
        if isinstance(param_value, dict):
            # Recursively create a nested model for nested dictionaries
            nested_model = _create_pydantic_model_from_dict(name, param_value, __base__=__base__, __config__=__config__)
            fields[param_name] = (nested_model, ...)
        else:
            # Use the type of the value as the field type
            param_type = type(param_value)
            if param_type is type(None):
                param_type = dg.Any

            fields[param_name] = (param_type, param_value)

    if __base__ is None:
        model = create_model(name, __config__=__config__, **fields)
    else:
        model = create_model(name, __base__=__base__, **fields)
        model.config = __config__

    return model


def is_mlflow_enabled() -> bool:
    """Check if MLflow is enabled in the Kedro context.

    Returns:
        bool: True if MLflow is enabled, False otherwise.
    """
    try:
        import kedro_mlflow  # NOQA
        import mlflow  # NOQA

        return True
    except ImportError:
        return False


def _is_asset_name(dataset_name: str) -> bool:
    """Determine if a dataset name should be treated as an asset.

    Args:
        dataset_name (str): The dataset name.

    Returns:
        bool: True if the name is an asset, False otherwise.
    """
    return not dataset_name.startswith("params:") and dataset_name != "parameters"


def _get_node_pipeline_name(node: "Node") -> str:
    """Return the name of the pipeline that a node belongs to.

    Args:
        node (Node): The Kedro Node.

    Returns:
        str: Name of the pipeline the node belongs to.
    """
    pipelines: dict[str, Pipeline] = find_pipelines()

    for pipeline_name, pipeline in pipelines.items():
        if pipeline_name != "__default__":
            for pipeline_node in pipeline.nodes:
                if node.name == pipeline_node.name:
                    if "." in node.name:
                        namespace = ".".join(node.name.split(".")[:-1])
                        return dagster_format(f"{namespace}.{pipeline_name}")
                    return pipeline_name

    raise ValueError(f"Node `{node.name}` is not part of any pipelines.")


def get_filter_params_dict(pipeline_config: dict[str, Any]) -> dict[str, Any]:
    """Extract filter parameters from a pipeline config dict.

    Args:
        pipeline_config (dict[str, Any]): Pipeline configuration.

    Returns:
        dict[str, Any]: Filter parameters.
    """
    filter_params = dict(
        tags=pipeline_config.get("tags"),
        from_nodes=pipeline_config.get("from_nodes"),
        to_nodes=pipeline_config.get("to_nodes"),
        node_names=pipeline_config.get("node_names"),
        from_inputs=pipeline_config.get("from_inputs"),
        to_outputs=pipeline_config.get("to_outputs"),
        node_namespace=pipeline_config.get("node_namespace"),
    )

    return filter_params


def get_mlflow_resource_from_config(mlflow_config: "BaseModel") -> dg.ResourceDefinition:
    """Create a Dagster resource definition from MLflow config.

    Args:
        mlflow_config (BaseModel): MLflow configuration.

    Returns:
        ResourceDefinition: Dagster resource definition for MLflow.
    """
    from dagster_mlflow import mlflow_tracking

    mlflow_resource = mlflow_tracking.configured({
        "experiment_name": mlflow_config.tracking.experiment.name,
        "mlflow_tracking_uri": mlflow_config.server.mlflow_tracking_uri,
        "parent_run_id": None,
    })

    return mlflow_resource
