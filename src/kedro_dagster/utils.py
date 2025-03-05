"""Utility functions."""

from pathlib import Path
from typing import TYPE_CHECKING, Any

import dagster as dg
from jinja2 import Environment, FileSystemLoader
from pydantic import ConfigDict, create_model

if TYPE_CHECKING:
    from kedro.pipeline import Pipeline
    from kedro.pipeline.node import Node
    from pydantic import BaseModel


def render_jinja_template(src: str | Path, is_cookiecutter=False, **kwargs) -> str:
    """This functions enable to copy a file and render the
    tags (identified by {{ my_tag }}) with the values provided in kwargs.

    Arguments:
        src (Union[str, Path]): The path to the template which should be rendered

    Returns:
        str: A string that contains all the files with replaced tags.
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
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        parsed_template = template.render(cookiecutter=FalseCookieCutter(**kwargs))
    else:
        parsed_template = template.render(**kwargs)

    return parsed_template


def write_jinja_template(src: str | Path, dst: str | Path, **kwargs) -> None:
    """Write a template file and replace tis jinja's tags
     (identified by {{ my_tag }}) with the values provided in kwargs.

    Arguments:
        src (Union[str, Path]): Path to the template which should be rendered.
        dst (Union[str, Path]): Path where the rendered template should be saved.
    """
    dst = Path(dst)
    parsed_template = render_jinja_template(src, **kwargs)
    with open(dst, "w") as file_handler:
        file_handler.write(parsed_template)


def get_asset_key_from_dataset_name(dataset_name: str, env: str) -> dg.AssetKey:
    """Get the asset key from a dataset name.

    Args:
        dataset_name (str): The name of the dataset.

    Returns:
        AssetKey: The asset key.
    """
    return dg.AssetKey([env] + dataset_name.split("."))


def dagster_format(name):
    """Format a name for Dagster.

    Args:
        name (str): The name to format.

    Returns:
        str: The name formatted in a Dagster-friendly way.
    """
    return name.replace(".", "__")


def kedro_format(name):
    """Format a name for Kedro.

    Args:
        name (str): The name to format.

    Returns:
        str: The name formatted in a Kedro-friendly way.
    """

    return name.replace("__", ".")


# TODO: Improve
def _create_pydantic_model_from_dict(
    name: str, params: dict[str, Any], __base__, __config__: ConfigDict | None = None
) -> "BaseModel":
    """Create a Pydantic model from a dictionary.

    Args:
        name (str): Name of the created class.
        params (dict[str, Any]): The dictionary of parameters.
        __base__: The base class for the model.
        __config__: The configuration for the model.

    Returns:
        BaseModel: The Pydantic model.
    """
    fields = {}
    for param_name, param_value in params.items():
        if isinstance(param_value, dict):
            # Recursively create a nested model for nested dictionaries
            nested_model = _create_pydantic_model_from_dict(name, param_value, __base__=__base__, __config__=__config__)
            # TODO: Nested __base__? Yes for NodeParams, no for IOManagers?

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
    """Check if `mlflow` is enabled:

    Returns:
        bool: Whether `mlflow` is enabled.

    """
    try:
        import kedro_mlflow  # NOQA
        import mlflow  # NOQA

        return True
    except ImportError:
        return False


def _is_asset_name(dataset_name: str) -> bool:
    """Check if a dataset name is an asset name.

    Args:
        dataset_name (str): The name of the dataset.

    Returns:
        bool: Whether the dataset is an asset.
    """
    return not dataset_name.startswith("params:") and dataset_name != "parameters"


def _get_node_pipeline_name(pipelines: dict[str, "Pipeline"], node: "Node"):
    """Return the name of the pipeline that a node belongs to.

    Args:
        pipelines (dict[str, Pipeline]): Dictionary of Kedro pipelines.
        node Node: The Kedro ``Node`` for which the pipeline name is being retrieved.

    Returns:
        str: Name of the ``Pipeline`` that the ``Node`` belongs to.
    """
    for pipeline_name, pipeline in pipelines.items():
        if pipeline_name != "__default__":
            for pipeline_node in pipeline.nodes:
                if node.name == pipeline_node.name:
                    if "." in node.name:
                        namespace = ".".join(node.name.split(".")[:-1])
                        return dagster_format(f"{namespace}.{pipeline_name}")
                    return pipeline_name


def get_mlflow_resource_from_config(mlflow_config: "BaseModel") -> dg.ResourceDefinition:
    """Get `mlflow` resource and configure it based on the `mlflow` config.

    Args:
        mlflow_config (BaseModel): `kedro-mlflow` configuration.

    Returns:
        ResourceDefinition: Configured `mlflow` resource.
    """
    from dagster_mlflow import mlflow_tracking

    mlflow_resource = mlflow_tracking.configured({
        "experiment_name": mlflow_config.tracking.experiment.name,
        "mlflow_tracking_uri": mlflow_config.server.mlflow_tracking_uri,
        "parent_run_id": None,
    })

    return mlflow_resource
