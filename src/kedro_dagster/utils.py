"""Utility functions."""

import hashlib
import importlib
import re
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Any

import dagster as dg
from jinja2 import Environment, FileSystemLoader
from kedro.framework.project import find_pipelines
from pydantic import ConfigDict, create_model

from kedro_dagster.datasets import DagsterNothingDataset

try:
    from dagster_mlflow import mlflow_tracking
except ImportError:
    mlflow_tracking = None

if TYPE_CHECKING:
    from kedro.io import CatalogProtocol
    from kedro.io.catalog_config_resolver import CatalogConfigResolver
    from kedro.pipeline import Pipeline
    from kedro.pipeline.node import Node
    from pydantic import BaseModel

LOGGER = getLogger(__name__)
DAGSTER_ALLOWED_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
KEDRO_DAGSTER_SEPARATOR = "__"


def _get_version(package: str) -> tuple[int, int, int]:
    """Return a package version as a tuple: (major, minor, patch).

    Falls back to (0, 0, 0) when the package cannot be imported or the
    version string cannot be parsed. This avoids importing heavy internals
    and relies on the ``__version__`` attribute being present on the
    top-level package across versions.
    """
    try:
        module = importlib.import_module(package)
    except Exception:  # pragma: no cover
        return 0, 0, 0

    version_str = str(getattr(module, "__version__", "0.0.0"))
    # Most libraries (including Kedro and Dagster) use SemVer: X.Y.Z[...]
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)", version_str)
    if m:
        return int(m.group(1)), int(m.group(2)), int(m.group(3))

    return 0, 0, 0  # pragma: no cover


# Compute and expose module-level constants for importers.
KEDRO_VERSION = _get_version("kedro")
DAGSTER_VERSION = _get_version("dagster")


def find_kedro_project(current_dir: Path) -> Path | None:
    """Locate the Kedro project root starting from ``current_dir``.

    This wraps Kedro's ``_find_kedro_project`` with a version-compatible
    import that works across Kedro releases where the function moved modules.

    Args:
        current_dir: Directory to start searching from.

    Returns:
        Path | None: Project root path if found, else ``None``.
    """
    # Use the module-level constant to avoid repeated imports/parsing.
    if KEDRO_VERSION >= (1, 0, 0):
        FIND_KEDRO_PROJECT = getattr(importlib.import_module("kedro.utils"), "find_kedro_project", None)
    elif KEDRO_VERSION >= (0, 19, 12):  # pragma: no cover
        FIND_KEDRO_PROJECT = getattr(importlib.import_module("kedro.utils"), "_find_kedro_project", None)
    elif KEDRO_VERSION > (0, 0, 0):  # pragma: no cover
        FIND_KEDRO_PROJECT = getattr(importlib.import_module("kedro.framework.startup"), "_find_kedro_project", None)

    return FIND_KEDRO_PROJECT(current_dir)  # type: ignore[no-any-return]


def render_jinja_template(src: str | Path, is_cookiecutter: bool = False, **kwargs: Any) -> str:
    """Render a Jinja template from a file or string.

    Args:
        src (str | Path): Path to the template file or template string.
        is_cookiecutter (bool): Whether to use cookiecutter-style rendering.
        **kwargs: Variables to pass to the template.

    Returns:
        str: Rendered template as a string.
    """
    # Resolve to an absolute filesystem path to avoid platform-specific quirks
    # with drive-less absolute paths on Windows (e.g., "/tmp").
    src = Path(src).resolve()

    template_loader = FileSystemLoader(searchpath=str(src.parent))
    # the keep_trailing_new_line option is mandatory to
    # make sure that black formatting will be preserved
    template_env = Environment(loader=template_loader, keep_trailing_newline=True)
    template = template_env.get_template(src.name)
    if is_cookiecutter:
        # we need to match tags from a cookiecutter object
        # but cookiecutter only deals with folder, not file
        # thus we need to create an object with all necessary attributes
        class FalseCookieCutter:
            def __init__(self, **kwargs: Any):
                self.__dict__.update(kwargs)

        parsed_template = template.render(cookiecutter=FalseCookieCutter(**kwargs))
    else:
        parsed_template = template.render(**kwargs)

    return parsed_template  # type: ignore[no-any-return]


def write_jinja_template(src: str | Path, dst: str | Path, **kwargs: Any) -> None:
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
        dataset_name (str): Kedro dataset name.
        env (str): Kedro environment.

    Returns:
        AssetKey: Corresponding Dagster AssetKey.
    """
    return dg.AssetKey([env] + dataset_name.split("."))


def is_nothing_asset_name(catalog: "CatalogProtocol", dataset_name: str) -> bool:
    """Return True if the catalog entry is a DagsterNothingDataset.

    Args:
        catalog (CatalogProtocol): Kedro DataCatalog or mapping-like object.
        dataset_name (str): Kedro dataset name.

    Returns:
        bool: True if the dataset is a DagsterNothingDataset, False otherwise.
    """
    dataset = get_dataset_from_catalog(catalog, dataset_name)
    return isinstance(dataset, DagsterNothingDataset)


def get_dataset_from_catalog(catalog: "CatalogProtocol", dataset_name: str) -> Any | None:
    """Retrieve a dataset instance from a Kedro catalog across versions.

    This helper avoids relying on the private ``_get_dataset`` when it's not
    available (as in some Kedro 1.x catalog wrappers) and falls back to
    mapping-like access when possible.

    Args:
        catalog (CatalogProtocol): Kedro catalog or mapping-like object.
        dataset_name (str): Name of the dataset to retrieve.

    Returns:
        Any | None: Dataset instance if found, else ``None``.
    """
    result = None
    get_method = getattr(catalog, "_get_dataset", None)
    if callable(get_method):  # pragma: no cover
        try:
            result = get_method(dataset_name)
        except Exception:
            result = None
    else:
        # Mapping-like .get(name[, default])
        get_method = getattr(catalog, "get", None)
        if callable(get_method):
            try:
                result = get_method(dataset_name)
            except TypeError:  # pragma: no cover
                # Some get() signatures require a default
                try:
                    result = get_method(dataset_name, None)
                except Exception:
                    result = None
            except Exception:  # pragma: no cover
                result = None
        else:
            # Index access fallback
            try:
                result = catalog[dataset_name]
            except Exception:
                result = None

    if result is None:
        LOGGER.info(f"Dataset '{dataset_name}' not found in catalog.")

    return result


def get_match_pattern_from_catalog_resolver(config_resolver: "CatalogConfigResolver", ds_name: str) -> Any:
    """Return the matching dataset pattern from a CatalogConfigResolver.

    Kedro 1.x exposes ``match_dataset_pattern`` while older Kedro versions
    exposed ``match_pattern``. This helper abstracts over that difference and
    returns the first matching pattern string or ``None`` when no match is found
    or when the API is unavailable.

    Args:
        config_resolver: An instance of Kedro's CatalogConfigResolver.
        ds_name: Dataset name to match against resolver patterns.

    Returns:
        str | None: The first matching pattern or ``None``.
    """
    # Try both method names regardless of Kedro version, preferring the newer name
    for method_name in ("match_dataset_pattern", "match_pattern"):
        match_method = getattr(config_resolver, method_name, None)
        if callable(match_method):  # pragma: no cover
            try:
                return match_method(ds_name)
            except Exception:
                LOGGER.debug("Resolver.%s failed for dataset '%s'\n", method_name, ds_name, exc_info=True)
    return None


def get_partition_mapping(
    partition_mappings: dict[str, dg.PartitionMapping],
    upstream_asset_name: str,
    downstream_dataset_names: list[str],
    config_resolver: "CatalogConfigResolver",
) -> dg.PartitionMapping | None:
    """Get the appropriate partition mapping for an asset based on its downstream datasets.
    Args:
        partition_mappings (dict[str, PartitionMapping]): Dictionary of partition mappings.
        upstream_asset_name (str): Name of the upstream asset.
        downstream_dataset_names (list[str]): List of downstream dataset names.
        config_resolver (CatalogConfigResolver): Catalog config resolver to match patterns.

    Returns:
        PartitionMapping | None: Partition mapping or None if not found.
    """
    mapped_downstream_asset_names = partition_mappings.keys()
    if downstream_dataset_names:
        mapped_downstream_dataset_name = None
        for downstream_dataset_name in downstream_dataset_names:
            downstream_asset_name = format_dataset_name(downstream_dataset_name)
            if downstream_asset_name in mapped_downstream_asset_names:
                mapped_downstream_dataset_name = downstream_dataset_name
                break
            else:
                match_pattern = get_match_pattern_from_catalog_resolver(config_resolver, downstream_dataset_name)
                if match_pattern is not None:
                    mapped_downstream_dataset_name = match_pattern
                    break

        if mapped_downstream_dataset_name is not None:
            if mapped_downstream_dataset_name in partition_mappings:
                return partition_mappings[mapped_downstream_dataset_name]
            else:
                LOGGER.warning(
                    f"Downstream dataset `{mapped_downstream_dataset_name}` of `{upstream_asset_name}` "
                    "is not found in the partition mappings. "
                    "The default partition mapping (i.e., `AllPartitionMapping`) will be used."
                )
        else:
            LOGGER.warning(
                f"None of the downstream datasets `{downstream_dataset_names}` of `{upstream_asset_name}` "
                "is found in the partition mappings. "
                "The default partition mapping (i.e., `AllPartitionMapping`) will be used."
            )

    return None


def format_partition_key(partition_key: Any) -> str:
    """Format a partition key into a Dagster-safe suffix (^[A-Za-z0-9_]+$).

    Args:
        partition_key (Any): Partition key to serialize.

    Returns:
        str: Serialized partition key.
    """
    dagster_partition_key = re.sub(r"[^A-Za-z0-9_]", "_", partition_key)
    dagster_partition_key = dagster_partition_key.strip("_")

    if not dagster_partition_key:
        raise ValueError(f"Partition key `{partition_key}` cannot be formatted into a valid Dagster key.")
    return dagster_partition_key


def format_dataset_name(name: str) -> str:
    """Convert a dataset name so that it is valid under Dagster's naming convention.

    Args:
        name (str): Name to format.

    Returns:
        str: Formatted name.
    """
    # Special-case Dagster reserved identifiers to avoid conflicts with op/asset arg names
    # See dagster._core.definitions.utils.DISALLOWED_NAMES which includes "input" and "output".
    if name in {"input", "output"}:
        raise ValueError(
            f"Dataset name `{name}` is reserved in Dagster. "
            "Please rename your Kedro dataset to avoid conflicts with Dagster's naming convention."
        )

    dataset_name = name.replace(".", KEDRO_DAGSTER_SEPARATOR)

    if not DAGSTER_ALLOWED_PATTERN.match(dataset_name):
        dataset_name = re.sub(r"[^A-Za-z0-9_]", KEDRO_DAGSTER_SEPARATOR, dataset_name)
        LOGGER.warning(
            f"Dataset name `{name}` is not valid under Dagster's naming convention. "
            "Prefer naming your Kedro datasets with valid Dagster names. "
            f"Dataset named `{name}` has been converted to `{dataset_name}`."
        )

    return dataset_name


def format_node_name(name: str) -> str:
    """Convert a node name so that it is valid under Dagster's naming convention.

    Args:
        name (str): Node name to format.

    Returns:
        str: Formatted name.
    """
    dagster_name = name.replace(".", KEDRO_DAGSTER_SEPARATOR)

    if not DAGSTER_ALLOWED_PATTERN.match(dagster_name):
        dagster_name = f"unnamed_node_{hashlib.md5(name.encode('utf-8')).hexdigest()}"
        LOGGER.warning(
            "Node is either unnamed or not in regex ^[A-Za-z0-9_]+$. "
            "Prefer naming your Kedro nodes directly using a `name`. "
            f"Node named `{name}` has been converted to `{dagster_name}`."
        )

    return dagster_name


def unformat_asset_name(name: str) -> str:
    """Convert a Dagster-formatted asset name back to Kedro's naming convention.

    Args:
        name (str): Dagster-formatted name.

    Returns:
        str: Original Kedro name.
    """

    return name.replace(KEDRO_DAGSTER_SEPARATOR, ".")


def _create_pydantic_model_from_dict(
    name: str, params: dict[str, Any], __base__: Any, __config__: ConfigDict | None = None
) -> "BaseModel":
    """Dynamically create a Pydantic model from a dictionary of parameters.

    Args:
        name (str): Name of the model.
        params (dict[str, Any]): Parameters for the model.
        __base__: Base class for the model.
        __config__ (ConfigDict | None): Optional Pydantic config.

    Returns:
        BaseModel: Created Pydantic model.
    """
    fields = {}
    for param_name, param_value in params.items():
        if isinstance(param_value, dict):
            # Recursively create a nested model for nested dictionaries
            nested_model = _create_pydantic_model_from_dict(name, param_value, __base__=__base__, __config__=__config__)
            # Provide a default instance so the field is not required at construction time
            try:
                default_nested = nested_model()
            except Exception:
                # Fallback to raw dict if instantiation fails for any reason
                default_nested = param_value
                nested_model = dict
            fields[param_name] = (nested_model, default_nested)
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
        if __config__ is not None:
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


def _is_param_name(dataset_name: str) -> bool:
    """Determine if a dataset name should be treated as a parameter.

    Args:
    dataset_name (str): Dataset name.

    Returns:
        bool: True if the name is a parameter, False otherwise.
    """
    return dataset_name.startswith("params:") or dataset_name == "parameters"


def _get_node_pipeline_name(node: "Node") -> str:
    """Return the name of the pipeline that a node belongs to.

    Args:
    node (Node): Kedro node.

    Returns:
        str: Name of the pipeline the node belongs to.
    """
    pipelines: dict[str, Pipeline] = find_pipelines()

    for pipeline_name, pipeline in pipelines.items():
        if pipeline_name != "__default__":
            for pipeline_node in pipeline.nodes:
                if node.name == pipeline_node.name:
                    if "." in node.name:
                        namespace = format_node_name(".".join(node.name.split(".")[:-1]))
                        return f"{namespace}__{pipeline_name}"
                    return pipeline_name

    LOGGER.warning(
        f"Node `{node.name}` is not part of any pipelines. Assigning '__none__' as its corresponding pipeline name."
    )

    return "__none__"


def get_filter_params_dict(pipeline_config: dict[str, Any]) -> dict[str, Any]:
    """Extract filter parameters from a pipeline config dict.

    Args:
        pipeline_config (dict[str, Any]): Pipeline configuration.

    Returns:
        dict[str, Any]: Filter parameters.
    """
    filter_params: dict[str, Any] = dict(
        tags=pipeline_config.get("tags"),
        from_nodes=pipeline_config.get("from_nodes"),
        to_nodes=pipeline_config.get("to_nodes"),
        node_names=pipeline_config.get("node_names"),
        from_inputs=pipeline_config.get("from_inputs"),
        to_outputs=pipeline_config.get("to_outputs"),
    )

    # Kedro 1.x renamed the namespace filter kwarg to `node_namespaces` (plural).
    # Maintain backward compatibility by switching the key based on the Kedro major version.
    if KEDRO_VERSION[0] >= 1:
        # Prefer explicit `node_namespaces` from config if present; otherwise map from `node_namespace`.
        filter_params["node_namespaces"] = pipeline_config.get("node_namespaces")
    else:  # pragma: no cover
        filter_params["node_namespace"] = pipeline_config.get("node_namespace")

    return filter_params


def get_mlflow_resource_from_config(mlflow_config: "BaseModel") -> dg.ResourceDefinition:
    """Create a Dagster resource definition from MLflow config.

    Args:
        mlflow_config (BaseModel): MLflow configuration.

    Returns:
        ResourceDefinition: Dagster resource definition for MLflow.
    """
    if mlflow_tracking is None:
        raise ImportError("dagster-mlflow is not installed. Please install it to use MLflow integration.")

    mlflow_resource = mlflow_tracking.configured({
        "experiment_name": mlflow_config.tracking.experiment.name,
        "mlflow_tracking_uri": mlflow_config.server.mlflow_tracking_uri,
        "parent_run_id": None,
    })

    return mlflow_resource
