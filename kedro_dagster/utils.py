"""Utility functions."""

from pathlib import Path
from typing import Any, Union


from jinja2 import Environment, FileSystemLoader
from dagster import get_dagster_logger

from kedro.config import MissingConfigException
from kedro.framework.context import KedroContext
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project


def kedro_init(
    project_path: Path,
    env: str,
):
    """Initialize a Kedro session and returns the DataCatalog and KedroSession.

    Args:
        pipeline_name (str): The name of the pipeline to initialize.
        project_path (Path): The path to the Kedro project.
        env (str): Kedro environment to load the catalog and the parameters from.

    Returns: A tuple containing the config loader, the data catalog, the kedro session
        ID, and the memory asset names.
    """
    logger = get_dagster_logger()

    # bootstrap project within task / flow scope
    logger.info("Bootstrapping project")
    bootstrap_project(project_path)

    session = KedroSession.create(
        project_path=project_path,
        env=env,
    )

    logger.info("Session created with ID %s", session.session_id)

    logger.info("Loading context...")
    context = session.load_context()
    config_loader = context.config_loader
    catalog = context.catalog

    return config_loader, catalog, session.session_id


def _load_config(context: KedroContext) -> dict[str, Any]:
    # Backwards compatibility for ConfigLoader that does not support `config_patterns`
    config_loader = context.config_loader
    if not hasattr(config_loader, "config_patterns"):
        return config_loader.get("dagster*", "dagster/**")  # pragma: no cover

    # Set the default pattern for `dagster` if not provided in `settings.py`
    if "dagster" not in config_loader.config_patterns.keys():
        config_loader.config_patterns.update(  # pragma: no cover
            {"dagster": ["dagster*", "dagster/**"]}
        )

    assert "dagster" in config_loader.config_patterns.keys()

    # Load the config
    try:
        return config_loader["dagster"]
    except MissingConfigException:
        # File does not exist
        return {}


def _get_pipeline_config(config_dagster: dict, params: dict, pipeline_name: str):
    dag_config = {}
    # Load the default config if specified
    if "default" in config_dagster:
        dag_config.update(config_dagster["default"])
    # Update with pipeline-specific config if present
    if pipeline_name in config_dagster:
        dag_config.update(config_dagster[pipeline_name])

    # Update with params if provided
    dag_config.update(params)
    return dag_config


def render_jinja_template(
    src: Union[str, Path], is_cookiecutter=False, **kwargs
) -> str:
    """This functions enable to copy a file and render the
    tags (identified by {{ my_tag }}) with the values provided in kwargs.

    Arguments:
        src {Union[str, Path]} -- The path to the template which should be rendered

    Returns:
        str -- A string that contains all the files with replaced tags.
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


def write_jinja_template(
    src: Union[str, Path], dst: Union[str, Path], **kwargs
) -> None:
    """Write a template file and replace tis jinja's tags
     (identified by {{ my_tag }}) with the values provided in kwargs.

    Arguments:
        src {Union[str, Path]} -- Path to the template which should be rendered
        dst {Union[str, Path]} -- Path where the rendered template should be saved
    """
    dst = Path(dst)
    parsed_template = render_jinja_template(src, **kwargs)
    with open(dst, "w") as file_handler:
        file_handler.write(parsed_template)