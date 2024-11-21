"""Utility functions."""

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, ConfigDict, create_model


def render_jinja_template(src: str | Path, is_cookiecutter=False, **kwargs) -> str:
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


def write_jinja_template(src: str | Path, dst: str | Path, **kwargs) -> None:
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


def _include_mlflow():
    try:
        import dagster_mlflow  # noqa: F401
        import kedro_mlflow  # noqa: F401
        import mlflow  # noqa: F401
    except ImportError:
        return False
    return True


def _create_pydantic_model_from_dict(
    params: dict[str, Any], __base__, __config__: ConfigDict | None = None
) -> type[BaseModel]:
    fields = {}
    for param_name, param_value in params.items():
        if isinstance(param_value, dict):
            # Recursively create a nested model for nested dictionaries
            nested_model = _create_pydantic_model_from_dict(param_value, __base__=__base__, __config__=__config__)
            # TODO: Nested __base__? Yes for NodeParams, no for IOManagers?

            fields[param_name] = (nested_model, ...)
        else:
            # Use the type of the value as the field type
            fields[param_name] = (type(param_value), param_value)

    if __base__ is None:
        model = create_model("ParametersConfig", __config__=__config__, **fields)
    else:
        model = create_model("ParametersConfig", __base__=__base__, **fields)
        model.config = __config__

    return model
