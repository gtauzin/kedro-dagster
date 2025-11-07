from __future__ import annotations

import ast
from importlib.resources import files

from kedro_dagster.utils import render_jinja_template


def _read_text_from_package(relative_path: str) -> str:
    # Resolve path relative to the installed package root to avoid relying on
    # the `templates` directory being a Python package.
    pkg_root = files("kedro_dagster")
    path = pkg_root.joinpath(*relative_path.split("/"))
    assert path.is_file(), f"Missing template file: {relative_path}"
    return path.read_text(encoding="utf-8")


def test_definitions_template_is_valid_python_and_contains_expected_constructs():
    """definitions.py template parses and includes expected translator/definitions snippets."""
    code = _read_text_from_package("templates/definitions.py")

    # Check it parses as valid Python (without executing it)
    ast.parse(code)

    # Light content checks to ensure expected objects are present in the template
    expected_snippets = [
        "from kedro_dagster import KedroProjectTranslator",
        "translator = KedroProjectTranslator(",
        "translator.to_dagster()",
        "dg.Definitions(",
        "io_manager",
        "multiprocess_executor",
        "default_executor =",
    ]
    for snippet in expected_snippets:
        assert snippet in code, f"Template missing expected snippet: {snippet!r}"


def test_dg_toml_template_placeholders_and_renders(tmp_path):
    """dg.toml template includes expected placeholders and renders with provided variables."""
    # Read raw template text
    text = _read_text_from_package("templates/dg.toml")

    # Contains expected placeholders
    assert "root_module = {{ project_name }}" in text
    assert 'code_location_target_module = "{{ project_name }}.definitions"' in text
    assert 'code_location_name = "{{ package_name }}"' in text
    assert 'defs_module = "kedro_dagster_example"' in text

    # Render using utils to ensure Jinja processing is valid
    pkg_root = files("kedro_dagster")
    template_path = pkg_root.joinpath("templates", "dg.toml")
    rendered = render_jinja_template(str(template_path), project_name="my_pkg", package_name="My Project")

    # Check replacements happened
    assert "root_module = my_pkg" in rendered
    assert 'code_location_target_module = "my_pkg.definitions"' in rendered
    assert 'code_location_name = "My Project"' in rendered
    # Ensure defs_module remains the constant used by the integration
    assert 'defs_module = "kedro_dagster_example"' in rendered
