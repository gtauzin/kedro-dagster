from __future__ import annotations

import ast
from importlib.resources import files


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
