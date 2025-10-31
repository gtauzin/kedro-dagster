# mypy: ignore-errors

from __future__ import annotations

import ast
import importlib
import sys
from importlib.resources import files

import dagster as dg


def test_import_definitions_from_template_exec_filebacked_local(kedro_project_exec_filebacked_local, monkeypatch):
    """Import the `kedro_dagster.templates.definitions` template while inside a real
    Kedro project scenario and assert it exposes `default_executor` and `defs`.
    """

    project_opts = kedro_project_exec_filebacked_local
    project_path = project_opts.project_path

    # Ensure the template's runtime discovery picks up the test project
    monkeypatch.chdir(project_path)

    # Force re-import so the module executes in the context of the test project
    sys.modules.pop("kedro_dagster.templates.definitions", None)
    module = importlib.import_module("kedro_dagster.templates.definitions")

    assert hasattr(module, "default_executor"), "Template should define `default_executor`"
    assert hasattr(module, "defs"), "Template should define `defs` (Dagster Definitions)"

    assert isinstance(module.default_executor, dg.ExecutorDefinition)
    assert isinstance(module.defs, dg.Definitions)


def _has_translator_assignment(module: ast.Module) -> bool:
    for node in module.body:
        if isinstance(node, ast.Assign):
            # translator = ...
            if any(isinstance(t, ast.Name) and t.id == "translator" for t in node.targets):
                return True
    return False


def _calls_to_dagster(module: ast.Module) -> bool:
    for node in ast.walk(module):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr == "to_dagster":
                return True
    return False


def _builds_dg_definitions(module: ast.Module) -> bool:
    for node in ast.walk(module):
        if isinstance(node, ast.Call):
            func = node.func
            # dg.Definitions(...)
            if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                if func.value.id == "dg" and func.attr == "Definitions":
                    return True
            # Definitions(...) (fallback if imported differently)
            if isinstance(func, ast.Name) and func.id == "Definitions":
                return True
    return False


def test_definitions_template_ast_structure():
    """AST of definitions.py includes translator var, to_dagster() call, and dg.Definitions build."""
    code = (files("kedro_dagster") / "templates" / "definitions.py").read_text(encoding="utf-8")
    tree = ast.parse(code)

    assert _has_translator_assignment(tree), "Template should assign a 'translator' variable"
    assert _calls_to_dagster(tree), "Template should call translator.to_dagster()"
    assert _builds_dg_definitions(tree), "Template should build a dg.Definitions from translator output"
