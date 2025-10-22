from __future__ import annotations

import ast
from importlib.resources import files


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
    code = (files("kedro_dagster") / "templates" / "definitions.py").read_text(encoding="utf-8")
    tree = ast.parse(code)

    assert _has_translator_assignment(tree), "Template should assign a 'translator' variable"
    assert _calls_to_dagster(tree), "Template should call translator.to_dagster()"
    assert _builds_dg_definitions(tree), "Template should build a dg.Definitions from translator output"
