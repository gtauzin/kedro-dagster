from behave import given, when, then
from unittest.mock import patch, MagicMock
import subprocess

@given("I am in a Kedro project directory")
def step_impl(context):
    # Mock the project directory detection
    context.project_path = MagicMock()
    context.project_path.exists.return_value = True

@when("I run the dagster dev command")
def step_impl(context):
    with patch('subprocess.call') as mock_call:
        context.subprocess_call = mock_call
        # Call your CLI function here
        # You'll need to import and call the actual function

@then("the dagster development server should start")
def step_impl(context):
    context.subprocess_call.assert_called_once()
    args = context.subprocess_call.call_args[0][0]
    assert "dagster" in args
    assert "dev" in args

@then("it should use default configuration values")
def step_impl(context):
    args = context.subprocess_call.call_args[0][0]
    # Verify default values in the command arguments
    assert "--log-level" in args
    assert "info" in args
    assert "--port" in args
    assert "3000" in args
