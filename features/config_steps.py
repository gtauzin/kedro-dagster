from behave import given, when, then
from unittest.mock import patch, MagicMock

@given("there is no dagster.yml file")
def step_impl(context):
    context.config_loader = MagicMock()
    context.config_loader.side_effect = MissingConfigException()

@when("I load the dagster configuration")
def step_impl(context):
    with patch('logging.Logger.warning') as mock_warning:
        context.warning = mock_warning
        context.config = get_dagster_config(context.config_loader)

@then("I should get default configuration values")
def step_impl(context):
    assert context.config.dev.log_level == "info"
    assert context.config.dev.port == "3000"
    assert context.config.dev.host == "127.0.0.1"

@then("a warning message should be logged")
def step_impl(context):
    context.warning.assert_called_once()
