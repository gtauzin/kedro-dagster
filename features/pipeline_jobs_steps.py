from behave import given, when, then
from unittest.mock import MagicMock, patch
from kedro.pipeline import Pipeline, Node
from dagster import AssetSelection, define_asset_job

@given("I have a pipeline with a single node")
def step_impl(context):
    # Create a mock node
    node = MagicMock()
    node.name = "test_node"
    node.outputs = []
    
    # Create a mock pipeline
    context.pipeline = MagicMock(spec=Pipeline)
    context.pipeline.nodes = [node]
    context.job_name = "test_job"
    context.job_config = {}

@given("I have a pipeline with multiple output nodes")
def step_impl(context):
    # Create mock nodes with multiple outputs
    node1 = MagicMock()
    node1.name = "node1"
    node1.outputs = ["output1", "output2"]
    
    node2 = MagicMock()
    node2.name = "node2"
    node2.outputs = ["output3"]
    
    context.pipeline = MagicMock(spec=Pipeline)
    context.pipeline.nodes = [node1, node2]
    context.job_name = "multi_output_job"
    context.job_config = {}

@given("I have a pipeline with custom configuration")
def step_impl(context):
    node = MagicMock()
    node.name = "test_node"
    node.outputs = ["output1"]
    
    context.pipeline = MagicMock(spec=Pipeline)
    context.pipeline.nodes = [node]
    context.job_name = "custom_config_job"
    
    # Convert table data to config dictionary
    context.job_config = {}
    for row in context.table:
        # Handle nested dictionary strings (assuming JSON-like strings)
        if row['config_value'].startswith('{'):
            import json
            value = json.loads(row['config_value'])
        else:
            value = row['config_value']
        context.job_config[row['config_key']] = value

@when("I convert the pipeline to a job")
@when("I convert the pipeline to a job with name {job_name}")
def step_impl(context, job_name=None):
    if job_name:
        context.job_name = job_name
    
    with patch('dagster.AssetSelection.assets') as mock_asset_selection, \
         patch('dagster.define_asset_job') as mock_define_job:
        
        # Store mocks for later assertions
        context.mock_asset_selection = mock_asset_selection
        context.mock_define_job = mock_define_job
        
        # Call the actual function
        from jobs import get_job_from_pipeline
        context.job = get_job_from_pipeline(
            context.pipeline,
            context.job_name,
            context.job_config
        )

@when("I convert the pipeline to a job with the custom config")
def step_impl(context):
    context.execute_steps("When I convert the pipeline to a job")

@then("the job should be created with the correct asset")
def step_impl(context):
    # Verify AssetSelection.assets was called with the correct asset
    context.mock_asset_selection.assert_called_once_with("test_node")
    
    # Verify define_asset_job was called with correct parameters
    context.mock_define_job.assert_called_once()
    args, kwargs = context.mock_define_job.call_args
    assert args[0] == context.job_name

@then("the job should contain all output assets")
def step_impl(context):
    expected_assets = ["output1", "output2", "output3"]
    
    # Verify all outputs were included in asset selection
    assets_call_args = context.mock_asset_selection.call_args[0]
    for asset in expected_assets:
        assert asset in assets_call_args

@then("the job should have the correct name")
def step_impl(context):
    args, kwargs = context.mock_define_job.call_args
    assert args[0] == "multi_output_job"

@then("the job should have default configuration")
def step_impl(context):
    _, kwargs = context.mock_define_job.call_args
    assert kwargs.get('config') is None
    assert kwargs.get('description') is None
    assert kwargs.get('tags') is None
    assert kwargs.get('run_tags') is None

@then("the job should have the custom configuration applied")
def step_impl(context):
    _, kwargs = context.mock_define_job.call_args
    
    # Verify each custom config value was passed to define_asset_job
    for key, value in context.job_config.items():
        assert kwargs.get(key) == value
