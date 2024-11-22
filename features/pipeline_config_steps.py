from behave import given, when, then
from unittest.mock import MagicMock
from kedro.pipeline import Pipeline, Node
from kedro.io import DataCatalog, MemoryDataSet

@given('I have a dagster configuration with default settings')
def step_impl(context):
    context.config_dagster = {
        "default": {
            "config": {"execution": {"config": {"multiprocess": {}}}},
            "tags": {"default_tag": "value"},
        }
    }
    context.params = {}
    context.pipeline_name = "my_pipeline"

@given('I have a dagster configuration with pipeline-specific settings for "{pipeline_name}"')
def step_impl(context, pipeline_name):
    context.config_dagster = {
        "default": {
            "config": {"execution": {"config": {"multiprocess": {}}}},
            "tags": {"default_tag": "value"},
        },
        pipeline_name: {
            "config": {"execution": {"config": {"in_process": {}}}},
            "tags": {"pipeline_tag": "custom"},
        }
    }
    context.params = {}
    context.pipeline_name = pipeline_name

@given('I have custom parameters')
def step_impl(context):
    context.params = {
        "config": {"custom_param": "value"},
        "tags": {"custom_tag": "param_value"}
    }

@when('I get the pipeline configuration for "{pipeline_name}"')
def step_impl(context, pipeline_name):
    from utils import _get_pipeline_config
    context.result = _get_pipeline_config(
        context.config_dagster,
        context.params,
        pipeline_name
    )

@then('the configuration should contain the default settings')
def step_impl(context):
    assert "config" in context.result
    assert "tags" in context.result
    assert context.result["tags"]["default_tag"] == "value"

@then('the configuration should contain the pipeline-specific settings')
def step_impl(context):
    assert "config" in context.result
    assert "tags" in context.result
    assert context.result["tags"]["pipeline_tag"] == "custom"

@then('the configuration should contain the overridden parameters')
def step_impl(context):
    assert "config" in context.result
    assert "custom_param" in context.result["config"]
    assert context.result["tags"]["custom_tag"] == "param_value"

@given('I have a Kedro pipeline with multiple nodes')
def step_impl(context):
    def dummy_func(x):
        return x

    context.pipeline = Pipeline([
        Node(
            func=dummy_func,
            inputs={"x": "input_dataset"},
            outputs="intermediate_dataset",
            name="node1"
        ),
        Node(
            func=dummy_func,
            inputs={"x": "intermediate_dataset"},
            outputs="output_dataset",
            name="node2"
        )
    ])
    
    context.job_name = "test_job"
    context.job_config = {
        "config": {"execution": {"config": {"multiprocess": {}}}},
        "description": "Test job",
        "tags": {"job_tag": "test"}
    }

@when('I convert the pipeline to a Dagster job')
def step_impl(context):
    from jobs import get_job_from_pipeline
    context.job = get_job_from_pipeline(
        context.pipeline,
        context.job_name,
        context.job_config
    )

@then('the job should contain all pipeline nodes as assets')
def step_impl(context):
    # Note: The actual implementation might need to be adjusted based on how
    # your Dagster job object is structured
    assert context.job is not None
    
    # Verify that the job contains assets for all pipeline nodes
    expected_assets = ["node1", "node2", "intermediate_dataset", "output_dataset"]
    for asset in expected_assets:
        assert asset in str(context.job)

@then('the job should have the correct configuration')
def step_impl(context):
    # Verify job configuration
    assert context.job.config == context.job_config.get("config")
    assert context.job.description == context.job_config.get("description")
    assert context.job.tags == context.job_config.get("tags")
