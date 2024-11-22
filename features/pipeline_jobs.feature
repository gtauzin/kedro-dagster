Feature: Pipeline to Dagster Job Conversion
  As a developer
  I want to convert Kedro pipelines to Dagster jobs
  So that I can run my pipelines in the Dagster environment

  Scenario: Converting a simple pipeline to a job
    Given I have a pipeline with a single node
    When I convert the pipeline to a job
    Then the job should be created with the correct asset
    And the job should have default configuration

  Scenario: Converting a pipeline with multiple outputs to a job
    Given I have a pipeline with multiple output nodes
    When I convert the pipeline to a job with name "multi_output_job"
    Then the job should contain all output assets
    And the job should have the correct name

  Scenario: Converting a pipeline with custom configuration
    Given I have a pipeline with custom configuration
      | config_key     | config_value          |
      | description    | My custom description |
      | tags           | {"env": "prod"}       |
      | run_tags       | {"version": "1.0"}    |
    When I convert the pipeline to a job with the custom config
    Then the job should have the custom configuration applied
