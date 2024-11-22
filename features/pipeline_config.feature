Feature: Pipeline Configuration Management
  As a user
  I want to manage pipeline configurations
  So that I can customize how individual pipelines run in Dagster

  Scenario: Loading default pipeline configuration
    Given I have a dagster configuration with default settings
    When I get the pipeline configuration for "my_pipeline"
    Then the configuration should contain the default settings

  Scenario: Loading pipeline-specific configuration
    Given I have a dagster configuration with pipeline-specific settings for "my_pipeline"
    When I get the pipeline configuration for "my_pipeline"
    Then the configuration should contain the pipeline-specific settings

  Scenario: Overriding configuration with parameters
    Given I have a dagster configuration with default settings
    And I have custom parameters
    When I get the pipeline configuration for "my_pipeline"
    Then the configuration should contain the overridden parameters

  Scenario: Converting pipeline to Dagster job
    Given I have a Kedro pipeline with multiple nodes
    When I convert the pipeline to a Dagster job
    Then the job should contain all pipeline nodes as assets
    And the job should have the correct configuration
