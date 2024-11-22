Feature: Dagster Configuration Management
  As a user
  I want to manage Dagster configurations
  So that I can customize how my Kedro pipelines run in Dagster

  Scenario: Loading default configuration when no dagster.yml exists
    Given there is no dagster.yml file
    When I load the dagster configuration
    Then I should get default configuration values
    And a warning message should be logged

  Scenario: Loading custom configuration from dagster.yml
    Given there is a dagster.yml file with custom settings
    When I load the dagster configuration
    Then I should get the custom configuration values
