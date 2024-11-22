Feature: Dagster CLI Commands
  As a user
  I want to run dagster development server
  So that I can interact with my Kedro pipelines through Dagster UI

  Scenario: Running dagster dev command with default settings
    Given I am in a Kedro project directory
    When I run the dagster dev command
    Then the dagster development server should start
    And it should use default configuration values
      | setting             | value      |
      | log_level          | info       |
      | log_format         | colored    |
      | port               | 3000       |
      | host              | 127.0.0.1  |
      | live_data_poll_rate| 2000       |

  Scenario: Running dagster dev command with custom settings
    Given I am in a Kedro project directory
    When I run the dagster dev command with parameters:
      | parameter           | value      |
      | log_level          | debug      |
      | log_format         | json       |
      | port               | 4000       |
      | host              | localhost  |
      | live_data_poll_rate| 1000       |
    Then the dagster development server should start with custom settings
