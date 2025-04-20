@dagster-cli
Feature: dagster commands in new projects
  Background:
    Given I have prepared a config file
    And I run a non-interactive kedro new using spaceflights-pandas starter
    And I have installed the project dependencies

  Scenario: Execute dagster init
    When I execute the kedro command "dagster init"
    Then I should get a successful exit code
    And A dagster.yml file should exist
    And A definitions.py file should exist

  Scenario: Execute dagster dev
    When I execute the kedro command "dagster dev --env local ---port 3030 --host 127.0.0.1"
    Then I should get a successful exit code
    And the dagster UI should be served on "127.0.0.1:3030"
