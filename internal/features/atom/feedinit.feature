@feedinit
Feature: Feed initialization
  Scenario:
    Given a new feed environment
    When we start up the feed processor
    And some events are published
    And the number of events is lower than the feed threshold
    Then the events are stored in the atom_event table with a null feed id
    And there are no records in the feed table