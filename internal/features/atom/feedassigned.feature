@feedidassigned
Feature: Feed id assigned
  Scenario:
    Given some initial events and no feeds
    When the feed page threshold is reached
    Then feed is updated with a new feedid with a null previous feed
    And the recent items with a null id are updated with the feedid

  Scenario:
    Given some initial events and some feeds
    When the feed page threshold is reached again
    Then feed is updated with a new feedid with the previous feed id as previous
    And the most recent items with a null id are updated with the new feedid