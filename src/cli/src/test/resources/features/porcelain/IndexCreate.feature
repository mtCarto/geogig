Feature: "Index Create" command
  As a Geogig User
  I want to create indexes in a repository

  Scenario: Create an index on a tree
    Given I have a repository
    And   I have several commits
    When  I create an index on Points
    And   I list the index Points
    Then  the response should contain variable


  Scenario: I try to create an index without specifying a tree

  Scenario: I try to create an index with an incorrect tree specified

  Scenario: Create an index on a tree and attribute

  Scenario: I try to create an index with an incorrect attribute specified

  Scenario: I try to create an index with an empty attribute param

  Scenario: Create an index on the full history of a repository

  Scenario: I try to create a full history index on an empty repository

  Scenario: I try to create a full history index on a repository with a single commit

  Scenario: Create an index on a tree with an extra attribute (time)

  Scenario: I try to create an index with an incorrect extra attribute

  Scenario: I try to create an index on a tree with an empty extra-attribute param





