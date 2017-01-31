Feature: "Index Update" command
  As a Geogig User
  I want to update indexes in a repository

  Scenario: Update the index on a tree

  Scenario: I try to update the index without specifying a tree

  Scenario: Update the index for an attribute

  Scenario: I try to update the index leaving the attribute param empty

  Scenario: I try to update the index for a non-existent attribute

  Scenario: Update the index for an extra-attribute

  Scenario: I try to update the index leaving the extra-attribute param empty

  Scenario: I try to update the index for a non-existent extra-attribute

  Scenario: Update the index, overwrite existing extra-attribute in the index

  Scenario: I try to overwrite a non-existent extra-attribute

  Scenario: I try to overwrite an extra-attribute with a incorrect param

  Scenario: Update the index, add another attribute to the index

  Scenario: I try to add a non-existent attribute to the index

  Scenario: Update the index for the full history

  Scenario: I try to update the index for full-history when there is only one commit

  Scenario: I try to update the index in an empty repository
