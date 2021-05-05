---
name: Bug report
about: Create a report to help us improve
labels: triage
body:

- type: input
  id: cratedb-version
  attributes:
    label: CrateDB version
  validations:
    required: true

- type: textarea
  id: summary
  attributes:
    label: Summary of the problem

- type: textarea
  id: expected
  attributes:
    label: Expected behavior

- type: textarea
  id: actual
  attributes:
    label: Actual behavior
- type: markdown
  attributes:
    value: |
      Please provide exact error messages, include relevant logs and, if possible, provide stacktraces (Activate `\verbose` in [crash](https://github.com/crate/crash/))

- type: textarea
  id: reproduce
  attributes:
    label: Steps to reproduce the problem
- type: markdown
  attributes:
    value: |
      Please include information like the schema of your tables.
  

- type: textarea
  id: environment
  attributes:
    label: Additional information about the environment

- type: markdown
  attributes:
    value: |
      Please include information like:
        - Number of nodes
        - Platform and Distribution
        - Kernel version
        - If using Docker and the version

---
