====================
Documentation README
====================

Top-Level Structure
===================

The documentation is structured as follows:

+------------------------+----------------------------------------------------+
| Section Title          | Description                                        |
+========================+====================================================+
| Running CrateDB        | Documentation for the ``crate`` command.           |
+------------------------+----------------------------------------------------+
| Configuration          | Full documentation for configuration settings,     |
|                        | split into sections by topic.                      |
+------------------------+----------------------------------------------------+
| General Use            | Primary documentation for any feature primarily of |
|                        | interest to a general user, split into sections by |
|                        | topic.                                             |
+------------------------+----------------------------------------------------+
| Administration         | Primary documentation for any feature primarily of |
|                        | interest to a database administrator, split into   |
|                        | sections by topic.                                 |
+------------------------+----------------------------------------------------+
| SQL Syntax             | Full documentation for SQL syntax, split           |
|                        | into general SQL topics and SQL statements, one    |
|                        | document per statement.                            |
+------------------------+----------------------------------------------------+
| Client Interfaces      | Documentation for client interfaces, split         |
|                        | into documents by interface.                       |
+------------------------+----------------------------------------------------+
| Enterprise Features    | A list of each enterprise feature, with a link     |
|                        | to the primary documentation.                      |
+------------------------+----------------------------------------------------+
| Appendix               | Supplementary information.                         |
|                        |                                                    |
|                        | This also includes collection of release notes,    |
|                        | one document per release. Each document should     |
|                        | contain upgrade information as well as a list of   |
|                        | changes.                                           |
+------------------------+----------------------------------------------------+

How to Document a Feature
=========================

A single feature may have three types of documentation:

- Primary documentation
- Configuration documentation
- SQL syntax documentation

Primary documentation is located under one of the following sections:

- *CrateDB SQL*
- *Administration*
- *Plugins*

Primary documentation should cover:

- What the feature does
- How the feature can be used

Primary documentation should not include a full configuration settings
reference or a full SQL syntax reference. Instead, full references should be
added to *Configuration* or *SQL Syntax* sections.

When a feature is documented in more than one place, those locations should be
interlinked.

Enterprise features must be documented (with an explanatory note) alongside
standard features in the appropriate section of the documentation. The
*Enterprise Features* page must then be updated to link through to the primary
feature documentation.
