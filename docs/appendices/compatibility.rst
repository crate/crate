.. _crate_standard_sql:

=============
Compatibility
=============

CrateDB aims to provide an SQL implementation that is familiar to anyone having
used other databases providing a standards-compliant SQL language. However, it
is worth being aware of some unique characteristics in CrateDB's SQL dialect.

.. rubric:: Table of contents

.. contents::
   :local:

Implementation notes
====================

Data types
----------

CrateDB supports a set of primitive data types. The following table defines
how data types of standard SQL map to CrateDB :ref:`data-types`.

+-----------------------------------+-----------------------------+
| Standard SQL                      | CrateDB                     |
+===================================+=============================+
| integer                           | integer, int, int4          |
+-----------------------------------+-----------------------------+
| bit[8]                            | byte, char                  |
+-----------------------------------+-----------------------------+
| boolean, bool                     | boolean                     |
+-----------------------------------+-----------------------------+
| char [(n)], varchar [(n)]         | string, text                |
+-----------------------------------+-----------------------------+
| timestamp with time zone          | timestamp with time zone,   |
|                                   | timestamptz                 |
+-----------------------------------+-----------------------------+
| timestamp                         | timestamp without time zone |
+-----------------------------------+-----------------------------+
| smallint                          | short, int2, smallint       |
+-----------------------------------+-----------------------------+
| bigint                            | long, bigint, int8          |
+-----------------------------------+-----------------------------+
| real                              | float, real                 |
+-----------------------------------+-----------------------------+
| double precision                  | double, double precision    |
+-----------------------------------+-----------------------------+

Create table
------------

:ref:`ref-create-table` supports additional storage and table parameters for
sharding, replication and routing of the data, and does not support
inheritance.

Alter table
-----------

``ALTER COLUMN`` and ``DROP COLUMN`` actions are not currently supported (see
:ref:`ref-alter-table`).

System information tables
-------------------------

The read-only :ref:`system-information` and :ref:`information_schema` tables
have a slightly different schema than specified in standard SQL. They provide
schema information and can be queried to get real-time statistical data about
the cluster, its nodes, and their shards.

BLOB support
------------

Standard SQL defines a binary string type, called ``BLOB`` or ``BINARY LARGE
OBJECT``. With CrateDB, Binary Data is instead stored in separate BLOB Tables
(see :ref:`blob_support`) which can be sharded and replicated.

Transactions (``BEGIN``, ``COMMIT``, and ``ROLLBACK``)
------------------------------------------------------

CrateDB is focused on providing analytical capabilities over supporting
traditional transactional use cases, and thus it does not provide transaction
control. Every statement commits immediately and is replicated within the
cluster.

However, every row in CrateDB has a version number that is incremented whenever
the record is modified. This version number can be used to implement patterns
like :ref:`sql_occ`, which can be used to solve many of the use cases that
would otherwise require traditional transactions.

Unsupported features and functions
==================================

These **features** of standard SQL are not supported:

- Stored Procedures

- Triggers

- ``VALUES`` list used as constant tables

- ``WITH`` Statements

- Sequences

- Inheritance

- Constraints

  - Unique

  - Foreign key

  - Check constraints

  - Exclusion constraints

These **functions** are either not supported or only partly supported:

- Aggregate functions

  - Various functions available (see :ref:`aggregation`)

- Window Functions

  - Various functions available (see :ref:`window-functions`)

- ``ENUM`` support functions

- ``IS DISTINCT FROM``

- Network address functions and operators

- Mathematical functions

  - Certain functions supported (see :ref:`mathematical_functions`)

- Set returning functions

- Trigger functions

- XML functions

The currently supported and unsupported features in CrateDB are exposed in the
:ref:`information_schema` table (see :ref:`sql_features` for usage).

If you are missing features, functions or dialect improvements and have a great
use case for it, let us know on `Github`_. We're always improving and extending
CrateDB, and we love to hear feedback.

.. _Github: https://github.com/crate/crate
