.. _crate_standard_sql:

=================
SQL compatibility
=================

CrateDB aims to provide a `SQL implementation`_ that is familiar to anyone who
has used databases that provide a standards-compliant SQL language. However,
you should be aware of some unique characteristics in CrateDB's SQL dialect.

.. rubric:: Table of contents

.. contents::
   :local:


Implementation notes
====================


Data types
----------

CrateDB supports a set of primitive data types. The following table defines
how data types of `standard SQL`_ map to CrateDB :ref:`data-types`.

+-----------------------------------+-----------------------------+
| Standard SQL                      | CrateDB                     |
+===================================+=============================+
| integer                           | integer, int, int4          |
+-----------------------------------+-----------------------------+
| bit[8]                            | byte, char                  |
+-----------------------------------+-----------------------------+
| boolean, bool                     | boolean                     |
+-----------------------------------+-----------------------------+
| char [(n)], varchar [(n)]         | string, text, varchar,      |
|                                   | character varying           |
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

:ref:`sql-create-table` supports additional storage and table parameters for
sharding, replication and routing of data, and does not support inheritance.


Alter table
-----------

``ALTER COLUMN`` and ``DROP COLUMN`` actions are not currently supported (see
:ref:`sql-alter-table`).


System information tables
-------------------------

The read-only :ref:`system-information` and :ref:`information_schema` tables
have a slightly different schema than specified in standard SQL. They provide
schema information and can be queried to get real-time statistical data about
the cluster, its nodes, and their shards.


BLOB support
------------

`Standard SQL`_ defines a binary string type, called ``BLOB`` or ``BINARY LARGE
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

These **features** of `standard SQL`_ are not supported:

- Stored procedures

- Triggers

  - ``WITH`` Queries (Common Table Expressions)

- Sequences

- Inheritance

- Constraints

  - Unique

  - Foreign key

  - Exclusion constraints

These **functions** of `standard SQL`_ are either not supported or only partly supported:

- Aggregate functions

  - Various functions available (see :ref:`aggregation`)

- Window functions

  - Various functions available (see :ref:`window-functions`)

- ``ENUM`` support functions

- ``IS DISTINCT FROM``

- Network address functions and :ref:`operators <gloss-operator>`

- Mathematical functions

  - Certain functions supported (see :ref:`mathematical_functions`)

- Set returning functions

- Trigger functions

- XML functions

**Note**: The currently supported and unsupported features in CrateDB are
exposed in the :ref:`information_schema` table (see :ref:`sql_features` for
usage).

CrateDB also supports the `PostgreSQL wire protocol`_.

If you have use cases for any missing features, functions, or dialect
improvements, let us know on `Github`_! We are always improving and extending
CrateDB and would love to hear your feedback.


.. _Github: https://github.com/crate/crate
.. _PostgreSQL wire protocol: https://crate.io/docs/crate/reference/en/latest/interfaces/postgres.html
.. _SQL implementation: https://crate.io/docs/crate/reference/en/latest/appendices/compliance.html
.. _standard SQL: https://crate.io/docs/crate/reference/en/4.2/appendices/compliance.html
