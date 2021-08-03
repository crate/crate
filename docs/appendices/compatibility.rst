.. _appendix-compatibility:

=================
SQL compatibility
=================

CrateDB provides a :ref:`standards-based <sql_supported_features>` SQL
implementation similar to many other SQL databases. In particular, CrateDB aims
for compatibility with :ref:`PostgreSQL <interface-postgresql>`. However,
CrateDB's SQL dialect does have some unique characteristics, documented on this
page.

.. SEEALSO::

    :ref:`SQL: Syntax reference <sql>`

.. rubric:: Table of contents

.. contents::
   :local:


.. _appendix-compat-notes:

Implementation notes
====================


.. _appendix-compat-data-types:

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


.. _appendix-compat-create-table:

Create table
------------

:ref:`sql-create-table` supports additional storage and table parameters for
sharding, replication and routing of data, and does not support inheritance.


.. _appendix-compat-alter-table:

Alter table
-----------

``ALTER COLUMN`` and ``DROP COLUMN`` actions are not currently supported (see
:ref:`sql-alter-table`).


.. _appendix-compat-sys-info:

System information tables
-------------------------

The read-only :ref:`system-information` and :ref:`information_schema` tables
have a slightly different schema than specified in standard SQL. They provide
schema information and can be queried to get real-time statistical data about
the cluster, its nodes, and their shards.


.. _appendix-compat-blob:

BLOB support
------------

`Standard SQL`_ defines a binary string type, called ``BLOB`` or ``BINARY LARGE
OBJECT``. With CrateDB, Binary Data is instead stored in separate BLOB Tables
(see :ref:`blob_support`) which can be sharded and replicated.


.. _appendix-compat-transactions:

Transactions (``BEGIN``, ``START``, ``COMMIT``, and ``ROLLBACK``)
-----------------------------------------------------------------

CrateDB is focused on providing analytical capabilities over supporting
traditional transactional use cases, and thus it does not provide transaction
control. Every statement commits immediately and is replicated within the
cluster.

However, every row in CrateDB has a version number that is incremented whenever
the record is modified. This version number can be used to implement patterns
like :ref:`sql_occ`, which can be used to solve many of the use cases that
would otherwise require traditional transactions.


.. _appendix-compat-unsupported:

Unsupported features and functions
==================================

These *features* of `standard SQL`_ are not supported:

- Stored procedures

- Triggers

  - ``WITH`` Queries (Common Table Expressions)

- Sequences

- Inheritance

- Constraints

  - Unique

  - Foreign key

  - Exclusion constraints

These *functions* of `standard SQL`_ are either not supported or only partly
supported:

- :ref:`Aggregate functions <aggregation-functions>`

  - Various functions available (see :ref:`aggregation`)

- :ref:`Window functions <window-functions>`

  - Various functions available (see :ref:`window-functions`)

- ``ENUM`` support functions

- ``IS DISTINCT FROM``

- Network address functions and :ref:`operators <gloss-operator>`

- Mathematical functions

  - Certain functions supported (see :ref:`scalar-math`)

- Set returning functions

- Trigger functions

- XML functions

.. NOTE::

    The currently supported and unsupported features in CrateDB are exposed in
    the :ref:`information_schema` table (see :ref:`sql_features` for usage).

CrateDB also supports the `PostgreSQL wire protocol`_.

If you have use cases for any missing features, :ref:`functions
<gloss-function>`, or dialect improvements, let us know on `Github`_! We are
always improving and extending CrateDB and would love to hear your feedback.


.. _Github: https://github.com/crate/crate
.. _PostgreSQL wire protocol: https://crate.io/docs/crate/reference/en/latest/interfaces/postgres.html
.. _SQL implementation: https://crate.io/docs/crate/reference/en/latest/appendices/compliance.html
.. _standard SQL: https://crate.io/docs/crate/reference/en/4.2/appendices/compliance.html
