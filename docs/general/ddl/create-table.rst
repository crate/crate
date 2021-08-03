.. _ddl-create-table:

===============
Creating tables
===============

Tables are the basic building blocks of a relational database. A table can hold
multiple rows (i.e., records), with each row having multiple columns and each
column holding a single data element (i.e., value). You can :ref:`query <dql>`
tables to :ref:`insert data <dml-inserting-data>`, :ref:`select <sql_dql_queries>`
(i.e., retrieve) data, and :ref:`delete data <dml-deleting-data>`.

.. rubric:: Table of contents

.. contents::
   :local:


.. _ddl-create-table-definition:

Table definition
================

To create a table, use the :ref:`sql-create-table` :ref:`statement
<gloss-statement>`.

At a minimum, you must specify a table name and one or more column
definitions. A column definition must specify a column name and a corresponding
:ref:`data type <data-types>`.

Here's an example statement::

    cr> CREATE TABLE my_table (
    ...   first_column integer,
    ...   second_column text
    ... );
    CREATE OK, 1 row affected (... sec)

This statement creates a table named ``my_table`` with two columns named
``first_column`` and ``second_column`` with types :ref:`integer
<type-numeric>` and :ref:`text <type-text>`.

A table can be dropped (i.e., deleted) by using the :ref:`drop-table`
statement::

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)

If the ``my_table`` table did not exist, the ``DROP TABLE`` statement above
would return an error message. If you specify the ``IF EXISTS`` clause, the
instruction is conditional on the table's existence and would not return an
error message::

    cr> DROP TABLE IF EXISTS my_table;
    DROP OK, 0 rows affected (... sec)

.. TIP::

    By default, CrateDB will enforce the column definitions you specified with
    the ``CREATE TABLE`` statement (what's known as a *strict* :ref:`column
    policy <column_policy>`).

    However, you can configure the :ref:`column_policy <column_policy>` table
    parameter so that the :ref:`INSERT <sql-insert>`, :ref:`UPDATE
    <ref-update>`, and :ref:`COPY FROM <sql-copy-from>` statements can
    arbitrarily create new columns as needed (what's known as a *dynamic*
    column policy).


.. _ddl-create-table-schemas:

Schemas
-------

Tables can be created in different schemas. These are created implicitly on
table creation and cannot be created explicitly. If a schema did not exist yet,
it will be created.

You can create a table called ``my_table`` in a schema called ``my_schema``
schema like so::

    cr> create table my_schema.my_table (
    ...   pk int primary key,
    ...   label text,
    ...   position geo_point
    ... );
    CREATE OK, 1 row affected (... sec)

We can confirm this by looking up this table in the
:ref:`information_schema.tables <information_schema_tables>` table::

    cr> select table_schema, table_name from information_schema.tables
    ... where table_name='my_table';
    +--------------+------------+
    | table_schema | table_name |
    +--------------+------------+
    | my_schema    | my_table   |
    +--------------+------------+
    SELECT 1 row in set (... sec)

The following schema names are reserved and may not be used:

- ``blob``
- ``information_schema``
- ``sys``

.. TIP::

   Schemas are primarily namespaces for tables. You can use :ref:`privileges
   <administration-privileges>` to control access to schemas.

A user-created schema exists as long as there are tables with the same schema
name. If the last table with that schema is dropped, the schema is gone (except
for the ``blob`` and ``doc`` schema)::

    cr> drop table my_schema.my_table ;
    DROP OK, 1 row affected (... sec)

Every table that is created without an explicit schema name, will be created in
the ``doc`` schema::

    cr> create table my_doc_table (
    ...   a_column char,
    ...   another_one geo_point
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> select table_schema, table_name from information_schema.tables
    ... where table_name='my_doc_table';
    +--------------+--------------+
    | table_schema | table_name   |
    +--------------+--------------+
    | doc          | my_doc_table |
    +--------------+--------------+
    SELECT 1 row in set (... sec)

.. Hidden: drop tables::

    cr> drop table my_doc_table;
    DROP OK, 1 row affected (... sec)


.. _ddl-create-table-naming:

Naming restrictions
-------------------

Table, schema and column identifiers cannot have the same names as reserved key
words. Please refer to the :ref:`sql_lexical` section for more information
about naming.

Additionally, table and schema names are restricted in terms of characters and
length. They:

  - may not contain one of the following characters: ``\ / * ? " < > |
    <whitespace> , # .``

  - may not contain upper case letters

  - may not *start* with an underscore: ``_``

  - should not exceed 255 bytes when encoded with ``utf-8`` (this
    limit applies on the optionally schema-qualified table name)

Column names are restricted in terms of patterns:

  - Columns are not allowed to contain a dot (``.``), since this conflicts
    with internal path definitions.

  - Columns that conflict with the naming scheme of
    :ref:`virtual system columns <sql_administration_system_columns>` are
    restricted.

  - Character sequences that conform to the
    :ref:`subscript notation <sql_dql_object_arrays>` (e.g. ``col['id']``) are
    not allowed.


.. _ddl-create-table-configuration:

Table configuration
===================

You can configure tables in many different ways to take advantage of the range
of functionality that CrateDB supports. For example:

.. rst-class:: open

- CrateDB transparently segments the underlying storage of table data into
  :ref:`shards <ddl-sharding>` (four by default). You can configure the number
  of shards with the :ref:`CLUSTERED BY <sql-create-table-clustered>`
  clause. You control how CrateDB routes table rows to shards by specifying a
  :ref:`routing column <gloss-routing-column>`.

  You can use :ref:`cluster settings <conf_routing>` to configure how shards
  are :ref:`balanced <conf-routing-allocation-balance>` across a cluster and
  :ref:`allocated <ddl_shard_allocation>` to nodes (with :ref:`attribute-based
  allocation <conf-routing-allocation-attributes>`, :ref:`disk-based allocation
  <conf-routing-allocation-disk>`, or both).

  .. SEEALSO::

      `How-to guides: Tuning sharding performance`_

- You can :ref:`replicate <ddl-replication>` shards :ref:`WITH
  <sql-create-table-with>` the :ref:`number_of_replicas
  <sql-create-table-number-of-replicas>` table setting. CrateDB will split
  replicated partitions into primary shards, with each primary shard having one
  or more replica shards.

  When you lose a primary shard (e.g., due to node failure), CrateDB will
  promote a replica shard to primary. More table replicas mean a smaller chance
  of permanent data loss (through increased `data redundancy`_) in exchange for
  more disk space utilization and intra-cluster network traffic.

  Replication can also improve read performance because any increase in the
  number of shards distributed across a cluster also increases the
  opportunities for CrateDB to `parallelize`_ query execution across multiple
  nodes.

- You can :ref:`partition <partitioned-tables>` a table into one or more
  partitions with the :ref:`PARTITIONED BY <sql-create-table-partitioned-by>`
  clause. You control how tables are partitioned by specifying one or more
  :ref:`partition columns <gloss-partition-column>`. Each unique combination of
  partition column values results in a new partition.

  By partitioning a table, you can segment some :ref:`SQL statements
  <gloss-statement>` (e.g., those used for :ref:`table optimization
  <optimize>`, :ref:`import and export <dml-importing-data>`, and :ref:`backup and
  restore <snapshot-restore>`) by constraining them to one or more partitions.

  .. SEEALSO::

      `How-to guides: Tuning partitions for insert performance`_

- You can fine-tune table operation by setting table parameters using the
  :ref:`WITH <sql-create-table-with>` clause. Available parameters include
  those used to configure replication, sharding, :ref:`refresh interval
  <sql-create-table-refresh-interval>`, read and write operations, soft
  deletes, :ref:`durability <concept-durability>`, :ref:`column policy
  <column_policy>`, and more.


.. _data availability: https://en.wikipedia.org/wiki/High_availability
.. _data redundancy: https://en.wikipedia.org/wiki/Data_redundancy
.. _disaster recovery: https://en.wikipedia.org/wiki/Disaster_recovery
.. _How-to guides\: Tuning partitions for insert performance: https://crate.io/docs/crate/howtos/en/latest/performance/inserts/bulk.html#split-your-tables-into-partitions
.. _How-to guides\: Tuning sharding performance: https://crate.io/docs/crate/howtos/en/latest/performance/sharding.html
.. _parallelize: https://en.wikipedia.org/wiki/Distributed_computing
.. _service resilience: https://en.wikipedia.org/wiki/Resilience_(network)
