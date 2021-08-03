.. highlight:: psql
.. _information_schema:

==================
Information schema
==================

``information_schema`` is a special schema that contains virtual tables which
are read-only and can be queried to get information about the state of the
cluster.

.. rubric:: Table of contents

.. contents::
   :local:

Access
======

When the user management is enabled, accessing the ``information_schema`` is
open to all users and it does not require any privileges.

However, being able to query ``information_schema`` tables will not allow the
user to retrieve all the rows in the table, as it can contain information
related to tables over which the connected user does not have any privileges.
The only rows that will be returned will be the ones the user is allowed to
access.

For example, if the user ``john`` has any privilege on the ``doc.books`` table
but no privilege at all on ``doc.locations``, when ``john`` issues a ``SELECT *
FROM information_schema.tables`` statement, the tables information related to
the ``doc.locations`` table will not be returned.

Virtual tables
==============

.. _information_schema_tables:

``tables``
----------

The ``information_schema.tables`` virtual table can be queried to get a list of
all available tables and views and their settings, such as number of shards or
number of replicas.

.. hide: CREATE VIEW::

   cr> CREATE VIEW galaxies AS
   ... SELECT id, name, description FROM locations WHERE kind = 'Galaxy';
   CREATE OK, 1 row affected (... sec)

.. hide: CREATE TABLE::

   cr> create table partitioned_table (
   ... id bigint,
   ... title text,
   ... date timestamp with time zone
   ... ) partitioned by (date);
   CREATE OK, 1 row affected (... sec)

::

    cr> SELECT table_schema, table_name, table_type, number_of_shards, number_of_replicas
    ... FROM information_schema.tables
    ... ORDER BY table_schema ASC, table_name ASC;
    +--------------------+-------------------------+------------+------------------+--------------------+
    | table_schema       | table_name              | table_type | number_of_shards | number_of_replicas |
    +--------------------+-------------------------+------------+------------------+--------------------+
    | doc                | galaxies                | VIEW       |             NULL | NULL               |
    | doc                | locations               | BASE TABLE |                2 | 0                  |
    | doc                | partitioned_table       | BASE TABLE |                4 | 0-1                |
    | doc                | quotes                  | BASE TABLE |                2 | 0                  |
    | information_schema | character_sets          | BASE TABLE |             NULL | NULL               |
    | information_schema | columns                 | BASE TABLE |             NULL | NULL               |
    | information_schema | key_column_usage        | BASE TABLE |             NULL | NULL               |
    | information_schema | referential_constraints | BASE TABLE |             NULL | NULL               |
    | information_schema | routines                | BASE TABLE |             NULL | NULL               |
    | information_schema | schemata                | BASE TABLE |             NULL | NULL               |
    | information_schema | sql_features            | BASE TABLE |             NULL | NULL               |
    | information_schema | table_constraints       | BASE TABLE |             NULL | NULL               |
    | information_schema | table_partitions        | BASE TABLE |             NULL | NULL               |
    | information_schema | tables                  | BASE TABLE |             NULL | NULL               |
    | information_schema | views                   | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_am                   | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_attrdef              | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_attribute            | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_class                | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_constraint           | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_database             | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_description          | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_enum                 | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_index                | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_namespace            | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_proc                 | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_range                | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_roles                | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_settings             | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_stats                | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_tablespace           | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_type                 | BASE TABLE |             NULL | NULL               |
    | sys                | allocations             | BASE TABLE |             NULL | NULL               |
    | sys                | checks                  | BASE TABLE |             NULL | NULL               |
    | sys                | cluster                 | BASE TABLE |             NULL | NULL               |
    | sys                | health                  | BASE TABLE |             NULL | NULL               |
    | sys                | jobs                    | BASE TABLE |             NULL | NULL               |
    | sys                | jobs_log                | BASE TABLE |             NULL | NULL               |
    | sys                | jobs_metrics            | BASE TABLE |             NULL | NULL               |
    | sys                | node_checks             | BASE TABLE |             NULL | NULL               |
    | sys                | nodes                   | BASE TABLE |             NULL | NULL               |
    | sys                | operations              | BASE TABLE |             NULL | NULL               |
    | sys                | operations_log          | BASE TABLE |             NULL | NULL               |
    | sys                | privileges              | BASE TABLE |             NULL | NULL               |
    | sys                | repositories            | BASE TABLE |             NULL | NULL               |
    | sys                | segments                | BASE TABLE |             NULL | NULL               |
    | sys                | shards                  | BASE TABLE |             NULL | NULL               |
    | sys                | snapshot_restore        | BASE TABLE |             NULL | NULL               |
    | sys                | snapshots               | BASE TABLE |             NULL | NULL               |
    | sys                | summits                 | BASE TABLE |             NULL | NULL               |
    | sys                | users                   | BASE TABLE |             NULL | NULL               |
    +--------------------+-------------------------+------------+------------------+--------------------+
    SELECT 51 rows in set (... sec)

The table also contains additional information such as the specified
:ref:`routing column <gloss-routing-column>` and :ref:`partition columns
<gloss-partition-column>`::

    cr> SELECT table_name, clustered_by, partitioned_by
    ... FROM information_schema.tables
    ... WHERE table_schema = 'doc'
    ... ORDER BY table_schema ASC, table_name ASC;
    +-------------------+--------------+----------------+
    | table_name        | clustered_by | partitioned_by |
    +-------------------+--------------+----------------+
    | galaxies          | NULL         | NULL           |
    | locations         | id           | NULL           |
    | partitioned_table | _id          | ["date"]       |
    | quotes            | id           | NULL           |
    +-------------------+--------------+----------------+
    SELECT 4 rows in set (... sec)

.. rubric:: Schema

+----------------------------------+------------------------------------------------------------------------------------+-------------+
| Name                             | Description                                                                        | Data Type   |
+==================================+====================================================================================+=============+
| ``blobs_path``                   | The data path of the blob table                                                    | ``TEXT``    |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``closed``                       | The state of the table                                                             | ``BOOLEAN`` |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``clustered_by``                 | The :ref:`routing column <gloss-routing-column>` used to cluster the table         | ``TEXT``    |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``column_policy``                | Defines whether the table uses a ``STRICT`` or a ``DYNAMIC`` :ref:`column_policy`  | ``TEXT``    |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``number_of_replicas``           | The number of replicas the table currently has                                     | ``INTEGER`` |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``number_of_shards``             | The number of shards the table is currently distributed across                     | ``INTEGER`` |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``partitioned_by``               | The :ref:`partition columns <gloss-partition-column>` (used to partition the       | ``TEXT``    |
|                                  | table)                                                                             |             |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``reference_generation``         | Specifies how values in the self-referencing column are generated                  | ``TEXT``    |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``routing_hash_function``        | The name of the hash function used for internal :ref:`routing <sharding-routing>`  | ``TEXT``    |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``self_referencing_column_name`` | The name of the column that uniquely identifies each row (always ``_id``)          | ``TEXT``    |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``settings``                     | :ref:`sql-create-table-with`                                                       | ``OBJECT``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``table_catalog``                | Refers to the ``table_schema``                                                     | ``TEXT``    |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``table_name``                   | The name of the table                                                              | ``TEXT``    |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``table_schema``                 | The name of the schema the table belongs to                                        | ``TEXT``    |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``table_type``                   | The type of the table (``BASE TABLE`` for tables, ``VIEW`` for views)              | ``TEXT``    |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``version``                      | A collection of version numbers relevent to the table                              | ``OBJECT``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+

``settings``
............

Table settings specify configuration parameters for tables. Some settings can
be set during Cluster runtime and others are only applied on cluster restart.

This list of table settings in :ref:`sql-create-table-with` shows detailed
information of each parameter.

Table parameters can be applied with ``CREATE TABLE`` on creation of a table.
With ``ALTER TABLE`` they can be set on already existing tables.

The following statement creates a new table and sets the refresh interval of
shards to 500 ms and sets the :ref:`shard allocation <gloss-shard-allocation>`
for primary shards only::

    cr> create table parameterized_table (id integer, content text)
    ... with ("refresh_interval"=500, "routing.allocation.enable"='primaries');
    CREATE OK, 1 row affected (... sec)

The settings can be verified by querying ``information_schema.tables``::

    cr> select settings['routing']['allocation']['enable'] as alloc_enable,
    ...   settings['refresh_interval'] as refresh_interval
    ... from information_schema.tables
    ... where table_name='parameterized_table';
    +--------------+------------------+
    | alloc_enable | refresh_interval |
    +--------------+------------------+
    | primaries    |              500 |
    +--------------+------------------+
    SELECT 1 row in set (... sec)

On existing tables this needs to be done with ``ALTER TABLE`` statement::

    cr> alter table parameterized_table
    ... set ("routing.allocation.enable"='none');
    ALTER OK, -1 rows affected (... sec)

.. hide:

    cr> drop table parameterized_table;
    DROP OK, 1 row affected (... sec)

``views``
---------

The table ``information_schema.views`` contains the name, definition and
options of all available views.

::

    cr> SELECT table_schema, table_name, view_definition
    ... FROM information_schema.views
    ... ORDER BY table_schema ASC, table_name ASC;
    +--------------+------------+-------------------------+
    | table_schema | table_name | view_definition         |
    +--------------+------------+-------------------------+
    | doc          | galaxies   | SELECT                  |
    |              |            |   "id"                  |
    |              |            | , "name"                |
    |              |            | , "description"         |
    |              |            | FROM "locations"        |
    |              |            | WHERE "kind" = 'Galaxy' |
    +--------------+------------+-------------------------+
    SELECT 1 row in set (... sec)

.. rubric:: Schema

+---------------------+-------------------------------------------------------------------------------------+-------------+
| Name                | Description                                                                         | Data Type   |
+=====================+=====================================================================================+=============+
| ``table_catalog``   | The catalog of the table of the view (refers to ``table_schema``)                   | ``TEXT``    |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``table_schema``    | The schema of the table of the view                                                 | ``TEXT``    |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``table_name``      | The name of the table of the view                                                   | ``TEXT``    |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``view_definition`` | The SELECT statement that defines the view                                          | ``TEXT``    |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``check_option``    | Not applicable for CrateDB, always return ``NONE``                                  | ``TEXT``    |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``is_updatable``    | Whether the view is updatable. Not applicable for CrateDB, always returns ``FALSE`` | ``BOOLEAN`` |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``owner``           | The user that created the view                                                      | ``TEXT``    |
+---------------------+-------------------------------------------------------------------------------------+-------------+

.. note::

   If you drop the table of a view, the view will still exist and show up in
   the ``information_schema.tables`` and ``information_schema.views`` tables.

.. hide:

   cr> DROP view galaxies;
   DROP OK, 1 row affected (... sec)

.. _information_schema_columns:

``columns``
-----------

This table can be queried to get a list of all available columns of all tables
and views and their definition like data type and ordinal position inside the
table::

    cr> select table_name, column_name, ordinal_position as pos, data_type
    ... from information_schema.columns
    ... where table_schema = 'doc' and table_name not like 'my_table%'
    ... order by table_name asc, column_name asc;
    +-------------------+--------------------------------+-----+--------------------------+
    | table_name        | column_name                    | pos | data_type                |
    +-------------------+--------------------------------+-----+--------------------------+
    | locations         | date                           |   3 | timestamp with time zone |
    | locations         | description                    |   6 | text                     |
    | locations         | id                             |   1 | integer                  |
    | locations         | information                    |  11 | object_array             |
    | locations         | information['evolution_level'] |  13 | smallint                 |
    | locations         | information['population']      |  12 | bigint                   |
    | locations         | inhabitants                    |   7 | object                   |
    | locations         | inhabitants['description']     |   9 | text                     |
    | locations         | inhabitants['interests']       |   8 | text_array               |
    | locations         | inhabitants['name']            |  10 | text                     |
    | locations         | kind                           |   4 | text                     |
    | locations         | landmarks                      |  14 | text_array               |
    | locations         | name                           |   2 | text                     |
    | locations         | position                       |   5 | integer                  |
    | partitioned_table | date                           |   3 | timestamp with time zone |
    | partitioned_table | id                             |   1 | bigint                   |
    | partitioned_table | title                          |   2 | text                     |
    | quotes            | id                             |   1 | integer                  |
    | quotes            | quote                          |   2 | text                     |
    +-------------------+--------------------------------+-----+--------------------------+
    SELECT 19 rows in set (... sec)

You can even query this table's own columns (attention: this might lead to
infinite recursion of your mind, beware!)::

    cr> select column_name, data_type, ordinal_position
    ... from information_schema.columns
    ... where table_schema = 'information_schema'
    ... and table_name = 'columns' order by column_name asc;
    +--------------------------+-----------+------------------+
    | column_name              | data_type | ordinal_position |
    +--------------------------+-----------+------------------+
    | character_maximum_length | integer   |                1 |
    | character_octet_length   | integer   |                2 |
    | character_set_catalog    | text      |                3 |
    | character_set_name       | text      |                4 |
    | character_set_schema     | text      |                5 |
    | check_action             | integer   |                6 |
    | check_references         | text      |                7 |
    | collation_catalog        | text      |                8 |
    | collation_name           | text      |                9 |
    | collation_schema         | text      |               10 |
    | column_default           | text      |               11 |
    | column_name              | text      |               12 |
    | data_type                | text      |               13 |
    | datetime_precision       | integer   |               14 |
    | domain_catalog           | text      |               15 |
    | domain_name              | text      |               16 |
    | domain_schema            | text      |               17 |
    | generation_expression    | text      |               18 |
    | interval_precision       | integer   |               19 |
    | interval_type            | text      |               20 |
    | is_generated             | text      |               21 |
    | is_nullable              | boolean   |               22 |
    | numeric_precision        | integer   |               23 |
    | numeric_precision_radix  | integer   |               24 |
    | numeric_scale            | integer   |               25 |
    | ordinal_position         | integer   |               26 |
    | table_catalog            | text      |               27 |
    | table_name               | text      |               28 |
    | table_schema             | text      |               29 |
    | udt_catalog              | text      |               30 |
    | udt_name                 | text      |               31 |
    | udt_schema               | text      |               32 |
    +--------------------------+-----------+------------------+
    SELECT 32 rows in set (... sec)


.. rubric:: Schema

+-------------------------------+-----------------------------------------------+---------------+
|            Name               |                Description                    |   Data Type   |
+===============================+===============================================+===============+
| ``table_catalog``             | Refers to the ``table_schema``                | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``table_schema``              | Schema name containing the table              | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``table_name``                | Table Name                                    | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``column_name``               | Column Name                                   | ``TEXT``      |
|                               | For fields in object columns this is not an   |               |
|                               | identifier but a path and therefore must not  |               |
|                               | be double quoted when programmatically        |               |
|                               | obtained.                                     |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``ordinal_position``          | The position of the column within the         | ``INTEGER``   |
|                               | table                                         |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``is_nullable``               | Whether the column is nullable                | ``BOOLEAN``   |
+-------------------------------+-----------------------------------------------+---------------+
| ``data_type``                 | The data type of the column                   | ``TEXT``      |
|                               |                                               |               |
|                               | For further information see :ref:`data-types` |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``column_default``            | The default :ref:`expression                  | ``TEXT``      |
|                               | <gloss-expression>` of the column             |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``character_maximum_length``  | If the data type is a :ref:`character type    | ``INTEGER``   |
|                               | <data-types-character-data>` then return the  |               |
|                               | declared length limit; otherwise ``NULL``.    |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``character_octet_length``    | Not implemented (always returns ``NULL``)     | ``INTEGER``   |
|                               |                                               |               |
|                               | Please refer to :ref:`type-text` type         |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``numeric_precision``         | Indicates the number of significant digits    | ``INTEGER``   |
|                               | for a numeric ``data_type``. For all other    |               |
|                               | data types this column is ``NULL``.           |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``numeric_precision_radix``   | Indicates in which base the value in the      | ``INTEGER``   |
|                               | column ``numeric_precision`` for a numeric    |               |
|                               | ``data_type`` is exposed. This can either be  |               |
|                               | 2 (binary) or 10 (decimal). For all other     |               |
|                               | data types this column is ``NULL``.           |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``numeric_scale``             | Not implemented (always returns ``NULL``)     | ``INTEGER``   |
+-------------------------------+-----------------------------------------------+---------------+
| ``datetime_precision``        | Contains the fractional seconds precision for | ``INTEGER``   |
|                               | a ``timestamp`` ``data_type``. For all other  |               |
|                               | data types this column is ``null``.           |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``interval_type``             | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``interval_precision``        | Not implemented (always returns ``NULL``)     | ``INTEGER``   |
+-------------------------------+-----------------------------------------------+---------------+
| ``character_set_catalog``     | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``character_set_schema``      | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``character_set_name``        | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``collation_catalog``         | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``collation_schema``          | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``collation_name``            | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``domain_catalog``            | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``domain_schema``             | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``domain_name``               | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``udt_catalog``               | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``udt_schema``                | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``udt_name``                  | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``check_references``          | Not implemented (always returns ``NULL``)     | ``TEXT``      |
+-------------------------------+-----------------------------------------------+---------------+
| ``check_action``              | Not implemented (always returns ``NULL``)     | ``INTEGER``   |
+-------------------------------+-----------------------------------------------+---------------+
| ``generation_expression``     | The expression used to generate ad column.    | ``TEXT``      |
|                               | If the column is not generated ``NULL`` is    |               |
|                               | returned.                                     |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``is_generated``              | Returns ``ALWAYS`` or ``NEVER`` wether the    | ``TEXT``      |
|                               | column is generated or not.                   |               |
+-------------------------------+-----------------------------------------------+---------------+

.. _information_schema_table_constraints:

``table_constraints``
---------------------

This table can be queried to get a list of all defined table constraints, their
type, name and which table they are defined in.

.. NOTE::

    Currently only ``PRIMARY_KEY`` constraints are supported.

.. hide:

    cr> create table tbl (col TEXT NOT NULL);
    CREATE OK, 1 row affected (... sec)

::

    cr> select table_schema, table_name, constraint_name, constraint_type as type
    ... from information_schema.table_constraints
    ... where table_name = 'tables'
    ...   or table_name = 'quotes'
    ...   or table_name = 'documents'
    ...   or table_name = 'tbl'
    ... order by table_schema desc, table_name asc limit 10;
    +--------------------+------------+-...------------------+-------------+
    | table_schema       | table_name | constraint_name      | type        |
    +--------------------+------------+-...------------------+-------------+
    | information_schema | tables     | tables_pk            | PRIMARY KEY |
    | doc                | quotes     | quotes_pk            | PRIMARY KEY |
    | doc                | tbl        | doc_tbl_col_not_null | CHECK       |
    +--------------------+------------+-...------------------+-------------+
    SELECT 3 rows in set (... sec)


``key_column_usage``
--------------------

This table may be queried to retrieve primary key information from all user
tables:

.. hide:

    cr> create table students (id bigint, department integer, name text, primary key(id, department))
    CREATE OK, 1 row affected (... sec)

::

    cr> select constraint_name, table_name, column_name, ordinal_position
    ... from information_schema.key_column_usage
    ... where table_name = 'students'
    +-----------------+------------+-------------+------------------+
    | constraint_name | table_name | column_name | ordinal_position |
    +-----------------+------------+-------------+------------------+
    | students_pk     | students   | id          |                1 |
    | students_pk     | students   | department  |                2 |
    +-----------------+------------+-------------+------------------+
    SELECT 2 rows in set (... sec)

.. rubric:: Schema

+-------------------------+-------------------------------------------------------------------------+-------------+
| Name                    | Description                                                             | Data Type   |
+=========================+=========================================================================+=============+
| ``constraint_catalog``  | Refers to ``table_catalog``                                             | ``TEXT``    |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``constraint_schema``   | Refers to ``table_schema``                                              | ``TEXT``    |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``constraint_name``     | Name of the constraint                                                  | ``TEXT``    |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``table_catalog``       | Refers to ``table_schema``                                              | ``TEXT``    |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``table_schema``        | Name of the schema that contains the table that contains the constraint | ``TEXT``    |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``table_name``          | Name of the table that contains the constraint                          | ``TEXT``    |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``column_name``         | Name of the column that contains the constraint                         | ``TEXT``    |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``ordinal_position``    | Position of the column within the contraint (starts with 1)             | ``INTEGER`` |
+-------------------------+-------------------------------------------------------------------------+-------------+

.. _is_table_partitions:

``table_partitions``
--------------------

This table can be queried to get information about all :ref:`partitioned tables
<partitioned-tables>`, Each partition of a table is represented as one row. The
row contains the information table name, schema name, partition ident, and the
values of the partition. ``values`` is a key-value object with the
:ref:`partition column <gloss-partition-column>` (or columns) as key(s) and the
corresponding value as value(s).

.. hide:

    cr> create table a_partitioned_table (id integer, content text)
    ... partitioned by (content);
    CREATE OK, 1 row affected (... sec)

::

    cr> insert into a_partitioned_table (id, content) values (1, 'content_a');
    INSERT OK, 1 row affected (... sec)

::

    cr> alter table a_partitioned_table set (number_of_shards=5);
    ALTER OK, -1 rows affected (... sec)

::

    cr> insert into a_partitioned_table (id, content) values (2, 'content_b');
    INSERT OK, 1 row affected (... sec)

The following example shows a table where the column ``content`` of table
``a_partitioned_table`` has been used to partition the table. The table has two
partitions. The partitions are introduced when data is inserted where
``content`` is ``content_a``, and ``content_b``.::

    cr> select table_name, table_schema as schema, partition_ident, "values"
    ... from information_schema.table_partitions
    ... order by table_name, partition_ident;
    +---------------------+--------+--------------------+--------------------------+
    | table_name          | schema | partition_ident    | values                   |
    +---------------------+--------+--------------------+--------------------------+
    | a_partitioned_table | doc    | 04566rreehimst2vc4 | {"content": "content_a"} |
    | a_partitioned_table | doc    | 04566rreehimst2vc8 | {"content": "content_b"} |
    +---------------------+--------+--------------------+--------------------------+
    SELECT 2 rows in set (... sec)

The second partition has been created after the number of shards for future
partitions have been changed on the partitioned table, so they show ``5``
instead of ``4``::

    cr> select table_name, partition_ident,
    ... number_of_shards, number_of_replicas
    ... from information_schema.table_partitions
    ... order by table_name, partition_ident;
    +---------------------+--------------------+------------------+--------------------+
    | table_name          | partition_ident    | number_of_shards | number_of_replicas |
    +---------------------+--------------------+------------------+--------------------+
    | a_partitioned_table | 04566rreehimst2vc4 |                4 | 0-1                |
    | a_partitioned_table | 04566rreehimst2vc8 |                5 | 0-1                |
    +---------------------+--------------------+------------------+--------------------+
    SELECT 2 rows in set (... sec)

``routines``
------------

The routines table contains tokenizers, token-filters, char-filters, custom
analyzers created by ``CREATE ANALYZER`` statements (see
:ref:`sql-ddl-custom-analyzer`), and :ref:`functions <user-defined-functions>`
created by ``CREATE FUNCTION`` statements::

    cr> select routine_name, routine_type
    ... from information_schema.routines
    ... group by routine_name, routine_type
    ... order by routine_name asc limit 5;
    +----------------------+--------------+
    | routine_name         | routine_type |
    +----------------------+--------------+
    | PathHierarchy        | TOKENIZER    |
    | apostrophe           | TOKEN_FILTER |
    | arabic               | ANALYZER     |
    | arabic_normalization | TOKEN_FILTER |
    | arabic_stem          | TOKEN_FILTER |
    +----------------------+--------------+
    SELECT 5 rows in set (... sec)

For example you can use this table to list existing tokenizers like this::

    cr> select routine_name
    ... from information_schema.routines
    ... where routine_type='TOKENIZER'
    ... order by routine_name asc limit 10;
    +----------------+
    | routine_name   |
    +----------------+
    | PathHierarchy  |
    | char_group     |
    | classic        |
    | edge_ngram     |
    | keyword        |
    | letter         |
    | lowercase      |
    | ngram          |
    | path_hierarchy |
    | pattern        |
    +----------------+
    SELECT 10 rows in set (... sec)

Or get an overview of how many routines and routine types are available::

    cr> select count(*), routine_type
    ... from information_schema.routines
    ... group by routine_type
    ... order by routine_type;
    +----------+--------------+
    | count(*) | routine_type |
    +----------+--------------+
    |       45 | ANALYZER     |
    |        3 | CHAR_FILTER  |
    |       16 | TOKENIZER    |
    |       62 | TOKEN_FILTER |
    +----------+--------------+
    SELECT 4 rows in set (... sec)

.. rubric:: Schema

+--------------------+-------------+
| Name               | Data Type   |
+====================+=============+
| routine_name       | ``TEXT``    |
+--------------------+-------------+
| routine_type       | ``TEXT``    |
+--------------------+-------------+
| routine_body       | ``TEXT``    |
+--------------------+-------------+
| routine_schema     | ``TEXT``    |
+--------------------+-------------+
| data_type          | ``TEXT``    |
+--------------------+-------------+
| is_deterministic   | ``BOOLEAN`` |
+--------------------+-------------+
| routine_definition | ``TEXT``    |
+--------------------+-------------+
| specific_name      | ``TEXT``    |
+--------------------+-------------+

:routine_name:
    Name of the routine (might be duplicated in case of overloading)
:routine_type:
    Type of the routine.
    Can be ``FUNCTION``, ``ANALYZER``, ``CHAR_FILTER``, ``TOKEN_FILTER``
    or ``TOKEN_FILTER``.
:routine_schema:
    The schema where the routine was defined.
    If it doesn't apply, then ``NULL``.
:routine_body:
    The language used for the routine implementation.
    If it doesn't apply, then ``NULL``.
:data_type:
    The return type of the function.
    If it doesn't apply, then ``NULL``.
:is_deterministic:
    If the routine is deterministic then ``True``, else ``False`` (``NULL`` if
    it doesn't apply).
:routine_definition:
    The function definition (``NULL`` if it doesn't apply).
:specific_name:
    Used to uniquely identify the function in a schema, even if the function is
    overloaded.  Currently the specific name contains the types of the function
    arguments. As the format might change in the future, it should be only used
    to compare it to other instances of ``specific_name``.

``schemata``
------------

The schemata table lists all existing schemas. Thes ``blob``,
``information_schema``, and ``sys`` schemas are always available. The ``doc``
schema is available after the first user table is created.

::

    cr> select schema_name from information_schema.schemata order by schema_name;
    +--------------------+
    | schema_name        |
    +--------------------+
    | blob               |
    | doc                |
    | information_schema |
    | pg_catalog         |
    | sys                |
    +--------------------+
    SELECT 5 rows in set (... sec)

.. _sql_features:

``sql_features``
----------------

The ``sql_features`` table outlines supported and unsupported SQL features of
CrateDB based to the current SQL standard (see :ref:`sql_supported_features`)::

    cr> select feature_name, is_supported, sub_feature_id, sub_feature_name
    ... from information_schema.sql_features
    ... where feature_id='F501';
    +--------------------------------+--------------+----------------+--------------------+
    | feature_name                   | is_supported | sub_feature_id | sub_feature_name   |
    +--------------------------------+--------------+----------------+--------------------+
    | Features and conformance views | FALSE        |                |                    |
    | Features and conformance views | TRUE         | 1              | SQL_FEATURES view  |
    | Features and conformance views | FALSE        | 2              | SQL_SIZING view    |
    | Features and conformance views | FALSE        | 3              | SQL_LANGUAGES view |
    +--------------------------------+--------------+----------------+--------------------+
    SELECT 4 rows in set (... sec)

+------------------+-----------+----------+
| Name             | Data Type | Nullable |
+==================+===========+==========+
| feature_id       | ``TEXT``  | NO       |
+------------------+-----------+----------+
| feature_name     | ``TEXT``  | NO       |
+------------------+-----------+----------+
| sub_feature_id   | ``TEXT``  | NO       |
+------------------+-----------+----------+
| sub_feature_name | ``TEXT``  | NO       |
+------------------+-----------+----------+
| is_supported     | ``TEXT``  | NO       |
+------------------+-----------+----------+
| is_verified_by   | ``TEXT``  | YES      |
+------------------+-----------+----------+
| comments         | ``TEXT``  | YES      |
+------------------+-----------+----------+

:feature_id:
    Identifier of the feature
:feature_name:
    Descriptive name of the feature by the Standard
:sub_feature_id:
    Identifier of the subfeature;
    If it has zero-length, this is a feature
:sub_feature_name:
    Descriptive name of the subfeature by the Standard;
    If it has zero-length, this is a feature
:is_supported:
    ``YES`` if the feature is fully supported by the current version of
    CrateDB, ``NO`` if not
:is_verified_by:
    Identifies the conformance test used to verify the claim;

    Always ``NULL`` since the CrateDB development group does not perform formal
    testing of feature conformance
:comments:
    Either ``NULL`` or shows a comment about the supported status of the
    feature


.. _character_sets:

``character_sets``
------------------

The ``character_sets`` table identifies the character sets available in the
current database.

In CrateDB there is always a single entry listing `UTF8`::

    cr> SELECT character_set_name, character_repertoire FROM information_schema.character_sets;
    +--------------------+----------------------+
    | character_set_name | character_repertoire |
    +--------------------+----------------------+
    | UTF8               | UCS                  |
    +--------------------+----------------------+
    SELECT 1 row in set (... sec)


.. list-table::
    :header-rows: 1

    * - Column Name
      - Return Type
      - Description
    * - ``character_set_catalog``
      - ``TEXT``
      - Not implemented, this column is always null.
    * - ``character_set_schema``
      - ``TEXT``
      - Not implemented, this column is always null.
    * - ``character_set_name``
      - ``TEXT``
      - Name of the character set
    * - ``character_repertoire``
      - ``TEXT``
      - Character repertoire
    * - ``form_of_use``
      - ``TEXT``
      - Character encoding form, same as ``character_set_name``
    * - ``default_collate_catalog``
      - ``TEXT``
      - Name of the database containing the default collation (Always ``crate``)
    * - ``default_collate_schema``
      - ``TEXT``
      - Name of the schema containing the default collation (Always ``NULL``)
    * - ``default_collate_name``
      - ``TEXT``
      - Name of the default collation (Always ``NULL``)
