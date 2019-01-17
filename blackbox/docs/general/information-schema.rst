.. highlight:: psql
.. _information_schema:

==================
Information Schema
==================

``information_schema`` is a special schema that contains virtual tables which
are read-only and can be queried to get information about the state of the
cluster.

.. rubric:: Table of Contents

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

Virtual Tables
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
   ... id long,
   ... title string,
   ... date timestamp
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
    | information_schema | columns                 | BASE TABLE |             NULL | NULL               |
    | information_schema | ingestion_rules         | BASE TABLE |             NULL | NULL               |
    | information_schema | key_column_usage        | BASE TABLE |             NULL | NULL               |
    | information_schema | referential_constraints | BASE TABLE |             NULL | NULL               |
    | information_schema | routines                | BASE TABLE |             NULL | NULL               |
    | information_schema | schemata                | BASE TABLE |             NULL | NULL               |
    | information_schema | sql_features            | BASE TABLE |             NULL | NULL               |
    | information_schema | table_constraints       | BASE TABLE |             NULL | NULL               |
    | information_schema | table_partitions        | BASE TABLE |             NULL | NULL               |
    | information_schema | tables                  | BASE TABLE |             NULL | NULL               |
    | information_schema | views                   | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_attrdef              | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_attribute            | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_class                | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_constraint           | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_database             | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_description          | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_index                | BASE TABLE |             NULL | NULL               |
    | pg_catalog         | pg_namespace            | BASE TABLE |             NULL | NULL               |
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
    | sys                | shards                  | BASE TABLE |             NULL | NULL               |
    | sys                | snapshots               | BASE TABLE |             NULL | NULL               |
    | sys                | summits                 | BASE TABLE |             NULL | NULL               |
    | sys                | users                   | BASE TABLE |             NULL | NULL               |
    +--------------------+-------------------------+------------+------------------+--------------------+
    SELECT 41 rows in set (... sec)

The table also contains additional information such as specified routing
(:ref:`sql_ddl_sharding`) and partitioned by (:ref:`partitioned_tables`)
columns::

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
| ``blobs_path``                   | The data path of the blob table                                                    | ``String``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``closed``                       | The state of the table                                                             | ``Boolean`` |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``clustered_by``                 | The routing column used to cluster the table                                       | ``String``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``column_policy``                | Defines whether the table uses a ``STRICT`` or a ``DYNAMIC`` :ref:`column_policy`  | ``String``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``number_of_replicas``           | The number of replicas the table currently has                                     | ``Integer`` |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``number_of_shards``             | The number of shards the table is currently distributed across                     | ``Integer`` |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``partitioned_by``               | The column used to partition the table                                             | ``String``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``reference_generation``         | Specifies how values in the self-referencing column are generated                  | ``String``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``routing_hash_function``        | The name of the hash function used for internal routing                            | ``String``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``self_referencing_column_name`` | The name of the column that uniquely identifies each row (always ``_id``)          | ``String``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``settings``                     | :ref:`with_clause`                                                                 | ``Object``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``table_catalog``                | Refers to the ``table_schema``                                                     | ``String``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``table_name``                   | The name of the table                                                              | ``String``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``table_schema``                 | The name of the schema the table belongs to                                        | ``String``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``table_type``                   | The type of the table (``BASE TABLE`` for tables, ``VIEW`` for views)              | ``String``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+
| ``version``                      | A collection of version numbers relevent to the table                              | ``Object``  |
+----------------------------------+------------------------------------------------------------------------------------+-------------+

``settings``
............

Table settings specify configuration parameters for tables. Some settings can
be set during Cluster runtime and others are only applied on cluster restart.

This list of table settings in :ref:`with_clause` shows detailed information
of each parameter.

Table parameters can be applied with ``CREATE TABLE`` on creation of a table.
With ``ALTER TABLE`` they can be set on already existing tables.

The following statement creates a new table and sets the refresh interval of
shards to 500 ms and sets the shard allocation for primary shards only::

    cr> create table parameterized_table (id int, content string)
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
| ``table_catalog``   | The catalog of the table of the view (refers to ``table_schema``)                   | ``String``  |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``table_schema``    | The schema of the table of the view                                                 | ``String``  |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``table_name``      | The name of the table of the view                                                   | ``String``  |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``view_definition`` | The SELECT statement that defines the view                                          | ``String``  |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``check_option``    | Not applicable for CrateDB, always return ``NONE``                                  | ``String``  |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``is_updatable``    | Whether the view is updatable. Not applicable for CrateDB, always returns ``FALSE`` | ``Boolean`` |
+---------------------+-------------------------------------------------------------------------------------+-------------+
| ``owner``           | The user that created the view                                                      | ``String``  |
+---------------------+-------------------------------------------------------------------------------------+-------------+

.. note::

   If you drop the table of a view, the view will still exist and show up in
   the ``information_schema.tables`` and ``information_schema.views`` tables.

.. hide:

   cr> DROP view galaxies;
   DROP OK, 1 row affected (... sec)

``columns``
-----------

This table can be queried to get a list of all available columns of all tables
and views and their definition like data type and ordinal position inside the
table::

    cr> select table_name, column_name, ordinal_position as pos, data_type
    ... from information_schema.columns
    ... where table_schema = 'doc' and table_name not like 'my_table%'
    ... order by table_name asc, column_name asc;
    +-------------------+--------------------------------+------+--------------+
    | table_name        | column_name                    |  pos | data_type    |
    +-------------------+--------------------------------+------+--------------+
    | locations         | date                           |    1 | timestamp    |
    | locations         | description                    |    2 | string       |
    | locations         | id                             |    3 | string       |
    | locations         | information                    |    4 | object_array |
    | locations         | information['evolution_level'] | NULL | short        |
    | locations         | information['population']      | NULL | long         |
    | locations         | kind                           |    5 | string       |
    | locations         | name                           |    6 | string       |
    | locations         | position                       |    7 | integer      |
    | locations         | race                           |    8 | object       |
    | locations         | race['description']            | NULL | string       |
    | locations         | race['interests']              | NULL | string_array |
    | locations         | race['name']                   | NULL | string       |
    | partitioned_table | date                           |    1 | timestamp    |
    | partitioned_table | id                             |    2 | long         |
    | partitioned_table | title                          |    3 | string       |
    | quotes            | id                             |    1 | integer      |
    | quotes            | quote                          |    2 | string       |
    +-------------------+--------------------------------+------+--------------+
    SELECT 18 rows in set (... sec)

You can even query this tables' own columns (attention: this might lead to
infinite recursion of your mind, beware!)::

    cr> select column_name, data_type, ordinal_position
    ... from information_schema.columns
    ... where table_schema = 'information_schema'
    ... and table_name = 'columns' order by ordinal_position asc;
    +---------------------------+-----------+------------------+
    | column_name               | data_type | ordinal_position |
    +---------------------------+-----------+------------------+
    | character_maximum_length  | integer   |                1 |
    | character_octet_length    | integer   |                2 |
    | character_set_catalog     | string    |                3 |
    | character_set_name        | string    |                4 |
    | character_set_schema      | string    |                5 |
    | check_action              | integer   |                6 |
    | check_references          | string    |                7 |
    | collation_catalog         | string    |                8 |
    | collation_name            | string    |                9 |
    | collation_schema          | string    |               10 |
    | column_default            | string    |               11 |
    | column_name               | string    |               12 |
    | data_type                 | string    |               13 |
    | datetime_precision        | integer   |               14 |
    | domain_catalog            | string    |               15 |
    | domain_name               | string    |               16 |
    | domain_schema             | string    |               17 |
    | generation_expression     | string    |               18 |
    | interval_precision        | integer   |               19 |
    | interval_type             | string    |               20 |
    | is_generated              | boolean   |               21 |
    | is_nullable               | boolean   |               22 |
    | numeric_precision         | integer   |               23 |
    | numeric_precision_radix   | integer   |               24 |
    | numeric_scale             | integer   |               25 |
    | ordinal_position          | short     |               26 |
    | table_catalog             | string    |               27 |
    | table_name                | string    |               28 |
    | table_schema              | string    |               29 |
    | user_defined_type_catalog | string    |               30 |
    | user_defined_type_name    | string    |               31 |
    | user_defined_type_schema  | string    |               32 |
    +---------------------------+-----------+------------------+
    SELECT 32 rows in set (... sec)

.. NOTE::

  Columns are always sorted alphabetically in ascending order regardless of the
  order they were defined on table creation. Thus the ``ordinal_position``
  reflects the alphabetical position.

.. rubric:: Schema

+-------------------------------+-----------------------------------------------+---------------+
|            Name               |                Description                    |   Data Type   |
+===============================+===============================================+===============+
| ``table_catalog``             | Refers to the ``table_schema``                | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``table_schema``              | Schema name containing the table              | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``table_name``                | Table Name                                    | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``column_name``               | Column Name                                   | ``String``    |
|                               | For fields in object columns this is not an   |               |
|                               | identifier but a path and therefore must not  |               |
|                               | be double quoted when programmatically        |               |
|                               | obtained.                                     |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``ordinal_position``          | The position of the column within the         | ``Integer``   |
|                               | table                                         |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``is_nullable``               | Whether the column is nullable                | ``Boolean``   |
+-------------------------------+-----------------------------------------------+---------------+
| ``data_type``                 | The data type of the column                   | ``String``    |
|                               |                                               |               |
|                               | For further information see :ref:`data-types` |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``column_default``            | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``character_maximum_length``  | Not implemented (always returns ``NULL``)     | ``Integer``   |
|                               |                                               |               |
|                               | Please refer to :ref:`data-type-string` type  |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``character_octet_length``    | Not implemented (always returns ``NULL``)     | ``Integer``   |
|                               |                                               |               |
|                               | Please refer to :ref:`data-type-string` type  |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``numeric_precision``         | Indicates the number of significant digits    | ``Integer``   |
|                               | for a numeric ``data_type``. For all other    |               |
|                               | data types this column is ``NULL``.           |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``numeric_precision_radix``   | Indicates in which base the value in the      | ``Integer``   |
|                               | column ``numeric_precision`` for a numeric    |               |
|                               | ``data_type`` is exposed. This can either be  |               |
|                               | 2 (binary) or 10 (decimal). For all other     |               |
|                               | data types this column is ``NULL``.           |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``numeric_scale``             | Not implemented (always returns ``NULL``)     | ``Integer``   |
+-------------------------------+-----------------------------------------------+---------------+
| ``datetime_precision``        | Contains the fractional seconds precision for | ``Integer``   |
|                               | a ``timestamp`` ``data_type``. For all other  |               |
|                               | data types this column is ``null``.           |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``interval_type``             | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``interval_precision``        | Not implemented (always returns ``NULL``)     | ``Integer``   |
+-------------------------------+-----------------------------------------------+---------------+
| ``character_set_catalog``     | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``character_set_schema``      | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``character_set_name``        | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``collation_catalog``         | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``collation_schema``          | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``collation_name``            | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``domain_catalog``            | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``domain_schema``             | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``domain_name``               | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``user_defined_type_catalog`` | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``user_defined_type_schema``  | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``user_defined_type_name``    | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``check_references``          | Not implemented (always returns ``NULL``)     | ``String``    |
+-------------------------------+-----------------------------------------------+---------------+
| ``check_action``              | Not implemented (always returns ``NULL``)     | ``Integer``   |
+-------------------------------+-----------------------------------------------+---------------+
| ``generation_expression``     | The expression used to generate ad column.    | ``String``    |
|                               | If the column is not generated ``NULL`` is    |               |
|                               | returned.                                     |               |
+-------------------------------+-----------------------------------------------+---------------+
| ``is_generated``              | Returns ``true`` or ``false`` wether the      | ``Boolean``   |
|                               | column is generated or not                    |               |
+-------------------------------+-----------------------------------------------+---------------+


``table_constraints``
---------------------

This table can be queried to get a list of all defined table constraints, their
type, name and which table they are defined in.

.. NOTE::

    Currently only ``PRIMARY_KEY`` constraints are supported.

.. hide:

    cr> create table tbl (col STRING NOT NULL);
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

    cr> create table students (id long, department integer, name string, primary key(id, department))
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
| ``constraint_catalog``  | Refers to ``table_catalog``                                             | ``String``  |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``constraint_schema``   | Refers to ``table_schema``                                              | ``String``  |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``constraint_name``     | Name of the constraint                                                  | ``String``  |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``table_catalog``       | Refers to ``table_schema``                                              | ``String``  |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``table_schema``        | Name of the schema that contains the table that contains the constraint | ``String``  |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``table_name``          | Name of the table that contains the constraint                          | ``String``  |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``column_name``         | Name of the column that contains the constraint                         | ``String``  |
+-------------------------+-------------------------------------------------------------------------+-------------+
| ``ordinal_position``    | Position of the column within the contraint (starts with 1)             | ``Integer`` |
+-------------------------+-------------------------------------------------------------------------+-------------+

.. _is_table_partitions:

``table_partitions``
--------------------

This table can be queried to get information about all partitioned tables, Each
partition of a table is represented as one row. The row contains the
information table name, schema name, partition ident, and the values of the
partition. ``values`` is a key-value object with the 'partitioned by column' as
key(s) and the corresponding value as value(s).

For further information see :ref:`partitioned_tables`.

.. hide:

    cr> create table a_partitioned_table (id int, content string)
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

The following example shows a table where the column 'content' of table
'a_partitioned_table' has been used to partition the table. The table has two
partitions. The partitions are introduced when data is inserted where 'content'
is 'content_a', and 'content_b'.::

    cr> select table_name, schema_name as schema, partition_ident, "values"
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
:ref:`sql-ddl-custom-analyzer`), and functions created by ``CREATE FUNCTION``
statements::

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
    +---------------+
    | routine_name  |
    +---------------+
    | PathHierarchy |
    | char_group    |
    | classic       |
    | edgeNGram     |
    | edge_ngram    |
    | keyword       |
    | letter        |
    | lowercase     |
    | nGram         |
    | ngram         |
    +---------------+
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
    |       18 | TOKENIZER    |
    |       63 | TOKEN_FILTER |
    +----------+--------------+
    SELECT 4 rows in set (... sec)

.. rubric:: Schema

+--------------------+-------------+
| Name               | Data Type   |
+====================+=============+
| routine_name       | String      |
+--------------------+-------------+
| routine_type       | String      |
+--------------------+-------------+
| routine_body       | String      |
+--------------------+-------------+
| routine_schema     | String      |
+--------------------+-------------+
| data_type          | String      |
+--------------------+-------------+
| is_deterministic   | Boolean     |
+--------------------+-------------+
| routine_definition | String      |
+--------------------+-------------+
| specific_name      | String      |
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

The schemata table lists all existing schemas. These schemas are always
available: ``blob``, ``doc``, ``information_schema`` and ``sys``::

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
| feature_id       | String    | NO       |
+------------------+-----------+----------+
| feature_name     | String    | NO       |
+------------------+-----------+----------+
| sub_feature_id   | String    | NO       |
+------------------+-----------+----------+
| sub_feature_name | String    | NO       |
+------------------+-----------+----------+
| is_supported     | String    | NO       |
+------------------+-----------+----------+
| is_verified_by   | String    | YES      |
+------------------+-----------+----------+
| comments         | String    | YES      |
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

.. _information_schema_ingest:

``ingestion_rules``
-------------------

The ``ingestion_rules`` table contains rules created by
:ref:`create-ingest-rule` statements.

.. rubric:: Schema

+--------------------+-------------+
| Name               | Data Type   |
+====================+=============+
| rule_name          | String      |
+--------------------+-------------+
| source_ident       | String      |
+--------------------+-------------+
| target_table       | String      |
+--------------------+-------------+
| condition          | String      |
+--------------------+-------------+

:rule_name:
    The rule name
:source_ident:
    The ingestion source identifier
:target_table:
    The target table identifier
:condition:
    A boolean expression used to filter the source data
