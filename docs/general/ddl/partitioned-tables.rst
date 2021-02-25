.. highlight:: psql

.. _partitioned-tables:

==================
Partitioned tables
==================

.. rubric:: Table of contents

.. contents::
   :local:


.. _partitioned-intro:

Introduction
============

A partitioned table is a virtual table that can be created by naming one or
more columns by which it is split into separate internal tables, called
*partitions*.

When a record with a new distinct combination of values for the configured
:ref:`partition columns <gloss-partition-column>` is inserted, a new partition
is created and the document will be inserted into this partition.

You will end up with separate partitions under the hood that can be queried
like a single table.

If you are usually interested in separate partitions of your data only, as
might be the case for e.g. analyzing time based log data, you can query them
much much faster because you don't have to iterate over all rows of all
partitions.

Deletion is faster too if you delete whole partitions at once, as a whole table
can be deleted and no expensive query is involved.

.. NOTE::

    Keep in mind that the values of the partition columns are internally base32
    encoded into the partition name (which is a separate table).

    So, for every partition, the partition table name includes:

    - The table schema (optional)
    - The table name
    - The base32 encoded partition column value(s)
    - An internal overhead of 14 bytes

    Altogether, the table name length must not exceed the :ref:`255 bytes
    length limitation <sql_ddl_naming_restrictions>`.

.. CAUTION::

    Every table partition is clustered into as many shards as you configure for
    the table. Because of this, a good partition configuration depends on good
    :ref:`shard allocation <gloss-shard-allocation>`.

    Well tuned shard allocation is vital. Read the `sharding guide`_ to make
    sure you're getting the best performance out ot CrateDB.


.. _partitioned-creation:

Creation
========

It can be created using the :ref:`sql-create-table` statement using the
:ref:`sql-create-table-partitioned-by`::

    cr> CREATE TABLE parted_table (
    ...   id bigint,
    ...   title text,
    ...   content text,
    ...   width double precision,
    ...   day timestamp with time zone
    ... ) CLUSTERED BY (title) INTO 4 SHARDS PARTITIONED BY (day);
    CREATE OK, 1 row affected (... sec)

This creates an empty partitioned table which is not yet backed by real
partitions. Nonetheless does it behave like a *normal* table.

When the value to partition by references one or more
:ref:`sql-create-table-base-columns`, their values must be supplied upon
:ref:`ref-insert` or :ref:`sql-copy-from`. Often these values are computed on
client side. If this is not possible, a :ref:`generated column
<sql-create-table-generated-columns>` can be used to create a suitable
partition value from the given values on database-side::

    cr> CREATE TABLE computed_parted_table (
    ...   id bigint,
    ...   data double precision,
    ...   created_at timestamp with time zone,
    ...   month timestamp with time zone GENERATED ALWAYS AS date_trunc('month', created_at)
    ... ) PARTITIONED BY (month);
    CREATE OK, 1 row affected (... sec)


.. _partitioned-info-schema:

Information schema
==================

This table shows up in the ``information_schema.tables`` table, recognizable as
partitioned table by a non null ``partitioned_by`` column (aliased as ``p_b``
here)::

    cr> SELECT table_schema as schema,
    ...   table_name,
    ...   number_of_shards as num_shards,
    ...   number_of_replicas as num_reps,
    ...   clustered_by as c_b,
    ...   partitioned_by as p_b,
    ...   blobs_path
    ... FROM information_schema.tables
    ... WHERE table_name='parted_table';
    +--------+--------------+------------+----------+-------+---------+------------+
    | schema | table_name   | num_shards | num_reps | c_b   | p_b     | blobs_path |
    +--------+--------------+------------+----------+-------+---------+------------+
    | doc    | parted_table |          4 |      0-1 | title | ["day"] | NULL       |
    +--------+--------------+------------+----------+-------+---------+------------+
    SELECT 1 row in set (... sec)

::

    cr> SELECT table_schema as schema, table_name, column_name, data_type
    ... FROM information_schema.columns
    ... WHERE table_schema = 'doc' AND table_name = 'parted_table'
    ... ORDER BY table_schema, table_name, column_name;
    +--------+--------------+-------------+--------------------------+
    | schema | table_name   | column_name | data_type                |
    +--------+--------------+-------------+--------------------------+
    | doc    | parted_table | content     | text                     |
    | doc    | parted_table | day         | timestamp with time zone |
    | doc    | parted_table | id          | bigint                   |
    | doc    | parted_table | title       | text                     |
    | doc    | parted_table | width       | double precision         |
    +--------+--------------+-------------+--------------------------+
    SELECT 5 rows in set (... sec)

And so on.

You can get information about the partitions of a partitioned table by querying
the ``information_schema.table_partitions`` table::

    cr> SELECT count(*) as partition_count
    ... FROM information_schema.table_partitions
    ... WHERE table_schema = 'doc' AND table_name = 'parted_table';
    +-----------------+
    | partition_count |
    +-----------------+
    | 0               |
    +-----------------+
    SELECT 1 row in set (... sec)

As this table is still empty, no partitions have been created.


.. _partitioned-insert:

Insert
======

::

    cr> INSERT INTO parted_table (id, title, width, day)
    ... VALUES (1, 'Don''t Panic', 19.5, '2014-04-08');
    INSERT OK, 1 row affected (... sec)

::

    cr> SELECT partition_ident, "values", number_of_shards
    ... FROM information_schema.table_partitions
    ... WHERE table_schema = 'doc' AND table_name = 'parted_table'
    ... ORDER BY partition_ident;
    +--------------------------+------------------------+------------------+
    | partition_ident          | values                 | number_of_shards |
    +--------------------------+------------------------+------------------+
    | 04732cpp6osj2d9i60o30c1g | {"day": 1396915200000} |                4 |
    +--------------------------+------------------------+------------------+
    SELECT 1 row in set (... sec)

On subsequent inserts with the same :ref:`partition column
<gloss-partition-column>` values, no additional partition is created::

    cr> INSERT INTO parted_table (id, title, width, day)
    ... VALUES (2, 'Time is an illusion, lunchtime doubly so', 0.7, '2014-04-08');
    INSERT OK, 1 row affected (... sec)

::

    cr> REFRESH TABLE parted_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT partition_ident, "values", number_of_shards
    ... FROM information_schema.table_partitions
    ... WHERE table_schema = 'doc' AND table_name = 'parted_table'
    ... ORDER BY partition_ident;
    +--------------------------+------------------------+------------------+
    | partition_ident          | values                 | number_of_shards |
    +--------------------------+------------------------+------------------+
    | 04732cpp6osj2d9i60o30c1g | {"day": 1396915200000} |                4 |
    +--------------------------+------------------------+------------------+
    SELECT 1 row in set (... sec)


.. _partitioned-update:

Update
======

:ref:`Partition columns <gloss-partition-column>` cannot be changed, because
this would necessitate moving all affected documents. Such an operation would
not be atomic and could lead to inconsistent state::

    cr> UPDATE parted_table set content = 'now panic!', day = '2014-04-07'
    ... WHERE id = 1;
    ColumnValidationException[Validation failed for day: Updating a partitioned-by column is not supported]

When using a :ref:`generated column <sql-create-table-generated-columns>` as
partition column all the columns referenced in its *generation expression*
cannot be updated either::

    cr> UPDATE computed_parted_table set created_at='1970-01-01'
    ... WHERE id = 1;
    ColumnValidationException[Validation failed for created_at: Updating a column which is referenced in a partitioned by generated column expression is not supported]

::

    cr> UPDATE parted_table set content = 'now panic!'
    ... WHERE id = 2;
    UPDATE OK, 1 row affected (... sec)

::

    cr> REFRESH TABLE parted_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT * from parted_table WHERE id = 2;
    +----+------------------------------------------+------------+-------+---------------+
    | id | title                                    | content    | width |           day |
    +----+------------------------------------------+------------+-------+---------------+
    |  2 | Time is an illusion, lunchtime doubly so | now panic! |   0.7 | 1396915200000 |
    +----+------------------------------------------+------------+-------+---------------+
    SELECT 1 row in set (... sec)


.. _partitioned-delete:

Delete
======

Deleting with a ``WHERE`` clause matching all rows of a partition will drop the
whole partition instead of deleting every matching document, which is a lot
faster::

    cr> delete from parted_table where day = 1396915200000;
    DELETE OK, -1 rows affected (... sec)

::

    cr> SELECT count(*) as partition_count
    ... FROM information_schema.table_partitions
    ... WHERE table_schema = 'doc' AND table_name = 'parted_table';
    +-----------------+
    | partition_count |
    +-----------------+
    | 0               |
    +-----------------+
    SELECT 1 row in set (... sec)


.. _partitioned-querying:

Querying
========

``UPDATE``, ``DELETE`` and ``SELECT`` queries are all optimized to only affect
as few partitions as possible based on the partitions referenced in the
``WHERE`` clause.

The ``WHERE`` clause is analyzed for partition use by checking the ``WHERE``
conditions against the values of the :ref:`partition columns
<gloss-partition-column>`.

For example, the following query will only operate on the partition for
``day=1396915200000``:

.. Hidden: insert some rows::

    cr> INSERT INTO parted_table (id, title, content, width, day) VALUES
    ... (1, 'The incredible foo', 'foo is incredible', 12.9, '2015-11-16'),
    ... (2, 'The dark bar rises', 'na, na, na, na, na, na, na, na, barman!', 0.5, '1970-01-01'),
    ... (3, 'Kill baz', '*splatter*, *oommph*, *zip*', 13.5, '1970-01-01'),
    ... (4, 'Spice Pork And haM', 'want some roses?', -0.0, '1999-12-12');
    INSERT OK, 4 rows affected (... sec)

.. Hidden: refresh

    cr> REFRESH TABLE parted_table;
    REFRESH OK, 3 rows affected (... sec)

::

    cr> SELECT count(*) FROM parted_table
    ... WHERE day='1970-01-01'
    ... ORDER by 1;
    +----------+
    | count(*) |
    +----------+
    | 2        |
    +----------+
    SELECT 1 row in set (... sec)

Any combination of conditions that can be evaluated to a partition before
actually executing the query is supported::

    cr> SELECT id, title FROM parted_table
    ... WHERE date_trunc('year', day) > '1970-01-01'
    ... OR extract(day_of_week from day) = 1
    ... ORDER BY id DESC;
    +----+--------------------+
    | id | title              |
    +----+--------------------+
    |  4 | Spice Pork And haM |
    |  1 | The incredible foo |
    +----+--------------------+
    SELECT 2 rows in set (... sec)

Internally the ``WHERE`` clause is evaluated against the existing partitions
and their partition values. These partitions are then filtered to obtain the
list of partitions that need to be accessed.

.. Hidden: delete::

    cr> DELETE FROM parted_table;
    DELETE OK, -1 rows affected (... sec)


.. _partitioned-generated:

Partitioning by generated columns
---------------------------------

Querying on tables partitioned by generated columns is optimized to infer a
minimum list of partitions from the :ref:`partition columns
<gloss-partition-column>` referenced in the ``WHERE`` clause:

.. Hidden: insert some stuff::

    cr> INSERT INTO computed_parted_table (id, data, created_at) VALUES
    ... (1, 42.0, '2015-11-16T14:27:00+01:00'),
    ... (2, 0.0, '2015-11-16T00:00:00Z'),
    ... (3, 23.0,'1970-01-01');
    INSERT OK, 3 rows affected (... sec)

.. Hidden: refresh::

    cr> REFRESH TABLE computed_parted_table;
    REFRESH OK, 2 rows affected (... sec)

::

    cr> SELECT id, date_format('%Y-%m', month) as m FROM computed_parted_table
    ... WHERE created_at = '2015-11-16T13:27:00.000Z'
    ... ORDER BY id;
    +----+---------+
    | id | m       |
    +----+---------+
    | 1  | 2015-11 |
    +----+---------+
    SELECT 1 row in set (... sec)


.. _partitioned-alter:

Alter
=====

Parameters of partitioned tables can be changed as usual (see
:ref:`sql_ddl_alter_table` for more information on how to alter regular tables)
with the :ref:`sql-alter-table` statement. Common ``ALTER TABLE`` parameters
affect both existing partitions and partitions that will be created in the
future.

::

    cr> ALTER TABLE parted_table SET (number_of_replicas = '0-all')
    ALTER OK, -1 rows affected (... sec)

Altering schema information (such as the column policy or adding columns) can
only be done on the table (not on single partitions) and will take effect on
both existing and new partitions of the table.

::

    cr> ALTER TABLE parted_table ADD COLUMN new_col text
    ALTER OK, -1 rows affected (... sec)


.. _partitioned-alter-shards:

Changing the number of shards
-----------------------------

It is possible at any time to change the number of shards of a partitioned
table.

::

    cr> ALTER TABLE parted_table SET (number_of_shards = 10)
    ALTER OK, -1 rows affected (... sec)

.. NOTE::

  This will **not** change the number of shards of existing partitions,
  but the new number of shards will be taken into account when **new**
  partitions are created.

::

    cr> INSERT INTO parted_table (id, title, width, day)
    ... VALUES (2, 'All Good', 3.1415, '2014-04-08');
    INSERT OK, 1 row affected (... sec)

.. Hidden: refresh table::

    cr> REFRESH TABLE parted_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT count(*) as num_shards, sum(num_docs) as num_docs
    ... FROM sys.shards
    ... WHERE schema_name = 'doc' AND table_name = 'parted_table';
    +------------+----------+
    | num_shards | num_docs |
    +------------+----------+
    |         10 |      1   |
    +------------+----------+
    SELECT 1 row in set (... sec)

::

    cr> SELECT partition_ident, "values", number_of_shards
    ... FROM information_schema.table_partitions
    ... WHERE table_schema = 'doc' AND table_name = 'parted_table'
    ... ORDER BY partition_ident;
    +--------------------------+------------------------+------------------+
    | partition_ident          | values                 | number_of_shards |
    +--------------------------+------------------------+------------------+
    | 04732cpp6osj2d9i60o30c1g | {"day": 1396915200000} |               10 |
    +--------------------------+------------------------+------------------+
    SELECT 1 row in set (... sec)


.. _partitioned-alter-single:

Altering a single partition
...........................

We also provide the option to change the number of shards that are already
:ref:`allocated <gloss-shard-allocation>` for an existing partition. This
option operates on a partition basis, thus a specific partition needs to be
specified::

    cr> ALTER TABLE parted_table PARTITION (day=1396915200000) SET ("blocks.write" = true)
    ALTER OK, -1 rows affected (... sec)

    cr> ALTER TABLE parted_table PARTITION (day=1396915200000) SET (number_of_shards = 5)
    ALTER OK, 0 rows affected (... sec)

    cr> ALTER TABLE parted_table PARTITION (day=1396915200000) SET ("blocks.write" = false)
    ALTER OK, -1 rows affected (... sec)

::

    cr> SELECT partition_ident, "values", number_of_shards
    ... FROM information_schema.table_partitions
    ... WHERE table_schema = 'doc' AND table_name = 'parted_table'
    ... ORDER BY partition_ident;
    +--------------------------+------------------------+------------------+
    | partition_ident          | values                 | number_of_shards |
    +--------------------------+------------------------+------------------+
    | 04732cpp6osj2d9i60o30c1g | {"day": 1396915200000} |                5 |
    +--------------------------+------------------------+------------------+
    SELECT 1 row in set (... sec)

.. NOTE::

   The same prerequisites and restrictions as with normal tables apply. See
   :ref:`alter-shard-number`.


.. _partitioned-alter-parameters:

Alter table parameters
----------------------

It is also possible to alter parameters of single partitions of a partitioned
table. However, unlike with partitioned tables, it is not possible to alter the
schema information of single partitions.

To change table parameters such as ``number_of_replicas`` or other table
settings use the :ref:`sql-alter-table-partition`.

::

    cr> ALTER TABLE parted_table PARTITION (day=1396915200000) RESET (number_of_replicas)
    ALTER OK, -1 rows affected (... sec)


.. _partitioned-alter-table:

Alter table ``ONLY``
--------------------

Sometimes one wants to alter a partitioned table, but the changes should only
affect new partitions and not existing ones. This can be done by using the
``ONLY`` keyword.

::

    cr> ALTER TABLE ONLY parted_table SET (number_of_replicas = 1);
    ALTER OK, -1 rows affected (... sec)


.. _partitioned-alter-close-open:

Closing and opening a partition
-------------------------------

A single partition within a partitioned table can be opened and closed in the
same way a normal table can.

::

    cr> ALTER TABLE parted_table PARTITION (day=1396915200000) CLOSE;
    ALTER OK, -1 rows affected (... sec)

This will all operations beside ``ALTER TABLE ... OPEN`` to fail on this
partition. The partition will also not be included in any query on the
partitioned table.


.. _partitioned-limitations:

Limitations
===========

* ``WHERE`` clauses cannot contain queries like ``partitioned_by_column='x' OR
  normal_column=x``


.. _partitioned-consistency:

Consistency notes related to concurrent DML statement
=====================================================

If a partition is deleted during an active insert or update bulk operation this
partition won't be re-created.

The number of affected rows will always reflect the real number of
inserted/updated documents.

.. Hidden: drop table::

    cr> drop table parted_table;
    DROP OK, 1 row affected (... sec)

.. Hidden: drop computed table::

    cr> DROP TABLE computed_parted_table;
    DROP OK, 1 row affected (... sec)


.. _sharding guide: https://crate.io/docs/crate/howtos/en/latest/performance/sharding.html
