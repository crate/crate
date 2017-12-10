.. highlight:: psql
.. _optimize:

============
Optimization
============

.. rubric:: Table of Contents

.. contents::
   :local:

Introduction
============

In CrateDB every table (or if partitioned every partition) consists of
segments. When inserting/deleting/updating data new segments are created
following as an append-only strategy, which gives the advantage of fast writes
but on the other hand can result into a big number of segments. As the number
of segments increases the read operations become slower since more segments
need to be visited. Moreover each segment consumes file handles, memory and
CPU. CrateDB solves this problem by merging segments automatically in the
background. Small segments are merged into bigger segments, which, in turn, are
merged into even bigger segments. Furthermore any deleted rows and documents
are not copied to the new bigger segment during this process.

If required one or more tables or table partitions can be optimized explicitly
in order to improve performance. A few parameters can also be configured for
the optimization process, like the max number of segments you wish to have when
optimization is completed, or if you only wish to merge segments with deleted
data, etc. See :ref:`sql_ref_optimize` for detailed description of parameters.

::

    cr> OPTIMIZE table locations;
    OPTIMIZE OK, 1 row affected (... sec)

.. NOTE::

    System tables cannot be optimized.

Multiple Table Optimization
===========================

.. Hidden: CREATE TABLE::

    cr> CREATE TABLE IF NOT EXISTS parted_table (
    ...   id long,
    ...   title string,
    ...   content string,
    ...   width double,
    ...   day timestamp
    ... ) CLUSTERED BY (title) INTO 4 SHARDS PARTITIONED BY (day);
    CREATE OK, 1 row affected (... sec)

.. Hidden: INSERT INTO::

    cr> INSERT INTO parted_table (id, title, width, day)
    ... VALUES (1, 'Don''t Panic', 19.5, '2014-04-08');
    INSERT OK, 1 row affected (... sec)

If needed, multiple tables can be defined comma-separated in a single SQL
request. The result message is printed if the request on every given table is
completed.

::

    cr> OPTIMIZE TABLE locations, parted_table;
    OPTIMIZE OK, 2 rows affected (... sec)

.. NOTE::

   If one or more tables or partitions do not exist, none of the given
   tables/partitions are optimized and an error is returned. The error returns
   only the first non-existent table/partition.

Partition Optimization
======================

Additionally it is possible to define a specific ``PARTITION`` of a partitioned
table which should be optimized (see :ref:`partitioned_tables`).

By using the ``PARTITION`` clause in the optimize statement a separate request
for a given partition can be performed. That means that only specific
partitions of a partitioned table are optimized. For further details on how to
create an optimize request on partitioned tables see the SQL syntax and its
synopsis (see :ref:`sql_ref_optimize`).

::

    cr> OPTIMIZE TABLE parted_table PARTITION (day='2014-04-08');
    OPTIMIZE OK, 1 row affected (... sec)

In case the ``PARTITION`` clause is omitted all partitions will be optimized.
If a table has many partitions this should be avoided due to performance
reasons.

.. hide:

  cr> DROP TABLE parted_table;
  DROP OK, 1 row affected (... sec)

.. _optimize_segments_upgrade:

Segments Upgrade
================

In case that some or all of the segments of a table or a table partition are
created with an older version of the storage engine, then with the use of
``OPTIMIZE``, these segments can be upgraded to the current version of the
storage engine.

::

    cr> OPTIMIZE TABLE locations WITH (upgrade_segments=true);
    OPTIMIZE OK, 1 row affected (... sec)
