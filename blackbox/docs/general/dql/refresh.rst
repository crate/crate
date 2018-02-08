.. highlight:: psql
.. _refresh_data:

=======
Refresh
=======

.. rubric:: Table of Contents

.. contents::
   :local:

Introduction
============

CrateDB is `eventually consistent`_. Data written with a former statement is
not guaranteed to be fetched with the next following select statement for the
affected rows.

If required one or more tables can be refreshed explicitly in order to ensure
that the latest state of the table gets fetched.

::

    cr> refresh table locations;
    REFRESH OK, 1 row affected (... sec)

A table is refreshed periodically with a specified refresh interval. By
default, the refresh interval is set to 1000 milliseconds. The refresh interval
of a table can be changed with the table parameter ``refresh_interval`` (see
:ref:`sql_ref_refresh_interval`).

.. SEEALSO::

 :ref:`Optimistic Concurrency Control <sql_occ>`

Multiple Table Refresh
======================

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
request. This ensures that they all get refreshed and so their datasets get
consistent. The result message is printed if the request on every given table
is completed.

::

    cr> REFRESH TABLE locations, parted_table;
    REFRESH OK, 2 rows affected (... sec)

.. NOTE::

    If one or more tables or partitions do not exist, none of the given
    tables/partitions are refreshed and an error is returned. The error returns
    only the first non-existent table/partition.

Partition Refresh
=================

Additionally it is possible to define a specific ``PARTITION`` of a partitioned
table which should be refreshed (see :ref:`partitioned_tables`).

By using the ``PARTITION`` clause in the refresh statement a separate request
for a given partition can be performed. That means that only specific
partitions of a partitioned table are refreshed. For further details on how to
create a refresh request on partitioned tables see the SQL syntax and its
synopsis (see :ref:`sql_ref_refresh`).

::

    cr> REFRESH TABLE parted_table PARTITION (day='2014-04-08');
    REFRESH OK, 1 row affected (... sec)

In case the ``PARTITION`` clause is omitted all partitions will be refreshed.
If a table has many partitions this should be avoided due to performance
reasons.

.. _`eventually consistent`: http://en.wikipedia.org/wiki/Eventual_consistency

.. hide:

  cr> DROP TABLE parted_table;
  DROP OK, 1 row affected (... sec)
