.. _sql_ddl_sharding:

========
Sharding
========

.. rubric:: Table of Contents

.. contents::
   :local:

Introduction
============

Every table partition (see :ref:`partitioned_tables`) is split into a
configured number of shards. Shards are then distributed across the cluster. As
nodes are added to the cluster, CrateDB will move shards around to achieve
maximum possible distribution.

.. TIP::

   Unpartitioned tables function as a single partition, so unpartitioned tables
   are still split into the configured number of shards.

Shards are transparent at the table-level. You do not need to know about or
think about shards when querying a table.

Read requests are broken down and executed in parallel across multiple shards
on multiple nodes, massively improving read performance.

Number of Shards
================

The number of shards can be defined by using the ``CLUSTERED INTO <number>
SHARDS`` statement upon the table creation.

Example::

    cr> create table my_table5 (
    ...   first_column int
    ... ) clustered into 10 shards;
    CREATE OK, 1 row affected (... sec)

If the number of shards is not defined explicitly, the sensible default value
is applied, (see :ref:`ref_clustered_clause`).

.. NOTE::

   The number of shards can only be set on table creation, it cannot be changed
   later on.

.. CAUTION::

   Well tuned shard allocation is vital. Read the `Sharding Guide`_ to make
   sure you're getting the best performance out ot CrateDB.

.. _Sharding Guide: https://crate.io/docs/crate/guide/best_practices/sharding.html

.. _routing:

Routing
=======

CrateDB algorithmically determines which shard each table row belongs to. It
does this using the value of a routing column.

The column used for routing can be freely defined using the ``CLUSTERED BY
(<column>)`` statement and is used to route a row to a particular shard.

Example::

    cr> create table my_table6 (
    ...   first_column int,
    ...   second_column string
    ... ) clustered by (first_column);
    CREATE OK, 1 row affected (... sec)

If primary key constraints are defined, the routing column definition can be
omitted as primary key columns are always used for routing by default.

If the routing column is defined explicitly, it must match a primary key
column::

    cr> create table my_table8 (
    ...   first_column int primary key,
    ...   second_column string primary key,
    ...   third_column string
    ... ) clustered by (first_column);
    CREATE OK, 1 row affected (... sec)

Example for combining custom routing and shard definition::

    cr> create table my_table9 (
    ...   first_column int primary key,
    ...   second_column string primary key,
    ...   third_column string
    ... ) clustered by (first_column) into 10 shards;
    CREATE OK, 1 row affected (... sec)
