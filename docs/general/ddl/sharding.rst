.. _sql_ddl_sharding:

========
Sharding
========

.. rubric:: Table of contents

.. contents::
   :local:

Introduction
============

Every table partition (see :ref:`partitioned_tables`) is split into a
configured number of shards. Shards are then distributed across the cluster. As
nodes are added to the cluster, CrateDB will move shards around to achieve
maximum possible distribution.

.. TIP::

   Non-partitioned tables function as a single partition, so non-partitioned tables
   are still split into the configured number of shards.

Shards are transparent at the table-level. You do not need to know about or
think about shards when querying a table.

Read requests are broken down and executed in parallel across multiple shards
on multiple nodes, massively improving read performance.

Number of shards
================

The number of shards can be defined by using the ``CLUSTERED INTO <number>
SHARDS`` statement upon the table creation.

Example::

    cr> create table my_table5 (
    ...   first_column integer
    ... ) clustered into 10 shards;
    CREATE OK, 1 row affected (... sec)

If the number of shards is not defined explicitly, the sensible default value
is applied, (see :ref:`ref_clustered_clause`).

.. NOTE::

   The number of shards can be changed later on but it involves having the table
   in a ``READ-ONLY`` state until the operation is completed
   (see :ref:`alter_change_number_of_shard`).

.. CAUTION::

   Well tuned shard allocation is vital. Read the `Sharding Guide`_ to make
   sure you're getting the best performance out ot CrateDB.

.. _Sharding Guide: https://crate.io/docs/crate/howtos/en/latest/performance/sharding.html

.. _routing:

Routing
=======

Given a fixed number of primary shards, individual rows can be routed to a
fixed shard number using a simple formula:

*shard number = hash(routing column) % total primary shards*

When hash values are distributed evenly (which will be approximately true in
most cases), rows will be distributed evenly amongst the fixed amount of
available shards.

The routing column can be specified with the ``CLUSTERED BY (<column>)``
statement. If no column is specified, the internal document ID is used.

Example::

    cr> create table my_table6 (
    ...   first_column integer,
    ...   second_column text
    ... ) clustered by (first_column);
    CREATE OK, 1 row affected (... sec)


If primary key constraints are defined, the routing column definition can be
omitted as primary key columns are always used for routing by default.

If the routing column is defined explicitly, it must match a primary key
column::

    cr> create table my_table8 (
    ...   first_column integer primary key,
    ...   second_column text primary key,
    ...   third_column text
    ... ) clustered by (first_column);
    CREATE OK, 1 row affected (... sec)

Example for combining custom routing and shard definition::

    cr> create table my_table9 (
    ...   first_column integer primary key,
    ...   second_column text primary key,
    ...   third_column text
    ... ) clustered by (first_column) into 10 shards;
    CREATE OK, 1 row affected (... sec)
