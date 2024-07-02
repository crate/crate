.. _ddl-sharding:

========
Sharding
========

.. rubric:: Table of contents

.. contents::
   :local:


.. _sharding-intro:

Introduction
============

Every :ref:`table partition <partitioned-tables>` is split into a configured
number of shards. Shards are then distributed across the cluster. As nodes are
added to the cluster, CrateDB will move shards around to achieve maximum
possible distribution.

.. TIP::

   Non-partitioned tables function as a single partition, so non-partitioned
   tables are still split into the configured number of shards.

Shards are transparent at the table-level. You do not need to know about or
think about shards when querying a table.

Read requests are broken down and executed in parallel across multiple shards
on multiple nodes, massively improving read performance.


.. _sharding-number:

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
is applied.

.. SEEALSO::

    :ref:`CREATE TABLE: CLUSTERED <sql-create-table-clustered>`

.. NOTE::

   The number of shards :ref:`can be changed <alter-shard-number>` after table
   creation, providing the value is a multiple of
   :ref:`number_of_routing_shards <sql-create-table-number-of-routing-shards>`
   (set at table-creation time, automatically, or explicitly). Altering the
   number of shards will put the table into a read-only state until the
   operation has completed.

.. CAUTION::

   Well tuned :ref:`shard allocation <ddl_shard_allocation>` is vital. Read the
   `Sharding Guide`_ to make sure you're getting the best performance out of
   CrateDB.


.. _sharding-routing:

Routing
=======

Given a fixed number of primary shards, individual rows can be routed to a
fixed shard number with a simple formula:

*shard number = hash(routing column) % total primary shards*

When hash values are distributed evenly (which will be approximately true in
most cases), rows will be distributed evenly amongst the fixed amount of
available shards.

The :ref:`routing column <gloss-routing-column>` can be specified with the
:ref:`CLUSTERED <sql-create-table-clustered>` clause when creating the table.
All rows that have the same routing column row value are stored in the same
shard. If a :ref:`primary key <primary_key_constraint>` has been defined, it
will be used as the default routing column, otherwise the :ref:`internal
document ID <sql_administration_system_column_id>` is used.

Example::

    cr> create table my_table6 (
    ...   first_column integer,
    ...   second_column text
    ... ) clustered by (first_column);
    CREATE OK, 1 row affected (... sec)


If :ref:`primary key constraints <constraints-primary-key>` are defined, the
routing column definition can be omitted as primary key columns are always used
for routing by default.

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


.. _Sharding Guide: https://crate.io/docs/crate/howtos/en/latest/performance/sharding.html
