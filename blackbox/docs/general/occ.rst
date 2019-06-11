.. highlight:: psql
.. _sql_occ:

==============================
Optimistic Concurrency Control
==============================

.. rubric:: Table of contents

.. contents::
   :local:

Introduction
============

Even though CrateDB does not support transactions, `Optimistic Concurrency
Control`_ can be achieved by using the internal system columns
:ref:`_seq_no <sql_administration_system_columns_seq_no>` and
:ref:`_primary_term <sql_administration_system_columns_primary_term>`.

Every new primary shard row has an initial sequence number of ``0``. This value
is increased by ``1`` on every insert, delete or update operation the primary
shard executes. The primary term will be incremented when a shard is promoted
to primary so the user can know if they are executing an update against the
most up to date cluster configuration.

.. Hidden: update some documents to raise their ``_seq_no`` values.::

    cr> update locations set date = 0
    ... where name < 'Altair' and kind = 'Star System';
    UPDATE OK, 3 rows affected (... sec)

    cr> update locations set date = 2
    ... where name < 'Altair' and kind = 'Star System';
    UPDATE OK, 3 rows affected (... sec)

    cr> refresh table locations;
    REFRESH OK, 1 row affected (... sec)

It's possible to fetch the ``_seq_no`` and ``_primary_term`` by selecting
them::

    cr> select name, id, _seq_no, _primary_term from locations
    ... where kind = 'Star System' order by name asc;
    +----------------+----+---------+---------------+
    | name           | id | _seq_no | _primary_term |
    +----------------+----+---------+---------------+
    | Aldebaran      | 4  |      11 |             1 |
    | Algol          | 5  |      12 |             1 |
    | Alpha Centauri | 6  |      13 |             1 |
    | Altair         | 7  |       1 |             1 |
    +----------------+----+---------+---------------+
    SELECT 4 rows in set (... sec)

These ``_seq_no`` and ``_primary_term`` values can now be used on updates
and deletes.

.. NOTE::

    Optimistic concurrency control only works using the ``=`` operator,
    checking for the exact ``_seq_no`` and ``_primary_term`` your update/delete
    is based on.

Optimistic update
=================

Querying for the correct ``_seq_no`` and ``_primary_term`` ensures that no
concurrent update and cluster configuration change has taken place::

    cr> update locations set description = 'Updated description'
    ... where id=5 and "_seq_no" = 12 and "_primary_term" = 1;
    UPDATE OK, 1 row affected (... sec)

Updating a row with a wrong or outdated sequence number or primary term will
not execute the update and results in 0 affected rows::

    cr> update locations set description = 'Updated description'
    ... where id=5 and "_seq_no" = 222 and "_primary_term" = 1;
    UPDATE OK, 0 rows affected (... sec)

Optimistic delete
=================

The same can be done when deleting a row::

    cr> delete from locations where id = '6' and "_seq_no" = 13
    ... and "_primary_term" = 1;
    DELETE OK, 1 row affected (... sec)

Known limitations
=================

 - The ``_seq_no`` and ``_primary_term`` columns can only be used when
   specifying the whole primary key in a query. For example, the query below is
   not possible with our used testing data because ``name`` is not declared as
   a primary key and results in an error::

    cr> delete from locations where name = 'Aldebaran' and "_seq_no" = 3
    ... and "_primary_term" = 1;
    SQLActionException... "_seq_no" and "_primary_term" columns can only be used together in the WHERE clause with equals comparisons ...

 - In order to use the optimistic concurrency control mechanism both the
   ``_seq_no`` and ``_primary_term`` columns need to be specified. It is not
   possible to only specify one of them. For example, the query below will
   result in an error::

    cr> delete from locations where name = 'Aldebaran' and "_seq_no" = 3;
    SQLActionException... "_seq_no" and "_primary_term" columns can only be used together in the WHERE clause with equals comparisons ...

.. NOTE::

   Both, ``DELETE`` and ``UPDATE``, commands will return a row count of 0 if
   the given required version does not match the actual version of the relevant
   row.

.. _Optimistic Concurrency Control: http://en.wikipedia.org/wiki/Optimistic_concurrency_control
