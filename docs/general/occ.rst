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

    cr> CREATE TABLE sensors (
    ...   id text primary key,
    ...   type text,
    ...   last_verification timestamp
    ... );
    CREATE OK, 1 row affected  (... sec)

    cr> INSERT INTO sensors (id, type, last_verification) VALUES ('ID1', 'DHT11', null);
    INSERT OK, 1 row affected (... sec)

    cr> INSERT INTO sensors (id, type, last_verification) VALUES ('ID2', 'DHT21', null);
    INSERT OK, 1 row affected (... sec)

    cr> refresh table sensors;
    REFRESH OK, 1 row affected (... sec)

It's possible to fetch the ``_seq_no`` and ``_primary_term`` by selecting
them::

    cr> SELECT id, type, _seq_no, _primary_term FROM sensors ORDER BY 1;
    +-----+-------+---------+---------------+
    | id  | type  | _seq_no | _primary_term |
    +-----+-------+---------+---------------+
    | ID1 | DHT11 |       0 |             1 |
    | ID2 | DHT21 |       0 |             1 |
    +-----+-------+---------+---------------+
    SELECT 2 rows in set (... sec)

These ``_seq_no`` and ``_primary_term`` values can now be used on updates
and deletes.

.. NOTE::

    Optimistic concurrency control only works using the ``=`` :ref:`operator
    <gloss-operator>`, checking for the exact ``_seq_no`` and ``_primary_term``
    your update or delete is based on.

Optimistic update
=================

Querying for the correct ``_seq_no`` and ``_primary_term`` ensures that no
concurrent update and cluster configuration change has taken place::

    cr> UPDATE sensors SET last_verification = '2020-01-10 09:40'
    ... WHERE
    ...   id = 'ID1'
    ...   AND "_seq_no" = 0
    ...   AND "_primary_term" = 1;
    UPDATE OK, 1 row affected (... sec)

Updating a row with a wrong or outdated sequence number or primary term will
not execute the update and results in 0 affected rows::

    cr> UPDATE sensors SET last_verification = '2020-01-10 09:40'
    ... WHERE
    ...   id = 'ID1'
    ...   AND "_seq_no" = 42
    ...   AND "_primary_term" = 5;
    UPDATE OK, 0 rows affected (... sec)

Optimistic delete
=================

The same can be done when deleting a row::

    cr> DELETE FROM sensors WHERE id = 'ID2'
    ...   AND "_seq_no" = 0
    ...   AND "_primary_term" = 1;
    DELETE OK, 1 row affected (... sec)

Known limitations
=================

- The ``_seq_no`` and ``_primary_term`` columns can only be used when
  specifying the whole primary key in a query. For example, the query below is
  not possible with the database schema used for testing, because ``type`` is
  not declared as a primary key::

      cr> DELETE FROM sensors WHERE type = 'DHT11'
      ...   AND "_seq_no" = 3
      ...   AND "_primary_term" = 1;
      UnsupportedFeatureException["_seq_no" and "_primary_term" columns can only be used
      together in the WHERE clause with equals comparisons and if there are also equals
      comparisons on primary key columns]

- In order to use the optimistic concurrency control mechanism, both the
  ``_seq_no`` and ``_primary_term`` columns need to be specified. It is not
  possible to only specify one of them. For example, the query below will
  result in an error::

      cr> DELETE FROM sensors WHERE id = 'ID1' AND "_seq_no" = 3;
      VersioningValidationException["_seq_no" and "_primary_term" columns can only be used
      together in the WHERE clause with equals comparisons and if there are also equals
      comparisons on primary key columns]

.. NOTE::

   Both ``DELETE`` and ``UPDATE`` commands will return a row count of ``0``, if
   the given required version does not match the actual version of the relevant
   row.

.. _Optimistic Concurrency Control: https://en.wikipedia.org/wiki/Optimistic_concurrency_control
