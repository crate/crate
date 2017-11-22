.. highlight:: psql
.. _sql_occ:

==============================
Optimistic Concurrency Control
==============================

.. rubric:: Table of Contents

.. contents::
   :local:

Introduction
============

Even though CrateDB does not support transactions, `Optimistic Concurrency
Control`_ can be achieved by using the internal system column :ref:`_version
<sql_administration_system_column_version>`.

Every new row has an initial version of ``1``. This value is increased by ``1``
on every update.

.. Hidden: update some documents to raise their ``_version`` to 3. formerly
   done in other tests.::

    cr> update locations set date = 0
    ... where name < 'Altair' and kind = 'Star System';
    UPDATE OK, 3 rows affected (... sec)

    cr> update locations set date = 2
    ... where name < 'Altair' and kind = 'Star System';
    UPDATE OK, 3 rows affected (... sec)

    cr> refresh table locations;
    REFRESH OK, 1 row affected (... sec)

It's possible to fetch the ``_version`` by selecting it::

    cr> select name, id, "_version" from locations
    ... where kind = 'Star System' order by name asc;
    +----------------+----+----------+
    | name           | id | _version |
    +----------------+----+----------+
    | Aldebaran      | 4  | 3        |
    | Algol          | 5  | 3        |
    | Alpha Centauri | 6  | 3        |
    | Altair         | 7  | 1        |
    +----------------+----+----------+
    SELECT 4 rows in set (... sec)

These ``_version`` values can now be used on updates and deletes.

.. NOTE::

    Optimistic concurrency control only works using the ``=`` operator,
    checking for the exact ``_version`` your update/delete is based on.

Optimistic Update
=================

Querying for the correct ``_version`` ensures that no concurrent update has
taken place::

    cr> update locations set description = 'Updated description'
    ... where id=5 and "_version" = 3;
    UPDATE OK, 1 row affected (... sec)

Updating a row with a wrong or outdated version number will not execute the
update and results in 0 affected rows::

    cr> update locations set description = 'Updated description'
    ... where id=5 and "_version" = 2;
    UPDATE OK, 0 rows affected (... sec)

Optimistic Delete
=================

Of course the same can be done when deleting a row::

    cr> delete from locations where id = '6' and "_version" = 3;
    DELETE OK, 1 row affected (... sec)

Known Limitations
=================

 - The ``_version`` column can only be used when specifying the whole primary
   key in a query. For example, the query below is not possible with our used
   testing data because ``name`` is not declared as a primary key and results
   in an error::

    cr> delete from locations where name = 'Aldebaran' and "_version" = 3;
    SQLActionException... "_version" column can only be used in the WHERE ...

.. NOTE::

   Both, ``DELETE`` and ``UPDATE``, commands will return a row count of 0 if
   the given required version does not match the actual version of the relevant
   row.

.. _Optimistic Concurrency Control: http://en.wikipedia.org/wiki/Optimistic_concurrency_control
