.. highlight:: psql

.. _sql-declare:


===========
``DECLARE``
===========

Create a cursor.

.. _sql-declare-synopsis:

Synopsis
========

::

    DECLARE name [ ASENSITIVE | INSENSITIVE ] [ [ NO ] SCROLL ]
    CURSOR [ { WITH | WITHOUT } HOLD ] FOR query


Where ``name`` is an arbitrary name for the cursor and ``query`` is a
:ref:`SELECT <sql-select>` statement.


Description
===========

A cursor is used to retrieve a small number of rows at a time from a query with
a larger result set. After creating a cursor, rows are fetched using :ref:`FETCH
<sql-fetch>`.

Declared cursors are visible in the ``pg_catalog.pg_cursors`` table.


.. SEEALSO::

   :ref:`FETCH <sql-fetch>`
   :ref:`CLOSE <sql-close>`

Clauses
-------

.. _sql-declare-hold:

``WITH | WITHOUT HOLD``
.......................

Defaults to ``WITHOUT HOLD``, causing a cursor to be bound to the life-time
of a transaction. Using ``WITHOUT HOLD`` without active transaction (Started
with ``BEGIN``) is an error.

``WITH HOLD`` changes the life-time of a cursor to that of the connection.

Committing a transaction closes all cursors created with ``WITHOUT HOLD``.
Closing a connection closes all cursors created within that connection.


.. NOTE::

    CrateDB doesn't support full transactions. A transaction cannot be rolled
    back and any write operations within a ``BEGIN`` clause may become visible
    to other statements before committing the transaction.


``[ ASENSITIVE | INSENSITIVE ]``
................................

This clause has no effect in CrateDB
Cursors in CrateDB are always insensitive.

.. _sql-declare-scroll:

``[ NO ] SCROLL``
.................

``NO SCROLL`` (the default) specifies that the cursor can only be used to move
forward.

``SCROLL`` allows using a cursor for backward movement but also adds memory
overhead.
