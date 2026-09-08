.. highlight:: psql

.. _sql-create-materialized-view:

==============================
``CREATE MATERIALIZED VIEW``
==============================

Create a persisted result of a query.


Synopsis
========

::

    CREATE MATERIALIZED VIEW [ IF NOT EXISTS ] view_ident AS { ( query ) | query }


Description
===========

``CREATE MATERIALIZED VIEW`` executes ``query`` and stores its result in a
read-only relation. Queries against the materialized view read the stored rows
instead of executing the defining query again.

Materialized views use one primary shard by default. PostgreSQL clients can
discover them through ``pg_catalog.pg_class`` with ``relkind = 'm'`` and
through ``pg_catalog.pg_matviews``. They are not listed in
``pg_catalog.pg_tables``.

The stored rows do not change when source relations change. Use
:ref:`REFRESH MATERIALIZED VIEW <sql-refresh-materialized-view>` to rebuild the
result. Refresh creates the new result separately and swaps it with the current
result, so readers continue to see the previous complete result until the
refresh finishes.

If ``IF NOT EXISTS`` is specified and a relation with the same name already
exists, the statement has no effect.


Parameters
==========

:view_ident:
  The optionally schema-qualified name of the materialized view.

:query:
  A :ref:`SELECT <sql-select>` statement that supplies the rows to store.


Privileges
==========

The user needs ``DDL`` permission on the target schema and ``DQL`` permission
on every relation referenced by ``query``.


.. SEEALSO::

    :ref:`SQL syntax: REFRESH MATERIALIZED VIEW <sql-refresh-materialized-view>`,
    :ref:`SQL syntax: DROP MATERIALIZED VIEW <sql-drop-materialized-view>`
