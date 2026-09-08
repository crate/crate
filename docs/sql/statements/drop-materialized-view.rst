.. highlight:: psql

.. _sql-drop-materialized-view:

============================
``DROP MATERIALIZED VIEW``
============================

Drop a materialized view.


Synopsis
========

::

    DROP MATERIALIZED VIEW [ IF EXISTS ] view_ident


Description
===========

``DROP MATERIALIZED VIEW`` removes a materialized view and its stored rows.
It rejects regular tables and non-materialized views.

If ``IF EXISTS`` is specified, the statement has no effect when the
materialized view does not exist.


Parameters
==========

:view_ident:
  The optionally schema-qualified name of the materialized view to drop.


Privileges
==========

The user needs ``DDL`` permission on the materialized view.


.. SEEALSO::

    :ref:`SQL syntax: CREATE MATERIALIZED VIEW <sql-create-materialized-view>`,
    :ref:`SQL syntax: REFRESH MATERIALIZED VIEW <sql-refresh-materialized-view>`
