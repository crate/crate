.. highlight:: psql

.. _sql-refresh-materialized-view:

===============================
``REFRESH MATERIALIZED VIEW``
===============================

Rebuild the stored result of a materialized view.


Synopsis
========

::

    REFRESH MATERIALIZED VIEW view_ident


Description
===========

``REFRESH MATERIALIZED VIEW`` executes the query saved by
:ref:`CREATE MATERIALIZED VIEW <sql-create-materialized-view>` and replaces the
stored result. CrateDB builds and refreshes the replacement before swapping it
with the existing relation. Concurrent readers therefore see either the old
complete result or the new complete result.

The search path in effect when the materialized view was created is also used
when its defining query is refreshed.


Parameters
==========

:view_ident:
  The optionally schema-qualified name of an existing materialized view.


Privileges
==========

The user needs ``DDL`` permission on the materialized view and ``DQL``
permission on every relation referenced by its defining query.


.. SEEALSO::

    :ref:`SQL syntax: CREATE MATERIALIZED VIEW <sql-create-materialized-view>`,
    :ref:`SQL syntax: DROP MATERIALIZED VIEW <sql-drop-materialized-view>`
