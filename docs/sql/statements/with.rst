.. highlight:: psql

.. _sql_with:

========
``WITH``
========

Synopsis
========

::

    WITH with_query [, ...] select_query


where ``with_query`` is:

::

    with_query_name [ ( column_name [, ...] ) ] AS (select_query)

and ``select_query`` any :ref:`SELECT <sql-select>` clause.

Description
===========

The ``WITH`` clause allows you to specify one or more subqueries that can be
referenced by name in the primary query. The subqueries effectively act as
temporary tables or views for the duration of the primary query.

A name (without schema qualification) must be specified for each ``WITH`` query.
Optionally, a list of column names can be specified; if this is omitted, the
column names are inferred from the subquery.

.. seealso:: :ref:`sql_dql_with`
