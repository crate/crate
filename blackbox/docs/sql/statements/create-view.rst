.. highlight:: psql
.. _ref-create-view:

===============
``CREATE VIEW``
===============

Define a new view.

.. rubric:: Table of Contents

.. contents::
    :local:

Synopsis
========

::

    CREATE [ OR REPLACE ] VIEW view_ident AS query


Where ``query`` is a :ref:`SELECT statement <sql_reference_select>`.


Description
===========

CREATE VIEW creates a named definition of a query. This name can be used in
other statements instead of a table name to reference the saved query
definition. A view is not materialized, instead the query is run every time a
view is referenced in a query.

If ``OR REPLACE`` is used, an already existing view with the same name will be
replaced.

If a schema name is given in the ``view_ident`` (``some_schema.view_name``),
the view will be created in the specified schema.

Table and view names must be unique within a schema. A view cannot have the
name of an already existing table.

Views are read-only. They cannot be used in write operations.

.. SEEALSO::

    :ref:`ref-drop-view`
