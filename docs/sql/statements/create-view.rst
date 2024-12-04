.. highlight:: psql

.. _sql-create-view:

===============
``CREATE VIEW``
===============

Define a new :ref:`view <ddl-views>`.

.. rubric:: Table of contents

.. contents::
    :local:


Synopsis
========

::

    CREATE [ OR REPLACE ] VIEW view_ident AS query

Or

    CREATE [ OR REPLACE ] VIEW view_ident AS (query)


Where ``query`` is a :ref:`SELECT <sql-select>` statement.


Description
===========

``CREATE VIEW`` creates a named definition of a query. This name can be used in
other statements instead of a table name to reference the saved query
definition. A view is not materialized, instead the query is run every time a
view is referenced in a query.

If ``OR REPLACE`` is used, an already existing view with the same name will be
replaced.

If a schema name is given in the ``view_ident`` (``some_schema.view_name``),
the view will be created in the specified schema.

Table and view names must be unique within a schema. A view cannot have the
name of an already existing table.

Views are read-only. They cannot be used as a target relation in write
operations.

.. SEEALSO::

    :ref:`SQL syntax: DROP VIEW <sql-drop-view>`

.. NOTE::

  If a ``*`` is used to select the columns within the views query definition,
  this ``*`` will be resolved at query time like the rest of the query
  definition. This means if columns are added to the table after the view had
  been created, these columns will show up in subsequent queries on the view.
  It is generally recommended to avoid using ``*`` in view definitions.

 .. NOTE::

   If an object column is selected, sub-columns are visible in the
   ``information_schema.columns`` table. Added sub-columns at the source
   relation are visible there while dropped columns won't show up there anymore
   as well. This applies both to views where ``*`` is used to select columns
   and to views with a static selection of top level columns.


Privileges
==========

Regular users need to have ``DDL`` permissions on the schema in which the view
is being created. In addition the user creating the view requires ``DQL``
permissions on all relations that occur within the views query definition.
