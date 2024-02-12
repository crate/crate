.. highlight:: psql
.. _ref-create-foreign-table:

========================
``CREATE FOREIGN TABLE``
========================

Create a foreign table

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  CREATE FOREIGN TABLE [ IF NOT EXISTS ] table_ident ([
    { column_name data_type }
      [, ... ]
  ])
    SERVER server_name
  [ OPTIONS ( option 'value' [, ... ] ) ]


Description
===========

``CREATE FOREIGN TABLE`` is DDL statement that creates a new foreign table.
A foreign table is a view onto data in a foreign system.

To create a foreign table you must first create a foreign server using
:ref:`ref-create-server`.

The name of the table must be unique, and distinct from the name of other
relations like user tables or views.

Foreign tables are listed in the ``information_schema.tables`` view and
``information_schema.foreign_tables``.

Clauses
=======

``IF NOT EXISTS``
-----------------

Do not raise an error if the table already exists.

``OPTIONS``
-----------

:option value:

Key value pairs defining foreign data wrapper specific options for the server.
See :ref:`administration-fdw` for the foreign data wrapper specific options.

.. seealso::

   - :ref:`administration-fdw`
   - :ref:`ref-drop-foreign-table`
   - :ref:`ref-create-server`
