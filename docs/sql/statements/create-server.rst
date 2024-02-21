.. highlight:: psql
.. _ref-create-server:

=================
``CREATE SERVER``
=================

Create a new foreign server

.. rubric:: Table of contents

.. contents::
   :local:


Synopsis
========

.. code-block:: psql

  CREATE SERVER [IF NOT EXISTS] server_name FOREIGN DATA WRAPPER fdw_name
  [ OPTIONS ( option value [, ...] ) ]

Description
===========

``CREATE SERVER`` is a DDL statement that creates a foreign server.

Servers created via ``CREATE SERVER`` are visible in
:ref:`information_schema.foreign_servers <foreign_servers>` and their options in
:ref:`information_schema.foreign_table_options <foreign_server_options>`.

Creating a server requires ``AL`` permission on cluster level.

Parameters
==========

:server_name:
  A unique name for the server.

:fdw_name:
  The foreign data wrapper that's used to communicate with the server.
  See :ref:`administration-fdw` for more information about the foreign data
  wrappers.


Clauses
=======

``IF NOT EXISTS``
-----------------

Do not raise an error if the server already exists.

``OPTIONS``
-----------

:option value:

Key value pairs defining foreign data wrapper specific options for the server.
For example, for the ``jdbc`` foreign data wrapper you can define a ``url``
property to define the connection string::

    CREATE SERVER pg FOREIGN DATA WRAPPER jdbc
    OPTIONS (url 'jdbc:postgresql://example.com:5432')

See :ref:`administration-fdw` for the foreign data wrapper specific options.

.. seealso::

   - :ref:`administration-fdw`
   - :ref:`ref-create-foreign-table`
   - :ref:`ref-create-user-mapping`
   - :ref:`ref-drop-server`
