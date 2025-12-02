.. highlight:: psql
.. _ref-create-schema:

=================
``CREATE SCHEMA``
=================

Create a new schema.

Synopsis
========

.. code-block:: psql

  CREATE SCHEMA [IF NOT EXISTS] name


Description
===========

``CREATE SCHEMA`` is a DDL statement that creates a schema.

A schema is a namespace for relations like tables and views. Schemas are visible
in :ref:`information_schema.schemata <schemata>`.

Note that for historic reasons CrateDB also implicitly creates missing schemas
with other ``CREATE`` statements like :ref:`sql-create-table`.

Implicitly created schemas are also implicitly removed once the last remaining
object within the schema is dropped.

Explicitly created schemas remain until explicitly dropped using
:ref:`ref-drop-schema`.


Parameters
==========

:name:
  A unique name for the schema.


Clauses
=======

``IF NOT EXISTS``
-----------------

Do not raise an error if the schema already exists.
