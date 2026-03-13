.. highlight:: psql
.. _ref-drop-schema:

===============
``DROP SCHEMA``
===============

Drops a schema.

Synopsis
========

.. code-block:: psql

  DROP SCHEMA [IF EXISTS] name [, ...] [ CASCADE | RESTRICT ]


Description
===========

``DROP SCHEMA`` is a DDL statement that drops one or more schemas.

Parameters
==========

:name:
  One or more schema names to drop, separated by ``,``.

Clauses
=======

``IF EXISTS``
-----------------

Do not raise an error if the schema is missing.


``CASCADE``
-----------------

If a schema still contains other objects (tables, views,
:ref:`UDFs <user-defined-functions>`), all those objects are also dropped.

``CASCADE | RESTRICT``
----------------------

``RESTRICT`` causes ``DROP SCHEMA`` to raise an error if a schema still contains
 other objects (tables, views, :ref:`UDFs <user-defined-functions>`), or if
 objects of other schemas depend on objects of the schema to be dropped
 (e.g.: views). This is the default behavior.

``CASCADE`` instead causes ``DROP SCHEMA`` to also delete any objects of the
schema but also any dependend objects in other schemas.

.. seealso::

   - :ref:`ref-create-schema`
   - :ref:`schemata`
