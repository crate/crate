.. highlight:: psql
.. _ref-drop-schema:

===============
``DROP SCHEMA``
===============

Drops a schema.

Synopsis
========

.. code-block:: psql

  DROP SCHEMA [IF EXISTS] name [, ...]


Description
===========

``DROP SCHEMA`` is a DDL statement that drops one or more schemas.

If a schema still contains other objects the statement raises an error.

Parameters
==========

:name:
  One or more schema names to drop, separated by ``,``.

Clauses
=======

``IF EXISTS``
-----------------

Do not raise an error if the schema is missing.


.. seealso::

   - :ref:`ref-create-schema`
   - :ref:`schemata`
