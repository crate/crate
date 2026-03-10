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

Drop all objects (tables, views, ...) within the schema and all objects that
depend on those.

``RESTRICT``
------------

``RESTRICT`` causes ``DROP SCHEMA`` to raise an error if a schema still contains
 other objects (tables, views, :ref:`UDFs <user-defined-functions>`).
 This is the default behavior.


.. seealso::

   - :ref:`ref-create-schema`
   - :ref:`schemata`
