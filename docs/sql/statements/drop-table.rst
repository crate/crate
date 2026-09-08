.. _drop-table:

==============
``DROP TABLE``
==============

Remove a table.

Synopsis
========

.. code-block:: sql

    DROP [BLOB] TABLE [IF EXISTS] table_ident [, ...]

Description
===========

DROP TABLE removes tables from the cluster.

Use the ``BLOB`` keyword in order to remove a blob table (see
:ref:`blob_support`).

If the ``IF EXISTS`` clause is provided, the statement does not fail, if the
referenced table does not exists.

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of the table to be removed.

  Multiple tables can be removed with a single statement by providing a
  comma-separated list of table names. Dropping multiple tables is not
  atomic: if dropping one table fails, tables listed before it may already
  have been dropped. ``DROP BLOB TABLE`` supports only a single table.

