.. highlight:: psql
.. _sql_reference_delete:

==========
``DELETE``
==========

Delete rows of a table.

Synopsis
========

::

    DELETE FROM table_ident [ [AS] table_alias ] [ WHERE condition ]

Description
===========

DELETE deletes rows that satisfy the WHERE clause from the specified table. If
the WHERE clause is absent, the effect is to delete all rows in the table. The
result is a valid, but empty table.

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of an existing table to delete rows
  from.

:table_alias:
  A substitute name for the target table. When an alias is provided, it
  completely hides the actual name of the table. For example, given ``DELETE
  FROM foo AS f``, the remainder of the ``DELETE`` statement must refer to this
  table as ``f`` not ``foo``.

:condition:
  An expression that returns a value of type boolean. Only rows for which this
  expression returns ``true`` will be deleted.
