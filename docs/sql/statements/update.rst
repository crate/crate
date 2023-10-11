.. highlight:: psql
.. _ref-update:

==========
``UPDATE``
==========

Update rows of a table.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

    UPDATE table_ident [ [AS] table_alias ] SET
        { column_ident = expression } [, ...]
      [ WHERE condition ]
      [ RETURNING { * | output_expression [ [ AS ] output_name ] | relation.* } [, ...] ]

Description
===========

``UPDATE`` changes the values of the specified columns in all rows that satisfy
the condition. Only the columns to be modified need be mentioned in the SET
clause; columns not explicitly modified retain their previous values.

The optional ``RETURNING`` clause for ``UPDATE`` causes the query to return the
specified values from each row that was updated. Any :ref:`expression
<gloss-expression>` using the table's columns can be computed. The new
(post-update) values of the table's columns are used. The syntax of the
``RETURNING`` list is identical to that of the output list of ``SELECT``.

Parameters
==========

:table_ident:
    The identifier (optionally schema-qualified) of an existing table.

:table_alias:
    A substitute name for the target table.

    When an alias is provided, it completely hides the actual name of the
    table. For example, given ``UPDATE foo AS f``, the remainder of the
    ``UPDATE`` statement must refer to this table as ``f`` not ``foo``.

:column_ident:
    The name of a column in the table identified by ``table_ident``. It is also
    possible to use :ref:`object subscript <sql-object-subscript>` to address
    the inner fields of an object column and
    :ref:`array subscript <sql-array-subscript>` elements of an array.

:expression:
    An :ref:`expression <gloss-expression>` to assign to the column.

:condition:
    An expression that returns a value of type boolean. Only rows for
    which this expression returns true will be updated.

:output_expression:
    An expression to be computed and returned by the ``UPDATE`` command after
    each row is updated. The expression can use any column names of the table
    or ``*`` to return all columns. :ref:`System columns
    <sql_administration_system_columns>` can also be returned.

:output_name:
    A name to use for the result of the output expression.
