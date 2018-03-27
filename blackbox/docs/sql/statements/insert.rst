.. highlight:: psql
.. _ref-insert:

==========
``INSERT``
==========

Create new rows in a table.

.. rubric:: Table of Contents

.. contents::
   :local:

.. _insert_synopsis:

Synopsis
========

::

    INSERT INTO table_ident
      [ ( column_ident [, ...] ) ]
      { VALUES ( expression [, ...] ) [, ...] | ( query ) | query }
      [ ON CONFLICT DO UPDATE SET { column_ident = expression } [, ...] |
        ON DUPLICATE KEY UPDATE { column_ident = expression } [, ...]]

Description
===========

INSERT creates one or more rows specified by value expressions.

The target column names can be listed in any order. If no list of column names
is given at all, the default is all the columns of the table in lexical order;
or the first N column names, if there are only N columns supplied by the VALUES
clause. The values supplied by the VALUES clause are associated with the
explicit or implicit column list left-to-right.

Each column not present in the explicit or implicit column list will not be
filled.

If the expression for any column is not of the correct data type, automatic
type conversion will be attempted.

.. _on_duplicate_key_update:

``ON DUPLICATE KEY UPDATE``
---------------------------

.. warning::
      This clause of the ``INSERT`` statement has been deprecated. Please use
      the ``ON CONFLICT DO UPDATE SET`` clause instead.

If ``ON DUPLICATE KEY UPDATE`` is specified and a row is inserted that would
cause a duplicate-key conflict, an update of the existing row is performed.

::

      ON DUPLICATE KEY UPDATE { column_ident = expression } [, ...]

Within expressions in the ``UPDATE`` clause you can use the
``VALUES(column_ident)`` function to refer to column values from the INSERT
statement.

So ``VALUES(column_ident)`` in the ``UPDATE`` clause refers to the value of
the column_ident that would be inserted if no duplicate-key conflict occured.

This function is especially useful in multiple-row inserts, because the values
of the current row can be referenced.

``ON CONFLICT DO UPDATE SET``
-----------------------------

``ON CONFLICT DO UPDATE SET`` works just like ``ON DUPLICATE KEY UPDATE``, but
its syntax is slightly different.

::

     ON CONFLICT DO UPDATE SET { column_ident = expression } [, ...]

Within expressions in the ``DO UPDATE SET`` clause, you can use the
``EXCLUDED`` table to refer to column values from the INSERT
statement values. For example:

::

     INSERT INTO t (col1, col2) VALUES (1, 41)
     ON CONFLICT DO UPDATE SET { col2 = excluded.col2 + 1 }

The above statement would update ``col2`` to ``42`` if ``col1`` was a primary
key and the value ``1`` already existed for ``col1``.

Insert From Dynamic Queries Constraints
---------------------------------------

In some cases ``SELECT`` statements produce invalid data. This opens a rare
occasion for inconsistent outcomes. If the select statement produces data where
a few rows contain invalid column names, or where you have rows which types are
not compatible among themselves, some rows will be inserted while others will
fail. In this case the errors are logged on the node. This could happen in the
following cases:

  * If you select invalid columns or incompatible data types with unnest
    e.g.::

        select unnest([{foo=2}, {foo='a string'}])

    or::

        select unnest([{_invalid_col='foo', valid_col='bar'}])

  * If you select from an ignored object which contains different data
    types for the same object column, e.g.::

        insert into from_table (o) values ({col='foo'}),({col=1})
        insert into to_table (i) (select o['col'] from t)

Any updates which happened before the failure will be persisted, which will
lead to inconsistent outcomes. So special care needs to be taken by the
application when using statements which might produce dynamic data.

Parameters
==========

:table_ident:
  The identifier (optionally schema-qualified) of an existing table.

:column_ident:
  The name of a column or field in the table pointed to by *table_ident*.

:expression:
  An expression or value to assign to the corresponding column. Within a
  ``ON DUPLICATE KEY UPDATE`` clause the expression may also refer to an
  expression from VALUES by using ``VALUES ( column_ident )``

:query:
  A query (SELECT statement) that supplies the rows to be inserted.
  Refer to the ``SELECT`` statement for a description of the syntax.
