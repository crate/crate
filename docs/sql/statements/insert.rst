.. highlight:: psql
.. _ref-insert:

==========
``INSERT``
==========

Create new rows in a table.

.. rubric:: Table of contents

.. contents::
   :local:

.. _insert_synopsis:

Synopsis
========

::

    INSERT INTO table_ident
      [ ( column_ident [, ...] ) ]
      { VALUES ( expression [, ...] ) [, ...] | ( query ) | query }
      [ ON CONFLICT (column_ident [, ...]) DO UPDATE SET { column_ident = expression [, ...] } |
        ON CONFLICT [ ( column_ident [, ...] ) ] DO NOTHING ]
      [ RETURNING { * | output_expression [ [ AS ] output_name ] | relation.* } [, ...] ]

Description
===========

``INSERT`` creates one or more rows specified by :ref:`value expressions
<sql-value-expressions>`.

The target column names can be listed in any order. If the target column names
are omitted, they default to all columns of the table or up to N columns if
there are fewer values in the ``VALUES`` clause or ``query``.

Implicitly inferred column names are ordered by their ordinal value. The
ordinal value depends on the ordering of the columns within the ``CREATE
TABLE`` statement.

The values supplied by the ``VALUES`` clause or ``query`` are associated with
the explicit or implicit column list left-to-right.

Each column not present in the explicit or implicit column list will not be
filled.

If the :ref:`expression <gloss-expression>` for any column is not of the
correct data type, automatic type conversion will be attempted.

The optional ``RETURNING`` clause causes ``INSERT`` to compute and return
values based from each row actually inserted (or updated, if an ``ON
CONFLICT DO UPDATE`` clause was used). This is primarily useful for obtaining
values that were supplied by defaults, such as a :ref:`_id
<sql_administration_system_column_id>`, also any expression using the table's
columns is allowed.

``ON CONFLICT DO UPDATE SET``
-----------------------------

This clause can be used to update a record if a conflicting record is
encountered.

::

     ON CONFLICT (conflict_target) DO UPDATE SET { assignments }

     WHERE

      conflict_target := column_ident [, ... ]
      assignments := column_ident = expression [, ... ]


Within expressions in the ``DO UPDATE SET`` clause, you can use the special
``excluded`` table to refer to column values from the INSERT statement values.
For example:

::

     INSERT INTO t (col1, col2) VALUES (1, 41)
     ON CONFLICT (col1) DO UPDATE SET col2 = excluded.col2 + 1

The above statement would update ``col2`` to ``42`` if ``col1`` was a primary
key and the value ``1`` already existed for ``col1``.

``ON CONFLICT DO NOTHING``
--------------------------

When ``ON CONFLICT DO NOTHING`` is specified, rows which caused a duplicate
key conflict will not be inserted. No exception will be thrown. For example:

::

     INSERT INTO t (col1, col2) VALUES (1, 42)
     ON CONFLICT DO NOTHING

In the above statement, if ``col1`` had a primary key constraint and the value
``1`` already existed for ``col1``, no insert would be performed. The conflict
target after ``ON CONFLICT`` is optional.

Insert from dynamic queries constraints
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
    An :ref:`expression <gloss-expression>` or value to assign to the
    corresponding column.

:query:
    A query (``SELECT`` statement) that supplies the rows to be inserted.
    Refer to the ``SELECT`` statement for a description of the syntax.

:output_expression:
    An expression to be computed and returned by the ``INSERT`` command
    after each row is updated. The expression can use any column names
    of the table or use ``*`` to return all columns. :ref:`System columns
    <sql_administration_system_columns>` can also be returned.

:output_name:
    A name to use for the result of the output expression.
