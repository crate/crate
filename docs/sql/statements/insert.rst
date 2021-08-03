.. highlight:: psql

.. _sql-insert:

==========
``INSERT``
==========

You can use the ``INSERT`` :ref:`statement <gloss-statement>` to :ref:`insert
new rows <dml-inserting-data>` into a table.

.. rubric:: Table of contents

.. contents::
   :local:


.. _sql-insert-synopsis:

Synopsis
========

CrateDB defines the full ``INSERT`` syntax as:

::

    INSERT INTO table_ident
      [ ( column_ident [, ...] ) ]
      { VALUES ( expression [, ...] ) [, ...] | ( query ) | query }
      [ ON CONFLICT (column_ident [, ...]) DO UPDATE SET { column_ident = expression [, ...] } |
        ON CONFLICT [ ( column_ident [, ...] ) ] DO NOTHING ]
      [ RETURNING { * | output_expression [ [ AS ] output_name ] | relation.* } [, ...] ]



.. _sql-insert-synopsis-params:

Parameters
----------

:table_ident:
    The identifier (optionally schema-qualified) of an existing table.

:column_ident:
    The name of a column or field in the ``table_ident`` table.

:expression:
    An :ref:`expression <gloss-expression>` or value to assign to the
    corresponding column.

:query:
    A query (i.e., :ref:`SELECT <sql-select>`) that supplies rows for the
    statement to insert.

:output_expression:
    An expression to be computed and returned by the ``INSERT`` statement after
    each row is updated. This expression can use any of the table column names,
    the ``*`` character to return all table columns, as well as any
    :ref:`system columns <sql_administration_system_columns>`.

:output_name:
    A name to use for the result of the output expression.


.. _sql-insert-desc:

Description
===========

The ``INSERT`` :ref:`statement <gloss-statement>` creates one or more rows
specified by :ref:`value expressions <sql-value-expressions>`.

You can list target column names in any order. If you omit the target column
names, they default to all columns of the table or up to *n* columns if there
are fewer values in the ``VALUES`` clause or ``query``.

CrateDB will order implicitly inferred column names by their ordinal value. The
ordinal value depends on the ordering of the columns within the :ref:`CREATE
TABLE <sql-create-table>` statement.

The values supplied by the ``VALUES`` clause or ``query`` are associated with
the explicit or implicit column list left-to-right.

CrateDB will not fill any column not present in the explicit or implicit column
list.

If the :ref:`expression <gloss-expression>` for any column is not of the
correct data type, CrateDB will attempt automatic :ref:`type conversion
<data-types-casting>`.

The optional ``RETURNING`` clause causes the ``INSERT`` statement to compute
and return values from each row inserted (or updated, in the case of ``ON
CONFLICT DO UPDATE``). You can take advantage of this behavior to obtain values
that CrateDB supplied from defaults, such as as :ref:`_id
<sql_administration_system_column_id>`.


.. _sql-insert-desc-dynamic:

.. CAUTION::

    Dynamic :ref:`SELECT <sql-select>` statements may produce inconsistent
    values for insertion when used with the ``query`` parameter.

    For example, this use of `unnest`_ produces a single column (``foo``) with
    incompatible data types (:ref:`numeric <type-numeric>` and
    :ref:`character <data-types-character-data>`, respectively)::

        SELECT unnest([{foo=1}, {foo='a string'}])

    The same problem could happen like this::

        INSERT INTO table_a (obj_col) VALUES ({foo=1}), ({foo='a string'})
        INSERT INTO table_a (int_col) (SELECT obj_col['foo'] FROM table_a)

    In this example, problems will arise if ``valid_col`` is a valid column
    name, but ``invalid_col`` is not::

        SELECT unnest([{valid_col='foo', invalid_col='bar'}])

    Any inserts that were successful before CrateDB encountered an error will
    remain, but CrateDB will reject the rest, potentially leading to
    inconsistent data.

    Users need to take special care when inserting data from queries that might
    produce dynamic values like the ones above.


.. _sql-insert-on-conflict-do-update:

``ON CONFLICT DO UPDATE SET``
-----------------------------

If your table has a primary key, you can use the ``ON CONFLICT DO UPDATE SET``
clause to modify the existing record (instead of inserting a new one) if
CrateDB encounters a primary key conflict during the ``INSERT`` operation.

Syntax::

     ON CONFLICT (conflict_target) DO UPDATE SET { assignments }

Where ``conflict_target`` can be one or more column identifiers::

    column_ident [, ... ]

And ``assignments`` can be one or more column assignments::

    assignments = expression [, ... ]

.. NOTE::

    CrateDB does not support unique constraints, foreign key constraints, or
    exclusion constraints (see :ref:`SQL compatibility: Unsupported features
    and functions <appendix-compat-unsupported>`). Therefore, the only
    constraint capable of producing a conflict that CrateDB supports is a
    :ref:`primary key <constraints-primary-key>` constraint.

    When using the ``ON CONFLICT DO UPDATE SET`` clause with a primary key
    constraint, the ``conflict_target`` must always match the primary key
    definition.

    For example, if ``my_table`` had a primary key ``col_a``, the correct
    syntax would be::

        ON CONFLICT (col_a) DO UPDATE SET { assignments }

    However, if ``my_table`` had a primary key on both ``col_a`` and ``col_b``,
    the correct syntax would be::

        ON CONFLICT (col_a, col_b) DO UPDATE SET { assignments }

For example::

    cr> INSERT INTO uservisits (id, name, visits, last_visit) VALUES
    ... (
    ...     0,
    ...     'Ford',
    ...     1,
    ...     '2015-09-12'
    ... ) ON CONFLICT (id) DO UPDATE SET
    ...     visits = visits + 1;
    INSERT OK, 1 row affected (... sec)

This statement instructs CrateDB to do the following:

.. rst-class:: open

- Attempt to insert a new ``uservisits`` record for user ID ``0``.

- If the insert would cause a primary key conflict on ``id`` (i.e., the user
  already has a record in the ``uservists`` table), update the existing record
  by incrementing the ``visits`` count.

You can also use a virtual table named ``excluded`` to reference values from
the failed (i.e., *excluded*) ``INSERT`` record. For example::

    cr> INSERT INTO uservisits (id, name, visits, last_visit) VALUES
    ... (
    ...     0,
    ...     'Ford',
    ...     1,
    ...     '2015-09-12'
    ... ) ON CONFLICT (id) DO UPDATE SET
    ...     visits = visits + 1,
    ...     last_visit = excluded.last_visit;
    INSERT OK, 1 row affected (... sec)

The addition of ``last_visit = excluded.last_visit`` instructs CrateDB to
overwrite the existing value of ``last_visits`` with the attempted insert
value.

.. SEEALSO::

    :ref:`Inserting data: Upserts <dml-inserting-upserts>`


.. _sql-insert-on-conflict-do-nothing:

``ON CONFLICT DO NOTHING``
--------------------------

If you use the ``ON CONFLICT DO NOTHING`` clause, CrateDB will silently ignore
rows that would cause a duplicate key conflict (i.e., CrateDB will not insert
them and will not produce an error). For example::

     INSERT INTO my_table (col_a, col_b) VALUES (1, 42)
     ON CONFLICT DO NOTHING

In the statement above, if ``col_a`` had a primary key constraint and the value
``1`` already existed for ``col_a``, CrateDB would not perform an insert.

.. NOTE::

    You may specify an explicit primary key as the ``conflict_target`` (i.e.,
    ``ON CONFLICT (conflict_target) DO NOTHING``), as with :ref:`ON CONFLICT DO
    UPDATE SET <sql-insert-on-conflict-do-update>`. However, doing so is
    optional.


.. _unnest: https://crate.io/docs/crate/howtos/en/latest/performance/inserts/methods.html#unnest
