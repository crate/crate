.. highlight:: psql

.. _sql-select:

==========
``SELECT``
==========

Retrieve rows from a table.

.. rubric:: Table of contents

.. contents::
   :local:


.. _sql-select-synopsis:

Synopsis
========

::

    SELECT [ ALL | DISTINCT ] * | expression [ [ AS ] output_name ] [, ...]
      [ OVER ( window_definition ) [, ...] ]
      [ FROM relation ]
      [ WHERE condition ]
      [ GROUP BY expression [, ...] [HAVING condition] ]
      [ UNION ALL query_specification ]
      [ WINDOW window_name AS ( window_definition ) [, ...] ]
      [ ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]
      [ LIMIT num_results ]
      [ OFFSET start ]

where ``relation`` is::

    relation_reference | joined_relation | table_function | subselect


.. _sql-select-description:

Description
===========

``SELECT`` retrieves rows from a table. The general processing of ``SELECT`` is
as follows:

- The :ref:`FROM <sql-select-from>` item points to the table where the data
  should be retrieved from. If no ``FROM`` item is specified, the query is
  executed against a virtual table with no columns.

- If the :ref:`WHERE <sql-select-where>` clause is specified, all rows that do
  not satisfy the condition are eliminated from the output.

- If the :ref:`GROUP BY <sql-select-group-by>` clause is specified, the output
  is combined into groups of rows that match on one or more values.

- The actual output rows are computed using the ``SELECT`` output
  :ref:`expressions <gloss-expression>` for each selected row or row group.

- If the :ref:`ORDER BY <sql-select-order-by>` clause is specified, the
  returned rows are sorted in the specified order. If ``ORDER BY`` is not
  given, the rows are returned in whatever order the system finds fastest to
  produce.

- If :ref:`DISTINCT <sql-select-list>` is specified, one unique row is kept.
  All other duplicate rows are removed from the result set.

- If the :ref:`LIMIT <sql-select-limit>` or :ref:`OFFSET <sql-select-offset>`
  clause is specified, the ``SELECT`` statement only returns a subset of the
  result rows.


.. _sql-select-parameters:

Parameters
==========


.. _sql-select-list:

The ``SELECT`` List
-------------------

The ``SELECT`` list specifies :ref:`expressions <gloss-expression>` that form
the output rows of the ``SELECT`` statement. The expressions can (and usually
do) refer to columns computed in the ``FROM`` clause.

::

    SELECT [ ALL | DISTINCT ] * | expression [ [ AS ] output_name ] [, ...]

Just as in a table, every output column of a ``SELECT`` has a name. In a simple
``SELECT``, this name is just used to label the column for display. To specify
the name to use for an output column, write ``AS output_name`` after the
column's ``expression``. (You can omit ``AS``, but only if the desired output
name does not match any reserved keyword. For protection against possible
future keyword additions, it is recommended that you always either write ``AS``
or double-quote the output name.) If you do not specify a column name, a name
is chosen automatically by CrateDB. If the column's expression is a simple
column reference, then the chosen name is the same as that column's name. In
more complex cases, a :ref:`function <gloss-function>` or type name may be
used, or the system may fall back on a generated name.

An output column's name can be used to refer to the column's value in
:ref:`ORDER BY <sql-select-order-by>` and :ref:`GROUP BY <sql-select-group-by>`
clauses, but not in the :ref:`WHERE <sql-select-where>` clause; there you must
write out the expression instead.

Instead of an expression, ``*`` can be written in the output list as a
shorthand for all the columns of the selected rows. Also, you can write
``table_name.*`` as a shorthand for the columns coming from just that table. In
these cases it is not possible to specify new names with ``AS``; the output
column names will be the same as the table columns' names.


.. _sql-select-clauses:

Clauses
-------


.. _sql-select-over:

``OVER``
........

The ``OVER`` clause defines a window.

::

   OVER ( window_definition )

The ``window_definition`` determines the partitioning and ordering of rows
before the :ref:`window function <window-functions>` is applied.

.. SEEALSO::

    :ref:`Window functions: Window definition <window-definition>`


.. _sql-select-from:

``FROM``
........

The ``FROM`` clause specifies the source relation for the ``SELECT``::

    FROM relation

The relation can be any of the following relations.


.. _sql-select-relation-reference:

Relation reference
''''''''''''''''''

A ``relation_reference`` is an ident which can either reference a table or a
view with an optional alias::

    relation_ident [ [AS] alias ]

:relation_ident:
  The name (optionally schema-qualified) of an existing table or view.

.. _sql_reference_relation_alias:

:alias:
  A substitute name for the ``FROM`` item containing the alias.

  An alias is used for brevity. When an alias is provided, it completely hides
  the actual name of the relation. For example given ``FROM foo AS f``, the
  remainder of the ``SELECT`` must refer to this ``FROM`` item as ``f`` not
  ``foo``.

.. SEEALSO::

    :ref:`SQL syntax: CREATE TABLE <sql-create-table>`

    :ref:`SQL syntax: CREATE VIEW <sql-create-view>`


.. _sql-select-joined-relation:

Joined relation
'''''''''''''''

A ``joined_relation`` is a relation which :ref:`joins <sql_dql_joins>` two
relations together.

::

    relation { , | join_type JOIN } relation [ { ON join_condition  |  USING (col_names) } ]

:join_type:
  ``LEFT [OUTER]``, ``RIGHT [OUTER]``, ``FULL [OUTER]``, ``CROSS`` or
  ``INNER``.

:join_condition:
  An :ref:`expression <gloss-expression>` which specifies which rows in a join
  are considered a match.

  The ``join_condition`` is not applicable for joins of type ``CROSS`` and must
  have a returning value of type ``boolean``.

:col_names:
  A comma-separated list of column names. The joined relations need to contain
  the specified columns.


.. _sql-select-table-function:

Table function
''''''''''''''

``table_function`` is a :ref:`function <gloss-function>` that produces a set of
rows and has columns.

::

    function_call

:function_call:
  The :ref:`call declaration <sql-function-call>` of the function. Usually in
  the form of ``function_name ( [ args ] )``.

  Depending on the function the parenthesis and arguments are either optional
  or required.

.. SEEALSO::

    :ref:`Built-ins: Table functions <table-functions>`


.. _sql-select-sub-select:

Subselect
''''''''''

A ``subselect`` is another ``SELECT`` statement surrounded by parentheses with
an alias:

::

    ( select_stmt ) [ AS ] alias

The :ref:`subselect <gloss-subquery>` behaves like a temporary table that is
:ref:`evaluated <gloss-evaluation>` at runtime. The clauses of the surrounding
``SELECT`` statements are applied on the result of the inner ``SELECT``
statement.

:select_stmt:
  A ``SELECT`` statement.

:alias:
  An :ref:`alias <sql_reference_relation_alias>` for the subselect.


.. _sql-select-where:

``WHERE``
.........

The optional ``WHERE`` clause defines the condition to be met for a row to be
returned::

    WHERE condition

:condition:
  A ``WHERE`` condition is any :ref:`expression <gloss-expression>` that
  :ref:`evaluates <gloss-evaluation>` to a result of type boolean.

  Any row that does not satisfy this condition will be eliminated from the
  output. A row satisfies the condition if it returns true when the actual row
  values are substituted for any variable references.


.. _sql-select-group-by:

``GROUP BY``
............

The optional ``GROUP BY`` clause will condense all selected rows that share the
same values for the grouped expression into a single row.

:ref:`Aggregate expressions <aggregation-expressions>`, if any are used, are
computed across all rows making up each group, producing a separate value for
each group.

::

    GROUP BY expression [, ...] [HAVING condition]

:expression:
  An arbitrary :ref:`expression <sql-value-expressions>` formed from column
  references of the queried relation that are also present in the result column
  list. Numeric literals are interpreted as ordinals referencing an output
  column from the select list.

  It can also reference output columns by name.

  In case of ambiguity, a ``GROUP BY`` name will be interpreted as a name of a
  column from the queried relation rather than an output column name.


.. _sql-select-having:

``HAVING``
''''''''''

The optional ``HAVING`` clause defines the condition to be met for values
within a resulting row of a ``GROUP BY`` clause.

:condition:
  A ``HAVING`` condition is any :ref:`expression <sql-literal-value>` that
  :ref:`evaluates <gloss-evaluation>` to a result of type boolean. Every row
  for which the condition is not satisfied will be eliminated from the output.

.. NOTE::

   When ``GROUP BY`` is present, it is not valid for the ``SELECT`` list
   expressions to refer to ungrouped columns except within :ref:`aggregate
   functions <aggregation-functions>`, since there would otherwise be more than
   one possible value to return for an ungrouped column.

   Additionally, grouping can only be applied on indexed fields.

.. SEEALSO::

    :ref:`Fulltext indices : Disable indexing <sql_ddl_index_off>`


.. _sql-select-union-all:

UNION ALL
.........

The ``UNION ALL`` :ref:`operator <gloss-operator>` combines the result sets of
two or more ``SELECT`` statements. The two ``SELECT`` statements that represent
the direct :ref:`operands <gloss-operand>` of the ``UNION ALL`` must produce
the same number of columns, and corresponding columns must be of the same data
types. The result of ``UNION ALL`` may contain duplicate rows. You can find
:ref:`here <sql_union>` sample usage of ``UNION ALL``.

::

    UNION ALL query_specification

:query_specification:
  Can be any ``SELECT`` statement.

``ORDER BY``, ``LIMIT``, and ``OFFSET`` can only be applied after the last
``SELECT`` statement of the ``UNION ALL``, as they are applied to the complete
result of the ``UNION`` operation. In order to apply an ``ORDER BY`` and/or
``LIMIT`` and/or ``OFFSET`` to any of the partial ``SELECT`` statements, those
statements need to become :ref:`subqueries <gloss-subquery>`.

Column names used in ``ORDER BY`` must be position numbers or refer to the
outputs of the first ``SELECT`` statement, and no :ref:`functions
<gloss-function>` can be applied on top of the ``ORDER BY`` symbols. To achieve
more complex ordering, ``UNION ALL`` must become a subselect and the more
complex ``ORDER BY`` should be applied on the outer ``SELECT`` wrapping the
``UNION ALL`` subselect.

The ordering of the outcome is not guaranteed unless ``ORDER BY`` is used.


.. _sql-select-order-by:

``ORDER BY``
............

The ``ORDER BY`` clause causes the result rows to be sorted according to the
specified expression(s).

::

    ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...]

:expression:
  Can be the name or ordinal number of an output column, or it can be an
  arbitrary :ref:`expression <gloss-expression>` formed from input-column
  values.

The optional keyword ``ASC`` (ascending) or ``DESC`` (descending) after any
expression allows to define the direction in which values are sorted. The
default is ascending.

If ``NULLS FIRST`` is specified, null values sort before non-null values. If
``NULLS LAST`` is specified, null values sort after non-null values.  If
neither is specified nulls are considered larger than any value. That means the
default for ``ASC`` is ``NULLS LAST`` and the default for ``DESC`` is ``NULLS
FIRST``.

If two rows are equal according to the leftmost expression, they are compared
according to the next expression and so on. If they are equal according to all
specified expressions, they are returned in an implementation-dependent order.

Character-string data is sorted by its UTF-8 representation.

.. NOTE::

    Sorting can only be applied on indexed fields.

    Additionally, sorting on :ref:`data-types-geo-point`,
    :ref:`data-types-geo-shape`, :ref:`data-types-arrays`, and
    :ref:`data-types-objects` is not supported.

.. SEEALSO::

    :ref:`Fulltext indices : Disable indexing <sql_ddl_index_off>`


.. _sql-select-window:

``WINDOW``
..........

The optional ``WINDOW`` clause has a form:

::

   WINDOW window_name AS ( window_definition ) [, ...]

The ``window_name`` is a name that can be referenced from ``OVER`` clauses or
subsequent window definitions.

The ``window_definition`` determines the partitioning and ordering of rows
before the :ref:`window function <window-functions>` is applied.

.. SEEALSO::

    :ref:`Window functions: Window definition <window-definition>`

    :ref:`Window functions: Named windows <window-definition-named-windows>`


.. _sql-select-limit:

``LIMIT``
.........

The optional ``LIMIT`` clause allows to limit the number of returned result
rows::

    LIMIT num_results

:num_results:
  Specifies the maximum number of result rows to return.

.. NOTE::

   It is possible for repeated executions of the same ``LIMIT`` query to return
   different subsets of the rows of a table, if there is not an ``ORDER BY`` to
   enforce selection of a deterministic subset.


.. _sql-select-offset:

``OFFSET``
..........

The optional ``OFFSET`` clause allows to skip result rows at the beginning::

    OFFSET start

:start:
  Specifies the number of rows to skip before starting to return rows.
