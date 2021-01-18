.. highlight:: psql
.. _sql_reference_expression:

=================
Value expressions
=================

Value expressions are expressions which return a single value.

They can be used in many contexts of many statements.

.. rubric:: Table of contents

.. contents::
   :local:

Literal value
=============

A literal is a notation to represent a value within a statement.

Different types have different notations. The simplest forms are:

- boolean literals: ``true`` or ``false``
- string literals: ``'this is a string literal'``
- numeric literals: ``42``
- interval literals: ``INTERVAL '1' SECOND``

.. SEEALSO::

    - :ref:`sql_lexical`
    - :ref:`data-types`

Column reference
================

A column reference is the name of a column. It's represented using an
identifier. An identifier is an unquoted or double quoted string.

- unquoted: ``columnname``

- quoted: ``"columnName"``

It's also possible to include the name of a table or alias in order to
unambiguously identify a column of a specific relation if a statement contains
multiple alias or table definitions::

    tab0.columnname

.. SEEALSO::

    :ref:`sql_lexical`

Parameter reference
===================

A parameter reference is a placeholder for a value.

CrateDB clients usually have some kind of API to provide those values.

Parameter references can either be unnumbered or numbered:

- Question mark as an unnumbered placeholder: ``select * from t where x = ?``

- ``$n`` as numbered placeholder: ``select * from t where x = $1 or x = $2``

Operator invocation
===================

There are two different types of operators in CrateDB:

- Binary: ``expression operator expression``

- Unary: ``operator expression``

.. SEEALSO::

    - :ref:`sql_dql_where_clause`
    - :ref:`arithmetic`

.. _sql_expressions_subscript:

Subscript expression
====================

A subscript expression is an expression which contains a subscript operator
(``[ ]``). It can be used to access a sub value of a composite type value.

Array subscript
---------------

The subscript operator can be used on array expressions to retrieve a single
element of an array::

    array_expression[ array_index ]

``array_index`` is a 1 based integer specifying the position of the element in
the array which should be retrieved.

.. SEEALSO::

    :ref:`sql_dql_object_arrays`

.. _object-subscript:

Object subscript
----------------

On object expressions the subscript operator can be used to access an inner
element of the object::

    obj_expression['key']

The ``key`` must be a string literal which is the name of the element which
should be retrieved.

.. SEEALSO::

    :ref:`sql_dql_objects`

.. _record-subscript:

Record subscript
----------------

Record subscript retrieves the value of a field within a record or object. This
is similar to :ref:`object subscripts <object-subscript>`.


Synopsis:

::

    (record_expression).fieldName


Example::

    cr> SELECT (information_schema._pg_expandarray(ARRAY['a', 'b'])).n AS n
    +---+
    | n |
    +---+
    | 1 |
    | 2 |
    +---+
    SELECT 2 rows in set (... sec)


``expression`` is an expression of type ``record`` or ``object`` and ``key`` is
an identifier that must refer to a field of the record.


Function call
=============

A function is declared by its name followed by its arguments enclosed in
parentheses::

    function_name([expression [, expression ... ]])
      [OVER( [PARTITION BY expression [, ...] ] [ORDER BY expression [, ...] ]) ]

.. SEEALSO::

    - :ref:`scalar`
    - :ref:`aggregation`
    - :ref:`window-functions`

Type cast
=========

A type cast specifies the conversion from one type to another. The syntax is::

    CAST(expression as type)

Another variant to do type casts is ``try_cast``. Instead of raising an error
this returns ``null`` if a value cannot be converted to the given type::

    TRY_CAST(expression as type)

.. SEEALSO::

    :ref:`data-types`

Object constructor
==================

A object constructor is an expression which builds an object using its
arguments.

It consists of one ore more ``ident = expression``, separated by commas and
enclosed in curly brackets::

    { elementNameIdent = valueExpression [, elementNameIdent = valueExpression ...] }

.. SEEALSO::

    :ref:`data-type-object-literals`

Array constructor
=================

A array constructor is an expression which builds an array. It consists of one
or more expressions separated by commas, enclosed in square brackets and
optionally prefixed with ``ARRAY``::

    [ ARRAY ] '[' expression [, expression ... ] ']'

.. SEEALSO::

    :ref:`data-type-array-literals`

.. _sql_expressions_array_subquery:

Another way to construct an array is by using an ``ARRAY(subquery)`` expression
as part of the :ref:`Select list <sql_reference_select_list>` of a ``SELECT`` statement::

    ARRAY '(' subquery ')'

Example::

    cr> select array(select height from sys.summits order by height desc limit 5)
    ... as top5_mountains_array;
    +--------------------------------+
    | top5_mountains_array           |
    +--------------------------------+
    | [4808, 4634, 4545, 4527, 4506] |
    +--------------------------------+
    SELECT 1 row in set (... sec)

.. NOTE::

    Array constructor only supports subqueries returning a single column.

Scalar subquery
===============

A scalar subquery is a regular SELECT statement in parentheses that returns
zero or one row with one column.

If zero rows are returned it is treated as null value. In case more than one
row is returned it is an error.

Columns from relations from outside of the subquery cannot be accessed from
within the subquery. Trying to do so will result in an error which states that
the column is unknown.

.. NOTE::

    Scalar subqueries are restricted to SELECT, DELETE and UPDATE statements
    and cannot be used in other statements.
