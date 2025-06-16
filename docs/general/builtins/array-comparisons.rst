.. highlight:: psql

.. _sql_array_comparisons:

Array comparisons
=================

An array comparison :ref:`operator <gloss-operator>` test the relationship
between a value and an array and return ``true``, ``false``, or ``NULL``.

.. SEEALSO::

    :ref:`sql_subquery_expressions`


.. _sql_in_array_comparison:

``IN (value [, ...])``
----------------------

Syntax:

.. code-block:: sql

    expression IN (value [, ...])

The ``IN`` :ref:`operator <gloss-operator>` returns ``true`` if the left-hand
matches at least one value contained within the right-hand side.

The operator returns ``NULL`` if:

- The left-hand :ref:`expression <gloss-expression>` :ref:`evaluates
  <gloss-evaluation>` to ``NULL``

- There are no matching right-hand values and at least one right-hand value is
  ``NULL``

Here's an example::

    cr> SELECT
    ...   1 in (1, 2, 3) AS a,
    ...   4 in (1, 2, 3) AS b,
    ...   5 in (1, 2, null) as c;
    +------+-------+------+
    | a    | b     | c    |
    +------+-------+------+
    | TRUE | FALSE | NULL |
    +------+-------+------+
    SELECT 1 row in set (... sec)


.. _sql_any_array_comparison:

``ANY/SOME (array expression)``
-------------------------------

Syntax:

.. code-block:: sql

    expression <comparison> ANY | SOME (array_expression)

Here, ``<comparison>`` can be any :ref:`basic comparison operator
<comparison-operators-basic>`.

An example::

    cr> SELECT
    ...   1 = ANY ([1,2,3]) AS a,
    ...   4 = ANY ([1,2,3]) AS b;
    +------+-------+
    | a    | b     |
    +------+-------+
    | TRUE | FALSE |
    +------+-------+
    SELECT 1 row in set (... sec)

The ``ANY`` :ref:`operator <gloss-operator>` returns ``true`` if the defined
comparison is ``true`` for any of the values in the right-hand array
:ref:`expression <gloss-expression>`.

If the right side is a multi-dimension array it is automatically unnested to the
required dimension.

An example::


    cr> SELECT
    ...   4 = ANY ([[1, 2], [3, 4]]) as a,
    ...   5 = ANY ([[1, 2], [3, 4]]) as b,
    ...   [1, 2] = ANY ([[1,2], [3, 4]]) as c,
    ...   [1, 3] = ANY ([[1,2], [3, 4]]) as d;
    +------+-------+------+-------+
    | a    | b     | c    | d     |
    +------+-------+------+-------+
    | TRUE | FALSE | TRUE | FALSE |
    +------+-------+------+-------+
    SELECT 1 row in set (... sec)


The operator returns ``false`` if the comparison returns ``false`` for all
right-hand values or if there are no right-hand values.

The operator returns ``NULL`` if:

- The left-hand expression :ref:`evaluates <gloss-evaluation>` to ``NULL``

- There are no matching right-hand values and at least one right-hand value is
  ``NULL``

.. TIP::

    When doing ``NOT <value> = ANY(<array_col>)``, query performance may be
    degraded because special handling is required to implement the `3-valued
    logic`_. To achieve better performance, consider using the :ref:`ignore3vl
    function <scalar-ignore3vl>`.


.. _all_array_comparison:

``ALL (array_expression)``
--------------------------

Syntax:

.. code-block:: sql

    value comparison ALL (array_expression)

Here, ``comparison`` can be any :ref:`basic comparison operator
<comparison-operators-basic>`. Objects and arrays of objects are not supported
for either :ref:`operand <gloss-operand>`.

Here's an example::

    cr> SELECT 1 <> ALL(ARRAY[2, 3, 4]) AS x;
    +------+
    | x    |
    +------+
    | TRUE |
    +------+
    SELECT 1 row in set (... sec)


The ``ALL`` :ref:`operator <gloss-operator>` returns ``true`` if the defined
comparison is ``true`` for all values in the right-hand :ref:`array expression
<sql-array-constructor>`.

The operator returns ``false`` if the comparison returns ``false`` for all
right-hand values.

The operator returns ``NULL`` if:

- The left-hand expression :ref:`evaluates <gloss-evaluation>` to ``NULL``

- No comparison returns ``false`` and at least one right-hand value is ``NULL``


.. _array_overlap_operator:

``array expression && array expression``
----------------------------------------

Syntax:

.. code-block:: sql

    array_expression && array_expression

The ``&&`` :ref:`operator <gloss-operator>` returns ``true`` if the two arrays
have at least one element in common.
If one of the argument is ``NULL`` the result is ``NULL``.

This operator is an alias to the :ref:`scalar-array_overlap` function.


.. _3-valued logic: https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
