.. highlight:: psql

.. _sql_array_comparisons:

Array comparisons
=================

An array comparison :ref:`operator <gloss-operator>` tests the relationship
between two arrays and returns a corresponding value of ``true``, ``false``, or
``NULL``.

.. SEEALSO::

    :ref:`sql_subquery_expressions`

.. rubric:: Table of contents

.. contents::
   :local:

.. _sql_in_array_comparison:

``IN (value [, ...])``
----------------------

Syntax:

.. code-block:: sql

    expression IN (value [, ...])

Here's an example::

    cr> select 1 in (1,2,3) AS a, 4 in (1,2,3) AS b;
    +------+-------+
    | a    | b     |
    +------+-------+
    | TRUE | FALSE |
    +------+-------+
    SELECT 1 row in set (... sec)

The ``IN`` :ref:`operator <gloss-operator>` returns ``true`` if any of the
right-hand values matches the left-hand :ref:`operand <gloss-operand>`.
Otherwise, it returns ``false`` (including the case where there are no
right-hand values).

The operator returns ``NULL`` if:

- The left-hand :ref:`expression <gloss-expression>` :ref:`evaluates
  <gloss-evaluation>` to ``NULL``

- There are no matching right-hand values and at least one right-hand value is
  ``NULL``


.. _sql_any_array_comparison:

``ANY/SOME (array expression)``
-------------------------------

Syntax:

.. code-block:: sql

    expression comparison ANY | SOME (array_expression)

Here, ``comparison`` can be any :ref:`basic comparison operator
<comparison-operators-basic>`. Objects and arrays of objects are not supported
for either :ref:`operand <gloss-operand>`.

Here's an example::

    cr> select 1 = any ([1,2,3]) AS a, 4 = any ([1,2,3]) AS b;
    +------+-------+
    | a    | b     |
    +------+-------+
    | TRUE | FALSE |
    +------+-------+
    SELECT 1 row in set (... sec)

The ``ANY`` :ref:`operator <gloss-operator>` returns ``true`` if the defined
comparison is ``true`` for any of the values in the right-hand array
:ref:`expression <gloss-expression>`.

The operator returns ``false`` if the comparison returns ``false`` for all
right-hand values or there are no right-hand values.

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


.. _3-valued logic: https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
