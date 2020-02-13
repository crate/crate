.. highlight:: psql
.. _sql_array_comparisons:

Array comparisons
=================

This section contains several constructs that can be used to make comparisons
between a list of values. Comparison operations result in a boolean value
(``true``/``false``) or ``null``.

These :ref:`sql_operators` are supported for doing array comparisons.

For additional examples of row comparisons, see :ref:`sql_subquery_expressions`.

.. rubric:: Table of contents

.. contents::
   :local:

.. _sql_in_array_comparison:

``IN (value [, ...])``
----------------------

Syntax:

.. code-block:: sql

    expression IN (value [, ...])

The binary operator ``IN`` allows you to verify the membership of the left-hand
operand in the right-hand parenthesized list of scalar expressions.

Returns ``true`` if any of the right-hand expression is found in the result of
the left-hand expression. It returns ``false`` otherwise.

Here's an example::

    cr> select 1 in (1,2,3) AS a, 4 in (1,2,3) AS b;
    +------+-------+
    | a    | b     |
    +------+-------+
    | TRUE | FALSE |
    +------+-------+
    SELECT 1 row in set (... sec)

The result of the ``IN`` construct yields ``null`` if:

- The left-hand expression evaluates to ``null``, and

- There are no equal right-hand values and at least one right-hand value yields
  ``null``


.. _sql_any_array_comparison:

``ANY/SOME (array expression)``
-------------------------------

Syntax:

.. code-block:: sql

    expression operator ANY | SOME (array expression)

The ``ANY`` construct returns ``true`` if the defined comparison is ``true``
for any of the values on the right-hand side array expression. It returns
``false`` if the values in the array expression do not match with the provided
comparison.

For example::

    cr> select 1 = any ([1,2,3]) AS a, 4 = any ([1,2,3]) AS b;
    +------+-------+
    | a    | b     |
    +------+-------+
    | TRUE | FALSE |
    +------+-------+
    SELECT 1 row in set (... sec)


The result of the ``ANY`` construct yields ``null`` if:

- Either the expression or the array is ``null``, and

- No ``true`` comparison is obtained and any element of the array is ``null``

.. NOTE::

    The following is not supported by the ``ANY`` operator:

    - ``is null`` and ``is not null`` as ``operator``

    - Arrays of type ``object``

    - Objects as ``expressions``

.. TIP::

    When using ``NOT <value> = ANY(<array_col>)`` the performance of the query
    could be quite bad, because special handling is required to implement the
    `3-valued logic`_. To achieve better performance, consider using the
    :ref:`ignore3vl function<ignore3vl>`.


.. _all_array_comparison:

``ALL (array expression)``
--------------------------

Syntax:

.. code-block:: sql

    value operator ALL (array)

The left-hand expression is evaluated and compared against each element of the
right-hand array using the supplied operator. The result of ``ALL`` is ``true``
if all comparisons yield ``true``. The result is ``false`` if the comparison of
at least one element does not match.

The result is ``NULL`` if either the value or the array is ``NULL`` or if no
comparison is ``false`` and at least one comparison returns ``NULL``.

::

    cr> SELECT 1 <> ALL(ARRAY[2, 3, 4]) AS x;
    +------+
    | x    |
    +------+
    | TRUE |
    +------+
    SELECT 1 row in set (... sec)


Supported operators are:

- ``=``
- ``>=``
- ``>``
- ``<=``
- ``<``
- ``<>``


.. _`3-valued logic`: https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
