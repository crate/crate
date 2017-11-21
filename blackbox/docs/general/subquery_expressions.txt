.. highlight:: psql
.. _sql_subquery_expressions:

Subquery Expressions
====================

The following operators can be used with a subquery to form a subquery
expression that results in a boolean value (``true``/``false``) or ``null``.

.. NOTE::

    The used subquery has to be uncorrelated which means that the subquery does
    not contain references to relations in a parent statement.

These :ref:`sql_operators` are supported for subquery expressions.

To compare a list of values other than subqueries, see
:ref:`sql_array_comparisons`.

.. rubric:: Table of Contents

.. contents::
   :local:

.. _sql_in_subquery_expression:

``IN (subquery)``
-----------------

Syntax:

.. code-block:: sql

    expression IN (subquery)

The binary operator ``IN``, which allows you to verify the membership of
left-hand operand in the right-hand side subquery that returns exactly one
column.

Returns ``true`` if any equal subquery row equals the left-hand operand.
Returns ``false`` otherwise (including the case where the subquery returns no
rows).

For example::

    cr> select name, surname, sex from employees
    ... where dept_id in (select id from departments where name = 'Marketing')
    ... order by name, surname;
    +--------+----------+-----+
    | name   | surname  | sex |
    +--------+----------+-----+
    | David  | Bowe     | M   |
    | David  | Limb     | M   |
    | Sarrah | Mcmillan | F   |
    | Smith  | Clark    | M   |
    +--------+----------+-----+
    SELECT 4 rows in set (... sec)

The result of the ``IN`` construct yields to ``null`` if:

- The left-hand expression evaluates to ``null``

- There are no equal right-hand values and at least one right-hand value yields
  ``null``

.. NOTE::

    ``IN (subquery)`` is an alias for ``= ANY (subquery)`` and therefore their
    results are equivalent.


.. _sql_any_subquery_expression:

``ANY/SOME (subquery)``
-----------------------

Syntax:

.. code-block:: sql

    expression operator ANY | SOME (subquery)

The ``ANY`` construct returns ``true`` if the defined comparison is ``true``
for any of the values in the column that is returned by the subquery.

It returns ``false`` if the subquery does not match with the provided comparison
or the subquery returns no rows::

    cr> select name, population from countries
    ... where population > any (select * from unnest([8000000, 22000000, null]))
    ... order by population, name;
    +--------------+------------+
    | name         | population |
    +--------------+------------+
    | Austria      |    8747000 |
    | South Africa |   55910000 |
    | France       |   66900000 |
    | Turkey       |   79510000 |
    | Germany      |   82670000 |
    +--------------+------------+
    SELECT 5 rows in set (... sec)


The result of the ``ANY`` construct yields ``null`` if:

- Either the expression or the array is ``null``, and

- No ``true`` comparison is obtained and any element of the array is ``null``

.. NOTE::

    The following is not supported by the ``ANY`` operator:

    - ``is null`` and ``is not null`` as ``operator``

    - Matching as many columns as there are expressions on the left-hand row
      e.g. ``(x,y) = ANY (select x, y from t)``

      Only single-column subqueries are supported
