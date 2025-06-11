.. highlight:: psql

.. _sql_subquery_expressions:

Subquery expressions
====================

Some :ref:`operators <gloss-operator>` can be used with an :ref:`uncorrelated
subquery <gloss-uncorrelated-subquery>` to form a *subquery expression* that
returns a boolean value (i.e., ``true`` or ``false``) or ``NULL``.

.. SEEALSO::

    :ref:`SQL: Value expressions <sql-scalar-subquery>`


.. _sql_in_subquery_expression:

``IN (subquery)``
-----------------

Syntax:

.. code-block:: sql

    expression IN (subquery)

The ``subquery`` must produce result rows with a single column only.

Here's an example::

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

The ``IN`` :ref:`operator <gloss-operator>` returns ``true`` if any
:ref:`subquery <gloss-subquery>` row equals the left-hand :ref:`operand
<gloss-operand>`. Otherwise, it returns ``false`` (including the case where the
subquery returns no rows).

The operator returns ``NULL`` if:

- The left-hand expression :ref:`evaluates <gloss-evaluation>` to ``NULL``

- There are no matching right-hand values and at least one right-hand value is
  ``NULL``

.. NOTE::

    ``IN (subquery)`` is an alias for ``= ANY (subquery)``


.. _sql_any_subquery_expression:

``ANY/SOME (subquery)``
-----------------------

Syntax:

.. code-block:: sql

    expression comparison ANY | SOME (subquery)

Here, ``comparison`` can be any :ref:`basic comparison operator
<comparison-operators-basic>`. The ``subquery`` must produce result rows with a
single column only.

Here's an example::

    cr> select name, population from countries
    ... where population > any (select * from unnest([8000000, 22000000, NULL]))
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

The ``ANY`` :ref:`operator <gloss-operator>` returns ``true`` if the defined
comparison is ``true`` for any of the result rows of the right-hand
:ref:`subquery <gloss-subquery>`.

The operator returns ``false`` if the comparison returns ``false`` for all
result rows of the subquery or if the subquery returns no rows.

The operator returns ``NULL`` if:

- The left-hand expression :ref:`evaluates <gloss-evaluation>` to ``NULL``

- There are no matching right-hand values and at least one right-hand value is
  ``NULL``

.. NOTE::

    The following is not supported:

    - ``IS NULL`` or ``IS NOT NULL`` as ``comparison``

    - Matching as many columns as there are expressions on the left-hand row
      e.g. ``(x,y) = ANY (select x, y from t)``


``ALL (subquery)``
------------------

Syntax:

.. code-block:: sql

    value comparison ALL (subquery)

Here, ``comparison`` can be any :ref:`basic comparison operator
<comparison-operators-basic>`. The ``subquery`` must produce result rows with a
single column only.

Here's an example::

    cr> select 100 <> ALL (select height from sys.summits) AS x;
    +------+
    | x    |
    +------+
    | TRUE |
    +------+
    SELECT 1 row in set (... sec)

The ``ALL`` :ref:`operator <gloss-operator>` returns ``true`` if the defined
comparison is ``true`` for all of the result rows of the right-hand
:ref:`subquery <gloss-subquery>`.

The operator returns ``false`` if the comparison returns ``false`` for any
result rows of the subquery.

The operator returns ``NULL`` if:

- The left-hand expression :ref:`evaluates <gloss-evaluation>` to ``NULL``

- No comparison returns ``false`` and at least one right-hand value is ``NULL``
