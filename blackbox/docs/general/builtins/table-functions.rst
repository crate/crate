.. highlight:: psql

.. _ref-table-functions:

===============
Table Functions
===============

Table functions are functions that produce a set of rows. They are used like a
table or subquery in the ``FROM`` clause of a query.

Table functions can also be used in the select list of a query. The table
functions will be evaluated per row of the relations in the ``FROM`` clause,
generating one or more rows which are appended to the result set.
If multiple table functions with different amount of rows are used, ``null``
values will be returned for the functions that are exhausted. An example::


    cr> select unnest([1, 2, 3]), unnest([1, 2]); 
    +-------------------+----------------+
    | unnest([1, 2, 3]) | unnest([1, 2]) |
    +-------------------+----------------+
    |                 1 |              1 |
    |                 2 |              2 |
    |                 3 |           NULL |
    +-------------------+----------------+
    SELECT 3 rows in set (... sec)


.. note::

    Table functions in the select list are executed after aggregations. So
    aggregations can be used as arguments to table functions, but the other way
    around is not allowed, unless sub queries are utilized.
    (SELECT aggregate_func(col) FROM (SELECT table_func(...) as col) ...)

.. NOTE::

  In addition to the pure table functions listed below, :ref:`scalar`
  can also be used :ref:`in the context of the FROM clause
  <table-functions-scalar>`.

.. rubric:: Table of Contents

.. contents::
   :local:

``empty_row( )``
================
empty_row doesn't take any argument and produces a table with an empty row and
no column.

::

    cr> select * from empty_row();
    SELECT OK, 1 row affected  (... sec)


.. _unnest:

``unnest( array [ array , ] )``
===============================

unnest takes any number of array parameters and produces a table where each
provided array argument results in a column.

The columns are named ``colN`` where N is a number starting at 1.

::

    cr> select * from unnest([1, 2, 3], ['Arthur', 'Trillian', 'Marvin']);
    +------+----------+
    | col1 | col2     |
    +------+----------+
    |    1 | Arthur   |
    |    2 | Trillian |
    |    3 | Marvin   |
    +------+----------+
    SELECT 3 rows in set (... sec)

If an array with object literals is passed into unnest the object will be
regarded as a object with column policy ``ignored``. This means that it is not
possible to access values of the object using the subscript notation::

    cr> select col1['x'] from unnest([{x=10}]);
    SQLActionException[ColumnUnknownException: Column col1['x'] unknown]

.. table-functions-generate-series:

``generate_series(start, stop, [step])``
========================================

Generate a series of values from inclusive start to inclusive stop with
``step`` increments.
The ``step`` parameter is optional and defaults to 1.

The arguments can be either of type ``integer`` or ``bigint`` and the return
value will match the argument types.

::

    cr> SELECT * FROM generate_series(1, 4);
    +------+
    | col1 |
    +------+
    |    1 |
    |    2 |
    |    3 |
    |    4 |
    +------+
    SELECT 4 rows in set (... sec)

.. _table-functions-scalar:

Scalar functions
================

:ref:`scalar` can also return a set of rows when used in the ``FROM`` clause
of a query as a table or subquery. The result set of such functions is always
a relation of one row and one column which contains the value returned from
the scalar function.

::

    cr> SELECT * FROM abs(-5), initcap('hello world');
    +-----+-------------+
    | abs | initcap     |
    +-----+-------------+
    |   5 | Hello World |
    +-----+-------------+
    SELECT 1 row in set (... sec)

