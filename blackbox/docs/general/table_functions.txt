.. highlight:: psql

.. _ref-table-functions:

===============
Table Functions
===============

Table functions are functions that produce a set of rows. They are used like a
table or subquery in the FROM clause of a query.

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
