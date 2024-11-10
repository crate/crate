.. _sql-group-by-all:

==================
``GROUP BY ALL``
==================

Synopsis
========

::

   SELECT select_expr [, ...]
   FROM table_reference
   GROUP BY ALL
   [HAVING condition]


Description
===========

``GROUP BY ALL`` is a shorthand syntax that automatically groups by all non-aggregate
output columns in the SELECT list. This eliminates the need to explicitly list out
all grouping columns.


Example
=======

Using ``GROUP BY ALL``::

   SELECT department, title, AVG(salary) as avg_salary
   FROM employees
   GROUP BY ALL;

Is equivalent to writing out all non-aggregate columns explicitly::

   SELECT department, title, AVG(salary) as avg_salary
   FROM employees
   GROUP BY department, title;


Parameters
==========

:select_expr:
   The columns and expressions to select. Must be either an aggregate function or
   a column that will be used for grouping.

:table_reference:
   The table to select from.

:HAVING condition:
   An optional condition that filters the grouped results.


Notes
=====

- When using ``GROUP BY ALL``, all non-aggregate columns in the SELECT list are
 automatically used as grouping columns.

- Aggregate functions like COUNT, SUM, AVG etc. are excluded from the automatic
 grouping.

- The order of columns in the grouping is the same as their order in the SELECT
 list.

- Grouping can only be applied on indexed fields.

See Also
========

:ref:`sql-select`, :ref:`sql-group-by`
