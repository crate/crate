.. highlight:: psql
.. _ref-values:

==========
``VALUES``
==========

``VALUES`` computes a set of rows.


Synopsis
========

::

    VALUES ( expression [, ...] ) [, ...]


Description
===========

``VALUES`` can be used to generate a result set containing constant values.

When more than 1 row is specified, all rows must have the same number of
elements.

An example::

   cr> VALUES (1, 'one'), (2, 'two'), (3, 'three');
   +------+-------+
   | col1 | col2  |
   +------+-------+
   |    1 | one   |
   |    2 | two   |
   |    3 | three |
   +------+-------+
   VALUES 3 rows in set (... sec)


It is commonly used in :ref:`ref-insert` to provide values to insert into a
table.

All expressions within the same column must have the same type.
