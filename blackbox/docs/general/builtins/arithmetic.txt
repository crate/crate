.. highlight:: psql

.. _arithmetic:

====================
Arithmetic Operators
====================

Arithmetic operators perform mathematical operations on two expressions of
numeric data types or timestamps.

.. NOTE::

    The same restrictions that apply to scalar functions also apply to
    arithmetic operators. See :ref:`scalar`.

.. rubric:: Table of Contents

.. contents::
   :local:

Supported operators
-------------------

========   =========================================================
operator   description
========   =========================================================
\+         add one number to another
\-         subtract the second number from the first
\*         multiply the first number with the second
/          divide the first number by the second
%          finds the remainder of division of one number by another
========   =========================================================

Below is an example using all available arithmetic operators::

    cr> select ((2 * 4.0 - 2 + 1) / 2) % 3  from sys.cluster;
    +-----------------------------------+
    | (((((2 * 4.0) - 2) + 1) / 2) % 3) |
    +-----------------------------------+
    | 0.5                               |
    +-----------------------------------+
    SELECT 1 row in set (... sec)

Result types
------------

Arithmetic operators always return the data type of the argument with the
higher precision. In the case of ``divide`` this means that if both arguments
are of type integer the result will also be an integer with the fractional part
truncated::

    cr> select 5 / 2,  5 / 2.0 from sys.cluster;
    +---------+-----------+
    | (5 / 2) | (5 / 2.0) |
    +---------+-----------+
    | 2       | 2.5       |
    +---------+-----------+
    SELECT 1 row in set (... sec)
