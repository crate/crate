.. highlight:: psql

.. _arithmetic:

====================
Arithmetic operators
====================

Arithmetic :ref:`operators <gloss-operator>` perform mathematical operations on
numeric values (including timestamps):

========   =========================================================
Operator   Description
========   =========================================================
``+``      Add one number to another
``-``      Subtract the second number from the first
``*``      Multiply the first number with the second
``/``      Divide the first number by the second
``%``      Finds the remainder of division of one number by another
========   =========================================================

Here's an example that uses all of the available arithmetic operators::

    cr> select ((2 * 4.0 - 2 + 1) / 2) % 3 AS n;
    +-----+
    |   n |
    +-----+
    | 0.5 |
    +-----+
    SELECT 1 row in set (... sec)

Arithmetic operators always return the data type of the argument with the
higher precision.

In the case of devision, if both arguments are integers, the result will also
be an integer with the fractional part truncated::

    cr> select 5 / 2 AS a,  5 / 2.0 AS b;
    +---+-----+
    | a |   b |
    +---+-----+
    | 2 | 2.5 |
    +---+-----+
    SELECT 1 row in set (... sec)

.. NOTE::

    The same restrictions that apply to :ref:`scalar functions <scalar>` also
    apply to arithmetic operators.

