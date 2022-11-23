.. highlight:: psql

.. _bit-operators:

=============
Bit operators
=============

Bit :ref:`operators <gloss-operator>` perform bitwise operations on numeric
integral values and :ref:`bit <data-type-bit>` strings:

========  ========================
Operator  Description
========  ========================
``&``     Bitwise AND of operands.
``|``     Bitwise OR of operands.
``#``     Bitwise XOR of operands.
========  ========================

Here's an example that uses all of the available bit operators::

    cr> select 1 & 2 | 3 # 4 AS n;
    +---+
    | n |
    +---+
    | 7 |
    +---+
    SELECT 1 row in set (... sec)

And an example with bit strings::

    cr> select B'101' # B'011' AS n;
    +--------+
    | n      |
    +--------+
    | B'110' |
    +--------+
    SELECT 1 row in set (... sec)

When applied to numeric operands, bit operators always return the data type
of the argument with the higher precision.

If at least one operand is ``NULL``, bit operators return ``NULL``.

When applied to ``BIT`` strings, operands must have equal length.

.. NOTE::

    Bit operators have the same precedence and evaluated from left to right.
    Use parentheses if you want to ensure a specific order of evaluation.
