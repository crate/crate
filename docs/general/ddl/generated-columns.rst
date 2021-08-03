.. highlight:: psql

.. _ddl-generated-columns:

=================
Generated columns
=================

It is possible to define columns whose value is computed by applying a
*generation expression* in the context of the current row. The generation
:ref:`expression <gloss-expression>` can reference the values of other columns.

.. rubric:: Table of contents

.. contents::
   :local:


.. _ddl-generated-columns-expressions:

Generation expressions
======================

Generated columns are defined by providing a generation expression. Providing
a data type is optional. It is inferred by the return type of the supplied
expression if omitted::

    cr> CREATE TABLE computed (
    ...   dividend double precision,
    ...   divisor double precision,
    ...   quotient GENERATED ALWAYS AS (dividend / divisor)
    ... );
    CREATE OK, 1 row affected (... sec)

.. SEEALSO::

   For a full syntax description, see :ref:`sql-create-table`.

Generated columns are read-only. Their values are computed as needed for every
``INSERT`` and ``UPDATE`` operation.

For example::

    cr> INSERT INTO computed (dividend, divisor)
    ... VALUES (1.7, 1.5), (0.0, 10.0);
    INSERT OK, 2 rows affected (... sec)

.. Hidden: Refresh::

    cr> refresh table computed;
    REFRESH OK, 1 row affected (... sec)

The generated column is now filled with the computed value::

    cr> SELECT dividend, divisor, quotient
    ... FROM computed
    ... ORDER BY quotient;
    +----------+---------+--------------------+
    | dividend | divisor |           quotient |
    +----------+---------+--------------------+
    |      0.0 |    10.0 | 0.0                |
    |      1.7 |     1.5 | 1.1333333333333333 |
    +----------+---------+--------------------+
    SELECT 2 rows in set (... sec)

The generation expression is :ref:`evaluated <gloss-evaluation>` in the context
of the current row. This means that you can compute a generated value from the
values of base columns in the same row. However, it is not possible to
reference other generated columns from within a generation expression.

.. NOTE::

   If the generated expression is deterministic, its value will not be
   recomputed unless the value of a referenced column has changed.

If values are supplied for generated columns, these values are validated
against the result of applying the generation expression::

    cr> INSERT INTO computed (dividend, divisor, quotient)
    ... VALUES (100.0, 2.0, 12.0);
    SQLParseException[Given value 12.0 for generated column quotient does not match calculation (dividend / divisor) = 50.0]

.. WARNING::

   Supplied values for generated columns are not validated when they are
   imported using ``COPY FROM``.


.. _ddl-generated-columns-last-modified:

Last modified dates
===================

Because :ref:`scalar-current_timestamp` is non-deterministic, you can use this
expression to record a last modified date that is set when the row is first
inserted, and subsequently updated every time the row is updated::

    cr> CREATE TABLE computed_non_deterministic (
    ...   id LONG,
    ...   last_modified TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS CURRENT_TIMESTAMP
    ... );
    CREATE OK, 1 row affected (... sec)


.. _ddl-generated-columns-partitioning:

Partitioning
============

Generated columns can be used with the :ref:`sql-create-table-partitioned-by`
clause to compute the :ref:`partition column <gloss-partition-column>` value
from existing columns in the table::

    cr> CREATE TABLE computed_and_partitioned (
    ...   huge_cardinality bigint,
    ...   big_data text,
    ...   partition_value GENERATED ALWAYS AS (huge_cardinality % 10)
    ... ) PARTITIONED BY (partition_value);
    CREATE OK, 1 row affected (... sec)

.. SEEALSO::

    :ref:`Partitioned tables: Generated columns <partitioned-generated>`

.. Hidden: drop tables::

    cr> DROP TABLE computed;
    DROP OK, 1 row affected (... sec)
    cr> DROP TABLE computed_non_deterministic;
    DROP OK, 1 row affected (... sec)
    cr> DROP TABLE computed_and_partitioned;
    DROP OK, 1 row affected (... sec)
