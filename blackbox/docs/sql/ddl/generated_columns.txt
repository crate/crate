.. _sql-ddl-generated-columns:

=================
Generated Columns
=================

It is possible to define columns whose value is computed by applying a
*generation expression* in the context of the current row, where it is possible
to reference the values of other columns.

Generated columns are defined by providing a *generation expression*. Providing
a data type is optional. It is inferred by the return type of the supplied
expression if omitted::

    cr> CREATE TABLE computed (
    ...   dividend double,
    ...   divisor double,
    ...   quotient AS (dividend / divisor)
    ... );
    CREATE OK, 1 row affected (... sec)

For a full syntax description, see :ref:`ref-create-table`.

Generated columns are read-only as they are computed internally. They are
computed upon ``INSERT`` and ``UPDATE``::

    cr> INSERT INTO computed (dividend, divisor) VALUES (1.7, 1.5), (0.0, 10.0);
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

If values are supplied for generated columns, these values are validated
against the result of applying the *generation expression*::

    cr> INSERT INTO computed (dividend, divisor, quotient) VALUES (100.0, 2.0, 12.0);
    SQLActionException[SQLParseException: Given value 12.0 for generated column does not match defined generated expression value 50.0]

.. WARNING::

   Supplied values for generated columns are not validated when they are
   imported using ``copy from``.

They can also be used in the :ref:`partitioned_by_clause` in order to compute
the value to partition by from existing columns in the table::

    cr> CREATE TABLE computed_and_partitioned (
    ...   huge_cardinality long,
    ...   big_data string,
    ...   partition_value AS ( huge_cardinality % 10 )
    ... ) PARTITIONED BY (partition_value);
    CREATE OK, 1 row affected (... sec)

.. Hidden: drop tables::

    cr> DROP TABLE computed;
    DROP OK, 1 row affected (... sec)
    cr> DROP TABLE computed_and_partitioned;
    DROP OK, 1 row affected (... sec)
