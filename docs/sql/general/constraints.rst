.. highlight:: psql
.. _table_constraints:

===========
Constraints
===========

.. rubric:: Table of contents

.. contents::
   :local:

Tables Constraints
==================

Table constraints are constraints that are applied to the table as a whole.

.. _primary_key_constraint:

``PRIMARY KEY``
---------------

The PRIMARY KEY constraint specifies that a column or columns of a table can
contain only unique (non-duplicate), non-null values.

Using columns of type ``object``, ``geo_point``, ``geo_shape`` or ``array`` as
PRIMARY KEY is not supported.

To use a whole ``object`` as PRIMARY KEY each column within the ``object`` can
be declared as PRIMARY KEY instead.

Adding a PRIMARY KEY column is only possible if the table is empty.

.. WARNING::

    The verification if the table is empty and the schema update isn't atomic.
    That means that it could be possible to add a primary key column to a table
    that isn't empty.

    If that is the case queries that contain the primary key columns in the
    WHERE clause will not behave as expected.

.. _index-constraint:

``INDEX``
---------

The INDEX constraint specifies a specific index method on one or more columns.

It is possible to define more than one index per table, whether as a column
constraint or a table constraint.

For further details about the meaning of the options see
:ref:`indices_and_fulltext`.

.. _check_constraint:

``CHECK``
---------

The CHECK constraint specifies that the value/s of certain column/s must satisfy
a boolean expression on insert and update.

Syntax:

::

  check(boolean_expression)
  constraint <name> check(boolean_expression)

With two options:

1) As many check constraints as required, as additional elements of
   a create table statement.
   When name is omitted, one is generated with pattern `fqtn_check_uuid`,
   where:

   - `fqtb`: full qualified table name.
   - `check`: check.
   - `uuid`: 6 random hexadecimal digits (2 octets),
     e.g. doc_my_table_check_bd3f57640937.

2) A single check constraint is allowed as part of a column definition.
   When name is omitted, the pattern is `fqtn_column_check_uuid`,
   where:

   - `column`: column name.

Example:

::

  cr> create table my_table (
  ...    first_column integer primary key,
  ...    second_column integer check(second_column >= 0),
  ...    third_column string constraint not_you check(third_column <> 'me'),
  ...    check(first_column >= 100),
  ...    constraint is_even check(second_column % 2 = 0)
  ...);
  CREATE OK, 1 row affected  (... sec)

::

  cr> select table_schema, table_name, constraint_type, constraint_name
  ...     from information_schema.table_constraints
  ...     where table_name = 'my_table';

.. WARNING::

   CHECK constraint conditions must be immutable, always giving the same
   result for the same input.
   A way to break this is to reference a user-defined function in a CHECK
   expression, and then change the behavior of that function. Some existing
   rows in the table could now violate the CHECK constraint. That would
   cause a subsequent database dump and reload to fail.

.. hide:

   cr> drop table my_table;
   DROP OK, 1 row affected (... sec)

.. _column_constraints:

Column Constraints
==================

Column constraints are constraints that are applied on each column of the table
separately.

The supported column constraints are:

 * :ref:`not_null_constraint`
 * :ref:`primary_key_constraint`
 * :ref:`check_constraint`

.. _not_null_constraint:

``NOT NULL``
------------

The NOT NULL constraint specifies that a column of a table can contain only
non-null values.

The columns that are part of the primary key of a table are NOT NULL by
default.
