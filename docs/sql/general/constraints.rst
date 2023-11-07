.. highlight:: psql
.. _table_constraints:

===========
Constraints
===========

.. rubric:: Table of contents

.. contents::
   :local:


Table constraints
=================

Table constraints are constraints that are applied to the table as a whole.


.. _primary_key_constraint:

``PRIMARY KEY``
---------------

The ``PRIMARY KEY`` constraint specifies that a column or columns of a table
can contain only unique (non-duplicate), non-null values.

Using columns of type ``object``, ``geo_point``, ``geo_shape`` or ``array`` as
``PRIMARY KEY`` is not supported.

To use a whole ``object`` as ``PRIMARY KEY`` each column within the ``object``
can be declared as ``PRIMARY KEY`` instead.

Adding a ``PRIMARY KEY`` column is only possible if the table is empty.

Syntax::

  [CONSTRAINT <name>] PRIMARY KEY [ column_name [, ... ] ]

For example, a table with a named ``PRIMARY KEY`` constraint can be created
with::

    cr> CREATE TABLE person (
    ...     firstname TEXT,
    ...     lastname TEXT,
    ...     CONSTRAINT c PRIMARY KEY (firstname, lastname)
    ... );
    CREATE OK, 1 row affected  (... sec)

The named ``PRIMARY KEY`` constraints can be inlined::

    cr> CREATE TABLE person2 (
    ...     firstname TEXT CONSTRAINT c PRIMARY KEY,
    ...     lastname TEXT CONSTRAINT c PRIMARY KEY
    ... );
    CREATE OK, 1 row affected  (... sec)

If a new column is required to be added as a ``PRIMARY KEY`` column::

    cr> ALTER TABLE person2 ADD COLUMN middleName text PRIMARY KEY;
    ALTER OK, -1 rows affected  (... sec)

The ``PRIMARY KEY`` constraint can also be unnamed, e.g.::

    cr> CREATE TABLE person3 (
    ...     firstname TEXT PRIMARY KEY,
    ...     lastname TEXT PRIMARY KEY
    ... );
    CREATE OK, 1 row affected  (... sec)

If ``CONSTRAINT <name>`` is omitted, CrateDB generates a unique name
automatically.  This name is visible in
:ref:`information_schema_table_constraints`.



.. WARNING::

    The verification if the table is empty and the schema update isn't atomic.
    That means that it could be possible to add a primary key column to a table
    that isn't empty.

    If that is the case queries that contain the primary key columns in the
    ``WHERE`` clause will not behave as expected.


.. _index-constraint:

``INDEX``
---------

The ``INDEX`` constraint specifies a specific index method on one or more
columns.

It is possible to define more than one index per table, whether as a column
constraint or a table constraint.

.. SEEALSO::

    :ref:`indices_and_fulltext`


.. _check_constraint:

``CHECK``
---------

The ``CHECK`` constraint specifies that the values of certain columns must
satisfy a :ref:`boolean expression <sql-literal-value>` on ``INSERT`` and
``UPDATE``.

Syntax::

  [CONSTRAINT <check_name>] CHECK (boolean_expression)

If ``CONSTRAINT <check_name>`` is omitted, CrateDB generates a unique name
automatically.  This name is visible in
:ref:`information_schema_table_constraints`. This name can be used with
:ref:`DROP CONSTRAINT <sql-alter-drop-constraint>` to remove the constraint.

The ``CONSTRAINT`` definition can either be inlined with a column, like this::

    cr> CREATE TABLE metrics1 (
    ...     weight REAL CONSTRAINT weight_is_positive CHECK (weight >= 0)
    ... );
    CREATE OK, 1 row affected  (... sec)

Or, also inlined, but without explicit name::

    cr> CREATE TABLE metrics2 (
    ...     weight REAL CHECK (weight >= 0)
    ... );
    CREATE OK, 1 row affected  (... sec)

Or, on a table level with explicit name::

    cr> CREATE TABLE metrics3 (
    ...     weight REAL,
    ...     CONSTRAINT weight_is_positive CHECK (weight >= 0)
    ... );
    CREATE OK, 1 row affected  (... sec)

Or without name::

    cr> CREATE TABLE metrics4 (
    ...     weight REAL,
    ...     CHECK (weight >= 0)
    ... );
    CREATE OK, 1 row affected  (... sec)

.. _check_constraint_multiple_cols:

You can reference multiple columns using table constraints::

    cr> CREATE TABLE metrics5 (
    ...     weight REAL,
    ...     qty INTEGER,
    ...     CHECK (weight * qty != 1918)
    ... );
    CREATE OK, 1 row affected  (... sec)


.. WARNING::

   The ``CHECK`` constraint conditions must be deterministic, always yielding
   the same result for the same input.

   A way to break this is to reference a :ref:`user-defined function
   <user-defined-functions>` in a ``CHECK`` expression, and then change the
   behavior of that function. Some existing rows in the table could now violate
   the ``CHECK`` constraint. That would cause a subsequent database dump and
   reload to fail.

.. NOTE::

   To add a ``CHECK`` constraint to a sub-column of an object column you must
   address the sub-column by it's full path::

      cr> CREATE TABLE metrics6 (properties OBJECT AS (weight INTEGER CHECK (properties['weight'] >= 0)))
      CREATE OK, 1 row affected (... sec)

.. hide:

   cr> drop table metrics1;
   DROP OK, 1 row affected (... sec)
   cr> drop table metrics2;
   DROP OK, 1 row affected (... sec)
   cr> drop table metrics3;
   DROP OK, 1 row affected (... sec)
   cr> drop table metrics4;
   DROP OK, 1 row affected (... sec)
   cr> drop table metrics5;
   DROP OK, 1 row affected (... sec)
   cr> drop table metrics6;
   DROP OK, 1 row affected (... sec)


.. _column_constraints:

Column constraints
==================

Column constraints are constraints that are applied on each column of the table
separately.

The supported column constraints are:

- :ref:`not_null_constraint`

- :ref:`primary_key_constraint`

- :ref:`check_constraint`


.. _null_constraint:

``NULL``
--------

The ``NULL`` constraint specifies that a column of a table can also contain
null values.

The columns that are part of the primary key of a table cannot be declared as
``NULL``.

A column cannot be declared both as ``NULL`` and ``NOT NULL``.

.. NOTE::

    ``NULL`` constraint is not shown in :ref:`ref-show-create-table`, as is the
    default for every column.


.. _not_null_constraint:

``NOT NULL``
------------

The ``NOT NULL`` constraint specifies that a column of a table can contain only
non-null values.

The columns that are part of the primary key of a table are ``NOT NULL`` by
default.
