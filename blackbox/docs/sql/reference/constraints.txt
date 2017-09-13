.. highlight:: psql
.. _table_constraints:

===========
Constraints
===========

.. rubric:: Table of Contents

.. contents::
   :local:

.. _primary_key_constraint:

Tables Constraints
==================

Table constraints are constraints that are applied to the table as a whole.

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

.. _column_constraints:

Column Constraints
==================

Column constraints are constraints that are applied on each column of the table
separately.

The supported column constraints are:

 * :ref:`not_null_constraint`
 * :ref:`primary_key_constraint`

.. _not_null_constraint:

``NOT NULL``
------------

The NOT NULL constraint specifies that a column of a table can contain only
non-null values.

The columns that are part of the primary key of a table are NOT NULL by
default.
