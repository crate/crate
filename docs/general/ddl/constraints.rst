.. _constraints:

===========
Constraints
===========

Columns can be constrained in three ways:

.. contents::
   :local:

The values of a constrained column must comply with the constraint.


.. _constraints-primary-key:

PRIMARY KEY
===========

The primary key constraint combines a unique constraint and a not-null
constraint. It also defines the default :ref:`routing value
<gloss-routing-column>` used for sharding.

Example::

    cr> create table my_table1 (
    ...   first_column integer primary key,
    ...   second_column text
    ... );
    CREATE OK, 1 row affected (... sec)

Currently primary keys cannot be auto generated and have to be specified if
data is inserted, otherwise an error is returned.

Defining multiple columns with a primary key constraint is also supported::

    cr> create table my_table1pk (
    ...   first_column integer primary key,
    ...   second_column text primary key,
    ...   third_column text
    ... );
    CREATE OK, 1 row affected (... sec)

Or using a alternate syntax::

    cr> create table my_table1pk1 (
    ...   first_column integer,
    ...   second_column text,
    ...   third_column text,
    ...   primary key (first_column, second_column)
    ... );
    CREATE OK, 1 row affected (... sec)

.. NOTE::

   Not all column types can be used as PRIMARY KEY.

   For further details see :ref:`primary_key_constraint`.

.. _constraints-null:

NULL
====

Explicitly states that the column can contain ``NULL`` values. This is the default.

Example::

    cr> create table my_table2 (
    ...   first_column integer primary key,
    ...   second_column text null
    ... );
    CREATE OK, 1 row affected (... sec)

.. SEEALSO::

   - :ref:`null_constraint`


.. _constraints-not-null:

NOT NULL
========

The not null constraint can be used on any table column and it prevents null
values from being inserted.

Example::

    cr> create table my_table3 (
    ...   first_column integer primary key,
    ...   second_column text not null
    ... );
    CREATE OK, 1 row affected (... sec)

.. SEEALSO::

   :ref:`not_null_constraint`


.. _constraints-check:

CHECK
=====

A check constraint allows you to specify that the values in a certain column
must satisfy a :ref:`boolean expression <sql-literal-value>`. This can be used
to ensure data integrity.  For example, if you have a table to store metrics
from sensors and you want to ensure that negative values are rejected::

     cr> create table metrics (
     ...   id TEXT PRIMARY KEY,
     ...   weight double CHECK (weight >= 0)
     ... );
     CREATE OK, 1 row affected  (... sec)

.. NOTE::

   For further details see :ref:`check_constraint`.

.. hide:

    cr> drop table my_table1;
    DROP OK, 1 row affected (... sec)
    cr> drop table my_table1pk;
    DROP OK, 1 row affected (... sec)
    cr> drop table my_table1pk1;
    DROP OK, 1 row affected (... sec)
    cr> drop table my_table2;
    DROP OK, 1 row affected (... sec)
    cr> drop table my_table3;
    DROP OK, 1 row affected (... sec)
    cr> drop table metrics;
    DROP OK, 1 row affected (... sec)
