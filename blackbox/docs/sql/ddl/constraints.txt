===========
Constraints
===========

Columns can be constrained in two ways:

.. contents::
   :local:

The values of a constrained column must comply with the constraint.

Primary Key
===========

The primary key constraint combines a unique constraint and a not-null
constraint. It also defines the default routing value used for sharding.

Example::

    cr> create table my_table1 (
    ...   first_column integer primary key,
    ...   second_column string
    ... );
    CREATE OK, 1 row affected (... sec)

Currently primary keys cannot be auto generated and have to be specified if
data is inserted, otherwise an error is returned.

Defining multiple columns with a primary key constraint is also supported::

    cr> create table my_table1pk (
    ...   first_column integer primary key,
    ...   second_column string primary key,
    ...   third_column string
    ... );
    CREATE OK, 1 row affected (... sec)

Or using a alternate syntax::

    cr> create table my_table1pk1 (
    ...   first_column integer,
    ...   second_column string,
    ...   third_column string,
    ...   primary key (first_column, second_column)
    ... );
    CREATE OK, 1 row affected (... sec)

.. NOTE::

   Not all column types can be used as PRIMARY KEY.

   See :ref:`primary_key_constraint`.

Not Null
========

The not null constraint can be used on any table column and it prevents null
values from being inserted.

Example::

    cr> create table my_table2 (
    ...   first_column integer primary key,
    ...   second_column string not null
    ... );
    CREATE OK, 1 row affected (... sec)

.. hide:

    cr> drop table my_table2;
    DROP OK, 1 row affected (... sec)

For further details see :ref:`not_null_constraint`.
