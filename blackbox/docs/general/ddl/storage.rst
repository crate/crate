.. _ddl-storage:

=========
 Storage
=========

Data storage options can be tuned for each column similar to how indexing is defined.

.. _ddl-storage-columnstore:

Column Store
============

Beside of storing the row data as-is (and indexing each value by default), each
value term is stored into a `Column Store`_ by default. The usage of a `Column
Store`_ is greatly improving global aggregations and groupings and enables
ordering possiblity as the data for one column is packed at one place. Using the
`Column Store`_ limits the values of :ref:`data-type-string` columns to a
maximal length of 32766 bytes.

Turning off the `Column Store`_ in conjunction of :ref:`turning off indexing
<sql_ddl_index_off>` will remove the length limitation.

Example:
::

    cr> CREATE TABLE t1 (
    ...   id INTEGER,
    ...   url STRING INDEX OFF STORAGE WITH (columnstore = false)
    ... );
    CREATE OK, 1 row affected  (... sec)

Doing so will enable support for inserting strings longer than 32766 bytes into
the ``url`` column, but the performance for global aggregations, groupings and
sorting using this ``url`` column will decrease.

.. hide:

    cr> drop table t1;
    DROP OK, 1 row affected  (... sec)

Supported data types
--------------------

Controling if values are stored into a `Column Store`_ is only supported on
follwing data types:

 - :ref:`data-type-string`

For all other :ref:`sql_ddl_datatypes_primitives`, it is enabled by default and
cannot be disabled. :ref:`sql_ddl_datatypes_compound` and
:ref:`sql_ddl_datatypes_geographic` do not support storing values into a
`Column Store`_ at all.

.. _Column Store: https://en.wikipedia.org/wiki/Column-oriented_DBMS
