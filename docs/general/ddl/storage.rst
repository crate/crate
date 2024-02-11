.. _ddl-storage:

=======
Storage
=======

Data storage options can be tuned for each column similar to how indexing is defined.

.. _ddl-storage-columnstore:

Column store
============

Beside of storing the row data as-is (and indexing each value by default), each
value term is stored into a `Column Store`_ by default. The usage of a `Column
Store`_ is greatly improving global aggregations and groupings and enables
ordering possibility as the data for one column is packed at one place. Using
the `Column Store`_ limits the values of :ref:`type-text` columns to a maximal
length of 32766 bytes.

Turning off the `Column Store`_ in conjunction of :ref:`turning off indexing
<sql_ddl_index_off>` will remove the length limitation.

Example:
::

    cr> CREATE TABLE t1 (
    ...   id INTEGER,
    ...   url TEXT INDEX OFF STORAGE WITH (columnstore = false)
    ... );
    CREATE OK, 1 row affected  (... sec)

Doing so will enable support for inserting strings longer than 32766 bytes into
the ``url`` column, but the performance for global aggregations, groupings and
sorting using this ``url`` column will decrease.

.. NOTE::

    ``INDEX OFF`` and therefore ``columnstore = false`` cannot be used with
    :ref:`partition columns <gloss-partition-column>`, as those are not stored
    as normal columns of a table.

.. hide:

    cr> drop table t1;
    DROP OK, 1 row affected  (... sec)

Supported data types
--------------------

Controlling if values are stored into a `Column Store`_ is only supported on
following data types:

- :ref:`type-text`
- :ref:`data-types-numeric`
- :ref:`type-timestamp`
- :ref:`type-timestamp-with-tz`

For all other :ref:`data-types-primitive` and :ref:`data-types-geo-point` it is
enabled by default and cannot be disabled. :ref:`data-types-container` and
:ref:`data-types-geo-shape` do not support storing values into a
`Column Store`_ at all.

.. _Column Store: https://en.wikipedia.org/wiki/Column-oriented_DBMS
