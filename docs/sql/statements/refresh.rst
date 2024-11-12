.. highlight:: psql

.. _sql-refresh:

===========
``REFRESH``
===========

Refresh one or more tables explicitly.

.. rubric:: Table of contents

.. contents::
   :local:


.. _sql-refresh-synopsis:

Synopsis
========

::

    REFRESH TABLE (table_ident [ PARTITION (partition_column=value [ , ... ])] [, ...] )


.. _sql-refresh-description:

Description
===========

The REFRESH TABLE command refreshes one or more tables, making all changes made
to that table visible to subsequent commands.

The ``PARTITION`` clause can be used to refresh specific partitions of a
partitioned table instead of all partitions. The ``PARTITION`` clause requires
a list of all :ref:`partition columns <gloss-partitioned-column>` as an
argument.

In case the ``PARTITION`` clause is omitted all open partitions will be
refreshed. Closed partitions are not refreshed.

.. SEEALSO::

    :ref:`partitioned-tables`

For performance reasons, refreshing tables or partitions should be avoided as
it is an expensive operation. By default CrateDB periodically refreshes the
tables anyway. See :ref:`refresh_data` and
:ref:`sql-create-table-refresh-interval` for more information about the
periodic refreshes.

Without an explicit ``REFRESH``, other statements like ``UPDATE``, ``DELETE``
or ``SELECT`` won't see data until the periodic refresh happens.

An exception to that are statements which can filter on a primary key with an
exact match on all primary key values within a record. For example, looking up
a single document in a table with a single primary key column::

    WHERE pk = 'ID1'

If the primary key consists of multiple columns it would look like this::

    WHERE pk1 = 'ID_PART_1' AND pk2 = 'ID_PART_2'

Or if you want to query multiple records::

    WHERE pk = 'ID1' OR pk = 'ID2' OR pk = 'ID3'


These kind of filters will result in a primary key lookup. You can use the
:ref:`EXPLAIN <ref-explain>` statement to verify if this is the case::

    cr> CREATE TABLE pk_demo (id int primary key);
    CREATE OK, 1 row affected  (... sec)

    cr> EXPLAIN SELECT * FROM pk_demo WHERE id = 1;
    +--------------------------------------------------------+
    | QUERY PLAN                                             |
    +--------------------------------------------------------+
    | Get[doc.pk_demo | id | DocKeys{1} | (id = 1)] (rows=1) |
    +--------------------------------------------------------+
    EXPLAIN 1 row in set (... sec)


This lists a ``Get`` :ref:`operator <gloss-operator>`, which is the internal
operator name for a primary key lookup. Compare this with the following
output::

    cr> EXPLAIN SELECT * FROM pk_demo WHERE id > 1;
    +-------------------------------------------------------+
    | QUERY PLAN                                            |
    +-------------------------------------------------------+
    | Collect[doc.pk_demo | [id] | (id > 1)] (rows=unknown) |
    +-------------------------------------------------------+
    EXPLAIN 1 row in set (... sec)


The filter changed to ``id > 1``, in this case CrateDB can no longer use a
primary key lookup and the used operator changed to a ``Collect`` operator.

To avoid the need for manual refreshes it can be useful to make use of primary
key lookups, as they see the data even if the table hasn't been refreshed yet.

See also :ref:`concept-consistency`.


.. _sql-refresh-description_collect_exception:

.. NOTE::

    Due to internal constraints, when the ``WHERE`` clause filters on multiple
    columns of a ``PRIMARY KEY``, but one or more of those columns is tested
    against lots of values, the query might be executed using a ``Collect``
    operator instead of a ``Get``, thus records might be unavailable until a
    ``REFRESH`` is run. The same situation could occur when the ``WHERE`` clause
    contains long complex expressions, e.g.::

        SELECT * FROM t
        WHERE pk1 IN (<long_list_of_values>) AND pk2 = 3 AND pk3 = 'foo'

        SELECT * FROM t
        WHERE pk1 = ?
            AND pk2 = ?
            AND pk3 = ?
            OR pk1 = ?
            AND pk2 = ?
            AND pk3 = ?
            OR pk1 = ?
            ...

.. _sql-refresh-parameters:

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of an existing table that is to
  be refreshed.


.. _sql-refresh-clauses:

Clauses
=======


.. _sql-refresh-partition:

``PARTITION``
-------------

.. EDITORIAL NOTE
   ##############

   Multiple files (in this directory) use the same standard text for
   documenting the ``PARTITION`` clause. (Minor verb changes are made to
   accomodate the specifics of the parent statement.)

   For consistency, if you make changes here, please be sure to make a
   corresponding change to the other files.

If the table is :ref:`partitioned <partitioned-tables>`, the optional
``PARTITION`` clause can be used to refresh one partition exclusively.

::

    [ PARTITION ( partition_column = value [ , ... ] ) ]

:partition_column:
  One of the column names used for table partitioning.

:value:
  The respective column value.

All :ref:`partition columns <gloss-partition-column>` (specified by the
:ref:`sql-create-table-partitioned-by` clause) must be listed inside the
parentheses along with their respective values using the ``partition_column =
value`` syntax (separated by commas).

Because each partition corresponds to a unique set of :ref:`partition column
<gloss-partition-column>` row values, this clause uniquely identifies a single
partition to refresh.

.. TIP::

    The :ref:`ref-show-create-table` statement will show you the complete list
    of partition columns specified by the
    :ref:`sql-create-table-partitioned-by` clause.
