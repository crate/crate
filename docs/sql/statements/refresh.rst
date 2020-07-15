.. highlight:: psql
.. _sql_ref_refresh:

===========
``REFRESH``
===========

Refresh one or more tables explicitly.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

    REFRESH TABLE (table_ident [ PARTITION (partition_column=value [ , ... ])] [, ...] )

Description
===========

The REFRESH TABLE command refreshes one or more tables, making all changes made
to that table visible to subsequent commands.

The ``PARTITION`` clause can be used to refresh specific partitions of a
partitioned table instead of all partitions. The ``PARTITION`` clause requires
all partitioned columns with the values that identify the partition as
argument.

In case the ``PARTITION`` clause is omitted all open partitions will be
refreshed. Closed partitions are not refreshed.

See :ref:`partitioned_tables` for more information on partitioned tables.


For performance reasons, refreshing tables or partitions should be avoided as
it is an expensive operation. By default CrateDB periodically refreshes the
tables anyway. See :ref:`refresh_data` and :ref:`sql_ref_refresh_interval` for
more information about the periodic refreshes.

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
    +------------------------------------+
    | EXPLAIN                            |
    +------------------------------------+
    | Get[doc.pk_demo | id | DocKeys{1}] |
    +------------------------------------+
    EXPLAIN 1 row in set (... sec)


This lists a ``Get`` operator, which is the internal operator name for a
primary key lookup. Compare this with the following output::

    cr> EXPLAIN SELECT * FROM pk_demo WHERE id > 1;
    +----------------------------------------+
    | EXPLAIN                                |
    +----------------------------------------+
    | Collect[doc.pk_demo | [id] | (id > 1)] |
    +----------------------------------------+
    EXPLAIN 1 row in set (... sec)


The filter changed to ``id > 1``, in this case CrateDB can no longer use a
primary key lookup and the used operator changed to a ``Collect`` operator.


To avoid the need for manual refreshes it can be useful to make use of primary
key lookups, as they see the data even if the table hasn't been refreshed yet.


See also :ref:`consistency`.


Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of an existing table that is to
  be refreshed.

:partition_column:
  Column name by which the table is partitioned.

Clauses
=======

``PARTITION``
-------------

::

    [ PARTITION ( partition_column = value [ , ... ] ) ]

:partition_column:
  The name of the column by which the table is partitioned.

  All partition columns that were part of the :ref:`partitioned_by_clause` of
  the :ref:`ref-create-table` statement must be specified.

:value:
  The columns value.
