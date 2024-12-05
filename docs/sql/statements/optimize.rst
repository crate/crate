.. highlight:: psql

.. _sql-optimize:

============
``OPTIMIZE``
============

Optimize one or more tables explicitly.

.. rubric:: Table of contents

.. contents::
   :local:


.. _sql-optimize-synopsis:

Synopsis
========

::

    OPTIMIZE TABLE table_ident [ PARTITION (partition_column=value [ , ... ]) ] [, ...]
    [ WITH ( optimization_parameter [= value] [, ... ] ) ]


.. _sql-optimize-description:

Description
===========

The OPTIMIZE TABLE command optimizes tables and table partitions by merging the
segments of a table or a partition and reducing their number.  It will also
drop deleted rows, and where possible remove redundant source and soft-delete
data structures for rows that have been fully replicated.

This command will block until the optimization process is complete. If
the connection to CrateDB is lost, the request will continue in the background,
and any new requests will block until the previous optimization is complete.

The ``PARTITION`` clause can be used to only optimize specific partitions of a
partitioned table. Specified values for all :ref:`partition columns
<gloss-partition-column>` are required.

In case the ``PARTITION`` clause is omitted all open partitions will be
optimized. Closed partitions are not optimized.
For performance reasons doing that should be avoided if possible.

See :ref:`partitioned-tables` for more information on partitioned tables.

For further information see :ref:`optimize`.

.. NOTE::

    System tables cannot be optimized.


.. _sql-optimize-parameters:

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of an existing table that is to
  be optimized.


.. _sql-optimize-clauses:

Clauses
=======


.. _sql-optimize-partition:

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
``PARTITION`` clause can be used to optimize one partition exclusively.

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
partition to optimize.

.. TIP::

    The :ref:`ref-show-create-table` statement will show you the complete list
    of partition columns specified by the
    :ref:`sql-create-table-partitioned-by` clause.


.. _sql-optimize-with:

``WITH``
--------

The optional WITH clause can specify parameters for the optimization request.

::

    [ WITH ( optimization_parameter [= value] [, ... ] ) ]

:optimization_parameter:
  Specifies an optional parameter for the optimization request.

Available parameters are:

:max_num_segments:
  The number of segments to merge to. To fully merge the table or
  partition set it to ``1``.

  Defaults to simply checking if a merge is necessary, and if so,
  executes it.

  .. CAUTION::

    Forcing a merge to a small number of segments can harm query performance
    if segments become too big.

    If ``max_num_segments`` gets omitted, CrateDB will automatically determine
    the ideal number of segments based on internal criteria.

:only_expunge_deletes:
  Should the merge process only expunge segments with deletes in it.

  In CrateDB, a row is not deleted from a segment, just marked as
  deleted. During a merge process of segments, a new segment is created
  that does not have those deletes. This flag allows to only merge
  segments that have deletes.

  Defaults to ``false``.

:flush:
  Instructs if a flush should be performed after the optimization.

  Defaults to ``true``.

:upgrade_segments:

  This option is deprecated and has no effect anymore.
