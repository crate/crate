.. highlight:: psql

.. _sql-create-table:

================
``CREATE TABLE``
================

Create a new table.

.. rubric:: Table of contents

.. contents::
   :local:


.. _sql-create-table-synopsis:

Synopsis
========

::

    CREATE TABLE [ IF NOT EXISTS ] table_ident ( [
        {
            base_column_definition
          | generated_column_definition
          | table_constraint
        }
        [, ... ] ]
    )
    [ PARTITIONED BY (column_name [, ...] ) ]
    [ CLUSTERED [ BY (routing_column) ] INTO num_shards SHARDS ]
    [ WITH ( table_parameter [= value] [, ... ] ) ]

where ``base_column_definition``::

    column_name data_type
    [ DEFAULT default_expr ]
    [ column_constraint [ ... ] ]  [ storage_options ]

where ``generated_column_definition`` is::

    column_name [ data_type ] [ GENERATED ALWAYS ]
    AS [ ( ] generation_expression [ ) ]
    [ column_constraint [ ... ] ]

where ``column_constraint`` is::

    { PRIMARY KEY |
      NOT NULL |
      INDEX { OFF | USING { PLAIN |
                            FULLTEXT [ WITH ( analyzer = analyzer_name ) ]  }
      [ CONSTRAINT constraint_name ] CHECK (boolean_expression)
    }

where ``storage_options`` is::

    STORAGE WITH ( option = value_expression [, ... ] )

and ``table_constraint`` is::

    { PRIMARY KEY ( column_name [, ... ] ) |
      INDEX index_name USING FULLTEXT ( column_name [, ... ] )
           [ WITH ( analyzer = analyzer_name ) ]
      [ CONSTRAINT constraint_name ] CHECK (boolean_expression)
    }


.. _sql-create-table-description:

Description
===========

``CREATE TABLE`` will create a new, initially empty table.

If the ``table_ident`` does not contain a schema, the table is created in the
``doc`` schema. Otherwise it is created in the given schema, which is
implicitly created, if it didn't exist yet.

A table consists of one or more *base columns* and any number of *generated
columns* and/or *table_constraints*.

The optional constraint clauses specify constraints (tests) that new or updated
rows must satisfy for an insert or update operation to succeed. A constraint is
an SQL object that helps define the set of valid values in the table in various
ways.

There are two ways to define constraints: table constraints and column
constraints. A column constraint is defined as part of a column definition. A
table constraint definition is not tied to a particular column, and it can
encompass more than one column. Every column constraint can also be written as
a table constraint; a column constraint is only a notational convenience for
use when the constraint only affects one column.

.. SEEALSO::

    :ref:`Data definition: Creating tables <ddl-create-table>`


.. _sql-create-table-elements:

Table elements
--------------


.. _sql-create-table-base-columns:

Base Columns
~~~~~~~~~~~~

A base column is a persistent column in the table metadata. In relational terms
it is an attribute of the tuple of the table-relation. It has a name, a type,
an optional default clause and optional constraints.

Base columns are readable and writable (if the table itself is writable).
Values for base columns are given in DML statements explicitly or omitted, in
which case their value is null.


.. _sql-create-table-default-clause:

Default clause
^^^^^^^^^^^^^^

The optional default clause defines the default value of the column. The value
is inserted when the column is a target of a ``INSERT`` statement that doesn't
contain an explicit value for it.

The default clause :ref:`expression <gloss-expression>` is variable-free, it
means that subqueries and cross-references to other columns are not allowed.


.. _sql-create-table-generated-columns:

Generated columns
~~~~~~~~~~~~~~~~~

A generated column is a persistent column that is computed as needed from the
``generation_expression`` for every ``INSERT`` and ``UPDATE`` operation.

The ``GENERATED ALWAYS`` part of the syntax is optional.

.. NOTE::

   A generated column is not a virtual column. The computed value is stored in
   the table like a base column is. The automatic computation of the value is
   what makes it different.

.. SEEALSO::

    :ref:`Data definition: Generated columns <ddl-generated-columns>`


.. _sql-create-table-table-constraints:

Table constraints
~~~~~~~~~~~~~~~~~

Table constraints are constraints that are applied to more than one column or
to the table as a whole.

.. SEEALSO::

    - :ref:`General SQL: Table constraints <table_constraints>`
    - :ref:`CHECK constraint <check_constraint>`


.. _sql-create-table-column-constraints:

Column constraints
~~~~~~~~~~~~~~~~~~

Column constraints are constraints that are applied on each column of the table
separately.

.. SEEALSO::

    - :ref:`General SQL: Column constraints <column_constraints>`
    - :ref:`CHECK constraint <check_constraint>`


.. _sql-create-table-storage-options:

Storage options
~~~~~~~~~~~~~~~

Storage options can be applied on each column of the table separately.

.. SEEALSO::

    :ref:`Data definition: Storage <ddl-storage>`


.. _sql-create-table-parameters:

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of the table to be created.

:column_name:
  The name of a column to be created in the new table.

:data_type:
  The :ref:`data type <data-types>` of the column. This can include array and
  object specifiers.

:generation_expression:
  An :ref:`expression <ddl-generated-columns-expressions>` (usually a
  :ref:`function call <sql-function-call>`) that is applied in the context of
  the current row. As such, it can reference other base columns of the table.
  Referencing other generated columns (including itself) is not supported. The
  generation expression is :ref:`evaluated <gloss-evaluation>` each time a row
  is inserted or the referenced base columns are updated.


.. _sql-create-table-if-not-exists:

``IF NOT EXISTS``
=================

If the optional ``IF NOT EXISTS`` clause is used, this statement won't do
anything if the table exists already.


.. _sql-create-table-clustered:

``CLUSTERED``
=============

The optional ``CLUSTERED`` clause specifies how a table should be distributed
accross a cluster.

::

    [ CLUSTERED [ BY (routing_column) ] INTO num_shards SHARDS ]

:num_shards:
  Specifies the number of :ref:`shards <ddl-sharding>` a table is stored
  in. Must be greater than 0. If not provided, the number of shards is
  calculated based on the number of currently active data nodes with the
  following formula::

      num_shards = max(4, num_data_nodes * 2)

  .. NOTE::

     The minimum value of ``num_shards`` is set to ``4``. This means if the
     calculation of ``num_shards`` does not exceeds its minimum it applies the
     minimum value to each table or partition as default.

:routing_column:
  Specify a :ref:`routing column <gloss-routing-column>` that :ref:`determines
  <sharding-routing>` how rows are sharded.

  All rows that have the same ``routing_column`` row value are stored in the
  same shard. If a :ref:`primary key <primary_key_constraint>` has been
  defined, it will be used as the default routing column, otherwise the
  :ref:`internal document ID <sql_administration_system_column_id>` is used.

.. SEEALSO::

    :ref:`Data definition: Sharding <ddl-sharding>`


.. _sql-create-table-partitioned-by:

``PARTITIONED BY``
==================

The ``PARTITIONED`` clause splits the created table into separate
:ref:`partitions <partitioned-tables>` for every distinct combination of row
values in the specified :ref:`partition columns <gloss-partition-column>`.

::

    [ PARTITIONED BY ( column_name [ , ... ] ) ]

:column_name:
  The name of a column to be used for partitioning. Multiple columns names can
  be specified inside the parentheses and must be separated by commas.


The following restrictions apply:

- Partition columns may not be part of the :ref:`sql-create-table-clustered`
  clause

- Partition columns must only contain :ref:`primitive types
  <data-types-primitive>`

- Partition columns may not be inside an object array

- Partition columns may not be indexed with a :ref:`fulltext index with
  analyzer <sql_ddl_index_fulltext>`

- If the table has a :ref:`primary_key_constraint` constraint, all of the
  partition columns must be included in the primary key definition

.. CAUTION::

    Partition columns :ref:`cannot be altered <partitioned-update>` by an
    ``UPDATE`` statement.


.. _sql-create-table-with:

``WITH``
========

The optional ``WITH`` clause can specify parameters for tables.

::

    [ WITH ( table_parameter [= value] [, ... ] ) ]

:table_parameter:
  Specifies an optional parameter for the table.

.. NOTE::

   Some parameters are nested, and therefore need to be wrapped in double
   quotes in order to be set. For example::

       WITH ("allocation.max_retries" = 5)

   Nested parameters are those that contain a ``.`` between parameter names
   (e.g. ``write.wait_for_active_shards``).

Available parameters are:


.. _sql-create-table-number-of-replicas:

``number_of_replicas``
----------------------

Specifies the number or range of replicas each shard of a table should have for
normal operation, the default is to have ``0-1`` replica.

The number of replicas is defined like this::

    min_replicas [ - [ max_replicas ] ]

:min_replicas:
  The minimum number of replicas required.

:max_replicas:
  The maximum number of replicas.

  The actual maximum number of replicas is max(num_replicas, N-1), where N is
  the number of data nodes in the cluster. If ``max_replicas`` is the string
  ``all`` then it will always be N.

.. SEEALSO::

    :ref:`ddl-replication`


.. _sql-create-table-number-of-routing-shards:

``number_of_routing_shards``
----------------------------

This number specifies the hashing space that is used internally to distribute
documents across shards.

This is an optional setting that enables users to later on increase the number
of shards using :ref:`sql-alter-table`.


.. _sql-create-table-refresh-interval:

``refresh_interval``
--------------------

Specifies the refresh interval of a shard in milliseconds. The default is set
to 1000 milliseconds.

:value:
  The refresh interval in milliseconds. A value of smaller or equal than 0
  turns off the automatic refresh. A value of greater than 0 schedules a
  periodic refresh of the table.

.. NOTE::

   A ``refresh_interval`` of 0 does not guarantee that new writes are *NOT*
   visible to subsequent reads. Only the periodic refresh is disabled. There
   are other internal factors that might trigger a refresh.

.. SEEALSO::

    :ref:`Querying: Refresh <refresh_data>`

    :ref:`SQL syntax: REFRESH <sql-refresh>`


.. _sql-create-table-write-wait:

.. _sql-create-table-write-wait-for-active-shards:

``write.wait_for_active_shards``
--------------------------------

Specifies the number of shard copies that need to be active for write
operations to proceed. If less shard copies are active the operation must wait
and retry for up to 30s before timing out.

:value:
  ``all`` or a positive integer up to the total number of configured shard
  copies (``number_of_replicas + 1``).

  A value of ``1`` means only the primary has to be active. A value of ``2``
  means the primary plus one replica shard has to be active, and so on.

  The default value is set to ``1``.

  ``all`` is a special value that means all shards (primary + replicas) must be
  active for write operations to proceed.

Increasing the number of shard copies to wait for improves the resiliency of
the system. It reduces the chance of write operations not writing to the
desired number of shard copies, but it does not eliminate the possibility
completely, because the check occurs before the write operation starts.

Replica shard copies that missed some writes will be brought up to date by the
system eventually, but in case a node holding the primary copy has a system
failure, the replica copy couldn't be promoted automatically as it would lead
to data loss since the system is aware that the replica shard didn't receive
all writes. In such a scenario, :ref:`ALTER TABLE .. REROUTE PROMOTE REPLICA
<alter-table-reroute-promote-replica>` can be used to force the
:ref:`allocation <gloss-shard-recovery>` of a stale replica copy to at least
recover the data that is available in the stale replica copy.

Say you've a 3 node cluster and a table with 1 configured replica. With
``write.wait_for_active_shards=1`` and ``number_of_replicas=1`` a node in the
cluster can be restarted without affecting write operations because the primary
copies are either active or the replicas can be quickly promoted.

If ``write.wait_for_active_shards`` would be set to ``2`` instead and a node is
stopped, the write operations would block until the replica is fully replicated
again or the write operations would timeout in case the replication is not fast
enough.


.. _sql-create-table-blocks:

.. _sql-create-table-blocks-read-only:

``blocks.read_only``
--------------------

Allows to have a read only table.

:value:
  Table is read only if value set to ``true``. Allows writes and table settings
  changes if set to ``false``.


.. _sql-create-table-blocks-read-only-allow-delete:

``blocks.read_only_allow_delete``
---------------------------------

Allows to have a read only table that additionally can be deleted.

:value:
  Table is read only and can be deleted if value set to ``true``. Allows writes
  and table settings changes if set to ``false``.

  When a disk on a node exceeds the
  ``cluster.routing.allocation.disk.watermark.flood_stage`` threshold, this
  block is applied (set to ``true``) to all tables on that affected node. Once
  you've freed disk space again and the threshold is undershot, you need to set
  the ``blocks.read_only_allow_delete`` table setting to ``false``.

.. SEEALSO::

    :ref:`Cluster-wide settings: Disk-based shard allocation
    <conf-routing-allocation-disk>`


.. _sql-create-table-blocks-read:

``blocks.read``
---------------

``disable``/``enable`` all the read operations

:value:
  Set to ``true`` to disable all read operations for a table, otherwise set
  ``false``.


.. _sql-create-table-blocks-write:

``blocks.write``
----------------

``disable``/``enable`` all the write operations

:value:
  Set to ``true`` to disable all write operations and table settings
  modifications, otherwise set ``false``.


.. _sql-create-table-blocks-metadata:

``blocks.metadata``
-------------------

``disable``/``enable`` the table settings modifications.

:values:
  Disables the table settings modifications if set to ``true``. If set to
  ``false``, table settings modifications are enabled.


.. _sql-create-table-soft-deletes:

.. _sql-create-table-soft-deletes-enabled:

``soft_deletes.enabled``
------------------------

Indicates whether soft deletes are enabled or disabled.

Soft deletes allow CrateDB to preserve recent deletions within the Lucene
index. This information is used for :ref:`shard recovery
<gloss-shard-recovery>`.

Before the introduction of soft deletes, CrateDB had to retain the information
in the :ref:`Translog <concept-durability>`. Using soft deletes uses less
storage than the Translog equivalent and is faster.

Soft deletes can only be configured when a table is created. This setting
cannot be changed using ``ALTER TABLE``.

This setting is deprecated and soft deletes will become mandatory in CrateDB
5.0.

:value:
  Defaults to ``true``. Set to ``false`` to disable soft deletes.


.. _sql-create-table-soft-deletes-retention-lease-period:

``soft_deletes.retention_lease.period``
---------------------------------------

The maximum period for which a retention lease is retained before it is
considered expired.

:value:
  ``12h`` (default). Any positive time value is allowed.

CrateDB sometimes needs to replay operations that were executed on one shard on
other shards. For example if a shard copy is temporarily unavailable but write
operations to the primary copy continues, the missed operations have to be
replayed once the shard copy becomes available again.

If soft deletes are enabled, CrateDB uses a Lucene feature to preserve recent
deletions in the Lucene index so that they can be replayed. Because of that,
deleted documents still occupy disk space, which is why CrateDB only preserves
certain recently-deleted documents. CrateDB eventually fully discards deleted
documents to prevent the index growing larger despite having deleted documents.

CrateDB keeps track of operations it expects to need to replay using a
mechanism called *shard history retention leases*. Retention leases are a
mechanism that allows CrateDB to determine which soft-deleted operations can be
safely discarded.

If a shard copy fails, it stops updating its shard history retention lease,
indicating that the soft-deleted operations should be preserved for later
recovery.

However, to prevent CrateDB from holding onto shard retention leases forever,
they expire after ``soft_deletes.retention_lease.period``, which defaults to
``12h``. Once a retention lease has expired CrateDB can again discard
soft-deleted operations. In case a shard copy recovers after a retention lease
has expired, CrateDB will fall back to copying the whole index since it can no
longer replay the missing history.


.. _sql-create-table-codec:

``codec``
---------

By default data is stored using ``LZ4`` compression. This can be changed to
``best_compression`` which uses ``DEFLATE`` for a higher compression ratio, at
the expense of slower column value lookups.

:values:
  ``default`` or ``best_compression``


.. _sql-create-table-store:

.. _sql-create-table-store-type:

``store.type``
--------------

The store type setting allows you to control how data is stored and accessed on
disk. The following storage types are supported:

:fs:
  Default file system implementation. It will pick the best implementation
  depending on the operating environment, which is currently ``hybridfs`` on
  all supported systems but is subject to change.

:simplefs:
  The ``Simple FS`` type is an implementation of file system storage (Lucene
  ``SimpleFsDirectory``) using a random access file.  This implementation has
  poor concurrent performance. It is usually better to use the ``niofs`` when
  you need index persistence.

:niofs:
  The ``NIO FS`` type stores the shard index on the file system (Lucene
  ``NIOFSDirectory``) using NIO. It allows multiple threads to read from the
  same file concurrently.

:mmapfs:
  The ``MMap FS`` type stores the shard index on the file system (Lucene
  ``MMapDirectory``) by mapping a file into memory (mmap).  Memory mapping uses
  up a portion of the virtual memory address space in your process equal to the
  size of the file being mapped. Before using this type, be sure you have
  allowed plenty of virtual address space.

:hybridfs:
  The ``hybridfs`` type is a hybrid of ``niofs`` and ``mmapfs``, which chooses
  the best file system type for each type of file based on the read access
  pattern. Similarly to ``mmapfs`` be sure you have allowed plenty of virtual
  address space.

It is possible to restrict the use of the ``mmapfs`` and ``hybridfs`` store
type via the :ref:`node.store.allow_mmap <node.store_allow_mmap>` node setting.


.. _sql-create-table-mapping:

.. _sql-create-table-mapping-total-fields-limit:

``mapping.total_fields.limit``
------------------------------

Sets the maximum number of columns that is allowed for a table. Default is
``1000``.

:value:
  Maximum amount of fields in the Lucene index mapping. This includes both the
  user facing mapping (columns) and internal fields.


.. _sql-create-table-translog:

.. _sql-create-table-translog-flush-threshold-size:

``translog.flush_threshold_size``
---------------------------------

Sets size of transaction log prior to flushing.

:value:
  Size (bytes) of translog.


.. _sql-create-table-translog-disable-flush:

``translog.disable_flush``
--------------------------

``enable``/``disable`` flushing.

:value:
  Set ``true`` to disable flushing, otherwise set to ``false``.

.. CAUTION::

   It is recommended to use ``disable_flush`` only for short periods of time.


.. _sql-create-table-translog-interval:

``translog.interval``
---------------------

Sets frequency of flush necessity check.

:value:
  Frequency in milliseconds.


.. _sql-create-table-translog-sync-interval:

``translog.sync_interval``
--------------------------

How often the translog is fsynced to disk. Defaults to 5s.  When setting this
interval, please keep in mind that changes logged during this interval and not
synced to disk may get lost in case of a failure. This setting only takes
effect if :ref:`translog.durability <sql-create-table-translog-durability>` is
set to ``ASYNC``.

:value:
  Interval in milliseconds.


.. _sql-create-table-translog-durability:

``translog.durability``
-----------------------

If set to ``ASYNC`` the translog gets flushed to disk in the background every
:ref:`translog.sync_interval <sql-create-table-translog-sync-interval>`. If set
to ``REQUEST`` the flush happens after every operation.

:value:
  ``REQUEST`` (default), ``ASYNC``


.. _sql-create-table-routing:

.. _sql-create-table-routing-allocation:

.. _sql-create-table-routing-allocation.total-shards-per-node:

``routing.allocation.total_shards_per_node``
--------------------------------------------

Controls the total number of shards (replicas and primaries) allowed to be
:ref:`allocated <gloss-shard-allocation>` on a single node. Defaults to
unbounded (-1).

:value:
  Number of shards per node.


.. _sql-create-table-routing-allocation-enable:

``routing.allocation.enable``
-----------------------------

Controls shard :ref:`allocation <gloss-shard-allocation>` for a specific table.
Can be set to:

:all:
  Allows shard allocation for all shards. (Default)

:primaries:
  Allows shard allocation only for primary shards.

:new_primaries:
  Allows shard allocation only for primary shards for new tables.

:none:
  No shard allocation allowed.


.. _sql-create-table-routing-allocation-max-retries:

``routing.allocation.max_retries``
----------------------------------

Defines the number of attempts to :ref:`allocate <gloss-shard-allocation>` a
shard before giving up and leaving the shard unallocated.

:value:
  Number of retries to allocate a shard. Defaults to 5.


.. _sql-create-table-routing-allocation-include:

``routing.allocation.include.{attribute}``
------------------------------------------

Assign the table to a node whose ``{attribute}`` has at least one of the
comma-separated values.

.. SEEALSO::

    :ref:`Data definition: Shard allocation filtering <ddl_shard_allocation>`


.. _sql-create-table-routing-allocation-require:

``routing.allocation.require.{attribute}``
------------------------------------------

Assign the table to a node whose ``{attribute}`` has all of the comma-separated
values.

.. SEEALSO::

    :ref:`Data definition: Shard allocation filtering <ddl_shard_allocation>`


.. _sql-create-table-routing-allocation-exclude:

``routing.allocation.exclude.{attribute}``
------------------------------------------

Assign the table to a node whose ``{attribute}`` has none of the
comma-separated values.

.. SEEALSO::

    :ref:`Data definition: Shard allocation filtering <ddl_shard_allocation>`


.. _sql-create-table-unassigned:

.. _sql-create-table-unassigned.node-left:

.. _sql-create-table-unassigned.node-left-delayed-timeout:

``unassigned.node_left.delayed_timeout``
----------------------------------------

Delay the :ref:`allocation <gloss-shard-allocation>` of replica shards which
have become unassigned because a node has left. It defaults to ``1m`` to give a
node time to restart completely (which can take some time when the node has
lots of shards). Setting the timeout to ``0`` will start allocation
immediately. This setting can be changed on runtime in order to
increase/decrease the delayed allocation if needed.


.. _sql-create-table-column-policy:

``column_policy``
-----------------

Specifies the column policy of the table. The default column policy is
``strict``.

The column policy is defined like this::

    WITH ( column_policy = {'dynamic' | 'strict'} )

:strict:
  Rejecting any column on insert, update or copy from which is not defined in
  the schema

:dynamic:
  New columns can be added using ``INSERT``, ``UPDATE`` or ``COPY FROM``. New
  columns added to ``dynamic`` tables are, once added, usable as usual
  columns. One can retrieve them, sort by them and use them in ``WHERE``
  clauses.

.. SEEALSO::

    :ref:`Data definition: Column policy <column_policy>`


.. _sql-create-table-max-ngram-diff:

``max_ngram_diff``
------------------

Specifies the maximum difference between ``max_ngram`` and ``min_ngram`` when
using the ``NGramTokenizer`` or the ``NGramTokenFilter``. The default is 1.


.. _sql-create-table-max-shingle-diff:

``max_shingle_diff``
--------------------

Specifies the maximum difference between ``min_shingle_size`` and
``max_shingle_size`` when using the ``ShingleTokenFilter``. The default is 3.


.. _sql-create-table-merge:

.. _sql-create-table-merge-scheduler:

.. _sql-create-table-merge-scheduler-max-thread-count:

``merge.scheduler.max_thread_count``
------------------------------------

The maximum number of threads on a single shard that may be merging at once.
Defaults to ``Math.max(1, Math.min(4,
Runtime.getRuntime().availableProcessors() / 2))`` which works well for a good
solid-state-disk (SSD). If your index is on spinning platter drives instead,
decrease this to 1.
