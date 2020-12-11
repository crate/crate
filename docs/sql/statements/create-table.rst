.. highlight:: psql
.. _ref-create-table:

================
``CREATE TABLE``
================

Define a new table.

.. rubric:: Table of contents

.. contents::
   :local:

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
    }

where ``storage_options`` is::

    STORAGE WITH ( option = value_expression [, ... ] )

and ``table_constraint`` is::

    { PRIMARY KEY ( column_name [, ... ] ) |
      INDEX index_name USING FULLTEXT ( column_name [, ... ] )
           [ WITH ( analyzer = analyzer_name ) ]
    }

Description
===========

CREATE TABLE will create a new, initially empty table.

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

Table elements
--------------

.. _ref-base-columns:

Base Columns
~~~~~~~~~~~~

A base column is a persistent column in the table metadata. In relational terms
it is an attribute of the tuple of the table-relation. It has a name, a type,
an optional default clause and optional constraints.

Base columns are readable and writable (if the table itself is writable).
Values for base columns are given in DML statements explicitly or omitted, in
which case their value is null.

.. _ref-default-clause:

Default clause
^^^^^^^^^^^^^^

The optional default clause defines the default value of the column. The value
is inserted when the column is a target of a ``INSERT`` statement that doesn't
contain an explicit value for it.

The default clause expression is variable-free, it means that subqueries and
cross-references to other columns are not allowed.

.. _ref-generated-columns:

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

   For more information, see :ref:`sql-ddl-generated-columns`.

Table constraints
~~~~~~~~~~~~~~~~~

Table constraints are constraints that are applied to more than one column or
to the table as a whole.

For further details see :ref:`table_constraints`.

Column constraints
~~~~~~~~~~~~~~~~~~

Column constraints are constraints that are applied on each column of the table
separately.

For further details see :ref:`column_constraints`.

Storage options
~~~~~~~~~~~~~~~

Storage options can be applied on each column of the table separately.

For further details and available options see :ref:`ddl-storage`.

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of the table to be created.

:column_name:
  The name of a column to be created in the new table.

:data_type:
  The data type of the column. This can include array and object specifiers. For
  more information on the data types supported by CrateDB see :ref:`data-types`.

:generation_expression:
  An expression (usually a function call) that is applied in the context
  of the current row. As such it can reference other base columns of the
  table. Referencing other generated columns (including itself) is not
  supported. The generation expression is evaluated each time a row is
  inserted or the referenced base columns are updated.

``IF NOT EXISTS``
=================

If the optional IF NOT EXISTS clause is used this statement won't do anything
if the table exists already.

.. _ref_clustered_clause:

``CLUSTERED``
=============

The optional CLUSTERED clause specifies how a table should be distributed
accross a cluster.

:num_shards:
  Specifies the number of shards a table is stored in. Must be greater
  than 0. If not provided the number of shards is calculated based on
  the number of currently active data nodes with the following formula::

      num_shards = max(4, num_data_nodes * 2)

  .. NOTE::

     The minimum value of ``num_shards`` is set to ``4``. This means if the
     calculation of ``num_shards`` does not exceeds its minimum it applies the
     minimum value to each table or partition as default.

:routing_column:
  Allows to explicitly specify a column or field on which basis rows are
  sharded. All rows having the same value in ``routing_column`` are
  stored in the same shard. The default is the primary key if specified,
  otherwise the internal ``_id`` column.

.. _partitioned_by_clause:

``PARTITIONED BY``
==================

The PARTITIONED clause splits the created table into separate partitions for
every distinct combination of values in the listed columns.

::

    [ PARTITIONED BY ( column_name [ , ... ] ) ]

:column_name:
  A column from the table definition this table gets partitioned by.

Several restrictions apply to columns that can be used here:

* columns may not be part of :ref:`ref_clustered_clause`.
* columns must have a :ref:`primitive type <sql_ddl_datatypes_primitives>`.
* columns may not be inside an object array.
* columns may not be indexed with a :ref:`sql_ddl_index_fulltext`.
* if the table has a :ref:`primary_key_constraint` the columns in PARTITIONED
  clause have to be part of it

.. NOTE::

   Columns referenced in the PARTITIONED clause cannot be altered by an
   ``UPDATE`` statement.

.. _with_clause:

``WITH``
========

The optional WITH clause can specify parameters for tables.

::

    [ WITH ( table_parameter [= value] [, ... ] ) ]

:table_parameter:
  Specifies an optional parameter for the table.

.. NOTE::

   Some parameters are nested, and therefore need to be wrapped in double quotes in order to be set:
   ``WITH ("allocation.max_retries" = 5)``.
   Nested parameters are those that contain a ``.`` between parameter names, e.g. ``write.wait_for_active_shards``.

Available parameters are:

.. _number_of_replicas:

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

  The actual maximum number of replicas is max(num_replicas, N-1), where
  N is the number of data nodes in the cluster. If ``max_replicas`` is
  the string ``all`` then it will always be N.

For further details and examples see :ref:`replication`.


``number_of_routing_shards``
----------------------------

This number specifies the hashing space that is used internally to distribute
documents across shards.

This is an optional setting that enables users to later on increase the number
of shards using :ref:`ref-alter-table`.


.. _sql_ref_refresh_interval:

``refresh_interval``
--------------------

Specifies the refresh interval of a shard in milliseconds. The default is set
to 1000 milliseconds.

:value:
  The refresh interval in milliseconds. A value of smaller or equal than
  0 turns off the automatic refresh. A value of greater than 0 schedules
  a periodic refresh of the table.

.. NOTE::

   A ``refresh_interval`` of 0 does not guarantee that new writes are *NOT*
   visible to subsequent reads. Only the periodic refresh is disabled. There
   are other internal factors that might trigger a refresh.

For further details see :ref:`refresh_data` or :ref:`sql_ref_refresh`.

.. _sql_ref_write_wait_for_active_shards:

``write.wait_for_active_shards``
--------------------------------

Specifies the number of shard copies that need to be active for write
operations to proceed. If less shard copies are active the operation must wait
and retry for up to 30s before timing out.

:value:
  ``all`` or a positive integer up to the total number of configured shard copies
  (``number_of_replicas + 1``).
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
<alter-table-reroute-promote-replica>` can be used to force the allocation of a
stale replica copy to at least recover the data that is available in the stale
replica copy.

Say you've a 3 node cluster and a table with 1 configured replica. With
``write.wait_for_active_shards=1`` and ``number_of_replicas=1`` a node in the
cluster can be restarted without affecting write operations because the primary
copies are either active or the replicas can be quickly promoted.

If ``write.wait_for_active_shards`` would be set to ``2`` instead and a node is
stopped, the write operations would block until the replica is fully replicated
again or the write operations would timeout in case the replication is not fast
enough.


.. _table-settings-blocks.read_only:

``blocks.read_only``
--------------------

Allows to have a read only table.

:value:
  Table is read only if value set to ``true``. Allows writes and table
  settings changes if set to ``false``.

.. _table-settings-blocks.read_only_allow_delete:

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

    :ref:`Disk-based shard allocation <cluster.routing.allocation.disk>`

``blocks.read``
---------------

``disable``/``enable`` all the read operations

:value:
  Set to ``true`` to disable all read operations for a table, otherwise
  set ``false``.

.. _table-settings-blocks.write:

``blocks.write``
----------------

``disable``/``enable`` all the write operations

:value:
  Set to ``true`` to disable all write operations and table settings
  modifications, otherwise set ``false``.

``blocks.metadata``
-------------------

``disable``/``enable`` the table settings modifications.

:values:
  Disables the table settings modifications if set to ``true``, if set
  to ``false`` — table settings modifications are enabled.

.. _table_parameter.soft_deletes.enabled:

``soft_deletes.enabled``
------------------------

Indicates whether soft deletes are enabled or disabled.

Soft deletes allow CrateDB to preserve recent deletions within the Lucene
index. This information is used for shard recovery.

Before the introduction of soft deletes, CrateDB had to retain the information
in the :ref:`Translog <durability>`. Using soft deletes uses less storage than
the Translog equivalent and is faster.

Soft deletes can only be configured when a table is created. This setting
cannot be changed using ``ALTER TABLE``.

Soft deletes will become mandatory in CrateDB 5.0.


:value:
  Defaults to ``true``. Set to ``false`` to disable soft deletes.


.. _table_parameter.soft_deletes.retention_lease.period:

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


.. _table_parameter.codec:

``codec``
---------

By default data is stored using ``LZ4`` compression. This can be changed to
``best_compression`` which uses ``DEFLATE`` for a higher compression ratio, at
the expense of slower column value lookups.

:values:
  ``default`` or ``best_compression``

.. _table_parameter.store_type:

``store.type``
--------------

The store type setting allows you to control how data is stored
and accessed on disk. The following storage types are supported:

:fs:
  Default file system implementation. It will pick the best implementation
  depending on the operating environment, which is currently ``hybridfs``
  on all supported systems but is subject to change.

:simplefs:
  The ``Simple FS`` type is an implementation of file system storage
  (Lucene ``SimpleFsDirectory``) using a random access file.
  This implementation has poor concurrent performance. It is usually
  better to use the ``niofs`` when you need index persistence.

:niofs:
  The ``NIO FS`` type stores the shard index on the file system
  (Lucene ``NIOFSDirectory``) using NIO. It allows multiple threads
  to read from the same file concurrently.

:mmapfs:
  The ``MMap FS`` type stores the shard index on the file system
  (Lucene ``MMapDirectory``) by mapping a file into memory (mmap).
  Memory mapping uses up a portion of the virtual memory address space
  in your process equal to the size of the file being mapped. Before
  using this type, be sure you have allowed plenty of virtual address space.

:hybridfs:
  The ``hybridfs`` type is a hybrid of ``niofs`` and ``mmapfs``, which
  chooses the best file system type for each type of file based on the
  read access pattern. Similarly to ``mmapfs`` be sure you have allowed
  plenty of virtual address space.

It is possible to restrict the use of the ``mmapfs`` and ``hybridfs`` store
type via the :ref:`node.store.allow_mmap <node.store_allow_mmap>` node setting.

``mapping.total_fields.limit``
------------------------------

Sets the maximum number of columns that is allowed for a table. Default is ``1000``.

:value:
  Maximum amount of fields in the Lucene index mapping. This includes
  both the user facing mapping (columns) and internal fields.

``translog.flush_threshold_size``
---------------------------------

Sets size of transaction log prior to flushing.

:value:
  Size (bytes) of translog.

``translog.disable_flush``
--------------------------

``enable``/``disable`` flushing.

:value:
  Set ``true`` to disable flushing, otherwise set to ``false``.

.. CAUTION::

   It is recommended to use ``disable_flush`` only for short periods of time.

``translog.interval``
---------------------

Sets frequency of flush necessity check.

:value:
  Frequency in milliseconds.

.. _translog_sync_interval:

``translog.sync_interval``
--------------------------

How often the translog is fsynced to disk. Defaults to 5s.
When setting this interval, please keep in mind that changes logged
during this interval and not synced to disk may get lost in case of a
failure. This setting only takes effect if :ref:`translog.durability
<translog_durability>` is set to ``ASYNC``.

:value:
  Interval in milliseconds.

.. _translog_durability:

``translog.durability``
-----------------------

If set to ``ASYNC`` the translog gets flushed to disk in the background
every :ref:`translog.sync_interval <translog_sync_interval>`. If set to
``REQUEST`` the flush happens after every operation.

:value:
  ``REQUEST`` (default), ``ASYNC``

``routing.allocation.total_shards_per_node``
--------------------------------------------

Controls the total number of shards (replicas and primaries) allowed to be
allocated on a single node. Defaults to unbounded (-1).

:value:
  Number of shards per node.

``routing.allocation.enable``
-----------------------------

Controls shard allocation for a specific table. Can be set to:

:all:
  Allows shard allocation for all shards. (Default)

:primaries:
  Allows shard allocation only for primary shards.

:new_primaries:
  Allows shard allocation only for primary shards for new tables.

:none:
  No shard allocation allowed.

.. _allocation_max_retries:

``routing.allocation.max_retries``
----------------------------------

Defines the number of attempts to allocate a shard before giving up and leaving
the shard unallocated.

:value:
  Number of retries to allocate a shard. Defaults to 5.

``routing.allocation.include.{attribute}``
------------------------------------------

Assign the table to a node whose ``{attribute}`` has at least one of the
comma-separated values.
See :ref:`ddl_shard_allocation` for details.

``routing.allocation.require.{attribute}``
------------------------------------------

Assign the table to a node whose ``{attribute}`` has all of the comma-separated
values.
See :ref:`ddl_shard_allocation` for details.

``routing.allocation.exclude.{attribute}``
------------------------------------------

Assign the table to a node whose ``{attribute}`` has none of the comma-separated
values.
See :ref:`ddl_shard_allocation` for details.

``warming.enable``
------------------

``disable``/``enable`` table warming.

Table warming allows to run registered queries to warm up the table before it
is available.

Enabled by default.

:value:
  `true`` to enable warming up, otherwise ``false``

``unassigned.node_left.delayed_timeout``
----------------------------------------

Delay the allocation of replica shards which have become unassigned because a
node has left. It defaults to ``1m`` to give a node time to restart
completely (which can take some time when the node has lots of shards).
Setting the timeout to ``0`` will start allocation immediately. This setting
can be changed on runtime in order to increase/decrease the delayed
allocation if needed.

.. _sql_ref_column_policy:

``column_policy``
-----------------

Specifies the column policy of the table. The default column policy is
``strict``.

The column policy is defined like this::

    WITH ( column_policy = {'dynamic' | 'strict'} )

:strict:
  Rejecting any column on insert, update or copy from which is not
  defined in the schema

:dynamic:
  New columns can be added using insert, update or copy from. New
  columns added to ``dynamic`` tables are, once added, usable as usual
  columns. One can retrieve them, sort by them and use them in where
  clauses.

For further details and examples see :ref:`column_policy` or :ref:`config`.

``max_ngram_diff``
------------------

Specifies the maximum difference between max_ngram and min_ngram when using
the NGramTokenizer or the NGramTokenFilter. The default is 1.

``max_shingle_diff``
--------------------

Specifies the maximum difference between min_shingle_size and max_shingle_size
when using the ShingleTokenFilter. The default is 3.


.. _merge.scheduler.max_thread_count:

``merge.scheduler.max_thread_count``
------------------------------------

The maximum number of threads on a single shard that may be merging at once.
Defaults to ``Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors() / 2))``
which works well for a good solid-state-disk (SSD). If your index is on
spinning platter drives instead, decrease this to 1.
