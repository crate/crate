.. highlight:: psql

.. _sql-alter-table:

===============
``ALTER TABLE``
===============

Alter an existing table.

.. rubric:: Table of contents

.. contents::
   :local:


.. _sql-alter-table-synopsis:

Synopsis
========

::

    ALTER [ BLOB ] TABLE { ONLY table_ident
                           | table_ident [ PARTITION (partition_column = value [ , ... ]) ] }
      { SET ( parameter = value [ , ... ] )
        | RESET ( parameter [ , ... ] )
        | { ADD [ COLUMN ] column_name data_type [ column_constraint [ ... ] ] } [, ... ]
        | { DROP [ COLUMN ] column_name } [, ... ]
        | OPEN
        | CLOSE
        | RENAME TO table_ident
        | REROUTE reroute_option
        | DROP CONSTRAINT constraint_name
      }

where ``column_constraint`` is::

    { PRIMARY KEY |
      NOT NULL |
      INDEX { OFF | USING { PLAIN |
                            FULLTEXT [ WITH ( analyzer = analyzer_name ) ]  } |
      [ CONSTRAINT constraint_name ] CHECK (boolean_expression)
    }


.. _sql-alter-table-description:

Description
===========

``ALTER TABLE`` can be used to modify an existing table definition. It provides
options to add columns, modify constraints, enabling or disabling table
parameters and allows to execute a shard  :ref:`reroute allocation
<sql-alter-table-reroute>`.

Use the ``BLOB`` keyword in order to alter a blob table (see
:ref:`blob_support`). Blob tables cannot have custom columns which means that
the ``ADD COLUMN`` keyword won't work.

While altering a partitioned table, using ``ONLY`` will apply changes for the
table *only* and not for any possible existing partitions. So these changes
will only be applied to new partitions. The ``ONLY`` keyword cannot be used
together with a `PARTITION`_ clause.

See ``CREATE TABLE`` :ref:`sql-create-table-with` for a list of available
parameters.

:table_ident:
  The name (optionally schema-qualified) of the table to alter.


.. _sql-alter-table-clauses:

Clauses
=======


.. _sql-alter-table-partition:

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
``PARTITION`` clause can be used to alter one partition exclusively.

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
partition to alter.

.. TIP::

    The :ref:`ref-show-create-table` statement will show you the complete list
    of partition columns specified by the
    :ref:`sql-create-table-partitioned-by` clause.

.. NOTE::

   BLOB tables cannot be partitioned and hence this clause cannot be used.

.. SEEALSO::

    :ref:`Partitioned tables: Alter <partitioned-alter>`


.. _sql-alter-table-arguments:

Arguments
=========


.. _sql-alter-table-set-reset:

``SET/RESET``
-------------

Can be used to change a table parameter to a different value.  Using ``RESET``
will reset the parameter to its default value.

:parameter:
  The name of the parameter that is set to a new value or its default.

The supported parameters are listed in the :ref:`CREATE TABLE WITH CLAUSE
<sql-create-table-with>` documentation. In addition to those, for dynamically
changing the number of :ref:`allocated shards <gloss-shard-allocation>`, the
parameter ``number_of_shards`` can be used. For more info on that, see
:ref:`alter-shard-number`.


.. _sql-alter-table-add-column:

``ADD COLUMN``
--------------

Can be used to add an additional column to a table. While columns can be added
at any time, adding a new :ref:`generated column
<sql-create-table-generated-columns>` is only possible if the table is empty.
In addition, adding a base column with :ref:`sql-create-table-default-clause`
is not supported. It is possible to define a ``CHECK`` constraint with the
restriction that only the column being added may be used in the :ref:`boolean
expression <sql-literal-value>`.

:data_type:
  Data type of the column which should be added.

:column_name:
  Name of the column which should be added. 
  This can be a sub-column on an existing `OBJECT`.

It's possible to add multiple columns at once.

.. _sql-alter-table-drop-column:

``DROP COLUMN``
---------------

Can be used to drop a column from a table.

:column_name:
  Name of the column which should be added.
  This can be a sub-column on an existing `OBJECT`.

It's possible to drop multiple columns at once.

.. NOTE::

    It's not allowed to drop a column which is part of a
    :ref:`PRIMARY KEY <primary_key_constraint>`, used in
    :ref:`CLUSTERED BY column <gloss-clustered-by-column>`,
    used in :ref:`PARTITIONED BY <gloss-partitioned-by-column>`,
    used in an :ref:`named index<named-index-column>` or is
    referenced in a :ref:`generated column <ddl-generated-columns-expressions>`.

.. NOTE::

   It's not allowed to drop all columns of a table.

.. _sql-alter-table-open-close:

``OPEN/CLOSE``
--------------

Can be used to open or close the table.

Closing a table means that all operations, except ``ALTER TABLE ...``, will
fail. Operations that fail will not return an error, but they will have no
effect. Operations on tables containing closed partitions won't fail, but those
operations will exclude all closed partitions.


.. _sql-alter-table-rename-to:

``RENAME TO``
-------------

Can be used to rename a table or view, while maintaining its schema and data.
If renaming a table, the shards of it become temporarily unavailable.


.. _sql-alter-table-reroute:

``REROUTE``
-----------

The ``REROUTE`` command provides various options to manually control the
:ref:`allocation of shards <gloss-shard-allocation>`. It allows the enforcement
of explicit allocations, cancellations and the moving of shards between nodes
in a cluster. See :ref:`ddl_reroute_shards` to get the convenient use-cases.

The row count defines if the reroute or allocation process of a shard was
acknowledged or rejected.

.. NOTE::

    Partitioned tables require a :ref:`sql-alter-table-partition` clause in
    order to specify a unique ``shard_id``.

::

    [ REROUTE reroute_option]


where ``reroute_option`` is::

    { MOVE SHARD shard_id FROM node TO node
      | ALLOCATE REPLICA SHARD shard_id ON node
      | PROMOTE REPLICA SHARD shard_id ON node [ WITH (accept_data_loss = { TRUE | FALSE }) ]
      | CANCEL SHARD shard_id ON node [ WITH (allow_primary = {TRUE|FALSE}) ]
    }

:shard_id:
  The shard ID. Ranges from 0 up to the specified number of :ref:`sys-shards`
  shards of a table.

:node:
  The ID or name of a node within the cluster.

  See :ref:`sys-nodes` how to gain the unique ID.


``REROUTE`` supports the following options to start/stop shard allocation:

**MOVE**
  A started shard gets moved from one node to another. It requests a
  ``table_ident`` and a ``shard_id`` to identify the shard that receives the
  new allocation. Specify ``FROM node`` for the node to move the shard from and
  ``TO node`` to move the shard to.

**ALLOCATE REPLICA**
  Allows to force allocation of an unassigned replica shard on a specific node.

.. _alter-table-reroute-promote-replica:

**PROMOTE REPLICA** Force promote a stale replica shard to a primary.  In case
  a node holding a primary copy of a shard had a failure and the replica shards
  are out of sync, the system won't promote the replica to primary
  automatically, as it would result in a silent data loss.

  Ideally the node holding the primary copy of the shard would be brought back
  into the cluster, but if that is not possible due to a permanent system
  failure, it is possible to accept the potential data loss and force promote a
  stale replica using this command.

  The parameter ``accept_data_loss`` needs to be set to ``true`` in order for
  this command to work. If it is not provided or set to false, the command will
  error out.

**CANCEL**
  This cancels the allocation or :ref:`recovery <gloss-shard-recovery>` of a
  ``shard_id`` of a ``table_ident`` on a given ``node``. The ``allow_primary``
  flag indicates if it is allowed to cancel the allocation of a primary shard.


.. _sql-alter-drop-constraint:

``DROP CONSTRAINT``
-------------------

Removes a :ref:`check_constraint` constraint from a table.

.. code-block:: sql

    ALTER TABLE table_ident DROP CONSTRAINT check_name

:table_ident:
  The name (optionally schema-qualified) of the table.

:check_name:
  The name of the check constraint to be removed.


.. WARNING::

    A removed CHECK constraints cannot be re-added to a table once dropped.
