.. highlight:: psql
.. _ref-alter-cluster:

=================
``ALTER CLUSTER``
=================

Alter the state of an existing cluster.

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========

::

    ALTER CLUSTER 
      { REROUTE RETRY FAILED
      | DECOMMISSION <nodeId | nodeName>
      | SWAP TABLE source TO target [ WITH ( expr = expr [ , ... ] ) ]
      | GC DANGLING ARTIFACTS
      }


Description
===========

``ALTER CLUSTER`` applies a change to the cluster state.

Arguments
=========

``REROUTE RETRY FAILED``
------------------------

The index setting :ref:`allocation.max_retries <allocation_max_retries>`
indicates the maximum of attempts to allocate a shard on a node. If this limit
is reached it leaves the shard unallocated.
This command allows the enforcement to retry the allocation of shards which
failed to allocate. See :ref:`ddl_reroute_shards` to get convenient use-cases.

The rowcount defines the number of shards that will be allocated.
A rowcount of ``-1`` reflects an error or indicates that the statement did not
get acknowledged.

.. NOTE::

    This statement can only be invoked by superusers that already exist in the
    cluster.

    Additionally, keep in mind that this statement only triggers the shard
    re-allocation and is therefore asynchronous. Unassigned shards with large
    size will take some time to allocate.

``DECOMMISSION <nodeId | nodeName>``
------------------------------------

This command triggers a graceful cluster node decommission. The node can be
specified by either its Id or name. See `Graceful stop`_ for more information
on decommissioning nodes gracefully.



``SWAP TABLE``
--------------

::

      SWAP TABLE source TO target [ WITH ( expr = expr [ , ... ] ) ]

This command swaps two tables. ``source`` will be renamed to ``target`` and
``target`` will be renamed to ``source``.

An example use case of this feature is some sort of schema migration using
``INSERT INTO ... query``. You'd create a new table with an updated schema,
copy over the data from the old table and then replace the old table with the
new table.

.. NOTE::

    Swapping two tables causes the shards to be unavailable for a short period.


Options
.......


**drop_source**
   | *Default*: ``false``

   A boolean option that if set to ``true`` causes the command to remove the
   ``source`` table after the rename. This causes the command to *replace*
   ``source`` with ``target``, instead of swapping the names.


``GC DANGLING ARTIFACTS``
-------------------------

::

   GC DANGLING ARTIFACTS


Some operations in CrateDB might temporarily create data to complete the
operation. If during such an operation the cluster starts failing these
temporary artifacts might not be cleaned up correctly.

The ``ALTER CLUSTER GC DANGLING ARTIFACTS`` command can be used to remove all
artifacts created by such operations.


.. _Graceful stop: http://crate.io/docs/crate/guide/best_practices/rolling_upgrade.html#step-2-graceful-stop