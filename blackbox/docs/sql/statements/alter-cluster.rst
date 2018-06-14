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
      { REROUTE RETRY FAILED }


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
