.. _concept-storage-consistency:

=======================
Storage and consistency
=======================

This document provides an overview on how CrateDB stores and distributes state
across the cluster and what consistency and durability guarantees are provided.

.. NOTE::

  Since CrateDB heavily relies on Elasticsearch_ and Lucene_ for storage and
  cluster consensus, concepts shown here might look familiar to Elasticsearch_
  users, since the implementation is actually reused from the Elasticsearch_
  code.

.. rubric:: Table of contents

.. contents::
   :local:


.. _concept-data-storage:

Data storage
============

Every table in CrateDB is sharded, which means that tables are divided and
distributed across the nodes of a cluster. Each shard in CrateDB is a Lucene_
index broken down into segments getting stored on the filesystem. Physically
the files reside under one of the configured data directories of a node.

Lucene only appends data to segment files, which means that data written to the
disc will never be mutated. This makes it easy for replication and
:ref:`recovery <gloss-shard-recovery>`, since syncing a shard is simply a
matter of fetching data from a specific marker.

An arbitrary number of replica shards can be configured per table. Every
operational replica holds a full synchronized copy of the primary shard.

With read operations, there is no difference between executing the
operation on the primary shard or on any of the replicas. CrateDB
randomly assigns a shard when routing an operation. It is possible to
configure this behavior if required, see our best practice guide on
`multi zone setups <https://crate.io/docs/crate/howtos/en/latest/scaling/multi-zone-setup.html>`_
for more details.

Write operations are handled differently than reads. Such operations are
synchronous over all active replicas with the following flow:

1. The primary shard and the active replicas are looked up in the cluster state
   for the given operation. The primary shard and a quorum of the configured
   replicas need to be available for this step to succeed.

2. The operation is routed to the according primary shard for execution.

3. The operation gets executed on the primary shard

4. If the operation succeeds on the primary, the operation gets executed on all
   replicas in parallel.

5. After all replica operations finish the operation result gets returned to
   the caller.

Should any replica shard fail to write the data or times out in step 5, it's
immediately considered as unavailable.


.. _concept-atomicity:

Atomicity at document level
===========================

Each row of a table in CrateDB is a semi structured document which can be
nested arbitrarily deep through the use of object and array types.

Operations on documents are atomic. Meaning that a write operation on a
document either succeeds as a whole or has no effect at all. This is always the
case, regardless of the nesting depth or size of the document.

CrateDB does not provide transactions. Since every document in CrateDB has a
version number assigned, which gets increased every time a change occurs,
patterns like `Optimistic Concurrency Control`_ can help to work around that
limitation.


.. _concept-durability:

Durability
==========

Each shard has a WAL_ also known as translog. It guarantees that operations on
documents are persisted to disk without having to issue a Lucene-Commit for
every write operation. When the translog gets flushed all data is written to
the persistent index storage of Lucene and the translog gets cleared.

In case of an unclean shutdown of a shard, the transactions in the translog are
getting replayed upon startup to ensure that all executed operations are
permanent.

The translog is also directly transferred when a newly allocated replica
initializes itself from the primary shard. There is no need to flush segments
to disc just for replica :ref:`recovery <gloss-shard-recovery>` purposes.


.. _concept-addressing-documents:

Addressing documents
====================

Every document has an :ref:`internal identifier
<sql_administration_system_column_id>`. By default this identifier is derived
from the primary key. Documents living in tables without a primary key are
assigned a unique auto-generated ID automatically when created.

Each document is :ref:`routed <sharding-routing>` to one specific shard
according to the :ref:`routing column <gloss-routing-column>`. All rows that
have the same routing column row value are stored in the same shard. The
routing column can be specified with the :ref:`CLUSTERED
<sql-create-table-clustered>` clause when creating the table. If a
:ref:`primary key <primary_key_constraint>` has been defined, it will be used
as the default routing column, otherwise the :ref:`internal document ID
<sql_administration_system_column_id>` is used.

While transparent to the user, internally there are two ways how CrateDB
accesses documents:

:get:
  Direct access by identifier. Only applicable if the routing key and the
  identifier can be computed from the given query specification. (e.g: the full
  primary key is defined in the where clause).

  This is the most efficient way to access a document, since only a single shard
  gets accessed and only a simple index lookup on the ``_id`` field has to be
  done.

:search:
  Query by matching against fields of documents across all candidate shards of
  the table.


.. _concept-consistency:

Consistency
===========

CrateDB is eventual consistent for search operations. Search operations are
performed on shared ``IndexReaders`` which besides other functionality, provide
caching and reverse lookup capabilities for shards. An ``IndexReader`` is
always bound to the Lucene_ segment it was started from, which means it has to
be refreshed in order to see new changes, this is done on a time based manner,
but can also be done manually (see `refresh`_). Therefore a search only sees a
change if the according ``IndexReader`` was refreshed after that change
occurred.

If a query specification results in a ``get`` operation, changes are visible
immediately. This is achieved by looking up the document in the translog first,
which will always have the most recent version of the document. The common
update and fetch use-case is therefore possible. If a client updates a row and
that row is looked up by its primary key after that update the changes will
always be visible, since the information will be retrieved directly from the
translog. There is an exception to that, when the ``WHERE`` clause contains
complex filtering and/or lots of Primary Key values. You can find more details
:ref:`here <sql-refresh-description_collect_exception>`.

.. NOTE::

  ``Dirty reads`` can occur if the primary shard becomes isolated. The primary
  will only realize it is isolated once it tries to communicate with its
  replicas or the master. At that point, a write operation is already committed
  into the primary and can be read by a concurrent read operation. In order to
  minimise the window of opportunity for this phenomena, the CrateDB nodes
  communicate with the master every second (by default) and once they realise
  no master is known, they will start rejecting write operations.

  Every replica shard is updated synchronously with its primary and always
  carries the same information. Therefore it does not matter if the primary or
  a replica shard is accessed in terms of consistency. Only the refresh of the
  ``IndexReader`` affects consistency.

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

.. CAUTION::

   Some outage conditions can affect these consistency claims. See the
   :ref:`resiliency documentation <concept-resiliency>` for details.


.. _concept-cluster-metadata:

Cluster meta data
=================

Cluster meta data is held in the so called "Cluster State", which contains the
following information:

- Tables schemas.

- Primary and replica shard locations. Basically just a mapping from shard
  number to the storage node.

- Status of each shard, which tells if a shard is currently ready for use or
  has any other state like "initializing", "recovering" or cannot be assigned
  at all.

- Information about discovered nodes and their status.

- Configuration information.

Every node has its own copy of the cluster state. However there is only one
node allowed to change the cluster state at runtime. This node is called the
"master" node and gets auto-elected. The "master" node has no special
configuration at all, all nodes are master-eligible by default, and any 
master-eligible node can be elected as the master. There
is also an automatic re-election if the current master node goes down for some
reason.

.. NOTE::

  To avoid a scenario where two masters could be elected due to network 
  partitioning, CrateDB automatically defines a quorum of nodes with 
  which it is possible to elect a master. For details on how this works
  and further information see :ref:`concept-master-election`.

To explain the flow of events for any cluster state change, here is an example
flow for an ``ALTER TABLE`` statement which changes the schema of a table:

#. A node in the cluster receives the ``ALTER TABLE`` request.

#. The node sends out a request to the current master node to change the table
   definition.

#. The master node applies the changes locally to the cluster state and sends
   out a notification to all affected nodes about the change.

#. The nodes apply the change, so that they are now in sync with the master.

#. Every node might take some local action depending on the type of cluster
   state change.

.. _Elasticsearch: https://www.elasticsearch.org/
.. _Lucene: https://lucene.apache.org/core/
.. _WAL: https://en.wikipedia.org/wiki/Write-ahead_logging
.. _Optimistic Concurrency Control: https://crate.io/docs/crate/reference/sql/occ.html
.. _refresh: https://crate.io/docs/crate/reference/sql/refresh.html
