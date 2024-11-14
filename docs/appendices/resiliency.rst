.. _appendix-resiliency:

=================
Resiliency Issues
=================

CrateDB uses Elasticsearch for data distribution and replication. Most of the
resiliency issues `exist in the Elasticsearch layer
<https://www.elastic.co/guide/en/elasticsearch/resiliency/current/>`_ and can
be tested by `Jepsen <https://github.com/jepsen-io/jepsen/tree/master/crate>`_.


.. rubric:: Table of contents

.. contents::
   :local:


Known issues
============


Retry of updates causes double execution
----------------------------------------

.. raw:: html

   <table class="summary">
   <tr>
    <td>Status</td>
    <td>Work ongoing (<a
      href="https://www.elastic.co/guide/en/elasticsearch/resiliency/current/index.html#_better_request_retry_mechanism_when_nodes_are_disconnected_status_ongoing"
      >More info</a>)
    </td>
   </tr>
   <tr>
    <td>Severity</td>
    <td>Moderate</td>
   </tr>
   <tr>
    <td>Likelihood</td>
    <td>Very rare</td>
   </tr>
   <tr>
    <td>Cause</td>
    <td>Network issues, unresponsive nodes</td>
   </tr>
   <tr>
    <td>Workloads</td>
    <td>Non-Idempotent writes</td>
   </tr>
   </table>

.. rubric:: Scenario

A node with a primary shard receives an update, writes it to disk, but goes
offline before having sent a confirmation back to the executing node. When the
node comes back online, it receives an update retry and executes the update
again.

.. rubric:: Consequence

Incorrect data for non-idempotent writes.

For example:

-  An double insert on a table without an explicit primary key would be
   executed twice and would result in duplicate data.

-  A double update would incorrectly increment the row version number twice.


Fixed issues
============

.. _resiliency_cluster_partitions_cause_lost_cluster_updates:

Repeated cluster partitions can cause lost cluster updates
----------------------------------------------------------

.. raw:: html

   <style>
   table.summary td {
     border: 1px solid;
     padding: 5px;
   }
   </style>

   <table class="summary">
   <tr>
    <td>Status</td>
    <td>Fixed in CrateDB v4.0 (<a
    href="https://github.com/elastic/elasticsearch/issues/32006">#32006</a>, <a
    href="https://github.com/elastic/elasticsearch/issues/32171">#32171</a>)
    </td>
   </tr>
   <tr>
    <td>Severity</td>
    <td>Moderate</td>
   </tr>
   <tr>
    <td>Likelihood</td>
    <td>Very rare</td>
   </tr>
   <tr>
    <td>Cause</td>
    <td>Network issues, unresponsive nodes</td>
   </tr>
   <tr>
    <td>Workloads</td>
    <td>All</td>
   </tr>
   </table>

.. rubric:: Scenario

A cluster is partitioned and a new master is elected on the side that has
quorum. The cluster is repaired and simultaneously a change is made to the
cluster state. The cluster is partitioned again before the new master node has
a chance to publish the new cluster state and the partition the master lands on
does not have quorum.

.. rubric:: Consequence

The node steps down as master and the uncommunicated state changes are lost.

Cluster state is very important and contains information like shard location,
schemas, and so on. Lost cluster state updates can cause data loss, reset
settings, and problems with table structures.

.. rubric:: Partially fixed


This problem is mostly fixed by `#20384
<https://github.com/elastic/elasticsearch/pull/20384>`_ (CrateDB v2.0.x),
which uses committed cluster state updates during master election process.
This does not fully solve this rare problem but considerably reduces the chance
of occurrence. The reason is that if the second partition happens concurrently
with a cluster state update and blocks the cluster state commit message from
reaching a majority of nodes, it may be that the in flight update is lost. If
the now-isolated master can still acknowledge the cluster state update to the
client this will result to the loss of an acknowledged change.

.. _resiliency_ambiguous_row_versions:

Version number representing ambiguous row versions
--------------------------------------------------

.. raw:: html

   <table class="summary">
   <tr>
    <td>Status</td>
    <td>Fixed in CrateDB v4.0 (<a
    href="https://github.com/elastic/elasticsearch/issues/19269">#19269</a>, <a
    href="https://github.com/elastic/elasticsearch/issues/10708">#10708</a>)
    </td>
   </tr>
   <tr>
    <td>Severity</td>
    <td>Significant</td>
   </tr>
   <tr>
    <td>Likelihood</td>
    <td>Very rare</td>
   </tr>
   <tr>
    <td>Cause</td>
    <td>Network issues, unresponsive nodes</td>
   </tr>
   <tr>
    <td>Workloads</td>
    <td>Versioned reads with replicated tables while writing.</td>
   </tr>
   </table>

.. rubric:: Scenario

A client is writing to a primary shard. The node holding the primary shard is
partitioned from the cluster. It usually takes between 30 and 60 seconds
(depending on `ping configuration
<https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-discovery-zen.html>`_)
before the master node notices the partition. During this time, the same row is
updated on both the primary shard (partitioned) and a replica shard (not
partitioned).

.. rubric:: Consequence

There are two different versions of the same row using the same version number.
When the primary shard rejoins the cluster and its data is replicated, the
update that was made on the replicated shard is lost but the new version number
matches the lost update. This will break `Optimistic Concurrency Control
<https://crate.io/docs/reference/sql/occ.html>`_.

.. _resiliency_replicas_fall_out_of_sync:

Replicas can fall out of sync when a primary shard fails
--------------------------------------------------------

.. raw:: html

   <table class="summary">
   <tr>
    <td>Status</td>
    <td>Fixed in CrateDB v4.0 (<a
    href="https://github.com/elastic/elasticsearch/issues/10708">#10708</a>)
    </td>
   </tr>
   <tr>
    <td>Severity</td>
    <td>Modest</td>
   </tr>
   <tr>
    <td>Likelihood</td>
    <td>Rare</td>
   </tr>
   <tr>
    <td>Cause</td>
    <td>Primary fails and in-flight writes are only written to a subset of its replicas</td>
   </tr>
   <tr>
    <td>Workloads</td>
    <td>Writes on replicated table</td>
   </tr>
   </table>

.. rubric:: Scenario

When a primary shard fails, a replica shard will be promoted to be the primary
shard. If there is more than one replica shard, it is possible for the remaining
replicas to be out of sync with the new primary shard. This is caused by
operations that were in-flight when the primary shard failed and may not have
been processed on all replica shards. Currently, the discrepancies are not
repaired on primary promotion but instead would be repaired if replica shards
are relocated (e.g., from hot to cold nodes); this does mean that the length of
time which replicas can be out of sync with the primary shard is
unbounded.

.. rubric:: Consequence

Stale data may be read from replicas.


Loss of rows due to network partition
-------------------------------------

.. raw:: html

   <table class="summary">
   <tr>
    <td>Status</td>
    <td>Fixed in Crate v2.0.x (<a
      href="https://github.com/elastic/elasticsearch/issues/7572">#7572</a>, <a
      href="https://github.com/elastic/elasticsearch/issues/14252">#14252</a>)
    </td>
   </tr>
   <tr>
    <td>Severity</td>
    <td>Significant</td>
   </tr>
   <tr>
    <td>Likelihood</td>
    <td>Very rare</td>
   </tr>
   <tr>
    <td>Cause</td>
    <td>Single node isolation</td>
   </tr>
   <tr>
    <td>Workloads</td>
    <td>Writes on replicated table</td>
   </tr>
   </table>

.. rubric:: Scenario

A node with a primary shard is partitioned from the cluster. The node continues
to accept writes until it notices the network partition. In the meantime,
another shard has been elected as the primary. Eventually, the partitioned node
rejoins the cluster.

.. rubric:: Consequence

Data that was written to the original primary shard on the partitioned node is
lost as data from the newly elected primary shard replaces it when it rejoins
the cluster.

The risk window depends on your `ping configuration
<https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-discovery-zen.html>`_.
The default configuration of a 30 second ping timeout with three retries
corresponds to a 90 second risk window. However, it is very rare for a node to
lose connectivity within the cluster but maintain connectivity with clients.


Dirty reads caused by bad primary handover
------------------------------------------

.. raw:: html

   <table class="summary">
   <tr>
    <td>Status</td>
    <td>Fixed in CrateDB v2.0.x (<a
      href="https://github.com/elastic/elasticsearch/pull/15900">#15900</a>, <a
      href="https://github.com/elastic/elasticsearch/issues/12573">#12573</a>)
    </td>
   </tr>
   <tr>
    <td>Severity</td>
    <td>Moderate</td>
   </tr>
   <tr>
    <td>Likelihood</td>
    <td>Rare</td>
   </tr>
   <tr>
    <td>Cause</td>
    <td>Race Condition</td>
   </tr>
   <tr>
    <td>Workloads</td>
    <td>Reads</td>
   </tr>
   </table>

.. rubric:: Scenario

During a primary handover, there is a small risk window when a shard can find
out it has been elected as the new primary before the old primary shard notices
that it is no longer the primary.

A primary handover can happen in the following scenarios:

- A shard is relocated and then elected as the new primary, as two separate but
  sequential actions. Relocating a shard means creating a new shard and then
  deleting the old shard.

- An existing replica shard gets promoted to primary because the primary shard
  was partitioned from the cluster.

.. rubric:: Consequence

Writes that occur on the new primary during the risk window will not be
replicated to the old shard (which still believes it is the primary) so any
subsequent reads on the old shard may return incorrect data.


Changes are overwritten by old data in danger of lost data
----------------------------------------------------------

.. raw:: html

   <table class="summary">
   <tr>
    <td>Status</td>
    <td>Fixed in CrateDB v2.0.x (<a
      href="https://github.com/elastic/elasticsearch/issues/14671">#14671</a>)
    </td>
   </tr>
   <tr>
    <td>Severity</td>
    <td>Significant</td>
   </tr>
   <tr>
    <td>Likelihood</td>
    <td>Very rare</td>
   </tr>
   <tr>
    <td>Cause</td>
    <td>Network problems</td>
   </tr>
   <tr>
    <td>Workloads</td>
    <td>Writes</td>
   </tr>
   </table>

.. rubric:: Scenario

A node with a primary that contains new data is partitioned from the cluster.

.. rubric:: Consequence

CrateDB prefers old data over no data, and so promotes an a shard with stale
data as a new primary. The data on the original primary shard is lost. Even if
the node with the original primary shard rejoins the cluster, CrateDB has no
way of distinguishing correct and incorrect data, so that data replaced with
data from the new primary shard.


Make table creation resilient to closing and full cluster crashes
-----------------------------------------------------------------

.. raw:: html

   <table class="summary">
   <tr>
    <td>Status</td>
    <td>
        The issue has been fixed with the following issues.
        Table recovery: <a href="https://github.com/elastic/elasticsearch/issues/9126">#9126</a>
        Reopening tables: <a href="https://github.com/elastic/elasticsearch/issues/14739">#14739</a>
        Allocation IDs: <a href="https://github.com/elastic/elasticsearch/issues/15281">#15281</a>
    </td>
   </tr>
   <tr>
    <td>Severity</td>
    <td>Modest</td>
   </tr>
   <tr>
    <td>Likelihood</td>
    <td>Very Rare</td>
   </tr>
   <tr>
    <td>Cause</td>
    <td>Either the cluster fails while recovering a table or
        the table is closed during shard creation.
    </td>
   </tr>
   <tr>
    <td>Workloads</td>
    <td>Table creation</td>
   </tr>
   </table>

.. rubric:: Scenario

Recovering a table requires a quorum of shard copies to be available to
:ref:`allocate <gloss-shard-allocation>` a primary. This means that a
primary cannot be assigned if the cluster dies before enough shards have been
allocated. The same happens if a table is closed before enough shard copies
were started, making it impossible to reopen the table. Allocation IDs solve
this issue by tracking allocated shard copies in the cluster. This makes it
possible to safely recover a table in the presence of a single shard copy.
Allocation IDs can also distinguish the situation where a table has been
created but none of the shards have been started. If such an table was
inadvertently closed before at least one shard could be started, a fresh shard
will be allocated upon reopening the table.

.. rubric:: Consequence

The primary shard of the table cannot be assigned or a closed table cannot be
re-opened.


Unaware master accepts cluster updates
--------------------------------------

.. raw:: html

   <table class="summary">
   <tr>
    <td>Status</td>
    <td>Fixed in CrateDB v2.0.x (<a
      href="https://github.com/elastic/elasticsearch/issues/13062">#13062</a>)
    </td>
   </tr>
   <tr>
    <td>Severity</td>
    <td>Moderate</td>
   </tr>
   <tr>
    <td>Likelihood</td>
    <td>Very rare</td>
   </tr>
   <tr>
    <td>Cause</td>
    <td>Network problems</td>
   </tr>
   <tr>
    <td>Workloads</td>
    <td>DDL statements</td>
   </tr>
   </table>

.. rubric:: Scenario

If a master has lost quorum (i.e. the number of nodes it is in communication
with has fallen below the configured minimum) it should step down as master and
stop answering requests to perform cluster updates. There is a small risk
window between losing quorum and noticing that quorum has been lost, depending
on your `ping configuration
<https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-discovery-zen.html>`_.

.. rubric:: Consequence

If a cluster update request is made to the node between losing quorum and
noticing the loss of quorum, that request will be confirmed. However, those
updates will be lost because the node will not be able to perform a successful
cluster update.

Cluster state is very important and contains information like shard location,
schemas, and so on. Lost cluster state updates can cause data loss, reset
settings, and problems with table structures.
