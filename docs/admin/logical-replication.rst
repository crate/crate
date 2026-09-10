.. _administration-logical-replication:

===================
Logical replication
===================

Logical replication is a method of data replication across multiple clusters.
CrateDB uses a publish and subscribe model where subscribers pull data from the
publications of the publisher they subscribed to.

Replicated tables on a subscriber can again be published further to other
clusters and thus chaining subscriptions is possible.

.. NOTE::

    Tables replicated by a subscription are read-only on the subscriber.
    While a table is subscribed, only ``SELECT``, ``SHOW CREATE``,
    ``REFRESH``, ``OPTIMIZE``, ``COPY TO`` and some ``ALTER TABLE`` variants
    are supported. ``INSERT``, ``UPDATE``, ``DELETE``, ``DROP`` and other
    ``ALTER TABLE`` variants are rejected, and subscribed tables cannot be
    included in snapshots. See :ref:`read-only behavior
    <logical-replication-read-only>`.

Logical replication is useful for the following use cases:

.. rst-class:: open

- Consolidating data from multiple clusters into a single one for aggregated
  reports.

- Ensure read availability if one cluster becomes unavailable. The subscriber
  can continue to serve queries while the publisher is down. Write access on
  the subscriber is not failed over automatically, see
  :ref:`logical-replication-disaster-recovery`.

- Replicating between different compatible versions of CrateDB.
  Replicating tables created on a cluster with higher major/minor version to a
  cluster with lower major/minor version is not supported.


.. SEEALSO::

    :ref:`replication.logical.ops_batch_size <replication.logical.ops_batch_size>`
    :ref:`replication.logical.reads_poll_duration <replication.logical.reads_poll_duration>`
    :ref:`replication.logical.recovery.chunk_size <replication.logical.recovery.chunk_size>`
    :ref:`replication.logical.recovery.max_concurrent_file_chunks <replication.logical.recovery.max_concurrent_file_chunks>`

.. _logical-replication-publication:

Publication
-----------

A publication is the upstream side of logical replication and it's created on
the cluster which acts as a data source.

Each table can be added to multiple publications if needed. Publications can
only contain tables. All operation types (``INSERT``, ``UPDATE``, ``DELETE`` and
schema changes) are replicated.

Every publication can have multiple subscribers.

A publication is created using the
:ref:`CREATE PUBLICATION <sql-create-publication>` command. The individual
tables can be added or removed dynamically using
:ref:`ALTER PUBLICATION <sql-alter-publication>`. Publications can be removed
using the :ref:`DROP PUBLICATION <sql-drop-publication>` command.

.. CAUTION::

    The publishing cluster should have:
    :ref:`sql-create-table-soft-deletes-retention-lease-period` greater than or
    equal to
    :ref:`replication.logical.reads_poll_duration <replication.logical.reads_poll_duration>`.


.. _logical-replication-subscription:

Subscription
------------

A subscription is the downstream side of logical replication. A subscription
defines the connection to another database and set of publications to which it
wants to subscribe. By default, the subscription creation triggers the replication
process on the subscriber cluster. The subscriber cluster behaves in the same
way as any other CrateDB cluster and can be used as a publisher for other
clusters by defining its own publications.

A cluster can have multiple subscriptions. It is also possible for a cluster to
have both subscriptions and publications. A cluster cannot subscribe to locally
already existing tables, therefore it is not possible to setup a bi-directional
replication (both sides subscribing to ``ALL TABLES`` leads to a cluster trying
to replicate its own tables from another cluster). However, two clusters still
can cross-subscribe to each other if one cluster subscribes to locally
non-existing tables of another cluster and vice versa.

A subscription is added using the
:ref:`CREATE SUBSCRIPTION <sql-create-subscription>` command and can be
removed using the :ref:`DROP SUBSCRIPTION <sql-drop-subscription>` command.
A subscription starts replicating on its creation and stops on its removal
(if no failure happen in-between).

Published tables must not exist on the subscriber. A cluster cannot subscribe
to a table on another cluster if it exists already on its side, therefore it's
not possible to drop and re-create a subscription without starting from scratch
i.e removing all replicated tables.

Only regular tables (including partitions) may be the target of a replication.
For example, you can not replicate system tables or views.

The tables are matched between the publisher and the subscriber using the fully
qualified table name. Replication to differently-named tables on the subscriber
is not supported.

.. _logical-replication-read-only:

Read-only behavior
------------------

The read-only restriction is tied to the table on the subscriber, not to the
health of the publisher. It stays in effect even when the publisher cluster is
unreachable or when the subscription is in the ``e`` (failed) state visible in
:ref:`pg_subscription_rel`.

There is no automatic failover of write access. A subscribed table becomes a
regular writable table only when:

.. rst-class:: open

- The subscription is removed with ``DROP SUBSCRIPTION``. This is a local
  operation on the subscriber cluster and does not require the publisher to
  be reachable.

- The table is dropped on the publisher, which causes the subscriber to stop
  tracking it.

After a subscription is removed, replication cannot be re-established over
the same tables without dropping them first.

.. _logical-replication-disaster-recovery:

Disaster recovery
-----------------

Logical replication provides a warm stand-by with continuous read access, but
it does not provide automatic failover, and switching back is not supported:

.. rst-class:: open

- **Publisher outage:** The subscriber keeps retrying and resumes replicating
  automatically once the publisher is reachable again. No manual action is
  needed, and subscribed tables remain read-only during the outage.

- **Total loss of the publisher:** The subscriber can be promoted by running
  ``DROP SUBSCRIPTION``. The tables turn into
  regular writable tables. This is a one-way decision: the promoted cluster
  becomes the new source of truth, and it diverges permanently from any
  future publisher. Data written on the subscriber after the last
  successfully replicated operation is retained, so the replication lag at
  the time of failure determines the number of lost writes.

- **Switching back:** Returning to the original cluster requires re-seeding
  it. A subscription cannot be created over tables that already exist on the
  subscriber, not even over empty tables with a matching schema. There is no
  incremental way to switch back.

- **Snapshots:** A snapshot restore cannot be combined with a subscription.
  Subscribed tables cannot be included in snapshots, and a subscription
  cannot be created over tables restored from a snapshot. Snapshots for
  backup purposes must therefore be taken on the publisher, or on a cluster
  that is not a subscription target.

Security
--------

To create, alter or drop a publication, a user must have the ``AL`` privilege
on the cluster. Only the owner (the user who created the publication) or a
superuser is allowed to ``ALTER`` or ``DROP`` a publication.
To add tables to a publication, the user must have
``DQL``, ``DML``, and ``DDL`` privileges on the table. When a user creates a
publication that publishes all tables automatically, only those tables where the
user has ``DQL``, ``DML``, and ``DDL`` privileges will be published.
The user a subscriber uses to connect to the publisher must have ``DQL``
privileges on the published tables. Tables, included into a publication but
not available for a subscriber due to lack of ``DQL`` privilege, will not be
replicated.


To create or drop a subscription, a user must have the ``AL`` privilege
on the cluster. Only the owner (the user who created the subscription) or a
superuser is allowed to ``DROP`` a subscription.

.. CAUTION::

   A network setup that allows the two clusters to communicate is a
   pre-requisite for a working publication/subscription setup.
   See :ref:`HBA <admin_hba_node>`.

Monitoring
----------

All publications are listed in the :ref:`pg_publication` table.
More details for a publication are available in the
:ref:`pg_publication_tables` table. It lists the replicated tables for a
specific publication.

All subscriptions are listed in the :ref:`pg_subscription` table.
More details for a subscription are available in the :ref:`pg_subscription_rel`
table. The table contains detailed information about the replication state per
table, including error messages if there was an error.

A per-table state of ``e`` indicates the subscription failed for that table;
the reason is shown in ``srsubstate_reason``. A failed subscription is not
removed automatically and its tables remain read-only; it must be removed
explicitly with ``DROP SUBSCRIPTION``. Transient
publisher outages do not change the state, the metadata tracker retries until
the publisher is reachable again.
