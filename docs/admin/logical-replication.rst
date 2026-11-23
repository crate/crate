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

    A replicated table or individual partition on a subscriber is read-only.

Logical replication is useful for the following use cases:

.. rst-class:: open

- Consolidating data from multiple clusters into a single one for aggregated
  reports.

- Ensure high availability if one cluster becomes unavailable.

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

Each table or individual partition can be added to multiple publications if
needed. For published tables, ``INSERT``, ``UPDATE``, ``DELETE`` and schema
changes are replicated. For individually published partitions, ``INSERT``,
``UPDATE`` and ``DELETE`` operations affecting those partitions are replicated,
while schema changes are rejected.

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

For whole-table subscriptions, published tables must not exist on the
subscriber. For individual partition subscriptions, the target partitions must
not exist; see :ref:`logical-replication-partitions`. Dropping a subscription
preserves its replicated data, so creating it again requires removing the
previously replicated tables or partitions first.

Only regular tables (including partitions) may be the target of a replication.
For example, you can not replicate system tables or views.

The tables are matched between the publisher and the subscriber using the fully
qualified table name. Replication to differently-named tables on the subscriber
is not supported.

.. _logical-replication-partitions:

Replicating individual partitions
---------------------------------

Use the ``PARTITION`` clause in a publication to replicate individual
partitions of a table.

For example, individual partition replication can combine data from multiple
remote sites in one table on a central cluster. Each site has a ``doc.events``
table partitioned by ``site_id``.

On site A, publish its partition::

    CREATE PUBLICATION pub_events_a
    FOR TABLE doc.events PARTITION (site_id = 'a');

On site B, publish its partition::

    CREATE PUBLICATION pub_events_b
    FOR TABLE doc.events PARTITION (site_id = 'b');

On the central cluster, subscribe to both publications::

    CREATE SUBSCRIPTION sub_events_a
    CONNECTION 'crate://site-a.example.com:4300?user=user_a&password=secret&mode=pg_tunnel'
    PUBLICATION pub_events_a;

    CREATE SUBSCRIPTION sub_events_b
    CONNECTION 'crate://site-b.example.com:4300?user=user_b&password=secret&mode=pg_tunnel'
    PUBLICATION pub_events_b;

The subscriptions copy the selected partitions into the same ``doc.events``
table and replicate their changes. Once the data is replicated, query both
sites on the central cluster::

    SELECT site_id, count(*) FROM doc.events GROUP BY site_id;

Publisher behavior and restrictions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``INSERT``, ``UPDATE`` and ``DELETE`` operations affecting individually
published partitions are replicated to their subscribers.

.. CAUTION::

    Partitions share their parent table's schema. Schema changes to a table
    that has individually published partitions are therefore rejected on the
    publisher. This also applies to automatic column creation through dynamic
    objects.

If an individually published partition is dropped on the publisher, its
subscription target is removed, but its replicated data remains on the
subscriber and becomes writable. This also applies when ``DELETE`` removes a
whole partition. In contrast, when the whole table is published, dropping one
of its partitions also removes that partition on the subscriber.

Dropping the parent table on the publisher also stops replication of its
individually published partitions and preserves their subscriber copies.

Subscriber behavior and restrictions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unlike whole-table replication, individual partitions can be replicated into
an existing table, provided its schema is compatible with the publisher and
the target partition does not already exist. If the parent table does not
exist, it is created as part of replication. Compatibility includes matching
column names, types as well as column OIDs. For example, creating and dropping
columns can result in different column OIDs, making the schemas incompatible.

Subscribed partitions are read-only. Other local partitions of the same table
remain writable with ``INSERT``, ``UPDATE`` and ``DELETE``. While any partition
is subscribed, table-level operations such as dropping the table or changing
its schema remain restricted.

As with whole-table subscriptions, dropping a subscription to an individual
partition preserves the replicated data and makes it writable.

Security
--------

To create, alter or drop a publication, a user must have the ``AL`` privilege
on the cluster. Only the owner (the user who created the publication) or a
superuser is allowed to ``ALTER`` or ``DROP`` a publication.
To add tables or individual partitions to a publication, the user must have
``DQL``, ``DML``, and ``DDL`` privileges on the table. For individual partitions,
these privileges are checked on the parent table. When a user creates a
publication that publishes all tables automatically, only those tables where the
user has ``DQL``, ``DML``, and ``DDL`` privileges will be published.
The user a subscriber uses to connect to the publisher must have ``DQL``
privileges on the published tables, including the parent tables of individually
published partitions. Tables, included into a publication but
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

(TODO: Exposing individual partition identifiers in
``pg_publication_tables`` and ``pg_subscription_rel`` is not yet implemented.)
