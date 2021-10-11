.. _administration-logical-replication:

===================
Logical replication
===================

.. rubric:: Table of contents

.. contents::
   :local:

Logical replication is a method of data replication across multiple clusters.
CrateDB uses a publish and subscribe model where subscribers pull data from the
publications of the publisher they subscribed to.

Replicated tables on a subscriber can be in turn, published further to other
clusters and thus chaining subscriptions is possible.

.. NOTE::

    A replicated index on a subscriber is read-only.

Logical replication is useful for the following use cases:

.. rst-class:: open

- Ensure High Availability if one cluster becomes unavailable.

- Replicating between different major versions of CrateDB via chain of
  compatible versions. Adjacent major versions are compatible.

- Consolidating multiple clusters into a single one for aggregated reports.

.. _logical-replication-publication:

Publication
-----------

A publication is a set of changes generated from a table or a group of tables,
and might also be described as a replication set.

Publications are different from schemas and do not affect how the table is
accessed. Each table can be added to multiple publications if needed.
Publications can only contain tables. All operation types
(INSERT, UPDATE, DELETE) are replicated.

Every publication can have multiple subscribers.

A publication is created using the
:ref:`CREATE PUBLICATION <sql-create-publication>` command and may later be
altered or dropped using corresponding commands. The individual tables can be
added and removed dynamically using ALTER PUBLICATION.

.. _logical-replication-subscription:

Subscription
------------

A subscription is the downstream side of logical replication. A subscription
defines the connection to another database and set of publications to which it
wants to subscribe. By default, subscription creation triggers the replication
process on the subscriber cluster. The subscriber cluster behaves in the same
way as any other CrateDB cluster and can be used as a publisher for other
clusters by defining its own publications.

A subscriber may have multiple subscriptions if desired. It is possible to
define multiple subscriptions between a single publisher-subscriber pair, in
which case care must be taken to ensure that the subscribed publication tables
don't overlap.

A subscription is added using
:ref:`CREATE SUBSCRIPTION <sql-create-subscription>` command and can be
stopped/resumed at any time using the
:ref:`ALTER SUBSCRIPTION <sql-alter-subscription>` command and removed using
:ref:`DROP SUBSCRIPTION <sql-drop-subscription>` command.

When a subscription is dropped and recreated, the synchronization information
is lost. This means that the data has to be resynchronized afterwards.

Published tables must not exist on the subscriber. A cluster cannot subscribe
to a table on another cluster if it exists already on its side. Only regular
tables (including partitions) may be the target of replication. For example,
you can't replicate a view.

The tables are matched between the publisher and the subscriber using the fully
qualified table name. Replication to differently-named tables on the subscriber
is not supported.
