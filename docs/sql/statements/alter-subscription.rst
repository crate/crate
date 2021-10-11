.. highlight:: psql

.. _sql-alter-subscription:

======================
``ALTER SUBSCRIPTION``
======================

You can use the ``ALTER SUBSCRIPTION`` :ref:`statement <gloss-statement>` to
update subscription status on the current cluster.

.. SEEALSO::

    :ref:`CREATE SUBSCRIPTION <sql-create-subscription>`
    :ref:`DROP SUBSCRIPTION <sql-drop-subscription>`

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


.. _sql-alter-subscription-synopsis:

Synopsis
========

::

    ALTER SUBSCRIPTION name (ENABLE | DISABLE)

.. _sql-alter-subscription-desc:

Description
===========

Disable or enable the subscription. Disabling subscription does not remove it
but stops getting updates from the publisher. After re-enabling it replication
resumes back. The replication will try to catch up with all changes made
in-between which could cause some initial delay based on the amount of changes.
Availability of the recent changes depends on
:ref:`retention setting <sql-create-table-soft-deletes-retention-lease-period>`
For update and delete operations, changes may not be available anymore on the
source shard, resulting in an error. In such cases a subscription must be
re-created to trigger a full restore of the data.

Parameters
==========

**name**
  The name of the subscription to be updated.

**ENABLE**
  Enables the previously disabled subscription.

**DISABLE**
  Disables the running subscription.