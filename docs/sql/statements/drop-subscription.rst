.. highlight:: psql

.. _sql-drop-subscription:

=====================
``DROP SUBSCRIPTION``
=====================

.. SEEALSO::

    :ref:`CREATE SUBSCRIPTION <sql-create-subscription>`


Synopsis
========

::

    DROP SUBSCRIPTION [ IF EXISTS ] name

.. _sql-drop-subscription-desc:

Description
===========

Removes an existing subscription from the cluster and stops the replication.
Existing tables will turn into regular writable tables. It's not possible to
resume dropped subscription.

The subscription is removed locally on the subscriber cluster; the publisher
does not need to be reachable. Replication cannot be resumed afterwards, and
the replicated tables cannot be subscribed to again without dropping them.

.. _sql-drop-subscription-params:

Parameters
===========

.. _sql-drop-subscription-name:

**name**
  The name of the subscription to be deleted.
