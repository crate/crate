.. highlight:: psql

.. _sql-create-subscription:

=======================
``CREATE SUBSCRIPTION``
=======================

You can use the ``CREATE SUBSCRIPTION`` :ref:`statement <gloss-statement>` to
add a new subscription to the current cluster.

.. SEEALSO::

    :ref:`DROP SUBSCRIPTION <sql-drop-subscription>`

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


.. _sql-create-subscription-synopsis:

Synopsis
========

::

    CREATE SUBSCRIPTION subscription_name
    CONNECTION 'conninfo'
    PUBLICATION publication_name [, ...]

.. _sql-create-subscription-desc:

Description
===========

Create a new :ref:`subscription <logical-replication-subscription>` to one or
more :ref:`publications <logical-replication-publication>` on a publisher. The
subscription name must be distinct from the name of any existing subscription
in the cluster. The subscription represents a replication connection to the
publisher. A logical replication will be started on a publisher once
the subscription is enabled, which is by default on creation.

.. _sql-create-subscription-params:

Parameters
==========

**subscription_name**
  The name of the new subscription.

.. _sql-create-subscription-conn-info:

**CONNECTION 'conninfo'**
  The connection string to the publisher, which is a URL in the following format:
  ::

      crate://host:[port]?params

  Parameters are given in the ``key=value`` format and separated by ``&``. Example:

  ::

      crate://example.com?user=my_user&password=1234&sslmode=disable


  Supported parameters:

  ``mode``: Sets how the subscriber cluster communicates with the publisher
  cluster. Two modes are supported: ``sniff`` (the default) and ``pg_tunnel``.

  In the ``sniff`` mode, the subscriber cluster will use the transport protocol
  to communicate with the other cluster and it will attempt to establish direct
  connections to each node of the publisher cluster. The ``port`` defaults to
  4300.

  In the ``sniff`` mode, there can be multiple ``host:port`` pairs, separated
  by a comma. Parameters will be the same for all hosts. These hosts are used
  as initial seed nodes to discover all eligible nodes from the remote cluster.
  Example:

  ::

      crate://example.com:4310,123.123.123.123


  In the ``pg_tunnel`` mode, the subscriber cluster will initiate the
  connection using the PostgreSQL wire protocol, and then proceed communicating
  via the transport protocol, but within the connection established via the
  PostgreSQL protocol. All requests from the subscriber cluster to the
  publisher cluster will get routed through a single node. The connection is
  only established to the first host listed in the connection string. The ``port``
  defaults to 5432.


  Parameters supported with both modes:

  ``user``: name of the user who connects to a publishing cluster. Required.

  ``password``: user password.

  .. CAUTION::

     The user specified in the connection string, must have ``DQL`` privileges 
     on all tables of the publication on a publisher cluster. Tables, for which
     the user does not have the ``DQL``  privilege, will not be replicated.


  Parameters supported in the ``pg_tunnel`` mode:

  ``sslmode``: Configures whether the connection should use SSL. You must have
  a working SSL setup for the PostgreSQL wire protocol on both the subscriber
  and publisher cluster.

  Allowed values are ``prefer``, ``require`` or ``disable``. Defaults to
  ``prefer``.


**PUBLICATION publication_name**
  Names of the publications on the publisher to subscribe to
