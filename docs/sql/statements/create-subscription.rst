.. highlight:: psql

.. _sql-create-subscription:

=======================
``CREATE SUBSCRIPTION``
=======================

You can use the ``CREATE SUBSCRIPTION`` :ref:`statement <gloss-statement>` to
add a new subscription into the current cluster.

.. SEEALSO::

    :ref:`ALTER SUBSCRIPTION <sql-alter-subscription>`
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
    [ WITH (parameter_name [= value], [, ...]) ]

.. _sql-create-subscription-desc:

Description
===========

Create a new :ref:`subscription <logical-replication-subscription>` to one or
more :ref:`publications <logical-replication-publication>` on a publisher. The
subscription name must be distinct from the name of any existing subscription
in the cluster. The subscription represents a replication connection to the
publisher. A logical replication will be started on a publisher once
subscription is enabled, which is by default on creation.

.. _sql-create-subscription-params:

Parameters
==========

**subscription_name**
  The name of the new subscription.

.. _sql-create-subscription-conn-info:

**CONNECTION 'conninfo'**
  The connection string to the publisher, which is URL in the following format:
  ::

      crate://host:[port]?params

  Port is optional and default value is ``4300``. Parameters are given in the
  ``key=value`` format and separated by ``&``. Example:

  ::

      crate://example.com?user=my_user&password=1234&sslmode=disable

  There can be multiple ``host:port`` pairs, separated by a comma. Parameters
  will be same for all hosts. Example:

  ::

      crate://example.com:4310,123.123.123.123

  Supported parameters:

  ``user``: name of the user who connects to a publishing cluster. Required.

  ``password``: user password.

  ``seeds``:  A comma separated list of initial seed nodes to discover eligible
  nodes from the remote cluster.


The following options are only supported with the ``pg_tunnel`` mode:

  ``sslmode``: Configures whether the connection should use SSL. You must have
  a working SSL setup for the PostgreSQL wire protocol on both the subscriber
  and publisher cluster.

  Allowed values are ``require`` or ``disable``. Defaults to ``disable``.


**PUBLICATION publication_name**
  Names of the publications on the publisher to subscribe to

Clauses
=======

``WITH``
--------

You can use the ``WITH`` clause to specify one or more repository parameter
values:

::

    [ WITH (parameter_name [= value], [, ...]) ]

Parameters
----------

This clause specifies optional parameters for a subscription. The following
parameters are supported:

**enabled**
  Specifies whether the subscription should be actively replicating, or whether
  it should be just setup but not started yet. The default is true.


