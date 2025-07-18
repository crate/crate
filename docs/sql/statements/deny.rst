.. _ref-deny:

========
``DENY``
========

Denies privilege to an existing user on the whole cluster or on a specific
object.

Synopsis
========

.. code-block:: psql

  DENY { { DQL | DML | DDL | AL [,...] } | ALL [ PRIVILEGES ] }
  [ON {SCHEMA | TABLE} identifier [, ...]]
  TO user_name [, ...];

Description
===========

``DENY`` is a management statement to deny one or many privileges
on a specific object to one or many existing users.

``ON {SCHEMA | TABLE}`` is optional, if not specified the privilege will be
denied on the ``CLUSTER`` level.

For usage of the ``DENY`` statement see :ref:`administration-privileges`.

Parameters
==========

:identifier:
  The identifier of the corresponding object.

  If ``TABLE`` is specified the ``identifier`` should include the
  table's full qualified name. Otherwise the table will be looked up in
  the current schema.

:user_name:
  The name of an existing user.
