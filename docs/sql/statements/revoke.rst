.. _ref-revoke:

==========
``REVOKE``
==========

Revokes a previously granted privilege on the whole cluster or on a specific
object from a user or a role.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  REVOKE { { DQL | DML | DDL | AL [,...] } | ALL [ PRIVILEGES ] }
  [ON {SCHEMA | TABLE | VIEW} identifier [, ...]]
  FROM name [, ...];

.. code-block:: psql

  REVOKE role_name_to_revoke [, ...] FROM name [, ...]

Description
===========

``REVOKE`` is a management statement which comes in two flavours.

The first one is used to revoke previously granted privileges on a specific
object from one or many existing users or roles.
``ON {SCHEMA | TABLE | VIEW}`` is optional, if not specified the privilege will
be revoked on the ``CLUSTER`` level.

The second one is used to revoke previously granted roles from one or many
existing users or roles. Thus, the users or roles loose the privileges which
had automatically :ref:`inherit <roles_inheritance>` from those previously
granted roles.

For usages of the ``REVOKE`` statement see :ref:`administration-privileges`.

Parameters
==========

:identifier:
  The identifier of the corresponding object.

  If ``TABLE`` or ``VIEW`` is specified the ``identifier`` should include the
  object's full qualified name. Otherwise it will be looked up in
  the current schema.

:role_name_to_revoke:
  The name of the role to revoke from another user or role.

:name:
  The name of an existing user or role.
