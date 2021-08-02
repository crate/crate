.. _ref-revoke:

==========
``REVOKE``
==========

Revokes a previously granted privilege on the whole cluster or on a specific
object.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  REVOKE { { DQL | DML | DDL | AL [,...] } | ALL [ PRIVILEGES ] }
  [ON {SCHEMA | TABLE | VIEW} identifier [, ...]]
  FROM user_name [, ...];

Description
===========

``REVOKE`` is a management statement to revoke previously granted privileges
on a specific object from one or many existing users.

``ON {SCHEMA | TABLE | VIEW}`` is optional, if not specified the privilege will
be revoked on the ``CLUSTER`` level.

For usage of the ``REVOKE`` statement see :ref:`administration-privileges`.

Parameters
==========

:identifier:
  The identifier of the corresponding object.

  If ``TABLE`` or ``VIEW`` is specified the ``identifier`` should include the
  object's full qualified name. Otherwise it will be looked up in
  the current schema.

:user_name:
  The name of an existing user.
