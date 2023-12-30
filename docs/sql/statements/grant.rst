.. _ref-grant:

=========
``GRANT``
=========

Grants privilege to an existing user or role on the whole cluster or on a
specific object, or grant one or more roles to a user or role.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  GRANT { { DQL | DML | DDL | AL [,...] } | ALL [ PRIVILEGES ] }
  [ON {SCHEMA | TABLE | VIEW} identifier [, ...]]
  TO name [, ...]

  GRANT role_name_to_grant [, ...] TO name [, ...]

Description
===========

``GRANT`` is a management statement which comes in two flavours.

The first one is used to grant one or many privileges on the whole cluster or on
a specific object to one or many existing users or roles.
``ON {SCHEMA | TABLE | VIEW}`` is optional, if not specified the privilege will
be granted on the ``CLUSTER`` level.

With the second one, ``GRANT`` can be used to grant one or more roles to one or
many others or roles. Thus, the users (or roles) inherit the privileges of the
roles which they are :ref:`granted<roles_inheritance>`.

For usage of the ``GRANT`` statement see :ref:`administration-privileges`.

Parameters
==========

:identifier:
  The identifier of the corresponding object.

  If ``TABLE`` or ``VIEW`` is specified the ``identifier`` should include the
  object's full qualified name. Otherwise it will be looked up in
  the current schema.

:role_name_to_grant:
  The name of the role to grant to another user or role.

:name:
  The name of an existing user or role.
