.. _ref-grant:

=========
``GRANT``
=========

Grants privilege to an existing user on the whole cluster or on a specific object.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  GRANT { { DQL | DML | DDL | AL [,...] } | ALL [ PRIVILEGES ] }
  [ON {SCHEMA | TABLE | VIEW} identifier [, ...]]
  TO user_name [, ...];

Description
===========

``GRANT`` is a management statement to grant one or many privileges
on the whole cluster or on a specific object to one or many existing users.

``ON {SCHEMA | TABLE | VIEW}`` is optional, if not specified the privilege will
be granted on the ``CLUSTER`` level.

For usage of the ``GRANT`` statement see :ref:`administration-privileges`.

Parameters
==========

:identifier:
  The identifier of the corresponding object.

  If ``TABLE`` or ``VIEW`` is specified the ``identifier`` should include the
  object's full qualified name. Otherwise it will be looked up in
  the current schema.

:user_name:
  The name of an existing user.
