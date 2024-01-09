.. highlight:: psql
.. _ref-drop-role:

=============
``DROP ROLE``
=============

Drop an existing database user or role.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  DROP ROLE [ IF EXISTS ] name;

Description
===========

``DROP ROLE`` is a management statement to remove an existing database user or
role from the CrateDB cluster.

For usage of the ``DROP ROLE`` statement see
:ref:`administration_user_management`.

Parameters
==========

:IF EXISTS:
  Do not fail if the user or role doesn't exist.

:name:
  The unique name of the database user or role to be removed.

  The name follows the principles of a SQL identifier (see
  :ref:`sql_lexical_keywords_identifiers`).

.. NOTE::

  If a role is granted to one ore more other roles and/or users, it cannot be
  dropped. The role must first be revoked from those roles and/or users.

