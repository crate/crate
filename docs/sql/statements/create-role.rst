.. highlight:: psql
.. _ref-create-role:

===============
``CREATE ROLE``
===============

Create a new database role.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  CREATE ROLE roleName

Description
===========

``CREATE ROLE`` is a management statement to create a new database role in the
CrateDB cluster. The newly created role does not have any special privileges,
and those must be assigned afterwards, for details see the
:ref:`privileges documentation<administration-privileges>`.

.. NOTE::

    ``ROLE`` is essentially the same as ``USER`` with the difference that a
    ``ROLE`` **cannot** login to the database, and therefore **cannot** be
    assigned a password, and can be :ref:`granted <granting_roles>` to another
    ``USER`` or ``ROLE``. On the other hand, a ``USER`` **can** login to the
    database and **can** also be assigned a password, but **cannot** be granted
    to another ``USER`` or ``ROLE``.

For usages of the ``CREATE ROLE`` statement see
:ref:`administration_user_management`.
