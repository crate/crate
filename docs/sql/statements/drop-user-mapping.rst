.. highlight:: psql
.. _ref-drop-user-mapping:

=====================
``DROP USER MAPPING``
=====================

Drops a user mapping for a foreign server.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  DROP USER MAPPING [ IF EXISTS ] FOR { user_name | USER | CURRENT_ROLE | CURRENT_USER } SERVER server_name

Description
===========

``DROP USER MAPPING`` is a DDL statement that removes a user mapping for a
foreign server.

Dropping a user mapping requires ``AL`` permission on cluster level.

Parameters
==========

:user_name:
  The name of the CrateDB user

:server_name:
  Name of the server for which to create the user mapping. See :ref:`ref-create-server`.

Clauses
=======

``IF EXISTS``
-------------

By default ``DROP FOREIGN TABLE`` raises an error if the given tables don't
exist. With the ``IF EXISTS`` clause the statement won't raise an error if any
of the given tables don't exist.

``USER``
--------

Uses the current user.

Aliases: ``CURRENT_USER`` and ``CURRENT_ROLE``

.. seealso::

   - :ref:`administration-fdw`
   - :ref:`ref-create-user-mapping`
