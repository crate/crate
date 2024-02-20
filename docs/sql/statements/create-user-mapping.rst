.. highlight:: psql
.. _ref-create-user-mapping:

===================
CREATE USER MAPPING
===================

Create a user mapping for a foreign server.


.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  CREATE USER MAPPING [ IF NOT EXISTS ] FOR { user_name | USER | CURRENT_ROLE | CURRENT_USER }
      SERVER server_name
      [ OPTIONS ( option value [ , ... ] ) ]

Description
===========

``CREATE USER MAPPING`` is a DDL statement that maps a CrateDB user to another
user on a foreign server.

To create a user mapping you must first create a foreign server using
:ref:`ref-create-server`.

The created user mappings will be used whenever a foreign table is queried. If
no user mapping exists, foreign data wrappers usually attempt to connect with
the username of the current CrateDB user. How exactly is specific to the
concrete foreign data wrapper implementation. See :ref:`administration-fdw` for
more details.

Creating a user mapping requires ``AL`` permission on cluster level.

User mappings are visible in the :ref:`user_mappings` table.

Parameters
==========

:user_name:
  The name of the CrateDB user.

:server_name:
  Name of the server for which to create the user mapping. See :ref:`ref-create-server`.

Clauses
=======

``USER``
--------

Creates a user mapping for the current user.

Aliases: ``CURRENT_USER`` and ``CURRENT_ROLE``


``OPTIONS``
-----------

:option value:

Key value pairs defining foreign data wrapper specific user options for the
server.

For example, for the ``jdbc`` foreign data wrapper, the options ``user`` and
``password`` are supported.

.. code-block:: psql


.. seealso::

   - :ref:`administration-fdw`
   - :ref:`ref-drop-user-mapping`
