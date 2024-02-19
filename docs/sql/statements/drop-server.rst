.. highlight:: psql
.. _ref-drop-server:

===========
DROP SERVER
===========

Drop a foreign server.


.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  DROP SERVER [ IF EXISTS ] name [, ...] [ CASCADE | RESTRICT ]

Description
===========

``DROP SERVER`` is a DDL statement that drops one or more foreign servers.

Dropping a server requires ``AL`` permission on cluster level.

Parameters
==========

:name:

The name of the server to drop. Separate multiple names by comma to drop
multiple servers at once.


Clauses
=======

``IF EXISTS``
-------------

By default, ``DROP SERVER`` raises an error if the given servers don't exist. If
the optional ``IF EXISTS`` clause is used, the statement won't raise an error if
any servers listed don't exist.


``CASCADE | RESTRICT``
----------------------

``RESTRICT`` causes ``DROP SERVER`` to raise an error if any foreign table or
user mappings for the given servers exist. This is the default

``CASCADE`` instead causes ``DROP SERVER`` to also delete all foreign tables and
mapped users using the given servers.

.. seealso::

   - :ref:`administration-fdw`
   - :ref:`ref-create-server`
