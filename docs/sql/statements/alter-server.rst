.. highlight:: psql
.. _ref-alter-server:

=================
``ALTER SERVER``
=================

Alter an existing foreign server

.. rubric:: Table of contents

.. contents::
   :local:


Synopsis
========

.. code-block:: psql

  ALTER SERVER server_name
  OPTIONS ( [ ADD | SET | DROP ] option ['value'] [, ... ] )

Description
===========

``ALTER SERVER`` is a DDL statement that changes the options of an existing
foreign server.

The servers options added, changed or dropped via the ``ALTER SERVER`` are
visible in
:ref:`information_schema.foreign_server_options <foreign_server_options>`.

Altering a server requires ``AL`` permission on cluster level.

Parameters
==========

:server_name:
  A unique name for the server.


Clauses
=======

``OPTIONS``
-----------

:[ ADD | SET | DROP ] option ['value']:
  Change options for the server. ``ADD``, ``SET``, and ``DROP`` specify the
  action to be performed. If no operation is explicitly specified, default to
  ``ADD``. Option names must be unique, duplicate names will result in an error.


See :ref:`administration-fdw` for the foreign data wrapper specific options.

.. seealso::

   - :ref:`ref-create-server`
   - :ref:`ref-drop-server`
