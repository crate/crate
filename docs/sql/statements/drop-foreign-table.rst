.. highlight:: psql
.. _ref-drop-foreign-table:

==================
DROP FOREIGN TABLE
==================

Drops a foreign table.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  DROP FOREIGN TABLE [ IF EXISTS ] name [, ...]

Description
===========

``DROP FOREIGN TABLE`` is a DDL statement that removes a foreign table.


Parameters
==========

:name:
  The name of the table to drop.
  Separate multiple names by comma to drop multiple tables at once.

Clauses
=======

``IF EXISTS``
-------------

By default ``DROP FOREIGN TABLE`` raises an error if the given tables don't
exist. With the ``IF EXISTS`` clause the statement won't raise an error if any
of the given tables don't exist.

.. seealso::

   - :ref:`administration-fdw`
   - :ref:`ref-create-foreign-table`
