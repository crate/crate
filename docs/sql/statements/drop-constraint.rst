.. _drop-constraint:

===================
``DROP CONSTRAINT``
===================

Remove a :ref:`check_constraint` constraint from a table.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: sql

    ALTER TABLE table_ident DROP CONSTRAINT check_name

Description
===========

Removes a CHECK constraint from the table.

.. WARNING::

    Removed CHECK constraints cannot be re-added to an
    existing table once dropped.


Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of the table.

:check_name:
  The name of the check constraint to be removed.
