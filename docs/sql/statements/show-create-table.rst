.. highlight:: psql
.. _ref-show-create-table:

=====================
``SHOW CREATE TABLE``
=====================

Shows the :ref:`sql-create-table` or :ref:`ref-create-foreign-table` statement
that created the table.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

    SHOW CREATE TABLE table_ident

Description
===========

``SHOW CREATE TABLE`` can be used to dump the schema of an existing
user-created table.

It is not possible to invoke ``SHOW CREATE TABLE`` on blob tables or system
tables.

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of the table which should be printed.
