.. highlight:: psql
.. _ref-begin:

=========
``BEGIN``
=========

Start a transaction block

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

   BEGIN [ WORK | TRANSACTION ] [ transaction_mode [ , ...] ]

where ``transaction_mode`` is one of::

   ISOLATION LEVEL isolation_level | (READ WRITE | READ ONLY) | [NOT] DEFERRABLE

where ``isolation_level`` is one of::

   { SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED }

Description
===========

The statement starts a transaction block until it committed or rolled back.

As CrateDB does not support transactions, this command has no effect and will
be ignored.

.. NOTE::

  For backwards compatibility reasons, the commas between successive
  ``transaction_modes`` can be omitted.

Parameters
==========

:WORK | TRANSACTION:
  Optional key words. They have no effect.

:transaction_mode:
  The transactional mode parameter. It has no effect.
