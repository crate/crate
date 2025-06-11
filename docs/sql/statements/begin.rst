.. highlight:: psql
.. _ref-begin:

=========
``BEGIN``
=========

Start a transaction block.

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

The statement starts a transaction block.

As CrateDB does not support transactions, the only effect of this command is
to start a scope in which cursors ``WITHOUT HOLD`` can be
:ref:`declared <sql-declare>`.

.. NOTE::

   Cursors ``WITHOUT HOLD`` are closed automatically after an
   :ref:`END <ref-end>` or :ref:`COMMIT <ref-commit>` command. There is no
   nesting and this happens regardless of how many times `BEGIN` has run.

.. NOTE::

  For backwards compatibility reasons, the commas between successive
  ``transaction_modes`` can be omitted.

Parameters
==========

:WORK | TRANSACTION:
  Optional key words. They have no effect.

:transaction_mode:
  The transactional mode parameter. It has no effect.
