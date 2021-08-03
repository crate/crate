.. highlight:: psql

.. _sql-start-transaction:

=====================
``START TRANSACTION``
=====================

CrateDB accepts the ``START TRANSACTION`` statement for compatibility with the
:ref:`PostgreSQL wire protocol <interface-postgresql>`. However, CrateDB does
not support transactions and will silently ignore this statement.
.. SEEALSO::

    :ref:`Appendix: SQL compatibility <appendix-compatibility>`

.. rubric:: Table of contents

.. contents::
   :local:


Synopsis
========

::

   START TRANSACTION [ transaction_mode [ , ...] ]

Where ``transaction_mode`` is one of::

   ISOLATION LEVEL isolation_level | (READ WRITE | READ ONLY) | [NOT] DEFERRABLE

Where ``isolation_level`` is one of::

   { SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED }


Description
===========

CrateDB will silently ignore the ``START TRANSACTION`` statement.

Parameters
==========

:transaction_mode:
  This parameter has no effect.
