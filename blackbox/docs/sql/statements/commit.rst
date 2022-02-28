.. highlight:: psql
.. _ref-commit:

==========
``COMMIT``
==========

.. include:: ../../_include/version-note.rst

Commit the current transaction

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========

::

   COMMIT

Description
===========

The statement commits the current transaction.

As CrateDB does not support transactions, this command has no effect and will
be ignored.
