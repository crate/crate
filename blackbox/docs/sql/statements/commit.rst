.. highlight:: psql
.. _ref-commit:

==========
``COMMIT``
==========

Commit the current transaction

.. rubric:: Table of contents

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
