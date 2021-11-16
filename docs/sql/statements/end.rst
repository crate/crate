.. highlight:: psql
.. _ref-end:

=======
``END``
=======

A synonym for :ref:`ref-commit`.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

   END [ WORK | TRANSACTION ]


Parameters
==========

`WORK`
`TRANSACTION`

Optional keywords. They have no effect.

Description
===========

The statement commits the current transaction.

As CrateDB does not support transactions, this command has no effect and will
be ignored.
