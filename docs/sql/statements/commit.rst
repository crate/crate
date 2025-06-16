.. highlight:: psql
.. _ref-commit:

==========
``COMMIT``
==========

Commit the current transaction.

Synopsis
========

::

   COMMIT [ WORK | TRANSACTION ]


Parameters
==========

`WORK`
`TRANSACTION`

Optional keywords. They have no effect.


Description
===========

The statement commits the current transaction.

As CrateDB does not support transactions, the only effect of this command is
to close all existing cursors ``WITHOUT HOLD`` in the current session.
