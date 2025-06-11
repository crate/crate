.. highlight:: psql
.. _ref-set-transaction:

===================
``SET TRANSACTION``
===================

Sets the characteristics of transactions.


Synopsis
========

::

    SET SESSION CHARACTERISTICS AS TRANSACTION transaction_mode [, ...]
    SET TRANSACTION transaction_mode [, ...]


    where transaction_mode is one of:

        ISOLATION LEVEL { SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED }
        READ WRITE | READ ONLY
        [ NOT ] DEFERRABLE

Description
===========

``SET SESSION CHARACTERISTICS`` sets the default transaction characteristics for
subsequent transactions of a session.

As CrateDB does not support transactions, this command has no effect and will be
ignored. The support was added for compatibility reasons.
