.. highlight:: psql
.. _ref-set-transation:

===================
``SET TRANSACTION``
===================

Sets the characteristics of transactions.

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========

::

    SET SESSION CHARACTERISTICS AS TRANSACTION transaction_mode [, ...]

Description
===========

``SET SESSION CHARACTERISTICS`` sets the default transaction characteristics for
subsequent transactions of a session.

As CrateDB does not support transactions, this command has no effect and will be
ignored. The support was added for compatibility reasons.

