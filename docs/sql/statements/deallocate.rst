.. _ref-deallocate:
.. highlight:: psql

==============
``DEALLOCATE``
==============

Deallocate a prepared statement

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

    DEALLOCATE [PREPARE] { name | ALL }

Description
===========

DEALLOCATE is used to deallocate a previously prepared SQL statement and free
the reserved resources. It is not meant to be explicitly issued by a user but
it's used by some clients (e.g. `libpq`_) over :ref:`Postgres Wire Protocol
<interface-postgresql>` as an alternative way of deallocating a prepared
statement to the protocol's ``Close (F)`` message.

Parameters
==========

:name:
  The name of the prepared statement to deallocate.

:ALL:
  Deallocate all prepared statements

.. _libpq: https://www.postgresql.org/docs/10/static/libpq.html
