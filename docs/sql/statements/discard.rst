.. highlight:: psql

.. _discard:

===========
``DISCARD``
===========

Discards session state.


Synopsis
========

::

    DISCARD { ALL | PLANS | SEQUENCES | TEMPORARY | TEMP }


Description
===========

Discard releases resources within a session.

``DISCARD ALL`` behaves like ``DEALLOCATE ALL``: it deallocates all previously
prepared SQL statements.

All other variants of the statement have no effect since CrateDB does not cache
query plans, has no sequences, and no temporary tables.
