.. highlight:: psql

.. _sql-fetch:

=========
``FETCH``
=========

Fetch rows from a cursor.

.. contents::
   :local:

.. _sql-fetch-synopsis:

Synopsis
========

::

    FETCH [ direction [ FROM | IN ] ] cursor_name

Where direction can be empty or one of:

    NEXT
    RELATIVE count
    count
    ALL
    FORWARD
    FORWARD count
    FORWARD ALL


Description
===========

Fetches rows from a cursor created using :ref:`DECLARE <sql-declare>`.

A cursor has a position and each time you use ``FETCH``, the position changes
and the rows spanning the position change get returned.


Parameters
===========


``direction``
.............

:NEXT:
  Fetch the next row. This is the default

:RELATIVE count:
  Fetch ``count`` rows relative to the current position.

:count:
  Fetch the next ``count`` rows

:ALL:
  Fetch all remaining rows

:FORWARD:
  Same as ``NEXT``

:FORWARD count:
  Same as ``count``

:FORWARD ALL:
  Same as ``ALL``


``count``
.........

A integer constant, determining which or how many rows to fetch


``cursor_name``
...............

Name of the cursor to fetch rows from.
