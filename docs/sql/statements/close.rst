.. highlight:: psql

.. _sql-close:

=========
``CLOSE``
=========

Close a cursor.

.. _sql-close-synopsis:

Synopsis
========

::

    CLOSE { name | ALL }


Description
===========

Closes cursors created with :ref:`DECLARE <sql-declare>`

``CLOSE ALL`` closes all cursors. ``CLOSE name`` closes the cursor identified by
its name. ``CLOSE name`` on a cursor that does not exist results in an error.
