.. highlight:: psql

.. _sql-drop-view:

=============
``DROP VIEW``
=============

Drop one or more :ref:`views <ddl-views>`.

Synopsis
========

::

    DROP VIEW [ IF EXISTS ] view_name [ , ... ]


Description
===========

``DROP VIEW`` drops one or more existing views.

If a view doesn't exist an error will be returned, unless ``IF EXISTS`` is
used, in which case all matching existing views will be dropped.

.. SEEALSO::

    :ref:`SQL syntax: CREATE VIEW <sql-create-view>`
