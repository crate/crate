.. highlight:: psql
.. _ref-drop-view:

=============
``DROP VIEW``
=============

Drop one or more views.

.. rubric:: Table of Contents

.. contents::
    :local:

Synopsis
========

::

    DROP VIEW [ IF EXISTS ] view_name [ , ... ]


Description
===========

Drop view drops one or more existing views.

If a view doesn't exist an error will be returned, unless ``IF EXISTS`` is
used, in which case all matching existing views will be dropped.

.. SEEALSO::

    :ref:`ref-create-view`
