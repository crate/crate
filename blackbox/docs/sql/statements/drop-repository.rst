.. highlight:: psql
.. _ref-drop-repository:

===================
``DROP REPOSITORY``
===================

Unregister a repository.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

    DROP REPOSITORY repository_name;

Description
===========

DROP REPOSITORY will unregister an existing repository from the cluster.

It cannot be used for creating, dropping or restoring snapshots anymore.

.. NOTE::

   Already stored snapshots in the repository remain unchanged during this
   operation.

Parameters
==========

:repository_name:
  The name of the repository as identifier
