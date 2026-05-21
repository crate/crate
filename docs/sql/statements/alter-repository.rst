.. highlight:: psql
.. _sql-alter-repository:

====================
``ALTER REPOSITORY``
====================

Alter the settings of an existing repository.

Synopsis
========

::

    ALTER REPOSITORY name SET ( parameter = value [, ...] )

Description
===========

``ALTER REPOSITORY SET`` changes properties on an existing repository. The type
of the repository cannot be changed.

``ALTER REPOSITORY`` cannot be executed while a snapshot is being created,
restored, or deleted for the given repository.

Arguments
=========

``name``
--------

The name of the repository properties of which need to be changed.


Clauses
=========

``SET``
-------

Change one or more properties on a repository. Any property supported by the
plugin used for the repository can be changed. They are described in more
details in :ref:`CREATE REPOSITORY <sql-create-repo-clauses>`.

Example
=======

::

    cr> ALTER REPOSITORY fs_repo SET (compress = false);
    ALTER 1
