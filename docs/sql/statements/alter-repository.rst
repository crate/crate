.. highlight:: psql
.. _sql-alter-repository:

====================
``ALTER REPOSITORY``
====================

Alter the settings of an existing repository.

Synopsis
========

::

    ALTER REPOSITORY name
        SET ( parameter = value [, ...] )
        | RESET [parameter | ALL]

Description
===========

``ALTER REPOSITORY SET`` changes properties on an existing repository. The type
of the repository cannot be changed.

``ALTER REPOSITORY RESET`` resets one optional property, a list of them, or all of
them at the same time to their default values. ``RESET`` fails if any of the
properties are required.

``ALTER REPOSITORY`` cannot be executed while a snapshot is being created,
restored, or deleted for the given repository.

Arguments
=========

``name``
--------

The name of the repository properties of which need to be changed.


Clauses
=======

``SET``
-------

Change one or more properties on a repository. Any property supported by the
plugin used for the repository can be changed. They are described in more
detail in :ref:`CREATE REPOSITORY <sql-create-repo-clauses>`.

``RESET``
---------

Reset one or more optional properties, or all of them at once (using ``ALL``),
back to their default configuration values.

Examples
========

Change a single property on an existing file system repository:

::

    cr> ALTER REPOSITORY fs_repo SET (compress = false);
    ALTER 1

Change multiple properties at the same time:

::

    cr> ALTER REPOSITORY s3_repo SET (chunk_size = '64mb', max_restore_bytes_per_sec = '40mb');
    ALTER 1

Reset a specific optional property back to its default value:

::

    cr> ALTER REPOSITORY fs_repo RESET compress;
    ALTER 1

Reset all optional properties of a repository to their defaults:

::

    cr> ALTER REPOSITORY s3_repo RESET ALL;
    ALTER 1
