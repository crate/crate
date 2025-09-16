.. highlight:: psql
.. _ref-drop-snapshot:

=================
``DROP SNAPSHOT``
=================

Delete an existing snapshot and all files referenced only by this snapshot.


Synopsis
========

::

    DROP SNAPSHOT repository_name.snapshot_name

Description
===========

Delete a snapshot from a repository and all files only referenced by this
snapshot.

If this statement is executed against a snapshot that is currently being
created, the creation is aborted and all files created so far are deleted.

Parameters
==========

:repository_name:
  The name of the repository the snapshot is stored in as ident.

:snapshot_name:
  The name of the snapshot to drop as ident.
