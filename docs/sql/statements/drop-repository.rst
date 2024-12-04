.. highlight:: psql

.. _sql-drop-repository:

===================
``DROP REPOSITORY``
===================

You can use the ``DROP REPOSITORY`` :ref:`statement <gloss-statement>` to
de-register a repository.

.. SEEALSO::

    :ref:`CREATE REPOSITORY <sql-create-repository>`

.. rubric:: Table of contents

.. contents::
   :local:


.. _sql-drop-repo-synopsis:

Synopsis
========

::

    DROP REPOSITORY repository_name;


.. _sql-drop-repo-desc:

Description
===========

When a repository is de-registered, it is no longer available for use.

.. NOTE::

    When you drop a repository, CrateDB deletes the corresponding record from
    :ref:`sys.repositories <sys-repositories>` but does not delete any
    snapshots from the corresponding backend data storage. If you create a new
    repository using the same backend data storage, any existing snapshots will
    become available again.

.. NOTE::

    A repository can only be dropped when not in use, i.e. when there are no 
    snapshots being taken.


.. _sql-drop-repo-params:

Parameters
==========

:repository_name:
  The name of the repository to de-register.
