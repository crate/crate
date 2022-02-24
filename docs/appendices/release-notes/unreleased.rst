==================
Unreleased Changes
==================

.. NOTE::

    These changes have not yet been released.

    If you are viewing this document on the Crate.io website, these changes
    reflect what exists on `the master branch`_ in Git. This is where we
    collect changes before they are ready for release.

.. WARNING::

    Unreleased changes may not be ready for general use and could lead to data
    corruption or data loss. You should `back up your data`_ before
    experimenting with unreleased changes.

.. _the master branch: https://github.com/crate/crate
.. _back up your data: https://crate.io/docs/crate/reference/en/latest/admin/snapshots.html

.. DEVELOPER README
.. ================

.. Changes should be recorded here as you are developing CrateDB. When a new
.. release is being cut, changes will be moved to the appropriate release notes
.. file.

.. When resetting this file during a release, leave the headers in place, but
.. add a single paragraph to each section with the word "None".

.. Always cluster items into bigger topics. Link to the documentation whenever feasible.
.. Remember to give the right level of information: Users should understand
.. the impact of the change without going into the depth of tech.

.. rubric:: Table of contents

.. contents::
   :local:


Breaking Changes
================

- Removed support for HDFS snapshot repositories. We suspect nobody uses it
  anymore. If you require HDFS support please reach out to us, if there is
  enough interest we may be able to provide a plugin with the functionality.


Deprecations
============

None


Changes
=======

- Allowed users to be able to specify different S3 compatible storage endpoints
  to ``COPY FROM/TO`` statements by embedding the host and port to the ``URI``
  parameter and also a ``WITH`` clause parameter ``protocol`` to choose between
  ``HTTP`` or ``HTTPS``.


Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue that could lead to errors when reading translog files from
  CrateDB versions < 4.0.

- Fixed an issue that could lead to an ``Couldn't create execution plan`` error
  when using a join condition referencing multiple other relations.

- Fixed an issue that caused an NPE when executing ``COPY FROM`` with a globbed
  URI having a parent directory that does not exist in the filesystem. This
  affected copying from filesystems local to CrateDB nodes only.

- Fixed an issue that led to an ``Invalid query used in CREATE VIEW`` error if
  using a scalar subquery within the query part of a ``CREATE VIEW`` statement.

- Fixed an issue that caused a failure when a window function over a partition
  is not used in an upper query. For example:
  ``select x from (select x, ROW_NUMBER() OVER (PARTITION BY y) from t) t1``

- Fixed an incorrect optimization of comparison function arguments for explicit
  cast arguments resulting in a wrong result set. Example:
  ```WHERE strCol::bigint > 3```

- Updated the bundled JDK to 17.0.2+8
