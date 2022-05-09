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

None


Deprecations
============

None


Changes
=======

- Updated the bundled JDK to 18.0.1+10

- Moved the :ref:`scalar-quote_ident` function to `pg_catalog` for improved
  compatibility with PostgreSQL.

- Added the :ref:`concat_ws <scalar-concat-ws>` scalar function which allows
  concatenation with a custom separator.

- Added ``decimal`` type as alias to ``numeric``

- Users with AL privileges can now run ``ANALYZE``

- Added ``typsend`` column to ``pg_catalog.pgtype`` table for improved
  compatibility with PostgreSQL.


Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue with primary key columns that have a ``DEFAULT`` clause. That
  could lead to queries on the primary key column not matching the row.

- Fixed an issue with the logical replication of tables metadata which caused
  to stop if the master node of the subscriber changed.
