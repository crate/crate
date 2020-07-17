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
.. _back up your data: https://crate.io/a/backing-up-and-restoring-crate/

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


Changes
=======

- Changed the privileges for ``KILL``, all users are now allowed to kill their
  own statements.

- Added the `pg_catalog.pg_roles table <postgres_pg_catalog>`


Fixes
=====

- Increased the default interval for `stats.service.interval
  <stats.service.interval>` from one hour to 24 hours because invoking it every
  hour caused significant extra load on a cluster.

- Updated the bundled JDK to 14.0.2-12

- Fixed an issue that caused ``SHOW CREATE TABLE`` to print columns of type
  ``VARCHAR(n)`` as ``TEXT``, leading to a loss of the length information when
  using the ``SHOW CREATE TABLE`` statement to re-create a table.

- Fixed an issue that prevented ``ALTER TABLE .. ADD COLUMN`` statements from
  working on tables containing a ``PRIMARY KEY`` column with a ``INDEX OFF``
  definition.
