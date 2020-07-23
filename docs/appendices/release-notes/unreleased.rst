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

None


Fixes
=====

- Fixed an issue that prevented queries executed using a ``query-then-fetch``
  strategy from being labelled correctly in the :ref:`sys.jobs_metrics
  <sys-jobs-metrics>` and :ref:`sys.jobs_log <sys-logs>` tables.

- Fixed a regression introduced in 4.2.0 which caused queries including a
  virtual table, and both a ``ORDER BY`` and ``LIMIT`` clause to fail.

- Fixed a regression introduced in 4.2.0 that resulted in ``NULL`` values being
  returned for columns used in the ``PARTITIONED BY`` clause instead of the
  actual values.

- Allow all users to execute ``DISCARD`` and ``SET TRANSACTION`` statement.
  These are session local statements and shouldn't require special privileges.

- Updated the bundled JDK to 14.0.2-12

- Increased the default interval for `stats.service.interval
  <stats.service.interval>` from one hour to 24 hours because invoking it every
  hour caused significant extra load on a cluster.

- Fixed an issue that caused ``SHOW CREATE TABLE`` to print columns of type
  ``VARCHAR(n)`` as ``TEXT``, leading to a loss of the length information when
  using the ``SHOW CREATE TABLE`` statement to re-create a table.

- Fixed an issue that prevented ``ALTER TABLE .. ADD COLUMN`` statements from
  working on tables containing a ``PRIMARY KEY`` column with a ``INDEX OFF``
  definition.
