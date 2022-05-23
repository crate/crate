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

None


Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Enabled to alter the setting ``blocks.read_only_allow_delete`` on blob tables
  to make it possible to drop read-only blob tables.

- Fixed an issue that could cause queries on ``sys.snapshots`` to get stuck and
  consume a significant amount of resources.

- Fixed an issue with primary key columns that have a ``DEFAULT`` clause. That
  could lead to queries on the primary key column not matching the row.

- Fixed an issue with the logical replication of tables metadata which caused
  to stop if the master node of the subscriber changed.

- Fixed an issue with aliased sub-relation outputs when used inside the outer
  where clause expression, resulting in a planner error. Example:
  ``SELECT * FROM (SELECT id, true AS a FROM t1) WHERE a``

- Fixed an edge case with the initial restore of subscribed tables when the
  restore operation finish almost instantly (e.g. restoring small tables).

- Fixed an issue with table functions parameter binding in ``SELECT`` queries
  without ``FROM`` clause. Example: ``SELECT unnest(?)``.

- Improved error handling when creating a subscription with unknown
  publications. Instead of successfully creating the subscription, an error
  is now presented to the user.

- Fixed an issue with client caching which lead to authentication error when
  creating a subscription with bad credentials and ``pg_tunnel`` followed by
  re-creating it second time with the same name and valid credentials.

- Fixed an issue with ``VARCHAR`` and ``BIT`` columns with a length
  limited used in primary key, generated or default column expression. An
  ``ALTER TABLE`` statement removed the length limit from such columns.

- Fixed an issue resulting in a broken subscription when a subscription is
  dropped and re-created within a short period of time.

