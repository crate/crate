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

- Changed the behavior of ``SHOW search_path`` to omit the implicit
  ``pg_catalog`` schema, unless the user set it explicitly. This matches the
  PostgreSQL behavior.

- Optimized the evaluation of ``CASE`` expressions to prevent stack overflows
  for very large expressions.

- Allowed schema and table names to contain upper case letters. This can be
  achieved by quoting the names. Unquoted names with upper case letters are
  converted to lower cases which has been the existing behaviour.

- Allowed schema and table names to start with ``_``.

- Improved the performance of queries using a correlated sub-query inside the
  ``WHERE`` clause in conjunction with a non-correlated filter clause.

- Added the :ref:`col_description(integer, integer) <scalar-col_description>` scalar
  function for improved PostgreSQL compatibility. CrateDB does not support
  comments for columns, so this function always returns ``NULL``.

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed a race condition that could lead to a ``ShardNotFoundException`` when
  executing ``UPDATE`` statements.
