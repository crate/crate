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

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue that caused ``IndexOutOfBoundsException`` to be thrown when
  trying to :ref:`FETCH <sql-fetch>` backwards from a ``CURSOR``, past the 1st
  row.

- Fixed an issue that caused wrong rows to be returned when scrolling backwards
  and then forwards through a :ref:`CURSOR <sql-fetch>`.

- Improved error message when :ref:`fetching <sql-fetch>` using ``ABSOLUTE``,
  past the last row returned by the cursor query.
