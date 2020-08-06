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

- Added support for flag ``g`` to function
  :ref:`regexp_matches <table-functions-regexp-matches>` and changed
  its type from ``scalar`` to ``table`` type. It now returns a table where each
  row contains a single column ``groups`` of type ``array(text)``.


Changes
=======

- Changed :ref:`OPTIMIZE <sql_ref_optimize>` to no longer implicitly refresh a
  table.

- Added table function :ref:`generate_subscripts <table-functions-generate-subscripts>`

- Changed the privileges for ``KILL``, all users are now allowed to kill their
  own statements.

- Added the `pg_catalog.pg_roles table <postgres_pg_catalog>`


Fixes
=====

- Fixed a regression introduced in 4.2.0 that caused ``sum`` and ``avg``
  global aggregates to return incorrect results when used on columns of
  ``real`` or ``double precision`` data types.

- Fixed an issue that caused primary key lookups to return an empty result
  instead of the row identified by the primary key values, if the primary key
  consists of multiple columns and if one of them is of type ``BOOLEAN``.
