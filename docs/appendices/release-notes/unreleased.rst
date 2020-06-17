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

Deprecations
============

None

Changes
=======

None

Fixes
=====

- Fixed an issue that prevented an optimization for ``SELECT DISTINCT
  <single_text_column> FROM <table>`` from working if used within a ``INSERT
  INTO`` statement.

- Re-enabled the IAM role authentication for
  :ref:`s3 repositories <ref-create-repository-types-s3>`

- Changed the required privileges to execute ``RESET`` statements to include
  the ``AL`` privilege. Users with ``AL`` could change settings using ``SET
  GLOBAL`` already.

- Fixed an issue that caused a ``NullPointerException`` if the :ref:`ANALYZE
  <analyze>` statement was executed on tables with primitive array type columns
  that contain ``NULL`` values.

- Fixed an issue that caused the ``OFFSET`` clause to be ignored in ``SELECT
  DISTINCT`` queries.

