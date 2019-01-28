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

.. rubric:: Table of Contents

.. contents::
   :local:

Breaking Changes
================

Changes
=======


Fixes
=====

- Fixed performance regression on ``UPDATE`` and ``DELETE`` operations.

- Fixed performance regression when inserting data using ``unnest()``.

- Fixed an issue where an ordered query with a specified limit that was much
  larger than the available rows would result in ``OutOfMemoryError`` even
  though the number of available rows could fit in memory.

- Fixed a ``NullPointerException`` that occurs on ``OUTER`` joins which can
  be rewritten to ``INNER`` joins and uses a function as a select item.
