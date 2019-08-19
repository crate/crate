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

Changes
=======

None

Fixes
=====

- Fixed a regression introduced in 4.0 that broke the ``MATCH`` predicate if
  used on aliased relations.

- Improved error handling if an argument of a window function is not used as a
  grouping symbol.

- Fixed an ``OUTER JOIN`` issue resulting in an ``ArrayOutOfBoundException``
  if the gap between matching rows of the tables was growing to big numbers.

- Fixed serialization issue that might occur in distributed queries that
  contain window function calls with the partition by clause in the select
  list.

- Fixed a race condition which could result in a ``AlreadyClosedException``
  when querying the ``sys.shards`` table.
