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

- Arithmetic operations now work on expressions of type :ref:`timestamp without
  time zone <date-time-types>`, to make it consistent with ``timestamp with time
  zone``.

- Fixed a possible NPE which could occur when querying the ``sys.allocations``
  table.

- Fixed the tables compatibility check to correctly indicate when tables need
  to be recreated in preparation for a CrateDB upgrade towards the next major
  version of CrateDB.

- Fixed an issue that led to DEFAULT constraints of inner columns of object
  columns to be ignored.

- The values provided in INSERT or UPDATE statements for object columns which
  contain generated expressions are now validated. The computed expression must
  match the provided value. This makes the behavior consistent with how top
  level columns of a table are treated.

- Fixed support for ordering by literal constants.
  Example: ``SELECT 1, * FROM t ORDER BY 1"``
