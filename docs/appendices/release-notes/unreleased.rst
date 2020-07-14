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

- Fixed an issue with the :ref:`quote_ident <scalar-quote-ident>` scalar
  function that caused it to quote subscript expressions like ``"col['x']"``
  instead of ``"col"['x']``.

- Fixed an issue that prevented the use of subscript expressions as conflict
  target in ``ON CONFLICT`` clauses of ``INSERT`` statements.

- Fixed an issue where :ref:`drop snapshot <ref-drop-snapshot>` on an
  :ref:`azure repository <ref-create-repository-types-azure>` would not delete
  all the related data.

- Fixed an issue that could lead to a ``Field is not streamable`` error message
  when using window functions.
