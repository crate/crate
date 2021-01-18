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

- Fixed an issue that could lead to a ``String index out of range`` error when
  streaming values of type ``TIMESTAMP WITH TIME ZONE`` using a PostgreSQL
  client.

- Fixed an issue that could lead to errors like ``Can't map PGType with oid=26
  to Crate type`` using a PostgreSQL client.

- Fixed an issue that could result in a ``The assembled list of
  ParameterSymbols is invalid. Missing parameters.`` error if using the
  ``MATCH`` predicate and parameter placeholders within a query.

- Bumped JNA library to version 5.6.0. This will make CrateDB start flawlessly
  and without warnings on recent versions of Windows.
