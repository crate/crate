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

- Fixed an issue which caused joins over multiple relations and implicit join
  conditions inside the ``WHERE`` clause to fail.

- The ``Access-Control-Allow-Origin`` header is now correctly served by
  resources in the ``/_blobs`` endpoint if the relevant settings are enabled.

- Fixed decoding of postgres specific array literal constant: unquoted elements
  and single element arrays were not decoded correctly and resulted in an empty
  array.
