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

- Fixed an issue that caused ``COPY FROM`` from a ``HTTPS`` URL to fail with
  ``No X509TrustManager implementation available`` if CrateDB is configured to
  use ``SSL``.

- Fixed an issue that would lead to incorrect result when selecting the
  :ref:`cluster license <sys-cluster-license>` object column, namely, the
  fields of the object would contain null values, even though the license
  was set.

- Fixed an issue that caused a ``OFFSET`` as part of a ``UNION`` to be applied
  incorrectly.

- Fixed an issue that could lead to incorrect ordering of a result sets if
  using ``ORDER BY`` on a column of type ``IP`` or on a scalar function.

- Fixed an issue that caused a ``NullPointerException`` if using ``COPY TO``
  with a ``WHERE`` clause with filters on primary key columns.
