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

- Removed the deprecated azure discovery functionality.

- Fields referencing ``catalog`` in :ref:`information_schema <information_schema>`
  tables now return ``'crate'`` (the only catalog in CrateDB) instead of the
  table ``schema``.

Deprecations
============

- Deprecated the ``upgrade_segments`` option of the ``OPTIMIZE TABLE``
  statement. The option will now longer have any effect and will be removed in
  the future.


Changes
=======

- Added a ``application_name`` session setting that can be used to identify
  clients or applications which connect to a CrateDB node.

- Added support for ``catalog`` in fully qualified table and column names,
  i.e.::

    SELECT * FROM crate.doc.t1;
    SELECT crate.doc.t1.a, crate.doc.t1.b FROM crate.doc.t1;

- Added support of ``GROUP BY`` on ``ARRAY`` typed columns.


Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fix an issue, causing ``IndexOutOfBoundsException`` to be thrown when using
  ``LEFT``/``RIGHT`` or ``FULL`` ``OUTER JOIN`` and one of the tables (or
  sub-selects) joined has 0 rows.

- Updated the bundled JDK from 18.0.1+10 to 18.0.2+9.

- Fixed a race condition that could cause a ``INSERT INTO`` operation to get
  stuck.

- Fixed an issue that could cause queries with ``objectColumn = ?`` expressions
  to fail if the object contains inner arrays.

- Fixed a ``NullPointerException`` when using a ``IS NULL`` expression on an
  object column that just had a child column added.

- Fixed an issue that caused ``array_upper`` and ``array_lower`` scalar
  functions return wrong results on multidimensional arrays.
