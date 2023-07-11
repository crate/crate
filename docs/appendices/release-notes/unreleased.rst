
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

- Added columns ``prosupport``, ``prokind``, ``prosqlbody`` and removed columns
  ``protransform``, ``proisagg`` and ``proiswindow`` from ``pg_proc`` table to
  be in sync with PostgreSQL version ``14``.

- Added column ``relrewrite`` and removed columns ``relhasoids`` and
  ``relhaspkey``from ``pg_class`` table to be in sync with PostgreSQL version
  ``14``.

- Added columns ``atthasmissing`` and ``attmissingval`` to ``pg_attribute`` table
  to be in sync with PostgreSQL version ``14``.

- Added column ``conparentid`` and removed column ``consrc`` from
  ``pg_constraint`` table to be in sync with PostgreSQL version ``14``.

- Added column ``indnkeyatts`` to ``pg_index`` table to be in sync with
  PostgreSQL version ``14``.

- Added columns ``typacl``, ``typalign``, ``typanalyze``, ``typdefaultbin``,
  ``typmodin``, ``typmodout``, ``typstorage``, ``typsubscript`` to ``pg_type``
  table to be in sync with PostgreSQL version ``14``.

- Changed ``pg_constraint.conbin`` column type from ``OBJECT`` to ``STRING`` and
  ``pg_proc.proargdefaults`` column type from ``OBJECT[]`` to ``STRING`` to be
  in sync with other similar columns, e.g.: ``pg_index.indexprs``.

- Changed ``pg_attribute.spcacl``, ``pg_class.relacl`` and
  ``pg_namespace.nspacl`` columns type from ``OBJECT[]`` to ``STRING[]`` to be
  in sync with other similar columns, e.g.: ``pg_database.datacl``.

- Raise an exception if duplicate columns are detected on
  :ref:`named index column definition <named-index-column>` instead of
  silently ignoring them.

- Adjusted allowed array index range to be from ``Integer.MIN_VALUE`` to
  ``Integer.MAX_VALUE``. The behavior is now also consistent between subscripts
  on array literals and on columns, and between index literals and index
  expressions. That means something like ``tags[-1]`` will now return ``NULL``
  just like ``ARRAY['AUT', 'GER'][-1]`` or ``ARRAY['AUT', 'GER'][1 - 5]`` did.


Deprecations
============

None


Changes
=======

SQL Statements
--------------

None


SQL Standard and PostgreSQL Compatibility
-----------------------------------------

None


Scalar and Aggregation Functions
--------------------------------

None


Performance and Resilience Improvements
---------------------------------------

None


Data Types
----------

None


Administration and Operations
-----------------------------

None


Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

None
