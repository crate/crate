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

- Removed support for HDFS snapshot repositories. We suspect nobody uses it
  anymore. If you require HDFS support please reach out to us, if there is
  enough interest we may be able to provide a plugin with the functionality.


Deprecations
============

None


Changes
=======

- Added an optimization to push down constant join conditions to the relation
  in an inner join, which results in a more efficient execution plan.

- Added support for :ref:`UNION DISTINCT or UNION <sql-union>` statement to be
  able to retrieve unique rows from multiple relations without using
  sub-queries with extra ``GROUP BY`` clauses.

- Implemented cancelling requests section of PostgreSQL wire protocol.

- Added the :ref:`Logical Replication <administration-logical-replication>`
  feature allowing to replicate data across multiple clusters.

- Added the :ref:`array_position <scalar-array_position>` function which 
  returns the position of the first occurrence of the provided value in an 
  array. A starting position can be optionally provided.

- Optimized the casting from string to arrays by avoiding an unnecessary string
  to byte conversion.

- Write blocks added due to low disk space are now automatically removed if a
  node again drops below the high watermark.

- Added a ``WITH`` clause parameter, ``validation`` to ``COPY FROM`` which
  can enable or disable the newly added type validation feature. Please see
  :ref:`validation <sql-copy-from-validation>` for more details.

- Added type validation logic to ``COPY FROM``. Now raw data will be parsed and
  validated against the target table schema and casted if possible utilizing
  :ref:`type casting <data-types-casting>`.

- Improved the evaluation performance of implicit casts by utilize the compile
  step of the function to determine the return type.

- Added a ``flush_stats`` column to the :ref:`sys.shards <sys-shards>` table.

- Allowed users to be able to specify different S3 compatible storage endpoints
  to ``COPY FROM/TO`` statements by embedding the host and port to the ``URI``
  parameter and also a ``WITH`` clause parameter ``protocol`` to choose between
  ``HTTP`` or ``HTTPS``.

- Added the option to import CSV files without field headers using the ``COPY
  FROM`` statement.

- Added the option to import only a subset of columns using ``COPY FROM`` when
  import CSV files with headers.

- Added the option to run ``COPY FROM`` and ``COPY TO`` operations in the
  background without waiting for them to complete.

- Updated to Admin UI 1.21.0, which improves console highlighting by adding
  various keywords and data types.

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Updated the bundled JDK to 17.0.3+7

- Fixed an issue with the handling of quoted identifiers in column names where
  certain characters break the processing. This makes sure any special characters
  can be used as column name.

- Fixed an issue with the handling of intervals in generated columns. The table
  creation failed when an interval is included in a function call as part of a
  generated column.

- Fixed a race condition that could cause a ``blocked by: [FORBIDDEN/4/Table or
  partition preparing to close`` error when inserting into a partitioned table
  where a single partition got closed.

- Fixed an issue that caused an ``Relation unknown`` error while trying to
  close an empty partitioned table using ``ALTER TABLE ... CLOSE``.

- Fixed an issue that caused ``COPY FROM RETURN SUMMARY`` fail non-gracefully
  in case of import from CSV containing invalid line(s).

- Bumped JNA library to version 5.10.0. This will make CrateDB start without
  JNA library warnings on M1 chip based MacOS systems.

- Updated to Admin UI 1.20.2, which fixes duplicate entries in query history.

- Fixed an issue that threw ``SQLParseException`` when a ``ILIKE`` operand
  contained '{' or '}'.

- Fixed an issue that caused ``ALTER TABLE ADD COLUMN`` to lose an optional
  ``routing_column`` information provided at table creation.
