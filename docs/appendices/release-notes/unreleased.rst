
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

None


Deprecations
============

None


Changes
=======

None

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Improved error message to be user-friendly, for definition of
  :ref:`CHECK <check_constraint>` at column level for object sub-columns,
  instead of a ``ConversionException``.

- Added validation to prevent creation of invalid nested array columns via
  ``INSERT INTO`` and dynamic column policy.

- Fixed parsing of ``ARRAY`` literals in PostgreSQL ``simple`` query mode.

- Fixed value of ``sys.jobs_log.stmt`` for various statements when issued via
  the PostgreSQL ``simple`` query mode by using the original query string
  instead of the statements string representation.

- Fixed an issue that could cause errors for queries with aggregations,
  ``UNION`` and ``LIMIT``, e.g. ::

    SELECT a, avg(c), b FROM t1 GROUP BY 1, 3
    UNION
    SELECT x, avg(z), y FROM t2 GROUP BY 1, 3
    UNION
    SELECT i, avg(k), j FROM t3 GROUP BY 1, 3
    LIMIT 10

- Fixed an issue which prevented ``INSERT INTO ... SELECT ...`` from inserting
  any records if the target table had a partitioned column of a non-string
  type, used in any expressions of ``GENERATED`` or ``CHECK`` definitions.

- Fixed an issue which caused ``INSERT INTO ... SELECT ...`` statements to
  skip ``NULL`` checks of ``CLUSTERED BY`` column values.

