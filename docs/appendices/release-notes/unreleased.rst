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

- Removed support for the ``CRATE_INCLUDE`` environment variable from the
  ``bin/crate`` start script.
  Configuration of CrateDB should happen via the ``crate.yml``, the
  ``CRATE_HEAP_SIZE`` environment variable and optionally ``CRATE_JAVA_OPTS``.

- Removed support for the ``-d`` and ``-p`` options from the ``bin/crate`` start
  script. It's recommended to run CrateDB either via a container runtime like
  Docker, or via a service manager like ``systemd`` where these options are not
  required.

- Subtraction of timestamps was returning their difference in milliseconds, but
  with result type ``TIMESTAMP`` which was wrong and led to issues with several
  PostgreSQL compliant clients. Instead of just fixing the result type, and
  change it to ``LONG``, the subtraction of timestamps was changed to return an
  ``INTERVAL`` and be compliant with PostgreSQL behaviour.

  Before::

    SELECT '2022-12-05T11:22:33.123456789'::timestamp - '2022-11-21T10:11:22.0012334'::timestamp;
    +-----------------------+
    | 1213871122::timestamp |
    +-----------------------+
    |            1213871122 |
    +-----------------------+


  After::

    SELECT '2022-12-05T11:22:33.123456789'::timestamp - '2022-11-21T10:11:22.0012334'::timestamp;
    +------------------------------+
    | 'PT337H11M11.122S'::interval |
    +------------------------------+
    | 337:11:11.122                |
    +------------------------------+

  To use the previous behaviour, timestamps can simply be cast to longs before
  subtracting them::

    SELECT (ts_end::long - ts_start::long) FROM test

  Alternatively, epoch can be extracted from the result of the subtraction::

    SELECT EXTRACT(epoch FROM ts_end - ts_start) FROM test


Deprecations
============

None


Changes
=======

- Added :ref:`has_database_privilege <scalar-has-database-priv>` scalar function
  which checks whether user (or current user if not specified) has specific
  privilege(s) for the database.

- Added support for :ref:`EXTRACT(field FROM interval) <scalar-extract>`.
  e.g.::

    SELECT EXTRACT(MINUTE FROM INTERVAL '49 hours 127 minutes')


- Added support for :ref:`SUM() <aggregation-sum>` aggregations on
  :ref:`INTERVAL type <type-interval>`. e.g.::

    SELECT SUM(tsEnd - tsStart) FROM test


- Exposed the ``require``, ``include`` and ``exclude`` ``routing.allocation``
  settings per partition within ``information_schema.table_partitions``.

- ``cancel`` messages sent from a client via the PostgreSQL wire protocol are
  now internally forwarded to other nodes to support setups with load-balancers.

- Extended the syntax for ``CREATE VIEW`` to allow parenthesis surrounding the
  query.

- Added ``attributes`` column to :ref:`sys.nodes <sys-nodes>` table to expose
  :ref:`custom node settings <conf-node-attributes>`.

- Added support for ``SCROLL`` and backward movement to cursors. See
  :ref:`DECLARE <sql-declare>` and :ref:`FETCH <sql-fetch>`.

- Added the :ref:`MAX_BY <aggregation-max_by>` and :ref:`MIN_BY
  <aggregation-min_by>` aggregation functions

- Added support for :ref:`bit operators <bit-operators>` on integral and
  ``BIT`` types.

- Added a :ref:`WITH clause <sql-copy-from-with>` option :ref:`SKIP
  <sql-copy-from-skip>` for :ref:`COPY FROM <sql-copy-from>` which allows
  skipping rows from the beginning while copying data.

- Updated to Admin UI 1.24.0, which added Italian translations, and updated some
  dependency packages across the board.

- Added support for dollar quoted strings,
  see :ref:`String Literal <string_literal>` for further details.

- Added a :ref:`datestyle <conf-session-datestyle>` session setting that shows 
  the display format for date and time values. Only the ``ISO`` style is 
  supported. Optionally provided pattern conventions for the order of date 
  parts (Day, Month, Year) are ignored.

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue that allowed users without the related privileges to check
  other users' privileges by calling
  :ref:`has_schema_privilege <scalar-has-schema-priv>` function.
