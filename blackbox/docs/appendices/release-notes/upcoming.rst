==================
Unreleased Changes
==================

This file collects *unreleased* changes only.

For release notes, see:

  https://crate.io/docs/reference/en/latest/release_notes/index.html

For developers: changes should be recorded here (using RST syntax) as you are
developing CrateDB. When a new release is being cut, changes will be moved to
the appropriate section of the docs.

Breaking Changes
================

Changes
=======

- Added the full PostgreSQL syntax of the ``BEGIN`` statement and the
  ``COMMIT`` statement.
  This improves the support for clients that are based on the Postgres wire
  protocol, such as the Golang lib/pg and pgx clients. The ``BEGIN`` and
  ``COMMIT`` statements and any of their parameters are simply ignored.

- Added a new scalar function ``ignore3vl`` which eliminates the 3-valued logic
  of null handling for every logical expression beneath it. If 3-valued logic
  is not required, the use of this function in the ``WHERE`` clause beneath a
  ``NOT`` operator can boost the query performance significantly. E.g.::

    SELECT * FROM t
    WHERE NOT IGNORE3VL(5 = ANY(t.int_array_col))

- Changed internals for DELETE and UPDATE queries which should generally result
  in a performance increase for queries which only match a subset of the rows
  and avoid ``CircuitBreakingException`` errors. But it might result in a
  slight performance decrease if the queries match all
  or almost all records.

- Added a new ``Connections`` MBean for JMX which exposes the number of open
  connections per protocol.

- Added a new ``connections`` column to the ``sys.nodes`` table which contains
  the number of currently open connections per protocol and the total number of
  connections per protocol opened over the life-time of a node.

- Added support for ``COPY FROM ... RETURN SUMMARY`` which will return a result
  set with detailed error reporting of imported rows.

- Added a new ``stats.jobs_log_filter`` setting which can be used to control
  what kind of entries are recorded into the ``sys.jobs_log`` table.
  In addition there is a new ``stats.jobs_log_persistent_filter`` setting which
  can be used to record entries also in the regular CrateDB log file.

- Expose statement classification in ``sys.jobs_log`` table.

- Added a ``sys.jobs_metrics`` table which contains query latency information.

- The setting ``es.api.enabled`` has been marked as deprecated and will be
  removed in a future version. Once removed it will no longer be possible to
  use the ES API.
  Please create feature requests if you're using the ES API and cannot use the
  SQL interface as substitute.

- Introduced the ``EXPLAIN ANALYZE`` statement for query profiling.

- Added ``typbasetype`` column to the ``pg_catalog.pg_type`` table.

- Added support for the ``SHOW TRANSACTION_ISOLATION`` statement.

Fixes
=====

- Fixed an issue that could prevent postgres clients from receiving an error and
  therefore getting stuck.

- Fixed an issue that would cause a ``CAST`` from ``TIMESTAMP`` to ``LONG`` to
  be ignored.

- Handle ``STRING_ARRAY`` as argument type for user-defined functions correctly
  to prevent an ``ArrayStoreException``.
