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

- Added settings ``s3.client.default.access_key`` and
  ``s3.client.default.secret_key`` which can be used to set default credentials
  for s3 repositories, if they are not passed as parameters to the
  ``CREATE REPOSITORY`` SQL statement.

- Implemented a thread-utilization down-scaling logic which dynamically adapts
  the number of threads used for ``SELECT`` queries to avoid running into
  ``RejectedExcecution`` errors if there are many shards per node involved in
  the queries.

- Changed internals for DELETE and UPDATE queries which should generally result
  in a performance increase for queries which only match a subset of the rows
  and avoid ``CircuitBreakingException`` errors. But it might result in a
  slight performance decrease if the queries match all
  or almost all records.

Fixes
=====

- Fixed an issue where the Admin UI was not loaded when it was served from
  another location than ``/`` resulting in a blank browser canvas.

- Made table setting ``blocks.read_only_allow_delete`` configurable for
  partitioned tables.

- Improved performance for expressions involving literal type conversions,
  e.g. ``select count(*) from users group by name having max(bytes) = 4``.

- Fixed an issue which could result in lost entries at the ``sys.jobs_log`` and
  ``sys.operations_log`` tables when the related settings are changed while
  entries are written.

- Fixed a dependecy issue with the bundled ``crash`` that caused the app not
  being able to connect to the server.

- Fixed an issue that could prevent postgres clients from receiving an error and
  therefore getting stuck.

- Fixed an issue that would cause a ``CAST`` from ``TIMESTAMP`` to ``LONG`` to
  be ignored.

- Handle ``STRING_ARRAY`` as argument type for user-defined functions correctly
  to prevent an ``ArrayStoreException``.
