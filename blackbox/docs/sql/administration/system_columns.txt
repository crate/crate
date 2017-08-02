.. _sql_administration_system_columns:

==============
System Columns
==============

On every table CrateDB implements several implicitly defined system columns.
Their names are reserved and cannot be used as user-defined column names. All
system columns are prefixed with an underscore, consist of lowercase letters
and might contain underscores in between.

.. _sql_administration_system_column_version:

_version
  CrateDB uses an internal versioning for every row, the version number is
  increased on every write. This column can be used for `Optimistic Concurrency
  Control`_, see :ref:`sql_occ` for usage details.

.. _sql_administration_system_column_score:

_score
  This internal system column is available on all documents retrieved by a
  ``SELECT`` query. It is representing the scoring ratio of the document
  related to the used query filter and makes most sense on fulltext searches.

  The scoring ratio is always related to the highest score determined by a
  search, thus scores are not directly comparable across searches.

  If the query does not include a fulltext search the value is 1.0f in most
  cases.

.. _sql_administration_system_column_id:

_id
  ``_id`` is an internal system column that is available on each indexed
  document and can be retrieved by a ``SELECT`` query from doc schema tables.

  The value is a unique identifier for each row in a table and is a compound
  string representation of all primary key values of that row. If no primary
  keys are defined the id is randomly generated. If no dedicated routing column
  is defined the ``_id`` value is used for distributing the records on the
  shards.

.. _Optimistic Concurrency Control: http://en.wikipedia.org/wiki/Optimistic_concurrency_control
