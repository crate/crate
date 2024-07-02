.. _sql_administration_system_columns:

==============
System columns
==============

On every user table CrateDB implements several implicitly defined system columns.
Their names are reserved and cannot be used as user-defined column names. All
system columns are prefixed with an underscore, consist of lowercase letters
and might contain underscores in between.

.. _sql_administration_system_column_version:

``_version``
  CrateDB uses an internal versioning for every row, the version number is
  increased on every write.

.. NOTE::

   Using the ``_version`` column for `Optimistic Concurrency Control`_ has been
   deprecated in favour of using the :ref:`_seq_no
   <sql_administration_system_columns_seq_no>` and :ref:`_primary_term
   <sql_administration_system_columns_primary_term>`.
   See :ref:`sql_occ` for usage details.

.. _sql_administration_system_columns_seq_no:

``_seq_no``
  The CrateDB primary shards will increment a sequence number for every insert,
  update and delete operation executed against a row. The current sequence
  number of a row is exposed under this column. This column can be used in
  conjunction with the :ref:`_primary_term
  <sql_administration_system_columns_primary_term>` column for
  `Optimistic Concurrency Control`_, see :ref:`sql_occ` for usage details.

.. _sql_administration_system_columns_primary_term:

``_primary_term``
  The sequence numbers give us an order of operations that happen at a primary
  shard, but they don't help us distinguish between old and new primaries. For
  example, if a primary is isolated in a minority partition, a possible up to
  date replica shard on the majority partition will be promoted to be the new
  primary shard and continue to process write operations, subject to the
  :ref:`sql-create-table-write-wait-for-active-shards` setting. When this
  partition heals we need a reliable way to know that the operations that come
  from the other shard are from an old primary and, equally, the operations
  that we send to the shard re-joining the cluster are from the newer primary.
  The cluster needs to have a consensus on which shards are the current serving
  primaries. In order to achieve this we use the primary terms which are
  generational counters that are incremented when a primary is promoted. Used
  in conjunction with :ref:`_seq_no <sql_administration_system_columns_seq_no>`
  we can obtain a total order of operations across shards and `Optimistic
  Concurrency Control`_.

.. _sql_administration_system_column_score:

``_score``
  This internal system column is available on all documents retrieved by a
  ``SELECT`` query. It represents the score of the document as returned by
  the query filter, and makes most sense for full text queries, giving an
  indication of how well the query expression matches the document.
  Documents with higher scores match the query more closely, allowing the
  score to be used for ranking.

  The score is calculated using Lucene's `BM25Similarity implementation`_,
  based on the widely-used `Okapi BM25`_ algorithm.  This uses a combination
  of partition-level term statistics and the frequencies of particular terms
  within a matching row.  Scores produced by different queries are not
  directly comparable.  Note that, because term statistics are calculated
  per-partition, identical rows in different partitions may produce slightly
  different scores for the same query expression.

  If the query does not include a fulltext search the value is 1.0f in most
  cases.

.. _BM25Similarity implementation: https://lucene.apache.org/core/9_9_0/core/org/apache/lucene/search/similarities/BM25Similarity.html
.. _Okapi BM25: https://en.wikipedia.org/wiki/Okapi_BM25

.. _sql_administration_system_column_id:

``_id``
  ``_id`` is an internal system column that is available on each indexed
  document and can be retrieved by a ``SELECT`` query from doc schema tables.

  The value is a unique identifier for each row in a table and is a compound
  string representation of all primary key values of that row. If no primary
  keys are defined the id is randomly generated. If no dedicated :ref:`routing
  column <gloss-routing-column>` is defined the ``_id`` value is used for
  distributing the records on the shards.

.. _Optimistic Concurrency Control: https://en.wikipedia.org/wiki/Optimistic_concurrency_control


``_docid``
  ``_docid`` exposes the internal id a document has within a Lucene segment.
  Although the id is unique within a segment, it is not unique across segments
  or shards and can change the value in case segments are merged.
