.. highlight:: psql
.. _sql_dql_fulltext_search:

===============
Fulltext search
===============

In order to use fulltext search on one or more columns, a
:ref:`fulltext index with an analyzer <sql_ddl_index_fulltext>` has to be
defined while creating the column: either with ``CREATE TABLE`` or ``ALTER
TABLE ADD COLUMN``. For more information see :ref:`fulltext-indices`.

.. rubric:: Table of contents

.. contents::
   :local:

.. _predicates_match:

``MATCH`` Predicate
===================

Synopsis
--------

::

    MATCH (
         {  column_or_idx_ident | ( column_or_idx_ident [boost]  [, ...] ) }
     , query_term
    )  [ using match_type [ with ( match_parameter [= value] [, ... ] ) ] ]

The MATCH predicate performs a fulltext search on one or more indexed columns
or indices and supports different matching techniques. It can also be used to
perform :ref:`geographical searches <sql_dql_geo_search>` on
:ref:`data-types-geo-shape` indices.

The actual applicability of the MATCH predicate depends on the index's type. In
fact, the availability of certain ``match_types`` and ``match_parameters``
depend on the index. This section however, only covers the usage of the MATCH
predicate on ``fulltext`` indices on ``text`` columns. To use MATCH on
:ref:`data-types-geo-shape` indices, see :ref:`sql_dql_geo_search`.

In order to use fulltext searches on a column, a :ref:`fulltext index with an
analyzer <sql_ddl_index_fulltext>` must be created for this column. See
:ref:`fulltext-indices` for details. There are different types of
:ref:`fulltext-indices` with different goals, however it's not possible to
query multiple index columns with different index types within the same MATCH
predicate.

To get the relevance of a matching row, a specific system column
:ref:`_score <sql_administration_system_column_score>` can be selected. It
contains a numeric score *relative to the other rows*: The higher, the more
relevant the row::

    cr> select name, _score from locations
    ... where match(name_description_ft, 'time') order by _score desc;
    +-----------+------------+
    | name      |     _score |
    +-----------+------------+
    | Bartledan | 0.75782394 |
    | Altair    | 0.63013375 |
    +-----------+------------+
    SELECT 2 rows in set (... sec)

The MATCH predicate in its simplest form performs a fulltext search against a
single column. It takes the ``query_term`` and, if no ``analyzer`` was
provided, analyzes the term with the analyzer configured on
``column_or_idx_ident``. The resulting tokens are then matched against the
index at ``column_or_idx_ident`` and if one of them matches, MATCH returns
``TRUE``.

The MATCH predicate can be also used to perform a fulltext search on multiple
columns with a single ``query_term`` and to add weight to specific columns it's
possible to add a ``boost`` argument to each ``column_or_idx_ident``. Matches
on columns with a higher boost result in a higher :ref:`_score
<sql_administration_system_column_score>` value for that document.

The ``match_type`` argument determines how a single ``query_term`` is applied
and and how the resulting :ref:`_score
<sql_administration_system_column_score>` is computed. For more information see
:ref:`predicates_match_types`.

Results are ordered by :ref:`_score <sql_administration_system_column_score>`
by default, but can be overridden by adding an ``ORDER BY`` clause.

Arguments
.........

:column_or_idx_ident:
  A reference to a column or an index.

  If the column has an implicit index (e.g. created with something like
  ``TEXT column_a INDEX USING FULLTEXT``) this should be the name of the
  column.

  If the column has an explicit index (e.g. created with something like ``INDEX
  "column_a_idx" USING FULLTEXT ("column_a") WITH (...)``) this should be the
  name of the index.

  By default every column is indexed but only the raw data is stored, so
  matching against a ``text`` column without a fulltext index is equivalent to
  using the ``=`` :ref:`operator <gloss-operator>`. To perform real fulltext
  searches use a :ref:`fulltext index <sql_ddl_index_fulltext>`.

:boost:
  A column ident can have a boost attached. That is a weight factor that
  increases the relevance of a column in respect to the other columns.
  The default boost is 1.

:query_term:
  This string is analyzed (using the explicitly given ``analyzer`` or
  the analyzer of the columns to perform the search on) and the
  resulting tokens are compared to the index. The tokens used for search
  are combined using the boolean ``OR`` operator unless stated otherwise
  using the ``operator`` option.

:match_type:
  Optional. Defaults to ``best_fields`` for fulltext indices. For
  details see :ref:`predicates_match_types`.

.. NOTE::

   The ``MATCH`` predicate can only be used in the :ref:`sql_dql_where_clause`
   and on user-created tables. Using the ``MATCH`` predicate on system tables is
   not supported.

   One ``MATCH`` predicate cannot combine columns of both relations of a join.

   Additionally, ``MATCH`` predicates cannot be used on columns of both
   relations of a join if they cannot be logically applied to each of them
   separately. For example:

   This is allowed::

       FROM t1, t2 WHERE match(t1.txt, 'foo') AND match(t2.txt, 'bar');``

   But this is not::

       FROM t1, t2 WHERE match(t1.txt, 'foo') OR match(t2.txt, 'bar');

.. _predicates_match_types:

Match Types
...........

The match type determines how the ``query_term`` is applied and the
:ref:`_score <sql_administration_system_column_score>` is created, thereby
influencing which documents are considered more relevant. The default
``match_type`` for fulltext indices is ``best_fields``.

:best_fields:
  Use the :ref:`_score <sql_administration_system_column_score>` of the
  column that matched best. For example if a column contains all the
  tokens of the ``query_term`` it's considered more relevant than other
  columns containing only one.

  This type is the default, if omitted.

:most_fields:
  This match type takes the :ref:`_score
  <sql_administration_system_column_score>` of every matching column and
  averages their scores.

:cross_fields:
  This match type analyzes the ``query_term`` into tokens and searches
  all tokens in all given columns at once as if they were one big column
  (given they have the same analyzer). All tokens have to be present in
  at least one column, so querying for ``foo bar`` should have the
  tokens ``foo`` in one column and ``bar`` in the same or any other.

:phrase:
  This match type differs from ``best_fields`` in that it constructs a
  phrase query from the ``query_term``. A phrase query will only match
  if the tokens in the columns are *exactly* in the same order as the
  analyzed columns from the ``query_term``. So, querying for ``foo bar``
  (analyzed tokens: ``foo`` and ``bar``) will only match if one of the
  columns contains those two token in that order - without any other
  tokens in between.

:phrase_prefix:
  This match type is roughly the same than ``phrase`` but it allows to
  match by prefix on the last token of the ``query_term``. For example
  if your query for ``foo ba``, one of the columns has to contain
  ``foo`` and a token that starts with ``ba`` in that order. So a column
  containing ``foo baz`` would match and ``foo bar`` too.

Options
```````

The match options further distinguish the way the matching process using a
certain match type works. Not all options are applicable to all match types.
See the options below for details.

:analyzer:
  The analyzer used to convert the ``query_term`` into tokens.

:boost:
  This numeric value is multiplied with the resulting :ref:`_score
  <sql_administration_system_column_score>` of this ``match`` call.

  If this ``match`` call is used with other conditions in the where
  clause a value above ``1.0`` will increase its influence on the overall
  :ref:`_score <sql_administration_system_column_score>` of the whole query, a
  value below ``1.0`` will decrease it.

:cutoff_frequency:
  The token frequency is the number of occurrences of a token in a
  column.

  This option specifies a minimum token frequency that excludes matching tokens
  with a higher frequency from the overall :ref:`_score
  <sql_administration_system_column_score>`. Their :ref:`_score
  <sql_administration_system_column_score>` is only included if another token
  with a lower frequency also matches. This can be used to suppress
  results where only high frequency terms like ``the`` would cause a
  match.

.. _match_option_fuzziness:

:fuzziness:
  Can be used to perform fuzzy full text search.

  On numeric columns use a numeric, on timestamp columns a long
  indicating milliseconds, on strings use a number indicating the
  maximum allowed Levenshtein Edit Distance. Use ``prefix_length``,
  ``fuzzy_rewrite`` and ``max_expansions`` to fine tune the fuzzy
  matching process.

:fuzzy_rewrite:
  The same than ``rewrite`` but only applies to queries using
  ``fuzziness``.

:max_expansions:
  When using ``fuzziness`` or ``phrase_prefix`` this options controls to
  how many different possible tokens a search token will be expanded.
  The ``fuzziness`` controls how big the distance or difference between
  the original token and the set of tokens it is expanded to can be.
  This option controls how big this set can get.

:minimum_should_match:
  The number of tokens from the ``query_term`` to match when ``or`` is
  used. Defaults to ``1``.

:operator:
  Can be ``or`` or ``and``. The default :ref:`operator <gloss-operator>` is
  ``or``. It is used to combine the tokens of the ``query_term``. If ``and`` is
  used, every token from the ``query_term`` has to match. If ``or`` is used
  only the number of ``minimum_should_match`` have to match.

:prefix_length:
  When used with ``fuzziness`` option or with ``phrase_prefix`` this
  options controls how long the common prefix of the tokens that are
  considered as similar (same prefix or fuzziness
  distance/difference)has to be.

:rewrite:
  When using ``phrase_prefix`` the prefix query is constructed using all
  possible terms and rewriting them into another kind of query to
  compute the score. Possible values are ``constant_score_auto``,
  ``constant_score_boolean``, ``constant_score_filter``,
  ``scoring_boolean``,``top_terms_N``, ``top_terms_boost_N``. The
  ``constant_...`` values can be used  together with the ``boost`` option to set
  a constant :ref:`_score <sql_administration_system_column_score>` for rows
  with a matching prefix or fuzzy match.

:slop:
  When matching for phrases this option controls how exact the phrase
  match should be (proximity search). If set to ``0`` (the default), the
  terms must be in the exact order. If two transposed terms should
  match, a minimum ``slop`` of ``2`` has to be set. Only applicable to
  ``phrase`` and ``phrase_prefix`` queries. As an example with ``slop``
  2, querying for ``foo bar`` will not only match ``foo bar`` but also
  ``foo what a bar``.

:tie_breaker:
  When using ``best_fields``, ``phrase`` or ``phrase_prefix`` the :ref:`_score
  <sql_administration_system_column_score>` of every other column will be
  multiplied with this value and added to the :ref:`_score
  <sql_administration_system_column_score>` of the best matching column.

  Defaults to ``0.0``.

  Not applicable to match type ``most_fields`` as this type is executed
  as if it had a ``tie_breaker`` of ``1.0``.

:zero_terms_query:
  If no tokens are generated analyzing the ``query_term`` then no
  documents are matched. If ``all`` is given here, all documents are
  matched.

Usage
=====

A fulltext search is done using the :ref:`predicates_match` predicate::

    cr> select name from locations where match(name_description_ft, 'time') order by _score desc;
    +-----------+
    | name      |
    +-----------+
    | Bartledan |
    | Altair    |
    +-----------+
    SELECT 2 rows in set (... sec)

It returns ``TRUE`` for rows which match the search string. To get more
detailed information about the quality of a match, the relevance of the row,
the :ref:`_score <sql_administration_system_column_score>` can be selected::

    cr> select name, _score
    ... from locations where match(name_description_ft, 'time') order by _score desc;
    +-----------+------------+
    | name      |     _score |
    +-----------+------------+
    | Bartledan | 0.75782394 |
    | Altair    | 0.63013375 |
    +-----------+------------+
    SELECT 2 rows in set (... sec)

.. NOTE::

   The ``_score`` is not an absolute value. It just sets a row in relation to
   the other ones.

Searching On Multiple Columns
=============================

There are two possibilities if a search should span the contents of multiple
columns:

* use a composite index column on your table. See
  :ref:`sql-ddl-composite-index`.

* use the :ref:`predicates_match` predicate on multiple columns.

When querying multiple columns, there are many ways how the relevance a.k.a.
:ref:`_score <sql_administration_system_column_score>` can be computed. These
different techniques are called :ref:`predicates_match_types`.

To increase the relevance of rows where one column matches extremely well, use
``best_fields`` (the default).

If rows with good matches spread over all included columns should be more
relevant, use ``most_fields``. If searching multiple columns as if they were
one, use ``cross_fields``.

For searching of matching phrases (tokens are in the exact same order) use
``phrase`` or ``phrase_prefix``::

    cr> select name, _score from locations
    ... where match(
    ...     (name_description_ft, inhabitants['name'] 1.5, kind 0.75),
    ...     'end of the galaxy'
    ... ) order by _score desc;
    +-------------------+------------+
    | name              |     _score |
    +-------------------+------------+
    | NULL              | 1.5614427  |
    | Altair            | 0.63013375 |
    | Aldebaran         | 0.55650693 |
    | Outer Eastern Rim | 0.38915473 |
    | North West Ripple | 0.37936807 |
    +-------------------+------------+
    SELECT 5 rows in set (... sec)

::

    cr> select name, description, _score from locations
    ... where match(
    ...     (name_description_ft), 'end of the galaxy'
    ... ) using phrase with (analyzer='english', slop=4);
    +------+-------------------------+-----------+
    | name | description             |    _score |
    +------+-------------------------+-----------+
    | NULL | The end of the Galaxy.% | 1.5614427 |
    +------+-------------------------+-----------+
    SELECT 1 row in set (... sec)

A vast amount of options exist to fine-tune your fulltext search. A detailed
reference can be found here :ref:`predicates_match`.

Negative Search
===============

A negative fulltext search can be done using a ``NOT`` clause::

    cr> select name, _score from locations
    ... where not match(name_description_ft, 'time')
    ... order by _score, name asc;
    +------------------------------------+--------+
    | name                               | _score |
    +------------------------------------+--------+
    |                                    |    1.0 |
    | Aldebaran                          |    1.0 |
    | Algol                              |    1.0 |
    | Allosimanius Syneca                |    1.0 |
    | Alpha Centauri                     |    1.0 |
    | Argabuthon                         |    1.0 |
    | Arkintoofle Minor                  |    1.0 |
    | Galactic Sector QQ7 Active J Gamma |    1.0 |
    | North West Ripple                  |    1.0 |
    | Outer Eastern Rim                  |    1.0 |
    | NULL                               |    1.0 |
    +------------------------------------+--------+
    SELECT 11 rows in set (... sec)

Filter By :ref:`_score <sql_administration_system_column_score>`
================================================================

It is possible to filter results by the :ref:`_score
<sql_administration_system_column_score>` column but as its value is a computed
value relative to the highest score of all results and consequently never
absolute or comparable across searches the usefulness outside of sorting is
very limited.

Although possible, filtering by the greater-than-or-equals :ref:`operator
<gloss-operator>` (``>=``)  on the :ref:`_score
<sql_administration_system_column_score>` column would not make much sense and
can lead to unpredictable result sets.

Anyway let's do it here for demonstration purpose::

    cr> select name, _score
    ... from locations where match(name_description_ft, 'time')
    ... and _score >= 0.8 order by _score;
    +-----------+-----------+
    | name      |    _score |
    +-----------+-----------+
    | Altair    | 1.6301337 |
    | Bartledan | 1.757824  |
    +-----------+-----------+
    SELECT 2 rows in set (... sec)

As you might have noticed, the :ref:`_score
<sql_administration_system_column_score>` value has changed for the same query
text and document because it's a ratio relative to all results, and by
filtering on :ref:`_score <sql_administration_system_column_score>`, 'all
results' has already changed.

.. CAUTION::

   As noted above :ref:`_score <sql_administration_system_column_score>` is a
   relative number and not comparable across searches. Filtering is therefore
   greatly discouraged.
