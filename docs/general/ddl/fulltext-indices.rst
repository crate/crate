.. _fulltext-indices:

.. _indices_and_fulltext:

================
Fulltext indices
================

Fulltext indices take the contents of one or more fields and split it up into
tokens that are used for fulltext-search. The transformation from a text to
separate tokens is done by an analyzer. In order to create fulltext search
queries a :ref:`fulltext index with an analyzer <sql_ddl_index_fulltext>` must
be defined for the related columns.

.. rubric:: Table of contents

.. contents::
   :local:

.. _sql_ddl_index_definition:

Index definition
================

In CrateDB, every column's data is indexed using the ``plain`` index method by
default. Currently there are three choices related to index definition:

- `Disable indexing`_

- `Plain index (Default)`_

- `Fulltext index with analyzer`_

.. WARNING::

   Creating an index after a table was already created is currently not
   supported, so think carefully while designing your table definition.

.. _sql_ddl_index_off:

Disable indexing
================

Indexing can be turned off by using the ``INDEX OFF`` column definition.
Consider that a column without an index can only be used as a result column
and will never produce a hit when queried.

::

    cr> create table table_a (
    ...   first_column text INDEX OFF
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> insert into table_a (first_column) values ('hello');
    INSERT OK, 1 row affected (... sec)

.. Hidden: Refresh::

    cr> refresh table table_a;
    REFRESH OK, ...

When a not indexed column is queried the query will return an error.

::

    cr> select * from table_a where first_column = 'hello';
    SQLParseException[Cannot search on field [first_column] since it is not indexed.]


.. NOTE::

    ``INDEX OFF`` cannot be used with
    :ref:`partition columns <gloss-partition-column>`, as those are not stored
    as normal columns of a table.

.. _sql_ddl_index_plain:

Plain index (default)
=====================

An index of type ``plain`` is indexing the input data as-is without analyzing.
Using the ``plain`` index method is the default behaviour but can also be
declared explicitly::

    cr> create table table_b1 (
    ...   first_column text INDEX using plain
    ... );
    CREATE OK, 1 row affected (... sec)

This results in the same behaviour than without any index declaration::

    cr> create table table_b2 (
    ...   first_column text
    ... );
    CREATE OK, 1 row affected (... sec)

.. _sql_ddl_index_fulltext:

Fulltext index with analyzer
----------------------------

By defining an index on a column, it's analyzed data is indexed instead of the
raw data.  Thus, depending on the used analyzer, querying for the exact data
may not work anymore.  See :ref:`builtin-analyzer` for details about available
builtin analyzer or :ref:`sql-ddl-custom-analyzer`.

If no analyzer is specified when using a fulltext index, the
:ref:`standard <standard-analyzer>` analyzer is used::

    cr> create table table_c (
    ...   first_column text INDEX using fulltext
    ... );
    CREATE OK, 1 row affected (... sec)

Defining the usage of a concrete analyzer is straight forward by defining the
analyzer as a parameter using the ``WITH`` statement::

    cr> create table table_d (
    ...   first_column text INDEX using fulltext with (analyzer = 'english')
    ... );
    CREATE OK, 1 row affected (... sec)

.. _named-index-column:

Defining a named index column definition
----------------------------------------

It's also possible to define an index column which treat the data of a given
column as input. This is especially useful if you want to search for both, the
exact and analyzed data::

    cr> create table table_e (
    ...   first_column text,
    ...   INDEX first_column_ft using fulltext (first_column)
    ... );
    CREATE OK, 1 row affected (... sec)

Of course defining a custom analyzer is possible here too::

    cr> create table table_f (
    ...   first_column text,
    ...   INDEX first_column_ft
    ...     using fulltext(first_column) with (analyzer = 'english')
    ... );
    CREATE OK, 1 row affected (... sec)

.. _sql-ddl-composite-index:

Defining a composite index
--------------------------

Defining a composite (or combined) index is done using the same syntax as above
despite multiple columns are given to the ``fulltext`` index method::

    cr> create table documents_a (
    ...   title text,
    ...   body text,
    ...   INDEX title_body_ft
    ...     using fulltext(title, body) with (analyzer = 'english')
    ... );
    CREATE OK, 1 row affected (... sec)

Composite indices can include nested columns within object columns as well::

    cr> create table documents_b (
    ...   title text,
    ...   author object(dynamic) as (
    ...     name text,
    ...     birthday timestamp with time zone
    ...   ),
    ...   INDEX author_title_ft using fulltext(title, author['name'])
    ... );
    CREATE OK, 1 row affected (... sec)

.. NOTE::

    If ``plain`` index method is used, this internally translates to
    ``fulltext with (analyzer = 'keyword')``.

.. _sql-ddl-custom-analyzer:

.. _create_custom_analyzer:

Creating a custom analyzer
==========================

An analyzer consists of one tokenizer, zero or more token-filters, and zero or
more char-filters.

When a field-content is analyzed to become a stream of tokens, the char-filter
is applied at first. It is used to filter some special chars from the stream of
characters that make up the content.

Tokenizers split the possibly filtered stream of characters into tokens.

Token-filters can add tokens, delete tokens or transform them to finally
produce the desired stream of tokens.

With these elements in place, analyzers provide fine grained control over
building a token stream used for fulltext search. For example you can use
language specific analyzers, tokenizers and token-filters to get proper search
results for data provided in a certain language.

Here is a simple Example::

    cr> CREATE ANALYZER myanalyzer (
    ...   TOKENIZER whitespace,
    ...   TOKEN_FILTERS (
    ...     lowercase,
    ...     kstem
    ...   ),
    ...   CHAR_FILTERS (
    ...     html_strip
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

.. hide: Test table creation with custom analyzer::

    cr> create table hidden_test_table (
    ...     fc text index using fulltext with (analyzer = 'myanalyzer')
    ... );
    CREATE OK...

    cr> drop table hidden_test_table;
    DROP OK, 1 row affected  (... sec)

This creates a custom analyzer called ``myanalyzer``. It uses the built-in
:ref:`whitespace-tokenizer` tokenizer and two built-in token filters.
:ref:`lowercase-tokenfilter` and :ref:`kstem-tokenfilter`, as well as a
:ref:`mapping-charfilter` char-filter.
:
It is possible to further customize the built-in token filters, char-filters or
tokenizers:

.. code-block:: sql

    cr> create ANALYZER myanalyzer_customized (
    ...   TOKENIZER whitespace,
    ...   TOKEN_FILTERS (
    ...     lowercase,
    ...     kstem
    ...   ),
    ...   CHAR_FILTERS (
    ...     mymapping WITH (
    ...       type='mapping',
    ...       mappings = ['ph=>f', 'qu=>q', 'foo=>bar']
    ...     )
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

This example creates another analyzer. This time called
``myanalyzer_customized``. It uses the same tokenizer and token filters as in
the previous example, but specifies custom options to the
:ref:`mapping-charfilter` char-filter.
:
The name (``mymapping``) is a custom name which may not conflict with built-in
char-filters or other custom char-filters.

The provided ``type`` property is **required** as it specifies which built-in
char-filter should be customized. The other option ``mappings`` is specific to
the used type/char-filter.

Tokenizer and token-filters can be customized in the same way.

.. NOTE::

    Altering analyzers is not supported yet.

.. SEEALSO::

  :ref:`ref-create-analyzer` for the syntax reference.

  :ref:`builtin-tokenizer` for a list of built-in tokenizer.

  :ref:`builtin-token-filter` for a list of built-in token-filter.

  :ref:`builtin-char-filter` for a list of built-in char-filter.

Extending a built-in analyzer
=============================

Existing Analyzers can be used to create custom Analyzers by means of extending
them.

You can extend and parameterize :ref:`builtin-analyzer` like this::

    cr> create ANALYZER "german_snowball" extends snowball WITH (
    ...   language = 'german'
    ... );
    CREATE OK, 1 row affected (... sec)

If you extend :ref:`builtin-analyzer`, tokenizer, char-filter or token-filter
cannot be defined.  In this case use the parameters available for the extended
:ref:`builtin-analyzer`.

If you extend custom-analyzers, every part of the analyzer that is omitted will
be taken from the extended one. Example::

    cr> create ANALYZER e2 EXTENDS myanalyzer (
    ...     TOKENIZER mypattern WITH (
    ...       type = 'pattern',
    ...       pattern = '.*'
    ...     )
    ... );
    CREATE OK, 1 row affected (... sec)

This analyzer will use the char-filters and token-filters from ``myanalyzer``
and will override the tokenizer with ``mypattern``.

.. SEEALSO::

   See the reference documentation of the :ref:`builtin-analyzer` to get
   detailed information on the available analyzers.


.. hide: Drop created tables and custom analyzers::

    cr> drop ANALYZER myanalyzer;
    DROP OK, 1 row affected (... sec)
    cr> drop ANALYZER myanalyzer_customized;
    DROP OK, 1 row affected (... sec)
    cr> drop ANALYZER german_snowball;
    DROP OK, 1 row affected (... sec)
    cr> drop ANALYZER e2;
    DROP OK, 1 row affected (... sec)
    cr> drop TABLE table_a;
    DROP OK, 1 row affected (... sec)
    cr> drop TABLE table_b1;
    DROP OK, 1 row affected (... sec)
    cr> drop TABLE table_b2;
    DROP OK, 1 row affected (... sec)
    cr> drop TABLE table_c;
    DROP OK, 1 row affected (... sec)
    cr> drop TABLE table_d;
    DROP OK, 1 row affected (... sec)
    cr> drop TABLE table_e;
    DROP OK, 1 row affected (... sec)
    cr> drop TABLE table_f;
    DROP OK, 1 row affected (... sec)
    cr> drop TABLE documents_a;
    DROP OK, 1 row affected (... sec)
    cr> drop TABLE documents_b;
    DROP OK, 1 row affected (... sec)
