.. _fulltext-indices:

.. _indices_and_fulltext:

================
Fulltext Indices
================

Fulltext indices take the contents of one or more fields and split it up into
tokens that are used for fulltext-search. The transformation from a text to
separate tokens is done by an analyzer. In order to create fulltext search
queries a :ref:`fulltext index with an analyzer <sql_ddl_index_fulltext>` must
be defined for the related columns.

.. rubric:: Table of Contents

.. contents::
   :local:

.. _sql_ddl_index_definition:

Index Definition
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

    cr> create table my_table1b (
    ...   first_column string INDEX OFF
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> insert into my_table1b (first_column) values ('hello');
    INSERT OK, 1 row affected (... sec)

.. Hidden: Refresh::

    cr> refresh table my_table1b;
    REFRESH OK, ...

When a not indexed column is queried the query will return an error.

::

    cr> select * from my_table1b where first_column = 'hello';
    SQLActionException[UnhandledServerException: java.lang.IllegalArgumentException: Cannot search on field [first_column] since it is not indexed.]

.. _sql_ddl_index_plain:

Plain index (Default)
=====================

An index of type ``plain`` is indexing the input data as-is without analyzing.
Using the ``plain`` index method is the default behaviour but can also be
declared explicitly::

    cr> create table my_table1b1 (
    ...   first_column string INDEX using plain
    ... );
    CREATE OK, 1 row affected (... sec)

This results in the same behaviour than without any index declaration::

    cr> create table my_table1b2 (
    ...   first_column string
    ... );
    CREATE OK, 1 row affected (... sec)

.. _sql_ddl_index_fulltext:

Fulltext Index With Analyzer
----------------------------

By defining an index on a column, it's analyzed data is indexed instead of the
raw data.  Thus, depending on the used analyzer, querying for the exact data
may not work anymore.  See :ref:`builtin-analyzer` for details about available
builtin analyzer or :ref:`sql-ddl-custom-analyzer`.

If no analyzer is specified when using a fulltext index, the
:ref:`standard <standard-analyzer>` analyzer is used::

    cr> create table my_table1c (
    ...   first_column string INDEX using fulltext
    ... );
    CREATE OK, 1 row affected (... sec)

Defining the usage of a concrete analyzer is straight forward by defining the
analyzer as a parameter using the ``WITH`` statement::

    cr> create table my_table1d (
    ...   first_column string INDEX using fulltext with (analyzer = 'english')
    ... );
    CREATE OK, 1 row affected (... sec)

Defining a Named Index Column Definition
----------------------------------------

It's also possible to define an index column which treat the data of a given
column as input. This is especially useful if you want to search for both, the
exact and analyzed data::

    cr> create table my_table1e (
    ...   first_column string,
    ...   INDEX first_column_ft using fulltext (first_column)
    ... );
    CREATE OK, 1 row affected (... sec)

Of course defining a custom analyzer is possible here too::

    cr> create table my_table1f (
    ...   first_column string,
    ...   INDEX first_column_ft
    ...     using fulltext(first_column) with (analyzer = 'english')
    ... );
    CREATE OK, 1 row affected (... sec)

.. _sql-ddl-composite-index:

Defining a Composite Index
--------------------------

Defining a composite (or combined) index is done using the same syntax as above
despite multiple columns are given to the ``fulltext`` index method::

    cr> create table documents (
    ...   title string,
    ...   body string,
    ...   INDEX title_body_ft
    ...     using fulltext(title, body) with (analyzer = 'english')
    ... );
    CREATE OK, 1 row affected (... sec)

Composite indices can include nested columns within object columns as well::

    cr> create table my_table1g (
    ...   title string,
    ...   author object(dynamic) as (
    ...     name string,
    ...     birthday timestamp
    ...   ),
    ...   INDEX author_title_ft using fulltext(title, author['name'])
    ... );
    CREATE OK, 1 row affected (... sec)

.. _sql-ddl-custom-analyzer:

.. _create_custom_analyzer:

Create a Custom Analyzer
========================

An analyzer consists of one tokenizer, zero or more token-filters, and zero or
more char-filters.

When a field-content is analyzed to become a stream of tokens, the char-filter
is applied at first. It is used to filter some special chars from the stream of
characters that make up the content.

Tokenizers split the possibly filtered stream of characters into tokens.

Token-filters can add tokens, delete tokens or transform them to finally
produce the desired stream of tokens.

With these elements in place, analyzers provide finegrained control over
building a token stream used for fulltext search. For example you can use
language specific analyzers, tokenizers and token-filters to get proper search
results for data provided in a certain language.

Here is a simple Example::

    cr> create ANALYZER myanalyzer (
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

.. hide:

    cr> create table hidden_test_table(
    ...     fc string index using fulltext with(analyzer=myanalyzer)
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

Extending a Bultin Analyzer
===========================

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

If you extend custom-analyzers, every part of the analyzer that is ommitted
will be taken from the extended one.  Example::

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
