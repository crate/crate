.. highlight:: psql
.. _sql_lexical:

=================
Lexical Structure
=================

An SQL input consists of a sequence of commands each of which is a sequence of
tokens, terminated by a semicolon (``;``).

The syntax of a command defines its set of valid tokens. A token can be a key
word, an identifier, a quoted identifier, a literal (or constant), or a special
character symbol.

.. rubric:: Table of Contents

.. contents::
   :local:

.. _string_literal:

String Literal
==============

String literals are defined as an arbitrary sequence of characters that are
delimited with single quotes ``'`` as defined in ANSI SQL, for example
``'This is a string'``.

Escape Strings
--------------

The escape character in CrateDB is the single-quote ``'``. A character gets
escaped when adding a single-quote before it. For example a single quote
character within a string literal can be included by writing two adjacent
single quotes, e.g. ``'Jack''s car'``.

.. NOTE::

   Two adjacent single quotes are **not** equivalent to the double-quote
   character ``"``.

.. _sql_lexical_keywords_identifiers:

Key Words and Identifiers
=========================

The table bellow lists **key words** reserved in CrateDB and need to be quoted
if used as identifiers. Items in bold are CrateDB specific and not included in
the SQL standards from SQL-92 to SQL-2011.

.. csv-table::
   :widths: 10, 10, 10, 10

    ABS, DEREF, MEMBER, SECOND
    ABSOLUTE, DESC, MERGE, SECTION
    ACTION, DESCRIBE, METHOD, SELECT
    ADD, DESCRIPTOR, MIN, SENSITIVE
    AFTER, DETERMINISTIC, MINUTE, SESSION
    ALL, DIAGNOSTICS, MOD, SESSION_USER
    STATE, **STRING**, **DIRECTORY**, **SHORT**
    ALLOCATE, DISCONNECT, MODIFIES, SET
    ALTER, DISTINCT, MODULE, SETS
    AND, DO, MONTH, SIGNAL
    ANY, DOMAIN, MULTISET, SIMILAR
    ARE, DOUBLE, NAMES, SIZE
    ARRAY, DROP, NATIONAL, SMALLINT
    ARRAY_AGG, DYNAMIC, NATURAL, SOME
    ARRAY_MAX_CARDINALITY, EACH, NCHAR, SPACE
    AS, ELEMENT, NCLOB, SPECIFIC
    ASC, ELSE, NEW, SPECIFICTYPE
    ASENSITIVE, ELSEIF, NEXT, SQL
    ASSERTION, END, NO, SQLCODE
    ASYMMETRIC, END_FRAME, NONE, SQLERROR
    AT, END_PARTITION, NORMALIZE, SQLEXCEPTION
    ATOMIC, END_EXEC, NOT, SQLSTATE
    AUTHORIZATION, EQUALS, NTH_VALUE, SQLWARNING
    AVG, ESCAPE, NTILE, SQRT
    BEFORE, EVERY, NULL, START
    BEGIN, EXCEPT, NULLIF, **NULLS**
    BEGIN_FRAME, EXCEPTION, NUMERIC, STATIC
    BEGIN_PARTITION, EXEC, OBJECT, STDDEV_POP
    BETWEEN, EXECUTE, OCTET_LENGTH, STDDEV_SAMP
    BIGINT, EXISTS, OF, SUBMULTISET
    BINARY, EXIT, OFFSET, SUBSTRING
    BIT, EXTERNAL, OLD, SUBSTRING_REGEX
    BIT_LENGTH, EXTRACT, ON, SUCCEEDSBLOB
    FALSE, ONLY, SUM, **UNBOUNDED**
    BOOLEAN, FETCH, OPEN, SYMMETRIC
    BOTH, FILTER, OPTION, SYSTEM
    BREADTH, FIRST, OR, SYSTEM_TIME
    BY, FIRST_VALUE, ORDER, SYSTEM_USER
    CALL, FLOAT, ORDINALITY, TABLE
    CALLED, FOR, OUT, TABLESAMPLE
    CARDINALITY, FOREIGN, OUTER, TEMPORARY
    CASCADE, FOUND, OUTPUT, THEN
    CASCADED, FRAME_ROW, OVER, TIME
    CASE, FREE, OVERLAPS, TIMESTAMP
    CAST, FROM, OVERLAY, TIMEZONE_HOUR
    CATALOG, FULL, PAD, TIMEZONE_MINUTE
    CEIL, FUNCTION, PARAMETER, TO
    CEILING, FUSION, PARTIAL, TRAILING
    YEAR, **PARTITION**, **TRY_CAST**, TRANSLATE
    CHAR, GENERAL, **PERSISTENT**, TRANSACTION
    CHAR_LENGTH, GET, PATH, **TRANSIENT**
    CHARACTER, GLOBAL, PERCENT, TRANSLATE_REGEX
    CHARACTER_LENGTH, GO, PERCENT_RANK, TRANSLATION
    CHECK, GOTO, PERCENTILE_CONT, TREAT
    CLOB, GRANT, PERCENTILE_DISC, TRIGGER
    CLOSE, GROUP, PERIOD, TRIM
    COALESCE, GROUPING, PORTION, TRIM_ARRAY
    COLLATE, GROUPS, POSITION, TRUE
    COLLATION, HANDLER, POSITION_REGEX, TRUNCATE
    COLLECT, HAVING, POWER, UESCAPE
    COLUMN, HOLD, PRECEDES, UNDER
    COMMIT, HOUR, PRECISION, UNDO
    CONDITION, IDENTITY, PREPARE, UNION
    CONNECT, IF, PRESERVE, UNIQUE
    CONNECTION, IMMEDIATE, PRIMARY, UNKNOWN
    CONSTRAINT, IN, PRIOR, UNNEST
    CONSTRAINTS, INDICATOR, PRIVILEGES, UNTIL
    CONSTRUCTOR, INITIALLY, PROCEDURE, UPDATE
    CONTAINS, INNER, PUBLIC, UPPER
    CONTINUE, INOUT, RANGE, USAGE
    CONVERT, INPUT, RANK, USER
    CORR, INSENSITIVE, READ, USING
    CORRESPONDING, INSERT, READS, VALUE
    COUNT, INT, REAL, VALUES
    COVAR_POP, INTEGER, RECURSIVE, VALUE_OF
    COVAR_SAMP, INTERSECT, REF, VAR_POP
    CREATE, INTERSECTION, REFERENCES, VAR_SAMP
    CROSS, INTERVAL, REFERENCING, VARBINARY
    CUBE, INTO, REGR_AVGX, VARCHAR
    CUME_DIST, IS, REGR_AVGY, VARYING
    CURRENT, ISOLATION, REGR_COUNT, VERSIONING
    CURRENT_CATALOG, ITERATE, REGR_INTERCEPT, VIEW
    CURRENT_DATE, JOIN, REGR_R2, WHEN
    **STRATIFY**, KEY, REGR_SLOPE, WHENEVER
    CURRENT_PATH, LANGUAGE, REGR_SXX, WHERE
    CURRENT_ROLE, LARGE, REGR_SXYREGR_SYY, WHILE
    CURRENT_ROW, LAST, RELATIVE, WIDTH_BUCKET
    CURRENT_SCHEMA, LAST_VALUE, RELEASE, WINDOW
    CURRENT_TIME, LATERAL, REPEAT, WITH
    CURRENT_TIMESTAMP, LEAD, RESIGNAL, WITHIN
    ZONE, LEADING, RESTRICT, WITHOUT
    CURRENT_USER, LEAVE, RESULT, WORK
    CURSOR, LEFT, RETURN, WRITE
    CYCLE, LEVEL, RETURNS, **BYTE**
    DATA, LIKE, REVOKE, **RESET**
    DATE, LIKE_REGEX, RIGHT, **INDEX**
    DAY, LIMIT, ROLE, **IP**
    DEALLOCATE, LN, ROLLBACK, SCROLL
    DEC, LOCAL, ROLLUP, **LONG**
    DECIMAL, LOCALTIME, ROUTINE, STRATIFY
    DECLARE, LOCALTIMESTAMP, ROW, SEARCH
    DEFAULT, LOCATOR, ROW_NUMBER, MAX
    DEFERRABLE, LOOP, ROWS, DEPTH
    DEFERRED, LOWER, SAVEPOINT,
    DELETE, MAP, SCHEMA,
    DENSE_RANK, MATCH, SCOPE,

Tokens such as ``my_table``, ``id``, ``name``, or ``data`` in the example above
are **identifiers**, which identify names of tables, columns, and other
database objects.

Example::

    CREATE TABLE my_table (
      id INTEGER,
      name STRING,
      data OBJECT
    ) WITH (number_of_replicas = 0);

.. NOTE::

  Key words and unquoted identifiers are case insensitive.

This means that::

  select foo from t;

is equivalent to::

  select Foo from t;

or::

  select FOO from t;

A widely used convention is to write key words in uppercase and identifiers in
lowercase, such as

::

  ALTER TABLE foo ADD COLUMN new_column INTEGER;

::

  INSERT INTO foo (id, name) VALUES (1, 'bar');

Quoted identifiers can contain an arbitrary sequence of charactes enclosed by
double quotes (``"``). Quoted identifiers are never keywords, so you can use
``"update"`` as a table or column name.

.. _sql_lexical_special_chars:

Special Characters
==================

Some non-alphanumeric characters do have a special meaning. For their usage
please refer to the sections where the respective syntax elements are
described.

:Semicolon:
    The semicolon (``;``) terminates an SQL statement. It cannot appear
    anywhere else within the command, except within a string or quoted
    identifier.

:Comma:
    The comma (``,``) is used in various syntactical elements to separate
    elements of a list.

:Brackets:
    Square brackets (``[]``) are used to select elements of arrays and objects,
    e.g. ``arr[1]`` or ``obj['key']``.

:Asterisk:
    The asterisk (``*``) is used in some contexts to denote all columns of a
    table. As an argument in global aggregate functions it has the meaning of
    *any field*, e.g. ``COUNT(*)``.

:Period:
    The period (``.``) is used for numeric values and to separate schema and
    table names, e.g. ``blob.my_blob_table``.

.. _sql_lexical_comments:

Comments
========

An SQL input can contain comments. Comments are not implemented on the server
side, but the `crash`_ command line interface ignores single line comments.
Single line comments start with a double dash (``--``) and end at the end of
that line.

Example::

  SELECT *
    FROM information_schema.tables
    WHERE table_schema = 'doc'; -- query information schema for doc tables

.. _`crash`: https://github.com/crate/crash/
