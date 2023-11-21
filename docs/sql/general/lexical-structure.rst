.. highlight:: psql

.. _sql_lexical:

=================
Lexical structure
=================

An SQL input consists of a sequence of commands each of which is a sequence of
tokens, terminated by a semicolon (``;``).

The syntax of a command defines its set of valid tokens. A token can be a key
word, an identifier, a quoted identifier, a literal (or constant), or a special
character symbol.

.. rubric:: Table of contents

.. contents::
   :local:


.. _string_literal:

String literal
==============

String literals are defined as an arbitrary sequence of characters that are
delimited with single quotes ``'`` as defined in ANSI SQL, for example
``'This is a string'``.

In addition, CrateDB supports dollar quoted strings to help avoid escaping
single quotes within single quoted strings.
For example, ``'I''m a string'`` can be re-written as
``$<tag>$I'm a string$<tag>$``, where the matching pair of ``<tag>`` can be
zero or more characters in length.

::

    cr> select 'I''m a string' = $tag1$I'm a string$tag1$;
    +------+
    | true |
    +------+
    | TRUE |
    +------+
    SELECT 1 row in set (... sec)

.. NOTE::

    Nested dollar quoted strings are currently not supported.


Escape strings
--------------

The escape character in CrateDB is the single-quote ``'``. A character gets
escaped when adding a single-quote before it. For example a single quote
character within a string literal can be included by writing two adjacent
single quotes, e.g., ``'Jack''s car'``.

.. NOTE::

   Two adjacent single quotes are **not** equivalent to the double-quote
   character ``"``.


.. _sql_escape_string_literals:

String literals with C-Style escapes
------------------------------------

In addition to the escaped character ``'``, CrateDB supports C-Style escaped
string sequences. Such a sequence is constructed by prefixing the string
literal with the letter ``E`` or ``e``, for example, ``e'hello\nWorld'``.
The following escaped sequences are supported:

==================================================   ================
Escape Sequence                                       Interpretation
==================================================   ================
``\b``                                                backspace
``\f``                                                form feed
``\n``                                                newline
``\r``                                                carriage return
``\t``                                                tab
``\o``, ``\oo``, ``\ooo`` (``o`` = [0-7])             octal byte value
``\xh``, ``xhh`` (``h`` = [0-9,A-F,a-f])              hexadecimal byte value
``\uxxxx``, ``\Uxxxxxxxx`` (``x`` = [0-9,A-F,a-f])    16 or 32-bit hexadecimal Unicode character value
==================================================   ================

For instance, the escape string literal ``e'\u0061\x61\141'`` is equivalent to
the ``'aaa'`` string literal.

::

    cr> select e'\u0061\x61\141' as col1;
    +------+
    | col1 |
    +------+
    | aaa  |
    +------+
    SELECT 1 row in set (... sec)

Any other character following a backslash is taken literally. Thus, to include
a backslash character ``\``, two adjacent backslashes need to be used
(i.e. ``\\``).

::

    cr> select e'aa\\nbb' as col1;
    +--------+
    | col1   |
    +--------+
    | aa\nbb |
    +--------+
    SELECT 1 row in set (... sec)

Finally, a single quote can be included in an escape string literal by also
using the escape backslash character: ``\'``, in addition to the single-quote
described in the :ref:`string literals <string_literal>` section.

::

    cr> select e'aa\'bb' as col1;
    +-------+
    | col1  |
    +-------+
    | aa'bb |
    +-------+
    SELECT 1 row in set (... sec)


.. _sql_lexical_keywords_identifiers:

Key words and identifiers
=========================

The table below lists all *reserved key words* in CrateDB. These need to be
quoted if used as identifiers::

    cr> SELECT word FROM pg_catalog.pg_get_keywords() WHERE catcode = 'R' ORDER BY 1;
    +-------------------+
    | word              |
    +-------------------+
    | add               |
    | all               |
    | alter             |
    | and               |
    | any               |
    | array             |
    | as                |
    | asc               |
    | between           |
    | by                |
    | called            |
    | case              |
    | cast              |
    | column            |
    | constraint        |
    | costs             |
    | create            |
    | cross             |
    | current_date      |
    | current_schema    |
    | current_time      |
    | current_timestamp |
    | current_user      |
    | default           |
    | delete            |
    | deny              |
    | desc              |
    | describe          |
    | directory         |
    | distinct          |
    | drop              |
    | else              |
    | end               |
    | escape            |
    | except            |
    | exists            |
    | extract           |
    | false             |
    | first             |
    | for               |
    | from              |
    | full              |
    | function          |
    | grant             |
    | group             |
    | having            |
    | if                |
    | in                |
    | index             |
    | inner             |
    | input             |
    | insert            |
    | intersect         |
    | into              |
    | is                |
    | join              |
    | last              |
    | left              |
    | like              |
    | limit             |
    | match             |
    | natural           |
    | not               |
    | null              |
    | nulls             |
    | object            |
    | offset            |
    | on                |
    | or                |
    | order             |
    | outer             |
    | persistent        |
    | recursive         |
    | reset             |
    | returns           |
    | revoke            |
    | right             |
    | select            |
    | session_user      |
    | set               |
    | some              |
    | stratify          |
    | table             |
    | then              |
    | transient         |
    | true              |
    | try_cast          |
    | unbounded         |
    | union             |
    | update            |
    | user              |
    | using             |
    | when              |
    | where             |
    | with              |
    +-------------------+
    SELECT 95 rows in set (... sec)

Tokens such as ``my_table``, ``id``, ``name``, or ``data`` in the example below
are *identifiers*, which identify names of tables, columns, and other database
objects.

Example::

    CREATE TABLE my_table (
      id INTEGER,
      name STRING,
      data OBJECT
    ) WITH (number_of_replicas = 0);

.. NOTE::

  Key words and unquoted identifiers are case insensitive while quoted
  identifiers are case sensitive.

This means that::

  select foo from t;

is equivalent to::

  select Foo from t;

or::

  select FOO from t;

To query a table named ``Foo``::

  select "Foo" from t;

A widely used convention is to write key words in uppercase and identifiers in
lowercase, such as

::

  ALTER TABLE foo ADD COLUMN new_column INTEGER;

::

  INSERT INTO foo (id, name) VALUES (1, 'bar');

Quoted identifiers can contain an arbitrary sequence of characters enclosed by
double quotes (``"``). Quoted identifiers are never keywords, so you can use
``"update"`` as a table or column name.


.. _sql_lexical_special_chars:

Special characters
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
    table. As an argument in global :ref:`aggregate functions
    <aggregation-functions>` it has the meaning of *any field*,
    e.g. ``COUNT(*)``.

:Period:
    The period (``.``) is used for numeric values and to separate schema and
    table names, e.g. ``blob.my_blob_table``.


.. _sql_lexical_comments:

Comments
========

An SQL statement can contain comments. Single line comments start with a double
dash (``--``) and end at the end of that line. Multi line comments start with
``/*`` and end with ``*/``.

Example::

  /*
   * Retrieve information about all tables in the 'doc' schema.
   */
  SELECT *
    FROM information_schema.tables
    WHERE table_schema = 'doc'; -- query information schema for doc tables

