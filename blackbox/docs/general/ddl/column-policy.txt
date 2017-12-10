.. _column_policy:

=============
Column Policy
=============

The Column Policy defines if a table enforces its defined schema or if it's
allowed to store additional columns which are a not defined in the table
schema.

If the column policy is not defined within the ``with`` clause, ``dynamic``
will be used.

``strict``
==========

The column policy can be configured to be ``strict``, rejecting any column on
insert/update/copy_to which is not defined in the schema.

Example::

    cr> create table my_table (
    ...   title string,
    ...   author string
    ... ) with (column_policy = 'strict');
    CREATE OK, 1 row affected (... sec)

If you try to insert using a column not specified in the table schema,
CrateDB will raise an error.

    cr> insert into my_table (new_col) values(1);
    SQLActionException[ColumnUnknownException: Column new_col unknown]

.. hide:

    cr> drop table my_table;
    DROP OK, 1 row affected (... sec)

``dynamic``
===========

The other option is ``dynamic`` which is the default policy. ``dynamic`` means
that new columns can be added using ``insert``, ``update`` or ``copy from``.

Note that adding new columns to a table with a ``dynamic`` policy will affect
the schema of the table. Once a column is added, it shows up in the
``information_schema.columns`` table and its type and attributes are fixed. It
will have the type that was guessed by its inserted/updated value and they will
always be ``not_indexed`` which means they are analyzed with the ``plain``
analyzer, which means as-is.

.. NOTE::

   The data type of the new column is guessed from the data type of the value
   provided in the insert statement that creates the column.

If a new column ``a`` was added with type ``boolean``, adding strings to this
column will result in an error, except the string can be implicit casted to a
``boolean`` value.

Here's an example::

    cr> create table my_table (
    ...   title string,
    ...   author string
    ... );
    CREATE OK, 1 row affected (... sec)

.. hide:

    cr> drop table my_table;
    DROP OK, 1 row affected (... sec)

Which is exactly the same as::

    cr> create table my_table (
    ...   title string,
    ...   author string
    ... ) with (column_policy = 'dynamic');
    CREATE OK, 1 row affected (... sec)

Now, you can do inserts that add columns::

    cr> insert into my_table (new_col) values(1);
    INSERT OK, 1 row affected  (... sec)

Which will update the table schema::

    cr> show create table my_table;
    +-----------------------------------------------------+
    | SHOW CREATE TABLE doc.my_table                      |
    +-----------------------------------------------------+
    | CREATE TABLE IF NOT EXISTS "doc"."my_table" (       |
    |    "author" STRING,                                 |
    |    "new_col" LONG,                                  |
    |    "title" STRING                                   |
    | )                                                   |
    | CLUSTERED INTO 4 SHARDS                             |
    ...

New columns added to dynamic tables are identical to regular columns. You can
retrieve them, sort by them and use them in where clauses.

.. hide:

    cr> drop table my_table;
    DROP OK, 1 row affected (... sec)

.. WARNING::

   The mapping update is processed asynchrously on multiple nodes. If a new
   field gets added to the local mapping of two shards, these shards are
   sending their mapping to the master. If this mapping update gets delivered
   later than the next query on the previously added column, it will result in
   a ``ColumnUnknownException``.
