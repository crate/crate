.. highlight:: psql

.. _dml:

=================
Data manipulation
=================

.. rubric:: Table of contents

.. contents::
   :local:

.. _inserting_data:

Inserting data
==============

Inserting data to CrateDB is done by using the SQL ``INSERT`` statement.

.. NOTE::

    The column list is always ordered based on the column position
    in the :ref:`ref-create-table` statement of the table. If the insert
    columns are omitted, the values in the ``VALUES`` clauses must
    correspond to the table columns in that order.

Inserting a row::

    cr> insert into locations (id, date, description, kind, name, position)
    ... values (
    ...   '14',
    ...   '2013-09-12T21:43:59.000Z',
    ...   'Blagulon Kappa is the planet to which the police are native.',
    ...   'Planet',
    ...   'Blagulon Kappa',
    ...   7
    ... );
    INSERT OK, 1 row affected (... sec)

When inserting a single row, if an error occurs an error is returned as a
response.

Inserting multiple rows at once (aka. bulk insert) can be done by defining
multiple values for the ``INSERT`` statement::

    cr> insert into locations (id, date, description, kind, name, position) values
    ... (
    ...   '16',
    ...   '2013-09-14T21:43:59.000Z',
    ...   'Blagulon Kappa II is the planet to which the police are native.',
    ...   'Planet',
    ...   'Blagulon Kappa II',
    ...   19
    ... ),
    ... (
    ...   '17',
    ...   '2013-09-13T16:43:59.000Z',
    ...   'Brontitall is a planet with a warm, rich atmosphere and no mountains.',
    ...   'Planet',
    ...   'Brontitall',
    ...   10
    ... );
    INSERT OK, 2 rows affected (... sec)

When inserting multiple rows, if an error occurs for some of these rows there
is no error returned but instead the number of rows affected would be decreased
by the number of rows that failed to be inserted.

When inserting into tables containing :ref:`ref-generated-columns`
or :ref:`ref-base-columns` having the :ref:`ref-default-clause` specified,
their values can be safely omitted. They are *generated* upon insert:

::

    cr> CREATE TABLE debit_card (
    ...   owner text,
    ...   num_part1 integer,
    ...   num_part2 integer,
    ...   check_sum integer GENERATED ALWAYS AS ((num_part1 + num_part2) * 42),
    ...   "user" text DEFAULT 'crate'
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> insert into debit_card (owner, num_part1, num_part2) values
    ... ('Zaphod Beeblebrox', 1234, 5678);
    INSERT OK, 1 row affected (... sec)

.. Hidden: refresh debit_card

    cr> refresh table debit_card
    REFRESH OK, 1 row affected (... sec)

::

    cr> select * from debit_card;
    +-------------------+-----------+-----------+-----------+-------+
    | owner             | num_part1 | num_part2 | check_sum | user  |
    +-------------------+-----------+-----------+-----------+-------+
    | Zaphod Beeblebrox |      1234 |      5678 |    290304 | crate |
    +-------------------+-----------+-----------+-----------+-------+
    SELECT 1 row in set (... sec)

For :ref:`ref-generated-columns`, if the value is given, it is validated
against the *generation clause* of the column and the currently inserted row::

    cr> insert into debit_card (owner, num_part1, num_part2, check_sum) values
    ... ('Arthur Dent', 9876, 5432, 642935);
    SQLParseException[Given value 642935 for generated column check_sum does not match calculation ((num_part1 + num_part2) * 42) = 642936]


Inserting data by query
-----------------------

.. Hidden: refresh locations

    cr> refresh table locations
    REFRESH OK, 1 row affected (... sec)

It is possible to insert data using a query instead of values. Column data
types of source and target table can differ as long as the values are castable.
This gives the opportunity to restructure the tables data, renaming a field,
changing a field's data type or convert a normal table into a partitioned one.

Example of changing a field's data type, in this case, changing the
``position`` data type from ``integer`` to ``smallint``::

    cr> create table locations2 (
    ...     id text primary key,
    ...     name text,
    ...     date timestamp with time zone,
    ...     kind text,
    ...     position smallint,
    ...     description text
    ... ) clustered by (id) into 2 shards with (number_of_replicas = 0);
    CREATE OK, 1 row affected (... sec)

::

    cr> insert into locations2 (id, name, date, kind, position, description)
    ... (
    ...     select id, name, date, kind, position, description
    ...     from locations
    ...     where position < 10
    ... );
    INSERT OK, 14 rows affected (... sec)

.. Hidden: drop previously created table

   cr> drop table locations2
    DROP OK, 1 row affected (... sec)

Example of creating a new partitioned table out of the ``locations`` table with
data partitioned by year::

    cr> create table locations_parted (
    ...     id text primary key,
    ...     name text,
    ...     year text primary key,
    ...     date timestamp with time zone,
    ...     kind text,
    ...     position integer
    ... ) clustered by (id) into 2 shards
    ... partitioned by (year) with (number_of_replicas = 0);
    CREATE OK, 1 row affected (... sec)

::

    cr> insert into locations_parted (id, name, year, date, kind, position)
    ... (
    ...     select
    ...         id,
    ...         name,
    ...         date_format('%Y', date),
    ...         date,
    ...         kind,
    ...         position
    ...     from locations
    ... );
    INSERT OK, 16 rows affected (... sec)

Resulting partitions of the last insert by query::

    cr> select table_name, partition_ident, values, number_of_shards, number_of_replicas
    ... from information_schema.table_partitions
    ... where table_name = 'locations_parted'
    ... order by partition_ident;
    +------------------+-----------------+------------------+------------------+--------------------+
    | table_name       | partition_ident | values           | number_of_shards | number_of_replicas |
    +------------------+-----------------+------------------+------------------+--------------------+
    | locations_parted | 042j2e9n74      | {"year": "1979"} |                2 |                  0 |
    | locations_parted | 042j4c1h6c      | {"year": "2013"} |                2 |                  0 |
    +------------------+-----------------+------------------+------------------+--------------------+
    SELECT 2 rows in set (... sec)

.. Hidden: drop previously created table

   cr> drop table locations_parted;
    DROP OK, 1 row affected (... sec)

.. NOTE::

   ``limit``, ``offset`` and ``order by`` are not supported inside the query
   statement.

Upserts (``ON CONFLICT DO UPDATE SET``)
---------------------------------------

The ``ON CONFLICT DO UPDATE SET`` clause is used to update the existing row if
inserting is not possible because of a duplicate-key conflict if a document
with the same ``PRIMARY KEY`` already exists. This is type of operation is
commonly referred to as an *upsert*, being a combination of "update" and
"insert".

::

    cr> select
    ...     name,
    ...     visits,
    ...     extract(year from last_visit) as last_visit
    ... from uservisits order by name;
    +----------+--------+------------+
    | name     | visits | last_visit |
    +----------+--------+------------+
    | Ford     |      1 | 2013       |
    | Trillian |      3 | 2013       |
    +----------+--------+------------+
    SELECT 2 rows in set (... sec)

::

    cr> insert into uservisits (id, name, visits, last_visit) values
    ... (
    ...     0,
    ...     'Ford',
    ...     1,
    ...     '2015-09-12'
    ... ) on conflict (id) do update set
    ...     visits = visits + 1,
    ...     last_visit = '2015-01-12';
    INSERT OK, 1 row affected (... sec)

.. Hidden: refresh uservisits

    cr> refresh table uservisits
    REFRESH OK, 1 row affected (... sec)

::

    cr> select
    ...     name,
    ...     visits,
    ...     extract(year from last_visit) as last_visit
    ... from uservisits where id = 0;
    +------+--------+------------+
    | name | visits | last_visit |
    +------+--------+------------+
    | Ford |      2 | 2015       |
    +------+--------+------------+
    SELECT 1 row in set (... sec)

It's possible to refer to values which would be inserted if no duplicate-key
conflict occurred, by using the special ``excluded`` table. This table is
especially useful in multiple-row inserts, to refer to the current rows
values::

    cr> insert into uservisits (id, name, visits, last_visit) values
    ... (
    ...     0,
    ...     'Ford',
    ...     2,
    ...     '2016-01-13'
    ... ),
    ... (
    ...     1,
    ...     'Trillian',
    ...     5,
    ...     '2016-01-15'
    ... ) on conflict (id) do update set
    ...     visits = visits + excluded.visits,
    ...     last_visit = excluded.last_visit;
    INSERT OK, 2 rows affected (... sec)

.. Hidden: refresh uservisits

    cr> refresh table uservisits
    REFRESH OK, 1 row affected (... sec)

::

    cr> select
    ...     name,
    ...     visits,
    ...     extract(year from last_visit) as last_visit
    ... from uservisits order by name;
    +----------+--------+------------+
    | name     | visits | last_visit |
    +----------+--------+------------+
    | Ford     |      4 | 2016       |
    | Trillian |      8 | 2016       |
    +----------+--------+------------+
    SELECT 2 rows in set (... sec)

This can also be done when using a query instead of values::

    cr> create table uservisits2 (
    ...   id integer primary key,
    ...   name text,
    ...   visits integer,
    ...   last_visit timestamp with time zone
    ... ) clustered by (id) into 2 shards with (number_of_replicas = 0);
    CREATE OK, 1 row affected (... sec)

::

    cr> insert into uservisits2 (id, name, visits, last_visit)
    ... (
    ...     select id, name, visits, last_visit
    ...     from uservisits
    ... );
    INSERT OK, 2 rows affected (... sec)

.. Hidden: refresh uservisits2

    cr> refresh table uservisits2
    REFRESH OK, 1 row affected (... sec)

::

    cr> insert into uservisits2 (id, name, visits, last_visit)
    ... (
    ...     select id, name, visits, last_visit
    ...     from uservisits
    ... ) on conflict (id) do update set
    ...     visits = visits + excluded.visits,
    ...     last_visit = excluded.last_visit;
    INSERT OK, 2 rows affected (... sec)

.. Hidden: refresh uservisits2

    cr> refresh table uservisits2
    REFRESH OK, 1 row affected (... sec)

::

    cr> select
    ...     name,
    ...     visits,
    ...     extract(year from last_visit) as last_visit
    ... from uservisits order by name;
    +----------+--------+------------+
    | name     | visits | last_visit |
    +----------+--------+------------+
    | Ford     |      4 | 2016       |
    | Trillian |      8 | 2016       |
    +----------+--------+------------+
    SELECT 2 rows in set (... sec)

.. Hidden: drop previously created table

   cr> drop table uservisits2
    DROP OK, 1 row affected (... sec)

.. _dml_updating_data:

Updating data
=============

In order to update documents in CrateDB the SQL ``UPDATE`` statement can be
used::

    cr> update locations set description = 'Updated description'
    ... where name = 'Bartledan';
    UPDATE OK, 1 row affected (... sec)

Updating nested objects is also supported::

    cr> update locations set inhabitants['name'] = 'Human' where name = 'Bartledan';
    UPDATE OK, 1 row affected (... sec)

It's also possible to reference a column within the expression, for example to
increment a number like this::

    cr> update locations set position = position + 1 where position < 3;
    UPDATE OK, 6 rows affected (... sec)

.. NOTE::

    If the same documents are updated concurrently an VersionConflictException
    might occur. CrateDB contains a retry logic that tries to resolve the
    conflict automatically.

.. _dml_deleting_data:

Deleting data
=============

Deleting rows in CrateDB is done using the SQL ``DELETE`` statement::

    cr> delete from locations where position > 3;
    DELETE OK, ... rows affected (... sec)

.. _importing_data:

Import and export
=================

Importing data
--------------

Using the ``COPY FROM`` statement, CrateDB nodes can import data from local
files or files that are available over the network.

The supported data formats are JSON and CSV. The format is inferred from the
file extension, if possible. Alternatively the format can also be provided as an
option (see :ref:`with_option`). If the format is not provided and cannot be
inferred from the file extension, it will be processed as JSON.

JSON files must contain a single JSON object per line.

Example JSON data::

    {"id": 1, "quote": "Don't panic"}
    {"id": 2, "quote": "Ford, you're turning into a penguin. Stop it."}

CSV files must contain a header with comma-separated values, which will
be added as columns.

Example CSV data::

    id,quote
    1,"Don't panic"
    2,"Ford, you're turning into a penguin. Stop it."

.. NOTE::

  * The ``COPY FROM`` statement will not convert or validate your data. Please
    make sure that it fits your schema.
  * Values for generated columns will be computed if the data does not contain
    them, otherwise they will be imported but not validated, so please make
    sure that they are correct.
  * Furthermore, column names in your data are considered case sensitive (as if
    they were quoted in a SQL statement).

For further information, including how to import data to
:ref:`partitioned_tables`, take a look at the :ref:`copy_from` reference.

Example
.......

.. highlight:: psql

Here's an example statement::

    cr> COPY quotes FROM 'file:///tmp/import_data/quotes.json';
    COPY OK, 3 rows affected (... sec)

This statement imports data from the ``/tmp/import_data/quotes.json`` file and
uses it to create a table named ``quotes``.

.. NOTE::

    The file you specify must be available on one of the CrateDB nodes. *This
    statement will not work with files that are local to your client.*

    For the above statement, every node in the cluster will attempt to import
    data from a file located at ``/tmp/import_data/quotes.json`` relative to
    the ``crate`` process (i.e., if you are running CrateDB inside a container,
    the file must also be inside the container).

    If you want to import data from a file that on your local computer using
    ``COPY FROM``, you must first transfer the file to one of the CrateDB
    nodes.

    Consult the :ref:`copy_from` reference for additional information.

.. Hidden: delete imported data

    cr> refresh table quotes;
    REFRESH OK, 1 row affected (... sec)
    cr> delete from quotes;
    DELETE OK, 3 rows affected (... sec)

If you want to import all files inside the ``/tmp/import_data`` directory on
every CrateDB node, you can use a wildcard, like so::

    cr> COPY quotes FROM '/tmp/import_data/*' WITH (bulk_size = 4);
    COPY OK, 3 rows affected (... sec)

.. Hidden: delete imported data

    cr> refresh table quotes;
    REFRESH OK, 1 row affected (... sec)
    cr> delete from quotes;
    DELETE OK, 3 rows affected (... sec)
    cr> refresh table quotes;
    REFRESH OK, 1 row affected (... sec)

This wildcard can also be used to only match certain files in a directory::

    cr> COPY quotes FROM '/tmp/import_data/qu*.json';
    COPY OK, 3 rows affected (... sec)

.. Hidden: delete imported data

    cr> refresh table quotes;
    REFRESH OK, 1 row affected (... sec)
    cr> delete from quotes;
    DELETE OK, 3 rows affected (... sec)
    cr> refresh table quotes;
    REFRESH OK, 1 row affected (... sec)

Detailed error reporting
........................

If the ``RETURN_SUMMARY`` clause is specified, a result set containing information
about failures and successfully imported records is returned.

.. Hidden: delete existing data

    cr> refresh table locations;
    REFRESH OK, 1 row affected (... sec)
    cr> delete from locations;
    DELETE OK, 8 rows affected (... sec)
    cr> refresh table locations;
    REFRESH OK, 1 row affected (... sec)

::

   cr> COPY locations FROM '/tmp/import_data/locations_with_failure/locations*.json' RETURN SUMMARY;
    +--...--+----------...--------+---------------+-------------+-------------------...--------------------------------------+
    | node  | uri                 | success_count | error_count | errors                                                     |
    +--...--+----------...--------+---------------+-------------+-------------------...--------------------------------------+
    | {...} | .../locations1.json |             6 |           0 | {}                                                         |
    | {...} | .../locations2.json |             5 |           2 | {"failed to parse ...{"count": 2, "line_numbers": [1, 2]}} |
    +--...--+----------...--------+---------------+-------------+-------------------...--------------------------------------+
    COPY 2 rows in set (... sec)

.. Hidden: delete imported data

    cr> refresh table locations;
    REFRESH OK, 1 row affected (... sec)
    cr> delete from locations;
    DELETE OK, ...
    cr> refresh table locations;
    REFRESH OK, 1 row affected (... sec)

If an error happens while processing the URI in general, the ``error_count`` and
``success_count`` columns will contains `NULL` values to indicate that no records were processed.

::

   cr> COPY locations FROM '/tmp/import_data/not-existing.json' RETURN SUMMARY;
    +--...--+-----------...---------+---------------+-------------+------------------------...------------------------+
    | node  | uri                   | success_count | error_count | errors                                            |
    +--...--+-----------...---------+---------------+-------------+------------------------...------------------------+
    | {...} | .../not-existing.json |          NULL |        NULL | {"...not-existing.json (...)": {"count": 1, ...}} |
    +--...--+-----------...---------+---------------+-------------+------------------------...------------------------+
   COPY 1 row in set (... sec)

See :ref:`copy_from` for more information.

.. _exporting_data:

Exporting data
--------------

Data can be exported using the ``COPY TO`` statement. Data is exported in a
distributed way, meaning each node will export its own data.

Replicated data is not exported. So every row of an exported table is stored
only once.

This example shows how to export a given table into files named after the table
and shard ID with gzip compression:

.. Hidden: import data

   cr> REFRESH TABLE quotes;
   REFRESH OK...
   cr> COPY quotes FROM '/tmp/import_data/*';
   COPY OK, 3 rows affected (... sec)

::

    cr> REFRESH TABLE quotes;
    REFRESH OK...

::

    cr> COPY quotes TO DIRECTORY '/tmp/' with (compression='gzip');
    COPY OK, 3 rows affected ...

Instead of exporting a whole table, rows can be filtered by an optional WHERE
clause condition. This is useful if only a subset of the data needs to be
exported::

    cr> COPY quotes WHERE match(quote_ft, 'time') TO DIRECTORY '/tmp/' WITH (compression='gzip');
    COPY OK, 2 rows affected ...

For further details see :ref:`copy_to`.

.. _crate-python: https://pypi.python.org/pypi/crate/
.. _PCRE: https://www.pcre.org/
