=================
Show Create Table
=================

.. hide:
    cr> create table if not exists my_table (
    ...   first_column integer primary key,
    ...   second_column text,
    ...   third_column timestamp with time zone,
    ...   fourth_column object(strict) as (
    ...     key text,
    ...     value text
    ...   )
    ... ) clustered by (first_column) into 5 shards with ("merge.scheduler.max_thread_count" = 1);
    CREATE OK, 1 row affected (... sec)

The ``SHOW CREATE TABLE`` statement prints the ``CREATE TABLE`` statement of an
existing user-created doc table in the cluster::

    cr> show create table my_table;
    +-----------------------------------------------+
    | SHOW CREATE TABLE doc.my_table                |
    +-----------------------------------------------+
    | CREATE TABLE IF NOT EXISTS "doc"."my_table" ( |
    |    "first_column" INTEGER NOT NULL,           |
    |    "second_column" TEXT,                      |
    |    "third_column" TIMESTAMP WITH TIME ZONE,   |
    |    "fourth_column" OBJECT(STRICT) AS (        |
    |       "key" TEXT,                             |
    |       "value" TEXT                            |
    |    ),                                         |
    |    PRIMARY KEY ("first_column")               |
    | )                                             |
    | CLUSTERED BY ("first_column") INTO 5 SHARDS   |
    | WITH (                                        |
    |    column_policy = 'strict',                  |
    |    "merge.scheduler.max_thread_count" = 1,    |
    |    number_of_replicas = '0-1'                 |
    | )                                             |
    +-----------------------------------------------+
    SHOW 1 row in set (... sec)

The ``WITH`` clause always shows ``column_policy`` and ``number_of_replicas``
plus any explicitly set settings.

To see all currently effective settings you can query the ``settings`` column of
:ref:`information_schema_tables`.
