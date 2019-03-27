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
    ... ) clustered by (first_column) into 5 shards;
    CREATE OK, 1 row affected (... sec)

The ``SHOW CREATE TABLE`` statement can be used to print the DDL statement of
already existing user-created doc tables in the cluster::

    cr> show create table my_table;
    +-----------------------------------------------------+
    | SHOW CREATE TABLE doc.my_table                      |
    +-----------------------------------------------------+
    | CREATE TABLE IF NOT EXISTS "doc"."my_table" (       |
    |    "first_column" INTEGER,                          |
    |    "second_column" TEXT,                            |
    |    "third_column" TIMESTAMP WITH TIME ZONE,         |
    |    "fourth_column" OBJECT(STRICT) AS (              |
    |       "key" TEXT,                                   |
    |       "value" TEXT                                  |
    |    ),                                               |
    |    PRIMARY KEY ("first_column")                     |
    | )                                                   |
    | CLUSTERED BY ("first_column") INTO 5 SHARDS         |
    | WITH (                                              |
    |    "allocation.max_retries" = 5,                    |
    |    "blocks.metadata" = false,                       |
    |    "blocks.read" = false,                           |
    |    "blocks.read_only" = false,                      |
    |    "blocks.read_only_allow_delete" = false,         |
    |    "blocks.write" = false,                          |
    |    column_policy = 'strict',                        |
    |    "mapping.total_fields.limit" = 1000,             |
    |    max_ngram_diff = 1,                              |
    |    max_shingle_diff = 3,                            |
    |    number_of_replicas = '0-1',                      |
    |    refresh_interval = 1000,                         |
    |    "routing.allocation.enable" = 'all',             |
    |    "routing.allocation.total_shards_per_node" = -1, |
    |    "translog.durability" = 'REQUEST',               |
    |    "translog.flush_threshold_size" = 536870912,     |
    |    "translog.sync_interval" = 5000,                 |
    |    "unassigned.node_left.delayed_timeout" = 60000,  |
    |    "warmer.enabled" = true,                         |
    |    "write.wait_for_active_shards" = 'ALL'           |
    | )                                                   |
    +-----------------------------------------------------+
    SHOW 1 row in set (... sec)

The table settings returned within the ``WITH`` clause of the result are all
available table settings showing their respective values at the time of the
execution of the ``SHOW`` statement.

Different versions of CrateDB may have different default table settings. This
means that if you re-create the table using the resulting ``CREATE TABLE``
statement the settings of the 'old' table may differ from the settings of the
'new' table. This is because the table settings are set explicitly on creation
time.
