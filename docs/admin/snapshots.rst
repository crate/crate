.. highlight:: psql
.. _snapshot-restore:

=========
Snapshots
=========

.. rubric:: Table of contents

.. contents::
   :local:

Snapshot
--------

In CrateDB, backups are called *Snapshots*. They represent the state of the
tables in a CrateDB cluster at the time the *Snapshot* was created. A
*Snapshot* is always stored in a *Repository* which has to be created first.

.. CAUTION::

   You cannot snapshot BLOB tables.

Creating a repository
.....................

Repositories are used to store, manage and restore snapshots.

They are created using the :ref:`sql-create-repository` statement::

    cr> CREATE REPOSITORY where_my_snapshots_go TYPE fs
    ... WITH (location='repo_path', compress=true);
    CREATE OK, 1 row affected (... sec)

Repositories are uniquely identified by their name. Every repository has a
specific type which determines how snapshots are stored.

CrateDB supports different repository types, see :ref:`sql-create-repo-types`.

The creation of a repository configures it inside the CrateDB cluster. In
general no data is written, no snapshots inside repositories changed or
deleted. This way you can tell the CrateDB cluster about existing repositories
which already contain snapshots.

Creating a repository with the same name will result in an error::

    cr> CREATE REPOSITORY where_my_snapshots_go TYPE fs
    ... WITH (location='another_repo_path', compress=false);
    RepositoryAlreadyExistsException[Repository 'where_my_snapshots_go' already exists]

Creating a snapshot
...................

Snapshots are created inside a repository and can contain any number of tables.
The :ref:`sql-create-snapshot` statement is used to create a snapshots::

    cr> CREATE SNAPSHOT where_my_snapshots_go.snapshot1 ALL
    ... WITH (wait_for_completion=true, ignore_unavailable=true);
    CREATE OK, 1 row affected (... sec)

A snapshot is referenced by the name of the repository and the snapshot name,
separated by a dot. If ``ALL`` is used, all *user created* tables of the
cluster (except blob tables) are stored inside the snapshot.

It's possible to only save a specific subset of tables in the snapshot by
listing them explicitly::

    cr> CREATE SNAPSHOT where_my_snapshots_go.snapshot2 TABLE quotes, doc.locations
    ... WITH (wait_for_completion=true);
    CREATE OK, 1 row affected (... sec)

.. Hidden: create partitioned table

    cr> CREATE TABLE parted_table (
    ...   id integer,
    ...   value text,
    ...   date timestamp with time zone
    ... ) clustered into 1 shards partitioned by (date) with (number_of_replicas=0);
    CREATE OK, 1 row affected (... sec)
    cr> INSERT INTO parted_table (id, value, date)
    ... VALUES (1, 'foo', '1970-01-01'), (2, 'bar', '2015-10-19');
    INSERT OK, 2 rows affected (... sec)
    cr> REFRESH TABLE parted_table;
    REFRESH OK, 2 rows affected (... sec)

Even single partition of :ref:`partitioned-tables` can be selected for backup.
This is especially useful if old partitions need to be deleted but it should be
possible to restore them if needed::

    cr> CREATE SNAPSHOT where_my_snapshots_go.snapshot3 TABLE
    ...    locations,
    ...    parted_table PARTITION (date='1970-01-01')
    ... WITH (wait_for_completion=true);
    CREATE OK, 1 row affected (... sec)

Snapshots are **incremental**. Snapshots of the same cluster created later only
store data not already contained in the repository.

All examples above are used with the argument ``wait_for_completion`` set to
*true*. As described in the :ref:`sql-create-repository` reference
documentation, by doing this, the statement will only respond (successfully or
not) when the snapshot is fully created. Otherwise the snapshot will be created
in the background and the statement will immediately respond as successful. The
status of a created snapshot can be retrieved by querying the
:ref:`sys.snapshots <sys-snapshots>` system table.

Restore
-------

.. CAUTION::

   If you are restoring a snapshot into a newer version of CrateDB, be sure to
   check the :ref:`release_notes` for upgrade instructions.

.. CAUTION::

   If you try to restore a table that already exists, CrateDB will return an
   error. However, if you try to restore metadata or cluster settings that
   already exist, they will be overwritten.

Once a snapshot is created, it can be used to restore its tables to the state
when the snapshot was created.

To get basic information about snapshots the :ref:`sys.snapshots
<sys-snapshots>` table can be queried::

    cr> SELECT repository, name, state, concrete_indices
    ... FROM sys.snapshots
    ... ORDER BY repository, name;
    +-----------------------+-----------+---------+--------------------...-+
    | repository            | name      | state   | concrete_indices       |
    +-----------------------+-----------+---------+--------------------...-+
    | where_my_snapshots_go | snapshot1 | SUCCESS | [...]                  |
    | where_my_snapshots_go | snapshot2 | SUCCESS | [...]                  |
    | where_my_snapshots_go | snapshot3 | SUCCESS | [...]                  |
    +-----------------------+-----------+---------+--------------------...-+
    SELECT 3 rows in set (... sec)

To restore a table from a snapshot we have to drop it beforehand::

    cr> DROP TABLE quotes;
    DROP OK, 1 row affected (... sec)

Restoring a snapshot using the :ref:`sql-restore-snapshot` statement.::

    cr> RESTORE SNAPSHOT where_my_snapshots_go.snapshot2
    ... TABLE quotes
    ... WITH (wait_for_completion=true);
    RESTORE OK, 1 row affected (... sec)

In this case only the ``quotes`` table from snapshot
``where_my_snapshots_go.snapshot2`` is restored.

It's not possible to restore tables that exist in the current cluster::

    cr> RESTORE SNAPSHOT where_my_snapshots_go.snapshot2 TABLE quotes;
    RelationAlreadyExists[Relation 'doc.quotes' already exists.]

Single partitions can be either imported into an existing partitioned table the
partition belongs to.

To monitor the progress of ``RESTORE SNAPSHOT`` operations please query
the :ref:`sys.snapshot_restore <sys-snapshot-restore>` table.

.. Hidden: drop partition::

    cr> DELETE FROM parted_table WHERE date = '1970-01-01';
    DELETE OK, -1 rows affected (... sec)

::

    cr> RESTORE SNAPSHOT where_my_snapshots_go.snapshot3 TABLE
    ...    parted_table PARTITION (date='1970-01-01')
    ... WITH (wait_for_completion=true);
    RESTORE OK, 1 row affected (... sec)

Or if no matching partition table exists, it will be implicitly created during
restore.

.. CAUTION::

    This is only possible with CrateDB version 0.55.5 or greater!

    Snapshots of single partitions that have been created with earlier versions
    of CrateDB may be restored, but lead to orphaned partitions!

    When using CrateDB prior to 0.55.5 you will have to create the table schema
    first before restoring.

.. Hidden: drop partition::

    cr> DROP TABLE parted_table;
    DROP OK, 1 row affected (... sec)

::

    cr> RESTORE SNAPSHOT where_my_snapshots_go.snapshot3 TABLE
    ...    parted_table PARTITION (date=0)
    ... WITH (wait_for_completion=true);
    RESTORE OK, 1 row affected (... sec)

Restore data granularity
........................

You are not limited to only being able to restore individual tables (or table
partitions). For example:

- You can use ``ALL`` instead of listing all tables to restore the whole
  snapshot, including all metadata and settings.

- You can use ``TABLES`` to restore all tables but no metadata or settings.
  On the other hand, you can use ``METADATA`` to restore *everything but*
  tables.

- You can use ``USERMANAGEMENT`` to restore database users, roles and their
  privileges.

See the :ref:`sql-restore-snapshot` documentation for all possible options.


Cleanup
-------

Dropping snapshots
..................

Dropping a snapshot deletes all files inside the repository that are only
referenced by this snapshot. Due to its incremental nature this might be very
few files (e.g. for intermediate snapshots). Snapshots are dropped using the
:ref:`ref-drop-snapshot` command::

    cr> DROP SNAPSHOT where_my_snapshots_go.snapshot3;
    DROP OK, 1 row affected (... sec)

Dropping repositories
.....................

.. Hidden: create repository

    cr> CREATE REPOSITORY "OldRepository" TYPE fs WITH (location='old_path');
    CREATE OK, 1 row affected (... sec)

If a repository is not needed anymore, it can be dropped using the
:ref:`sql-drop-repository` statement::

    cr> DROP REPOSITORY "OldRepository";
    DROP OK, 1 row affected (... sec)

This statement, like :ref:`sql-create-repository`, does not manipulate
repository contents but only deletes stored configuration for this repository
in the cluster state, so it's not accessible any more.

.. Hidden: cleanup

    cr> DROP TABLE parted_table;
    DROP OK, 1 row affected (... sec)
    cr> DROP SNAPSHOT where_my_snapshots_go.snapshot1;
    DROP OK, 1 row affected (... sec)
    cr> DROP SNAPSHOT where_my_snapshots_go.snapshot2;
    DROP OK, 1 row affected (... sec)
    cr> DROP REPOSITORY where_my_snapshots_go;
    DROP OK, 1 row affected (... sec)

