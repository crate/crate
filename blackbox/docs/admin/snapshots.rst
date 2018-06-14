.. highlight:: psql
.. _snapshot-restore:

=========
Snapshots
=========

.. rubric:: Table of Contents

.. contents::
   :local:

Snapshot
--------

In CrateDB, backups are called *Snapshots*. They represent the state of the
tables in a CrateDB cluster at the time the *Snapshot* was created. A
*Snapshot* is always stored in a *Repository* which has to be created first.

.. CAUTION::

   You cannot snapshot BLOB tables.

Creating a Repository
.....................

Repositories are used to store, manage and restore snapshots.

They are created using the :ref:`ref-create-repository` statement::

    cr> CREATE REPOSITORY where_my_snapshots_go TYPE fs
    ... WITH (location='repo_path', compress=true);
    CREATE OK, 1 row affected (... sec)

Repositories are uniquely identified by their name. Every repository has a
specific type which determines how snapshots are stored.

CrateDB supports different repository types:
:ref:`ref-create-repository-types-fs`, :ref:`ref-create-repository-types-hdfs`,
:ref:`ref-create-repository-types-s3`, and
:ref:`ref-create-repository-types-url`. Support for further types can be
added using `plugins`_.

The creation of a repository configures it inside the CrateDB cluster. In
general no data is written, no snapshots inside repositories changed or
deleted. This way you can tell the CrateDB cluster about existing repositories
which already contain snapshots.

Creating a repository with the same name will result in an error::

    cr> CREATE REPOSITORY where_my_snapshots_go TYPE fs
    ... WITH (location='another_repo_path', compress=false);
    SQLActionException[RepositoryAlreadyExistsException: Repository 'where_my_snapshots_go' already exists]

.. _plugins: https://github.com/crate/crate/blob/master/devs/docs/plugins.rst

Creating a Snapshot
...................

Snapshots are created inside a repository and can contain any number of tables.
The :ref:`ref-create-snapshot` statement is used to create a snapshots::

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
    ...   id int,
    ...   value string,
    ...   date timestamp
    ... ) clustered into 1 shards partitioned by (date) with (number_of_replicas=0);
    CREATE OK, 1 row affected (... sec)
    cr> INSERT INTO parted_table (id, value, date)
    ... VALUES (1, 'foo', '1970-01-01'), (2, 'bar', '2015-10-19');
    INSERT OK, 2 rows affected (... sec)
    cr> REFRESH TABLE parted_table;
    REFRESH OK, 2 rows affected (... sec)

Even single partition of :ref:`partitioned_tables` can be selected for backup.
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
*true*. As described in the :ref:`ref-create-repository` reference
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

Restoring a snapshot using the :ref:`ref-restore-snapshot` statement.::

    cr> RESTORE SNAPSHOT where_my_snapshots_go.snapshot2 TABLE quotes WITH (wait_for_completion=true);
    RESTORE OK, 1 row affected (... sec)

In this case only the ``quotes`` table from snapshot
``where_my_snapshots_go.snapshot2`` is restored. Using ``ALL`` instead of
listing all tables restores the whole snapshot.

It's not possible to restore tables that exist in the current cluster::

    cr> RESTORE SNAPSHOT where_my_snapshots_go.snapshot2 TABLE quotes;
    SQLActionException[RelationAlreadyExists: Relation 'doc.quotes' already exists.]

Single partitions can be either imported into an existing partitioned table the
partition belongs to.

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

::

    cr> DROP TABLE parted_table;
    DROP OK, 1 row affected (... sec)

    cr> RESTORE SNAPSHOT where_my_snapshots_go.snapshot3 TABLE
    ...    parted_table PARTITION (date=0)
    ... WITH (wait_for_completion=true);
    RESTORE OK, 1 row affected (... sec)

Cleanup
-------

Dropping Snapshots
..................

Dropping a snapshot deletes all files inside the repository that are only
referenced by this snapshot. Due to its incremental nature this might be very
few files (e.g. for intermediate snapshots). Snapshots are dropped using the
:ref:`ref-drop-snapshot` command::

    cr> DROP SNAPSHOT where_my_snapshots_go.snapshot3;
    DROP OK, 1 row affected (... sec)

Dropping Repositories
.....................

.. Hidden: create repository

    cr> CREATE REPOSITORY "OldRepository" TYPE fs WITH (location='old_path');
    CREATE OK, 1 row affected (... sec)

If a repository is not needed anymore, it can be dropped using the
:ref:`ref-drop-repository` statement::

    cr> DROP REPOSITORY "OldRepository";
    DROP OK, 1 row affected (... sec)

This statement, like :ref:`ref-create-repository`, does not manipulate
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

.. _snapshot-restore_hfs-requirements:

Requirements for Using HDFS Repositories
----------------------------------------

CrateDB supports repositories of type
:ref:`ref-create-repository-types-hdfs` type by default, but required
`Hadoop`_ java client libraries are not included in any CrateDB distribution
and need to be added to CrateDB's hdfs plugin folder. By default this is
``$CRATE_HOME/plugins/es-repository-hdfs``

Because some libraries `Hadoop`_ depends on are also required (and so deployed)
by CrateDB, only the `Hadoop`_ libraries listed below must be copied into the
``$CRATE_HOME/plugins/es-repository-hdfs`` folder, other libraries will be
ignored::

 - apacheds-i18n-2.0.0-M15.jar
 - apacheds-kerberos-codec-2.0.0-M15.jar
 - api-asn1-api-1.0.0-M20.jar
 - api-util-1.0.0-M20.jar
 - avro-1.7.4.jar
 - commons-collections-3.2.1.jar
 - commons-compress-1.4.1.jar
 - commons-configuration-1.6.jar
 - commons-digester-1.8.jar
 - commons-httpclient-3.1.jar
 - commons-io-2.4.jar
 - commons-lang-2.6.jar
 - commons-net-3.1.jar
 - curator-client-2.7.1.jar
 - curator-framework-2.7.1.jar
 - curator-recipes-2.7.1.jar
 - gson-2.2.4.jar
 - hadoop-annotations-2.8.1.jar
 - hadoop-auth-2.8.1.jar
 - hadoop-client-2.8.1.jar
 - hadoop-common-2.8.1.jar
 - hadoop-hdfs-2.8.1.jar
 - hadoop-hdfs-client-2.8.1.jar
 - htrace-core-3.1.0-incubating.jar
 - jackson-core-asl-1.9.13.jar
 - jackson-mapper-asl-1.9.13.jar
 - jline-0.9.94.jar
 - jsp-api-2.1.jar
 - leveldbjni-all-1.8.jar
 - protobuf-java-2.5.0.jar
 - paranamer-2.3.jar
 - snappy-java-1.0.4.1.jar
 - servlet-api-2.5.jar
 - xercesImpl-2.9.1.jar
 - xmlenc-0.52.jar
 - xml-apis-1.3.04.jar
 - xz-1.0.jar
 - zookeeper-3.4.6.jar

.. NOTE::

   Only `Hadoop`_ version **2.x** is supported and as of writing this
   documentation, the latest stable `Hadoop (YARN)`_ version is **2.8.1**.
   Required libraries may differ for other versions.

.. _Hadoop: https://hadoop.apache.org/
.. _Hadoop (YARN): https://hadoop.apache.org/docs/r2.8.0/hadoop-yarn/hadoop-yarn-site/YARN.html
