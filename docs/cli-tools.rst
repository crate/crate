.. highlight:: console

.. _cli:

=========
CLI tools
=========

CrateDB ships with *command-line interface* (CLI) tools (also referred to as
*executables*) in the ``bin`` directory.

If your working directory is :ref:`CRATE_HOME <conf-env-crate-home>`, you can
run an executable like this:

::

    sh$ bin/crate

Otherwise, you can run:

::

    sh$ <PATH_TO_CRATE_HOME>/bin/crate

Here, replace ``<PATH_TO_CRATE_HOME>`` with a path to :ref:`CRATE_HOME
<conf-env-crate-home>`.

Alternatively, if the CrateDB ``bin`` directory is on your
`PATH`_, you can run an executable directly:

::

    sh$ crate


.. _cli-crate:

``crate``
=========

The ``crate`` executable runs the CrateDB daemon.

.. SEEALSO::

    This section is a low-level command reference. For help installing CrateDB
    for the first time, check out the `CrateDB installation tutorial`_.
    Alternatively, consult the `deployment guide`_ for help running CrateDB in
    production.


.. _cli-crate-synopsis:

Synopsis
--------

::

   sh$ bin/crate [-dhvCDX] [-p <PID_FILE>]


.. _cli-crate-opts:

Options
-------

+------------------+---------------------------------------------------+
| Option           | Description                                       |
+==================+===================================================+
| ``-h``           | Print usage information                           |
+------------------+---------------------------------------------------+
| ``-v``           | Print version information                         |
+------------------+---------------------------------------------------+
| ``-C``           | Set a CrateDB :ref:`configuration <config>` value |
|                  | (overrides configuration file)                    |
+------------------+---------------------------------------------------+
| ``-D``           | Set a Java system property value                  |
+------------------+---------------------------------------------------+
| ``-X``           | Set a nonstandard java option                     |
+------------------+---------------------------------------------------+


.. _cli-crate-signals:

Signal handling
---------------

The CrateDB process can handle the following signals.

+-----------+------------------------+
| Signal    | Description            |
+===========+========================+
| ``TERM``  | Terminates the process |
+-----------+------------------------+
| ``INT``   | Terminates the process |
+-----------+------------------------+

.. TIP::

    The ``TERM`` signal stops CrateDB immediately. As a result, pending
    requests may fail. To ensure that CrateDB finishes handling pending
    requests before the node is stopped, you can, instead, perform a `graceful
    stop`_ with the :ref:`DECOMMISSION <alter_cluster_decommission>` statement.


.. _cli-crate-example:

Example
-------

The simplest way to start a CrateDB instance is to invoke ``crate`` without
parameters:

::

    sh$ bin/crate

This command starts the process in the foreground.

It's helpful to write the process ID (*PID*) to a PID file with the
use of ``echo $!``. So you execute the following:

::

    sh$ bin/crate & echo $! > "/tmp/crate.pid"

To stop the process, send a ``TERM`` signal using the PID file, like so:

::

  sh$ kill -TERM `cat /tmp/crate.pid`


.. _cli-crate-node:

``crate-node``
==============

The ``crate-node`` executable is a tool that can help you:

- `Repurpose a node`_
- `Perform an unsafe cluster bootstrap`_
- `Detach a node from its cluster`_

.. SEEALSO::

    This section is a low-level command reference. For help using
    ``crate-node``, consult the `troubleshooting guide`_.


.. _cli-crate-node-synopsis:

Synopsis
--------

::

   sh$ bin/crate-node repurpose|unsafe-bootstrap|detach-cluster
   [--ordinal <INT>] [-C<key>=<value>]
   [-h, --help] ([-s, --silent] | [-v, --verbose])


.. _cli-crate-node-commands:

Commands
--------

+----------------------+------------------------------------------------------+
| Command              | Description                                          |
+======================+======================================================+
| ``repurpose``        | Clean up any unnecessary data on disk after changing |
|                      | the role of a node.                                  |
+----------------------+------------------------------------------------------+
| ``unsafe-bootstrap`` | Force the election of a master and create a new      |
|                      | cluster in the event of losing the majority of       |
|                      | master-eligible nodes.                               |
+----------------------+------------------------------------------------------+
| ``detach-cluster``   | Detach a node from a cluster so that it can join a   |
|                      | new one.                                             |
+----------------------+------------------------------------------------------+
| ``remove-settings``  | Remove persistent settings from the cluster state in |
|                      | case where it contains incompatible settings that    |
|                      | prevent the cluster from forming.                    |
+----------------------+------------------------------------------------------+
| ``override-version`` | Override the version number stored in the data path  |
|                      | to be able to force a node to startup even when the  |
|                      | node version is not compatible with the meta data.   |
+----------------------+------------------------------------------------------+
| ``fix-metadata``     | Fix corrupted metadata after running table swap      |
|                      | like: ALTER CLUSTER SWAP TABLE "schema"."table" TO   |
|                      | "schema.table";                                      |
+----------------------+------------------------------------------------------+


.. _cli-crate-node-options:

Options
-------

+---------------------+-----------------------------------------------------+
| Option              | Description                                         |
+=====================+=====================================================+
| ``--ordinal <INT>`` | Specify which node to target if there is more than  |
|                     | one node sharing a data path                        |
+---------------------+-----------------------------------------------------+
| ``-C``              | Set a CrateDB :ref:`configuration <config>` value   |
|                     | (overrides configuration file)                      |
+---------------------+-----------------------------------------------------+
| ``-h, --help``      | Return all of the command parameters                |
+---------------------+-----------------------------------------------------+
| ``-s, --silent``    | Show minimal output                                 |
+---------------------+-----------------------------------------------------+
| ``-v, --verbose``   | Shows verbose output                                |
+---------------------+-----------------------------------------------------+

.. _cli-crate-shard:

``crate-shard``
===============

In some exceptionally cases, a CrateDB shard can become corrupted and the
related table isn't accessible. The ``crate-shard`` executable helps to remove
corrupted parts of a shard which cannot be automatically recovered from a good
copy (replica) or restored from a backup.

.. WARNING::

    You will lose data if you use ``crate-shard``. Use this tool only as a last
    resort when you have no other options to recover the data.


.. _cli-crate-shard-synopsis:

Synopsis
--------

::

   sh$ bin/crate-shard remove-corrupted-data
   ([--table <FullTableFQN>] (-Ppartitioned_column=value [ , ...]) [--shard-id <ShardId>] | [--dir <TablePath>])
   [--truncate-clean-translog]
   [-C<key>=<value>]
   [-h, --help] ([-s, --silent] | [-v, --verbose])


.. _cli-crate-shard-description:

Description
-----------

When CratedB detects that a shard’s data is corrupted, it fails that shard copy
and refuses to use it. Under normal conditions, the shard is automatically
recovered from another copy. If no good copy of the shard is available and you
cannot restore one from a snapshot, you can use ``crate-shard`` to remove the
corrupted data and restore access to any remaining data in unaffected segments.

.. WARNING::

    Stop the CrateDB node before using ``crate-shard``.

There are two ways to specify the shard that you want to fix:

- With the ``--table`` option, you can specify the fully qualified name of the
  table that contains the shard. If you want to fix a shard of a partitioned
  table, you must also specify the partition values with the ``-P`` option. If
  the table is partitioned by multiple columns, repeat the ``-P`` option for
  each partition column/value pair. Additionally, the shard ID using to target
  a specific shard copy must be specified using the ``--shard-id`` option.
- With the ``--dir`` option, you can specify the path to the table data on disk.
  The path must be the one that contains the shard data, for example
  ``/data/crate/data/0/nodes/0/indices/yxZabc/0``.

.. _cli-crate-shard-options:

Options
-------

+------------------------------+-----------------------------------------------------+
| Option                       | Description                                         |
+==============================+=====================================================+
| ``--table <fqn_table_name>`` | Specifies the full-qualified table name incl. the   |
|                              | schema name.                                        |
+------------------------------+-----------------------------------------------------+
| ``-P<parted_col>=<value>``   | Specifies the partition by partition columns.       |
|                              | Must be repeated for every partition column.        |
|                              | Optional, only needed for partitioned tables.       |
+------------------------------+-----------------------------------------------------+
| ``--shard-id <INT>``         | Specifies shard ID.                                 |
+------------------------------+-----------------------------------------------------+
| ``--dir <shard_path>``       | Specifies the path on disk of the shard to fix.     |
+------------------------------+-----------------------------------------------------+
| ``-C``                       | Set a CrateDB :ref:`configuration <config>` value   |
|                              | (overrides configuration file)                      |
+------------------------------+-----------------------------------------------------+
| ``-h, --help``               | Return all of the command parameters                |
+------------------------------+-----------------------------------------------------+
| ``-s, --silent``             | Show minimal output                                 |
+------------------------------+-----------------------------------------------------+
| ``-v, --verbose``            | Shows verbose output                                |
+------------------------------+-----------------------------------------------------+


.. _deployment guide: https://cratedb.com/docs/crate/howtos/en/latest/deployment/index.html
.. _Detach a node from its cluster: https://cratedb.com/docs/crate/howtos/en/latest/best-practices/crate-node.html#detach-a-node-from-its-cluster
.. _CrateDB installation tutorial: https://cratedb.com/docs/crate/tutorials/en/latest/install.html
.. _graceful stop: https://cratedb.com/docs/guide/admin/upgrade/rolling.html#graceful-stop
.. _PATH: https://servicenow.iu.edu/kb?id=kb_article_view&sysparm_article=KB0022615
.. _Perform an unsafe cluster bootstrap: https://cratedb.com/docs/crate/howtos/en/latest/best-practices/crate-node.html#perform-an-unsafe-cluster-bootstrap
.. _Repurpose a node: https://cratedb.com/docs/crate/howtos/en/latest/best-practices/crate-node.html#repurpose-a-node
.. _Rolling Upgrade: https://cratedb.com/docs/crate/howtos/en/latest/admin/rolling-upgrade.html
.. _troubleshooting guide: https://cratedb.com/docs/crate/howtos/en/latest/best-practices/crate-node.html
.. _Troubleshooting with crate-node CLI: `troubleshooting guide`_
