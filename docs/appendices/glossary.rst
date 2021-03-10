.. _appendix-glossary:

========
Glossary
========

This glossary defines key terms used in the CrateDB reference manual.

.. rubric:: Table of contents

.. contents::
   :local:


Terms
=====


.. _gloss-a:


.. _gloss-b:


.. _gloss-c:

C
-

.. _gloss-clustered-by-column:

**CLUSTERED BY column**
    Better known as a :ref:`routing column <gloss-routing-column>`.


.. _gloss-d:


.. _gloss-e:


.. _gloss-f:


.. _gloss-g:


.. _gloss-h:


.. _gloss-i:


.. _gloss-j:


.. _gloss-k:


.. _gloss-l:


.. _gloss-m:

M
-

.. _gloss-metadata-gateway:

**Metadata gateway**
    Persists cluster metadata on disk every time the metadata changes. This
    data is stored persistently across full cluster restarts and recovered
    after nodes are started again.

    .. SEEALSO::

         :ref:`Cluster configuration: Metadata gateway <metadata_gateway>`


.. _gloss-n:


.. _gloss-o:

O
-

.. _gloss-operator:

**Operator**
    A reserved keyword (e.g., :ref:`IN <sql_in_array_comparison>`) or sequence
    of symbols (e.g., :ref:`>= <comparison-operators-basic>`) that can be used
    in an SQL statement to manipulate one or more expressions and returns a
    result (e.g., ``true`` or ``false``). This process is known as an
    *operation* and the expressions can be called operands or arguments.

    .. SEEALSO::

        :ref:`arithmetic`

        :ref:`comparison-operators`

        :ref:`sql_array_comparisons`

        :ref:`sql_subquery_expressions`


.. _gloss-p:

P
-

.. _gloss-partition-column:

**Partition column**
    A column used to :ref:`partition a table <partitioned-tables>`. Specified
    by the :ref:`PARTITIONED BY clause <sql-create-table-partitioned-by>`.

    Also known as a :ref:`PARTITIONED BY column <gloss-partitioned-by-column>`
    or :ref:`partitioned column <gloss-partitioned-column>`.

    A table may be partitioned by one or more columns:

    - If a table is partitioned by one column, a new partition is created for
      every unique value in that partition column

    - If a table is partitoned by multiple columns, a new partition is created
      for every unique combination of row values in those partition columns

    .. SEEALSO::

        :ref:`partitioned-tables`

        :ref:`Generated columns: Partitioning
        <ddl-generated-columns-partitioning>`

        :ref:`CREATE TABLE: PARTITIONED BY clause
        <sql-create-table-partitioned-by>`

        :ref:`ALTER TABLE: PARTITION clause <sql-alter-table-partition>`

        :ref:`REFRESH: PARTITION clause <sql-refresh-partition>`

        :ref:`OPTIMIZE: PARTITION clause <sql-optimize-partition>`

        :ref:`COPY TO: PARTITION clause <sql-copy-to-partition>`

        :ref:`COPY FROM: PARTITION clause <sql-copy-from-partition>`

        :ref:`CREATE SNAPSHOT: PARTITION clause
        <sql-create-snapshot-partition>`

        :ref:`RESTORE SNAPSHOT: PARTITION clause
        <sql-restore-snapshot-partition>`

.. _gloss-partitioned-by-column:

**PARTITIONED BY column**
    Better known as a :ref:`partition column <gloss-partition-column>`.

.. _gloss-partitioned-column:

**Partitioned column**
    Better known as a :ref:`partition column <gloss-partition-column>`.


.. _gloss-q:


.. _gloss-r:

R
-

.. _gloss-routing-column:

**Routing column**
    Values in this column are used to compute a hash which is then used to
    route the corresponding row to a specific shard.

    Also known as the :ref:`CLUSTERED BY column <gloss-clustered-by-column>`.

    All rows that have the same routing column row value are stored in the same
    shard.

    .. NOTE::

        The routing of rows to a specific shard is not the same as the routing
        of shards to a specific node (also known as :ref:`shard allocation
        <gloss-shard-allocation>`).

    .. SEEALSO::

        :ref:`Storage and consistency: Addressing documents
        <concepts_addressing_documents>`

        :ref:`Sharding: Routing <routing>`

        :ref:`CREATE TABLE: CLUSTERED clause <sql-create-table-clustered>`


.. _gloss-s:

S
-

.. _gloss-shard-allocation:

**Shard allocation**
    The process by which CrateDB allocates shards to a specific nodes.

    .. NOTE::

        Shard allocation is also known as *shard routing*, which is not to be
        confused with :ref:`row routing <gloss-routing-column>`.

    .. SEEALSO::

        :ref:`ddl_shard_allocation`

        :ref:`Cluster configuration: Routing allocation <conf_routing>`

        :ref:`Sharding: Number of shards <number-of-shards>`

        :ref:`Altering tables: Changing the number of shards
        <alter-shard-number>`

        :ref:`Altering tables: Reroute shards <ddl_reroute_shards>`

.. _gloss-shard-recovery:

**Shard recovery**
    The process by which CrateDB synchronizes a replica shard from a primary
    shard.

    Shard recovery can happen during node startup, after node failure, when
    :ref:`replicating <replication>` a primary shard, when moving a shard to
    another node (i.e., when rebalancing the cluster), or during :ref:`snapshot
    restoration <snapshot-restore>`.

    A shard that is being recovered cannot be queried until the recovery
    process is complete.

    .. SEEALSO::

        :ref:`Cluster settings: Recovery <indices.recovery>`

        :ref:`System information: Checked node settings
        <sys-node-checks-settings>`


.. _gloss-t:


.. _gloss-u:

U
-

.. _gloss-uncorrelated-subquery:

**Uncorrelated subquery**
    A subquery that does not reference any relations in a parent statement.


.. _gloss-v:


.. _gloss-w:


.. _gloss-x:


.. _gloss-y:


.. _gloss-z:
