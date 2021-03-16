.. highlight:: psql

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


F
-

.. _gloss-function:

**Function**
    A token (e.g., :ref:`replace <scalar-replace>`) that takes zero or more
    arguments (e.g., three :ref:`strings <character-data-types>`), performs a
    specific task, and may return one or more values (e.g., a modified
    string). Functions that return more than one value are called
    :ref:`multi-valued functions <gloss-multi-valued-functions>`.

    Functions may be :ref:`called <sql-function-call>` in an SQL statement,
    like so::

        cr> SELECT replace('Hello world!', 'world', 'friend') as result;
        +---------------+
        | result        |
        +---------------+
        | Hello friend! |
        +---------------+
        SELECT 1 row in set (... sec)

    .. SEEALSO::

        :ref:`scalar-functions`

        :ref:`aggregation-functions`

        :ref:`table-functions`

        :ref:`window-functions`

        :ref:`user-defined-functions`

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

.. _gloss-multi-valued-functions:

**Multi-valued function**
    A :ref:`function <gloss-function>` that returns two or more values.

    .. SEEALSO::

        :ref:`table-functions`

        :ref:`window-functions`

.. _gloss-n:

N
-

.. _gloss-nonscalar:

**Nonscalar**
    A :ref:`data type <data-types>` that can have multiple component values
    (e.g., :ref:`arrays <data-type-array>` and :ref:`objects
    <object_data_type>`).

    Contrary to a :ref:`scalar <gloss-scalar>`.

    .. SEEALSO::

        :ref:`sql_ddl_datatypes_geographic`

        :ref:`data-types-container`


.. _gloss-o:

O
-

.. _gloss-operand:

**Operand**
    See :ref:`operator <gloss-operator>`.

.. _gloss-operation:

**Operation**
    See :ref:`operator <gloss-operator>`.

.. _gloss-operator:

**Operator**
    A reserved keyword (e.g., :ref:`IN <sql_in_array_comparison>`) or sequence
    of symbols (e.g., :ref:`>= <comparison-operators-basic>`) that can be used
    in an SQL statement to manipulate one or more expressions and return a
    result (e.g., ``true`` or ``false``). This process is known as an
    *operation* and the expressions can be called *operands* or *arguments*.

    An operator that takes one operand is known as a *unary operator* and an
    operator that takes two is known as a *binary operator*.

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

        :ref:`Sharding: Routing <sharding-routing>`

        :ref:`CREATE TABLE: CLUSTERED clause <sql-create-table-clustered>`


.. _gloss-s:

S
-

.. _gloss-scalar:

**Scalar**
    A :ref:`data type <data-types>` with a single value (e.g., :ref:`numbers
    <data-type-numeric>` and :ref:`strings <data-type-varchar>`).

    Contrary to a :ref:`nonscalar <gloss-nonscalar>`.

    .. SEEALSO::

        :ref:`sql_ddl_datatypes_primitives`


.. _gloss-shard-allocation:

**Shard allocation**
    The process by which CrateDB allocates shards to a specific nodes.

    .. NOTE::

        Shard allocation is sometimes referred to as :ref:`shard routing
        <gloss-shard-routing>`, which is not to be confused with :ref:`row
        routing <gloss-routing-column>`.

    .. SEEALSO::

        :ref:`ddl_shard_allocation`

        :ref:`Cluster configuration: Routing allocation <conf_routing>`

        :ref:`Sharding: Number of shards <sharding-number>`

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

.. _gloss-shard-routing:

**Shard routing**
    Properly known as :ref:`shard allocation <gloss-shard-allocation>`.


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
