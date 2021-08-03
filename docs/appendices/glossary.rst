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

B
-

.. _gloss-binary-operator:

**Binary operator**
    See :ref:`operation <gloss-operation>`.


.. _gloss-c:

C
-

.. _gloss-clustered-by-column:

**CLUSTERED BY column**
    See :ref:`routing column <gloss-routing-column>`.


.. _gloss-d:


.. _gloss-e:

E
-

.. _gloss-evaluation:

**Evaluation**
    See :ref:`expression <gloss-expression>`.

.. _gloss-expression:

**Expression**
    Any valid SQL that produces a value (e.g., :ref:`column references
    <sql-column-reference>`, :ref:`comparison operators
    <comparison-operators>`, and :ref:`functions <gloss-function>`) through a
    process known as *evaluation*.

    Contrary to a :ref:`statement <gloss-statement>`.

.. SEEALSO::

    :ref:`SQL: Value expressions <sql-value-expressions>`

    :ref:`Built-ins: Subquery expressions <sql_subquery_expressions>`

    :ref:`Data definition: Generation expressions
    <ddl-generated-columns-expressions>`

    :ref:`Scalar functions: Conditional functions and expressions
    <scalar-conditional-fn-exp>`

    :ref:`Aggregation: Aggregation expressions <aggregation-expressions>`


.. _gloss-f:

F
-

.. _gloss-function:

**Function**
    A token (e.g., :ref:`replace <scalar-replace>`) that takes zero or more
    arguments (e.g., three :ref:`strings <data-types-character-data>`),
    performs a specific task, and may return one or more values (e.g., a
    modified string). Functions that return more than one value are called
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
    A :ref:`data type <data-types>` that can have more than one value
    (e.g., :ref:`arrays <data-types-arrays>` and :ref:`objects
    <data-types-objects>`).

    Contrary to a :ref:`scalar <gloss-scalar>`.

    .. SEEALSO::

        :ref:`data-types-geo`

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
    in an SQL statement to manipulate one or more :ref:`expressions
    <gloss-expression>` and return a result (e.g., ``true`` or ``false``). This
    process is known as an *operation* and the expressions can be called
    *operands* or *arguments*.

    An operator that takes one operand is known as a *unary operator* and an
    operator that takes two is known as a *binary operator*.

    .. SEEALSO::

        :ref:`arithmetic`

        :ref:`comparison-operators`

        :ref:`sql_array_comparisons`


.. _gloss-p:

P
-

.. _gloss-partition-column:

**Partition column**
    A column used to :ref:`partition a table <partitioned-tables>`. Specified
    by the :ref:`PARTITIONED BY clause <sql-create-table-partitioned-by>`.

    Also known as a *PARTITIONED BY column* or *partitioned column*.

    A table may be partitioned by one or more columns:

    - If a table is partitioned by one column, a new partition is created for
      every unique value in that partition column

    - If a table is partitoned by multiple columns, a new partition is created
      for every unique combination of row values in those partition columns

    .. SEEALSO::

        :ref:`Data definition: Partitioned tables <partitioned-tables>`

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
    See :ref:`partition column <gloss-partition-column>`.

.. _gloss-partitioned-column:

**Partitioned column**
    See :ref:`partition column <gloss-partition-column>`.


.. _gloss-q:


.. _gloss-r:

R
-

.. _gloss-regular-expression:

**Regular expression**
    An :ref:`expression <gloss-expression>` used to search for patterns in a
    :ref:`string <data-type-varchar>`.

    .. SEEALSO::

        `Wikipedia: Regular expression`_

        :ref:`Data definition: Fulltext analyzers <sql-analyzer>`

        :ref:`Querying: Regular expressions <sql_dql_regexp>`

        :ref:`Scalar functions: Regular expressions <scalar-regexp>`

        :ref:`Table functions: regexp_matches <table-functions-regexp-matches>`

.. _gloss-routing-column:

**Routing column**
    Values in this column are used to compute a hash which is then used to
    route the corresponding row to a specific shard.

    Also known as the *CLUSTERED BY column*.

    All rows that have the same routing column row value are stored in the same
    shard.

    .. NOTE::

        The routing of rows to a specific shard is not the same as the routing
        of shards to a specific node (also known as :ref:`shard allocation
        <gloss-shard-allocation>`).

    .. SEEALSO::

        :ref:`Storage and consistency: Addressing documents
        <concept-addressing-documents>`

        :ref:`Sharding: Routing <sharding-routing>`

        :ref:`CREATE TABLE: CLUSTERED clause <sql-create-table-clustered>`


.. _gloss-s:

S
-

.. _gloss-scalar:

**Scalar**
    A :ref:`data type <data-types>` with a single value (e.g., :ref:`numbers
    <type-numeric>` and :ref:`strings <data-type-varchar>`).

    Contrary to a :ref:`nonscalar <gloss-nonscalar>`.

    .. SEEALSO::

        :ref:`data-types-primitive`

.. _gloss-shard-allocation:

**Shard allocation**
    The process by which CrateDB allocates shards to a specific nodes.

    .. NOTE::

        Shard allocation is sometimes referred to as *shard routing*, which is
        not to be confused with :ref:`row routing <gloss-routing-column>`.

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
    :ref:`replicating <ddl-replication>` a primary shard, when moving a shard
    to another node (i.e., when rebalancing the cluster), or during
    :ref:`snapshot restoration <snapshot-restore>`.

    A shard that is being recovered cannot be queried until the recovery
    process is complete.

    .. SEEALSO::

        :ref:`Cluster settings: Recovery <indices.recovery>`

        :ref:`System information: Checked node settings
        <sys-node-checks-settings>`

.. _gloss-shard-routing:

**Shard routing**
    See :ref:`shard allocation <gloss-shard-allocation>`.

.. _gloss-statement:

**Statement**
    Any valid SQL that serves as a database instruction (e.g., :ref:`CREATE
    TABLE <sql-create-table>`, :ref:`INSERT <sql-insert>`, and :ref:`SELECT
    <sql-select>`) instead of producing a value.

    Contrary to an :ref:`expression <gloss-expression>`.

    .. SEEALSO::

        :ref:`ddl`

        :ref:`dml`

        :ref:`dql`

        :ref:`sql-statements`

.. _gloss-subquery:

**Subquery**
    A :ref:`SELECT <sql-select>` statement used as a relation in the :ref:`FROM
    <sql-select-from>` clause of a parent ``SELECT`` statement.

    Also known as a *subselect*.

.. _gloss-subselect:

**Subselect**
    See :ref:`subquery <gloss-subquery>`.

.. _gloss-t:


.. _gloss-u:

U
-

.. _gloss-unary-operator:

**Unary operator**
    See :ref:`operation <gloss-operation>`.

.. _gloss-uncorrelated-subquery:

**Uncorrelated subquery**
    A :ref:`scalar subquery <sql-scalar-subquery>` that does not reference any
    relations (e.g., tables) in the parent :ref:`SELECT <sql-select>`
    statement.

    .. SEEALSO::

        :ref:`Built-ins: Subquery expressions <sql_subquery_expressions>`

.. _gloss-v:

V
-

.. _gloss-value-expression:

**Value expression**
    See :ref:`expression <gloss-expression>`.


.. _gloss-w:


.. _gloss-x:


.. _gloss-y:


.. _gloss-z:


.. _Wikipedia\: Regular expression: https://en.wikipedia.org/wiki/Regular_expression
