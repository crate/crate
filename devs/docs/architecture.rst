=====================
CrateDB Architecture
=====================

This document assumes that you are familiar with using CrateDB. If you have
not used CrateDB before, please check out our `tutorials`_.

CrateDB is a distributed SQL database. From a high-level perspective, the
following components can be found in each CrateDB node:

.. image:: architecture.svg

Components
-------------

SQL Engine
............

CrateDB's core consists of a SQL Engine which takes care of parsing SQL
statements and executing them in the cluster.

The engine is comprised of the following components:

1. **Parser**: Breaks down the SQL statement into its components and creates
   an AST (Abstract Syntax Tree).
2. **Analyzer**: Performs semantic processing of the statement including
   verification, type annotation, and normalization.
3. **Planner**: Builds an execution plan from the analyzed statement and
   optimizes it if possible. This is first done on a logical level
   (`LogicalPLan`), then broken down to the physical level (`ExecutionPlan`).
4. **Executor**: Executes the plan in the cluster and collects results.

Class entry point:

- `SqlParser`_
- `Analyzer`_
- `Planner`_
- `LogicalPlan`_ / `ExecutionPlan`_
- `NodeOperationTree`_
- `ExecutionPhasesTask`_
- `BatchIterator`_

Input
.....

CrateDB accepts input from a HTTP/REST interface as well as from a PostgreSQL
compatible wire protocol. The `admin interface`_, `Crash CLI`_ and some of the
connectors use the HTTP interface while the `JDBC client`_ uses the
`PostgreSQL wire protocol`_.

The PostgreSQL wire protocol support in CrateDB allows you to use CrateDB in
applications which were originally built to communicate with PostgreSQL. You
can also use PostgreSQL tools like `psql` with CrateDB.

Class entry points:

- `CrateRestMainAction`_
- `PostgresWireProtocol`_

Transport
..........

Transport refers to the communication between nodes in a CrateDB cluster.
CrateDB manages the cluster state and the transfer of data between cluster
nodes. This includes node discovery, partitioning of data, recovery, and
replication.


Class entry points:

- `TransportJobAction`_
- `TransportShardUpsertAction`_

Storage
........

CrateDB enables storing data in tables like you would in a traditional SQL
database with a strict schema. You can dynamically extend the schema or store
JSON objects inside columns which you can also query later. 

To distribute the data being stored, they are clustered by a column or
expression. Partitioned tables allow you to further distribute and split up
your data using other columns or expressions.

A table corresponds to an `Elasticsearch index`_. An index may have one or
multiple shards which represent the slices of a table which are stored across
the cluster. If a table is partitioned, each partition is a separate index.

The following table illustrates the relationship between table, partitions,
and indices. **Table1** has one index (=no partitions) with four shards. 
**Table2** has two indices (=two partitions) with two shards each. **Table3**
has one index which is split across three shards.

+------------+------------+------------+------------+------------+------+
|            | Table1     | Table2                  | Table3     | ...  |
+============+============+============+============+============+======+
| Partition  | Index      | Index1     | Index2     | Index      |      |
+------------+------------+------------+------------+------------+------+
| Shard1     | X          | X          | X          | X          |      |
+------------+------------+------------+------------+------------+------+
| Shard2     | X          | X          | X          | X          |      |
+------------+------------+------------+------------+------------+------+
| Shard3     | X          |            |            | X          |      |
+------------+------------+------------+------------+------------+------+
| Shard4     | X          |            |            |            |      |
+------------+------------+------------+------------+------------+------+
| ...        |            |            |            |            |      |
+------------+------------+------------+------------+------------+------+

To be able to retrieve data efficiently and perform aggregations, CrateDB uses
`Lucene`_ for indexing and storing data. Lucene itself stores a document with
the contents of each row. Retrieval is efficient because the fields of the
document are indexed. Aggregations can also be performed efficiently due to
Lucene's column store feature which stores columns separately in order to
quickly perform aggregations with them.

Class entry points:

- `LuceneQueryBuilder`_
- `LuceneBatchIterator`_


.. References:

.. _SqlParser: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql-parser/src/main/java/io/crate/sql/parser/SqlParser.java
.. _Analyzer: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/analyze/Analyzer.java
.. _Planner: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/planner/Planner.java
.. _LogicalPlan: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/planner/operators/LogicalPlan.java
.. _ExecutionPlan: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/planner/operators/LogicalPlan.java
.. _NodeOperationTree: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/execution/dsl/phases/NodeOperationTree.java
.. _ExecutionPhasesTask: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/execution/engine/ExecutionPhasesTask.java
.. _BatchIterator: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/dex/src/main/java/io/crate/data/BatchIterator.java


.. _CrateRestMainAction: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/http/src/main/java/io/crate/rest/CrateRestMainAction.java
.. _PostgresWireProtocol: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/protocols/postgres/PostgresWireProtocol.java

.. _TransportJobAction: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/execution/jobs/transport/TransportJobAction.java
.. _TransportShardUpsertAction: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/execution/dml/upsert/TransportShardUpsertAction.java

.. _LuceneQueryBuilder: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/lucene/LuceneQueryBuilder.java
.. _LuceneBatchIterator: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/execution/engine/collect/collectors/LuceneBatchIterator.java

.. _admin interface: https://crate.io/docs/crate/admin-ui/en/latest/
.. _Crash CLI: https://crate.io/docs/crate/crash/en/latest/
.. _Elasticsearch index: https://www.elastic.co/blog/what-is-an-elasticsearch-index
.. _JDBC client: https://crate.io/docs/jdbc/en/latest/
.. _Lucene: https://lucene.apache.org/
.. _PostgreSQL wire protocol: https://crate.io/docs/crate/reference/en/latest/interfaces/postgres.html
.. _tutorials: https://crate.io/docs/crate/tutorials/en/latest/