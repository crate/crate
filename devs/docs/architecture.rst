=====================
CrateDB Architecture
=====================

This document assumes that you are familiar with using CrateDB. If you haven't
used CrateDB before, please check out the `Getting Started
<https://crate.io/docs/crate/getting-started/en/latest/>`_ section of the user
documentation.

CrateDB is a distributed SQL database. From a high-level perspective, the
following components can be found in each node of CrateDB:

.. image:: ../resources/architecture.svg

Components
-------------

SQL Engine
............

CrateDB's heart consists of a SQL Engine which takes care of parsing SQL
statements and executing them in the cluster.

The engine is comprised of the following components:

1. Parser: Breaks down the SQL statement into its components and creates the AST
   (Abstract Syntax Tree).
2. Analyzer: Performs semantic processing of the statement including
   verification, type annotation, and normalization.
3. Planner: Builds an execution plan from the analyzed statement and optimizes
   it if possible. This is first done on a logical level (`LogicalPLan`), then
   broken down to the physical level (`ExecutionPlan`).
4. Executor: Executes the plan in the cluster and collects results.

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

CrateDB accepts input from an HTTP/REST interface as well as from a Postgres
compatible protocol. The Admin UI, crash and some of the connectors use the HTTP
interface whereas the JDBC client uses the Postgres protocol.

An advantage of the Postgres protocol in CrateDB is that you can use it also for
applications which were originally built to communicate with Postgres. You may
also use Postgres tools like `psql` with CrateDB.

The enterprise version of CrateDB also contains support for ingesting data with
the MQTT protocol.

Class entry points:

- `CrateRestMainAction`_
- `PostgresWireProtocol`_
- `MqttNettyHandler`_

Transport
..........

Transport denotes the communication between nodes in a CrateDB cluster. CrateDB
uses Elasticsearch to manage the cluster state and to transfer data between
cluster nodes. This includes node discovery, partitioning of data, recovery, and
replication.

CrateDB uses the internal API of Elasticsearch to build its own transport logic
on top of Elasticsearch. This lets us control how data is stored, how
replication is performed, and when indices are updated or built.

Class entry points:

- `TransportJobAction`_
- `TransportShardUpsertAction`_

Storage
........

CrateDB enables you to store your data in tables like you would in a traditional
SQL database with a strict schema. Additionally, you can dynamically extend the
schema or store JSON objects inside columns which you can also query. Data is
clustered by a column or expression to distribute the data. Partitioned tables
allow you to further distribute and split up your data using other columns or
expressions.

A table corresponds to an Elasticsearch index. An index may have one or multiple
shards which represent the slices of a table which are stored across the
cluster. If a table is partitioned, each partition is a separate index.

The following table illustrates the relationship between table, partitions, and
indices. Table1 has one index (=no partitions) with four shards. Table2 has two
indices (=two partitions) with two shards each. Table3 has one index which is
split across three shards.

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
Lucene for indexing and storing data. Lucene itself stores a document with the
contents of each row. Retrieval is efficient because the fields of the document
are indexed. Aggregations can also be performed efficiently due to Lucene's
column store feature which stores columns separately to quickly perform
aggregations with them.

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
.. _MqttNettyHandler: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/enterprise/mqtt/src/main/java/io/crate/mqtt/netty/MqttNettyHandler.java

.. _TransportJobAction: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/execution/jobs/transport/TransportJobAction.java
.. _TransportShardUpsertAction: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/execution/dml/upsert/TransportShardUpsertAction.java

.. _LuceneQueryBuilder: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/lucene/LuceneQueryBuilder.java
.. _LuceneBatchIterator: https://github.com/crate/crate/blob/98e5fe3d911c8ffdf605c7259f738b24ef1c4085/sql/src/main/java/io/crate/execution/engine/collect/collectors/LuceneBatchIterator.java
