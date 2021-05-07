/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.integrationtests;

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.metadata.PartitionName;
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_COLUMN;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.TestingHelpers.resolveCanonicalString;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(numClientNodes = 0, numDataNodes = 2, supportsDedicatedMasters = false)
public class SysShardsTest extends SQLIntegrationTestCase {

    @Before
    public void initTestData() throws Exception {
        Setup setup = new Setup(sqlExecutor);
        setup.groupBySetup();
        execute("create table quotes (id integer primary key, quote string) with (number_of_replicas=1)");
        execute("create blob table blobs clustered into 5 shards with (number_of_replicas=1)");
        ensureGreen();
    }

    @Test
    public void testPathAndBlobPath() throws Exception {
        Path blobs = createTempDir("blobs");
        execute("create table t1 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        execute("create blob table b1 clustered into 1 shards with (number_of_replicas = 0)");
        execute("create blob table b2 " +
                "clustered into 1 shards with (number_of_replicas = 0, blobs_path = '" + blobs.toString() + "')");
        ensureYellow();

        execute("select path, blob_path from sys.shards where table_name in ('t1', 'b1', 'b2') " +
                "order by table_name asc");
        // b1
        // path + /blobs == blob_path without custom blob path
        assertThat(response.rows()[0][0] + resolveCanonicalString("/blobs"), is(response.rows()[0][1]));

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        Metadata metadata = clusterService.state().getMetadata();
        IndexMetadata index = metadata.index(".blob_b2");
        String indexUUID = index.getIndexUUID();

        // b2
        String b2Path = (String) response.rows()[1][0];
        assertThat(b2Path, containsString(resolveCanonicalString("/nodes/")));
        assertThat(b2Path, endsWith(resolveCanonicalString("/indices/" + indexUUID + "/0")));

        String b2BlobPath = (String) response.rows()[1][1];
        assertThat(b2BlobPath, containsString(resolveCanonicalString("/nodes/")));
        assertThat(b2BlobPath, endsWith(resolveCanonicalString("/indices/" + indexUUID + "/0/blobs")));
        // t1
        assertThat(response.rows()[2][1], nullValue());
    }

    @Test
    public void testSelectGroupByWhereTable() throws Exception {
        SQLResponse response = execute("" +
                                       "select count(*), num_docs from sys.shards where table_name = 'characters' " +
                                       "group by num_docs order by count(*)");
        assertThat(response.rowCount(), greaterThan(0L));
    }

    @Test
    public void testSelectGroupByAllTables() throws Exception {
        SQLResponse response = execute("select count(*), table_name from sys.shards " +
                                       "group by table_name order by table_name");
        assertEquals(3L, response.rowCount());
        assertEquals(10L, response.rows()[0][0]);
        assertEquals("blobs", response.rows()[0][1]);
        assertEquals("characters", response.rows()[1][1]);
        assertEquals("quotes", response.rows()[2][1]);
    }

    @Test
    public void testGroupByWithLimitUnassignedShards() throws Exception {
        try {
            execute("create table t (id int, name string) with (number_of_replicas=2, \"write.wait_for_active_shards\"=1)");
            ensureYellow();

            SQLResponse response = execute("select sum(num_docs), table_name, sum(num_docs) from sys.shards group by table_name order by table_name desc limit 1000");
            assertThat(response.rowCount(), is(4L));
            assertThat(TestingHelpers.printedTable(response.rows()),
                is("0| t| 0\n" +
                   "0| quotes| 0\n" +
                   "14| characters| 14\n" +
                   "0| blobs| 0\n"));
        } finally {
            execute("drop table t");
        }
    }

    @Test
    public void testSelectGroupByWhereNotLike() throws Exception {
        SQLResponse response = execute("select count(*), table_name from sys.shards " +
                                       "where table_name not like 'my_table%' group by table_name order by table_name");
        assertEquals(3L, response.rowCount());
        assertEquals(10L, response.rows()[0][0]);
        assertEquals("blobs", response.rows()[0][1]);
        assertEquals(8L, response.rows()[1][0]);
        assertEquals("characters", response.rows()[1][1]);
        assertEquals(8L, response.rows()[2][0]);
        assertEquals("quotes", response.rows()[2][1]);
    }

    @Test
    public void testSelectWhereTable() throws Exception {
        SQLResponse response = execute(
            "select id, size from sys.shards " +
            "where table_name = 'characters'");
        assertEquals(8L, response.rowCount());
    }

    @Test
    public void testSelectStarWhereTable() throws Exception {
        SQLResponse response = execute(
            "select * from sys.shards where table_name = 'characters'");
        assertEquals(8L, response.rowCount());
    }

    @Test
    public void testSelectStarAllTables() throws Exception {
        SQLResponse response = execute("select * from sys.shards");
        assertEquals(26L, response.rowCount());
        assertEquals(20, response.cols().length);
        assertThat(response.cols(), arrayContaining(
            "blob_path",
            "closed",
            "id",
            "min_lucene_version",
            "node",
            "num_docs",
            "orphan_partition",
            "partition_ident",
            "path",
            "primary",
            "recovery",
            "relocating_node",
            "retention_leases",
            "routing_state",
            "schema_name",
            "seq_no_stats",
            "size",
            "state",
            "table_name",
            "translog_stats"));
    }

    @Test
    public void testSelectStarLike() throws Exception {
        SQLResponse response = execute(
            "select * from sys.shards where table_name like 'charact%'");
        assertEquals(8L, response.rowCount());
    }

    @Test
    public void testSelectStarNotLike() throws Exception {
        SQLResponse response = execute(
            "select * from sys.shards where table_name not like 'quotes%'");
        assertEquals(18L, response.rowCount());
    }

    @Test
    public void testSelectStarIn() throws Exception {
        SQLResponse response = execute(
            "select * from sys.shards where table_name in ('characters')");
        assertEquals(8L, response.rowCount());
    }

    @Test
    public void test_translog_stats_can_be_retrieved() {
        execute("SELECT translog_stats, translog_stats['size'] FROM sys.shards " +
                "WHERE id = 0 AND \"primary\" = true AND table_name = 'characters'");
        Object[] resultRow = response.rows()[0];
        Map<String, Object> translogStats = (Map<String, Object>) resultRow[0];
        assertThat(((Number) translogStats.get("size")).longValue(), is(resultRow[1]));
        assertThat(((Number) translogStats.get("uncommitted_size")).longValue(), greaterThanOrEqualTo(0L));
        assertThat(((Number) translogStats.get("number_of_operations")).longValue(), greaterThanOrEqualTo(0L));
        assertThat(((Number) translogStats.get("uncommitted_operations")).longValue(), greaterThanOrEqualTo(0L));
    }

    @Test
    public void testSelectStarMatch() throws Exception {
        assertThrows(() -> execute("select * from sys.shards where match(table_name, 'characters')"),
                     isSQLError(is("Cannot use MATCH on system tables"), INTERNAL_ERROR, BAD_REQUEST, 4004));
    }

    @Test
    public void testSelectOrderBy() throws Exception {
        SQLResponse response = execute("select table_name, min_lucene_version, * " +
                                       "from sys.shards order by table_name");
        assertEquals(26L, response.rowCount());
        List<String> tableNames = Arrays.asList("blobs", "characters", "quotes");
        for (Object[] row : response.rows()) {
            assertThat(tableNames.contains(row[0]), is(true));
            assertThat(row[1], is(Version.LATEST.toString()));
        }
    }

    @Test
    public void testSelectGreaterThan() throws Exception {
        SQLResponse response = execute("select * from sys.shards where num_docs > 0");
        assertThat(response.rowCount(), greaterThan(0L));
    }

    @Test
    public void testSelectWhereBoolean() throws Exception {
        SQLResponse response = execute("select * from sys.shards where \"primary\" = false");
        assertEquals(13L, response.rowCount());
    }

    @Test
    public void testSelectGlobalAggregates() throws Exception {
        SQLResponse response = execute(
            "select sum(size), min(size), max(size), avg(size) from sys.shards");
        assertEquals(1L, response.rowCount());
        assertEquals(4, response.rows()[0].length);
        assertNotNull(response.rows()[0][0]);
        assertNotNull(response.rows()[0][1]);
        assertNotNull(response.rows()[0][2]);
        assertNotNull(response.rows()[0][3]);
    }

    @Test
    public void testSelectGlobalCount() throws Exception {
        SQLResponse response = execute("select count(*) from sys.shards");
        assertEquals(1L, response.rowCount());
        assertEquals(26L, response.rows()[0][0]);
    }

    @Test
    public void testSelectGlobalCountAndOthers() throws Exception {
        SQLResponse response = execute("select count(*), max(table_name) from sys.shards");
        assertEquals(1L, response.rowCount());
        assertEquals(26L, response.rows()[0][0]);
        assertEquals("quotes", response.rows()[0][1]);
    }

    @Test
    public void testGroupByUnknownResultColumn() throws Exception {
        assertThrows(() -> execute("select lol from sys.shards group by table_name"),
                     isSQLError(is("Column lol unknown"), UNDEFINED_COLUMN, NOT_FOUND, 4043));
    }

    @Test
    public void testGroupByUnknownGroupByColumn() throws Exception {
        assertThrows(() -> execute("select max(num_docs) from sys.shards group by lol"),
                     isSQLError(is("Column lol unknown"), UNDEFINED_COLUMN, NOT_FOUND, 4043));
    }

    @Test
    public void testGroupByUnknownOrderBy() throws Exception {
        assertThrows(() -> execute(
            "select sum(num_docs), table_name from sys.shards group by table_name order by lol"),
                     isSQLError(is("Column lol unknown"), UNDEFINED_COLUMN, NOT_FOUND, 4043));
    }

    @Test
    public void testGroupByUnknownWhere() throws Exception {
        assertThrows(() -> execute(
            "select sum(num_docs), table_name from sys.shards where lol='funky' group by table_name"),
                     isSQLError(is("Column lol unknown"), UNDEFINED_COLUMN, NOT_FOUND, 4043));
        ;
    }

    @Test
    public void testGlobalAggregateUnknownWhere() throws Exception {
        assertThrows(() -> execute(
            "select sum(num_docs) from sys.shards where lol='funky'"),
                     isSQLError(is("Column lol unknown"), UNDEFINED_COLUMN, NOT_FOUND, 4043));
    }

    @Test
    public void testSelectShardIdFromSysNodes() throws Exception {
        assertThrows(() -> execute(
            "select sys.shards.id from sys.nodes"),
                     isSQLError(is("Relation 'sys.shards' unknown"), UNDEFINED_TABLE, NOT_FOUND, 4041));
    }

    @Test
    public void testSelectWithOrderByColumnNotInOutputs() throws Exception {
        // regression test... query failed with ArrayOutOfBoundsException due to inputColumn mangling in planner
        SQLResponse response = execute("select id from sys.shards order by table_name limit 1");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], instanceOf(Integer.class));
    }

    @Test
    public void testSelectGroupByHaving() throws Exception {
        SQLResponse response = execute("select count(*) " +
                                       "from sys.shards " +
                                       "group by table_name " +
                                       "having table_name = 'quotes'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("8\n"));
    }

    @Test
    public void testSelectNodeSysExpression() throws Exception {
        SQLResponse response = execute(
            "select node, node['name'], id from sys.shards order by node['name'], id  limit 1");
        assertEquals(1L, response.rowCount());
        Map<String, Object> fullNode = (Map<String, Object>) response.rows()[0][0];
        String nodeName = response.rows()[0][1].toString();
        assertEquals("node_s0", nodeName);
        assertEquals("node_s0", fullNode.get("name"));
    }

    @Test
    public void testOrphanedPartitionExpression() throws Exception {
        try {
            execute("create table c.orphan_test (id int primary key, p string primary key) " +
                    "partitioned by (p) " +
                    "clustered into 1 shards " +
                    "with (number_of_replicas = 0)"
            );
            execute("insert into c.orphan_test (id, p) values (1, 'foo')");
            ensureYellow();

            SQLResponse response = execute(
                "select orphan_partition from sys.shards where table_name = 'orphan_test'");
            assertThat(TestingHelpers.printedTable(response.rows()), is("false\n"));

            client().admin().indices()
                .prepareDeleteTemplate(PartitionName.templateName("c", "orphan_test"))
                .execute().get(1, TimeUnit.SECONDS);

            response = execute(
                "select orphan_partition from sys.shards where table_name = 'orphan_test'");
            assertThat(TestingHelpers.printedTable(response.rows()), is("true\n"));
        } finally {
            execute("drop table c.orphan_test");
        }
    }

    @Test
    public void testSelectNodeSysExpressionWithUnassignedShards() throws Exception {
        try {
            execute("create table users (id integer primary key, name string) " +
                    "clustered into 5 shards with (number_of_replicas=2, \"write.wait_for_active_shards\"=1)");
            ensureYellow();
            SQLResponse response = execute(
                "select node['name'], id from sys.shards where table_name = 'users' order by node['name'] nulls last"
            );
            String nodeName = response.rows()[0][0].toString();
            assertEquals("node_s0", nodeName);
            assertThat(response.rows()[12][0], is(nullValue()));
        } finally {
            execute("drop table users");
        }
    }

    @Test
    public void testSelectRecoveryExpression() throws Exception {
        SQLResponse response = execute("select recovery, " +
                                       "recovery['files'], recovery['files']['used'], recovery['files']['reused'], recovery['files']['recovered'], " +
                                       "recovery['size'], recovery['size']['used'], recovery['size']['reused'], recovery['size']['recovered'] " +
                                       "from sys.shards");
        for (Object[] row : response.rows()) {
            Map recovery = (Map) row[0];
            Map<String, Integer> files = (Map<String, Integer>) row[1];
            assertThat(((Map<String, Integer>) recovery.get("files")).entrySet(), equalTo(files.entrySet()));
            Map<String, Long> size = (Map<String, Long>) row[5];
            assertThat(((Map<String, Long>) recovery.get("size")).entrySet(), equalTo(size.entrySet()));
        }
    }

    @Test
    public void testScalarEvaluatesInErrorOnSysShards() throws Exception {
        // we need at least 1 shard, otherwise the table is empty and no evaluation occurs
        execute("create table t1 (id integer) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        assertThrows(() -> execute(
            "select 1/0 from sys.shards"),
                     isSQLError(is("/ by zero"), INTERNAL_ERROR, BAD_REQUEST, 4000));
    }

    @Test
    public void test_state_for_shards_of_closed_table() throws Exception {
        execute("create table doc.tbl (x int) clustered into 2 shards with(number_of_replicas=0)");
        execute("insert into doc.tbl values(1)");
        logger.info("---> Closing table doc.tbl");
        execute("alter table doc.tbl close");
        execute("select id, closed from sys.shards where table_name = 'tbl' order by id asc");
        assertThat(response.rows()[0][0], is(0));
        assertThat(response.rows()[0][1], is(true));
        assertThat(response.rows()[1][0], is(1));
        assertThat(response.rows()[1][1], is(true));
    }

    @UseJdbc(0)
    @Test
    public void testSelectFromClosedTableNotAllowed() throws Exception {
        execute("create table doc.tbl (x int)");
        execute("insert into doc.tbl values(1)");
        execute("alter table doc.tbl close");
        assertThrows(() -> execute("select * from doc.tbl"),
                     OperationOnInaccessibleRelationException.class,
                     "The relation \"doc.tbl\" doesn't support or allow READ operations, as it is currently closed.");
    }

    @UseJdbc(0)
    @Test
    public void testInsertFromClosedTableNotAllowed() throws Exception {
        execute("create table doc.tbl (x int)");
        execute("insert into doc.tbl values(1)");
        execute("alter table doc.tbl close");
        assertThrows(() -> execute("insert into doc.tbl values(2)"),
                     OperationOnInaccessibleRelationException.class,
                     "The relation \"doc.tbl\" doesn't support or allow INSERT operations, as it is currently closed.");
    }
}
