/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.action.sql.SQLActionException;
import io.crate.metadata.PartitionName;
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.apache.lucene.util.Version;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.printedTable;
import static io.crate.testing.TestingHelpers.resolveCanonicalString;
import static org.hamcrest.Matchers.*;

@ESIntegTestCase.ClusterScope(numClientNodes = 0, numDataNodes = 2)
@UseJdbc
public class SysShardsTest extends SQLTransportIntegrationTest {

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

        // b2
        String b2Path = (String) response.rows()[1][0];
        assertThat(b2Path, containsString(resolveCanonicalString("/nodes/")));
        assertThat(b2Path, endsWith(resolveCanonicalString("/indices/.blob_b2/0")));

        String b2BlobPath = (String) response.rows()[1][1];
        assertThat(b2BlobPath, containsString(resolveCanonicalString("/nodes/")));
        assertThat(b2BlobPath, endsWith(resolveCanonicalString("/indices/.blob_b2/0/blobs")));
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
            execute("create table t (id int, name string) with (number_of_replicas=2)");
            ensureYellow();

            SQLResponse response = execute("select sum(num_docs), table_name, sum(num_docs) from sys.shards group by table_name order by table_name desc limit 1000");
            assertThat(response.rowCount(), is(4L));
            assertThat(TestingHelpers.printedTable(response.rows()),
                is("0.0| t| 0.0\n" +
                   "0.0| quotes| 0.0\n" +
                   "14.0| characters| 14.0\n" +
                   "0.0| blobs| 0.0\n"));
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
        assertEquals(15, response.cols().length);
    }

    @Test
    public void testSelectStarAllTables() throws Exception {
        SQLResponse response = execute("select * from sys.shards");
        assertEquals(26L, response.rowCount());
        assertEquals(15, response.cols().length);
        assertThat(response.cols(), arrayContaining(
            "blob_path",
            "id",
            "min_lucene_version",
            "num_docs",
            "orphan_partition",
            "partition_ident",
            "path",
            "primary",
            "recovery",
            "relocating_node",
            "routing_state",
            "schema_name",
            "size",
            "state",
            "table_name"));
    }

    @Test
    public void testSelectStarLike() throws Exception {
        SQLResponse response = execute(
            "select * from sys.shards where table_name like 'charact%'");
        assertEquals(8L, response.rowCount());
        assertEquals(15, response.cols().length);
    }

    @Test
    public void testSelectStarNotLike() throws Exception {
        SQLResponse response = execute(
            "select * from sys.shards where table_name not like 'quotes%'");
        assertEquals(18L, response.rowCount());
        assertEquals(15, response.cols().length);
    }

    @Test
    public void testSelectStarIn() throws Exception {
        SQLResponse response = execute(
            "select * from sys.shards where table_name in ('characters')");
        assertEquals(8L, response.rowCount());
        assertEquals(15, response.cols().length);
    }

    @Test
    public void testSelectStarMatch() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot use match predicate on system tables");
        execute("select * from sys.shards where match(table_name, 'characters')");
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
        expectedException.expect(SQLActionException.class);
        execute("select lol from sys.shards group by table_name");
    }

    @Test
    public void testGroupByUnknownGroupByColumn() throws Exception {
        expectedException.expect(SQLActionException.class);
        execute("select max(num_docs) from sys.shards group by lol");
    }

    @Test
    public void testGroupByUnknownOrderBy() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Column lol unknown");
        execute(
            "select sum(num_docs), table_name from sys.shards group by table_name order by lol");
    }

    @Test
    public void testGroupByUnknownWhere() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Column lol unknown");
        execute(
            "select sum(num_docs), table_name from sys.shards where lol='funky' group by table_name");
    }

    @Test
    public void testGlobalAggregateUnknownWhere() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Column lol unknown");
        execute(
            "select sum(num_docs) from sys.shards where lol='funky'");
    }

    @Test
    public void testSelectShardIdFromSysNodes() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot resolve relation 'sys.shards'");
        execute("select sys.shards.id from sys.nodes");
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
            "select _node, _node['name'], id from sys.shards order by _node['name'], id  limit 1");
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
                    "clustered into 5 shards with (number_of_replicas=2)");
            ensureYellow();
            SQLResponse response = execute(
                "select _node['name'], id from sys.shards where table_name = 'users' order by _node['name'] nulls last"
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

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(" / by zero");
        execute("select 1/0 from sys.shards");
    }

    @Test
    public void testDistinctConcat() throws Exception {
        execute("SELECT distinct concat(schema_name, '.', table_name) as t FROM sys.shards order by 1");
        assertThat(printedTable(response.rows()),
            is("blob.blobs\n" +
               "doc.characters\n" +
               "doc.quotes\n"));
    }
}
