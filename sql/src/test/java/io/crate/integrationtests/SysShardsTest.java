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

import com.google.common.base.Joiner;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLResponse;
import io.crate.blob.v2.BlobIndices;
import io.crate.test.integration.ClassLifecycleIntegrationTest;
import io.crate.testing.SQLTransportExecutor;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class SysShardsTest extends ClassLifecycleIntegrationTest {

    private static boolean dataInitialized = false;
    private static SQLTransportExecutor transportExecutor;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void initTestData() throws Exception {
        synchronized (SysShardsTest.class) {
            if (dataInitialized) {
                return;
            }
            transportExecutor = SQLTransportExecutor.create(ClassLifecycleIntegrationTest.GLOBAL_CLUSTER);
            Setup setup = new Setup(transportExecutor);
            setup.groupBySetup();
            transportExecutor.exec(
                "create table quotes (id integer primary key, quote string) with(number_of_replicas=1)");
            BlobIndices blobIndices = GLOBAL_CLUSTER.getInstance(BlobIndices.class);
            Settings indexSettings = ImmutableSettings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5)
                    .build();
            blobIndices.createBlobTable("blobs", indexSettings);
            transportExecutor.ensureGreen();
            dataInitialized = true;
        }
    }

    @AfterClass
    public synchronized static void after() throws Exception {
        if (transportExecutor != null) {
            transportExecutor = null;
        }
    }

    @Test
    public void testSelectGroupByWhereTable() throws Exception {
        SQLResponse response = transportExecutor.exec("" +
            "select count(*), num_docs from sys.shards where table_name = 'characters' " +
            "group by num_docs order by count(*)");
        assertThat(response.rowCount(), greaterThan(0L));
    }

    @Test
    public void testSelectGroupByAllTables() throws Exception {
        SQLResponse response = transportExecutor.exec("select count(*), table_name from sys.shards " +
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
            transportExecutor.exec("create table t (id int, name string) with (number_of_replicas=2)");
            transportExecutor.ensureYellow();

            SQLResponse response = transportExecutor.exec("select sum(num_docs), table_name, sum(num_docs) from sys.shards group by table_name order by table_name desc limit 1000");
            assertThat(response.rowCount(), is(4L));
            assertThat(TestingHelpers.printedTable(response.rows()),
                    is("0.0| t| 0.0\n" +
                            "0.0| quotes| 0.0\n" +
                            "14.0| characters| 14.0\n" +
                            "0.0| blobs| 0.0\n"));
        } finally {
            transportExecutor.exec("drop table t");
        }
    }

    @Test
    public void testSelectGroupByWhereNotLike() throws Exception {
        SQLResponse response = transportExecutor.exec("select count(*), table_name from sys.shards " +
            "where table_name not like 'my_table%' group by table_name order by table_name");
        assertEquals(3L, response.rowCount());
        assertEquals(10L, response.rows()[0][0]);
        assertEquals("blobs", response.rows()[0][1]);
        assertEquals(10L, response.rows()[1][0]);
        assertEquals("characters", response.rows()[1][1]);
        assertEquals(10L, response.rows()[2][0]);
        assertEquals("quotes", response.rows()[2][1]);
    }

    @Test
    public void testSelectWhereTable() throws Exception {
        SQLResponse response = transportExecutor.exec(
            "select id, sys.nodes.name, size from sys.shards " +
            "where table_name = 'characters'");
        assertEquals(10L, response.rowCount());
    }

    @Test
    public void testSelectStarWhereTable() throws Exception {
        SQLResponse response = transportExecutor.exec(
            "select * from sys.shards where table_name = 'characters'");
        assertEquals(10L, response.rowCount());
        assertEquals(10, response.cols().length);
    }

    @Test
    public void testSelectStarAllTables() throws Exception {
        SQLResponse response = transportExecutor.exec("select * from sys.shards");
        assertEquals(30L, response.rowCount());
        assertEquals(10, response.cols().length);
        assertEquals("schema_name, table_name, id, partition_ident, num_docs, primary, relocating_node, size, state, orphan_partition",
            Joiner.on(", ").join(response.cols()));
    }

    @Test
    public void testSelectStarLike() throws Exception {
        SQLResponse response = transportExecutor.exec(
            "select * from sys.shards where table_name like 'charact%'");
        assertEquals(10L, response.rowCount());
        assertEquals(10, response.cols().length);
    }

    @Test
    public void testSelectStarNotLike() throws Exception {
        SQLResponse response = transportExecutor.exec(
            "select * from sys.shards where table_name not like 'quotes%'");
        assertEquals(20L, response.rowCount());
        assertEquals(10, response.cols().length);
    }

    @Test
    public void testSelectStarIn() throws Exception {
        SQLResponse response = transportExecutor.exec(
            "select * from sys.shards where table_name in ('characters')");
        assertEquals(10L, response.rowCount());
        assertEquals(10, response.cols().length);
    }

    @Test
    public void testSelectStarMatch() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot use match predicate on system tables");
        transportExecutor.exec("select * from sys.shards where match(table_name, 'characters')");
    }

    @Test
    public void testSelectOrderBy() throws Exception {
        SQLResponse response = transportExecutor.exec("select * from sys.shards order by table_name");
        assertEquals(30L, response.rowCount());
        String[] tableNames = {"blobs", "characters", "quotes"};
        for (int i=0; i<response.rowCount(); i++) {
            int idx = i/10;
            assertEquals(tableNames[idx], response.rows()[i][1]);
        }
    }

    @Test
    public void testSelectGreaterThan() throws Exception {
        SQLResponse response = transportExecutor.exec("select * from sys.shards where num_docs > 0");
        assertThat(response.rowCount(), greaterThan(0L));
    }

    @Test
    public void testSelectWhereBoolean() throws Exception {
        SQLResponse response = transportExecutor.exec("select * from sys.shards where \"primary\" = false");
        assertEquals(15L, response.rowCount());
    }

    @Test
    public void testSelectGlobalAggregates() throws Exception {
        SQLResponse response = transportExecutor.exec(
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
        SQLResponse response = transportExecutor.exec("select count(*) from sys.shards");
        assertEquals(1L, response.rowCount());
        assertEquals(30L, response.rows()[0][0]);
    }

    @Test
    public void testSelectGlobalCountAndOthers() throws Exception {
        SQLResponse response = transportExecutor.exec("select count(*), max(table_name) from sys.shards");
        assertEquals(1L, response.rowCount());
        assertEquals(30L, response.rows()[0][0]);
        assertEquals("quotes", response.rows()[0][1]);
    }

    @Test
    public void testSelectGlobalExpressionGroupBy() throws Exception {
        SQLResponse response = transportExecutor.exec("select count(*), table_name, sys.cluster.name from sys.shards " +
            "group by sys.cluster.name, table_name order by table_name");
        assertEquals(3, response.rowCount());
        assertEquals(10L, response.rows()[0][0]);
        assertEquals("blobs", response.rows()[0][1]);
        assertEquals(GLOBAL_CLUSTER.clusterName(), response.rows()[0][2]);

        assertEquals(10L, response.rows()[1][0]);
        assertEquals("characters", response.rows()[1][1]);
        assertEquals(GLOBAL_CLUSTER.clusterName(), response.rows()[1][2]);

        assertEquals(10L, response.rows()[2][0]);
        assertEquals("quotes", response.rows()[2][1]);
        assertEquals(GLOBAL_CLUSTER.clusterName(), response.rows()[2][2]);
    }

    @Test
    public void testGroupByUnknownResultColumn() throws Exception {
        expectedException.expect(SQLActionException.class);
        transportExecutor.exec("select lol from sys.shards group by table_name");
    }

    @Test
    public void testGroupByUnknownGroupByColumn() throws Exception {
        expectedException.expect(SQLActionException.class);
        transportExecutor.exec("select max(num_docs) from sys.shards group by lol");
    }

    @Test
    public void testGroupByUnknownOrderBy() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Column 'lol' unknown");
        transportExecutor.exec(
            "select sum(num_docs), table_name from sys.shards group by table_name order by lol");
    }

    @Test
    public void testGroupByUnknownWhere() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Column 'lol' unknown");
        transportExecutor.exec(
            "select sum(num_docs), table_name from sys.shards where lol='funky' group by table_name");
    }

    @Test
    public void testGlobalAggregateUnknownWhere() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Column 'lol' unknown");
        transportExecutor.exec(
            "select sum(num_docs) from sys.shards where lol='funky'");
    }


    @Test
    public void testShardGranularityFromNodeGranularityTable() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot handle Reference sys.shards.id");
        transportExecutor.exec("select sys.shards.id from sys.nodes");
    }
}
