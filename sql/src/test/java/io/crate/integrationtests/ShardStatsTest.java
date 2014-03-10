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
import io.crate.action.sql.SQLResponse;
<<<<<<< HEAD
=======
import io.crate.blob.v2.BlobIndices;
import io.crate.core.NumberOfReplicas;
>>>>>>> 8554e10... fixup! fixup! fixup! implemented blob shards on sys.shards
import io.crate.exceptions.CrateException;
import io.crate.exceptions.SQLParseException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 2)
public class ShardStatsTest extends SQLTransportIntegrationTest {

    private SQLResponse response;
    private Setup setup = new Setup(this);

    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
                .put("number_of_replicas", 1)
                .put("number_of_shards", 5).build();
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * override execute to store response in property for easier access
     */
    @Override
    public SQLResponse execute(String stmt, Object[] args) {
        response = super.execute(stmt, args);
        return response;
    }

    @Before
    public void initTestData() throws Exception {
        setup.groupBySetup();
        execute("create table quotes (id integer primary key, quote string) with(number_of_replicas=1)");
<<<<<<< HEAD
        client().admin().indices().prepareCreate(".blob_blobs")
                .setSettings(
                        ImmutableSettings.builder()
                                .put("blobs.enabled", true).build()).execute().actionGet();
=======
        BlobIndices blobIndices = cluster().getInstance(BlobIndices.class);
        blobIndices.createBlobTable("blobs", new NumberOfReplicas(1), 5);
>>>>>>> 8554e10... fixup! fixup! fixup! implemented blob shards on sys.shards
        ensureGreen();
    }


    @Test
    public void testSelectGroupByWhereTable() throws Exception {
        execute("select count(*), num_docs from sys.shards where table_name = 'characters' " +
                "group by num_docs order by count(*)");
        assertThat(response.rowCount(), greaterThan(0L));
    }

    @Test
    public void testSelectGroupByAllTables() throws Exception {
        execute("select count(*), table_name from sys.shards " +
                "group by table_name order by table_name");
        assertEquals(3L, response.rowCount());
        assertEquals(10L, response.rows()[0][0]);
        assertEquals("blobs", response.rows()[0][1]);
        assertEquals("characters", response.rows()[1][1]);
        assertEquals("quotes", response.rows()[2][1]);
    }

    @Test
    public void testSelectGroupByWhereNotLike() throws Exception {
        execute("select count(*), table_name from sys.shards " +
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
        execute("select id, sys.nodes.name, size from sys.shards where table_name = " +
                "'characters'");
        assertEquals(10L, response.rowCount());
    }

    @Test
    public void testSelectStarWhereTable() throws Exception {
        execute("select * from sys.shards where table_name = 'characters'");
        assertEquals(10L, response.rowCount());
        assertEquals(8, response.cols().length);
    }

    @Test
    public void testSelectStarAllTables() throws Exception {
        execute("select * from sys.shards");
        assertEquals(30L, response.rowCount());
        assertEquals(8, response.cols().length);
        assertEquals("schema_name, table_name, id, num_docs, primary, relocating_node, size, state", Joiner.on(", ").join(response.cols()));
    }

    @Test
    public void testSelectStarLike() throws Exception {
        execute("select * from sys.shards where table_name like 'charact%'");
        assertEquals(10L, response.rowCount());
        assertEquals(8, response.cols().length);
    }

    @Test
    public void testSelectStarNotLike() throws Exception {
        execute("select * from sys.shards where table_name not like 'quotes%'");
        assertEquals(20L, response.rowCount());
        assertEquals(8, response.cols().length);
    }

    @Test
    public void testSelectStarIn() throws Exception {
        execute("select * from sys.shards where table_name in ('characters')");
        assertEquals(10L, response.rowCount());
        assertEquals(8, response.cols().length);
    }

    @Test(expected = UnsupportedFeatureException.class)
    public void testSelectStarMatch() throws Exception {
        execute("select * from sys.shards where match(table_name, 'characters')");
    }

    @Test
    public void testSelectOrderBy() throws Exception {
        execute("select * from sys.shards order by table_name");
        assertEquals(30L, response.rowCount());
        String[] schemaNames = {"blob", "doc", "doc"};
        String[] tableNames = {"blobs", "characters", "quotes"};
        for (int i=0; i<response.rowCount(); i++) {
            int idx = i/10;
            assertEquals(tableNames[idx], response.rows()[i][1]);
        }
    }

    @Test
    public void testSelectGreaterThan() throws Exception {
        execute("select * from sys.shards where num_docs > 0");
        assertThat(response.rowCount(), greaterThan(0L));
    }

    @Test
    public void testSelectWhereBoolean() throws Exception {
        execute("select * from sys.shards where \"primary\" = false");
        assertEquals(15L, response.rowCount());
    }

    @Test
    public void testSelectIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) with(number_of_replicas=2)");
        refresh();

        client().admin().cluster().prepareHealth("locations").setWaitForYellowStatus().execute().actionGet();

        execute("select * from sys.shards order by state");
        assertEquals(45L, response.rowCount());
        assertEquals(8, response.cols().length);
    }

    @Test
    public void testSelectGroupByIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) with(number_of_replicas=2)");
        refresh();
        ensureYellow();

        execute("select count(*), state from sys.shards " +
                "group by state order by state desc");
        assertThat(response.rowCount(), greaterThanOrEqualTo(2L));
        assertEquals(2, response.cols().length);
        assertThat((Long) response.rows()[0][0], greaterThanOrEqualTo(5L));
        assertEquals("UNASSIGNED", response.rows()[0][1]);
    }

    @Test
    public void testSelectGlobalAggregates() throws Exception {
        execute("select sum(size), min(size), max(size), avg(size) from sys.shards");
        assertEquals(1L, response.rowCount());
        assertEquals(4, response.rows()[0].length);
        assertNotNull(response.rows()[0][0]);
        assertNotNull(response.rows()[0][1]);
        assertNotNull(response.rows()[0][2]);
        assertNotNull(response.rows()[0][3]);
    }

    @Test
    public void testSelectGlobalCount() throws Exception {
        execute("select count(*) from sys.shards");
        assertEquals(1L, response.rowCount());
        assertEquals(30L, response.rows()[0][0]);
    }

    @Test
    public void testSelectGlobalCountAndOthers() throws Exception {
        execute("select count(*), max(table_name) from sys.shards");
        assertEquals(1L, response.rowCount());
        assertEquals(30L, response.rows()[0][0]);
        assertEquals("quotes", response.rows()[0][1]);
    }

    @Test
    public void testSelectGlobalExpressionGroupBy() throws Exception {
        execute("select count(*), table_name, sys.cluster.name from sys.shards " +
                "group by sys.cluster.name, table_name order by table_name");
        assertEquals(3, response.rowCount());
        assertEquals(10L, response.rows()[0][0]);
        assertEquals("blobs", response.rows()[0][1]);
        assertEquals(cluster().clusterName(), response.rows()[0][2]);

        assertEquals(10L, response.rows()[1][0]);
        assertEquals("characters", response.rows()[1][1]);
        assertEquals(cluster().clusterName(), response.rows()[1][2]);

        assertEquals(10L, response.rows()[2][0]);
        assertEquals("quotes", response.rows()[2][1]);
        assertEquals(cluster().clusterName(), response.rows()[2][2]);
    }

    @Test
    public void testGroupByUnknownResultColumn() throws Exception {
        expectedException.expect(SQLParseException.class);
        execute("select lol from sys.shards group by table_name");
    }

    @Test
    public void testGroupByUnknownGroupByColumn() throws Exception {
        expectedException.expect(CrateException.class);
        execute("select max(num_docs) from sys.shards group by lol");
    }

    public void testGroupByUnknownOrderBy() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        execute("select sum(num_docs), table_name from sys.shards group by table_name order by lol");
    }

    @Test
    public void testGroupByUnknownWhere() throws Exception {
        execute("select sum(num_docs), table_name from sys.shards where lol='funky' group by table_name");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testGlobalAggregateUnknownWhere() throws Exception {
        execute("select sum(num_docs) from sys.shards where lol='funky'");
        assertEquals(1, response.rowCount()); // global aggregate always returns one row
    }

    @Test
    public void testGlobalAggregateUnknownOrderBy() throws Exception {
        // order is ignored because global aggregates return only 1 row
        execute("select sum(num_docs) from sys.shards order by lol");
    }
}
