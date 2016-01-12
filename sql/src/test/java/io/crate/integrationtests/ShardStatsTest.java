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

import io.crate.blob.v2.BlobIndices;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;


@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 2)
public class ShardStatsTest extends SQLTransportIntegrationTest {

    @Test
    public void testSelectZeroLimit() throws Exception {
        execute("create table test (col1 int) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("select * from sys.shards limit 0");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void testShardSelect() throws Exception {
        execute("create table test (col1 int) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();

        execute("select count(*) from sys.shards where table_name='test'");
        assertEquals(1, response.rowCount());
        assertEquals(3L, response.rows()[0][0]);
    }

    @Test
    public void testSelectIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) " +
            "clustered into 2 shards " +
            "with (number_of_replicas=2)");
        client().admin().cluster().prepareHealth("locations").setWaitForYellowStatus().execute().actionGet();

        execute("select state, \"primary\", recovery from sys.shards where table_name = 'locations' order by state, \"primary\"");
        assertEquals(6L, response.rowCount());
        for (int i = 4; i < response.rowCount(); i++) {
            Object[] row = response.rows()[i];
            assertEquals("UNASSIGNED", row[0]);
            assertEquals(false, row[1]);
            assertEquals(null, row[2]);
        }
    }

    @Test
    public void testSelectGroupByIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) " +
                "clustered into 5 shards " +
                "with(number_of_replicas=2)");
        ensureYellow();

        execute("select count(*), state, \"primary\" from sys.shards " +
                "group by state, \"primary\" order by state desc");
        assertThat(response.rowCount(), greaterThanOrEqualTo(2L));
        assertEquals(3, response.cols().length);
        assertThat((Long) response.rows()[0][0], greaterThanOrEqualTo(5L));
        assertEquals("UNASSIGNED", response.rows()[0][1]);
        assertEquals(false, response.rows()[0][2]);
    }

    @Test
    public void testSelectCountIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) " +
                "clustered into 5 shards " +
                "with(number_of_replicas=2)");
        ensureYellow();

        execute("select count(*) from sys.shards where schema_name='doc' AND table_name='locations'");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(15L));
    }

    @Test
    public void testTableNameBlobTable() throws Exception {
        BlobIndices blobIndices = internalCluster().getInstance(BlobIndices.class);
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .build();
        blobIndices.createBlobTable("blobs", indexSettings).get();
        ensureGreen();

        execute("select schema_name, table_name from sys.shards where table_name = 'blobs'");
        assertThat(response.rowCount(), is(2L));
        for (int i = 0; i<response.rowCount(); i++) {
            assertThat((String)response.rows()[i][0], is("blob"));
            assertThat((String)response.rows()[i][1], is("blobs"));
        }

        execute("create blob table sbolb clustered into 4 shards with (number_of_replicas=3)");
        ensureYellow();

        execute("select schema_name, table_name from sys.shards where table_name = 'sbolb'");
        assertThat(response.rowCount(), is(16L));
        for (int i = 0; i < response.rowCount(); i++) {
            assertThat((String)response.rows()[i][0], is("blob"));
            assertThat((String)response.rows()[i][1], is("sbolb"));
        }
        execute("select count(*) from sys.shards " +
                "where schema_name='blob' and table_name != 'blobs' " +
                "and table_name != 'sbolb'");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long)response.rows()[0][0], is(0L));
    }
}
