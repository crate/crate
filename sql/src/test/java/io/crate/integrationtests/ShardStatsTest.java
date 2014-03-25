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
import io.crate.core.NumberOfReplicas;
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;


@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class ShardStatsTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
                .put("number_of_replicas", 1)
                .put("number_of_shards", 5).build();
    }

    @Before
    public void initTestData() throws Exception {
        setup.groupBySetup();
        execute("create table quotes (id integer primary key, quote string) with(number_of_replicas=1)");
        BlobIndices blobIndices = cluster().getInstance(BlobIndices.class);
        blobIndices.createBlobTable("blobs", new NumberOfReplicas(1), 5);
        ensureGreen();
    }

    @Test
    public void testSelectIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) with(number_of_replicas=2)");
        refresh();

        client().admin().cluster().prepareHealth("locations").setWaitForYellowStatus().execute().actionGet();

        execute("select * from sys.shards order by state, \"primary\"");
        assertEquals(45L, response.rowCount());
        assertEquals(8, response.cols().length);
        assertEquals("UNASSIGNED", response.rows()[44][7]);
        assertEquals(null, response.rows()[44][4]);
    }

    @Test
    public void testSelectGroupByIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) with(number_of_replicas=2)");
        refresh();
        ensureYellow();

        execute("select count(*), state, \"primary\" from sys.shards " +
                "group by state, \"primary\" order by state desc");
        assertThat(response.rowCount(), greaterThanOrEqualTo(2L));
        assertEquals(3, response.cols().length);
        assertThat((Long) response.rows()[0][0], greaterThanOrEqualTo(5L));
        assertEquals("UNASSIGNED", response.rows()[0][1]);
        assertEquals(null, response.rows()[0][2]);
    }
}
