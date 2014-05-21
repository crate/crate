/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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


import io.crate.test.integration.CrateIntegrationTest;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.collect.MapBuilder;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 1)
public class TransportSQLActionSingleNodeTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testUnassignedShards() throws Exception {
        int numReplicas = 3;
        int numShards = 5;
        long expectedUnassignedShards = numShards * numReplicas; // calculation is correct for cluster.numNodes = 1

        execute("create table locations (id integer primary key, name string) " +
                "clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas + ")");
        ensureYellow();

        execute("select count(*) from sys.shards where table_name = 'locations' and state = 'UNASSIGNED'");
        assertThat(response.rowCount(), is(1L));
        assertEquals(1, response.cols().length);
        assertThat((Long) response.rows()[0][0], is(expectedUnassignedShards));


        // test that cluster and node expressions are also available for unassigned shards
        execute("select sys.cluster.name, sys.nodes.name, sys.nodes.port['http'] from sys.shards where table_name = 'locations'");
        assertThat(response.rowCount(), is(20L));
        assertThat((String)response.rows()[0][0], is(cluster().clusterName()));
    }

    @Test
    public void testPrimarySecondaryUnassignedShards() throws Exception {
        int numReplicas = 3;
        int numShards = 5;

        execute("create table locations (id integer primary key, name string) " +
                "clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas + ")");
        ensureYellow();

        execute("select \"primary\", state, count(*) from sys.shards where table_name = 'locations' group by \"primary\", state order by \"primary\"");
        assertThat(response.rowCount(), is(2L));
        assertEquals(new Boolean(false), response.rows()[0][0]);
        assertEquals("UNASSIGNED", response.rows()[0][1]);
        assertEquals(15L, response.rows()[0][2]);
        assertEquals(new Boolean(true), response.rows()[1][0]);
        assertEquals("STARTED", response.rows()[1][1]);
        assertEquals(5L, response.rows()[1][2]);
    }

    @Test
    public void testPrimarySecondaryUnassignedShardsWithPartitions() throws Exception {
        int numReplicas = 1;
        int numShards = 5;

        execute("create table locations (id integer primary key, name string) " +
                "partitioned by (id) clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas + ")");
        execute("insert into locations (id, name) values (1, 'name1')");
        execute("insert into locations (id, name) values (2, 'name2')");
        refresh();
        ensureYellow();

        execute("select \"primary\", state, count(*) from sys.shards where table_name = 'locations' group by \"primary\", state order by \"primary\"");
        assertThat(response.rowCount(), is(2L));
        assertEquals(new Boolean(false), response.rows()[0][0]);
        assertEquals("UNASSIGNED", response.rows()[0][1]);
        assertEquals(10L, response.rows()[0][2]);
        assertEquals(new Boolean(true), response.rows()[1][0]);
        assertEquals("STARTED", response.rows()[1][1]);
        assertEquals(10L, response.rows()[1][2]);
    }

    @Test
    public void testPartitionIdentPrimarySecondaryUnassignedShards() throws Exception {
        int numReplicas = 1;
        int numShards = 5;

        execute("create table locations (id integer primary key, name string) " +
                "partitioned by (id) clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas + ")");
        execute("insert into locations (id, name) values (1, 'name1')");
        execute("insert into locations (id, name) values (2, 'name2')");
        refresh();
        ensureYellow();

        execute("select table_name, partition_ident, state from sys.shards where table_name = 'locations' " +
                "group by table_name, partition_ident, state order by partition_ident, state");
        assertThat(response.rowCount(), is(4L));

        String expected = "locations| 04132| STARTED\n" +
                "locations| 04132| UNASSIGNED\n" +
                "locations| 04134| STARTED\n" +
                "locations| 04134| UNASSIGNED\n";

       assertEquals(expected, TestingHelpers.printedTable(response.rows()));
    }

    @Test
    public void testPartitionedTableNestedPk() throws Exception {
        execute("create table t (o object as (i int primary key, name string)) partitioned by (o['i']) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (o) values (?)", new Object[]{new MapBuilder<String, Object>().put("i", 1).put("name", "Zaphod").map()});
        ensureGreen();
        refresh();
        execute("select o['i'], o['name'] from t");
        assertThat((Integer)response.rows()[0][0], is(1));
        execute("select distinct table_name, partition_ident from sys.shards where table_name = 't'");
        assertEquals("t| 04132\n", TestingHelpers.printedTable(response.rows()));
    }

}
