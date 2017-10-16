/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;


@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class AlterTableRerouteIntegrationTest extends SQLTransportIntegrationTest {

    private void createPartedTable() {
        execute("create table parted (" +
            "id int primary key," +
            "date timestamp primary key" +
            ") clustered into 1 shards " +
            "partitioned by (date) " +
            "with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into parted (id, date) values (1, '2017-01-01')");
        execute("insert into parted (id, date) values (2, '2017-02-01')");
        execute("refresh table parted");
    }

    private void createTable() {
        execute("create table my_table (" +
            "id int primary key," +
            "date timestamp" +
            ") clustered into 1 shards " +
            "with (number_of_replicas=0)");
        ensureGreen();
    }

    private Tuple<String, String> getNodesForShardMovement(String table, int shardId, Map partition) {
        String partitionIdent = "";
        if (partition.isEmpty() == false) {
            execute("select partition_ident from information_schema.table_partitions where table_name = ? and values = ?", new Object[]{table, partition});
            partitionIdent = (String) response.rows()[0][0];
        }
        execute("select _node['id'] from sys.shards where id = ? and partition_ident = ?", new Object[]{shardId, partitionIdent});
        String fromNode = (String) response.rows()[0][0];
        execute("select id from sys.nodes where id != ?", new Object[]{fromNode});
        String toNode = (String) response.rows()[0][0];

        return new Tuple<>(fromNode, toNode);
    }

    @After
    public void cleanUp() throws Exception {
        execute("reset global \"cluster.routing.allocation.enable\"");
        execute("reset global \"cluster.routing.allocation.allow_rebalance\"");
    }

    @Test
    @Repeat(iterations = 10)
    public void testMoveShard() throws Exception {
        int shardId = 0;
        createTable();
        Tuple<String, String> rerouteInfo = getNodesForShardMovement("my_table", shardId, Collections.emptyMap());

        execute("ALTER TABLE my_table REROUTE MOVE SHARD ? FROM ? TO ?", new Object[]{shardId, rerouteInfo.v1(), rerouteInfo.v2()});
        assertThat(response.rowCount(), is(1L));
        execute("select count(*) from sys.shards where id = ? and _node['id'] = ? and table_name = 'my_table'", new Object[]{shardId, rerouteInfo.v2()});
        assertThat(response.rows()[0][0], is(1L));
    }

    @Test
    @Repeat(iterations = 10)
    public void testMoveShardFromPartitionedTable() throws Exception {
        int shardId = 0;
        Map<String, Long> partition = new HashMap<>();
        partition.put("date", 1483228800000L);

        createPartedTable();
        Tuple<String, String> rerouteInfo = getNodesForShardMovement("parted", shardId, partition);

        execute("ALTER TABLE parted PARTITION (date = ?) REROUTE MOVE SHARD ? from ? TO ?", new Object[]{partition.get("date"), shardId, rerouteInfo.v1(), rerouteInfo.v2()});
        assertThat(response.rowCount(), is(1L));
        execute("select partition_ident from sys.shards where id = ? and _node['id'] = ? and table_name = 'parted'", new Object[]{shardId, rerouteInfo.v2()});
        assertThat(response.rowCount(), is(2L));
        assertNotEquals(response.rows()[0][0], response.rows()[1][0]);
    }

    @Test
    @Repeat(iterations = 10)
    public void testCancelAllowPrimaryAllocationOfShard() throws Exception {
        int shardId = 0;
        createTable();
        Tuple<String, String> rerouteInfo = getNodesForShardMovement("my_table", shardId, Collections.emptyMap());

        execute("ALTER TABLE my_table REROUTE MOVE SHARD ? FROM ? TO ?", new Object[]{shardId, rerouteInfo.v1(), rerouteInfo.v2()});
        execute("ALTER TABLE my_table REROUTE CANCEL SHARD ? ON ? WITH (allow_primary = TRUE)", new Object[]{shardId, rerouteInfo.v2()});
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    @Repeat(iterations = 10)
    public void testAllocateReplicaShard() throws Exception {
        int shardId = 0;
        createTable();
        execute("set global \"cluster.routing.allocation.enable\"=\"primaries\"");
        execute("set global \"cluster.routing.allocation.allow_rebalance\"=\"indices_all_active\"");
        execute("alter table my_table set (number_of_replicas = 1)");
        ensureYellow();
        execute("select count(*) from sys.shards where state = 'UNASSIGNED' and table_name = 'my_table'");
        assertThat(response.rows()[0][0], is(1L));

        Tuple<String, String> rerouteInfo = getNodesForShardMovement("my_table", shardId, Collections.emptyMap());

        execute("reset global \"cluster.routing.allocation.enable\"");
        execute("ALTER TABLE my_table REROUTE ALLOCATE REPLICA SHARD ? ON ?", new Object[]{shardId, rerouteInfo.v2()});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        execute("select count(*) from sys.shards where state = 'UNASSIGNED' and table_name = 'my_table'");
        assertThat(response.rows()[0][0], is(0L));
    }

    @Test
    @Repeat(iterations = 10)
    public void testRetryFailedShards() throws Exception {
        createTable();
        execute("set global \"cluster.routing.allocation.enable\"=\"primaries\"");
        execute("set global \"cluster.routing.allocation.allow_rebalance\"=\"indices_all_active\"");
        execute("alter table my_table set (number_of_replicas = 1)");
        execute("select count(*) from sys.shards where state = 'UNASSIGNED' and table_name = 'my_table'");
        assertThat(response.rows()[0][0], is(1L));

        execute("reset global \"cluster.routing.allocation.enable\"");
        execute("ALTER TABLE my_table REROUTE RETRY FAILED");
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        execute("select count(*) from sys.shards where state = 'UNASSIGNED' and table_name = 'my_table'");
        assertThat(response.rows()[0][0], is(0L));
    }


    @Test
    public void testRerouteMoveBlobIndex() throws Exception {
        assertTrue(true);
    }
}
