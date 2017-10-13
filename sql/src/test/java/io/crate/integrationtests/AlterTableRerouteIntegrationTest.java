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
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;


@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class AlterTableRerouteIntegrationTest extends SQLTransportIntegrationTest {

    class RerouteInfo {
        public final String fromNodeId;
        public final String toNodeId;

        public RerouteInfo(String fromNodeId, String toNodeId) {
            this.fromNodeId = fromNodeId;
            this.toNodeId = toNodeId;
        }
    }

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
            ") clustered into 2 shards " +
            "with (number_of_replicas=0)");
        ensureGreen();
    }


    private RerouteInfo getNodesForShardMovement(String table, int shardId, Map partition) {
        String partitionIdent = "";
        if (partition.isEmpty() == false) {
            execute("select partition_ident from information_schema.table_partitions where table_name = ? and values = ?", new Object[]{table, partition});
            partitionIdent = (String) response.rows()[0][0];
        }
        execute("select _node['id'] from sys.shards where id = ? and partition_ident = ?", new Object[]{shardId, partitionIdent});
        String fromNode = (String) response.rows()[0][0];
        execute("select id from sys.nodes where id != ?", new Object[]{fromNode});
        String toNode = (String) response.rows()[0][0];

        return new RerouteInfo(fromNode, toNode);
    }

    @Test
    @Repeat(iterations = 10)
    public void testMoveShard() throws Exception {
        createTable();
        int shardId = 0;
        RerouteInfo nodes = getNodesForShardMovement("my_table", shardId, Collections.emptyMap());
        execute("ALTER TABLE my_table REROUTE MOVE SHARD ? FROM ? TO ?", new Object[]{shardId, nodes.fromNodeId, nodes.toNodeId});
        execute("select count(*) from sys.shards where id = ? and _node['id'] = ? and table_name = 'my_table'", new Object[]{shardId, nodes.toNodeId});
        assertThat(response.rows()[0][0], is(1L));
    }

    @Test
    @Repeat(iterations = 10)
    public void testMoveShardFromPartitionedTable() throws Exception {
        int shardId = 0;
        Map<String, Long> partition = new HashMap<>();
        partition.put("date", 1483228800000L);

        createPartedTable();

        RerouteInfo shardInfo = getNodesForShardMovement("parted", shardId, partition);
        execute("ALTER TABLE parted PARTITION (date = ?) REROUTE MOVE SHARD ? from ? TO ?", new Object[]{partition.get("date"), shardId, shardInfo.fromNodeId, shardInfo.toNodeId});
        execute("select partition_ident from sys.shards where id = ? and _node['id'] = ? and table_name = 'parted'", new Object[]{shardId, shardInfo.toNodeId});
        assertThat(response.rowCount(), is(2L));
        assertNotEquals(response.rows()[0][0], response.rows()[1][0]);
    }
}
