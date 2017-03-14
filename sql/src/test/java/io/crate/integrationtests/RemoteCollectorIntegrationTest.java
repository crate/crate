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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
@UseJdbc
public class RemoteCollectorIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testUpdateWithExpressionAndRelocatedShard() throws Exception {
        execute("create table t (id int primary key, x int) " +
                "clustered into 2 shards " +
                "with (number_of_replicas = 0)");
        ensureGreen();

        execute("insert into t (id, x) values (1, 10)");
        execute("insert into t (id, x) values (2, 20)");
        execute("refresh table t");

        PlanForNode plan = plan("update t set x = x * 2");

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        IndexShardRoutingTable t = clusterService.state().routingTable().shardRoutingTable("t", 0);

        String sourceNodeId = t.primaryShard().currentNodeId();
        assert sourceNodeId != null;
        String targetNodeId = null;

        for (ObjectCursor<String> cursor : clusterService.state().nodes().dataNodes().keys()) {
            if (!sourceNodeId.equals(cursor.value)) {
                targetNodeId = cursor.value;
            }
        }
        assert targetNodeId != null;

        client().admin().cluster().prepareReroute()
            .add(new MoveAllocationCommand(new ShardId("t", 0), sourceNodeId, targetNodeId))
            .execute().actionGet();

        client().admin().cluster().prepareHealth("t")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForRelocatingShards(0)
            .setTimeout(TimeValue.timeValueSeconds(5)).execute().actionGet();

        execute(plan).getResult();

        execute("refresh table t");
        assertThat(TestingHelpers.printedTable(execute("select * from t order by id").rows()),
            is("1| 20\n" +
               "2| 40\n")
        );
    }
}
