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

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import com.carrotsearch.hppc.cursors.ObjectCursor;

@IntegTestCase.ClusterScope(numDataNodes = 2)
public class RemoteCollectorIntegrationTest extends IntegTestCase {

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

        ClusterService clusterService = cluster().getInstance(ClusterService.class);
        IndexShardRoutingTable t = clusterService.state().routingTable().shardRoutingTable(getFqn("t"), 0);

        String sourceNodeId = t.primaryShard().currentNodeId();
        assert sourceNodeId != null;
        String targetNodeId = null;

        for (ObjectCursor<String> cursor : clusterService.state().nodes().getDataNodes().keys()) {
            if (!sourceNodeId.equals(cursor.value)) {
                targetNodeId = cursor.value;
            }
        }
        assert targetNodeId != null;

        execute("alter table t reroute move shard 0 from ? to ?", new Object[] { sourceNodeId, targetNodeId });

        assertBusy(() -> {
            execute("select * from sys.allocations where table_name = 't' and current_state = 'RELOCATING'");
            assertThat(response).hasRowCount(0);
        });

        execute(plan).getResult();

        execute("refresh table t");
        assertThat(execute("select * from t order by id")).hasRows(
            "1| 20",
            "2| 40"
        );
    }
}
