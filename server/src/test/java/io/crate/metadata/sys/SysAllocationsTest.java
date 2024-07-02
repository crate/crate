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

package io.crate.metadata.sys;

import static io.crate.testing.Asserts.assertThat;

import java.util.List;
import java.util.Map;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.testing.UseJdbc;
import io.crate.types.ArrayType;

@UseJdbc(0)
@IntegTestCase.ClusterScope(numDataNodes = 1)
public class SysAllocationsTest extends IntegTestCase {

    @Before
    public void initTestData() throws Exception {
        execute("CREATE TABLE t1 (id INTEGER, name STRING) CLUSTERED INTO 1 SHARDS WITH (number_of_replicas = 0)");
        execute("ALTER TABLE t1 SET (number_of_replicas = 1)");
    }

    @Test
    public void testUnassignedShardSimpleColumns() {
        execute("SELECT table_name, shard_id, primary, current_state, explanation " +
                "FROM sys.allocations " +
                "WHERE table_name = 't1' " +
                "ORDER BY primary, shard_id");

        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response).hasRows(
            "t1| 0| false| UNASSIGNED| cannot allocate because allocation is not permitted to any of the nodes",
            "t1| 0| true| STARTED| rebalancing is not allowed");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnassignedShardDecisionsColumn() {
        execute("SELECT decisions " +
                "FROM sys.allocations " +
                "WHERE table_name = 't1' " +
                "ORDER BY primary, shard_id");

        assertThat(response.columnTypes()[0].id()).isEqualTo(ArrayType.ID);
        assertThat(response.rowCount()).isEqualTo(2L);

        Object[] row;

        // first row: UNASSIGNED shard
        row = response.rows()[0];
        List<Map<String, Object>> decisions = (List<Map<String, Object>>) row[0];
        assertThat(decisions).hasSize(1);
        Map<String, Object> decision = decisions.getFirst();
        assertThat(decision.get("node_id")).as("nodeId must not be null").isNotNull();
        assertThat(decision.get("node_name")).as("nodeName must not be null").isNotNull();
        assertThat(((List<String>) decision.get("explanations")).getFirst()).startsWith(
            "a copy of this shard is already allocated to this node");

        // second row: STARTED shard
        row = response.rows()[1];
        decisions = (List<Map<String, Object>>) row[0];
        assertThat(decisions).as("for the stared shard decisions must be null").isNull();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnassignedShardDecisionsColumnSubscript() {
        execute("SELECT decisions['node_id'], decisions['node_name'], decisions['explanations'] " +
                "FROM sys.allocations " +
                "WHERE table_name = 't1' " +
                "ORDER BY primary, shard_id");
        assertThat(response.rowCount()).isEqualTo(2L);

        Object[] row;

        // first row: UNASSIGNED shard
        row = response.rows()[0];
        List<Object> nodeIds = (List<Object>) row[0];
        List<Object> nodeNames = (List<Object>) row[1];
        List<Object> explanations = (List<Object>) row[2];
        assertThat(nodeIds.getFirst()).as("first element of nodeId must not be null").isNotNull();
        assertThat(nodeNames.getFirst()).as("first element of nodeName must not be null").isNotNull();
        assertThat(explanations.getFirst()).as("first element of explanations must not be null").isNotNull();

        // second row: STARTED shard
        row = response.rows()[1];
        nodeIds = (List<Object>) row[0];
        nodeNames = (List<Object>) row[1];
        explanations = (List<Object>) row[2];
        assertThat(nodeIds).as("nodeId must be null").isNull();
        assertThat(nodeNames).as("nodeName must be null").isNull();
        assertThat(explanations).as("explanations must be null").isNull();
    }
}
