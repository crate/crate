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

package io.crate.analyze;

import io.crate.data.RowN;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.management.AlterTableReroutePlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class AlterTableRerouteAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addBlobTable("create blob table blobs;")
            .enableDefaultTables()
            .build();
        plannerContext = e.getPlannerContext(clusterService.state());
    }

    @SuppressWarnings("unchecked")
    private <S> S analyze(String stmt, Object... arguments) {
        AnalyzedStatement analyzedStatement = e.analyze(stmt);
        return (S) AlterTableReroutePlan.createRerouteCommand(
            analyzedStatement,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            new RowN(arguments),
            SubQueryResults.EMPTY,
            DiscoveryNodes.builder()
                .add(clusterService.localNode()) // n1
                .add(new DiscoveryNode(
                    "n2",
                    buildNewFakeTransportAddress(),
                    Map.of(),
                    Set.of(),
                    Version.CURRENT))
                .build()
        );
    }

    @Test
    public void testRerouteOnSystemTableIsNotAllowed() {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage(
            "The relation \"sys.cluster\" doesn't support or" +
            " allow ALTER REROUTE operations, as it is read-only.");
        analyze("ALTER TABLE sys.cluster REROUTE MOVE SHARD 0 FROM 'n1' TO 'n2'");
    }

    @Test
    public void testRerouteMoveShardWithLiterals() {
        MoveAllocationCommand command = analyze(
            "ALTER TABLE users REROUTE MOVE SHARD 0 FROM 'n2' TO 'n1'");
        assertThat(command.index(), is("users"));
        assertThat(command.shardId(), is(0));
        assertThat(command.fromNode(), is("n2"));
        assertThat(command.toNode(), is("n1"));
    }

    @Test
    public void testRerouteMoveShardWithParameters() {
        MoveAllocationCommand command = analyze(
            "ALTER TABLE users REROUTE MOVE SHARD 0 FROM ? TO ?", "n2", "n1");
        assertThat(command.index(), is("users"));
        assertThat(command.shardId(), is(0));
        assertThat(command.fromNode(), is("n2"));
        assertThat(command.toNode(), is("n1"));
    }

    @Test
    public void testRerouteMoveShardPartitionedTable() {
        MoveAllocationCommand command = analyze(
            "ALTER TABLE parted PARTITION (date = 1395874800000) REROUTE MOVE SHARD 0 FROM 'n1' TO 'n2'");
        assertThat(command.index(), is(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
        assertThat(command.shardId(), is(0));
        assertThat(command.fromNode(), is("n1"));
        assertThat(command.toNode(), is("n2"));
    }

    @Test
    public void testRerouteOnBlobTable() {
        MoveAllocationCommand command = analyze(
            "ALTER TABLE blob.blobs REROUTE MOVE SHARD 0 FROM 'n1' TO 'n2'");
        assertThat(command.index(), is(".blob_blobs"));
    }

    @Test
    public void testRerouteAllocateReplicaShardWithLiterals() {
        AllocateReplicaAllocationCommand command = analyze(
            "ALTER TABLE users REROUTE ALLOCATE REPLICA SHARD 0 ON 'n1'");
        assertThat(command.index(), is("users"));
        assertThat(command.shardId(), is(0));
        assertThat(command.node(), is("n1"));
    }

    @Test
    public void testRerouteAllocateReplicaShardWithParameters() {
        AllocateReplicaAllocationCommand command = analyze(
            "ALTER TABLE users REROUTE ALLOCATE REPLICA SHARD 0 ON ?", "n1");
        assertThat(command.index(), is("users"));
        assertThat(command.shardId(), is(0));
        assertThat(command.node(), is("n1"));
    }

    @Test
    public void testRerouteCancelShardWithLiterals() {
        CancelAllocationCommand command = analyze(
            "ALTER TABLE users REROUTE CANCEL SHARD 0 ON 'n2'");
        assertThat(command.index(), is("users"));
        assertThat(command.shardId(), is(0));
        assertThat(command.node(), is("n2"));
        assertThat(command.allowPrimary(), is(false));
    }

    @Test
    public void testRerouteCancelShardWithParameters() {
        CancelAllocationCommand command = analyze(
            "ALTER TABLE users REROUTE CANCEL SHARD 0 ON ?", "n2");
        assertThat(command.index(), is("users"));
        assertThat(command.shardId(), is(0));
        assertThat(command.node(), is("n2"));
        assertThat(command.allowPrimary(), is(false));
    }

    @Test
    public void testRerouteCancelShardWithOptions() {
        CancelAllocationCommand command = analyze(
            "ALTER TABLE users REROUTE CANCEL SHARD 0 ON 'n1' WITH (allow_primary = TRUE)");
        assertThat(command.index(), is("users"));
        assertThat(command.shardId(), is(0));
        assertThat(command.node(), is("n1"));
        assertThat(command.allowPrimary(), is(true));

        command = analyze(
            "ALTER TABLE users REROUTE CANCEL SHARD 0 ON 'n2' WITH (allow_primary = FALSE)");
        assertThat(command.index(), is("users"));
        assertThat(command.shardId(), is(0));
        assertThat(command.node(), is("n2"));
        assertThat(command.allowPrimary(), is(false));
    }

    @Test
    public void test_promote_replica_shard_with_literals() {
        AllocateStalePrimaryAllocationCommand command = analyze(
            "ALTER TABLE users REROUTE PROMOTE REPLICA SHARD 2 ON 'n1' WITH (accept_data_loss = true)");
        assertThat(command.index(), is("users"));
        assertThat(command.shardId(), is(2));
        assertThat(command.node(), is("n1"));
        assertThat(command.acceptDataLoss(), is(true));
    }

    @Test
    public void test_promote_replica_shard_with_parameters() {
        AllocateStalePrimaryAllocationCommand command = analyze(
            "ALTER TABLE users REROUTE PROMOTE REPLICA SHARD 2 ON ? WITH (accept_data_loss = ?)", "n1", true);
        assertThat(command.index(), is("users"));
        assertThat(command.shardId(), is(2));
        assertThat(command.node(), is("n1"));
        assertThat(command.acceptDataLoss(), is(true));
    }

    @Test
    public void test_promote_replica_fails_if_unsupported_option_is_provided() {
        expectedException.expectMessage("Unsupported options provided to REROUTE PROMOTE REPLICA: [foobar]");
        analyze("ALTER TABLE users REROUTE PROMOTE REPLICA SHARD ? ON ? WITH (foobar = true)");
    }

    @Test
    public void test_accept_data_loss_defaults_to_false_if_not_provided() {
        AllocateStalePrimaryAllocationCommand command = analyze(
            "ALTER TABLE users REROUTE PROMOTE REPLICA SHARD 0 ON 'n1'");
        assertThat(command.acceptDataLoss(), is(false));
    }
}
