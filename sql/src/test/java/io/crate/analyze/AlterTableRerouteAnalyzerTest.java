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

package io.crate.analyze;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class AlterTableRerouteAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testRerouteOnSystemTableIsNotAllowed() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("The relation \"sys.cluster\" doesn't support or allow ALTER operations, as it is read-only.");
        e.analyze("ALTER TABLE sys.cluster REROUTE RETRY FAILED");
    }


    @Test
    public void testRerouteRetryFailed() throws Exception {
        RerouteRetryFailedAnalyzedStatement analyzed = e.analyze("ALTER TABLE users REROUTE RETRY FAILED");
        assertThat(analyzed.tableInfo().ident().fqn(), is("doc.users"));
        assertNull(analyzed.partitionName());
        assertThat(analyzed.isWriteOperation(), is(true));
    }

    @Test
    public void testRerouteMoveShard() throws Exception {
        RerouteMoveShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE users REROUTE MOVE SHARD 0 FROM 'nodeOne' TO 'nodeTwo'");
        assertThat(analyzed.tableInfo().ident().fqn(), is("doc.users"));
        assertThat(analyzed.shardId(), is(0));
        assertThat(analyzed.fromNodeId(), is("nodeOne"));
        assertThat(analyzed.toNodeId(), is("nodeTwo"));
        assertNull(analyzed.partitionName());
        assertThat(analyzed.isWriteOperation(), is(true));
    }

    @Test
    public void testRerouteMoveShardWithParameters() throws Exception {
        RerouteMoveShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE users REROUTE MOVE SHARD ? FROM ? TO ?",
            new Object[]{ 0, "nodeOne", "nodeTwo" });
        assertThat(analyzed.shardId(), is(0));
        assertThat(analyzed.fromNodeId(), is("nodeOne"));
        assertThat(analyzed.toNodeId(), is("nodeTwo"));
    }

    @Test
    public void testRerouteMoveShardPartitionedTable() throws Exception {
        RerouteMoveShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE parted PARTITION (date = 1395874800000) REROUTE MOVE SHARD 0 FROM 'nodeOne' TO 'nodeTwo'");
        assertNotNull(analyzed.partitionName());
        assertThat(analyzed.partitionName().asIndexName(), is(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
    }

    @Test
    public void testRerouteMoveShardPartitionedTableUnknownPartition() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Referenced partition \".partitioned.parted.04732d9g60o30c1g60o30c1g\" does not exist.");
        e.analyze("ALTER TABLE parted PARTITION (date = 1500000000000) REROUTE MOVE SHARD 0 FROM 'nodeOne' TO 'nodeTwo'");
    }

    @Test
    public void testRerouteCancelShard() throws Exception {
        RerouteCancelShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE users REROUTE CANCEL SHARD 0 ON 'nodeOne'");
        assertThat(analyzed.tableInfo().ident().fqn(), is("doc.users"));
        assertThat(analyzed.shardId(), is(0));
        assertThat(analyzed.nodeId(), is("nodeOne"));
        assertNull(analyzed.partitionName());
        assertThat(analyzed.allowPrimary(), is(false));
        assertThat(analyzed.isWriteOperation(), is(true));
    }

    @Test
    public void testRerouteCancelShardWithOptions() throws Exception {
        RerouteCancelShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE users REROUTE CANCEL SHARD 0 ON 'nodeOne' WITH (allow_primary = TRUE)");
        assertThat(analyzed.allowPrimary(), is(true));
        analyzed = e.analyze("ALTER TABLE users REROUTE CANCEL SHARD 0 ON 'nodeOne' WITH (allow_primary = FALSE)");
        assertThat(analyzed.allowPrimary(), is(false));
    }

    @Test
    public void testRerouteCancelShardWithInvalidOption() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"not_allowed\" is not a valid setting for CANCEL SHARD");
        e.analyze("ALTER TABLE users REROUTE CANCEL SHARD 0 ON 'nodeOne' WITH (not_allowed = 0)");
    }

    @Test
    public void testRerouteAllocateReplicaShard() throws Exception {
        RerouteAllocateReplicaShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE users REROUTE ALLOCATE REPLICA SHARD 0 ON 'nodeOne'");
        assertThat(analyzed.tableInfo().ident().fqn(), is("doc.users"));
        assertThat(analyzed.shardId(), is(0));
        assertThat(analyzed.nodeId(), is("nodeOne"));
        assertNull(analyzed.partitionName());
        assertThat(analyzed.isWriteOperation(), is(true));
    }
}
