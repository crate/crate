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

import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.sql.tree.Literal;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;

public class AlterTableRerouteAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        TableIdent myBlobsIdent = new TableIdent(BlobSchemaInfo.NAME, "blobs");
        TestingBlobTableInfo myBlobsTableInfo = TableDefinitions.createBlobTable(myBlobsIdent);
        e = SQLExecutor.builder(clusterService).addBlobTable(myBlobsTableInfo).enableDefaultTables().build();
    }

    @Test
    public void testRerouteOnSystemTableIsNotAllowed() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("The relation \"sys.cluster\" doesn't support or allow ALTER REROUTE operations, as it is read-only.");
        e.analyze("ALTER TABLE sys.cluster REROUTE MOVE SHARD 0 FROM 'node1' TO 'node2'");
    }

    @Test
    public void testRerouteMoveShard() throws Exception {
        RerouteMoveShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE users REROUTE MOVE SHARD 0 FROM 'nodeOne' TO 'nodeTwo'");
        assertThat(analyzed.tableInfo().concreteIndices().length, is(1));
        assertThat(analyzed.tableInfo().concreteIndices()[0], is("users"));
        assertThat(analyzed.shardId(), is(Literal.fromObject(0)));
        assertThat(analyzed.fromNodeId(), is(Literal.fromObject("nodeOne")));
        assertThat(analyzed.toNodeId(), is(Literal.fromObject("nodeTwo")));
        assertThat(analyzed.isWriteOperation(), is(true));
    }

    @Test
    public void testRerouteMoveShardPartitionedTable() throws Exception {
        RerouteMoveShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE parted PARTITION (date = 1395874800000) REROUTE MOVE SHARD 0 FROM 'nodeOne' TO 'nodeTwo'");
        assertTrue(Arrays.asList(analyzed.tableInfo().concreteIndices()).contains(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
        assertFalse(analyzed.partitionProperties().isEmpty());
    }

    @Test
    public void testRerouteOnBlobTable() throws Exception {
        RerouteMoveShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE blob.blobs REROUTE MOVE SHARD 0 FROM 'nodeOne' TO 'nodeTwo'");
        assertThat(analyzed.tableInfo().concreteIndices().length, is(1));
        assertThat(analyzed.tableInfo().concreteIndices()[0], is("blob.blobs"));
        assertThat(analyzed.isWriteOperation(), is(true));
    }

    @Test
    public void testRerouteAllocateReplicaShard() throws Exception {
        RerouteAllocateReplicaShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE users REROUTE ALLOCATE REPLICA SHARD 0 ON 'nodeOne'");
        assertThat(analyzed.tableInfo().concreteIndices().length, is(1));
        assertThat(analyzed.tableInfo().concreteIndices()[0], is("users"));
        assertThat(analyzed.shardId(), is(Literal.fromObject(0)));
        assertThat(analyzed.nodeId(), is(Literal.fromObject("nodeOne")));
        assertThat(analyzed.isWriteOperation(), is(true));
    }

    @Test
    public void testRerouteCancelShard() throws Exception {
        RerouteCancelShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE users REROUTE CANCEL SHARD 0 ON 'nodeOne'");
        assertThat(analyzed.tableInfo().concreteIndices().length, is(1));
        assertThat(analyzed.tableInfo().concreteIndices()[0], is("users"));
        assertThat(analyzed.shardId(), is(Literal.fromObject(0)));
        assertThat(analyzed.nodeId(), is(Literal.fromObject("nodeOne")));
        assertNull(analyzed.properties().get("allow_primary"));
        assertThat(analyzed.isWriteOperation(), is(true));
    }

    @Test
    public void testRerouteCancelShardWithOptions() throws Exception {
        RerouteCancelShardAnalyzedStatement analyzed = e.analyze("ALTER TABLE users REROUTE CANCEL SHARD 0 ON 'nodeOne' WITH (allow_primary = TRUE)");
        assertThat(analyzed.properties().get("allow_primary"), is(Literal.fromObject(true)));
        analyzed = e.analyze("ALTER TABLE users REROUTE CANCEL SHARD 0 ON 'nodeOne' WITH (allow_primary = FALSE)");
        assertThat(analyzed.properties().get("allow_primary"), is(Literal.fromObject(false)));
    }
}
