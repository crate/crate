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

package io.crate.expression.reference.sys;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static io.crate.testing.TestingHelpers.refInfo;
import static io.crate.testing.TestingHelpers.resolveCanonicalString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.data.Input;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SystemTable;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.metadata.view.ViewInfoFactory;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;

public class SysShardsExpressionsTest extends CrateDummyClusterServiceUnitTest {

    private ReferenceResolver<?> resolver;
    private String indexName = "wikipedia_de";
    private IndexShard indexShard;
    private Schemas schemas;
    private String indexUUID;
    private SystemTable<ShardRowContext> sysShards;

    @Before
    public void prepare() {
        NodeContext nodeCtx = createNodeContext();
        indexShard = mockIndexShard();
        schemas = new Schemas(
            Map.of("sys", new SysSchemaInfo(this.clusterService, List::of)),
            clusterService,
            new DocSchemaInfoFactory(
                new DocTableInfoFactory(nodeCtx),
                new ViewInfoFactory(new RelationAnalyzer(nodeCtx))
            ),
            List::of
        );
        resolver = new ShardReferenceResolver(schemas, new ShardRowContext(indexShard, clusterService));
        sysShards = schemas.getTableInfo(SysShardsTableInfo.IDENT);
    }

    private IndexShard mockIndexShard() {
        IndexService indexService = mock(IndexService.class);
        indexUUID = UUIDs.randomBase64UUID();
        Index index = new Index(indexName, indexUUID);
        ShardId shardId = new ShardId(indexName, indexUUID, 1);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexService.index()).thenReturn(index);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);

        StoreStats storeStats = new StoreStats(123456L, 0L);
        when(indexShard.storeStats()).thenReturn(storeStats);

        Path dataPath = Paths.get("/dummy/" + indexUUID + "/" + shardId.id());
        when(indexShard.shardPath()).thenReturn(new ShardPath(false, dataPath, dataPath, shardId));

        DocsStats docsStats = new DocsStats(654321L, 0L, 200L);
        when(indexShard.docStats()).thenReturn(docsStats).thenThrow(IllegalIndexShardStateException.class);

        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId, true, RecoverySource.PeerRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        shardRouting = ShardRoutingHelper.initialize(shardRouting, "node1");
        shardRouting = ShardRoutingHelper.moveToStarted(shardRouting);
        shardRouting = ShardRoutingHelper.relocate(shardRouting, "node_X");
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.minimumCompatibleVersion()).thenReturn(Version.LATEST);

        RecoveryState recoveryState = mock(RecoveryState.class);
        when(indexShard.recoveryState()).thenReturn(recoveryState);
        RecoveryState.Index recoveryStateIndex = mock(RecoveryState.Index.class);
        RecoveryState.Timer recoveryStateTimer = mock(RecoveryState.Timer.class);
        when(recoveryState.getRecoverySource()).thenReturn(RecoverySource.PeerRecoverySource.INSTANCE);
        when(recoveryState.getIndex()).thenReturn(recoveryStateIndex);
        when(recoveryState.getStage()).thenReturn(RecoveryState.Stage.DONE);
        when(recoveryState.getTimer()).thenReturn(recoveryStateTimer);

        when(recoveryStateIndex.totalBytes()).thenReturn(2048L);
        when(recoveryStateIndex.reusedBytes()).thenReturn(1024L);
        when(recoveryStateIndex.recoveredBytes()).thenReturn(1024L);
        when(recoveryStateIndex.totalFileCount()).thenReturn(2);
        when(recoveryStateIndex.reusedFileCount()).thenReturn(1);
        when(recoveryStateIndex.recoveredFileCount()).thenReturn(1);
        when(recoveryStateTimer.time()).thenReturn(10000L);

        return indexShard;
    }

    @Test
    public void testPathExpression() throws Exception {
        Reference refInfo = refInfo("sys.shards.path", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo(resolveCanonicalString("/dummy/" + indexUUID + "/1"));
    }

    @Test
    public void testId() throws Exception {
        Reference refInfo = refInfo("sys.shards.id", DataTypes.INTEGER, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo(1);
    }

    @Test
    public void testSize() throws Exception {
        Reference refInfo = refInfo("sys.shards.size", DataTypes.LONG, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo(123456L);
    }

    @Test
    public void testNumDocs() throws Exception {
        Reference refInfo = refInfo("sys.shards.num_docs", DataTypes.LONG, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo(654321L);

        // second call should throw Exception
        assertNull(shardExpression.value());
    }

    @Test
    public void testState() throws Exception {
        Reference refInfo = refInfo("sys.shards.state", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo("STARTED");
    }

    @Test
    public void testRoutingState() throws Exception {
        Reference refInfo = refInfo("sys.shards.routing_state", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo("RELOCATING");
    }

    @Test
    public void testPrimary() throws Exception {
        Reference refInfo = refInfo("sys.shards.primary", DataTypes.BOOLEAN, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo(true);
    }

    @Test
    public void testRelocatingNode() throws Exception {
        Reference refInfo = refInfo("sys.shards.relocating_node", DataTypes.STRING, RowGranularity.CLUSTER);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo("node_X");
    }

    @Test
    public void testTableName() throws Exception {
        Reference refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo("wikipedia_de");
    }

    @Test
    public void testMinLuceneVersion() throws Exception {
        Reference refInfo = refInfo("sys.shards.min_lucene_version", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo(Version.LATEST.toString());

        doThrow(new AlreadyClosedException("Already closed")).when(indexShard).minimumCompatibleVersion();
        shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isNull();
    }

    @Test
    public void testTableNameOfPartition() throws Exception {
        // expression should return the real table name
        indexName = IndexParts.toIndexName("doc", "wikipedia_de", "foo");
        prepare();
        Reference refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo("wikipedia_de");

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testPartitionIdent() throws Exception {
        indexName = IndexParts.toIndexName("doc", "wikipedia_d1", "foo");
        prepare();
        Reference refInfo = refInfo("sys.shards.partition_ident", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo("foo");

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testPartitionIdentOfNonPartition() throws Exception {
        // expression should return NULL on non partitioned tables
        Reference refInfo = refInfo("sys.shards.partition_ident", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo("");
    }

    @Test
    public void testOrphanPartition() throws Exception {
        indexName = IndexParts.toIndexName("doc", "wikipedia_d1", "foo");
        prepare();
        Reference refInfo = refInfo("sys.shards.orphan_partition", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo(true);
        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testSchemaName() throws Exception {
        Reference refInfo = refInfo("sys.shards.schema_name", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo("doc");
    }

    @Test
    public void testCustomSchemaName() throws Exception {
        indexName = "my_schema.wikipedia_de";
        prepare();
        Reference refInfo = refInfo("sys.shards.schema_name", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo("my_schema");
        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testTableNameOfCustomSchema() throws Exception {
        // expression should return the real table name
        indexName = "my_schema.wikipedia_de";
        prepare();
        Reference refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        Input<?> shardExpression = resolver.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo("wikipedia_de");

        // reset indexName
        indexName = "wikipedia_de";
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRecoveryShardField() throws Exception {
        Reference refInfo = refInfo("sys.shards.recovery", DataTypes.UNTYPED_OBJECT, RowGranularity.SHARD);
        Input<?> ref = resolver.getImplementation(refInfo);
        assertThat(ref).isInstanceOf(NestableInput.class);
        assertThat(ref.value()).isInstanceOf(Map.class);
        Map<String, Object> recovery = (Map<String, Object>) ref.value();
        assertThat(recovery.get("stage")).isEqualTo(RecoveryState.Stage.DONE.name());
        assertThat(recovery.get("total_time")).isEqualTo(10_000L);

        Map<String, Object> expectedFiles = Map.of(
            "used", 2,
            "reused", 1,
            "recovered", 1,
            "percent", 0.0f);
        assertThat(recovery.get("files")).isEqualTo(expectedFiles);

        Map<String, Object> expectedBytes = Map.of(
            "used", 2_048L,
            "reused", 1_024L,
            "recovered", 1_024L,
            "percent", 0.0f);
        assertThat(recovery.get("size")).isEqualTo(expectedBytes);
    }

    @Test
    public void test_recovery_type_is_null_if_recovery_state_is_null() {
        when(indexShard.recoveryState()).thenReturn(null);

        var ref = sysShards.getReference(ColumnIdent.of("recovery", "type"));
        var input = resolver.getImplementation(ref);
        assertThat(input.value()).isNull();
    }

    @Test
    public void testShardSizeExpressionWhenIndexShardHasBeenClosed() {
        IndexShard mock = mockIndexShard();
        when(mock.storeStats()).thenThrow(new AlreadyClosedException("shard already closed"));

        ShardReferenceResolver res = new ShardReferenceResolver(schemas, new ShardRowContext(mock, clusterService));
        Reference refInfo = refInfo("sys.shards.size", DataTypes.LONG, RowGranularity.SHARD);
        Input<?> shardExpression = res.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isEqualTo(0L);
    }

    @Test
    public void test_retention_lease_is_null_on_index_shard_closed_exception() throws Exception {
        IndexShard mock = mockIndexShard();
        var shardId = mock.shardId();
        doThrow(new IndexShardClosedException(shardId)).when(mock).getRetentionLeaseStats();

        ShardReferenceResolver res = new ShardReferenceResolver(schemas, new ShardRowContext(mock, clusterService));
        Reference refInfo = refInfo("sys.shards.retention_leases", DataTypes.LONG, RowGranularity.SHARD, "version");
        Input<?> shardExpression = res.getImplementation(refInfo);
        assertThat(shardExpression).isInstanceOf(NestableInput.class);
        assertThat(shardExpression.value()).isNull();
    }
}
