/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.expression.NestableInput;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.license.CeLicenseService;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexParts;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SystemTable;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.doc.TestingDocTableInfoFactory;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
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
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.refInfo;
import static io.crate.testing.TestingHelpers.resolveCanonicalString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysShardsExpressionsTest extends CrateDummyClusterServiceUnitTest {

    private ReferenceResolver<?> resolver;
    private String indexName = "wikipedia_de";
    private IndexShard indexShard;
    private Schemas schemas;
    private String indexUUID;
    private SystemTable<ShardRowContext> sysShards;

    @Before
    public void prepare()  {
        indexShard = mockIndexShard();
        Functions functions = getFunctions();
        CrateSettings crateSettings = new CrateSettings(clusterService, clusterService.getSettings());
        UserDefinedFunctionService udfService = new UserDefinedFunctionService(clusterService, functions);
        schemas = new Schemas(
            Map.of("sys", new SysSchemaInfo(this.clusterService, crateSettings, new CeLicenseService())),
            clusterService,
            new DocSchemaInfoFactory(new TestingDocTableInfoFactory(Collections.emptyMap()), (ident, state) -> null , functions, udfService)
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

        StoreStats storeStats = new StoreStats(123456L);
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
        NestableInput<String> shardPathExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertThat(shardPathExpression.value(), is(resolveCanonicalString("/dummy/" + indexUUID + "/1")));
    }

    @Test
    public void testId() throws Exception {
        Reference refInfo = refInfo("sys.shards.id", DataTypes.INTEGER, RowGranularity.SHARD);
        NestableInput<Integer> shardExpression = (NestableInput<Integer>) resolver.getImplementation(refInfo);
        assertEquals(Integer.valueOf(1), shardExpression.value());
    }

    @Test
    public void testSize() throws Exception {
        Reference refInfo = refInfo("sys.shards.size", DataTypes.LONG, RowGranularity.SHARD);
        NestableInput<Long> shardExpression = (NestableInput<Long>) resolver.getImplementation(refInfo);
        assertEquals(Long.valueOf(123456), shardExpression.value());
    }

    @Test
    public void testNumDocs() throws Exception {
        Reference refInfo = refInfo("sys.shards.num_docs", DataTypes.LONG, RowGranularity.SHARD);
        NestableInput<Long> shardExpression = (NestableInput<Long>) resolver.getImplementation(refInfo);
        assertEquals(Long.valueOf(654321), shardExpression.value());

        // second call should throw Exception
        assertNull(shardExpression.value());
    }

    @Test
    public void testState() throws Exception {
        Reference refInfo = refInfo("sys.shards.state", DataTypes.STRING, RowGranularity.SHARD);
        NestableInput<String> shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals("STARTED", shardExpression.value());
    }

    @Test
    public void testRoutingState() throws Exception {
        Reference refInfo = refInfo("sys.shards.routing_state", DataTypes.STRING, RowGranularity.SHARD);
        NestableInput<String> shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals("RELOCATING", shardExpression.value());
    }

    @Test
    public void testPrimary() throws Exception {
        Reference refInfo = refInfo("sys.shards.primary", DataTypes.BOOLEAN, RowGranularity.SHARD);
        NestableInput<String> shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals(true, shardExpression.value());
    }

    @Test
    public void testRelocatingNode() throws Exception {
        Reference refInfo = refInfo("sys.shards.relocating_node", DataTypes.STRING, RowGranularity.CLUSTER);
        NestableInput<String> shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals("node_X", shardExpression.value());
    }

    @Test
    public void testTableName() throws Exception {
        Reference refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        NestableInput<String> shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals("wikipedia_de", shardExpression.value());
    }

    @Test
    public void testMinLuceneVersion() throws Exception {
        Reference refInfo = refInfo("sys.shards.min_lucene_version", DataTypes.STRING, RowGranularity.SHARD);
        NestableInput<String> shardExpression =
            (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals(Version.LATEST.toString(), shardExpression.value());

        doThrow(new AlreadyClosedException("Already closed")).when(indexShard).minimumCompatibleVersion();
        shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertThat(shardExpression.value(), nullValue());
    }

    @Test
    public void testTableNameOfPartition() throws Exception {
        // expression should return the real table name
        indexName = IndexParts.toIndexName("doc", "wikipedia_de", "foo");
        prepare();
        Reference refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        NestableInput<String> shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals("wikipedia_de", shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testPartitionIdent() throws Exception {
        indexName = IndexParts.toIndexName("doc", "wikipedia_d1", "foo");
        prepare();
        Reference refInfo = refInfo("sys.shards.partition_ident", DataTypes.STRING, RowGranularity.SHARD);
        NestableInput<String> shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals("foo", shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testPartitionIdentOfNonPartition() throws Exception {
        // expression should return NULL on non partitioned tables
        Reference refInfo = refInfo("sys.shards.partition_ident", DataTypes.STRING, RowGranularity.SHARD);
        NestableInput<String> shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals("", shardExpression.value());
    }

    @Test
    public void testOrphanPartition() throws Exception {
        indexName = IndexParts.toIndexName("doc", "wikipedia_d1", "foo");
        prepare();
        Reference refInfo = refInfo("sys.shards.orphan_partition", DataTypes.STRING, RowGranularity.SHARD);
        NestableInput<Boolean> shardExpression = (NestableInput<Boolean>) resolver.getImplementation(refInfo);
        assertEquals(true, shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testSchemaName() throws Exception {
        Reference refInfo = refInfo("sys.shards.schema_name", DataTypes.STRING, RowGranularity.SHARD);
        NestableInput<String> shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals("doc", shardExpression.value());
    }

    @Test
    public void testCustomSchemaName() throws Exception {
        indexName = "my_schema.wikipedia_de";
        prepare();
        Reference refInfo = refInfo("sys.shards.schema_name", DataTypes.STRING, RowGranularity.SHARD);
        NestableInput<String> shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals("my_schema", shardExpression.value());
        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testTableNameOfCustomSchema() throws Exception {
        // expression should return the real table name
        indexName = "my_schema.wikipedia_de";
        prepare();
        Reference refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        NestableInput<String> shardExpression = (NestableInput<String>) resolver.getImplementation(refInfo);
        assertEquals("wikipedia_de", shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testRecoveryShardField() throws Exception {
        Reference refInfo = refInfo("sys.shards.recovery", DataTypes.UNTYPED_OBJECT, RowGranularity.SHARD);
        NestableInput<Map<String, Object>> ref = (NestableInput<Map<String,Object>>) resolver.getImplementation(refInfo);

        Map<String, Object> recovery = ref.value();
        assertEquals(RecoveryState.Stage.DONE.name(), recovery.get("stage"));
        assertEquals(10_000L, recovery.get("total_time"));

        Map<String, Object> expectedFiles = new HashMap<String, Object>() {{
            put("used", 2);
            put("reused", 1);
            put("recovered", 1);
            put("percent", 0.0f);
        }};
        assertEquals(expectedFiles, recovery.get("files"));

        Map<String, Object> expectedBytes = new HashMap<String, Object>() {{
            put("used", 2_048L);
            put("reused", 1_024L);
            put("recovered", 1_024L);
            put("percent", 0.0f);
        }};
        assertEquals(expectedBytes, recovery.get("size"));
    }

    @Test
    public void test_recovery_type_is_null_if_recovery_state_is_null(){
        when(indexShard.recoveryState()).thenReturn(null);

        var ref = sysShards.getReference(new ColumnIdent("recovery", "type"));
        var input = resolver.getImplementation(ref);
        assertThat(input.value(), Matchers.nullValue());
    }

    @Test
    public void testShardSizeExpressionWhenIndexShardHasBeenClosed() {
        IndexShard mock = mockIndexShard();
        when(mock.storeStats()).thenThrow(new AlreadyClosedException("shard already closed"));

        ShardReferenceResolver resolver = new ShardReferenceResolver(schemas, new ShardRowContext(mock, clusterService));
        Reference refInfo = refInfo("sys.shards.size", DataTypes.LONG, RowGranularity.SHARD);
        NestableInput<Long> shardSizeExpression = (NestableInput<Long>) resolver.getImplementation(refInfo);
        assertThat(shardSizeExpression.value(), is(0L));
    }
}
