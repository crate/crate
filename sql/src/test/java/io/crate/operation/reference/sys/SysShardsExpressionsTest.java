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

package io.crate.operation.reference.sys;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.doc.TestingDocTableInfoFactory;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.operation.reference.sys.shard.ShardPathExpression;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.EngineClosedException;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.cluster.NoopClusterService;
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
import static org.mockito.Mockito.*;

@SuppressWarnings("ConstantConditions")
public class SysShardsExpressionsTest extends CrateUnitTest {

    private ReferenceResolver<?> resolver;
    private String indexName = "wikipedia_de";
    private IndexShard indexShard;
    private Schemas schemas;
    private Functions functions;
    private UserDefinedFunctionService udfService;

    @Before
    public void prepare() throws Exception {
        ClusterService clusterService = new NoopClusterService();
        indexShard = mockIndexShard();
        functions = getFunctions();
        udfService = new UserDefinedFunctionService(clusterService);
        schemas = new Schemas(
            Settings.EMPTY,
            ImmutableMap.of("sys", new SysSchemaInfo(clusterService)),
            clusterService,
            new DocSchemaInfoFactory(new TestingDocTableInfoFactory(Collections.emptyMap()), functions, udfService)
        );
        resolver = ShardReferenceResolver.create(
            clusterService,
            schemas,
            indexShard
        );
    }

    private IndexShard mockIndexShard() {
        IndexService indexService = mock(IndexService.class);
        Index index = new Index(indexName);
        ShardId shardId = new ShardId(indexName, 1);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexService.index()).thenReturn(index);
        when(indexShard.indexService()).thenReturn(indexService);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);

        StoreStats storeStats = mock(StoreStats.class);
        when(storeStats.getSizeInBytes()).thenReturn(123456L);
        when(indexShard.storeStats()).thenReturn(storeStats);

        Path dataPath = Paths.get("/dummy/" + indexName + "/1");
        when(indexShard.shardPath()).thenReturn(new ShardPath(false, dataPath, dataPath, "123", shardId));

        DocsStats docsStats = new DocsStats(654321L, 0L);
        when(indexShard.docStats()).thenReturn(docsStats).thenThrow(IllegalIndexShardStateException.class);

        ShardRouting shardRouting = ShardRouting.newUnassigned(
            index.name(), shardId.id(), null, true, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        ShardRoutingHelper.initialize(shardRouting, "node1");
        ShardRoutingHelper.moveToStarted(shardRouting);
        ShardRoutingHelper.relocate(shardRouting, "node_X");
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.minimumCompatibleVersion()).thenReturn(Version.LATEST);

        RecoveryState recoveryState = mock(RecoveryState.class);
        when(indexShard.recoveryState()).thenReturn(recoveryState);
        RecoveryState.Index recoveryStateIndex = mock(RecoveryState.Index.class);
        RecoveryState.Timer recoveryStateTimer = mock(RecoveryState.Timer.class);
        when(recoveryState.getIndex()).thenReturn(recoveryStateIndex);
        when(recoveryState.getStage()).thenReturn(RecoveryState.Stage.DONE);
        when(recoveryState.getTimer()).thenReturn(recoveryStateTimer);
        when(recoveryState.getType()).thenReturn(RecoveryState.Type.REPLICA);

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
    public void testShardInfoLookup() throws Exception {
        Reference info = new Reference(SysShardsTableInfo.ReferenceIdents.ID, RowGranularity.SHARD, IntegerType.INSTANCE);
        assertEquals(info, schemas.getTableInfo(SysShardsTableInfo.IDENT).getReference(SysShardsTableInfo.Columns.ID));
    }

    @Test
    public void testPathExpression() throws Exception {
        ShardPathExpression shardPathExpression = new ShardPathExpression(indexShard);
        assertThat(shardPathExpression.value().utf8ToString(), is(resolveCanonicalString("/dummy/wikipedia_de/1")));
    }

    @Test
    public void testId() throws Exception {
        Reference refInfo = refInfo("sys.shards.id", DataTypes.INTEGER, RowGranularity.SHARD);
        ReferenceImplementation<Integer> shardExpression = (ReferenceImplementation<Integer>) resolver.getImplementation(refInfo);
        assertEquals(Integer.valueOf(1), shardExpression.value());
    }

    @Test
    public void testSize() throws Exception {
        Reference refInfo = refInfo("sys.shards.size", DataTypes.LONG, RowGranularity.SHARD);
        ReferenceImplementation<Long> shardExpression = (ReferenceImplementation<Long>) resolver.getImplementation(refInfo);
        assertEquals(Long.valueOf(123456), shardExpression.value());
    }

    @Test
    public void testNumDocs() throws Exception {
        Reference refInfo = refInfo("sys.shards.num_docs", DataTypes.LONG, RowGranularity.SHARD);
        ReferenceImplementation<Long> shardExpression = (ReferenceImplementation<Long>) resolver.getImplementation(refInfo);
        assertEquals(Long.valueOf(654321), shardExpression.value());

        // second call should throw Exception
        assertNull(shardExpression.value());
    }

    @Test
    public void testState() throws Exception {
        Reference refInfo = refInfo("sys.shards.state", DataTypes.STRING, RowGranularity.SHARD);
        ReferenceImplementation<BytesRef> shardExpression = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("STARTED"), shardExpression.value());
    }

    @Test
    public void testRoutingState() throws Exception {
        Reference refInfo = refInfo("sys.shards.routing_state", DataTypes.STRING, RowGranularity.SHARD);
        ReferenceImplementation<BytesRef> shardExpression = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("RELOCATING"), shardExpression.value());
    }

    @Test
    public void testPrimary() throws Exception {
        Reference refInfo = refInfo("sys.shards.primary", DataTypes.BOOLEAN, RowGranularity.SHARD);
        ReferenceImplementation<BytesRef> shardExpression = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(true, shardExpression.value());
    }

    @Test
    public void testRelocatingNode() throws Exception {
        Reference refInfo = refInfo("sys.shards.relocating_node", DataTypes.STRING, RowGranularity.CLUSTER);
        ReferenceImplementation<BytesRef> shardExpression = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("node_X"), shardExpression.value());
    }

    @Test
    public void testTableName() throws Exception {
        Reference refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        ReferenceImplementation<BytesRef> shardExpression = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("wikipedia_de"), shardExpression.value());
    }

    @Test
    public void testMinLuceneVersion() throws Exception {
        Reference refInfo = refInfo("sys.shards.min_lucene_version", DataTypes.STRING, RowGranularity.SHARD);
        ReferenceImplementation<BytesRef> shardExpression =
            (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef(Version.LATEST.toString()), shardExpression.value());

        doThrow(new EngineClosedException(indexShard.shardId())).when(indexShard).minimumCompatibleVersion();
        assertThat(shardExpression.value(), nullValue());
    }

    @Test
    public void testTableNameOfPartition() throws Exception {
        // expression should return the real table name
        indexName = PartitionName.PARTITIONED_TABLE_PREFIX + ".wikipedia_de._1";
        prepare();
        Reference refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        ReferenceImplementation<BytesRef> shardExpression = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("wikipedia_de"), shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testPartitionIdent() throws Exception {
        indexName = PartitionName.PARTITIONED_TABLE_PREFIX + ".wikipedia_de._1";
        prepare();
        Reference refInfo = refInfo("sys.shards.partition_ident", DataTypes.STRING, RowGranularity.SHARD);
        ReferenceImplementation<BytesRef> shardExpression = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("_1"), shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testPartitionIdentOfNonPartition() throws Exception {
        // expression should return NULL on non partitioned tables
        Reference refInfo = refInfo("sys.shards.partition_ident", DataTypes.STRING, RowGranularity.SHARD);
        ReferenceImplementation<BytesRef> shardExpression = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef(""), shardExpression.value());
    }

    @Test
    public void testOrphanPartition() throws Exception {
        indexName = PartitionName.PARTITIONED_TABLE_PREFIX + ".wikipedia_de._1";
        prepare();
        Reference refInfo = refInfo("sys.shards.orphan_partition", DataTypes.STRING, RowGranularity.SHARD);
        ReferenceImplementation<Boolean> shardExpression = (ReferenceImplementation<Boolean>) resolver.getImplementation(refInfo);
        assertEquals(true, shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testSchemaName() throws Exception {
        Reference refInfo = refInfo("sys.shards.schema_name", DataTypes.STRING, RowGranularity.SHARD);
        ReferenceImplementation<BytesRef> shardExpression = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("doc"), shardExpression.value());
    }

    @Test
    public void testCustomSchemaName() throws Exception {
        indexName = "my_schema.wikipedia_de";
        prepare();
        Reference refInfo = refInfo("sys.shards.schema_name", DataTypes.STRING, RowGranularity.SHARD);
        ReferenceImplementation<BytesRef> shardExpression = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("my_schema"), shardExpression.value());
        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testTableNameOfCustomSchema() throws Exception {
        // expression should return the real table name
        indexName = "my_schema.wikipedia_de";
        prepare();
        Reference refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        ReferenceImplementation<BytesRef> shardExpression = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("wikipedia_de"), shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testRecoveryShardField() throws Exception {
        Reference refInfo = refInfo("sys.shards.recovery", DataTypes.OBJECT, RowGranularity.SHARD);
        NestedObjectExpression ref = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> recovery = ref.value();
        assertEquals(RecoveryState.Stage.DONE.name(), recovery.get("stage"));
        assertEquals(RecoveryState.Type.REPLICA.name(), recovery.get("type"));
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
}
