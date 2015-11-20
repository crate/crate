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

import io.crate.metadata.*;
import io.crate.metadata.shard.DynamicShardReferenceResolver;
import io.crate.metadata.shard.MetaDataShardModule;
import io.crate.metadata.shard.ShardReferenceImplementation;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.cluster.SysClusterExpressionModule;
import io.crate.operation.reference.sys.shard.SysShardExpressionModule;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.refInfo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("ConstantConditions")
public class SysShardsExpressionsTest extends CrateUnitTest {

    private AbstractReferenceResolver resolver;
    private Schemas schemas;

    private String indexName = "wikipedia_de";
    private static ThreadPool threadPool = new ThreadPool("testing");

    @Before
    public void prepare() throws Exception {
        Injector injector = new ModulesBuilder().add(
                new TestModule(),
                new MetaDataModule(),
                new MetaDataSysModule(),
                new SysClusterExpressionModule(),
                new MetaDataShardModule(),
                new SysShardExpressionModule()
        ).createInjector();
        AbstractReferenceResolver shardRefResolver = injector.getInstance(ShardReferenceResolver.class);
        IndexShard indexShard = injector.getInstance(IndexShard.class);
        resolver = new DynamicShardReferenceResolver(shardRefResolver, indexShard);
        schemas = injector.getInstance(Schemas.class);
    }

    @AfterClass
    public static void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
        threadPool =  null;
    }

    class TestModule extends AbstractModule {

        @SuppressWarnings("unchecked")
        @Override
        protected void configure() {
            bind(ThreadPool.class).toInstance(threadPool);
            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

            ClusterService clusterService = mock(ClusterService.class);
            bind(ClusterService.class).toInstance(clusterService);

            ClusterName clusterName = mock(ClusterName.class);
            when(clusterName.value()).thenReturn("crate");
            bind(ClusterName.class).toInstance(clusterName);

            Index index = new Index(SysShardsTableInfo.IDENT.name());
            bind(Index.class).toInstance(index);

            ShardId shardId = mock(ShardId.class);
            when(shardId.getId()).thenReturn(1);
            when(shardId.getIndex()).thenAnswer(new Answer<String>() {
                @Override
                public String answer(InvocationOnMock invocation) throws Throwable {
                    return indexName;
                }
            });
            bind(ShardId.class).toInstance(shardId);

            IndexShard indexShard = mock(IndexShard.class);
            bind(IndexShard.class).toInstance(indexShard);

            StoreStats storeStats = mock(StoreStats.class);
            when(indexShard.storeStats()).thenReturn(storeStats);
            when(storeStats.getSizeInBytes()).thenReturn(123456L);

            DocsStats docsStats = mock(DocsStats.class);
            when(indexShard.docStats()).thenReturn(docsStats).thenThrow(IllegalIndexShardStateException.class);
            when(docsStats.getCount()).thenReturn(654321L);

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

            ShardRouting shardRouting = mock(ShardRouting.class);
            when(indexShard.routingEntry()).thenReturn(shardRouting);
            when(shardRouting.primary()).thenReturn(true);
            when(shardRouting.relocatingNodeId()).thenReturn("node_X");

            TransportPutIndexTemplateAction transportPutIndexTemplateAction = mock(TransportPutIndexTemplateAction.class);
            bind(TransportPutIndexTemplateAction.class).toInstance(transportPutIndexTemplateAction);

            when(indexShard.state()).thenReturn(IndexShardState.STARTED);

            MetaData metaData = mock(MetaData.class);
            when(metaData.hasConcreteIndex(PartitionName.PARTITIONED_TABLE_PREFIX + ".wikipedia_de._1")).thenReturn(false);
            when(metaData.concreteAllOpenIndices()).thenReturn(new String[0]);
            when(metaData.templates()).thenReturn(ImmutableOpenMap.<String, IndexTemplateMetaData>of());
            ClusterState clusterState = mock(ClusterState.class);
            when(clusterService.state()).thenReturn(clusterState);
            when(clusterState.metaData()).thenReturn(metaData);

        }
    }

    @Test
    public void testShardInfoLookup() throws Exception {
        ReferenceInfo info = new ReferenceInfo(SysShardsTableInfo.ReferenceIdents.ID, RowGranularity.SHARD, IntegerType.INSTANCE);
        assertEquals(info, schemas.getTableInfo(SysShardsTableInfo.IDENT).getReferenceInfo(SysShardsTableInfo.Columns.ID));
    }

    @Test
    public void testClusterExpression() throws Exception {
        // Looking up cluster wide expressions must work too
        ReferenceInfo refInfo = refInfo("sys.cluster.name", DataTypes.STRING, RowGranularity.CLUSTER);
        ReferenceImplementation<BytesRef> name = (ReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("crate"), name.value());
    }

    @Test
    public void testId() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.shards.id", DataTypes.INTEGER, RowGranularity.SHARD);
        ShardReferenceImplementation<Integer> shardExpression = (ShardReferenceImplementation<Integer>) resolver.getImplementation(refInfo);
        assertEquals(new Integer(1), shardExpression.value());
    }

    @Test
    public void testSize() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.shards.size", DataTypes.LONG, RowGranularity.SHARD);
        ShardReferenceImplementation<Long> shardExpression = (ShardReferenceImplementation<Long>) resolver.getImplementation(refInfo);
        assertEquals(new Long(123456), shardExpression.value());
    }

    @Test
    public void testNumDocs() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.shards.num_docs", DataTypes.LONG, RowGranularity.SHARD);
        ShardReferenceImplementation<Long> shardExpression = (ShardReferenceImplementation<Long>) resolver.getImplementation(refInfo);
        assertEquals(new Long(654321), shardExpression.value());

        // second call should throw Exception
        assertNull(shardExpression.value());
    }

    @Test
    public void testState() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.shards.state", DataTypes.STRING, RowGranularity.SHARD);
        ShardReferenceImplementation<BytesRef> shardExpression = (ShardReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("STARTED"), shardExpression.value());
    }

    @Test
    public void testPrimary() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.shards.primary", DataTypes.BOOLEAN, RowGranularity.SHARD);
        ShardReferenceImplementation<BytesRef> shardExpression = (ShardReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(true, shardExpression.value());
    }

    @Test
    public void testRelocatingNode() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.shards.relocating_node", DataTypes.STRING, RowGranularity.CLUSTER);
        ShardReferenceImplementation<BytesRef> shardExpression = (ShardReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("node_X"), shardExpression.value());
    }

    @Test
    public void testTableName() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        ShardReferenceImplementation<BytesRef> shardExpression = (ShardReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("wikipedia_de"), shardExpression.value());
    }

    @Test
    public void testTableNameOfPartition() throws Exception {
        // expression should return the real table name
        indexName = PartitionName.PARTITIONED_TABLE_PREFIX + ".wikipedia_de._1";
        prepare();
        ReferenceInfo refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        ShardReferenceImplementation<BytesRef> shardExpression = (ShardReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("wikipedia_de"), shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testPartitionIdent() throws Exception {
        indexName = PartitionName.PARTITIONED_TABLE_PREFIX + ".wikipedia_de._1";
        prepare();
        ReferenceInfo refInfo = refInfo("sys.shards.partition_ident", DataTypes.STRING, RowGranularity.SHARD);
        ShardReferenceImplementation<BytesRef> shardExpression = (ShardReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("_1"), shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testPartitionIdentOfNonPartition() throws Exception {
        // expression should return NULL on non partitioned tables
        ReferenceInfo refInfo = refInfo("sys.shards.partition_ident", DataTypes.STRING, RowGranularity.SHARD);
        ShardReferenceImplementation<BytesRef> shardExpression = (ShardReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef(""), shardExpression.value());
    }

    @Test
    public void testOrphanPartition() throws Exception {
        indexName = PartitionName.PARTITIONED_TABLE_PREFIX + ".wikipedia_de._1";
        prepare();
        ReferenceInfo refInfo = refInfo("sys.shards.orphan_partition", DataTypes.STRING, RowGranularity.SHARD);
        ShardReferenceImplementation<Boolean> shardExpression = (ShardReferenceImplementation<Boolean>) resolver.getImplementation(refInfo);
        assertEquals(true, shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testSchemaName() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.shards.schema_name", DataTypes.STRING, RowGranularity.SHARD);
        ShardReferenceImplementation<BytesRef> shardExpression = (ShardReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("doc"), shardExpression.value());
    }

    @Test
    public void testCustomSchemaName() throws Exception {
        indexName = "my_schema.wikipedia_de";
        prepare();
        ReferenceInfo refInfo = refInfo("sys.shards.schema_name", DataTypes.STRING, RowGranularity.SHARD);
        ShardReferenceImplementation<BytesRef> shardExpression = (ShardReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("my_schema"), shardExpression.value());
        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testTableNameOfCustomSchema() throws Exception {
        // expression should return the real table name
        indexName = "my_schema.wikipedia_de";
        prepare();
        ReferenceInfo refInfo = refInfo("sys.shards.table_name", DataTypes.STRING, RowGranularity.SHARD);
        ShardReferenceImplementation<BytesRef> shardExpression = (ShardReferenceImplementation<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("wikipedia_de"), shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testRecoveryShardField() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.shards.recovery", DataTypes.OBJECT, RowGranularity.SHARD);
        NestedObjectExpression ref = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> recovery = ref.value();
        assertEquals(RecoveryState.Stage.DONE.name(), recovery.get("stage"));
        assertEquals(RecoveryState.Type.REPLICA.name(), recovery.get("type"));
        assertEquals(10_000L, recovery.get("total_time"));

        Map<String, Integer> expectedFiles = new HashMap<String, Integer>(){{
            put("used", 2);
            put("reused", 1);
            put("recovered", 1);
        }};
        assertEquals(expectedFiles, recovery.get("files"));

        Map<String, Long> expectedBytes = new HashMap<String, Long>(){{
            put("used", 2_048L);
            put("reused", 1_024L);
            put("recovered", 1_024L);
        }};
        assertEquals(expectedBytes, recovery.get("size"));

    }
}
