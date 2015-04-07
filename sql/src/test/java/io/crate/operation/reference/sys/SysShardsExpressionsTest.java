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
import io.crate.metadata.shard.MetaDataShardModule;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operation.reference.sys.cluster.SysClusterExpressionModule;
import io.crate.operation.reference.sys.shard.*;
import io.crate.test.integration.CrateUnitTest;
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
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.store.StoreStats;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("ConstantConditions")
public class SysShardsExpressionsTest extends CrateUnitTest {

    private Injector injector;
    private ReferenceResolver resolver;
    private ReferenceInfos referenceInfos;

    private String indexName = "wikipedia_de";

    class TestModule extends AbstractModule {

        @SuppressWarnings("unchecked")
        @Override
        protected void configure() {
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


    @Before
    public void prepare() throws Exception {
        injector = new ModulesBuilder().add(
                new TestModule(),
                new MetaDataModule(),
                new MetaDataSysModule(),
                new SysClusterExpressionModule(),
                new MetaDataShardModule(),
                new SysShardExpressionModule()
        ).createInjector();
        resolver = injector.getInstance(ShardReferenceResolver.class);
        referenceInfos = injector.getInstance(ReferenceInfos.class);
    }

    @Test
    public void testShardInfoLookup() throws Exception {
        ReferenceInfo info = SysShardsTableInfo.INFOS.get(new ColumnIdent("id"));
        assertEquals(info, referenceInfos.getTableInfo(SysShardsTableInfo.IDENT).getReferenceInfo(info.ident().columnIdent()));
    }

    @Test
    public void testClusterExpression() throws Exception {
        // Looking up cluster wide expressions must work too
        ReferenceIdent ident = new ReferenceIdent(SysClusterTableInfo.IDENT, "name");
        SysExpression<BytesRef> name = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("crate"), name.value());
    }

    @Test
    public void testId() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, "id");
        SysExpression<Integer> shardExpression = (SysExpression<Integer>) resolver.getImplementation(ident);
        assertEquals(new Integer(1), shardExpression.value());
    }

    @Test
    public void testSize() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, "size");
        SysExpression<Long> shardExpression = (SysExpression<Long>) resolver.getImplementation(ident);
        assertEquals(new Long(123456), shardExpression.value());
    }

    @Test
    public void testNumDocs() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, "num_docs");
        SysExpression<Long> shardExpression = (SysExpression<Long>) resolver.getImplementation(ident);
        assertEquals(new Long(654321), shardExpression.value());

        // second call should throw Exception
        assertNull(shardExpression.value());
    }

    @Test
    public void testState() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, "state");
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("STARTED"), shardExpression.value());
    }

    @Test
    public void testPrimary() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, "primary");
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(true, shardExpression.value());
    }

    @Test
    public void testRelocatingNode() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, "relocating_node");
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("node_X"), shardExpression.value());
    }

    @Test
    public void testTableName() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, ShardTableNameExpression.NAME);
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("wikipedia_de"), shardExpression.value());
    }

    @Test
    public void testTableNameOfPartition() throws Exception {
        // expression should return the real table name
        indexName = PartitionName.PARTITIONED_TABLE_PREFIX + ".wikipedia_de._1";
        prepare();
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, ShardTableNameExpression.NAME);
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("wikipedia_de"), shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testPartitionIdent() throws Exception {
        indexName = PartitionName.PARTITIONED_TABLE_PREFIX + ".wikipedia_de._1";
        prepare();
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, ShardPartitionIdentExpression.NAME);
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("_1"), shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testPartitionIdentOfNonPartition() throws Exception {
        // expression should return NULL on non partitioned tables
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, ShardPartitionIdentExpression.NAME);
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef(""), shardExpression.value());
    }

    @Test
    public void testOrphanPartition() throws Exception {
        indexName = PartitionName.PARTITIONED_TABLE_PREFIX + ".wikipedia_de._1";
        prepare();
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, ShardPartitionOrphanedExpression.NAME);
        SysExpression<Boolean> shardExpression = (SysExpression<Boolean>) resolver.getImplementation(ident);
        assertEquals(true, shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testSchemaName() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, ShardSchemaNameExpression.NAME);
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("doc"), shardExpression.value());
    }

    @Test
    public void testCustomSchemaName() throws Exception {
        indexName = "my_schema.wikipedia_de";
        prepare();
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, ShardSchemaNameExpression.NAME);
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("my_schema"), shardExpression.value());
        // reset indexName
        indexName = "wikipedia_de";
    }

    @Test
    public void testTableNameOfCustomSchema() throws Exception {
        // expression should return the real table name
        indexName = "my_schema.wikipedia_de";
        prepare();
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, ShardTableNameExpression.NAME);
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("wikipedia_de"), shardExpression.value());

        // reset indexName
        indexName = "wikipedia_de";
    }
}
