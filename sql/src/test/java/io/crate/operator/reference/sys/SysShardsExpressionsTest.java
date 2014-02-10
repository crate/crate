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

package io.crate.operator.reference.sys;

import io.crate.metadata.*;
import io.crate.metadata.shard.MetaDataShardModule;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operator.reference.sys.cluster.ClusterNameExpression;
import io.crate.operator.reference.sys.shard.ShardTableNameExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.store.StoreStats;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysShardsExpressionsTest {

    private Injector injector;
    private ReferenceResolver resolver;
    private ReferenceInfos referenceInfos;

    class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

            ClusterService clusterService = mock(ClusterService.class);
            bind(ClusterService.class).toInstance(clusterService);

            ClusterName clusterName = mock(ClusterName.class);
            when(clusterName.value()).thenReturn("crate");
            bind(ClusterName.class).toInstance(clusterName);

            ShardId shardId = mock(ShardId.class);
            when(shardId.getId()).thenReturn(1);
            when(shardId.getIndex()).thenReturn("wikipedia_de");
            bind(ShardId.class).toInstance(shardId);

            IndexShard indexShard = mock(IndexShard.class);
            bind(IndexShard.class).toInstance(indexShard);

            StoreStats storeStats = mock(StoreStats.class);
            when(indexShard.storeStats()).thenReturn(storeStats);
            when(storeStats.getSizeInBytes()).thenReturn(123456L);

            DocsStats docsStats = mock(DocsStats.class);
            when(indexShard.docStats()).thenReturn(docsStats);
            when(docsStats.getCount()).thenReturn(654321L);

            ShardRouting shardRouting = mock(ShardRouting.class);
            when(indexShard.routingEntry()).thenReturn(shardRouting);
            when(shardRouting.primary()).thenReturn(true);
            when(shardRouting.relocatingNodeId()).thenReturn("node_X");

            when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        }
    }


    @Before
    public void setUp() throws Exception {
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
        assertEquals(info, referenceInfos.getReferenceInfo(info.ident()));
    }

    @Test
    public void testClusterExpression() throws Exception {
        // Looking up cluster wide expressions must work too
        ReferenceIdent ident = ClusterNameExpression.INFO_NAME.ident();
        assertEquals(referenceInfos.getReferenceInfo(ident), ClusterNameExpression.INFO_NAME);

        ident = new ReferenceIdent(SysClusterTableInfo.IDENT, "name");
        SysExpression<String> name = (SysExpression<String>) resolver.getImplementation(ident);
        assertEquals(ClusterNameExpression.INFO_NAME, name.info());
        assertEquals("crate", name.value());
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
    }

    @Test
    public void testState() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, "state");
        SysExpression<String> shardExpression = (SysExpression<String>) resolver.getImplementation(ident);
        assertEquals("STARTED", shardExpression.value());
    }

    @Test
    public void testPrimary() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, "primary");
        SysExpression<String> shardExpression = (SysExpression<String>) resolver.getImplementation(ident);
        assertEquals(true, shardExpression.value());
    }

    @Test
    public void testRelocatingNode() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, "relocating_node");
        SysExpression<String> shardExpression = (SysExpression<String>) resolver.getImplementation(ident);
        assertEquals("node_X", shardExpression.value());
    }

    @Test
    public void testTableName() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysShardsTableInfo.IDENT, ShardTableNameExpression.NAME);
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("wikipedia_de"), shardExpression.value());
    }

}
