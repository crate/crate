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

import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.shard.MetaDataShardModule;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SystemReferences;
import io.crate.operator.reference.sys.cluster.ClusterNameExpression;
import io.crate.operator.reference.sys.shard.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
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

            Discovery discovery = mock(Discovery.class);
            bind(Discovery.class).toInstance(discovery);
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(discovery.localNode()).thenReturn(node);
            when(node.getId()).thenReturn("node-id-1");
            when(node.getName()).thenReturn("node 1");
        }
    }


    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder().add(
                new TestModule(),
                new MetaDataModule(),
                new SysClusterExpressionModule(),
                new MetaDataShardModule(),
                new SysShardExpressionModule()
        ).createInjector();
        resolver = injector.getInstance(ShardReferenceResolver.class);
    }

    @Test
    public void testShardInfoLookup() throws Exception {
        ReferenceIdent ident = ShardIdExpression.INFO_ID.ident();
        assertEquals(resolver.getInfo(ident), ShardIdExpression.INFO_ID);
    }

    @Test
    public void testClusterExpression() throws Exception {
        // Looking up cluster wide expressions must work too
        ReferenceIdent ident = ClusterNameExpression.INFO_NAME.ident();
        assertEquals(resolver.getInfo(ident), ClusterNameExpression.INFO_NAME);

        ident = new ReferenceIdent(SystemReferences.CLUSTER_IDENT, "name");
        SysExpression<String> name = (SysExpression<String>) resolver.getImplementation(ident);
        assertEquals(ClusterNameExpression.INFO_NAME, name.info());
        assertEquals("crate", name.value());
    }

    @Test
    public void testId() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SystemReferences.SHARDS_IDENT, "id");
        SysExpression<Integer> shardExpression = (SysExpression<Integer>) resolver.getImplementation(ident);
        assertEquals(ShardIdExpression.INFO_ID, shardExpression.info());

        assertEquals(new Integer(1), shardExpression.value());
    }

    @Test
    public void testSize() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SystemReferences.SHARDS_IDENT, "size");
        SysExpression<Long> shardExpression = (SysExpression<Long>) resolver.getImplementation(ident);
        assertEquals(ShardSizeExpression.INFO_SIZE, shardExpression.info());

        assertEquals(new Long(123456), shardExpression.value());
    }

    @Test
    public void testNumDocs() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SystemReferences.SHARDS_IDENT, "num_docs");
        SysExpression<Long> shardExpression = (SysExpression<Long>) resolver.getImplementation(ident);
        assertEquals(ShardNumDocsExpression.INFO_NUM_DOCS, shardExpression.info());

        assertEquals(new Long(654321), shardExpression.value());
    }

    @Test
    public void testState() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SystemReferences.SHARDS_IDENT, "state");
        SysExpression<String> shardExpression = (SysExpression<String>) resolver.getImplementation(ident);
        assertEquals(ShardStateExpression.INFO_STATE, shardExpression.info());

        assertEquals("STARTED", shardExpression.value());
    }

    @Test
    public void testPrimary() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SystemReferences.SHARDS_IDENT, "primary");
        SysExpression<String> shardExpression = (SysExpression<String>) resolver.getImplementation(ident);
        assertEquals(ShardPrimaryExpression.INFO_PRIMARY, shardExpression.info());

        assertEquals(true, shardExpression.value());
    }

    @Test
    public void testRelocatingNode() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SystemReferences.SHARDS_IDENT, "relocating_node");
        SysExpression<String> shardExpression = (SysExpression<String>) resolver.getImplementation(ident);
        assertEquals(ShardRelocatingNodeExpression.INFO_RELOCATING_NODE, shardExpression.info());

        assertEquals("node_X", shardExpression.value());
    }

    @Test
    public void testTableName() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SystemReferences.SHARDS_IDENT, "table_name");
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(ShardTableNameExpression.INFO_TABLE_NAME, shardExpression.info());

        assertEquals(new BytesRef("wikipedia_de"), shardExpression.value());
    }

    @Test
    public void testNodeId() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SystemReferences.SHARDS_IDENT, "node_id");
        SysExpression<BytesRef> shardExpression = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(ShardNodeIdExpression.INFO_NODE_ID, shardExpression.info());

        assertEquals(new BytesRef("node-id-1"), shardExpression.value());
    }

}
