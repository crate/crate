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

package io.crate.operation.reference.sys.node;

import com.google.common.collect.ImmutableSet;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.protocols.postgres.PostgresNetty;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.Inet4Address;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeStatsContextFieldResolverTest {

    private final ClusterService clusterService = mock(ClusterService.class);
    private final OsService osService = mock(OsService.class);
    private final NodeService nodeService = mock(NodeService.class);
    private final JvmService jvmService = mock(JvmService.class);
    private final ThreadPool threadPool = mock(ThreadPool.class);
    private final ExtendedNodeInfo extendedNodeInfo = mock(ExtendedNodeInfo.class);
    private final PostgresNetty postgresNetty = mock(PostgresNetty.class);

    private NodeStatsContextFieldResolver resolver;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);
        when(discoveryNode.getId()).thenReturn("node_id");
        when(discoveryNode.getName()).thenReturn("node_name");
        when(clusterService.localNode()).thenReturn(discoveryNode);

        resolver = new NodeStatsContextFieldResolver(
            clusterService,
            osService,
            nodeService,
            jvmService,
            threadPool,
            extendedNodeInfo,
            postgresNetty);
    }

    @Test
    public void testEmptyColumnIdents() {
        NodeStatsContext context = resolver.forColumns(ImmutableSet.<ColumnIdent>of());
        assertDefaultDiscoveryContext(context);
    }

    @Test
    public void testColumnIdentsResolution() {
        NodeStatsContext context = resolver.forColumns(ImmutableSet.of(
            new ColumnIdent(SysNodesTableInfo.Columns.ID.name()),
            new ColumnIdent(SysNodesTableInfo.Columns.NAME.name())
        ));
        assertThat(context.isComplete(), is(true));
        assertThat(context.id(), is(notNullValue()));
        assertThat(context.name(), is(notNullValue()));
        assertThat(context.hostname(), is(nullValue()));
    }

    @Test
    public void testPSQLPortResolution() throws IOException {
        NodeInfo nodeInfo = mock(NodeInfo.class);
        when(nodeService.info(
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean()
        )).thenReturn(nodeInfo);
        NodeStats stats = mock(NodeStats.class);
        when(nodeService.stats(
            eq(CommonStatsFlags.NONE),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean()
        )).thenReturn(stats);
        when(stats.getNode()).thenReturn(mock(DiscoveryNode.class));

        InetSocketTransportAddress inetAddress = new InetSocketTransportAddress(Inet4Address.getLocalHost(), 5432);
        BoundTransportAddress boundAddress = new BoundTransportAddress(new TransportAddress[]{inetAddress}, inetAddress);
        when(postgresNetty.boundAddress()).thenReturn(boundAddress);

        NodeStatsContext context = resolver.forColumns(ImmutableSet.of(
            new ColumnIdent(SysNodesTableInfo.Columns.PORT.name())
        ));
        assertThat(context.isComplete(), is(true));
        assertThat(context.port().get("psql"), is(5432));
    }

    @Test
    public void testResolveForNonExistingColumnIdent() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Cannot resolve NodeStatsContext field for \"dummy\" column ident.");

        resolver.forColumns(ImmutableSet.of(
            new ColumnIdent(SysNodesTableInfo.Columns.ID.name()),
            new ColumnIdent("dummy"))
        );
    }

    private void assertDefaultDiscoveryContext(NodeStatsContext context) {
        assertThat(context.isComplete(), is(true));
        assertThat(context.id(), is(nullValue()));
        assertThat(context.name(), is(nullValue()));
        assertThat(context.hostname(), is(nullValue()));
        assertThat(context.build(), is(nullValue()));
        assertThat(context.restUrl(), is(nullValue()));
        assertThat(context.port(), is(nullValue()));
        assertThat(context.jvmStats(), is(nullValue()));
        assertThat(context.osInfo(), is(nullValue()));
        assertThat(context.processStats(), is(nullValue()));
        assertThat(context.osStats(), is(nullValue()));
        assertThat(context.extendedOsStats(), is(nullValue()));
        assertThat(context.networkStats(), is(nullValue()));
        assertThat(context.extendedFsStats(), is(nullValue()));
        assertThat(context.threadPools(), is(nullValue()));
        assertThat(context.javaVersion(), is(notNullValue()));
    }
}
