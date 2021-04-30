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

package io.crate.expression.reference.sys.node;

import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.protocols.ConnectionStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;

import static io.crate.testing.DiscoveryNodes.newNode;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeStatsContextFieldResolverTest {

    private NodeStatsContextFieldResolver resolver;

    private TransportAddress postgresAddress;

    @Before
    public void setup() throws UnknownHostException {
        final OsService osService = mock(OsService.class);
        final OsStats osStats = mock(OsStats.class);
        final MonitorService monitorService = mock(MonitorService.class);

        when(monitorService.osService()).thenReturn(osService);
        when(osService.stats()).thenReturn(osStats);
        DiscoveryNode discoveryNode = newNode("node_name", "node_id");

        postgresAddress = new TransportAddress(Inet4Address.getLocalHost(), 5432);
        resolver = new NodeStatsContextFieldResolver(
            () -> discoveryNode,
            monitorService,
            () -> null,
            () -> new HttpStats(20L, 30L),
            mock(ThreadPool.class),
            new ExtendedNodeInfo(),
            () -> new ConnectionStats(2L, 4L),
            () -> postgresAddress,
            () -> 12L,
            () -> 1L
        );
    }

    @Test
    public void testEmptyColumnIdents() {
        NodeStatsContext context = resolver.forTopColumnIdents(Set.of());
        assertDefaultDiscoveryContext(context);
    }

    @Test
    public void testConnectionsHttpLookupAndExpression() {
        NodeStatsContext statsContext = resolver.forTopColumnIdents(
            Collections.singletonList(SysNodesTableInfo.Columns.CONNECTIONS));
        RowCollectExpressionFactory<NodeStatsContext> expressionFactory =
            SysNodesTableInfo.create().expressions().get(SysNodesTableInfo.Columns.CONNECTIONS);
        NestableCollectExpression<NodeStatsContext, ?> expression = expressionFactory.create();

        NestableCollectExpression http = (NestableCollectExpression) expression.getChild("http");
        NestableCollectExpression open = (NestableCollectExpression) http.getChild("open");
        open.setNextRow(statsContext);
        assertThat(open.value(), is(20L));

        NestableCollectExpression total = (NestableCollectExpression) http.getChild("total");
        total.setNextRow(statsContext);
        assertThat(total.value(), is(30L));
    }

    @Test
    public void testNumberOfPSqlConnectionsCanBeRetrieved() {
        // tests the resolver and the expression
        NodeStatsContext statsContext = resolver.forTopColumnIdents(
            Collections.singletonList(SysNodesTableInfo.Columns.CONNECTIONS));
        RowCollectExpressionFactory<NodeStatsContext> expressionFactory =
            SysNodesTableInfo.create().expressions().get(SysNodesTableInfo.Columns.CONNECTIONS);
        NestableCollectExpression<NodeStatsContext, ?> expression = expressionFactory.create();

        NestableCollectExpression psql = (NestableCollectExpression) expression.getChild("psql");
        NestableCollectExpression open = (NestableCollectExpression) psql.getChild("open");
        open.setNextRow(statsContext);
        assertThat(open.value(), is(2L));

        NestableCollectExpression total = (NestableCollectExpression) psql.getChild("total");
        total.setNextRow(statsContext);
        assertThat(total.value(), is(4L));
    }

    @Test
    public void testNumberOfTransportConnectionsCanBeRetrieved() {
        // tests the resolver and the expression
        NodeStatsContext statsContext = resolver.forTopColumnIdents(
            Collections.singletonList(SysNodesTableInfo.Columns.CONNECTIONS));
        RowCollectExpressionFactory<NodeStatsContext> expressionFactory =
            SysNodesTableInfo.create().expressions().get(SysNodesTableInfo.Columns.CONNECTIONS);
        NestableCollectExpression<NodeStatsContext, ?> expression = expressionFactory.create();

        NestableCollectExpression psql = (NestableCollectExpression) expression.getChild("transport");
        NestableCollectExpression open = (NestableCollectExpression) psql.getChild("open");
        open.setNextRow(statsContext);
        assertThat(open.value(), is(12L));
    }

    @Test
    public void testColumnIdentsResolution() {
        NodeStatsContext context = resolver.forTopColumnIdents(Set.of(
            SysNodesTableInfo.Columns.ID,
            SysNodesTableInfo.Columns.NAME
        ));
        assertThat(context.isComplete(), is(true));
        assertThat(context.id(), is(notNullValue()));
        assertThat(context.name(), is(notNullValue()));
        assertThat(context.hostname(), is(nullValue()));
    }

    @Test
    public void testNoteStatsContextTimestampResolvedCorrectly() {
        NodeStatsContext context = resolver.forTopColumnIdents(Set.of(SysNodesTableInfo.Columns.OS));
        assertThat(context.timestamp(), greaterThan(0L));
    }

    @Test
    public void testPSQLPortResolution() throws IOException {
        NodeStatsContext context = resolver.forTopColumnIdents(Set.of(
            new ColumnIdent(SysNodesTableInfo.Columns.PORT.name())
        ));
        assertThat(context.isComplete(), is(true));
        assertThat(context.pgPort(), is(5432));
    }

    @Test
    public void testClusterStateVersion() throws IOException {
        NodeStatsContext context = resolver.forTopColumnIdents(Set.of(
            new ColumnIdent(SysNodesTableInfo.Columns.CLUSTER_STATE_VERSION.name())
        ));
        assertThat(context.isComplete(), is(true));
        assertThat(context.clusterStateVersion(), is(1L));
    }

    @Test
    public void testResolveForNonExistingColumnIdent() {
        assertThrows(IllegalArgumentException.class,
                     () -> resolver.forTopColumnIdents(Set.of(SysNodesTableInfo.Columns.ID, new ColumnIdent("dummy"))),
                     "Cannot resolve NodeStatsContext field for \"dummy\" column ident.");
    }

    private void assertDefaultDiscoveryContext(NodeStatsContext context) {
        assertThat(context.isComplete(), is(true));
        assertThat(context.id(), is(nullValue()));
        assertThat(context.name(), is(nullValue()));
        assertThat(context.hostname(), is(nullValue()));
        assertThat(context.build(), is(nullValue()));
        assertThat(context.restUrl(), is(nullValue()));
        assertThat(context.pgPort(), is(nullValue()));
        assertThat(context.jvmStats(), is(nullValue()));
        assertThat(context.osInfo(), is(nullValue()));
        assertThat(context.processStats(), is(nullValue()));
        assertThat(context.osStats(), is(nullValue()));
        assertThat(context.extendedOsStats(), is(nullValue()));
        assertThat(context.threadPools(), is(nullValue()));
        assertThat(context.javaVersion(), is(notNullValue()));
    }
}
