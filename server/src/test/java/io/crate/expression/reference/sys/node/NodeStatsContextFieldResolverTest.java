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

import static io.crate.testing.DiscoveryNodes.newNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.protocols.ConnectionStats;

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
            () -> new ConnectionStats(1, 2, 3, 4, 5, 6),
            mock(ThreadPool.class),
            new ExtendedNodeInfo(),
            () -> new ConnectionStats(11, 22, 33, 44, 55, 66),
            () -> postgresAddress,
            () -> new ConnectionStats(111, 222, 333, 444, 555, 666),
            () -> 1L
        );
    }

    @Test
    public void testEmptyColumnIdents() {
        NodeStatsContext context = resolver.forTopColumnIdents(Set.of());
        assertDefaultDiscoveryContext(context);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testConnectionsHttpLookupAndExpression() {
        NodeStatsContext statsContext = resolver.forTopColumnIdents(
            Collections.singletonList(SysNodesTableInfo.Columns.CONNECTIONS));
        RowCollectExpressionFactory<NodeStatsContext> expressionFactory =
            SysNodesTableInfo.INSTANCE.expressions().get(SysNodesTableInfo.Columns.CONNECTIONS);
        NestableCollectExpression<NodeStatsContext, ?> expression = expressionFactory.create();

        NestableCollectExpression http = (NestableCollectExpression) expression.getChild("http");
        NestableCollectExpression open = (NestableCollectExpression) http.getChild("open");
        open.setNextRow(statsContext);
        assertThat(open.value()).isEqualTo(1L);

        NestableCollectExpression total = (NestableCollectExpression) http.getChild("total");
        total.setNextRow(statsContext);
        assertThat(total.value()).isEqualTo(2L);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testNumberOfPSqlConnectionsCanBeRetrieved() {
        // tests the resolver and the expression
        NodeStatsContext statsContext = resolver.forTopColumnIdents(
            Collections.singletonList(SysNodesTableInfo.Columns.CONNECTIONS));
        RowCollectExpressionFactory<NodeStatsContext> expressionFactory =
            SysNodesTableInfo.INSTANCE.expressions().get(SysNodesTableInfo.Columns.CONNECTIONS);
        NestableCollectExpression<NodeStatsContext, ?> expression = expressionFactory.create();

        NestableCollectExpression psql = (NestableCollectExpression) expression.getChild("psql");
        NestableCollectExpression open = (NestableCollectExpression) psql.getChild("open");
        open.setNextRow(statsContext);
        assertThat(open.value()).isEqualTo(11L);

        NestableCollectExpression total = (NestableCollectExpression) psql.getChild("total");
        total.setNextRow(statsContext);
        assertThat(total.value()).isEqualTo(22L);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testNumberOfTransportConnectionsCanBeRetrieved() {
        // tests the resolver and the expression
        NodeStatsContext statsContext = resolver.forTopColumnIdents(
            Collections.singletonList(SysNodesTableInfo.Columns.CONNECTIONS));
        RowCollectExpressionFactory<NodeStatsContext> expressionFactory =
            SysNodesTableInfo.INSTANCE.expressions().get(SysNodesTableInfo.Columns.CONNECTIONS);
        NestableCollectExpression<NodeStatsContext, ?> expression = expressionFactory.create();

        NestableCollectExpression psql = (NestableCollectExpression) expression.getChild("transport");
        NestableCollectExpression open = (NestableCollectExpression) psql.getChild("open");
        open.setNextRow(statsContext);
        assertThat(open.value()).isEqualTo(111L);

        NestableCollectExpression total = (NestableCollectExpression) psql.getChild("total");
        total.setNextRow(statsContext);
        assertThat(total.value()).isEqualTo(222L);
    }

    @Test
    public void testColumnIdentsResolution() {
        NodeStatsContext context = resolver.forTopColumnIdents(Set.of(
            SysNodesTableInfo.Columns.ID,
            SysNodesTableInfo.Columns.NAME
        ));
        assertThat(context.isComplete()).isTrue();
        assertThat(context.id()).isNotNull();
        assertThat(context.name()).isNotNull();
        assertThat(context.hostname()).isNull();
    }

    @Test
    public void testNoteStatsContextTimestampResolvedCorrectly() {
        NodeStatsContext context = resolver.forTopColumnIdents(Set.of(SysNodesTableInfo.Columns.OS));
        assertThat(context.timestamp()).isGreaterThan(0);
    }

    @Test
    public void testPSQLPortResolution() throws IOException {
        NodeStatsContext context = resolver.forTopColumnIdents(Set.of(
            ColumnIdent.of(SysNodesTableInfo.Columns.PORT.name())
        ));
        assertThat(context.isComplete()).isTrue();
        assertThat(context.pgPort()).isEqualTo(5432);
    }

    @Test
    public void testClusterStateVersion() throws IOException {
        NodeStatsContext context = resolver.forTopColumnIdents(Set.of(
            ColumnIdent.of(SysNodesTableInfo.Columns.CLUSTER_STATE_VERSION.name())
        ));
        assertThat(context.isComplete()).isTrue();
        assertThat(context.clusterStateVersion()).isEqualTo(1L);
    }

    @Test
    public void testResolveForNonExistingColumnIdent() {
        assertThatThrownBy(() ->
            resolver.forTopColumnIdents(Set.of(SysNodesTableInfo.Columns.ID, ColumnIdent.of("dummy"))))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot resolve NodeStatsContext field for \"dummy\" column ident.");
    }

    private void assertDefaultDiscoveryContext(NodeStatsContext context) {
        assertThat(context.isComplete()).isTrue();
        assertThat(context.id()).isNull();
        assertThat(context.name()).isNull();
        assertThat(context.hostname()).isNull();
        assertThat(context.build()).isNull();
        assertThat(context.restUrl()).isNull();
        assertThat(context.pgPort()).isNull();
        assertThat(context.jvmStats()).isNull();
        assertThat(context.osInfo()).isNull();
        assertThat(context.processStats()).isNull();
        assertThat(context.osStats()).isNull();
        assertThat(context.extendedOsStats()).isNull();
        assertThat(context.threadPools()).isNull();
        assertThat(context.javaVersion()).isNotNull();
    }
}
