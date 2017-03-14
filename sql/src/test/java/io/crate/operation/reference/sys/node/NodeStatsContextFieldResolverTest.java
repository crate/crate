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
import io.crate.monitor.ZeroExtendedNodeInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;

import static io.crate.testing.DiscoveryNodes.newNode;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class NodeStatsContextFieldResolverTest {

    private NodeStatsContextFieldResolver resolver;

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private InetSocketTransportAddress postgresAddress;

    @Before
    public void setup() throws UnknownHostException {
        DiscoveryNode discoveryNode = newNode("node_name", "node_id");

        postgresAddress = new InetSocketTransportAddress(Inet4Address.getLocalHost(), 5432);
        resolver = new NodeStatsContextFieldResolver(
            () -> discoveryNode,
            mock(MonitorService.class),
            () -> null,
            mock(ThreadPool.class),
            new ZeroExtendedNodeInfo(),
            () -> postgresAddress
        );
    }

    @Test
    public void testEmptyColumnIdents() {
        NodeStatsContext context = resolver.forTopColumnIdents(ImmutableSet.of());
        assertDefaultDiscoveryContext(context);
    }

    @Test
    public void testColumnIdentsResolution() {
        NodeStatsContext context = resolver.forTopColumnIdents(ImmutableSet.of(
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
        NodeStatsContext context = resolver.forTopColumnIdents(ImmutableSet.of(SysNodesTableInfo.Columns.OS));
        assertThat(context.timestamp(), greaterThan(0L));
    }

    @Test
    public void testPSQLPortResolution() throws IOException {
        NodeStatsContext context = resolver.forTopColumnIdents(ImmutableSet.of(
            new ColumnIdent(SysNodesTableInfo.Columns.PORT.name())
        ));
        assertThat(context.isComplete(), is(true));
        assertThat(context.port().get("psql"), is(5432));
    }

    @Test
    public void testResolveForNonExistingColumnIdent() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Cannot resolve NodeStatsContext field for \"dummy\" column ident.");
        resolver.forTopColumnIdents(ImmutableSet.of(SysNodesTableInfo.Columns.ID, new ColumnIdent("dummy")));
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
