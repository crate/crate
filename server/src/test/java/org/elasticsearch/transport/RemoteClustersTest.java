/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;

import org.assertj.core.api.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.netty.NettyBootstrap;
import io.crate.protocols.postgres.PgClient;
import io.crate.protocols.postgres.PgClientFactory;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class RemoteClustersTest extends CrateDummyClusterServiceUnitTest {

    private NettyBootstrap nettyBootstrap;

    @Before
    public void setupNetty() {
        nettyBootstrap = new NettyBootstrap(Settings.EMPTY);
        nettyBootstrap.start();
    }

    @After
    public void teardownNetty() {
        nettyBootstrap.close();
    }

    @Test
    public void test_connect_reconnects_if_previous_call_raised_an_exception() throws Exception {
        var pgClientFactory = mock(PgClientFactory.class);
        when(pgClientFactory.createClient(anyString(), any(ConnectionInfo.class))).thenThrow(new RuntimeException("dummy"));

        var remoteClusters = new RemoteClusters(
            Settings.EMPTY,
            pgClientFactory,
            MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, THREAD_POOL, nettyBootstrap)
        );

        var future = remoteClusters.connect("foo", ConnectionInfo.fromURL("crate://localhost?mode=pg_tunnel"));
        assertThat(future.isCompletedExceptionally()).isTrue();

        // second call must also result in a `pgFactory.createClient()` call and such throw an exception
        future = remoteClusters.connect("foo", ConnectionInfo.fromURL("crate://localhost?mode=pg_tunnel"));
        assertThat(future.isCompletedExceptionally()).isTrue();

        remoteClusters.close();
    }

    @Test
    public void test_client_is_not_cached_if_failed_to_connect() throws Exception {
        var pgClientFactory = mock(PgClientFactory.class);
        when(pgClientFactory.createClient(anyString(), any(ConnectionInfo.class))).thenThrow(new RuntimeException("dummy"));

        var remoteClusters = new RemoteClusters(
            Settings.EMPTY,
            pgClientFactory,
            MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, THREAD_POOL, nettyBootstrap)
        );

        String subName = "sub1";
        var future = remoteClusters.connect(subName, ConnectionInfo.fromURL("crate://localhost?mode=pg_tunnel"));
        assertThat(future.isCompletedExceptionally()).isTrue();

        // Check that failed client is not cached so that later can re-create a subscription with the same name but with valid credentials.
        // https://github.com/crate/crate/issues/12462
        Assertions.assertThatThrownBy(() -> remoteClusters.getClient(subName))
            .isExactlyInstanceOf(NoSuchRemoteClusterException.class)
            .hasMessageContaining("no such remote cluster: [" + subName + "]");

        remoteClusters.close();
    }

    @Test
    public void test_late_connection_failure_does_not_remove_new_remote_cluster() throws Exception {
        ConnectionInfo connectionInfo = ConnectionInfo.fromURL("crate://localhost?mode=pg_tunnel");
        PgClientFactory pgClientFactory = mock(PgClientFactory.class);
        PgClient firstClient = mock(PgClient.class);
        PgClient secondClient = mock(PgClient.class);
        CompletableFuture<Transport.Connection> firstConnection = new CompletableFuture<>();
        when(pgClientFactory.createClient("sub1", connectionInfo)).thenReturn(firstClient, secondClient);
        when(firstClient.ensureConnected()).thenReturn(firstConnection);
        when(secondClient.ensureConnected()).thenReturn(
            CompletableFuture.completedFuture(mock(Transport.Connection.class)));

        try (RemoteClusters remoteClusters = new RemoteClusters(
            Settings.EMPTY, pgClientFactory, mock(TransportService.class))) {
            remoteClusters.connect("sub1", connectionInfo);
            remoteClusters.remove("sub1");
            assertThat(remoteClusters.connect("sub1", connectionInfo).join()).isSameAs(secondClient);

            // Trigger the first connection's failure callback after a new RemoteCluster containing 'secondClient'
            // has been registered under the same name.
            firstConnection.completeExceptionally(new ConnectException("connection failed"));

            // Ensure the second client is not affected by the first connection's exceptional completion.
            assertThat(remoteClusters.getClient("sub1")).isSameAs(secondClient);
            verify(secondClient, never()).close();
        }
    }
}
