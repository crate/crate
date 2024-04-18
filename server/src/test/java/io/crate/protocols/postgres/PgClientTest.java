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

package io.crate.protocols.postgres;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import io.crate.action.sql.Session;
import io.crate.action.sql.Sessions;
import io.crate.auth.AlwaysOKAuthentication;
import io.crate.auth.Authentication;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.netty.NettyBootstrap;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.role.Role;
import io.crate.role.StubRoleManager;

public class PgClientTest extends CrateDummyClusterServiceUnitTest {

    private List<LifecycleComponent> toClose = new ArrayList<>();
    private PgClient pgClient;

    @After
    public void stop() throws Exception {
        for (var lifecycleComponent : toClose) {
            lifecycleComponent.close();
        }
        assertBusy(() -> {
            assertThat(pgClient.nettyBootstrap.workerIsShutdown(), is(true));
        });
    }


    @Test
    // keeping some iterations because there was an issue with the initial implementation that caused the test timeout after a few iterations
    @Repeat(iterations = 10)
    public void test_pg_client_can_connect_to_postgres_netty() throws Exception {
        var serverNodeSettings = Settings.builder()
            .put("node.name", "server")
            .build();
        var clientSettings = Settings.builder()
            .put("node.name", "client")
            .build();
        var nettyBootstrap = new NettyBootstrap(Settings.EMPTY);
        nettyBootstrap.start();
        toClose.add(nettyBootstrap);
        var pageCacheRecycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;
        var networkService = new NetworkService(List.of());
        var namedWriteableRegistry = new NamedWriteableRegistry(List.of());
        var circuitBreakerService = new NoneCircuitBreakerService();
        Authentication authentication = new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER));
        var sslContextProvider = new SslContextProvider(serverNodeSettings);
        var serverTransport = new Netty4Transport(
            serverNodeSettings,
            Version.CURRENT,
            THREAD_POOL,
            networkService,
            pageCacheRecycler,
            namedWriteableRegistry,
            circuitBreakerService,
            nettyBootstrap,
            authentication,
            sslContextProvider
        );
        toClose.add(serverTransport);
        var clientTransport = new Netty4Transport(
            clientSettings,
            Version.CURRENT,
            THREAD_POOL,
            networkService,
            pageCacheRecycler,
            namedWriteableRegistry,
            circuitBreakerService,
            nettyBootstrap,
            authentication,
            sslContextProvider
        ); // clientTransport is closed via clientTransportService
        var sqlOperations = mock(Sessions.class);
        when(sqlOperations.newSession(any(String.class), any(Role.class))).thenReturn(mock(Session.class));
        PostgresNetty postgresNetty = new PostgresNetty(
            serverNodeSettings,
            new SessionSettingRegistry(Set.of()),
            sqlOperations,
            new StubRoleManager(),
            networkService,
            authentication,
            nettyBootstrap,
            serverTransport,
            pageCacheRecycler,
            sslContextProvider
        );
        toClose.add(postgresNetty);
        postgresNetty.start();
        TransportAddress serverAddress = postgresNetty.boundAddress().publishAddress();
        DiscoveryNode localNode = new DiscoveryNode("client", serverAddress, Version.CURRENT);
        var clientTransportService = new TransportService(
            clientSettings,
            clientTransport,
            THREAD_POOL,
            address -> localNode,
            null
        );
        toClose.add(clientTransportService);
        clientTransportService.start();
        clientTransportService.acceptIncomingRequests();
        pgClient = new PgClient(
            "dummy",
            clientSettings,
            clientTransportService,
            nettyBootstrap,
            clientTransport,
            null, // sslContextProvider
            pageCacheRecycler,
            new ConnectionInfo(
                List.of(serverAddress.getAddress() + ':' + serverAddress.getPort()),
                Settings.builder().put("user", "crate").build()
            )
        );

        CompletableFuture<Connection> connect = pgClient.ensureConnected();
        Connection connection = connect.get(120, TimeUnit.SECONDS);
        assertThat(connection.getNode().getAddress(), is(serverAddress));

        // Must be able to call ensureConnected again
        CompletableFuture<Connection> conn2 = pgClient.ensureConnected();
        CompletableFuture<Connection> conn3 = pgClient.ensureConnected();

        conn2.get(120, TimeUnit.SECONDS);
        conn3.get(120, TimeUnit.SECONDS);

        assertThat(connect, sameInstance(conn2));
        assertThat(conn2, sameInstance(conn3));
        connection.close();

        // if a connection failed, calling ensureConnected again should return a new connection
        connect.obtrudeException(new IllegalStateException("test"));
        CompletableFuture<Connection> conn4 = pgClient.ensureConnected();
        assertThat(conn4, Matchers.is(Matchers.not(conn3)));
        connection = conn4.get(120, TimeUnit.SECONDS);

        pgClient.close();
        assertThat("pgClient.close must close connection", connection.isClosed(), is(true));
    }
}
