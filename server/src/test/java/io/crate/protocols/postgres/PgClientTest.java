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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import io.crate.auth.AlwaysOKAuthentication;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.netty.NettyBootstrap;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.role.Role;
import io.crate.role.StubRoleManager;
import io.crate.session.Session;
import io.crate.session.Sessions;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class PgClientTest extends CrateDummyClusterServiceUnitTest {

    private List<LifecycleComponent> toClose = new ArrayList<>();
    private NettyBootstrap nettyBootstrap;
    private PgClient pgClient;

    @Before
    public void init() {
        nettyBootstrap = new NettyBootstrap(Settings.EMPTY);
        nettyBootstrap.start();
        toClose.add(nettyBootstrap);
    }

    @After
    public void stop() throws Exception {
        for (var lifecycleComponent : toClose.reversed()) {
            lifecycleComponent.close();
        }
        assertBusy(() -> {
            assertThat(nettyBootstrap.workerIsShutdown()).isTrue();
        });
    }


    @Test
    // keeping some iterations because there was an issue with the initial implementation that caused the test timeout after a few iterations
    @Repeat(iterations = 10)
    public void test_pg_client_can_connect_to_postgres_netty() throws Exception {
        var serverNodeSettings = Settings.builder()
            .put("node.name", "server")
            .build();
        TransportAddress serverAddress = startPostgresNetty(serverNodeSettings);
        pgClient = pgClient(serverAddress, Settings.EMPTY);


        CompletableFuture<Connection> connect = pgClient.ensureConnected();
        Connection connection = connect.get(10, TimeUnit.SECONDS);
        assertThat(connection.getNode().getAddress()).isEqualTo(serverAddress);

        // Must be able to call ensureConnected again
        CompletableFuture<Connection> conn2 = pgClient.ensureConnected();
        CompletableFuture<Connection> conn3 = pgClient.ensureConnected();

        conn2.get(10, TimeUnit.SECONDS);
        conn3.get(10, TimeUnit.SECONDS);

        assertThat(connect).isSameAs(conn2);
        assertThat(conn2).isSameAs(conn3);
        connection.close();

        // if a connection failed, calling ensureConnected again should return a new connection
        connect.obtrudeException(new IllegalStateException("test"));
        CompletableFuture<Connection> conn4 = pgClient.ensureConnected();
        assertThat(conn4).isNotEqualTo(conn3);
        connection = conn4.get(10, TimeUnit.SECONDS);

        pgClient.close();
        assertThat(connection.isClosed()).as("pgClient.close must close connection").isTrue();
    }

    @Test
    public void test_connect_fails_if_ssl_is_required_but_server_does_not_support_it() throws Exception {
        TransportAddress serverAddress = startPostgresNetty(Settings.builder().put("node.name", "server").build());
        pgClient = pgClient(serverAddress, Settings.builder().put("sslmode", "require").build());

        CompletableFuture<Connection> connect = pgClient.ensureConnected();
        assertThatThrownBy(() -> connect.get(10, TimeUnit.SECONDS))
            .isExactlyInstanceOf(ExecutionException.class)
            .hasMessageContaining("SSL required but not supported");
        pgClient.close();
    }

    private Netty4Transport newTransport(Settings settings) {
        return new Netty4Transport(
            settings,
            Version.CURRENT,
            THREAD_POOL,
            new NetworkService(List.of()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(List.of()),
            new NoneCircuitBreakerService(),
            nettyBootstrap,
            new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
            new SslContextProvider(settings)
        );
    }

    private TransportAddress startPostgresNetty(Settings serverNodeSettings) {
        var transport = newTransport(serverNodeSettings);
        toClose.add(transport); // postgresNetty doesn't close transport.

        var sqlOperations = mock(Sessions.class);
        when(sqlOperations.newSession(
            any(ConnectionProperties.class),
            any(String.class),
            any(Role.class))
        ).thenReturn(mock(Session.class, Answers.RETURNS_MOCKS));

        PostgresNetty postgresNetty = new PostgresNetty(
            serverNodeSettings,
            new SessionSettingRegistry(Set.of()),
            sqlOperations,
            new StubRoleManager(),
            new NetworkService(List.of()),
            new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
            nettyBootstrap,
            transport,
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new SslContextProvider(serverNodeSettings));

        toClose.add(postgresNetty);
        postgresNetty.start();
        return postgresNetty.boundAddress().publishAddress();
    }

    private PgClient pgClient(TransportAddress serverAddress, Settings connectionSettings) {
        var clientSettings = Settings.builder().put("node.name", "client").build();
        var clientTransport = newTransport(clientSettings); // clientTransport is closed via clientTransportService
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
        return new PgClient(
            "dummy",
            clientSettings,
            clientTransportService,
            nettyBootstrap,
            clientTransport,
            new SslContextProvider(clientSettings),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new ConnectionInfo(
                List.of(serverAddress.getAddress() + ':' + serverAddress.getPort()),
                Settings.builder().put("user", "crate").put(connectionSettings).build()
            )
        );
    }
}
