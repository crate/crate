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

import static io.crate.protocols.postgres.PostgresNetty.resolvePublishPort;
import static io.crate.testing.Asserts.assertThat;
import static java.net.InetAddress.getByName;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.action.sql.Sessions;
import io.crate.auth.AlwaysOKAuthentication;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.netty.NettyBootstrap;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.role.Role;
import io.crate.role.StubRoleManager;

public class PostgresNettyPublishPortTest extends ESTestCase {

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
    public void testPSQLPublishPort() throws Exception {
        int boundPort = randomIntBetween(9000, 9100);
        int otherBoundPort = randomIntBetween(9200, 9300);

        int publishPort = resolvePublishPort(asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)), getByName("127.0.0.1"));
        assertThat(publishPort)
            .as("Publish port should be derived from matched address").isEqualTo(boundPort);

        publishPort = resolvePublishPort(asList(address("127.0.0.1", boundPort), address("127.0.0.2", boundPort)), getByName("127.0.0.3"));
        assertThat(publishPort)
            .as("Publish port should be derived from unique port of bound addresses").isEqualTo(boundPort);

        publishPort = resolvePublishPort(asList(address("0.0.0.0", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1"));
        assertThat(publishPort)
            .as("Publish port should be derived from matching wildcard address").isEqualTo(boundPort);
    }

    @Test
    public void testNonUniqueBoundAddress() throws Exception {
        int boundPort = randomIntBetween(9000, 9100);
        int otherBoundPort = randomIntBetween(9200, 9300);

        assertThatThrownBy(() -> resolvePublishPort(
            asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)), getByName("127.0.0.3")))
            .isExactlyInstanceOf(BindHttpException.class)
            .hasMessageStartingWith("Failed to auto-resolve psql publish port, multiple bound addresses");
    }

    @Test
    public void testBindAndPublishAddressDefault() {
        // First check if binding to a local works
        NetworkService networkService = new NetworkService(Collections.emptyList());
        StubRoleManager userManager = new StubRoleManager();
        PostgresNetty psql = new PostgresNetty(
            Settings.EMPTY,
            new SessionSettingRegistry(Set.of()),
            mock(Sessions.class),
            userManager,
            networkService,
            new AlwaysOKAuthentication(userManager),
            nettyBootstrap,
            mock(Netty4Transport.class),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            mock(SslContextProvider.class));
        try {
            psql.doStart();
        } finally {
            psql.doStop();
            psql.close();
        }
    }

    @Test
    public void testGeneralBindAndPublishAddressOverrideSetting() {
        // Check override for network.host
        Settings settingsWithCustomHost = Settings.builder().put("network.host", "cantbindtothis").build();
        NetworkService networkService = new NetworkService(Collections.emptyList());
        StubRoleManager userManager = new StubRoleManager();
        PostgresNetty psql = new PostgresNetty(
            settingsWithCustomHost,
            new SessionSettingRegistry(Set.of()),
            mock(Sessions.class),
            userManager,
            networkService,
            new AlwaysOKAuthentication(userManager),
            nettyBootstrap,
            mock(Netty4Transport.class),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            mock(SslContextProvider.class));
        try {
            psql.doStart();
            fail("Should have failed due to custom hostname");
        } catch (BindPostgresException e) {
            // that's what we want
            assertThat(e.getCause()).isExactlyInstanceOf((UnknownHostException.class));
        } finally {
            psql.doStop();
            psql.close();
        }
    }

    @Test
    public void testBindAddressOverrideSetting() {
        // Check override for network.bind_host
        Settings settingsWithCustomBind = Settings.builder().put("network.bind_host", "cantbindtothis").build();
        NetworkService networkService = new NetworkService(Collections.emptyList());
        StubRoleManager userManager = new StubRoleManager();
        PostgresNetty psql = new PostgresNetty(
            settingsWithCustomBind,
            new SessionSettingRegistry(Set.of()),
            mock(Sessions.class),
            userManager,
            networkService,
            new AlwaysOKAuthentication(userManager),
            nettyBootstrap,
            mock(Netty4Transport.class),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            mock(SslContextProvider.class));
        try {
            psql.doStart();
            fail("Should have failed due to custom hostname");
        } catch (BindPostgresException e) {
            // that's what we want
            assertThat(e.getCause()).isExactlyInstanceOf(UnknownHostException.class);
        } finally {
            psql.doStop();
            psql.close();
        }
    }

    @Test
    public void testPublishAddressOverride() {
        // Check override for network.publish_host
        Settings settingsWithCustomPublish = Settings.builder().put("network.publish_host", "cantbindtothis").build();
        NetworkService networkService = new NetworkService(Collections.emptyList());
        StubRoleManager userManager = new StubRoleManager();
        PostgresNetty psql = new PostgresNetty(
            settingsWithCustomPublish,
            new SessionSettingRegistry(Set.of()),
            mock(Sessions.class),
            userManager,
            networkService,
            new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
            nettyBootstrap,
            mock(Netty4Transport.class),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            mock(SslContextProvider.class));
        try {
            psql.doStart();
            fail("Should have failed due to custom hostname");
        } catch (BindTransportException e) {
            // that's what we want
            assertThat(e.getCause()).isExactlyInstanceOf(UnknownHostException.class);
        } finally {
            psql.doStop();
            psql.close();
        }
    }

    private TransportAddress address(String host, int port) throws UnknownHostException {
        return new TransportAddress(getByName(host), port);
    }
}
