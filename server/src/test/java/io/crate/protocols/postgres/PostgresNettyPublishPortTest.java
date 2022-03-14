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

import io.crate.action.sql.SQLOperations;
import io.crate.auth.AlwaysOKAuthentication;
import io.crate.netty.NettyBootstrap;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.user.StubUserManager;
import io.crate.user.User;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.transport.BindTransportException;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Collections;

import static io.crate.protocols.postgres.PostgresNetty.resolvePublishPort;
import static java.net.InetAddress.getByName;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class PostgresNettyPublishPortTest extends ESTestCase {

    @Test
    public void testPSQLPublishPort() throws Exception {
        int boundPort = randomIntBetween(9000, 9100);
        int otherBoundPort = randomIntBetween(9200, 9300);

        int publishPort = resolvePublishPort(asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)), getByName("127.0.0.1"));
        assertThat("Publish port should be derived from matched address", publishPort, equalTo(boundPort));

        publishPort = resolvePublishPort(asList(address("127.0.0.1", boundPort), address("127.0.0.2", boundPort)), getByName("127.0.0.3"));
        assertThat("Publish port should be derived from unique port of bound addresses", publishPort, equalTo(boundPort));

        publishPort = resolvePublishPort(asList(address("0.0.0.0", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1"));
        assertThat("Publish port should be derived from matching wildcard address", publishPort, equalTo(boundPort));
    }

    @Test
    public void testNonUniqueBoundAddress() throws Exception {
        int boundPort = randomIntBetween(9000, 9100);
        int otherBoundPort = randomIntBetween(9200, 9300);

        expectedException.expect(BindHttpException.class);
        expectedException.expectMessage("Failed to auto-resolve psql publish port, multiple bound addresses");
        resolvePublishPort(asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.3"));
    }

    @Test
    public void testBindAndPublishAddressDefault() {
        // First check if binding to a local works
        NetworkService networkService = new NetworkService(Collections.emptyList());
        StubUserManager userManager = new StubUserManager();
        PostgresNetty psql = new PostgresNetty(
            Settings.EMPTY,
            mock(SQLOperations.class),
            userManager,
            networkService,
            null, new AlwaysOKAuthentication(userManager),
            new NettyBootstrap(),
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
        StubUserManager userManager = new StubUserManager();
        PostgresNetty psql = new PostgresNetty(
            settingsWithCustomHost,
            mock(SQLOperations.class),
            userManager,
            networkService,
            null, new AlwaysOKAuthentication(userManager),
            new NettyBootstrap(),
            mock(SslContextProvider.class));
        try {
            psql.doStart();
            fail("Should have failed due to custom hostname");
        } catch (BindPostgresException e) {
            // that's what we want
            assertThat(e.getCause(), instanceOf(UnknownHostException.class));
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
        StubUserManager userManager = new StubUserManager();
        PostgresNetty psql = new PostgresNetty(
            settingsWithCustomBind,
            mock(SQLOperations.class),
            userManager,
            networkService,
            null, new AlwaysOKAuthentication(userManager),
            new NettyBootstrap(),
            mock(SslContextProvider.class));
        try {
            psql.doStart();
            fail("Should have failed due to custom hostname");
        } catch (BindPostgresException e) {
            // that's what we want
            assertThat(e.getCause(), instanceOf(UnknownHostException.class));
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
        StubUserManager userManager = new StubUserManager();
        PostgresNetty psql = new PostgresNetty(
            settingsWithCustomPublish,
            mock(SQLOperations.class),
            userManager,
            networkService,
            null, new AlwaysOKAuthentication(userName -> User.CRATE_USER),
            new NettyBootstrap(),
            mock(SslContextProvider.class));
        try {
            psql.doStart();
            fail("Should have failed due to custom hostname");
        } catch (BindTransportException e) {
            // that's what we want
            assertThat(e.getCause(), instanceOf(UnknownHostException.class));
        } finally {
            psql.doStop();
            psql.close();
        }
    }

    private TransportAddress address(String host, int port) throws UnknownHostException {
        return new TransportAddress(getByName(host), port);
    }
}
