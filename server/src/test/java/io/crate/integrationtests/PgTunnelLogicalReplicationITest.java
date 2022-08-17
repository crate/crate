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

package io.crate.integrationtests;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.SettingsBasedSeedHostsProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.transport.Netty4Plugin;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.protocols.ssl.SslSettings;
import io.crate.replication.logical.LogicalReplicationSettings;
import io.crate.replication.logical.metadata.ConnectionInfo.SSLMode;
import io.crate.testing.Asserts;
import io.crate.testing.SQLErrorMatcher;
import io.crate.testing.SQLTransportExecutor;
import io.crate.testing.TestingHelpers;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.util.SelfSignedCertificate;

public class PgTunnelLogicalReplicationITest extends ESTestCase {

    private InternalTestCluster publisherCluster;
    private InternalTestCluster subscriberCluster;
    private SQLTransportExecutor publisher;
    private SQLTransportExecutor subscriber;

    private InternalTestCluster newCluster(String clusterName, Settings settings) throws Exception {
        Settings allSettings = Settings.builder()
            .putList(SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING.getKey())
            .put(LogicalReplicationSettings.REPLICATION_READ_POLL_DURATION.getKey(), "10ms")
            .put(settings)
            .build();
        var nodeConfigurationSource = new NodeConfigurationSource() {

            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return allSettings;
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }
        };
        var cluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            false,
            true,
            1,
            1,
            clusterName,
            nodeConfigurationSource,
            0,
            clusterName + "_node",
            List.of(
                MockHttpTransport.TestPlugin.class,
                Netty4Plugin.class
            ),
            true
        );
        cluster.beforeTest(random());
        cluster.ensureAtLeastNumDataNodes(1);
        return cluster;
    }

    private void stopCluster(@Nullable InternalTestCluster cluster) throws Exception {
        if (cluster == null) {
            return;
        }
        try {
            cluster.beforeIndexDeletion();
            cluster.assertSeqNos();
            cluster.assertSameDocIdsOnShards();
            cluster.assertConsistentHistoryBetweenTranslogAndLuceneIndex();
        } finally {
            cluster.wipe();
            cluster.close();
        }
    }

    @After
    public void clearClusters() throws Exception {
        if (subscriber != null) {
            try {
                subscriber.exec("drop subscription sub1");
            } catch (Exception e) {
                // ignore
            }
        }
        stopCluster(publisherCluster);
        stopCluster(subscriberCluster);
    }

    @Test
    public void test_pg_client_can_use_password_authentication_for_replication() throws Exception {
        publisherCluster = newCluster("publisher", Settings.builder()
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config.0.user", "marvin")
            .put("auth.host_based.config.0.method", "password")
            .put("auth.host_based.config.0.protocol", "pg")
            .build()
        );
        subscriberCluster = newCluster("subscriber", Settings.EMPTY);

        publisher = publisherCluster.createSQLTransportExecutor();
        publisher.exec("create table tbl (x int) with (number_of_replicas = 0)");
        publisher.exec("insert into tbl values (1)");
        publisher.ensureGreen();
        publisher.exec("create user marvin with (password = 'secret')");
        publisher.exec("grant ALL TO marvin");
        publisher.exec("create publication pub1 FOR TABLE tbl");


        PostgresNetty postgres = publisherCluster.getInstance(PostgresNetty.class);
        InetSocketAddress postgresAddress = postgres.boundAddress().publishAddress().address();
        subscriber = subscriberCluster.createSQLTransportExecutor();

        subscriber.exec(String.format(Locale.ENGLISH, """
            CREATE SUBSCRIPTION sub1
            CONNECTION 'crate://%s:%d?user=marvin&password=secret&mode=pg_tunnel'
            PUBLICATION pub1
            """,
            postgresAddress.getHostName(),
            postgresAddress.getPort()
        ));

        assertThat(TestingHelpers.printedTable(subscriber.exec("select subname from pg_subscription").rows()), is(
            "sub1\n"
        ));

        assertBusy(() -> {
            try {
                assertThat(subscriber.exec("select * from tbl").rowCount(), is(1L));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, 10, TimeUnit.SECONDS);
    }

    @Test
    public void test_cannot_create_subscription_with_invalid_user_password() throws Exception {
        publisherCluster = newCluster("publisher", Settings.builder()
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config.0.user", "marvin")
            .put("auth.host_based.config.0.method", "password")
            .put("auth.host_based.config.0.protocol", "pg")
            .build()
        );
        subscriberCluster = newCluster("subscriber", Settings.EMPTY);

        publisher = publisherCluster.createSQLTransportExecutor();
        publisher.exec("create user marvin with (password = 'secret')");
        publisher.exec("create publication pub1 FOR ALL TABLES");


        PostgresNetty postgres = publisherCluster.getInstance(PostgresNetty.class);
        InetSocketAddress postgresAddress = postgres.boundAddress().publishAddress().address();
        subscriber = subscriberCluster.createSQLTransportExecutor();

        Asserts.assertThrowsMatches(
            () -> {
                subscriber.exec(String.format(Locale.ENGLISH, """
                    CREATE SUBSCRIPTION sub1
                        CONNECTION 'crate://%s:%d?user=marvin&password=invalid&mode=pg_tunnel'
                        PUBLICATION pub1
                    """,
                    postgresAddress.getHostName(),
                    postgresAddress.getPort()));
            },
            SQLErrorMatcher.isSQLError(
                Matchers.containsString("password authentication failed for user \"marvin\""),
                PGErrorStatus.EXCLUSION_VIOLATION,
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                5000));
    }

    @Test
    public void test_can_use_ssl_in_pg_tunnel_subscription() throws Exception {
        // self signed certificate doesn't work with randomized locales
        Locale.setDefault(Locale.ENGLISH);

        Path keystorePath = createTempFile();
        var certificate = new SelfSignedCertificate();
        char[] emptyPassword = new char[0];
        var keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null);
        keyStore.setKeyEntry("key", certificate.key(), emptyPassword, new Certificate[] { certificate.cert() });
        try (var out = Files.newOutputStream(keystorePath)) {
            keyStore.store(out, emptyPassword);
        }

        publisherCluster = newCluster("publisher", Settings.builder()
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config.0.user", "marvin")
            .put("auth.host_based.config.0.method", "password")
            .put("auth.host_based.config.0.protocol", "pg")
            .put("auth.host_based.config.0.ssl", "on")
            .put(SslSettings.SSL_PSQL_ENABLED.getKey(), true)
            .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keystorePath.toAbsolutePath().toString())
            .build()
        );
        subscriberCluster = newCluster("subscriber", Settings.builder()
            .put(SslSettings.SSL_PSQL_ENABLED.getKey(), true)
            .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keystorePath.toAbsolutePath().toString())
            .build()
        );

        publisher = publisherCluster.createSQLTransportExecutor();
        publisher.exec("create table tbl (x int) with (number_of_replicas = 0)");
        publisher.exec("create user marvin with (password = 'secret')");
        publisher.exec("grant ALL TO marvin");
        publisher.exec("create publication pub1 FOR ALL TABLES");


        PostgresNetty postgres = publisherCluster.getInstance(PostgresNetty.class);
        InetSocketAddress postgresAddress = postgres.boundAddress().publishAddress().address();
        subscriber = subscriberCluster.createSQLTransportExecutor(true, null);
        subscriber.exec(String.format(Locale.ENGLISH, """
            CREATE SUBSCRIPTION sub1
                CONNECTION 'crate://%s:%d?user=marvin&password=secret&mode=pg_tunnel&sslmode=%s'
                PUBLICATION pub1
            """,
            postgresAddress.getHostName(),
            postgresAddress.getPort(),
            random().nextBoolean() ? SSLMode.PREFER.name() : SSLMode.REQUIRE.name()
        ));

        assertThat(TestingHelpers.printedTable(subscriber.exec("select subname from pg_subscription").rows()), is(
            "sub1\n"
        ));
        assertBusy(() -> {
            try {
                assertThat(subscriber.exec("select * from tbl").rowCount(), is(0L));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, 10, TimeUnit.SECONDS);
    }
}
