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

import io.crate.action.sql.SQLOperations;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.plugin.SQLPlugin;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.MockTcpTransportPlugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.hamcrest.Matchers.is;

public class LogicalReplicationITest extends ESTestCase {

    InternalTestCluster publisherCluster;
    SQLTransportExecutor publisherSqlExecutor;

    InternalTestCluster subscriberCluster;
    SQLTransportExecutor subscriberSqlExecutor;

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            SQLPlugin.class,
            Netty4Plugin.class
        );
    }

    @Before
    public void setupClusters() throws IOException, InterruptedException {
        Collection<Class<? extends Plugin>> mockPlugins = List.of(
            ESIntegTestCase.TestSeedPlugin.class,
            MockHttpTransport.TestPlugin.class,
            MockTransportService.TestPlugin.class,
            MockTcpTransportPlugin.class,
            InternalSettingsPlugin.class
        );

        publisherCluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            true,
            true,
            2,
            2,
            "publishing_cluster",
            createNodeConfigurationSource(),
            0,
            "publisher",
            Stream.concat(nodePlugins().stream(), mockPlugins.stream())
                .collect(Collectors.toList()),
            true
        );
        publisherCluster.beforeTest(random());
        publisherCluster.ensureAtLeastNumDataNodes(2);
        publisherSqlExecutor = sqlExecutor(publisherCluster);

        subscriberCluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            false,
            true,
            1,
            1,
            "subscribing_cluster",
            createNodeConfigurationSource(),
            0,
            "subscriber",
            Stream.concat(nodePlugins().stream(), mockPlugins.stream())
                .collect(Collectors.toList()),
            true
        );
        subscriberCluster.beforeTest(random());
        subscriberCluster.ensureAtLeastNumDataNodes(1);
        subscriberSqlExecutor = sqlExecutor(subscriberCluster);
    }

    @After
    public void clearCluster() throws Exception {
        try {
            publisherCluster.beforeIndexDeletion();
            publisherCluster.assertSeqNos();
            publisherCluster.assertSameDocIdsOnShards();
            publisherCluster.assertConsistentHistoryBetweenTranslogAndLuceneIndex();

            subscriberCluster.beforeIndexDeletion();
            subscriberCluster.assertSeqNos();
            subscriberCluster.assertSameDocIdsOnShards();
            subscriberCluster.assertConsistentHistoryBetweenTranslogAndLuceneIndex();
        } finally {
            publisherCluster.wipe(Collections.emptySet());
            publisherCluster.close();
            subscriberCluster.wipe(Collections.emptySet());
            subscriberCluster.close();
        }
    }

    private SQLResponse executeOnPublisher(String sql) {
        return publisherSqlExecutor.exec(sql);
    }

    private SQLResponse executeOnSubscriber(String sql) {
        return subscriberSqlExecutor.exec(sql);
    }

    private static NodeConfigurationSource createNodeConfigurationSource() {
        Settings.Builder builder = Settings.builder();
        // disables a port scan for other nodes by setting seeds to an empty list
        builder.putList(DISCOVERY_SEED_HOSTS_SETTING.getKey());
        builder.putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "file");
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
        return new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return builder.build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }

            @Override
            public Settings transportClientSettings() {
                return super.transportClientSettings();
            }

            @Override
            public Collection<Class<? extends Plugin>> transportClientPlugins() {
                return List.of(getTestTransportPlugin());
            }
        };
    }

    private static SQLTransportExecutor sqlExecutor(InternalTestCluster cluster) {
        return new SQLTransportExecutor(
            new SQLTransportExecutor.ClientProvider() {
                @Override
                public Client client() {
                    return ESIntegTestCase.client();
                }

                @Override
                public String pgUrl() {
                    PostgresNetty postgresNetty = cluster.getInstance(PostgresNetty.class);
                    BoundTransportAddress boundTransportAddress = postgresNetty.boundAddress();
                    if (boundTransportAddress != null) {
                        InetSocketAddress address = boundTransportAddress.publishAddress().address();
                        return String.format(Locale.ENGLISH, "jdbc:postgresql://%s:%d/?ssl=%s&sslmode=%s",
                                             address.getHostName(), address.getPort(), false, "disable");
                    }
                    return null;
                }

                @Override
                public SQLOperations sqlOperations() {
                    return cluster.getInstance(SQLOperations.class);
                }
            }
        );
    }

    private String publisherConnectionUrl() {
        var transportService = publisherCluster.getInstance(TransportService.class);
        InetSocketAddress address = transportService.boundAddress().publishAddress().address();
        return String.format(
            Locale.ENGLISH,
            "crate://%s:%d?mode=sniff&seeds=%s:%d",
            address.getHostName(),
            address.getPort(),
            address.getHostName(),
            address.getPort()
        );
    }

    @Test
    public void test_create_publication_for_concrete_table() {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT)");
        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1");
        var response = executeOnPublisher(
            "SELECT oid, p.pubname, pubowner, puballtables, schemaname, tablename, pubinsert, pubupdate, pubdelete" +
            " FROM pg_publication p" +
            " JOIN pg_publication_tables t ON p.pubname = t.pubname" +
            " ORDER BY p.pubname, schemaname, tablename");
        assertThat(printedTable(response.rows()),
                   is("-119974068| pub1| -450373579| false| doc| t1| true| true| true\n"));
    }

    @Test
    public void test_create_publication_for_all_tables() {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT)");
        executeOnPublisher("CREATE PUBLICATION pub1 FOR ALL TABLES");
        executeOnPublisher("CREATE TABLE my_schema.t2 (id INT)");
        var response = executeOnPublisher(
            "SELECT oid, p.pubname, pubowner, puballtables, schemaname, tablename, pubinsert, pubupdate, pubdelete" +
            " FROM pg_publication p" +
            " JOIN pg_publication_tables t ON p.pubname = t.pubname" +
            " ORDER BY p.pubname, schemaname, tablename");
        assertThat(printedTable(response.rows()),
                   is("284890074| pub1| -450373579| true| doc| t1| true| true| true\n" +
                      "284890074| pub1| -450373579| true| my_schema| t2| true| true| true\n"));
    }

    @Test
    public void test_drop_publication() {
        executeOnPublisher("CREATE TABLE doc.t3 (id INT)");
        executeOnPublisher("CREATE PUBLICATION pub2 FOR TABLE doc.t3");
        executeOnPublisher("DROP PUBLICATION pub2");

        var response = executeOnPublisher("SELECT * FROM pg_publication WHERE pubname = 'pub2'");
        assertThat(response.rowCount(), is(0L));
        response = executeOnPublisher("SELECT * FROM pg_publication_tables WHERE pubname = 'pub2'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void test_subscribing_to_single_publication() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1");

        executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1");

        executeOnSubscriber("REFRESH TABLE doc.t1");
        var response = executeOnSubscriber("SELECT * FROM doc.t1");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));
    }

    @Test
    public void test_subscribing_to_publication_while_table_exists_raises_error() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1");

        executeOnSubscriber("CREATE TABLE doc.t1 (id int)");
        assertThrows(
            "Subscription 'sub1' cannot be created as included relation 'doc.t1' already exists",
            RelationAlreadyExists.class,
            () -> executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1")
        );
    }

    @Test
    public void test_subscribing_to_single_publication_with_multiple_tables() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");

        executeOnPublisher("CREATE TABLE doc.t2 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t2 (id) VALUES (3), (4)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1, doc.t2");

        executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1");

        executeOnSubscriber("REFRESH TABLE doc.t1");
        var response = executeOnSubscriber("SELECT * FROM doc.t1");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));
        executeOnSubscriber("REFRESH TABLE doc.t2");
        response = executeOnSubscriber("SELECT * FROM doc.t2");
        assertThat(printedTable(response.rows()), is("3\n" +
                                                     "4\n"));
    }

    @Test
    public void test_subscribing_to_single_publication_with_partitioned_table() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT, p INT) CLUSTERED INTO 1 SHARDS PARTITIONED BY (p) WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id, p) VALUES (1, 1), (2, 2)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1");

        executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1");

        executeOnSubscriber("REFRESH TABLE doc.t1");
        var response = executeOnSubscriber("SELECT * FROM doc.t1");
        assertThat(printedTable(response.rows()), is("1| 1\n" +
                                                     "2| 2\n"));
    }

    @Test
    public void test_subscribing_to_single_publication_for_all_tables() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT, p INT) CLUSTERED INTO 1 SHARDS PARTITIONED BY (p) WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id, p) VALUES (1, 1), (2, 2)");
        executeOnPublisher("CREATE TABLE my_schema.t2 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO my_schema.t2 (id) VALUES (1), (2)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR ALL TABLES");

        executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1");

        executeOnSubscriber("REFRESH TABLE doc.t1");
        var response = executeOnSubscriber("SELECT * FROM doc.t1");
        assertThat(printedTable(response.rows()), is("1| 1\n" +
                                                     "2| 2\n"));

        executeOnSubscriber("REFRESH TABLE my_schema.t2");
        response = executeOnSubscriber("SELECT * FROM my_schema.t2");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));
    }

    @Test
    public void test_subscribed_tables_are_followed_and_updated() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");

        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE doc.t1");

        executeOnSubscriber("CREATE SUBSCRIPTION sub1 CONNECTION '" + publisherConnectionUrl() + "' publication pub1");

        executeOnSubscriber("REFRESH TABLE doc.t1");
        var response = executeOnSubscriber("SELECT * FROM doc.t1");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));

        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (3), (4)");

        assertBusy(
            () -> {
                executeOnSubscriber("REFRESH TABLE doc.t1");
                var res = executeOnSubscriber("SELECT * FROM doc.t1");
                assertThat(printedTable(res.rows()), is("1\n" +
                                                        "2\n" +
                                                        "3\n" +
                                                        "4\n"));

            },
            10, TimeUnit.SECONDS
        );
    }
}
