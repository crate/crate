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

import io.crate.protocols.postgres.PostgresNetty;
import io.crate.replication.logical.LogicalReplicationSettings;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import io.crate.user.User;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_READ_POLL_DURATION;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.hamcrest.Matchers.is;

public abstract class LogicalReplicationITestCase extends ESTestCase {

    protected InternalTestCluster publisherCluster;
    SQLTransportExecutor publisherSqlExecutor;

    public InternalTestCluster subscriberCluster;
    SQLTransportExecutor subscriberSqlExecutor;

    protected static final String SUBSCRIBING_USER = "subscriber";

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            Netty4Plugin.class
        );
    }

    @Before
    public void setupClusters() throws IOException, InterruptedException {
        Collection<Class<? extends Plugin>> mockPlugins = List.of(
            ESIntegTestCase.TestSeedPlugin.class,
            MockHttpTransport.TestPlugin.class,
            MockTransportService.TestPlugin.class,
            InternalSettingsPlugin.class
        );

        publisherCluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            getPublisherSupportsDedicatedMasters(),
            getPublisherAutoManageMasterNodes(),
            getPublisherNumberOfNodes(),
            getPublisherNumberOfNodes(),
            "publishing_cluster",
            createNodeConfigurationSource(),
            0,
            "publisher",
            Stream.concat(nodePlugins().stream(), mockPlugins.stream())
                .collect(Collectors.toList()),
            true
        );
        publisherCluster.beforeTest(random());
        publisherCluster.ensureAtLeastNumDataNodes(getPublisherNumberOfNodes());
        publisherSqlExecutor = publisherCluster.createSQLTransportExecutor();

        subscriberCluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            getSubscriberSupportsDedicatedMasters(),
            getSubscriberAutoManageMasterNodes(),
            getSubscriberNumberOfNodes(),
            getSubscriberNumberOfNodes(),
            "subscribing_cluster",
            createNodeConfigurationSource(),
            0,
            "subscriber",
            Stream.concat(nodePlugins().stream(), mockPlugins.stream())
                .collect(Collectors.toList()),
            true
        );
        subscriberCluster.beforeTest(random());
        subscriberCluster.ensureAtLeastNumDataNodes(getSubscriberNumberOfNodes());
        subscriberSqlExecutor = subscriberCluster.createSQLTransportExecutor();
    }

    @After
    public void clearCluster() throws Exception {
        // Existing subscriptions must be dropped first before any index is deleted.
        // Otherwise, the metadata tracking logic would try to restore these indices.
        dropSubscriptions();
        stopCluster(subscriberCluster);
        subscriberCluster = null;
        stopCluster(publisherCluster);
        publisherCluster = null;
    }

    protected void stopCluster(InternalTestCluster cluster) throws Exception {
        if (cluster != null) {
            try {
                cluster.beforeIndexDeletion();
                cluster.assertSeqNos();
                cluster.assertSameDocIdsOnShards();
                cluster.assertConsistentHistoryBetweenTranslogAndLuceneIndex();
            } finally {
                cluster.wipe(Collections.emptySet());
                cluster.close();
            }
        }
    }

    private void dropSubscriptions() {
        var state = subscriberCluster.client().admin().cluster().prepareState().execute().actionGet().getState();
        SubscriptionsMetadata subscriptionsMetadata = state.metadata().custom(SubscriptionsMetadata.TYPE);
        if (subscriptionsMetadata == null) {
            return;
        }
        for (var subscriptionName : subscriptionsMetadata.subscription().keySet()) {
            subscriberSqlExecutor.exec("DROP SUBSCRIPTION " + subscriptionName);
        }
    }

    public String defaultTableSettings() {
        var joiner = new StringJoiner(",");
        // disable replicas to avoid waiting on global checkpoint synchronization on the remote cluster
        // which slows down test speed a lot
        joiner.add("number_of_replicas=0");
        // flush documents to lucene immediately so they can be seen by the changes tracker
        joiner.add("\"translog.flush_threshold_size\"='64b'");
        return joiner.toString();
    }

    public SQLResponse executeOnPublisher(String sql) {
        return publisherSqlExecutor.exec(sql);
    }

    SQLResponse executeOnPublisherAsUser(String sql, User user) {
        return publisherSqlExecutor.executeAs(sql, user);
    }

    long[] executeBulkOnPublisher(String sql, @Nullable Object[][] bulkArgs) {
        return publisherSqlExecutor.execBulk(sql, bulkArgs);
    }

    public SQLResponse executeOnSubscriber(String sql) {
        return subscriberSqlExecutor.exec(sql);
    }

    Settings logicalReplicationSettings() {
        Settings.Builder builder = Settings.builder();
        // reduce time waiting for changes and rather poll faster
        builder.put(REPLICATION_READ_POLL_DURATION.getKey(), "10ms");
        // reduce batch size to test repeated polling of changes
        builder.put(LogicalReplicationSettings.REPLICATION_CHANGE_BATCH_SIZE.getKey(), "20");
        return builder.build();
    }

    NodeConfigurationSource createNodeConfigurationSource() {
        Settings.Builder builder = Settings.builder();
        builder.put(logicalReplicationSettings());
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
        };
    }

    public String publisherConnectionUrl() {
        if (randomBoolean()) {
            var postgres = publisherCluster.getInstance(PostgresNetty.class);
            InetSocketAddress address = postgres.boundAddress().publishAddress().address();
            return String.format(
                Locale.ENGLISH,
                "crate://%s:%d?user=%s&mode=pg_tunnel",
                address.getHostName(),
                address.getPort(),
                SUBSCRIBING_USER
            );
        } else {
            var transportService = publisherCluster.getInstance(TransportService.class);
            InetSocketAddress address = transportService.boundAddress().publishAddress().address();
            return String.format(
                Locale.ENGLISH,
                "crate://%s:%d?user=%s&mode=sniff",
                address.getHostName(),
                address.getPort(),
                SUBSCRIBING_USER
            );
        }
    }

    protected void ensureGreenOnSubscriber() throws Exception {
        assertBusy(() -> {
            try {
                var response = executeOnSubscriber(
                    "SELECT health, count(*) FROM sys.health GROUP BY 1");
                assertThat(response.rowCount(), is(1L));
                assertThat(response.rows()[0][0], is("GREEN"));
            } catch (Exception e) {
                fail();
            }
        }, 10, TimeUnit.SECONDS);

    }

    protected void createPublication(String pub, boolean isForAllTables, List<String> tables) {
        String tablesAsString = tables.stream().collect(Collectors.joining(","));
        if (isForAllTables) {
            executeOnPublisher("CREATE PUBLICATION " + pub + " FOR ALL TABLES");
        } else {
            executeOnPublisher("CREATE PUBLICATION " + pub + " FOR TABLE " + tablesAsString);
        }
        executeOnPublisher("CREATE USER " + SUBSCRIBING_USER);
        executeOnPublisher("GRANT DQL ON TABLE " + tablesAsString + " TO " + SUBSCRIBING_USER);
    }

    protected void createSubscription(String subName, String pubName) throws Exception {
        executeOnSubscriber("CREATE SUBSCRIPTION " + subName +
                " CONNECTION '" + publisherConnectionUrl() + "' publication " + pubName);
        ensureGreenOnSubscriber();

    }

    protected void createSubscriptionAsUser(String subName, String pubName, User user) throws Exception {
        subscriberSqlExecutor.executeAs("CREATE SUBSCRIPTION " + subName +
            " CONNECTION '" + publisherConnectionUrl() + "' publication " + pubName, user);
        ensureGreenOnSubscriber();

    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface PublisherClusterScope {
        int numberOfNodes() default 2;

        boolean supportsDedicatedMasters() default true;

        boolean autoManageMasterNodes() default true;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface SubscriberClusterScope {
        int numberOfNodes() default 2;

        boolean supportsDedicatedMasters() default true;

        boolean autoManageMasterNodes() default true;
    }

    private int getPublisherNumberOfNodes() {
        PublisherClusterScope annotation = getAnnotation(this.getClass(), PublisherClusterScope.class);
        return annotation == null ? 2 : annotation.numberOfNodes();
    }

    private boolean getPublisherSupportsDedicatedMasters() {
        PublisherClusterScope annotation = getAnnotation(this.getClass(), PublisherClusterScope.class);
        return annotation == null || annotation.supportsDedicatedMasters();
    }

    private boolean getPublisherAutoManageMasterNodes() {
        PublisherClusterScope annotation = getAnnotation(this.getClass(), PublisherClusterScope.class);
        return annotation == null || annotation.autoManageMasterNodes();
    }

    private int getSubscriberNumberOfNodes() {
        PublisherClusterScope annotation = getAnnotation(this.getClass(), PublisherClusterScope.class);
        return annotation == null ? 2 : annotation.numberOfNodes();
    }

    private boolean getSubscriberSupportsDedicatedMasters() {
        PublisherClusterScope annotation = getAnnotation(this.getClass(), PublisherClusterScope.class);
        return annotation == null || annotation.supportsDedicatedMasters();
    }

    private boolean getSubscriberAutoManageMasterNodes() {
        PublisherClusterScope annotation = getAnnotation(this.getClass(), PublisherClusterScope.class);
        return annotation == null || annotation.autoManageMasterNodes();
    }

    private static <A extends Annotation> A getAnnotation(Class<?> clazz, Class<A> annotationClass) {
        if (clazz == Object.class || clazz == LogicalReplicationITestCase.class) {
            return null;
        }
        A annotation = clazz.getAnnotation(annotationClass);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass(), annotationClass);
    }
}
