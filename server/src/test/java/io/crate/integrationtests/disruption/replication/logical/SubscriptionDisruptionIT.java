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

package io.crate.integrationtests.disruption.replication.logical;

import static io.crate.integrationtests.disruption.discovery.AbstractDisruptionTestCase.isolateNode;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.test.IntegTestCase.ensureStableCluster;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.integrationtests.LogicalReplicationITestCase;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.MetadataTracker;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.testing.UseRandomizedSchema;

@UseRandomizedSchema(random = false)
@LogicalReplicationITestCase.PublisherClusterScope(numberOfNodes = 1, supportsDedicatedMasters = false)
@LogicalReplicationITestCase.SubscriberClusterScope(numberOfNodes = 3, supportsDedicatedMasters = false)
public class SubscriptionDisruptionIT extends LogicalReplicationITestCase {

    @Test
    public void test_subscription_state_on_restore_failure() throws Exception {
        String subscriptionName = "sub1";
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH (number_of_replicas=0)");
        createPublication("pub1", false, List.of("doc.t1"));

        var requestCnt = new AtomicInteger(0);
        List<MockTransportService> transportServices = new ArrayList<>();
        for (TransportService transportService : publisherCluster.getDataOrMasterNodeInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            transportServices.add(mockTransportService);
            mockTransportService.addRequestHandlingBehavior(PublicationsStateAction.NAME, (handler, request, channel) -> {
                // First request is sent on subscription creation while we want to fail on the asynchronous
                // restore afterwards -> fail on 2nd request
                if (request instanceof PublicationsStateAction.Request && requestCnt.getAndIncrement() == 1) {
                    channel.sendResponse(new ElasticsearchException("fail on logical replication repository restore"));
                } else {
                    handler.messageReceived(request, channel);
                }
            });
        }

        executeOnSubscriber("CREATE SUBSCRIPTION " + subscriptionName +
                            " CONNECTION '" + publisherConnectionUrl() + "' publication pub1");
        try {
            assertBusy(
                () -> {
                    var res = executeOnSubscriber(
                        "SELECT s.subname, s.subpublications, sr.srrelid::text, sr.srsubstate, sr.srsubstate_reason" +
                        " FROM pg_subscription s" +
                        " JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
                        " ORDER BY s.subname");
                    assertThat(printedTable(res.rows()),
                               is("sub1| [pub1]| doc.t1| e| Tracking of metadata failed for subscription 'sub1' with unrecoverable error, stop tracking.\nReason: fail on logical replication repository restore\n"));
                }
            );
        } finally {
            transportServices.forEach(MockTransportService::clearAllRules);
        }
    }

    @Test
    public void test_subscription_metadata_tracker_retries_on_publisher_disconnect() throws Exception {
        String subscriptionName = "sub1";
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" + defaultTableSettings() + ")");
        createPublication("pub1", false, List.of("doc.t1"));
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");
        executeOnSubscriber("CREATE SUBSCRIPTION " + subscriptionName +
                            " CONNECTION '" + publisherConnectionUrl() + "' publication pub1");

        // Ensure tracker started and initial recovery is done
        assertBusy(() -> {
            assertThat(isMetadataTrackerActive()).isTrue();
            var res = executeOnSubscriber(
                "SELECT s.subname, s.subpublications, sr.srrelid::text, sr.srsubstate, sr.srsubstate_reason" +
                    " FROM pg_subscription s" +
                    " JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
                    " ORDER BY s.subname");
            assertThat(printedTable(res.rows()), is(
                "sub1| [pub1]| doc.t1| r| NULL\n"
            ));
        });

        var expectedLogMessage = "Retrieving remote metadata failed for subscription 'sub1', will retry";
        var mockAppender = appendLogger(expectedLogMessage, MetadataTracker.class, Level.WARN);

        startDisrupting(MockTransportService::addFailToSendNoConnectRule);
        try {
            assertBusy(mockAppender::assertAllExpectationsMatched);
        } finally {
            stopAndRemoveLogger(MetadataTracker.class, mockAppender);
            stopDisrupting();
        }

        // Ensure tracker is still running
        assertBusy(() -> assertThat(isMetadataTrackerActive(), is(true)));

        // Ensure new metadata keeps replicating
        executeOnPublisher("ALTER TABLE doc.t1 ADD COLUMN value string");
        assertBusy(() -> {
            var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                                        " WHERE table_name = 't1'" +
                                        " ORDER BY ordinal_position");
            assertThat(printedTable(r.rows()), is("id\n" +
                                                  "value\n"));
        });
    }

    @Test
    public void test_subscription_metadata_tracker_stops_on_unresolvable_error() throws Exception {
        String subscriptionName = "sub1";
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" + defaultTableSettings() + ")");
        createPublication("pub1", false, List.of("doc.t1"));
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");
        executeOnSubscriber("CREATE SUBSCRIPTION " + subscriptionName +
                            " CONNECTION '" + publisherConnectionUrl() + "' publication pub1");

        // Ensure tracker started and initial recovery is done
        assertBusy(() -> {
            assertThat(isMetadataTrackerActive()).isTrue();
            var res = executeOnSubscriber(
                "SELECT s.subname, s.subpublications, sr.srrelid::text, sr.srsubstate, sr.srsubstate_reason" +
                " FROM pg_subscription s" +
                " JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
                " ORDER BY s.subname");
            assertThat(printedTable(res.rows()), is(
                "sub1| [pub1]| doc.t1| r| NULL\n"
            ));
        });

        var expectedLogMessage = "Tracking of metadata failed for subscription 'sub1' with unrecoverable error, stop tracking";
        var mockAppender = appendLogger(expectedLogMessage, MetadataTracker.class, Level.ERROR);

        startDisrupting((subscriberTransport, publisherAddress) -> {
            subscriberTransport.addSendBehavior(publisherAddress, (connection, requestId, action, request, options) -> {
                if (action.equals(PublicationsStateAction.NAME)) {
                    throw new ElasticsearchException("rejected");
                }
                connection.sendRequest(requestId, action, request, options);
            });
        });
        try {
            assertBusy(mockAppender::assertAllExpectationsMatched);
        } finally {
            stopAndRemoveLogger(MetadataTracker.class, mockAppender);
            stopDisrupting();
        }

        // Ensure tracker stopped
        assertBusy(() -> assertThat(isMetadataTrackerActive(), is(false)));

        // Ensure failure state is set correctly
        var res = executeOnSubscriber(
            "SELECT s.subname, s.subpublications, sr.srrelid::text, sr.srsubstate, sr.srsubstate_reason" +
            " FROM pg_subscription s" +
            " JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
            " ORDER BY s.subname");
        assertThat(printedTable(res.rows()),
                   is("sub1| [pub1]| doc.t1| e| Tracking of metadata failed for subscription 'sub1' with unrecoverable error, stop tracking.\nReason: rejected\n"));
    }

    @Test
    public void test_subscription_keeps_tracking_on_subscriber_master_node_changed() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" + defaultTableSettings() + ")");
        createPublication("pub1", false, List.of("doc.t1"));
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");
        executeOnSubscriber("CREATE SUBSCRIPTION sub1" +
            " CONNECTION '" + publisherConnectionUrl() + "' publication pub1");

        // Ensure tracker started and initial recovery is done
        assertBusy(() -> {
            assertThat(isMetadataTrackerActive()).isTrue();
            var res = executeOnSubscriber(
                "SELECT s.subname, s.subpublications, sr.srrelid::text, sr.srsubstate, sr.srsubstate_reason" +
                    " FROM pg_subscription s" +
                    " JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
                    " ORDER BY s.subname");
            assertThat(printedTable(res.rows()), is(
                "sub1| [pub1]| doc.t1| r| NULL\n"
            ));
        });

        String isolatedNode = subscriberCluster.getMasterName();
        NetworkDisruption.TwoPartitions partitions = isolateNode(subscriberCluster, isolatedNode);
        NetworkDisruption networkDisruption = new NetworkDisruption(partitions, new NetworkDisruption.NetworkDisconnect());
        subscriberCluster.setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        String nonIsolatedNode = partitions.getMajoritySide().iterator().next();

        // make sure cluster reforms
        ensureStableCluster(subscriberCluster, 2, nonIsolatedNode, logger);

        // restore isolation
        networkDisruption.stopDisrupting();

        // Ensure tracker is still running
        assertBusy(() -> assertThat(isMetadataTrackerActive(), is(true)));

        // Ensure new metadata keeps replicating
        executeOnPublisher("ALTER TABLE doc.t1 ADD COLUMN value string");
        assertBusy(() -> {
            var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                " WHERE table_name = 't1'" +
                " ORDER BY ordinal_position");
            assertThat(printedTable(r.rows()), is("id\n" +
                "value\n"));
        });

    }

    private boolean isMetadataTrackerActive() throws Exception {
        var masterNode = subscriberCluster.getMasterName();
        var replicationService = subscriberCluster.getInstance(LogicalReplicationService.class, masterNode);
        Field m = replicationService.getClass().getDeclaredField("metadataTracker");
        m.setAccessible(true);
        MetadataTracker metadataTracker = (MetadataTracker) m.get(replicationService);

        // Ensure tracker started
        Field f1 = metadataTracker.getClass().getDeclaredField("isActive");
        f1.setAccessible(true);
        return (boolean) f1.get(metadataTracker);
    }

    private MockLogAppender appendLogger(String expectedLogMessage, Class<?> clazz, Level level) throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
            expectedLogMessage,
            clazz.getCanonicalName(),
            level,
            expectedLogMessage));
        Logger classLogger = LogManager.getLogger(clazz);
        Loggers.addAppender(classLogger, mockAppender);
        return mockAppender;
    }

    private void stopAndRemoveLogger(Class<?> clazz, MockLogAppender mockAppender) {
        Logger classLogger = LogManager.getLogger(clazz);
        Loggers.removeAppender(classLogger, mockAppender);
        mockAppender.stop();
    }

    private void startDisrupting(BiConsumer<MockTransportService, TransportAddress> failureBehaviour) {
        logger.info("--> start disrupting subscriber<->publisher cluster");
        String subscriberNode = subscriberCluster.getMasterName();
        String publisherNode = publisherCluster.getMasterName();
        MockTransportService subscriberTransport = (MockTransportService) subscriberCluster.getInstance(TransportService.class, subscriberNode);
        MockTransportService publisherTransport = (MockTransportService) publisherCluster.getInstance(TransportService.class, publisherNode);

        PostgresNetty publisherPostgres = publisherCluster.getInstance(PostgresNetty.class);
        for (var address : MockTransportService.extractTransportAddresses(publisherTransport)) {
            failureBehaviour.accept(subscriberTransport, address);
        }
        failureBehaviour.accept(subscriberTransport, publisherPostgres.boundAddress().publishAddress());
    }

    private void stopDisrupting() {
        logger.info("--> stop disrupting subscriber<->publisher cluster");
        String subscriberNode = subscriberCluster.getMasterName();
        String publisherNode = publisherCluster.getMasterName();
        MockTransportService subscriberTransport = (MockTransportService) subscriberCluster.getInstance(TransportService.class, subscriberNode);
        MockTransportService publisherTransport = (MockTransportService) publisherCluster.getInstance(TransportService.class, publisherNode);
        PostgresNetty publisherPostgres = publisherCluster.getInstance(PostgresNetty.class);
        subscriberTransport.clearOutboundRules(publisherTransport);
        subscriberTransport.clearOutboundRules(publisherPostgres.boundAddress().publishAddress());
    }
}
