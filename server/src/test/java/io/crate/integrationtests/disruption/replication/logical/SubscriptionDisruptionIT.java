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

import io.crate.integrationtests.LogicalReplicationITestCase;
import io.crate.replication.logical.action.PublicationsStateAction;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;

@LogicalReplicationITestCase.PublisherClusterScope(numberOfNodes = 1, supportsDedicatedMasters = false)
@LogicalReplicationITestCase.SubscriberClusterScope(numberOfNodes = 1, supportsDedicatedMasters = false)
public class SubscriptionDisruptionIT extends LogicalReplicationITestCase {

    @Test
    public void test_subscription_state_on_restore_failure() throws Exception {
        String subscriptionName = "sub1";
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

        try {
            executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH (number_of_replicas=0)");
            createPublication("pub1", false, List.of("doc.t1"));
            executeOnSubscriber("CREATE SUBSCRIPTION " + subscriptionName +
                                " CONNECTION '" + publisherConnectionUrl() + "' publication pub1");

            assertBusy(
                () -> {
                    var res = executeOnSubscriber(
                        "SELECT s.subname, s.subpublications, sr.srrelid::text, sr.srsubstate, sr.srsubstate_reason" +
                        " FROM pg_subscription s" +
                        " JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
                        " ORDER BY s.subname");
                    assertThat(printedTable(res.rows()),
                               is("sub1| [pub1]| doc.t1| e| Failed to request the publications state\n"));
                }
            );
        } finally {
            transportServices.forEach(MockTransportService::clearAllRules);
        }
    }
}
