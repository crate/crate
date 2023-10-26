/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.junit.Assert.fail;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.ThreadPool;

public class TransportServiceDeserializationFailureTests extends ESTestCase {

    public void testDeserializationFailureLogIdentifiesListener() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode otherNode = new DiscoveryNode("other", buildNewFakeTransportAddress(), Version.CURRENT);

        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "local").build();

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings, random());

        final String testActionName = "internal:test-action";

        final MockTransport transport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                if (action.equals(TransportService.HANDSHAKE_ACTION_NAME)) {
                    handleResponse(requestId, new TransportService.HandshakeResponse(otherNode, new ClusterName(""), Version.CURRENT));
                }
            }
        };
        final TransportService transportService = transport.createTransportService(Settings.EMPTY,
                deterministicTaskQueue.getThreadPool(), ignored -> localNode, null);

        transportService.registerRequestHandler(testActionName, ThreadPool.Names.SAME, f -> new TransportRequest.Empty(),
                (request, channel) -> channel.sendResponse(TransportResponse.Empty.INSTANCE));

        transportService.start();
        transportService.acceptIncomingRequests();

        final PlainActionFuture<Void> connectionFuture = new PlainActionFuture<>();
        transportService.connectToNode(otherNode, connectionFuture);
        assertThat(connectionFuture.isDone()).isTrue();

        {
            // requests without a parent task are recorded directly in the response context

            transportService.sendRequest(otherNode, testActionName, TransportRequest.Empty.INSTANCE,
                    TransportRequestOptions.EMPTY, new TransportResponseHandler<TransportResponse.Empty>() {
                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            fail("should not be called");
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            fail("should not be called");
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public TransportResponse.Empty read(StreamInput in) {
                            throw new AssertionError("should not be called");
                        }

                        @Override
                        public String toString() {
                            return "test handler without parent";
                        }
                    });

            final List<Transport.ResponseContext<? extends TransportResponse>> responseContexts
                    = transport.getResponseHandlers().prune(ignored -> true);
            assertThat(responseContexts).hasSize(1);
            final TransportResponseHandler<? extends TransportResponse> handler = responseContexts.get(0).handler();
            assertThat(handler.toString()).contains("test handler without parent");
        }

        {

            transportService.sendChildRequest(otherNode, testActionName, TransportRequest.Empty.INSTANCE,
                    TransportRequestOptions.EMPTY, new TransportResponseHandler<TransportResponse.Empty>() {
                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            fail("should not be called");
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            fail("should not be called");
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public TransportResponse.Empty read(StreamInput in) {
                            throw new AssertionError("should not be called");
                        }

                        @Override
                        public String toString() {
                            return "test handler with parent";
                        }
                    });

            final List<Transport.ResponseContext<? extends TransportResponse>> responseContexts
                    = transport.getResponseHandlers().prune(ignored -> true);
            assertThat(responseContexts).hasSize(1);
            final TransportResponseHandler<? extends TransportResponse> handler = responseContexts.get(0).handler();
            assertThat(handler.toString()).contains("test handler with parent", testActionName);
        }
    }

}
