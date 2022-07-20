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

package org.elasticsearch.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import io.crate.common.io.IOUtils;
import io.crate.netty.NettyBootstrap;

public class TransportActionProxyTests extends ESTestCase {
    protected ThreadPool threadPool;
    // we use always a non-alpha or beta version here otherwise minimumCompatibilityVersion will be different for the two used versions
    private static final Version CURRENT_VERSION = Version.fromString(String.valueOf(Version.CURRENT.major) + ".0.0");
    protected static final Version version0 = CURRENT_VERSION.minimumCompatibilityVersion();

    protected DiscoveryNode nodeA;
    protected MockTransportService serviceA;

    protected static final Version version1 = Version.fromId(CURRENT_VERSION.externalId + 1);
    protected DiscoveryNode nodeB;
    protected MockTransportService serviceB;

    protected DiscoveryNode nodeC;
    protected MockTransportService serviceC;
    private NettyBootstrap nettyBootstrap;


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        nettyBootstrap = new NettyBootstrap(Settings.EMPTY);
        nettyBootstrap.start();
        threadPool = new TestThreadPool(getClass().getName());
        serviceA = buildService(version0); // this one supports dynamic tracer updates
        nodeA = serviceA.getLocalDiscoNode();
        serviceB = buildService(version1); // this one doesn't support dynamic tracer updates
        nodeB = serviceB.getLocalDiscoNode();
        serviceC = buildService(version1); // this one doesn't support dynamic tracer updates
        nodeC = serviceC.getLocalDiscoNode();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(serviceA, serviceB, serviceC, nettyBootstrap, () -> {
            terminate(threadPool);
        });
    }

    private MockTransportService buildService(final Version version) {
        MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, version, threadPool, nettyBootstrap);
        service.start();
        service.acceptIncomingRequests();
        return service;

    }


    public void testSendMessage() throws InterruptedException {
        serviceA.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
                                        (request, channel) -> {
                                            assertEquals(request.sourceNode, "TS_A");
                                            SimpleTestResponse response = new SimpleTestResponse("TS_A");
                                            channel.sendResponse(response);
                                        });
        TransportActionProxy.registerProxyAction(serviceA, "internal:test", SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceA, nodeB);

        serviceB.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
                                        (request, channel) -> {
                                            assertEquals(request.sourceNode, "TS_A");
                                            SimpleTestResponse response = new SimpleTestResponse("TS_B");
                                            channel.sendResponse(response);
                                        });
        TransportActionProxy.registerProxyAction(serviceB, "internal:test", SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceB, nodeC);
        serviceC.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
                                        (request, channel) -> {
                                            assertEquals(request.sourceNode, "TS_A");
                                            SimpleTestResponse response = new SimpleTestResponse("TS_C");
                                            channel.sendResponse(response);
                                        });
        TransportActionProxy.registerProxyAction(serviceC, "internal:test", SimpleTestResponse::new);

        CountDownLatch latch = new CountDownLatch(1);
        serviceA.sendRequest(nodeB, TransportActionProxy.getProxyAction("internal:test"), TransportActionProxy.wrapRequest(nodeC,
                                                                                                                           new SimpleTestRequest("TS_A")), new TransportResponseHandler<SimpleTestResponse>() {
            @Override
            public SimpleTestResponse read(StreamInput in) throws IOException {
                return new SimpleTestResponse(in);
            }

            @Override
            public void handleResponse(SimpleTestResponse response) {
                try {
                    assertEquals("TS_C", response.targetNode);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                try {
                    throw new AssertionError(exp);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        });
        latch.await();
    }

    public void testException() throws InterruptedException {
        serviceA.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
                                        (request, channel) -> {
                                            assertEquals(request.sourceNode, "TS_A");
                                            SimpleTestResponse response = new SimpleTestResponse("TS_A");
                                            channel.sendResponse(response);
                                        });
        TransportActionProxy.registerProxyAction(serviceA, "internal:test", SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceA, nodeB);

        serviceB.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
                                        (request, channel) -> {
                                            assertEquals(request.sourceNode, "TS_A");
                                            SimpleTestResponse response = new SimpleTestResponse("TS_B");
                                            channel.sendResponse(response);
                                        });
        TransportActionProxy.registerProxyAction(serviceB, "internal:test", SimpleTestResponse::new);
        AbstractSimpleTransportTestCase.connectToNode(serviceB, nodeC);
        serviceC.registerRequestHandler("internal:test", ThreadPool.Names.SAME, SimpleTestRequest::new,
                                        (request, channel) -> {
                                            throw new ElasticsearchException("greetings from TS_C");
                                        });
        TransportActionProxy.registerProxyAction(serviceC, "internal:test", SimpleTestResponse::new);

        CountDownLatch latch = new CountDownLatch(1);
        serviceA.sendRequest(nodeB, TransportActionProxy.getProxyAction("internal:test"), TransportActionProxy.wrapRequest(nodeC,
                                                                                                                           new SimpleTestRequest("TS_A")), new TransportResponseHandler<SimpleTestResponse>() {
            @Override
            public SimpleTestResponse read(StreamInput in) throws IOException {
                return new SimpleTestResponse(in);
            }

            @Override
            public void handleResponse(SimpleTestResponse response) {
                try {
                    fail("expected exception");
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                try {
                    Throwable cause = ExceptionsHelper.unwrap(exp);
                    assertEquals("greetings from TS_C", cause.getMessage());
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        });
        latch.await();
    }

    public static class SimpleTestRequest extends TransportRequest {
        String sourceNode;

        public SimpleTestRequest(String sourceNode) {
            this.sourceNode = sourceNode;
        }
        public SimpleTestRequest() {}

        public SimpleTestRequest(StreamInput in) throws IOException {
            super(in);
            sourceNode = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceNode);
        }
    }

    public static class SimpleTestResponse extends TransportResponse {
        final String targetNode;

        SimpleTestResponse(String targetNode) {
            this.targetNode = targetNode;
        }

        SimpleTestResponse(StreamInput in) throws IOException {
            this.targetNode = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(targetNode);
        }
    }

    public void testGetAction() {
        String action = "foo/bar";
        String proxyAction = TransportActionProxy.getProxyAction(action);
        assertTrue(proxyAction.endsWith(action));
        assertEquals("internal:transport/proxy/foo/bar", proxyAction);
    }

    public void testUnwrap() {
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(nodeA, TransportService.HandshakeRequest.INSTANCE);
        assertTrue(transportRequest instanceof TransportActionProxy.ProxyRequest);
        assertSame(TransportService.HandshakeRequest.INSTANCE, TransportActionProxy.unwrapRequest(transportRequest));
    }

    public void testIsProxyAction() {
        String action = "foo/bar";
        String proxyAction = TransportActionProxy.getProxyAction(action);
        assertTrue(TransportActionProxy.isProxyAction(proxyAction));
        assertFalse(TransportActionProxy.isProxyAction(action));
    }

    public void testIsProxyRequest() {
        assertTrue(TransportActionProxy.isProxyRequest(new TransportActionProxy.ProxyRequest<>(TransportRequest.Empty.INSTANCE, null)));
        assertFalse(TransportActionProxy.isProxyRequest(TransportRequest.Empty.INSTANCE));
    }
}
