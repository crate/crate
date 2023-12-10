/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import io.crate.action.FutureActionListener;
import io.crate.common.unit.TimeValue;
import io.netty.channel.embedded.EmbeddedChannel;

public class TransportHandshakerTests extends ESTestCase {

    @Rule
    public MockitoRule initRule = MockitoJUnit.rule();

    private TransportHandshaker handshaker;
    private DiscoveryNode node;
    private CloseableChannel channel;
    private TestThreadPool threadPool;
    @Mock
    private TransportHandshaker.HandshakeRequestSender requestSender;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        String nodeId = "node-id";
        channel = new CloseableChannel(new EmbeddedChannel(), false);
        node = new DiscoveryNode(nodeId, nodeId, nodeId, "host", "host_address", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.emptySet(), Version.CURRENT);
        threadPool = new TestThreadPool("thread-poll");
        handshaker = new TransportHandshaker(Version.CURRENT, threadPool, requestSender);
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdown();
        super.tearDown();
    }

    @Test
    public void testHandshakeRequestAndResponse() throws Exception {
        FutureActionListener<Version> versionFuture = new FutureActionListener<>();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, Version.CURRENT.minimumCompatibilityVersion());

        assertFalse(versionFuture.isDone());

        TransportHandshaker.HandshakeRequest handshakeRequest = new TransportHandshaker.HandshakeRequest(Version.CURRENT);
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        handshakeRequest.writeTo(bytesStreamOutput);
        StreamInput input = bytesStreamOutput.bytes().streamInput();
        final FutureActionListener<TransportResponse> responseFuture = new FutureActionListener<>();
        final TestTransportChannel channel = new TestTransportChannel(responseFuture);
        handshaker.handleHandshake(channel, reqId, input);

        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleResponse((TransportHandshaker.HandshakeResponse) responseFuture.get());

        assertTrue(versionFuture.isDone());
        assertEquals(Version.CURRENT, versionFuture.get());
    }

    @Test
    public void testHandshakeRequestFutureVersionsCompatibility() throws Exception {
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), new FutureActionListener<>());

        verify(requestSender).sendRequest(node, channel, reqId, Version.CURRENT.minimumCompatibilityVersion());

        TransportHandshaker.HandshakeRequest handshakeRequest = new TransportHandshaker.HandshakeRequest(Version.CURRENT);
        BytesStreamOutput currentHandshakeBytes = new BytesStreamOutput();
        handshakeRequest.writeTo(currentHandshakeBytes);

        BytesStreamOutput lengthCheckingHandshake = new BytesStreamOutput();
        BytesStreamOutput futureHandshake = new BytesStreamOutput();
        TaskId.EMPTY_TASK_ID.writeTo(lengthCheckingHandshake);
        TaskId.EMPTY_TASK_ID.writeTo(futureHandshake);
        try (BytesStreamOutput internalMessage = new BytesStreamOutput()) {
            Version.writeVersion(Version.CURRENT, internalMessage);
            lengthCheckingHandshake.writeBytesReference(internalMessage.bytes());
            internalMessage.write(new byte[1024]);
            futureHandshake.writeBytesReference(internalMessage.bytes());
        }
        StreamInput futureHandshakeStream = futureHandshake.bytes().streamInput();
        // We check that the handshake we serialize for this test equals the actual request.
        // Otherwise, we need to update the test.
        assertEquals(currentHandshakeBytes.bytes().length(), lengthCheckingHandshake.bytes().length());
        assertEquals(1031, futureHandshakeStream.available());
        final FutureActionListener<TransportResponse> responseFuture = new FutureActionListener<>();
        final TestTransportChannel channel = new TestTransportChannel(responseFuture);
        handshaker.handleHandshake(channel, reqId, futureHandshakeStream);
        assertEquals(0, futureHandshakeStream.available());

        TransportHandshaker.HandshakeResponse response = (TransportHandshaker.HandshakeResponse) responseFuture.get();

        assertEquals(Version.CURRENT, response.getResponseVersion());
    }

    @Test
    public void testHandshakeError() throws IOException {
        FutureActionListener<Version> versionFuture = new FutureActionListener<>();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, Version.CURRENT.minimumCompatibilityVersion());

        assertFalse(versionFuture.isDone());

        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleException(new TransportException("failed"));

        assertTrue(versionFuture.isDone());
        assertThatThrownBy(versionFuture::get)
            .cause()
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("handshake failed");
    }

    @Test
    public void testSendRequestThrowsException() throws IOException {
        FutureActionListener<Version> versionFuture = new FutureActionListener<>();
        long reqId = randomLongBetween(1, 10);
        Version compatibilityVersion = Version.CURRENT.minimumCompatibilityVersion();
        doThrow(new IOException("boom")).when(requestSender).sendRequest(node, channel, reqId, compatibilityVersion);

        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        assertTrue(versionFuture.isDone());
        assertThatThrownBy(versionFuture::get)
            .cause()
            .isExactlyInstanceOf(ConnectTransportException.class)
            .hasMessageContaining("failure to send internal:tcp/handshake");
        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }

    @Test
    public void testHandshakeTimeout() throws IOException {
        FutureActionListener<Version> versionFuture = new FutureActionListener<>();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(100, TimeUnit.MILLISECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, Version.CURRENT.minimumCompatibilityVersion());

        assertThatThrownBy(versionFuture::get)
            .cause()
            .isExactlyInstanceOf(ConnectTransportException.class)
            .hasMessageContaining("handshake_timeout");

        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }
}
