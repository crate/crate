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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.common.collections.Tuple;
import io.crate.common.io.Streams;
import io.crate.common.unit.TimeValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;

public class OutboundHandlerTests extends ESTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final TransportRequestOptions options = TransportRequestOptions.EMPTY;
    private final AtomicReference<Tuple<Header, BytesReference>> message = new AtomicReference<>();
    private InboundPipeline pipeline;
    private OutboundHandler handler;
    private CloseableChannel channel;
    private DiscoveryNode node;
    private EmbeddedChannel embeddedChannel;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        embeddedChannel = new EmbeddedChannel();
        TransportAddress transportAddress = buildNewFakeTransportAddress();
        channel = new CloseableChannel(embeddedChannel, randomBoolean()) {

            @Override
            public InetSocketAddress getLocalAddress() {
                return transportAddress.address();
            }
        };
        node = new DiscoveryNode("", transportAddress, Version.CURRENT);
        StatsTracker statsTracker = new StatsTracker();
        handler = new OutboundHandler("node", Version.CURRENT, statsTracker, threadPool, BigArrays.NON_RECYCLING_INSTANCE);

        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final Supplier<CircuitBreaker> breaker = () -> new NoopCircuitBreaker("test");
        final InboundAggregator aggregator = new InboundAggregator(breaker, (Predicate<String>) action -> true);
        pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator,
            (c, m) -> {
                try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
                    Streams.copy(m.openOrGetStreamInput(), streamOutput);
                    message.set(new Tuple<>(m.getHeader(), streamOutput.bytes()));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    @Test
    public void testSendRawBytes() throws Throwable {
        byte[] bytes = "message".getBytes(StandardCharsets.UTF_8);

        ChannelFuture future1 = handler.sendBytes(channel, bytes);
        ByteBuf msg = (ByteBuf) embeddedChannel.outboundMessages().poll();
        assertThat(Unpooled.wrappedBuffer(bytes)).isEqualTo(msg);
        assertThat(future1.get(5, TimeUnit.SECONDS));

        embeddedChannel.disconnect();
        ChannelFuture future2 = handler.sendBytes(channel, bytes);
        assertThatThrownBy(() -> future2.get(5, TimeUnit.SECONDS))
            .hasCauseInstanceOf(ClosedChannelException.class);
    }

    @Test
    public void testSendRequest() throws IOException {
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        String value = "message";
        TestRequest request = new TestRequest(value);

        AtomicReference<DiscoveryNode> nodeRef = new AtomicReference<>();
        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<TransportRequest> requestRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request,
                                      TransportRequestOptions options) {
                nodeRef.set(node);
                requestIdRef.set(requestId);
                actionRef.set(action);
                requestRef.set(request);
            }
        });
        handler.sendRequest(node, channel, requestId, action, request, options, version, compress, isHandshake);

        ByteBuf msg = (ByteBuf) embeddedChannel.outboundMessages().poll();
        BytesReference reference = Netty4Utils.toBytesReference(msg);
        assertThat(nodeRef.get()).isEqualTo(node);
        assertThat(requestIdRef.get()).isEqualTo(requestId);
        assertThat(actionRef.get()).isEqualTo(action);
        assertThat(requestRef.get()).isEqualTo(request);

        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {
        }));
        final Tuple<Header, BytesReference> tuple = message.get();
        final Header header = tuple.v1();
        final TestRequest message = new TestRequest(tuple.v2().streamInput());
        assertThat(header.getVersion()).isEqualTo(version);
        assertThat(header.getRequestId()).isEqualTo(requestId);
        assertThat(header.isRequest()).isTrue();
        assertThat(header.isResponse()).isFalse();
        if (isHandshake) {
            assertThat(header.isHandshake()).isTrue();
        } else {
            assertThat(header.isHandshake()).isFalse();
        }
        if (compress) {
            assertThat(header.isCompressed()).isTrue();
        } else {
            assertThat(header.isCompressed()).isFalse();
        }

        assertThat(message.value).isEqualTo(value);
    }

    @Test
    public void testSendResponse() throws IOException {
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        String value = "message";
        TestResponse response = new TestResponse(value);

        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<TransportResponse> responseRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, TransportResponse response) {
                requestIdRef.set(requestId);
                actionRef.set(action);
                responseRef.set(response);
            }
        });
        handler.sendResponse(version, channel, requestId, action, response, compress, isHandshake);

        ByteBuf msg = (ByteBuf) embeddedChannel.outboundMessages().poll();
        BytesReference reference = Netty4Utils.toBytesReference(msg);
        assertThat(requestIdRef.get()).isEqualTo(requestId);
        assertThat(actionRef.get()).isEqualTo(action);
        assertThat(responseRef.get()).isEqualTo(response);

        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {
        }));
        final Tuple<Header, BytesReference> tuple = message.get();
        final Header header = tuple.v1();
        final TestResponse message = new TestResponse(tuple.v2().streamInput());
        assertThat(header.getVersion()).isEqualTo(version);
        assertThat(header.getRequestId()).isEqualTo(requestId);
        assertThat(header.isRequest()).isFalse();
        assertThat(header.isResponse()).isTrue();
        if (isHandshake) {
            assertThat(header.isHandshake()).isTrue();
        } else {
            assertThat(header.isHandshake()).isFalse();
        }
        if (compress) {
            assertThat(header.isCompressed()).isTrue();
        } else {
            assertThat(header.isCompressed()).isFalse();
        }

        assertThat(header.isError()).isFalse();

        assertThat(message.value).isEqualTo(value);
    }

    @Test
    public void testErrorResponse() throws IOException {
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        ElasticsearchException error = new ElasticsearchException("boom");

        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<Exception> responseRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, Exception error) {
                requestIdRef.set(requestId);
                actionRef.set(action);
                responseRef.set(error);
            }
        });
        handler.sendErrorResponse(version, channel, requestId, action, error);

        ByteBuf msg = (ByteBuf) embeddedChannel.outboundMessages().poll();
        BytesReference reference = Netty4Utils.toBytesReference(msg);
        assertThat(requestIdRef.get()).isEqualTo(requestId);
        assertThat(actionRef.get()).isEqualTo(action);
        assertThat(responseRef.get()).isEqualTo(error);


        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {
        }));
        final Tuple<Header, BytesReference> tuple = message.get();
        final Header header = tuple.v1();
        assertThat(header.getVersion()).isEqualTo(version);
        assertThat(header.getRequestId()).isEqualTo(requestId);
        assertThat(header.isRequest()).isFalse();
        assertThat(header.isResponse()).isTrue();
        assertThat(header.isCompressed()).isFalse();
        assertThat(header.isHandshake()).isFalse();
        assertThat(header.isError()).isTrue();

        RemoteTransportException remoteException = tuple.v2().streamInput().readException();
        assertThat(remoteException.getCause()).isInstanceOf(ElasticsearchException.class);
        assertThat("boom").isEqualTo(remoteException.getCause().getMessage());
        assertThat(remoteException.action()).isEqualTo(action);
        assertThat(remoteException.address().address()).isEqualTo(channel.getLocalAddress());
    }
}
